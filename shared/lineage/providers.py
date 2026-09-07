"""统一的 ProgramSource provider 边界。

Provider 只读取并映射程序来源，不解析 SQL，也不构建 Physical DAG。
"""

from __future__ import annotations

import re
from collections.abc import Callable, Generator, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from shared.config.env import metadata_table, required_env, safe_identifier
from shared.lineage.domain import (
    ProgramSource,
    compute_source_hash,
    decode_code,
    normalize_expected_target,
    normalize_program_name,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "configs" / "lineage_providers.local.yaml"
EXAMPLE_CONFIG_PATH = ROOT_DIR / "configs" / "lineage_providers.example.yaml"


class ProviderError(RuntimeError):
    """Provider 读取或映射失败，错误信息不包含密码或完整连接串。"""


class LineageDependencyError(RuntimeError):
    """Lineage CLI 启动所需的 Python 依赖不可用。"""

    def __init__(self, dependency: str) -> None:
        self.dependency = dependency
        super().__init__(f"missing Python dependency: {dependency}")


class LineageConfigError(ValueError):
    """配置加载或校验失败；字段信息只包含安全的结构诊断。"""

    def __init__(
        self,
        code: str,
        *,
        reason: str | None = None,
        line: int | None = None,
        column: int | None = None,
    ) -> None:
        self.code = code
        self.reason = reason
        self.line = line
        self.column = column
        details = [code]
        if reason:
            details.append(f"reason={reason}")
        if line is not None:
            details.append(f"line={line}")
        if column is not None:
            details.append(f"column={column}")
        super().__init__(" ".join(details))


@runtime_checkable
class ProgramSourceProvider(Protocol):
    """向后续 Parser 暴露的最小程序来源协议。"""

    def iter_program_sources(self) -> Iterable[ProgramSource]:
        """以 streaming 方式产生统一的 ``ProgramSource``。"""
        return ()


_CONNECTION_KEYS = ("host", "port", "user", "password", "database")
_LEGACY_CONNECTION_ENV_FIELDS = (
    "host_env",
    "port_env",
    "user_env",
    "password_env",
    "database_env",
)


@dataclass(frozen=True, slots=True)
class MySQLConnectionSettings:
    """已归一化的 MySQL 连接参数。"""

    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str
    charset: str = "utf8mb4"
    autocommit: bool = True


@dataclass(frozen=True, slots=True)
class MySQLProcessProfile:
    """一个 DEV MySQL 来源 profile 的连接和 metadata 配置描述。

    连接来源可以是本地 ``connection`` 值、``connection_env`` 环境变量名，
    或兼容现有配置的顶层 ``*_env`` 字段。三种来源都只在 Provider 开始
    读取时归一为 ``MySQLConnectionSettings``；嵌套连接配置不会进入 profile
    的 repr，避免直接密码被意外打印。
    """

    name: str
    environment: str
    # These fields remain in their original order for positional-call compatibility.
    host_env: str = ""
    port_env: str = ""
    user_env: str = ""
    password_env: str = field(default_factory=str)
    database_env: str = ""
    process_table: str = "demo_meta.processes"
    program_name_column: str = "process_name"
    script_code_column: str = "script_code"
    expected_target_column: str | None = None
    batch_size: int = 200
    charset: str = "utf8mb4"
    autocommit: bool = True
    connection: Mapping[str, object] | None = field(
        default=None, repr=False, hash=False
    )
    connection_env: Mapping[str, object] | None = field(
        default=None, repr=False, hash=False
    )

    def __post_init__(self) -> None:
        for field_name in (
            "name",
            "environment",
            "process_table",
            "program_name_column",
            "script_code_column",
            "charset",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())

        target_column = self.expected_target_column
        if target_column is not None:
            if not isinstance(target_column, str):
                raise ValueError("expected_target_column must be a string or None")
            target_column = target_column.strip() or None
            object.__setattr__(self, "expected_target_column", target_column)

        if isinstance(self.batch_size, bool) or not isinstance(self.batch_size, int):
            raise ValueError("batch_size must be a positive integer")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if not isinstance(self.autocommit, bool):
            raise ValueError("autocommit must be a boolean")

        safe_identifier(self.process_table, "process table")
        safe_identifier(self.program_name_column, "program name column")
        safe_identifier(self.script_code_column, "script code column")
        if self.expected_target_column is not None:
            safe_identifier(self.expected_target_column, "expected target column")

        legacy_fields_present = any(
            value is not None and (not isinstance(value, str) or bool(value.strip()))
            for value in (
                getattr(self, field_name)
                for field_name in _LEGACY_CONNECTION_ENV_FIELDS
            )
        )
        source_count = sum(
            source is not None
            for source in (self.connection, self.connection_env)
        ) + int(legacy_fields_present)
        if source_count != 1:
            raise ValueError(
                "mysql process profile must configure exactly one of "
                "connection, connection_env, or legacy *_env fields"
            )

        if self.connection is not None:
            object.__setattr__(
                self,
                "connection",
                _copy_connection_mapping(self.connection, "connection"),
            )
        if self.connection_env is not None:
            object.__setattr__(
                self,
                "connection_env",
                _copy_connection_env_mapping(self.connection_env),
            )
        if legacy_fields_present:
            for field_name in _LEGACY_CONNECTION_ENV_FIELDS:
                value = getattr(self, field_name)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{field_name} must be a non-empty string")
                object.__setattr__(self, field_name, value.strip())

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> MySQLProcessProfile:
        """从 example/local YAML 的一项 profile 创建配置对象。"""

        if not isinstance(raw, Mapping):
            raise ValueError("mysql process profile must be a mapping")

        def text(key: str) -> str:
            value = str(raw.get(key, "") or "").strip()
            if not value:
                raise ValueError(f"mysql process profile missing required field: {key}")
            return value

        def optional_text(key: str) -> str:
            if key not in raw or raw[key] is None:
                return ""
            return str(raw[key]).strip()

        def source_mapping(key: str) -> Mapping[str, object] | None:
            value = raw.get(key)
            if value is None:
                return None
            if not isinstance(value, Mapping):
                raise ValueError(f"mysql process profile {key} must be a mapping")
            return value

        raw_batch_size = raw.get("batch_size", 200)
        if isinstance(raw_batch_size, bool) or not isinstance(
            raw_batch_size, (int, str)
        ):
            raise ValueError("mysql process profile batch_size must be an integer")
        try:
            batch_size = int(raw_batch_size)
        except ValueError as exc:
            raise ValueError(
                "mysql process profile batch_size must be an integer"
            ) from exc

        raw_autocommit = raw.get("autocommit", True)
        if isinstance(raw_autocommit, str):
            normalized_autocommit = raw_autocommit.strip().lower()
            if normalized_autocommit in {"true", "1", "yes"}:
                raw_autocommit = True
            elif normalized_autocommit in {"false", "0", "no"}:
                raw_autocommit = False
            else:
                raise ValueError("mysql process profile autocommit must be a boolean")
        if not isinstance(raw_autocommit, bool):
            raise ValueError("mysql process profile autocommit must be a boolean")

        process_table = str(
            raw.get("table", raw.get("process_table", "")) or ""
        ).strip()
        if not process_table:
            process_table = metadata_table("processes", "processes")

        expected_target_column = raw.get("expected_target_column")
        if expected_target_column is not None:
            expected_target_column = str(expected_target_column).strip() or None

        return cls(
            name=text("name"),
            environment=text("environment"),
            host_env=optional_text("host_env"),
            port_env=optional_text("port_env"),
            user_env=optional_text("user_env"),
            password_env=optional_text("password_env"),
            database_env=optional_text("database_env"),
            process_table=process_table,
            program_name_column=str(
                raw.get("program_name_column", "process_name")
            ).strip(),
            script_code_column=str(
                raw.get("script_code_column", "script_code")
            ).strip(),
            expected_target_column=expected_target_column,
            batch_size=batch_size,
            charset=str(raw.get("charset", "utf8mb4") or "utf8mb4").strip(),
            autocommit=raw_autocommit,
            connection=source_mapping("connection"),
            connection_env=source_mapping("connection_env"),
        )

    def resolve_connection_settings(self) -> MySQLConnectionSettings:
        """解析三种连接配置来源，统一返回 ``MySQLConnectionSettings``。"""

        context = _profile_context(self.environment, self.name)
        if self.connection is not None:
            return _settings_from_connection(
                self.connection,
                context,
                charset=self.charset,
                autocommit=self.autocommit,
            )
        if self.connection_env is not None:
            return _settings_from_environment(
                self.connection_env,
                context,
                charset=self.charset,
                autocommit=self.autocommit,
            )
        return _settings_from_environment(
            {
                "host": self.host_env,
                "port": self.port_env,
                "user": self.user_env,
                "password": self.password_env,
                "database": self.database_env,
            },
            context,
            charset=self.charset,
            autocommit=self.autocommit,
        )


def _copy_connection_mapping(
    source: Mapping[str, object], label: str
) -> dict[str, object]:
    if not isinstance(source, Mapping):
        raise ValueError(f"{label} must be a mapping")
    copied = dict(source)
    for key in _CONNECTION_KEYS:
        if key not in copied:
            raise ValueError(f"{label} missing required field: {key}")
    return copied


def _copy_connection_env_mapping(
    source: Mapping[str, object],
) -> dict[str, object]:
    copied = _copy_connection_mapping(source, "connection_env")
    for key in _CONNECTION_KEYS:
        value = copied[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"connection_env.{key} must be a non-empty environment variable name"
            )
        copied[key] = value.strip()
    return copied


def _settings_from_connection(
    values: Mapping[str, object],
    context: str,
    *,
    charset: str,
    autocommit: bool,
) -> MySQLConnectionSettings:
    host = _direct_text(values, "host", context)
    user = _direct_text(values, "user", context)
    password = _direct_text(values, "password", context, preserve=True)
    database = _direct_text(values, "database", context)
    port = _parse_port(values.get("port"), context, source="connection")
    return MySQLConnectionSettings(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
        autocommit=autocommit,
    )


def _direct_text(
    values: Mapping[str, object],
    key: str,
    context: str,
    *,
    preserve: bool = False,
) -> str:
    if key not in values:
        raise ProviderError(f"{context}: connection missing required field {key}")
    value = values[key]
    if not isinstance(value, str) or not value.strip():
        raise ProviderError(f"{context}: connection {key} must be a non-empty string")
    return value if preserve else value.strip()


def _settings_from_environment(
    env_names: Mapping[str, object],
    context: str,
    *,
    charset: str,
    autocommit: bool,
) -> MySQLConnectionSettings:
    def required_value(key: str, label: str, *, preserve: bool = False) -> str:
        env_name = env_names.get(key)
        if not isinstance(env_name, str) or not env_name.strip():
            raise ProviderError(
                f"{context}: missing environment variable name for {label}"
            )
        env_name = env_name.strip()
        try:
            value = required_env(env_name)
        except RuntimeError as exc:
            raise ProviderError(
                f"{context}: missing {label} environment variable {env_name}"
            ) from exc
        return value if preserve else value.strip()

    host = required_value("host", "host")
    user = required_value("user", "user")
    password = required_value("password", "password", preserve=True)
    database = required_value("database", "database")
    port_text = required_value("port", "port")
    port = _parse_port(port_text, context)
    return MySQLConnectionSettings(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
        autocommit=autocommit,
    )


def _parse_port(
    value: object,
    context: str,
    *,
    source: str | None = None,
) -> int:
    prefix = f"{source} " if source else ""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ProviderError(f"{context}: {prefix}port must be an integer")
    try:
        port = int(value) if isinstance(value, int) else int(value.strip())
    except (TypeError, ValueError) as exc:
        raise ProviderError(f"{context}: {prefix}port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ProviderError(
            f"{context}: {prefix}port must be between 1 and 65535"
        )
    return port


ConnectionFactory = Callable[[MySQLConnectionSettings], Any]
LegacyProcessLoader = Callable[[], Iterable[object]]
ExpectedTargetGetter = Callable[[object], object | None]


def _profile_context(environment: str, source_profile: str) -> str:
    return f"environment={environment} source_profile={source_profile}"


def default_mysql_connection_factory(settings: MySQLConnectionSettings):
    """创建真实 MySQL 连接；不提供任何 host/账号/密码默认值。"""

    import pymysql

    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        charset=settings.charset,
        autocommit=settings.autocommit,
    )


def _build_process_query(profile: MySQLProcessProfile) -> str:
    columns = [
        safe_identifier(profile.program_name_column, "program name column"),
        safe_identifier(profile.script_code_column, "script code column"),
    ]
    if profile.expected_target_column is not None:
        columns.append(
            safe_identifier(profile.expected_target_column, "expected target column")
        )
    table = safe_identifier(profile.process_table, "process table")
    script_column = safe_identifier(profile.script_code_column, "script code column")
    # 这里只拼接已通过 identifier 校验的配置名，没有把运行时值拼入 SQL。
    # pi-lens-ignore: python-sql-injection
    return (
        f"SELECT {', '.join(columns)} FROM {table} "  # noqa: S608
        f"WHERE {script_column} IS NOT NULL"
    )


def _close_quietly(resource: Any) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        return


class MySQLProcessProvider:
    """从一个 MySQL profile 分批产生 ``ProgramSource``。"""

    def __init__(
        self,
        profile: MySQLProcessProfile,
        *,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if not isinstance(profile, MySQLProcessProfile):
            raise TypeError("profile must be a MySQLProcessProfile")
        self.profile = profile
        self.connection_factory = connection_factory or default_mysql_connection_factory
        self.query = _build_process_query(profile)

    def iter_program_sources(self) -> Generator[ProgramSource, None, None]:
        """解析凭据后返回 lazy iterator；数据库连接在开始迭代时建立。"""

        settings = self.profile.resolve_connection_settings()
        return self._iter_program_sources(settings)

    def _iter_program_sources(
        self, settings: MySQLConnectionSettings
    ) -> Generator[ProgramSource, None, None]:
        context = _profile_context(self.profile.environment, self.profile.name)
        connection = None
        cursor = None
        try:
            try:
                connection = self.connection_factory(settings)
                cursor = connection.cursor()
                # SQL 仅包含经过 safe_identifier 校验的配置标识符。
                cursor.execute(self.query)
            except Exception as exc:
                raise ProviderError(
                    f"{context}: failed to open or query MySQL metadata"
                ) from exc

            row_number = 0
            while True:
                try:
                    batch = cursor.fetchmany(self.profile.batch_size)
                except Exception as exc:
                    raise ProviderError(
                        f"{context}: failed to fetch a metadata batch"
                    ) from exc
                if not batch:
                    break
                for row in batch:
                    row_number += 1
                    try:
                        yield self._row_to_program_source(row)
                    except ProviderError:
                        raise
                    except Exception as exc:
                        raise ProviderError(
                            f"{context}: invalid metadata row {row_number}: {exc}"
                        ) from exc
        finally:
            _close_quietly(cursor)
            _close_quietly(connection)

    def _row_to_program_source(self, row: object) -> ProgramSource:
        program_value = _row_value(row, 0, self.profile.program_name_column)
        script_value = _row_value(row, 1, self.profile.script_code_column)
        target_value = None
        if self.profile.expected_target_column is not None:
            target_value = _row_value(row, 2, self.profile.expected_target_column)

        program_name = normalize_program_name(program_value)
        if not program_name:
            raise ValueError("program_name must be a non-empty value")
        script_code = decode_code(script_value)
        expected_target = normalize_expected_target(target_value)
        return ProgramSource(
            environment=self.profile.environment,
            source_profile=self.profile.name,
            program_name=program_name,
            script_code=script_code,
            expected_target=expected_target,
            source_hash=compute_source_hash(program_name, script_code, expected_target),
        )


def _row_value(row: object, index: int, key: str) -> object:
    if isinstance(row, Mapping):
        if key not in row:
            raise ValueError(f"row is missing column {key}")
        return row[key]
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, KeyError, TypeError) as exc:
        raise ValueError(f"row is missing column {key}") from exc


def _default_legacy_process_loader() -> Iterable[object]:
    """惰性导入旧 loader，避免修改旧入口或在 import 时连接数据库。"""

    from shared.lineage.lineage_builder import load_process_infos

    return load_process_infos()


class ProductionProvider:
    """将现有 production metadata loader 适配为 ``ProgramSource``。

    默认只使用旧 ``ProcessInfo.process_name`` 和 ``script_code``。旧对象没有
    独立 expected target 字段时保持 ``None``；调用方可注入 getter 提供明确的
    metadata 字段，但 adapter 不会从程序名或 SQL 内容猜测 target。
    """

    def __init__(
        self,
        process_loader: LegacyProcessLoader | None = None,
        *,
        environment: str = "PROD",
        source_profile: str = "production_metadata",
        expected_target_getter: ExpectedTargetGetter | None = None,
    ) -> None:
        if not isinstance(environment, str) or not environment.strip():
            raise ValueError("environment must be a non-empty string")
        if not isinstance(source_profile, str) or not source_profile.strip():
            raise ValueError("source_profile must be a non-empty string")
        self.environment = environment.strip()
        self.source_profile = source_profile.strip()
        self.process_loader = process_loader or _default_legacy_process_loader
        self.expected_target_getter = expected_target_getter

    def iter_program_sources(self) -> Generator[ProgramSource, None, None]:
        context = _profile_context(self.environment, self.source_profile)
        try:
            legacy_rows = self.process_loader()
        except Exception as exc:
            raise ProviderError(
                f"{context}: failed to load production metadata"
            ) from exc
        return self._iter_legacy_rows(legacy_rows)

    def _iter_legacy_rows(
        self, legacy_rows: Iterable[object]
    ) -> Generator[ProgramSource, None, None]:
        context = _profile_context(self.environment, self.source_profile)
        try:
            for row_number, row in enumerate(legacy_rows, start=1):
                try:
                    program_value = _legacy_value(
                        row, ("program_name", "process_name"), required=True
                    )
                    script_value = _legacy_value(
                        row, ("script_code", "code"), required=True
                    )
                    if self.expected_target_getter is not None:
                        target_value = self.expected_target_getter(row)
                    else:
                        target_value = _legacy_value(
                            row, ("expected_target",), required=False
                        )
                    program_name = normalize_program_name(program_value)
                    if not program_name:
                        raise ValueError("program_name must be a non-empty value")
                    script_code = decode_code(script_value)
                    expected_target = normalize_expected_target(target_value)
                    yield ProgramSource(
                        environment=self.environment,
                        source_profile=self.source_profile,
                        program_name=program_name,
                        script_code=script_code,
                        expected_target=expected_target,
                        source_hash=compute_source_hash(
                            program_name, script_code, expected_target
                        ),
                    )
                except ProviderError:
                    raise
                except Exception as exc:
                    raise ProviderError(
                        f"{context}: invalid legacy metadata row {row_number}: {exc}"
                    ) from exc
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(
                f"{context}: failed while iterating production metadata"
            ) from exc


def _legacy_value(
    row: object, names: tuple[str, ...], *, required: bool
) -> object | None:
    missing = object()
    value: object = missing
    if isinstance(row, Mapping):
        for name in names:
            if name in row:
                value = row[name]
                break
    else:
        for name in names:
            if hasattr(row, name):
                value = getattr(row, name)
                break
    if value is missing:
        if required:
            raise ValueError(f"legacy row is missing field {names[0]}")
        return None
    return value


def iter_program_sources(
    providers: Iterable[ProgramSourceProvider],
) -> Generator[ProgramSource, None, None]:
    """按 provider 顺序 streaming 聚合，不缓存全部程序。"""

    for provider in providers:
        yield from provider.iter_program_sources()


_FIELD_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_SAFE_VALIDATION_TAILS = {
    "must be a non-empty string",
    "must be a string or None",
    "must be a positive integer",
    "must be a boolean",
    "must be a mapping",
    "must be an integer",
}


def _safe_profile_reason(error: Exception) -> str:
    """将 profile 校验错误归一为不包含配置值的字段级诊断。"""

    message = str(error)
    if message == "mysql process profile must be a mapping":
        return "profile must be a mapping"
    if message == (
        "mysql process profile must configure exactly one of "
        "connection, connection_env, or legacy *_env fields"
    ):
        return "configure exactly one connection source"

    required_prefix = "mysql process profile missing required field: "
    if message.startswith(required_prefix):
        field_name = message.removeprefix(required_prefix)
        if _FIELD_NAME_RE.fullmatch(field_name):
            return f"missing required field: {field_name}"

    connection_required_prefix = "connection missing required field: "
    if message.startswith(connection_required_prefix):
        field_name = message.removeprefix(connection_required_prefix)
        if _FIELD_NAME_RE.fullmatch(field_name):
            return f"connection missing required field: {field_name}"

    if message.startswith("Invalid SQL "):
        label = message.removeprefix("Invalid SQL ").split(":", 1)[0].strip()
        if label and all(character.isalnum() or character == " " for character in label):
            return f"invalid SQL identifier for {label.lower()}"

    candidate = message
    if candidate.startswith("mysql process profile "):
        candidate = candidate.removeprefix("mysql process profile ")
    if " must " in candidate:
        field_name, tail = candidate.split(" must ", 1)
        if _FIELD_NAME_RE.fullmatch(field_name) and f"must {tail}" in _SAFE_VALIDATION_TAILS:
            return f"{field_name} must {tail}"

    return "profile fields or structure are invalid"


def _yaml_error_location(error: Exception) -> tuple[int | None, int | None]:
    mark = getattr(error, "problem_mark", None) or getattr(
        error, "context_mark", None
    )
    line = getattr(mark, "line", None)
    column = getattr(mark, "column", None)
    return (
        line + 1 if isinstance(line, int) and line >= 0 else None,
        column + 1 if isinstance(column, int) and column >= 0 else None,
    )


def load_mysql_process_profiles(
    config_path: str | Path | None = None,
) -> list[MySQLProcessProfile]:
    """加载 local/example YAML 中的 1..N 个 MySQL process profiles。"""

    try:
        import yaml
    except ModuleNotFoundError as exc:
        if exc.name == "yaml":
            raise LineageDependencyError("PyYAML") from exc
        raise

    path = (
        Path(config_path)
        if config_path is not None
        else (CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH)
    )
    try:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise LineageConfigError("CONFIG_NOT_FOUND") from exc
    except (OSError, UnicodeError) as exc:
        raise LineageConfigError("CONFIG_READ_ERROR") from exc
    except yaml.YAMLError as exc:
        line, column = _yaml_error_location(exc)
        raise LineageConfigError(
            "YAML_INVALID",
            line=line,
            column=column,
        ) from exc

    if not isinstance(data, Mapping):
        raise LineageConfigError(
            "CONFIG_INVALID",
            reason="root must be a mapping",
        )

    raw_profiles = data.get("mysql_process_profiles", [])
    if not isinstance(raw_profiles, list):
        raise LineageConfigError(
            "CONFIG_INVALID",
            reason="mysql_process_profiles must be a list",
        )

    profiles: list[MySQLProcessProfile] = []
    for index, item in enumerate(raw_profiles):
        try:
            profiles.append(MySQLProcessProfile.from_mapping(item))
        except (TypeError, ValueError) as exc:
            reason = _safe_profile_reason(exc)
            raise LineageConfigError(
                "CONFIG_INVALID",
                reason=f"profile[{index}] {reason}",
            ) from exc
    return profiles


__all__ = [
    "ConnectionFactory",
    "ExpectedTargetGetter",
    "EXAMPLE_CONFIG_PATH",
    "LegacyProcessLoader",
    "LineageConfigError",
    "LineageDependencyError",
    "MySQLConnectionSettings",
    "MySQLProcessProfile",
    "MySQLProcessProvider",
    "ProductionProvider",
    "ProgramSourceProvider",
    "ProviderError",
    "default_mysql_connection_factory",
    "iter_program_sources",
    "load_mysql_process_profiles",
]
