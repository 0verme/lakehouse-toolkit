"""开发环境 MySQL 调度配置血缘的 domain、scope 与 provider。

该模块只读取已配置的 relation table，将每条 ``SRC_TABLE_KEY ->
TAR_TABLE_KEY`` 映射成一个 configured schedule lineage fact。它不构建
process DAG，也不改写 SQL parser 使用的 DatasetIdentity normalization。
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from shared.config.env import safe_identifier
from shared.lineage.domain import (
    canonicalize_dataset_name,
    decode_code,
    normalize_asset_name,
    normalize_legacy_program_namespace,
)
from shared.lineage.providers import (
    MySQLConnectionSettings,
    MySQLProcessProfile,
    ProviderError,
    ScheduleLineageConfig,
)

SCHEDULE_KEY_SEPARATOR = "\x1f"
SCHEDULE_DWS_TABLE = "lineage_schedule_edge"


def _required_text(value: object, field_name: str) -> str:
    text = decode_code(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def _canonical_key_text(value: object, field_name: str) -> str:
    return _required_text(value, field_name).upper()


def _stable_hash(prefix: str, *parts: str) -> str:
    values = (_required_text(prefix, "key prefix"),)
    values += tuple(_canonical_key_text(part, "key part") for part in parts)
    if any(SCHEDULE_KEY_SEPARATOR in value for value in values):
        raise ValueError("schedule stable-key values must not contain the separator")
    return hashlib.sha256(
        SCHEDULE_KEY_SEPARATOR.join(values).encode("utf-8")
    ).hexdigest()


def normalize_schedule_table_key(value: object) -> str:
    """规范 schedule comparison identity，仅转换明确的 DWS wrapper schema。

    ``DWS_DWF.A`` 等调度 wrapper 使用已有的显式 legacy namespace registry
    映射为 ``DWF.A``；非 ``DWS_`` schema 原样保留其 canonical schema/table。
    SQL parser 的 ``normalize_table_name()`` 和 ``DatasetIdentity`` 不调用此
    helper。
    """

    text = normalize_asset_name(decode_code(value))
    canonical = canonicalize_dataset_name(text)
    if canonical is None:
        raise ValueError("schedule table must be a qualified schema.table")
    schema = canonical.split(".", 1)[0]
    if schema.startswith("DWS_"):
        mapped = normalize_legacy_program_namespace(canonical)
        if mapped is not None:
            return mapped
    return canonical


def _raw_table_text(value: object, field_name: str) -> str:
    text = decode_code(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def schedule_edge_key(edge: "ScheduleLineageEdge") -> str:
    """Return a deterministic batch-independent identity for one schedule fact."""

    if not isinstance(edge, ScheduleLineageEdge):
        raise TypeError("edge must be a ScheduleLineageEdge")
    return _stable_hash(
        "schedule-edge",
        _required_text(edge.environment, "environment"),
        _required_text(edge.source_profile, "source_profile"),
        _required_text(edge.process_name, "process_name"),
        _required_text(edge.project_version_key, "project_version_key"),
        _required_text(edge.source_table, "source_table"),
        _required_text(edge.target_table, "target_table"),
    )


def schedule_row_key(batch_id: str, edge_key: str) -> str:
    """Return the snapshot-specific row identity."""

    return _stable_hash("row", SCHEDULE_DWS_TABLE, batch_id, edge_key)


@dataclass(frozen=True, slots=True)
class ScheduleLineageEdge:
    """一条已配置的调度前后置表关系及其 comparison identity。"""

    environment: str
    source_profile: str
    process_name: str
    project_version_key: str
    raw_source_table: str
    raw_target_table: str
    source_table: str = ""
    target_table: str = ""

    def __post_init__(self) -> None:
        for field_name in (
            "environment",
            "source_profile",
            "process_name",
            "project_version_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "raw_source_table",
            _raw_table_text(self.raw_source_table, "raw_source_table"),
        )
        object.__setattr__(
            self,
            "raw_target_table",
            _raw_table_text(self.raw_target_table, "raw_target_table"),
        )
        object.__setattr__(
            self,
            "source_table",
            normalize_schedule_table_key(
                self.raw_source_table if not self.source_table else self.source_table
            ),
        )
        object.__setattr__(
            self,
            "target_table",
            normalize_schedule_table_key(
                self.raw_target_table if not self.target_table else self.target_table
            ),
        )

    @property
    def schedule_edge_key(self) -> str:
        return schedule_edge_key(self)

    @property
    def scope(self) -> tuple[str, str]:
        return (self.environment, self.source_profile)

    def row_key(self, batch_id: str) -> str:
        return schedule_row_key(batch_id, self.schedule_edge_key)


@dataclass(frozen=True, slots=True)
class ScheduleLineageLoadStats:
    """仅包含可安全输出的来源 aggregate。"""

    source_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    normalized_edges: int = 0
    deduplicated_edges: int = 0
    selected_edges: int = 0
    invalid_rows: int = 0
    source_complete: bool = True


@dataclass(frozen=True, slots=True)
class ScheduleLineageLoadResult:
    edges: tuple[ScheduleLineageEdge, ...]
    stats: ScheduleLineageLoadStats


def deduplicate_schedule_edges(
    edges: Iterable[ScheduleLineageEdge],
) -> tuple[ScheduleLineageEdge, ...]:
    """按 stable identity 去重，同 process 的重复配置只保留一条。"""

    selected: dict[str, ScheduleLineageEdge] = {}
    for edge in edges:
        if not isinstance(edge, ScheduleLineageEdge):
            raise TypeError("edges must contain ScheduleLineageEdge values")
        key = edge.schedule_edge_key
        current = selected.get(key)
        if current is None or _edge_sort_key(edge) < _edge_sort_key(current):
            selected[key] = edge
    return tuple(sorted(selected.values(), key=_edge_sort_key))


def _edge_sort_key(edge: ScheduleLineageEdge) -> tuple[str, ...]:
    return (
        edge.schedule_edge_key,
        edge.raw_source_table,
        edge.raw_target_table,
        edge.process_name,
        edge.project_version_key,
    )


def _scope_match(
    config: ScheduleLineageConfig,
    project_version_key: str,
    raw_target_table: str,
) -> bool:
    project = decode_code(project_version_key).strip().upper()
    if project in config.include_projects:
        return True
    normalized_target = normalize_asset_name(raw_target_table)
    return any(
        project == conditional.project
        and normalized_target.startswith(conditional.target_schema_prefix)
        for conditional in config.conditional_projects
    )


ConnectionFactory = Callable[[MySQLConnectionSettings], Any]


class MySQLScheduleLineageProvider:
    """从一个现有 ``mysql_process_profiles`` 读取 schedule relation facts。"""

    def __init__(
        self,
        profile: MySQLProcessProfile,
        *,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if not isinstance(profile, MySQLProcessProfile):
            raise TypeError("profile must be a MySQLProcessProfile")
        config = profile.schedule_lineage
        if config is None or not config.enabled:
            raise ValueError("schedule_lineage is not enabled for this profile")
        if profile.environment.strip().upper() != "DEV":
            raise ValueError("schedule lineage only supports DEV profiles")
        self.profile = profile
        self.schedule_config = config
        self.source_profile = profile.name
        self.environment = profile.environment
        self.connection_factory = connection_factory or self._default_connection_factory
        self.query = self._build_query(config)
        self.last_result: ScheduleLineageLoadResult | None = None

    @staticmethod
    def _default_connection_factory(settings: MySQLConnectionSettings) -> Any:
        from shared.lineage.providers import default_mysql_connection_factory

        return default_mysql_connection_factory(settings)

    @staticmethod
    def _build_query(config: ScheduleLineageConfig) -> str:
        identifiers = (
            config.process_name_column,
            config.project_version_column,
            config.source_table_column,
            config.target_table_column,
        )
        safe_columns = tuple(
            safe_identifier(value, "schedule lineage column") for value in identifiers
        )
        table = safe_identifier(config.table, "schedule lineage table")
        # The query contains only validated identifiers. Scope filtering stays in
        # Python so the provider can report rejected rows without a second query.
        return (
            f"SELECT {safe_columns[0]}, {safe_columns[1]}, {safe_columns[2]}, "
            f"{safe_columns[3]} FROM {table}"
        )

    def iter_edges(self) -> Iterator[ScheduleLineageEdge]:
        """兼容 streaming 调用方；最终 aggregate 保存在 ``last_result``。"""

        yield from self.load().edges

    def load(self, limit: int | None = None) -> ScheduleLineageLoadResult:
        if isinstance(limit, bool) or (limit is not None and limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        settings = self.profile.resolve_connection_settings()
        connection = None
        cursor = None
        source_rows = 0
        accepted_rows = 0
        rejected_rows = 0
        normalized_edges = 0
        invalid_rows = 0
        candidates: list[ScheduleLineageEdge] = []
        context = f"environment={self.environment} source_profile={self.source_profile}"
        try:
            try:
                connection = self.connection_factory(settings)
                cursor = connection.cursor()
                # SQL contains validated identifiers only; relation values are read,
                # never interpolated or written back to MySQL.
                cursor.execute(self.query)
            except Exception as exc:
                raise ProviderError(
                    f"{context}: failed to open or query schedule relation"
                ) from exc

            while True:
                try:
                    rows = cursor.fetchmany(self.profile.batch_size)
                except Exception as exc:
                    raise ProviderError(
                        f"{context}: failed to fetch schedule relation batch"
                    ) from exc
                if not rows:
                    break
                for row in rows:
                    source_rows += 1
                    try:
                        process_name, project, raw_source, raw_target = (
                            self._row_values(row)
                        )
                        process_text = decode_code(process_name).strip()
                        project_text = decode_code(project).strip()
                        raw_source_text = decode_code(raw_source).strip()
                        raw_target_text = decode_code(raw_target).strip()
                        if not _scope_match(
                            self.schedule_config, project_text, raw_target_text
                        ):
                            rejected_rows += 1
                            continue
                        edge = ScheduleLineageEdge(
                            environment=self.environment,
                            source_profile=self.source_profile,
                            process_name=process_text,
                            project_version_key=project_text,
                            raw_source_table=raw_source_text,
                            raw_target_table=raw_target_text,
                        )
                        candidates.append(edge)
                        accepted_rows += 1
                        normalized_edges += 1
                    except Exception:
                        rejected_rows += 1
                        invalid_rows += 1
        finally:
            self._close_quietly(cursor)
            self._close_quietly(connection)

        deduplicated = deduplicate_schedule_edges(candidates)
        selected = deduplicated if limit is None else deduplicated[:limit]
        result = ScheduleLineageLoadResult(
            edges=selected,
            stats=ScheduleLineageLoadStats(
                source_rows=source_rows,
                accepted_rows=accepted_rows,
                rejected_rows=rejected_rows,
                normalized_edges=normalized_edges,
                deduplicated_edges=len(deduplicated),
                selected_edges=len(selected),
                invalid_rows=invalid_rows,
                source_complete=invalid_rows == 0,
            ),
        )
        self.last_result = result
        return result

    def _row_values(self, row: object) -> tuple[object, object, object, object]:
        columns = (
            self.schedule_config.process_name_column,
            self.schedule_config.project_version_column,
            self.schedule_config.source_table_column,
            self.schedule_config.target_table_column,
        )
        if isinstance(row, Mapping):
            try:
                return (
                    row[columns[0]],
                    row[columns[1]],
                    row[columns[2]],
                    row[columns[3]],
                )
            except KeyError as exc:
                raise ValueError(
                    f"schedule row is missing column {exc.args[0]}"
                ) from exc
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            try:
                return (row[0], row[1], row[2], row[3])
            except IndexError as exc:
                raise ValueError(
                    "schedule row must contain four configured columns"
                ) from exc
        raise ValueError("schedule row must contain four configured columns")

    @staticmethod
    def _close_quietly(resource: Any) -> None:
        if resource is None:
            return
        try:
            resource.close()
        except Exception:
            return


__all__ = [
    "ConnectionFactory",
    "MySQLScheduleLineageProvider",
    "SCHEDULE_DWS_TABLE",
    "ScheduleLineageEdge",
    "ScheduleLineageLoadResult",
    "ScheduleLineageLoadStats",
    "deduplicate_schedule_edges",
    "normalize_schedule_table_key",
    "schedule_edge_key",
    "schedule_row_key",
]
