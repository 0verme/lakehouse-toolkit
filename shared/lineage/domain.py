"""Phase 1 lineage domain objects and semantic boundaries.

This module is deliberately independent from database clients, metadata schemas,
filesystem access, and parser implementations. Providers create ``ProgramSource``
objects; later parser and builder phases exchange the physical graph objects;
materialization consumes ``LineageEdge`` and ``LineageIssue``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .version import LINEAGE_PIPELINE_VERSION


class PhysicalNodeKind(str, Enum):
    """资产在程序 Physical DAG 中的边界分类。"""

    FORMAL_ASSET = "formal_asset"
    TEMPORARY_ASSET = "temporary_asset"


class IssueType(str, Enum):
    """Phase 1 冻结的首批血缘审计问题类型。"""

    ORPHAN_BRANCH = "ORPHAN_BRANCH"
    MULTI_SINK_CANDIDATE = "MULTI_SINK_CANDIDATE"
    TARGET_NOT_FOUND = "TARGET_NOT_FOUND"
    TARGET_MISMATCH = "TARGET_MISMATCH"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    SELF_REFERENCE = "SELF_REFERENCE"
    LINEAGE_BRANCH_BROKEN = "LINEAGE_BRANCH_BROKEN"


TemporaryAssetRule = Callable[[str], bool]

# 现有 apps/svn_check/core/lakehouse/_sql_parser.py 负责识别 CREATE TEMP
# TABLE 语句，ddl_rule.py 负责 TMP_ 命名检查。这里不复制 SQL 语句解析，
# 只为 Physical DAG 提供可替换的资产名称分类边界。
_DEFAULT_TMP_NAME_RE = re.compile(r"^TMP(?:$|[_-]|\d)")


def normalize_asset_name(value: str | None) -> str:
    """仅做资产名称分类所需的轻量清理，不替代项目既有表名 normalize。"""

    text = str(value or "").strip().upper()
    text = (
        text.replace("`", "")
        .replace('"', "")
        .replace("'", "")
        .replace("[", "")
        .replace("]", "")
    )
    return re.sub(r"\s+", "", text)


def _default_tmp_name_rule(normalized_name: str) -> bool:
    short_name = normalized_name.rsplit(".", 1)[-1]
    return bool(_DEFAULT_TMP_NAME_RE.match(short_name))


DEFAULT_TEMPORARY_ASSET_RULES: tuple[TemporaryAssetRule, ...] = (
    _default_tmp_name_rule,
)

# ``DEMO_`` is the public fixture/legacy naming namespace.  Production
# providers must opt in explicitly with ``primary_target_strategy`` and a
# configured prefix; this default only keeps the standalone parser compatible
# with the documented demo format.
DEFAULT_PROGRAM_NAME_TARGET_PREFIX = "DEMO_"
_DECLARED_TARGET_SCHEMAS = frozenset(
    {"DM", "DWA", "DWD", "DWF", "DWM", "DWO", "DWP", "DWE"}
)
_PROGRAM_NAME_SEQUENCE_RE = re.compile(r"^\d{3}$")
_PROGRAM_NAME_REVISION_RE = re.compile(r"^\d+$")
_PROGRAM_NAME_CLOCK_RE = re.compile(r"^\d{2}$")
_DECLARED_TARGET_RE = re.compile(
    r"^(?P<schema>[A-Z][A-Z0-9_]*)\.(?P<table>[A-Z][A-Z0-9_$]*)$"
)
_PROGRAM_NAME_TARGET_PREFIX_RE = re.compile(r"^[A-Z][A-Z0-9_]*_$")


def extract_program_declared_target_token(program_name: object) -> str | None:
    """从高置信 legacy ``program_name`` 格式提取第二段 target token。

    公开格式为 ``NNN:<program-target>:<revision>:<clock>``。只接受四段、
    数字 sequence/revision/clock 和非空 target 段，避免把任意带冒号的名称
    当成 declared target。
    """

    parts = decode_code(program_name).strip().split(":")
    if len(parts) != 4:
        return None

    sequence, target_token, revision, clock = (part.strip() for part in parts)
    if not _PROGRAM_NAME_SEQUENCE_RE.fullmatch(sequence):
        return None
    if not _PROGRAM_NAME_REVISION_RE.fullmatch(revision):
        return None
    if not _PROGRAM_NAME_CLOCK_RE.fullmatch(clock):
        return None
    if not target_token or ":" in target_token:
        return None
    return target_token.upper()


def _normalize_program_name_target_prefix(prefix: object) -> str | None:
    normalized = decode_code(prefix).strip().upper()
    if not normalized or not _PROGRAM_NAME_TARGET_PREFIX_RE.fullmatch(normalized):
        return None
    return normalized


def normalize_declared_target_from_program_name(
    target_token: object,
    program_name_target_prefix: object = DEFAULT_PROGRAM_NAME_TARGET_PREFIX,
) -> str | None:
    """按明确配置剥离程序命名空间并返回 plain ``SCHEMA.TABLE``。

    这里只移除完整匹配的配置前缀，不根据下划线位置、最后一个 schema
    或 ``XXX_DWM`` 等形状猜测。schema 也必须属于当前已知的 warehouse
    schema 集合；失败时统一返回 ``None``。
    """

    prefix = _normalize_program_name_target_prefix(program_name_target_prefix)
    token = decode_code(target_token).strip().upper()
    if prefix is None or not token.startswith(prefix):
        return None

    candidate = token[len(prefix) :]
    match = _DECLARED_TARGET_RE.fullmatch(candidate)
    if match is None:
        return None
    schema = match.group("schema")
    if schema not in _DECLARED_TARGET_SCHEMAS:
        return None
    return f"{schema}.{match.group('table')}"


def parse_declared_primary_target(
    program_name: object,
    program_name_target_prefix: object = DEFAULT_PROGRAM_NAME_TARGET_PREFIX,
) -> str | None:
    """提取并规范化 ``program_name`` 中的 declared primary target hint。"""

    target_token = extract_program_declared_target_token(program_name)
    if target_token is None:
        return None
    return normalize_declared_target_from_program_name(
        target_token, program_name_target_prefix
    )


def is_temporary_asset(
    asset_name: str | None,
    *,
    rules: tuple[TemporaryAssetRule, ...] | None = None,
) -> bool:
    """判断资产名称是否符合默认或调用方提供的 TMP 规则。

    规则接收已大写、去空白和去引号的完整名称，例如 ``DWM.TMP_1``。
    ``rules=()`` 可显式关闭默认规则；不会把未知命名静默默认定为 TMP。
    """

    normalized = normalize_asset_name(asset_name)
    active_rules = DEFAULT_TEMPORARY_ASSET_RULES if rules is None else rules
    return bool(normalized) and any(rule(normalized) for rule in active_rules)


def is_formal_asset(
    asset_name: str | None,
    *,
    rules: tuple[TemporaryAssetRule, ...] | None = None,
) -> bool:
    """判断名称是否为非 TMP 的候选正式资产。"""

    normalized = normalize_asset_name(asset_name)
    return bool(normalized) and not is_temporary_asset(normalized, rules=rules)


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def decode_code(value: object) -> str:
    """将 metadata 中常见的代码值转换为稳定的文本。"""

    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="ignore")
    return str(value)


def normalize_program_name(value: object) -> str:
    """解码并清理程序名；Provider 会对空值显式失败。"""

    return decode_code(value).strip()


def normalize_expected_target(value: object) -> str | None:
    """将可选的 expected target 映射为空值或非空文本。"""

    text = decode_code(value).strip()
    return text or None


def compute_source_hash(
    program_name: str,
    script_code: str,
    expected_target: str | None,
) -> str:
    """按固定 JSON canonical 计算 Phase 2 Provider 的 SHA-256。"""

    if not isinstance(program_name, str):
        raise TypeError("program_name must be a string")
    if not isinstance(script_code, str):
        raise TypeError("script_code must be a string")
    if expected_target is not None and not isinstance(expected_target, str):
        raise TypeError("expected_target must be a string or None")

    canonical = json.dumps(
        {
            "expected_target": expected_target,
            "program_name": program_name,
            "script_code": script_code,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonicalize_dataset_part(value: object, field_name: str) -> str:
    text = decode_code(value).strip()
    text = (
        text.replace("`", "")
        .replace('"', "")
        .replace("'", "")
        .replace("[", "")
        .replace("]", "")
        .strip()
    )
    if not text or "." in text:
        raise ValueError(f"{field_name} must be a non-empty identifier part")
    return text.upper()


def canonicalize_schema(value: object) -> str:
    """规范 DatasetIdentity 的 schema 值；只做 trim + upper。"""

    return _canonicalize_dataset_part(value, "schema")


def canonicalize_table(value: object) -> str:
    """规范 DatasetIdentity 的 table 值；只做 trim + upper。"""

    return _canonicalize_dataset_part(value, "table")


def _dataset_name_parts(value: object) -> tuple[str, str] | None:
    text = decode_code(value).strip()
    if not text:
        return None
    parts = tuple(part.strip() for part in text.split("."))
    if len(parts) != 2 or not all(parts):
        return None
    try:
        return canonicalize_schema(parts[0]), canonicalize_table(parts[1])
    except ValueError:
        return None


def canonicalize_dataset_name(value: object) -> str | None:
    """规范 ``schema.table``；缺失或多余 namespace 时返回 ``None``。"""

    parts = _dataset_name_parts(value)
    return None if parts is None else ".".join(parts)


@dataclass(frozen=True, slots=True)
class DatasetIdentity:
    """Dataset Identity Contract V1 的 physical dataset value object。

    Identity 只有 ``environment + canonical_schema + canonical_table``；不含
    ``source_profile``、platform、catalog 或其它数据库层级。TMP 和缺失
    schema 的引用不能构成此对象。
    """

    environment: str
    canonical_schema: str
    canonical_table: str

    def __post_init__(self) -> None:
        _require_text(self.environment, "environment")
        schema = canonicalize_schema(self.canonical_schema)
        table = canonicalize_table(self.canonical_table)
        canonical_name = f"{schema}.{table}"
        if is_temporary_asset(canonical_name):
            raise ValueError("temporary assets cannot be DatasetIdentity values")
        object.__setattr__(self, "environment", self.environment.strip())
        object.__setattr__(self, "canonical_schema", schema)
        object.__setattr__(self, "canonical_table", table)

    @classmethod
    def from_name(
        cls,
        environment: str,
        dataset_name: object,
    ) -> DatasetIdentity | None:
        """从明确的 ``schema.table`` 创建 identity；无法解析时不猜 namespace。"""

        parts = _dataset_name_parts(dataset_name)
        if parts is None:
            return None
        try:
            return cls(environment, *parts)
        except ValueError:
            return None

    @classmethod
    def from_table_name(
        cls,
        environment: str,
        table_name: object,
    ) -> DatasetIdentity | None:
        """``from_name`` 的语义别名，便于调用方表达 table namespace。"""

        return cls.from_name(environment, table_name)

    @property
    def schema(self) -> str:
        return self.canonical_schema

    @property
    def table(self) -> str:
        return self.canonical_table

    @property
    def canonical_name(self) -> str:
        return f"{self.canonical_schema}.{self.canonical_table}"

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.environment, self.canonical_schema, self.canonical_table)

    def to_dict(self) -> dict[str, str]:
        return {
            "environment": self.environment,
            "canonical_schema": self.canonical_schema,
            "canonical_table": self.canonical_table,
        }


@dataclass(frozen=True, slots=True)
class ProgramIdentity:
    """一个程序实例的稳定 identity。

    ``program_name`` 只有在 ``environment`` 和 ``source_profile`` 相同的
    scope 内才有意义。Phase 7 不把没有稳定来源的 ``job_key`` 猜测性地加入
    identity；如果 Provider 以后提供稳定 job identity，应单独扩展 Provider
    contract，而不是改变本类已有三元组的语义。
    """

    environment: str
    source_profile: str
    program_name: str

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile", "program_name"):
            _require_text(getattr(self, field_name), field_name)
            object.__setattr__(self, field_name, getattr(self, field_name).strip())

    @property
    def scope(self) -> tuple[str, str]:
        return (self.environment, self.source_profile)

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.environment, self.source_profile, self.program_name)


@dataclass(frozen=True, slots=True)
class ProgramSource:
    """Parser 的统一程序输入，不携带来源连接或文件系统细节。

    ``environment``、``source_profile``、``program_name`` 和 ``script_code``
    是必填文本。``expected_target`` 表示 provider 当前是否能提供预期结果
    表；未知时使用 ``None``。``source_hash`` 由 provider 在有能力时提供，
    Phase 1 不计算、不校验算法，也不把 bytes 作为领域输入；bytes decode
    属于后续 provider 边界。
    """

    environment: str
    source_profile: str
    program_name: str
    script_code: str
    expected_target: str | None = None
    source_hash: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "environment",
            "source_profile",
            "program_name",
        ):
            _require_text(getattr(self, field_name), field_name)
        if not isinstance(self.script_code, str):
            raise ValueError("script_code must be a string")
        if self.expected_target is not None and not isinstance(
            self.expected_target, str
        ):
            raise ValueError("expected_target must be a string or None")
        if self.source_hash is not None and not isinstance(self.source_hash, str):
            raise ValueError("source_hash must be a string or None")

    @property
    def identity(self) -> ProgramIdentity:
        return ProgramIdentity(
            environment=self.environment,
            source_profile=self.source_profile,
            program_name=self.program_name,
        )


@dataclass(frozen=True, slots=True)
class ProgramState:
    """可持久化的当前程序状态；历史 batch 通过 ``batch_id`` 保留。

    ``pipeline_version=None`` 表示旧 schema 或旧手工 state 没有版本信息，
    planner 必须将其保守地视为需要 rebuild。
    """

    environment: str
    source_profile: str
    program_name: str
    source_hash: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime | None = None
    batch_id: str | None = None
    is_active: bool = True
    pipeline_version: str | None = None

    def __post_init__(self) -> None:
        identity = ProgramIdentity(
            self.environment,
            self.source_profile,
            self.program_name,
        )
        object.__setattr__(self, "environment", identity.environment)
        object.__setattr__(self, "source_profile", identity.source_profile)
        object.__setattr__(self, "program_name", identity.program_name)
        if self.source_hash is not None and (
            not isinstance(self.source_hash, str) or not self.source_hash.strip()
        ):
            raise ValueError("source_hash must be a non-empty string or None")
        if self.pipeline_version is not None:
            if (
                not isinstance(self.pipeline_version, str)
                or not self.pipeline_version.strip()
            ):
                raise ValueError("pipeline_version must be a non-empty string or None")
            object.__setattr__(self, "pipeline_version", self.pipeline_version.strip())
        for field_name in ("first_seen_at", "last_seen_at"):
            if not isinstance(getattr(self, field_name), datetime):
                raise TypeError(f"{field_name} must be a datetime")
        if self.last_changed_at is not None and not isinstance(
            self.last_changed_at, datetime
        ):
            raise TypeError("last_changed_at must be a datetime or None")
        if self.batch_id is not None:
            _require_text(self.batch_id, "batch_id")
            object.__setattr__(self, "batch_id", self.batch_id.strip())
        if not isinstance(self.is_active, bool):
            raise TypeError("is_active must be a boolean")

    @property
    def identity(self) -> ProgramIdentity:
        return ProgramIdentity(
            self.environment,
            self.source_profile,
            self.program_name,
        )

    @classmethod
    def from_source(
        cls,
        source: ProgramSource,
        *,
        observed_at: datetime,
        batch_id: str | None = None,
        first_seen_at: datetime | None = None,
        last_changed_at: datetime | None = None,
        pipeline_version: str | None = LINEAGE_PIPELINE_VERSION,
    ) -> ProgramState:
        if not isinstance(source, ProgramSource):
            raise TypeError("source must be a ProgramSource")
        return cls(
            environment=source.environment,
            source_profile=source.source_profile,
            program_name=source.program_name,
            source_hash=source.source_hash,
            first_seen_at=first_seen_at or observed_at,
            last_seen_at=observed_at,
            last_changed_at=last_changed_at,
            batch_id=batch_id,
            is_active=True,
            pipeline_version=pipeline_version,
        )


@dataclass(frozen=True, slots=True)
class PhysicalNode:
    """程序内部 DAG 节点；TMP 节点必须在 Physical 层保留。"""

    node_key: str
    asset_name: str
    kind: PhysicalNodeKind | None = None

    def __post_init__(self) -> None:
        _require_text(self.node_key, "node_key")
        _require_text(self.asset_name, "asset_name")
        resolved_kind = (
            PhysicalNodeKind(self.kind)
            if self.kind is not None
            else (
                PhysicalNodeKind.TEMPORARY_ASSET
                if is_temporary_asset(self.asset_name)
                else PhysicalNodeKind.FORMAL_ASSET
            )
        )
        object.__setattr__(self, "kind", resolved_kind)

    @property
    def is_temporary(self) -> bool:
        return self.kind is PhysicalNodeKind.TEMPORARY_ASSET

    @property
    def is_formal(self) -> bool:
        return self.kind is PhysicalNodeKind.FORMAL_ASSET


@dataclass(frozen=True, slots=True)
class PhysicalEdge:
    """Physical DAG 的有向边，``source`` 永远是上游、``target`` 是下游。

    该对象允许 source/target 为 TMP 节点，也允许暂时保留自引用边，供后续
    audit 阶段生成 ``SELF_REFERENCE``，而不是在构图阶段静默丢弃。
    """

    source: str
    target: str
    evidence_type: str = "program_dag"
    evidence: Mapping[str, object] | str | None = None

    def __post_init__(self) -> None:
        _require_text(self.source, "source")
        _require_text(self.target, "target")


@dataclass(frozen=True, slots=True)
class LineageEdge:
    """正式资产之间的直接业务血缘事实。

    一条 ``LineageEdge`` 表示某环境下，一个正式上游资产到一个正式下游
    资产的直接业务血缘事实。它不是全量递归祖先关系；TMP 只在 Physical
    DAG 阶段保留，默认不能作为正式业务资产进入此对象。source/target 必须是
    可解析的 ``schema.table`` DatasetIdentity；``evidence`` 可携带不含完整源码
    的结构化 provenance，供 materialization adapter 序列化。
    """

    environment: str
    source_profile: str
    source_table: str
    target_table: str
    program_name: str | None = None
    job_key: str | None = None
    evidence_type: str = "physical_dag"
    source_hash: str | None = None
    batch_id: str | None = None
    observed_at: datetime | None = None
    updated_at: datetime | None = None
    is_active: bool = True
    evidence: Mapping[str, object] | str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "environment",
            "source_profile",
            "source_table",
            "target_table",
        ):
            _require_text(getattr(self, field_name), field_name)
        if is_temporary_asset(self.source_table) or is_temporary_asset(
            self.target_table
        ):
            raise ValueError(
                "LineageEdge endpoints must be formal assets; keep TMP in Physical DAG"
            )
        source_identity = DatasetIdentity.from_name(self.environment, self.source_table)
        target_identity = DatasetIdentity.from_name(self.environment, self.target_table)
        if source_identity is None or target_identity is None:
            raise ValueError(
                "LineageEdge endpoints must be qualified schema.table dataset references"
            )
        object.__setattr__(self, "environment", source_identity.environment)
        object.__setattr__(self, "source_table", source_identity.canonical_name)
        object.__setattr__(self, "target_table", target_identity.canonical_name)
        if self.program_name is not None:
            _require_text(self.program_name, "program_name")
        if self.job_key is not None:
            _require_text(self.job_key, "job_key")

    @property
    def source_dataset_identity(self) -> DatasetIdentity:
        identity = DatasetIdentity.from_name(self.environment, self.source_table)
        if identity is None:
            raise RuntimeError("LineageEdge source endpoint lost DatasetIdentity")
        return identity

    @property
    def target_dataset_identity(self) -> DatasetIdentity:
        identity = DatasetIdentity.from_name(self.environment, self.target_table)
        if identity is None:
            raise RuntimeError("LineageEdge target endpoint lost DatasetIdentity")
        return identity


@dataclass(frozen=True, slots=True)
class LineageIssue:
    """Physical DAG 审计事实及其可追踪生命周期。"""

    environment: str
    source_profile: str
    program_name: str
    issue_type: IssueType | str
    severity: str
    message: str
    node_key: str | None = None
    branch_sink: str | None = None
    evidence: Mapping[str, object] | str | None = None
    batch_id: str | None = None
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    is_active: bool = True
    stable_key: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile", "program_name"):
            _require_text(getattr(self, field_name), field_name)
        _require_text(self.severity, "severity")
        _require_text(self.message, "message")
        if self.node_key is not None:
            _require_text(self.node_key, "node_key")
        if self.branch_sink is not None:
            _require_text(self.branch_sink, "branch_sink")
        if self.stable_key is not None:
            _require_text(self.stable_key, "stable_key")
        object.__setattr__(self, "issue_type", IssueType(self.issue_type))

    @property
    def issue_key(self) -> str | None:
        """兼容调用方对稳定 issue identity 的另一种命名。"""

        return self.stable_key

    @property
    def fingerprint(self) -> str | None:
        """稳定 identity 的语义别名；不会根据 message 或时间变化。"""

        return self.stable_key


__all__ = [
    "DEFAULT_PROGRAM_NAME_TARGET_PREFIX",
    "DEFAULT_TEMPORARY_ASSET_RULES",
    "canonicalize_dataset_name",
    "canonicalize_schema",
    "canonicalize_table",
    "compute_source_hash",
    "decode_code",
    "DatasetIdentity",
    "IssueType",
    "LineageEdge",
    "LineageIssue",
    "PhysicalEdge",
    "PhysicalNode",
    "PhysicalNodeKind",
    "ProgramIdentity",
    "ProgramSource",
    "ProgramState",
    "TemporaryAssetRule",
    "is_formal_asset",
    "is_temporary_asset",
    "normalize_asset_name",
    "normalize_declared_target_from_program_name",
    "normalize_expected_target",
    "normalize_program_name",
    "parse_declared_primary_target",
    "extract_program_declared_target_token",
]
