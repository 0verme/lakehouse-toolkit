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
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
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


class AuditConfidence(str, Enum):
    """Audit detector 对证据充分性的离散判断，不是统计概率。"""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


class IssueDisposition(str, Enum):
    """业务处置状态；它不改变 detector 发现的事实或 stable identity。"""

    OPEN = "OPEN"
    ACCEPTED = "ACCEPTED"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    RESOLVED = "RESOLVED"


# SQLite/reference adapter 读取没有新增 policy 字段的旧行时使用这些值。
LEGACY_AUDIT_RULE_VERSION = "audit-rule-legacy"
LEGACY_AUDIT_POLICY_VERSION = "audit-policy-legacy"


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

PROGRAM_NAME_LEGACY_MARKER = "005"
PROGRAM_NAME_DEFAULT_SUFFIX = "00"

# 保留这个名称只为避免旧调用方在升级时 import 失败。它不再参与解析，
# 也不代表可以配置多个 program-name prefix；legacy grammar 只有固定 marker。
DEFAULT_PROGRAM_NAME_TARGET_PREFIX: str | None = None

_PROGRAM_NAME_STEP_RE = re.compile(r"^[1-9]\d*$")
_DECLARED_TARGET_RE = re.compile(
    r"^(?P<schema>[A-Z][A-Z0-9_]*)\.(?P<table>[A-Z][A-Z0-9_$]*)$"
)

# These are program-name namespace wrappers, not SQL physical schemas. Keep the
# contract explicit until a versioned namespace registry is established.
_LEGACY_PROGRAM_NAMESPACE_MAP = {
    "DWS_DM": "DM",
    "DWS_DWM": "DWM",
    "DWS_DWA": "DWA",
    "DWS_DWP": "DWP",
    "DWS_DWD": "DWD",
    "DWS_DWF": "DWF",
    "DWS_DWUPRR": "DWUPRR",
    "DWS_DWO": "DWO",
    "DWS_DLO": "DLO",
    "DLK_DLO": "DLO",
}

# Business Asset Boundary V1: DLO/DWO are physical-only pre-business layers;
# DWF is the lowest internal business warehouse layer.  The explicit registry
# is intentionally small so a new/unknown schema is never reclassified by a
# fuzzy prefix or a sink name.
BUSINESS_ASSET_MINIMUM_SCHEMA = "DWF"
PRE_BUSINESS_ASSET_SCHEMAS = frozenset({"DLO", "DWO"})


class ProgramNameDiagnostic(str, Enum):
    """``program_name`` 解析产生的非审计诊断。"""

    PROGRAM_NAME_TARGET_RESOLVED = "PROGRAM_NAME_TARGET_RESOLVED"
    PROGRAM_NAME_INCOMPLETE = "PROGRAM_NAME_INCOMPLETE"
    PROGRAM_NAME_STEP_MISSING = "PROGRAM_NAME_STEP_MISSING"
    PROGRAM_NAME_STEP_INVALID = "PROGRAM_NAME_STEP_INVALID"
    PROGRAM_NAME_TARGET_INVALID = "PROGRAM_NAME_TARGET_INVALID"
    PROGRAM_NAME_SUFFIX_NONSTANDARD = "PROGRAM_NAME_SUFFIX_NONSTANDARD"
    PROGRAM_NAME_MARKER_INVALID = "PROGRAM_NAME_MARKER_INVALID"
    PROGRAM_NAME_FORMAT_INVALID = "PROGRAM_NAME_FORMAT_INVALID"
    PROGRAM_NAME_FORMAT_UNSUPPORTED = "PROGRAM_NAME_FORMAT_UNSUPPORTED"


@dataclass(frozen=True, slots=True)
class ProgramNameSemantics:
    """从 legacy ``program_name`` 恢复出的 logical target / step 语义。

    ``logical_target`` 是唯一可以参与 target authority 的 program-name
    字段；当 raw target 使用已确认的 legacy namespace wrapper 时，
    ``logical_target`` 保存规范化后的 physical ``schema.table``。三段 legacy
    shape 只把同一规范化结果暴露为 ``target_hint``，不授予 target authority。
    raw token 不单独进入 identity，仍由原始 ``program_name`` 保留 provenance。
    ``legacy_marker``、``target_hint`` 和 ``opaque_suffix`` 都不参与 Dataset
    Identity；``step_seq`` 也只表达 expected order evidence，不表达 scheduler fact。
    """

    program_name: str
    legacy_marker: str | None
    logical_target: str | None
    step_seq: int | None
    opaque_suffix: str | None
    diagnostics: tuple[ProgramNameDiagnostic, ...] = ()
    target_hint: str | None = None

    @property
    def target_resolved(self) -> bool:
        return self.logical_target is not None

    @property
    def step_resolved(self) -> bool:
        return self.step_seq is not None

    @property
    def is_incomplete(self) -> bool:
        return ProgramNameDiagnostic.PROGRAM_NAME_INCOMPLETE in self.diagnostics

    @property
    def expected_processing_order(self) -> tuple[int, ...]:
        """单个 program 只能提供自己的 step；不会伪造 scheduler dependency。"""

        return () if self.step_seq is None else (self.step_seq,)


def normalize_legacy_program_namespace(target: object) -> str | None:
    """将 program-name-derived target 的已确认 legacy namespace 规范化。

    只处理显式 contract 中的 namespace mapping；未知 schema 保持原值，
    不通过 prefix/suffix 相似度猜测 physical schema。该 helper 只属于
    program-name target authority 边界，DatasetIdentity 不调用它。
    """

    token = decode_code(target).strip().upper()
    match = _DECLARED_TARGET_RE.fullmatch(token)
    if match is None:
        return None
    schema = _LEGACY_PROGRAM_NAMESPACE_MAP.get(
        match.group("schema"), match.group("schema")
    )
    candidate = f"{schema}.{match.group('table')}"
    if is_temporary_asset(candidate):
        return None
    return candidate


def _normalize_program_name_target_token(target_token: object) -> str | None:
    return normalize_legacy_program_namespace(target_token)


def _parse_positive_step(token: str) -> int | None:
    if _PROGRAM_NAME_STEP_RE.fullmatch(token) is None:
        return None
    value = 0
    for character in token:
        value = value * 10 + (ord(character) - ord("0"))
    return value


def parse_program_name(program_name: object) -> ProgramNameSemantics:
    """按固定 ``005`` grammar 以 conservative 策略解析程序名。

    只有严格四段形态才足以授予 program-name target authority；四段中的 raw
    target 会先经过显式 legacy namespace normalization。三段形态会把第二段同样
    规范化为 ``target_hint``，但仍不产生 ``logical_target`` 或 ``step``，避免把
    未知 legacy grammar 变成错误的 authoritative target。
    """

    normalized_name = decode_code(program_name).strip()
    parts = normalized_name.split(":") if normalized_name else []
    marker = parts[0].strip().upper() if parts else None
    diagnostics: list[ProgramNameDiagnostic] = []

    if marker != PROGRAM_NAME_LEGACY_MARKER:
        if marker is not None:
            diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_MARKER_INVALID)
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_TARGET_INVALID)
        if len(parts) > 4:
            diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_FORMAT_INVALID)
        return ProgramNameSemantics(
            program_name=normalized_name,
            legacy_marker=marker,
            logical_target=None,
            step_seq=None,
            opaque_suffix=None,
            target_hint=None,
            diagnostics=tuple(diagnostics),
        )

    target_token = parts[1].strip() if len(parts) > 1 else ""
    candidate_target = _normalize_program_name_target_token(target_token)
    if candidate_target is None:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_TARGET_INVALID)

    if len(parts) != 4:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_FORMAT_UNSUPPORTED)
        if len(parts) < 4:
            diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_INCOMPLETE)
        else:
            diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_FORMAT_INVALID)
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_STEP_MISSING)
        return ProgramNameSemantics(
            program_name=normalized_name,
            legacy_marker=marker,
            logical_target=None,
            step_seq=None,
            opaque_suffix=None,
            target_hint=candidate_target if len(parts) == 3 else None,
            diagnostics=tuple(diagnostics),
        )

    logical_target = candidate_target
    if logical_target is not None:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED)

    step_token = parts[2].strip()
    step_seq = _parse_positive_step(step_token)
    if step_seq is None:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_STEP_INVALID)

    opaque_suffix = parts[3].strip()
    if not opaque_suffix:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_INCOMPLETE)
    elif opaque_suffix != PROGRAM_NAME_DEFAULT_SUFFIX:
        diagnostics.append(ProgramNameDiagnostic.PROGRAM_NAME_SUFFIX_NONSTANDARD)

    return ProgramNameSemantics(
        program_name=normalized_name,
        legacy_marker=marker,
        logical_target=logical_target,
        step_seq=step_seq,
        opaque_suffix=opaque_suffix,
        target_hint=candidate_target,
        diagnostics=tuple(diagnostics),
    )


# 这些函数是已有调用方使用的语义化入口；实现统一委托给 target-first parser。
def extract_program_declared_target_token(program_name: object) -> str | None:
    """仅从 canonical 四段 legacy 程序名提取 authoritative logical target。"""

    return parse_program_name(program_name).logical_target


def extract_program_target_hint(program_name: object) -> str | None:
    """提取不授予 authority 的三段/四段规范化 target candidate。"""

    return parse_program_name(program_name).target_hint


def normalize_declared_target_from_program_name(
    target_token: object,
    program_name_target_prefix: object = DEFAULT_PROGRAM_NAME_TARGET_PREFIX,
) -> str | None:
    """验证并规范化 program-name-derived 的完整 ``schema.table`` target。

    仅应用显式 legacy namespace mapping；未知 schema 保持原值。
    ``program_name_target_prefix`` 仅为旧签名保留，不再参与解析；固定
    ``005`` grammar 不支持 multi-prefix abstraction。
    """

    del program_name_target_prefix
    return _normalize_program_name_target_token(target_token)


def parse_declared_primary_target(
    program_name: object,
    program_name_target_prefix: object = DEFAULT_PROGRAM_NAME_TARGET_PREFIX,
) -> str | None:
    """返回 program-name-derived logical target，不猜测其它字段。"""

    del program_name_target_prefix
    return parse_program_name(program_name).logical_target


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
    ``source_profile``、platform、catalog 或其它数据库层级。canonicalization
    只清理格式并保留 SQL 中观察到的物理 schema，不做 namespace 推断或改写。
    TMP 和缺失 schema 的引用不能构成此对象。
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


def normalize_lineage_schema(value: object) -> str | None:
    """Return the explicit lineage layer name for a schema token.

    Physical DatasetIdentity keeps the observed schema unchanged.  This helper
    only applies the existing explicit legacy/DWS namespace registry when a
    caller needs to classify a business boundary; unknown wrappers remain
    unknown instead of being guessed from a prefix.
    """

    try:
        schema = canonicalize_schema(value)
    except (TypeError, ValueError):
        return None
    return _LEGACY_PROGRAM_NAMESPACE_MAP.get(schema, schema)


def _asset_schema_for_boundary(
    asset_name: object,
    *,
    environment: str | None,
) -> str | None:
    if isinstance(asset_name, DatasetIdentity):
        return normalize_lineage_schema(asset_name.canonical_schema)
    if environment is not None:
        identity = DatasetIdentity.from_name(environment, asset_name)
        if identity is None:
            return None
        return normalize_lineage_schema(identity.canonical_schema)
    parts = _dataset_name_parts(asset_name)
    if parts is None:
        return None
    return normalize_lineage_schema(parts[0])


def is_technical_asset(
    asset_name: object,
    *,
    environment: str | None = None,
) -> bool:
    """Return whether a qualified asset is DLO/DWO technical-only lineage.

    ``environment`` is optional for callers that already have a canonical
    ``schema.table`` value.  Passing it makes the check go through
    ``DatasetIdentity`` and therefore uses the same identity validation as
    ``LineageEdge``.
    """

    if isinstance(asset_name, DatasetIdentity):
        if is_temporary_asset(asset_name.canonical_name):
            return False
    elif is_temporary_asset(asset_name if isinstance(asset_name, str) else None):
        return False
    schema = _asset_schema_for_boundary(asset_name, environment=environment)
    return schema in PRE_BUSINESS_ASSET_SCHEMAS


def is_business_asset(
    asset_name: object,
    *,
    environment: str | None = None,
) -> bool:
    """Return whether a qualified asset may be a Business Lineage endpoint.

    DLO and DWO (including their registered DWS/legacy wrappers) remain valid
    ``DatasetIdentity``/Physical DAG values but are deliberately excluded from
    Business Lineage endpoints.  Other existing qualified formal sources keep
    the compatibility behavior of the current table-level contract.
    """

    if isinstance(asset_name, DatasetIdentity):
        if is_temporary_asset(asset_name.canonical_name):
            return False
    elif is_temporary_asset(asset_name if isinstance(asset_name, str) else None):
        return False
    schema = _asset_schema_for_boundary(asset_name, environment=environment)
    return schema is not None and schema not in PRE_BUSINESS_ASSET_SCHEMAS


@dataclass(frozen=True, slots=True)
class ProgramIdentity:
    """一个程序实例的稳定 identity。

    ``program_name`` 只有在 ``environment`` 和 ``source_profile`` 相同的
    scope 内才有意义。Identity boundary 只去除三个字段的 surrounding
    whitespace，保留大小写；不为了追求与 DatasetIdentity 相同而重写既有
    program/profile 名称。Phase 7 不把没有稳定来源的 ``job_key`` 猜测性地加入
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
        """返回不依赖数据库 surrogate id 的 canonical identity tuple。"""

        return (self.environment, self.source_profile, self.program_name)

    def to_dict(self) -> dict[str, str]:
        """返回可用于稳定 JSON 序列化的 identity payload。"""

        return {
            "environment": self.environment,
            "source_profile": self.source_profile,
            "program_name": self.program_name,
        }


@dataclass(frozen=True, slots=True)
class ProgramSource:
    """Parser 的统一程序输入，不携带来源连接或文件系统细节。

    ``environment``、``source_profile``、``program_name`` 和 ``script_code``
    是必填文本。``expected_target`` 表示 provider 当前是否能提供预期结果
    表；未知时使用 ``None``。``target_hint`` 由 program-name parser 暴露，
    但绝不替代 authoritative ``expected_target``。``source_hash`` 由 provider
    在有能力时提供，
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
    def program_name_semantics(self) -> ProgramNameSemantics:
        """返回不修改 raw ``program_name`` 的解析结果。"""

        return parse_program_name(self.program_name)

    @property
    def logical_target(self) -> str | None:
        """返回 program-name-derived target；显式 target 不在此属性中覆盖它。"""

        return self.program_name_semantics.logical_target

    @property
    def target_hint(self) -> str | None:
        """返回可用于 multi-sink 消歧的 non-authoritative target candidate。"""

        return self.program_name_semantics.target_hint

    @property
    def step_seq(self) -> int | None:
        """返回正整数 program step sequence。"""

        return self.program_name_semantics.step_seq

    @property
    def program_step_seq(self) -> int | None:
        """``step_seq`` 的语义别名，强调它属于 Program Step。"""

        return self.step_seq

    @property
    def opaque_suffix(self) -> str | None:
        """返回不参与 lineage identity/order 的原始 suffix。"""

        return self.program_name_semantics.opaque_suffix

    @property
    def program_name_diagnostics(self) -> tuple[ProgramNameDiagnostic, ...]:
        """返回解析诊断；这些值不是 Audit issue，也不会改变 lineage。"""

        return self.program_name_semantics.diagnostics

    @property
    def resolved_target(self) -> str | None:
        """按 explicit/provider → canonical program-name target 返回 authority。"""

        explicit_target = normalize_expected_target(self.expected_target)
        return explicit_target or self.logical_target

    @property
    def logical_processing_unit_key(self) -> tuple[str, str, str] | None:
        """返回可用于 logical grouping 的 ``environment/profile/target`` key。"""

        target = self.logical_target
        if target is None:
            return None
        return (self.environment, self.source_profile, target)

    @property
    def identity(self) -> ProgramIdentity:
        return ProgramIdentity(
            environment=self.environment,
            source_profile=self.source_profile,
            program_name=self.program_name,
        )


def expected_processing_order(
    program_sources: Iterable[ProgramSource | ProgramNameSemantics],
) -> tuple[int, ...]:
    """按升序返回已识别 step，作为 expected order evidence。

    该函数只返回 program-name-derived 顺序，不创建或暗示 scheduler
    dependency；无法解析的 step 被排除而不被猜测。
    """

    steps: set[int] = set()
    for item in program_sources:
        semantics = (
            item.program_name_semantics if isinstance(item, ProgramSource) else item
        )
        if not isinstance(semantics, ProgramNameSemantics):
            raise TypeError(
                "program_sources must contain ProgramSource or ProgramNameSemantics"
            )
        if semantics.step_seq is not None:
            steps.add(semantics.step_seq)
    return tuple(sorted(steps))


def group_program_sources_by_logical_target(
    program_sources: Iterable[ProgramSource],
) -> dict[str, tuple[ProgramSource, ...]]:
    """按 logical target 聚合 ProgramSource，同时保留每个 raw step provenance。"""

    groups: dict[str, list[ProgramSource]] = {}
    for source in program_sources:
        if not isinstance(source, ProgramSource):
            raise TypeError("program_sources must contain ProgramSource values")
        target = source.logical_target
        if target is not None:
            groups.setdefault(target, []).append(source)

    return {
        target: tuple(
            sorted(
                values,
                key=lambda item: (
                    item.step_seq is None,
                    item.step_seq if item.step_seq is not None else 0,
                    item.program_name,
                ),
            )
        )
        for target, values in sorted(groups.items())
    }


# 更短的兼容别名，避免调用方为 grouping 引入新的 domain hierarchy。
group_program_steps = group_program_sources_by_logical_target


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

    一条 ``LineageEdge`` 表示某环境下，一个 Business Asset 上游到一个 Business Asset
    下游的直接业务血缘事实。它不是全量递归祖先关系；TMP、DLO、DWO 只在 Physical
    DAG/collapse evidence 阶段保留，不能作为 Business endpoint 进入此对象。source/target 必须是
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
        if not is_business_asset(source_identity) or not is_business_asset(target_identity):
            raise ValueError(
                "LineageEdge endpoints must be Business Assets; keep DLO/DWO in Physical DAG"
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
    """AuditFact 的兼容 persistence projection 及其可追踪生命周期。

    ``severity``、``disposition`` 和 ``policy_version`` 来自 policy；
    ``issue_type``、``confidence``、``rule_version``、evidence 和 stable key
    来自 detector fact。保留该扁平对象是为了兼容现有 materialization/SQLite API。
    """

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
    confidence: AuditConfidence | str = AuditConfidence.UNKNOWN
    rule_version: str = LEGACY_AUDIT_RULE_VERSION
    disposition: IssueDisposition | str = IssueDisposition.OPEN
    policy_version: str = LEGACY_AUDIT_POLICY_VERSION
    disposition_updated_at: datetime | None = None
    disposition_updated_by: str | None = None

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
        object.__setattr__(self, "confidence", AuditConfidence(self.confidence))
        object.__setattr__(self, "disposition", IssueDisposition(self.disposition))
        for field_name in ("rule_version", "policy_version"):
            value = getattr(self, field_name)
            _require_text(value, field_name)
            object.__setattr__(self, field_name, value.strip())
        if self.disposition_updated_at is not None and not isinstance(
            self.disposition_updated_at, datetime
        ):
            raise TypeError("disposition_updated_at must be a datetime or None")
        if self.disposition_updated_by is not None:
            _require_text(self.disposition_updated_by, "disposition_updated_by")

    def with_disposition(
        self,
        disposition: IssueDisposition | str,
        *,
        updated_at: datetime | None = None,
        updated_by: str | None = None,
    ) -> "LineageIssue":
        """返回带人工处置记录的新 projection，不改写当前/历史 row。"""

        resolved = IssueDisposition(disposition)
        if updated_at is not None and not isinstance(updated_at, datetime):
            raise TypeError("updated_at must be a datetime or None")
        if updated_by is not None:
            _require_text(updated_by, "updated_by")
        return replace(
            self,
            disposition=resolved,
            disposition_updated_at=updated_at,
            disposition_updated_by=updated_by,
        )

    @property
    def issue_key(self) -> str | None:
        """兼容调用方对稳定 issue identity 的另一种命名。"""

        return self.stable_key

    @property
    def stable_issue_identity(self) -> str | None:
        """stable issue identity 的显式名称；不包含 policy/lifecycle 字段。"""

        return self.stable_key

    @property
    def fingerprint(self) -> str | None:
        """稳定 identity 的语义别名；不会根据 message 或时间变化。"""

        return self.stable_key


__all__ = [
    "BUSINESS_ASSET_MINIMUM_SCHEMA",
    "DEFAULT_PROGRAM_NAME_TARGET_PREFIX",
    "DEFAULT_TEMPORARY_ASSET_RULES",
    "PRE_BUSINESS_ASSET_SCHEMAS",
    "PROGRAM_NAME_DEFAULT_SUFFIX",
    "PROGRAM_NAME_LEGACY_MARKER",
    "ProgramNameDiagnostic",
    "ProgramNameSemantics",
    "canonicalize_dataset_name",
    "canonicalize_schema",
    "canonicalize_table",
    "compute_source_hash",
    "decode_code",
    "AuditConfidence",
    "DatasetIdentity",
    "IssueDisposition",
    "IssueType",
    "LEGACY_AUDIT_POLICY_VERSION",
    "LEGACY_AUDIT_RULE_VERSION",
    "LineageEdge",
    "LineageIssue",
    "PhysicalEdge",
    "PhysicalNode",
    "PhysicalNodeKind",
    "ProgramIdentity",
    "ProgramSource",
    "ProgramState",
    "TemporaryAssetRule",
    "is_business_asset",
    "is_formal_asset",
    "is_technical_asset",
    "is_temporary_asset",
    "normalize_asset_name",
    "normalize_lineage_schema",
    "normalize_declared_target_from_program_name",
    "normalize_expected_target",
    "normalize_program_name",
    "parse_declared_primary_target",
    "parse_program_name",
    "extract_program_declared_target_token",
    "expected_processing_order",
    "group_program_sources_by_logical_target",
    "group_program_steps",
]
