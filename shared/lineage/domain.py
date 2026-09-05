"""Phase 1 lineage domain objects and semantic boundaries.

This module is deliberately independent from database clients, metadata schemas,
filesystem access, and parser implementations. Providers create ``ProgramSource``
objects; later parser and builder phases exchange the physical graph objects;
materialization consumes ``LineageEdge`` and ``LineageIssue``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


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
    DAG 阶段保留，默认不能作为正式业务资产进入此对象。
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
        if self.program_name is not None:
            _require_text(self.program_name, "program_name")
        if self.job_key is not None:
            _require_text(self.job_key, "job_key")


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

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile", "program_name"):
            _require_text(getattr(self, field_name), field_name)
        _require_text(self.severity, "severity")
        _require_text(self.message, "message")
        if self.node_key is not None:
            _require_text(self.node_key, "node_key")
        if self.branch_sink is not None:
            _require_text(self.branch_sink, "branch_sink")
        object.__setattr__(self, "issue_type", IssueType(self.issue_type))


__all__ = [
    "DEFAULT_TEMPORARY_ASSET_RULES",
    "IssueType",
    "LineageEdge",
    "LineageIssue",
    "PhysicalEdge",
    "PhysicalNode",
    "PhysicalNodeKind",
    "ProgramSource",
    "TemporaryAssetRule",
    "is_formal_asset",
    "is_temporary_asset",
    "normalize_asset_name",
]
