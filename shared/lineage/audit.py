"""基于 ``ProgramPhysicalDAG`` 的轻量血缘审计与异常检测。

本模块是 Physical DAG 的只读观察者：它只消费 Phase 3 已确认的节点、边、
steps 和 sinks，不重新解析 ``script_code``，也不修改图、不折叠 TMP、不生成
正式 ``LineageEdge``。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType

from shared.lineage.domain import (
    AuditConfidence,
    IssueDisposition,
    IssueType,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    is_business_asset,
    is_technical_asset,
)
from shared.lineage.physical_dag import ProgramPhysicalDAG

AUDIT_RULE_VERSION = "audit-rule-v1"
AUDIT_POLICY_VERSION = "audit-policy-v1"

ISSUE_SEVERITY_POLICY: Mapping[IssueType, str] = MappingProxyType(
    {
        IssueType.TARGET_NOT_FOUND: "HIGH",
        IssueType.TARGET_MISMATCH: "HIGH",
        IssueType.CYCLE_DETECTED: "HIGH",
        IssueType.SELF_REFERENCE: "HIGH",
        IssueType.ORPHAN_BRANCH: "MEDIUM",
        IssueType.LINEAGE_BRANCH_BROKEN: "HIGH",
        IssueType.MULTI_SINK_CANDIDATE: "MEDIUM",
    }
)


@dataclass(frozen=True, slots=True)
class AuditFact:
    """Detector 输出的事实；不携带 severity、disposition 或 lifecycle。"""

    environment: str
    source_profile: str
    program_name: str
    issue_type: IssueType | str
    message: str = ""
    node_key: str | None = None
    branch_sink: str | None = None
    evidence: Mapping[str, object] | str | None = None
    confidence: AuditConfidence | str = AuditConfidence.HIGH
    rule_version: str = AUDIT_RULE_VERSION
    stable_key: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile", "program_name"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        issue_type = IssueType(self.issue_type)
        object.__setattr__(self, "issue_type", issue_type)
        if not isinstance(self.message, str):
            raise TypeError("message must be a string")
        message = self.message.strip() or issue_type.value
        object.__setattr__(self, "message", message)
        for field_name in ("node_key", "branch_sink"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{field_name} must be a non-empty string or None")
            if isinstance(value, str):
                object.__setattr__(self, field_name, value.strip())
        if isinstance(self.evidence, Mapping):
            object.__setattr__(self, "evidence", dict(self.evidence))
        object.__setattr__(self, "confidence", AuditConfidence(self.confidence))
        if not isinstance(self.rule_version, str) or not self.rule_version.strip():
            raise ValueError("rule_version must be a non-empty string")
        object.__setattr__(self, "rule_version", self.rule_version.strip())
        stable_key = self.stable_key
        if stable_key is not None and (
            not isinstance(stable_key, str) or not stable_key.strip()
        ):
            raise ValueError("stable_key must be a non-empty string or None")
        if stable_key is None:
            cycle_nodes: Iterable[str] = ()
            if issue_type is IssueType.CYCLE_DETECTED and isinstance(
                self.evidence, Mapping
            ):
                raw_nodes = self.evidence.get("cycle_nodes", ())
                if isinstance(raw_nodes, (list, tuple, set, frozenset)):
                    cycle_nodes = (str(node) for node in raw_nodes)
            stable_key = compute_lineage_issue_stable_key(
                self.environment,
                self.source_profile,
                self.program_name,
                issue_type,
                node_key=self.node_key,
                branch_sink=self.branch_sink,
                cycle_nodes=cycle_nodes,
            )
        object.__setattr__(self, "stable_key", stable_key.strip())

    @property
    def stable_issue_identity(self) -> str:
        """仅由 fact 语义决定的 stable issue identity。"""

        if self.stable_key is None:
            raise RuntimeError("AuditFact stable_key was not initialized")
        return self.stable_key

    @property
    def issue_key(self) -> str:
        """兼容 ``LineageIssue.issue_key`` 的 fact identity 别名。"""

        return self.stable_issue_identity

    @property
    def fingerprint(self) -> str:
        """兼容 ``LineageIssue.fingerprint`` 的 fact identity 别名。"""

        return self.stable_issue_identity

    @classmethod
    def from_issue(cls, issue: LineageIssue) -> "AuditFact":
        """从旧/持久化的扁平 issue 恢复 fact 部分，丢弃 policy 字段。"""

        if not isinstance(issue, LineageIssue):
            raise TypeError("issue must be a LineageIssue")
        return cls(
            environment=issue.environment,
            source_profile=issue.source_profile,
            program_name=issue.program_name,
            issue_type=issue.issue_type,
            message=issue.message,
            node_key=issue.node_key,
            branch_sink=issue.branch_sink,
            evidence=issue.evidence,
            confidence=issue.confidence,
            rule_version=issue.rule_version,
            stable_key=issue.stable_key,
        )


@dataclass(frozen=True, slots=True)
class AuditPolicyResult:
    """一个 fact 经 policy 投影后的风险与处置结果。"""

    fact: AuditFact
    severity: str
    disposition: IssueDisposition | str
    policy_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.fact, AuditFact):
            raise TypeError("fact must be an AuditFact")
        if not isinstance(self.severity, str) or not self.severity.strip():
            raise ValueError("severity must be a non-empty string")
        object.__setattr__(self, "severity", self.severity.strip())
        object.__setattr__(self, "disposition", IssueDisposition(self.disposition))
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version must be a non-empty string")
        object.__setattr__(self, "policy_version", self.policy_version.strip())

    @property
    def stable_key(self) -> str:
        return self.fact.stable_issue_identity

    @property
    def issue_type(self) -> IssueType:
        return IssueType(self.fact.issue_type)

    @property
    def confidence(self) -> AuditConfidence:
        return AuditConfidence(self.fact.confidence)

    @property
    def rule_version(self) -> str:
        return self.fact.rule_version

    def to_issue(
        self,
        *,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
        first_seen_at: datetime | None = None,
        last_seen_at: datetime | None = None,
        is_active: bool = True,
        disposition_updated_at: datetime | None = None,
        disposition_updated_by: str | None = None,
    ) -> LineageIssue:
        """生成兼容现有 materialization/SQLite API 的扁平 projection。"""

        if observed_at is not None:
            first_seen_at = first_seen_at or observed_at
            last_seen_at = last_seen_at or observed_at
        return LineageIssue(
            environment=self.fact.environment,
            source_profile=self.fact.source_profile,
            program_name=self.fact.program_name,
            issue_type=self.fact.issue_type,
            severity=self.severity,
            message=self.fact.message,
            node_key=self.fact.node_key,
            branch_sink=self.fact.branch_sink,
            evidence=self.fact.evidence,
            batch_id=batch_id,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            is_active=is_active,
            stable_key=self.fact.stable_issue_identity,
            confidence=self.fact.confidence,
            rule_version=self.fact.rule_version,
            disposition=self.disposition,
            policy_version=self.policy_version,
            disposition_updated_at=disposition_updated_at,
            disposition_updated_by=disposition_updated_by,
        )


@dataclass(frozen=True, slots=True)
class AuditPolicy:
    """独立于 detector 的 severity/disposition policy。"""

    severity_by_issue_type: Mapping[IssueType | str, str] = field(default_factory=dict)
    default_disposition: IssueDisposition | str = IssueDisposition.OPEN
    disposition_by_issue_type: Mapping[IssueType | str, IssueDisposition | str] = field(
        default_factory=dict
    )
    policy_version: str = AUDIT_POLICY_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.severity_by_issue_type, Mapping):
            raise TypeError("severity_by_issue_type must be a mapping")
        severity: dict[IssueType, str] = dict(ISSUE_SEVERITY_POLICY)
        for raw_type, raw_severity in self.severity_by_issue_type.items():
            issue_type = IssueType(raw_type)
            if not isinstance(raw_severity, str) or not raw_severity.strip():
                raise ValueError("severity values must be non-empty strings")
            severity[issue_type] = raw_severity.strip()
        if not isinstance(self.disposition_by_issue_type, Mapping):
            raise TypeError("disposition_by_issue_type must be a mapping")
        default_disposition = IssueDisposition(self.default_disposition)
        dispositions: dict[IssueType, IssueDisposition] = {}
        for raw_type, raw_disposition in self.disposition_by_issue_type.items():
            dispositions[IssueType(raw_type)] = IssueDisposition(raw_disposition)
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version must be a non-empty string")
        object.__setattr__(self, "severity_by_issue_type", MappingProxyType(severity))
        object.__setattr__(self, "default_disposition", default_disposition)
        object.__setattr__(self, "disposition_by_issue_type", MappingProxyType(dispositions))
        object.__setattr__(self, "policy_version", self.policy_version.strip())

    @property
    def version(self) -> str:
        """policy_version 的短别名。"""

        return self.policy_version

    def severity_for(self, issue_type: IssueType | str) -> str:
        return self.severity_by_issue_type[IssueType(issue_type)]

    def evaluate(
        self,
        fact: AuditFact,
        *,
        disposition: IssueDisposition | str | None = None,
    ) -> AuditPolicyResult:
        if not isinstance(fact, AuditFact):
            raise TypeError("fact must be an AuditFact")
        resolved_disposition = (
            IssueDisposition(disposition)
            if disposition is not None
            else self.disposition_by_issue_type.get(
                IssueType(fact.issue_type), self.default_disposition
            )
        )
        return AuditPolicyResult(
            fact=fact,
            severity=self.severity_for(fact.issue_type),
            disposition=resolved_disposition,
            policy_version=self.policy_version,
        )

    def evaluate_all(self, facts: Iterable[AuditFact]) -> tuple[AuditPolicyResult, ...]:
        values = tuple(facts)
        if any(not isinstance(fact, AuditFact) for fact in values):
            raise TypeError("facts must contain AuditFact values")
        return tuple(self.evaluate(fact) for fact in sorted(values, key=_fact_sort_key))

    def project(
        self,
        facts: Iterable[AuditFact],
        *,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> tuple[LineageIssue, ...]:
        return tuple(
            result.to_issue(batch_id=batch_id, observed_at=observed_at)
            for result in self.evaluate_all(facts)
        )

    apply = project

    def replay(
        self,
        values: Iterable[AuditFact | LineageIssue],
        *,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
        preserve_manual_disposition: bool = True,
    ) -> tuple[LineageIssue, ...]:
        """在不重建 DAG 的情况下重投影 facts/issues，供 policy replay 使用。"""

        projected: list[LineageIssue] = []
        for value in values:
            source_issue = value if isinstance(value, LineageIssue) else None
            if source_issue is not None:
                fact = AuditFact.from_issue(source_issue)
            else:
                if not isinstance(value, AuditFact):
                    raise TypeError(
                        "values must contain AuditFact or LineageIssue values"
                    )
                fact = value
            override: IssueDisposition | str | None = None
            if source_issue is not None and preserve_manual_disposition:
                source_disposition = IssueDisposition(source_issue.disposition)
                if source_disposition in (
                    IssueDisposition.ACCEPTED,
                    IssueDisposition.FALSE_POSITIVE,
                    IssueDisposition.RESOLVED,
                ) or (
                    source_disposition is IssueDisposition.OPEN
                    and (
                        source_issue.disposition_updated_at is not None
                        or source_issue.disposition_updated_by is not None
                    )
                ):
                    override = source_disposition
            result = self.evaluate(fact, disposition=override)
            source_batch_id = source_issue.batch_id if source_issue is not None else None
            projected.append(
                result.to_issue(
                    batch_id=(
                        batch_id if batch_id is not None else source_batch_id
                    ),
                    observed_at=observed_at,
                    first_seen_at=(
                        source_issue.first_seen_at
                        if source_issue is not None
                        else None
                    ),
                    last_seen_at=(
                        observed_at
                        if observed_at is not None
                        else (
                            source_issue.last_seen_at
                            if source_issue is not None
                            else None
                        )
                    ),
                    is_active=(source_issue.is_active if source_issue is not None else True),
                    disposition_updated_at=(
                        source_issue.disposition_updated_at
                        if source_issue is not None and override is not None
                        else None
                    ),
                    disposition_updated_by=(
                        source_issue.disposition_updated_by
                        if source_issue is not None and override is not None
                        else None
                    ),
                )
            )
        return tuple(sorted(projected, key=_issue_sort_key))


DEFAULT_AUDIT_POLICY = AuditPolicy()


_EDGE_EVIDENCE_KEYS = (
    "column_number",
    "insert_mode",
    "line_number",
    "normalized_source",
    "normalized_target",
    "occurrences",
    "raw_source",
    "raw_target",
    "statement_index",
    "statement_indices",
    "statement_type",
)
_OCCURRENCE_EVIDENCE_KEYS = tuple(
    key for key in _EDGE_EVIDENCE_KEYS if key != "occurrences"
)


class TargetSelectionMode(str, Enum):
    """Materialization target 的 authority 来源。"""

    NONE = "NONE"
    AUTHORITATIVE = "AUTHORITATIVE"
    UNIQUE_HINT = "UNIQUE_HINT"


@dataclass(frozen=True, slots=True)
class TargetSelectionResult:
    """把 authoritative target 与 non-authoritative hint 分开的选择事实。"""

    authoritative_target: str | None
    target_hint: str | None
    selected_target: str | None
    selection_mode: TargetSelectionMode
    hint_match_count: int = 0

    def __post_init__(self) -> None:
        mode = TargetSelectionMode(self.selection_mode)
        object.__setattr__(self, "selection_mode", mode)
        for field_name in (
            "authoritative_target",
            "target_hint",
            "selected_target",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None")
        if (
            not isinstance(self.hint_match_count, int)
            or isinstance(self.hint_match_count, bool)
            or self.hint_match_count < 0
        ):
            raise ValueError("hint_match_count must be a non-negative integer")
        if mode is TargetSelectionMode.NONE and self.selected_target is not None:
            raise ValueError("NONE selection cannot have a selected target")
        if mode is TargetSelectionMode.AUTHORITATIVE:
            if self.authoritative_target is None:
                raise ValueError(
                    "AUTHORITATIVE selection needs an authoritative target"
                )
            if self.selected_target != self.authoritative_target:
                raise ValueError(
                    "AUTHORITATIVE selection must select the authoritative target"
                )
        if mode is TargetSelectionMode.UNIQUE_HINT:
            if self.authoritative_target is not None:
                raise ValueError(
                    "UNIQUE_HINT selection cannot have an authoritative target"
                )
            if self.selected_target is None or self.hint_match_count != 1:
                raise ValueError(
                    "UNIQUE_HINT selection needs exactly one matched target"
                )

    @property
    def selected_materialization_target(self) -> str | None:
        """兼容 contract 文档中的 selected materialization target 名称。"""

        return self.selected_target


def select_materialization_target(
    *,
    authoritative_target: str | None,
    target_hint: str | None,
    formal_sinks: Iterable[str],
) -> TargetSelectionResult:
    """仅按 authority 或 exact unique hint 选择 materialization target。"""

    candidates = tuple(formal_sinks)
    matches = tuple(
        sink for sink in candidates if target_hint is not None and sink == target_hint
    )
    if authoritative_target is not None:
        return TargetSelectionResult(
            authoritative_target=authoritative_target,
            target_hint=target_hint,
            selected_target=authoritative_target,
            selection_mode=TargetSelectionMode.AUTHORITATIVE,
            hint_match_count=len(matches),
        )
    if len(candidates) > 1 and len(matches) == 1:
        return TargetSelectionResult(
            authoritative_target=None,
            target_hint=target_hint,
            selected_target=matches[0],
            selection_mode=TargetSelectionMode.UNIQUE_HINT,
            hint_match_count=1,
        )
    return TargetSelectionResult(
        authoritative_target=None,
        target_hint=target_hint,
        selected_target=None,
        selection_mode=TargetSelectionMode.NONE,
        hint_match_count=len(matches),
    )


@dataclass(frozen=True, slots=True)
class LineageAuditResult:
    """一次 Physical DAG audit 的结构化结果与可复用事实摘要。

    ``facts`` 是 detector 的 canonical 输出；``issues`` 是为兼容现有
    materialization/SQLite API 保留的 policy projection。两者共享 stable key，
    但 severity、disposition 和 policy version 只存在于 projection。
    ``target_reachable_nodes`` 只表示 authoritative target 的既有事实；
    ``selected_target_reachable_nodes`` 仅为 unique hint selection 提供
    materialization branch，不把 hint 提升为 audit authority。
    """

    dag: ProgramPhysicalDAG
    issues: tuple[LineageIssue, ...]
    expected_target: str | None
    target_reachable_nodes: tuple[str, ...] = ()
    orphan_branch_sinks: tuple[str, ...] = ()
    target_selection: TargetSelectionResult = field(
        default_factory=lambda: TargetSelectionResult(
            authoritative_target=None,
            target_hint=None,
            selected_target=None,
            selection_mode=TargetSelectionMode.NONE,
        )
    )
    selected_target_reachable_nodes: tuple[str, ...] = ()
    facts: tuple[AuditFact, ...] = ()
    policy_version: str = AUDIT_POLICY_VERSION

    def __post_init__(self) -> None:
        facts = tuple(self.facts)
        if any(not isinstance(fact, AuditFact) for fact in facts):
            raise TypeError("facts must contain AuditFact values")
        object.__setattr__(self, "facts", facts)
        issues = tuple(self.issues)
        if any(not isinstance(issue, LineageIssue) for issue in issues):
            raise TypeError("issues must contain LineageIssue values")
        object.__setattr__(self, "issues", issues)
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version must be a non-empty string")
        object.__setattr__(self, "policy_version", self.policy_version.strip())

    @property
    def authoritative_target(self) -> str | None:
        """返回 audit 的 authoritative expected target；不包含 hint。"""

        return self.expected_target

    @property
    def target_hint(self) -> str | None:
        return self.target_selection.target_hint

    @property
    def selected_materialization_target(self) -> str | None:
        return self.target_selection.selected_target

    @property
    def selection_mode(self) -> TargetSelectionMode:
        return self.target_selection.selection_mode

    @property
    def hint_match_count(self) -> int:
        return self.target_selection.hint_match_count

    def apply_policy(
        self,
        policy: AuditPolicy | None = None,
        *,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> tuple[LineageIssue, ...]:
        """对同一份 facts 重新计算 policy，不重新访问 DAG 或 parser。"""

        resolved_policy = policy or DEFAULT_AUDIT_POLICY
        if not isinstance(resolved_policy, AuditPolicy):
            raise TypeError("policy must be an AuditPolicy or None")
        if self.issues:
            return resolved_policy.replay(
                self.issues,
                batch_id=batch_id,
                observed_at=observed_at,
            )
        return resolved_policy.project(
            self.facts,
            batch_id=batch_id,
            observed_at=observed_at,
        )

    replay_policy = apply_policy

    @property
    def issue_types(self) -> tuple[IssueType, ...]:
        """按结果顺序返回 issue 类型，便于调用方做轻量统计。"""

        if self.facts:
            return tuple(IssueType(fact.issue_type) for fact in self.facts)
        return tuple(IssueType(issue.issue_type) for issue in self.issues)

    @property
    def has_issues(self) -> bool:
        return bool(self.facts or self.issues)


# 便于只关心结果对象的调用方使用短名称；正式文档使用 LineageAuditResult。
AuditResult = LineageAuditResult


def issue_severity(issue_type: IssueType | str) -> str:
    """兼容入口：返回默认 policy 的 severity，不参与 fact detection。"""

    return DEFAULT_AUDIT_POLICY.severity_for(issue_type)


def audit_fact_from_issue(issue: LineageIssue) -> AuditFact:
    """把兼容 projection 恢复为 policy 无关的 AuditFact。"""

    return AuditFact.from_issue(issue)


def apply_audit_policy(
    facts: Iterable[AuditFact],
    policy: AuditPolicy | None = None,
    *,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
) -> tuple[LineageIssue, ...]:
    """对既有 facts 应用 policy；不会重新构建 Physical DAG。"""

    resolved_policy = policy or DEFAULT_AUDIT_POLICY
    if not isinstance(resolved_policy, AuditPolicy):
        raise TypeError("policy must be an AuditPolicy or None")
    return resolved_policy.project(
        facts,
        batch_id=batch_id,
        observed_at=observed_at,
    )


def replay_audit_policy(
    values: Iterable[AuditFact | LineageIssue],
    policy: AuditPolicy | None = None,
    *,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    preserve_manual_disposition: bool = True,
) -> tuple[LineageIssue, ...]:
    """从 facts 或旧 projection 重放 policy，并保留人工处置语义。"""

    resolved_policy = policy or DEFAULT_AUDIT_POLICY
    if not isinstance(resolved_policy, AuditPolicy):
        raise TypeError("policy must be an AuditPolicy or None")
    return resolved_policy.replay(
        values,
        batch_id=batch_id,
        observed_at=observed_at,
        preserve_manual_disposition=preserve_manual_disposition,
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _canonicalize(value: object) -> object:
    """把 evidence 转成可稳定排序的 JSON-safe 值。"""

    if isinstance(value, Mapping):
        return {str(key): _canonicalize(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonicalize(item) for item in value]
        return sorted(items, key=_canonical_json)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _value_sort_key(value: object) -> tuple[int, object]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (0, value)
    return (1, str(value))


def _statement_indices(evidence: Mapping[str, object]) -> list[object]:
    raw_indices = evidence.get("statement_indices")
    if isinstance(raw_indices, (list, tuple, set, frozenset)):
        values = list(raw_indices)
    elif raw_indices is None:
        values = []
    else:
        values = [raw_indices]

    if not values and "statement_index" in evidence:
        values = [evidence["statement_index"]]

    valid_values = [
        value
        for value in values
        if isinstance(value, (int, str)) and not isinstance(value, bool)
    ]
    unique: dict[str, object] = {}
    for value in valid_values:
        unique[_canonical_json(value)] = value
    return sorted(unique.values(), key=_value_sort_key)


def _selected_statement_evidence(value: object) -> dict[str, object]:
    """只复制 Phase 3 的轻量 statement evidence，不带完整源码。"""

    if not isinstance(value, Mapping):
        return {}

    selected: dict[str, object] = {}
    for key in _EDGE_EVIDENCE_KEYS:
        if key not in value:
            continue
        raw_value = value[key]
        if key != "occurrences":
            selected[key] = _canonicalize(raw_value)
            continue
        if not isinstance(raw_value, (list, tuple)):
            continue
        occurrences: list[dict[str, object]] = []
        for occurrence in raw_value:
            if not isinstance(occurrence, Mapping):
                continue
            item = {
                str(item_key): _canonicalize(occurrence[item_key])
                for item_key in _OCCURRENCE_EVIDENCE_KEYS
                if item_key in occurrence
            }
            if item:
                occurrences.append(item)
        selected[key] = sorted(occurrences, key=_canonical_json)

    return {key: selected[key] for key in sorted(selected)}


def _edge_sort_key(edge: PhysicalEdge) -> tuple[str, str, str, str]:
    evidence = _selected_statement_evidence(edge.evidence)
    return (
        edge.source,
        edge.target,
        edge.evidence_type,
        _canonical_json(evidence),
    )


def _edge_record(edge: PhysicalEdge) -> dict[str, object]:
    """将一条 PhysicalEdge 转为稳定、可解释且不含 script_code 的 evidence。"""

    record: dict[str, object] = {
        "source": edge.source,
        "target": edge.target,
        "evidence_type": edge.evidence_type,
    }
    if isinstance(edge.evidence, Mapping):
        details = _selected_statement_evidence(edge.evidence)
        if details:
            record["evidence"] = details
            for key in (
                "column_number",
                "insert_mode",
                "line_number",
                "normalized_source",
                "normalized_target",
                "raw_source",
                "raw_target",
                "statement_index",
                "statement_type",
            ):
                if key in details:
                    record[key] = details[key]
        record["statement_indices"] = _statement_indices(edge.evidence)
    else:
        record["statement_indices"] = []
    return record


def _unique_sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


def _build_adjacency(
    nodes: Iterable[str], edges: Iterable[PhysicalEdge]
) -> tuple[dict[str, tuple[str, ...]], dict[str, tuple[PhysicalEdge, ...]]]:
    forward_sets: dict[str, set[str]] = {node: set() for node in nodes}
    reverse_lists: dict[str, list[PhysicalEdge]] = {node: [] for node in nodes}
    for edge in edges:
        forward_sets.setdefault(edge.source, set()).add(edge.target)
        reverse_lists.setdefault(edge.target, []).append(edge)

    forward = {node: tuple(sorted(targets)) for node, targets in forward_sets.items()}
    reverse = {
        node: tuple(sorted(items, key=_edge_sort_key))
        for node, items in reverse_lists.items()
    }
    return forward, reverse


def _reverse_reachable(
    start: str,
    reverse: Mapping[str, tuple[PhysicalEdge, ...]],
) -> set[str]:
    """沿 reverse edges 找到所有能到达 ``start`` 的节点。"""

    reachable = {start}
    pending = [start]
    while pending:
        current = pending.pop()
        for edge in reverse.get(current, ()):
            if edge.source in reachable:
                continue
            reachable.add(edge.source)
            pending.append(edge.source)
    return reachable


def _strongly_connected_components(
    nodes: Iterable[str],
    forward: Mapping[str, tuple[str, ...]],
    reverse: Mapping[str, tuple[PhysicalEdge, ...]],
) -> tuple[tuple[str, ...], ...]:
    """用确定性、迭代式 Kosaraju 遍历返回 SCC，避免 cycle 无限遍历。"""

    ordered_nodes = tuple(sorted(set(nodes)))
    visited: set[str] = set()
    finish_order: list[str] = []

    for start in ordered_nodes:
        if start in visited:
            continue
        pending: list[tuple[str, bool]] = [(start, False)]
        while pending:
            current, expanded = pending.pop()
            if expanded:
                finish_order.append(current)
                continue
            if current in visited:
                continue
            visited.add(current)
            pending.append((current, True))
            for target in reversed(forward.get(current, ())):
                if target not in visited:
                    pending.append((target, False))

    reverse_names = {
        node: tuple(sorted({edge.source for edge in edges}))
        for node, edges in reverse.items()
    }
    assigned: set[str] = set()
    components: list[tuple[str, ...]] = []
    for start in reversed(finish_order):
        if start in assigned:
            continue
        component: set[str] = set()
        component_pending: list[str] = [start]
        while component_pending:
            current = component_pending.pop()
            if current in assigned:
                continue
            assigned.add(current)
            component.add(current)
            for source in reverse_names.get(current, ()):
                if source not in assigned:
                    component_pending.append(source)
        components.append(tuple(sorted(component)))
    return tuple(components)


def _branch_for_sink(
    sink: str,
    reverse: Mapping[str, tuple[PhysicalEdge, ...]],
) -> tuple[tuple[str, ...], tuple[PhysicalEdge, ...]]:
    """按 terminal sink 收集整条反向 branch，并对 traversal 做 visited protection。"""

    branch_nodes = {sink}
    branch_edges: list[PhysicalEdge] = []
    pending = [sink]
    while pending:
        current = pending.pop()
        for edge in reverse.get(current, ()):
            branch_edges.append(edge)
            if edge.source in branch_nodes:
                continue
            branch_nodes.add(edge.source)
            pending.append(edge.source)
    return (
        tuple(sorted(branch_nodes)),
        tuple(sorted(branch_edges, key=_edge_sort_key)),
    )


def _branch_entry_sources(
    branch_nodes: Iterable[str], branch_edges: Iterable[PhysicalEdge]
) -> tuple[str, ...]:
    incoming_targets = {edge.target for edge in branch_edges}
    return tuple(sorted(set(branch_nodes) - incoming_targets))


def _node_kind_value(
    node_key: str,
    node_map: Mapping[str, PhysicalNode],
) -> str:
    node = node_map.get(node_key)
    if node is not None and node.kind is not None:
        return node.kind.value
    # No explicit temporary evidence: use the neutral default instead of
    # classifying by table name.
    return PhysicalNodeKind.FORMAL_ASSET.value


def _is_business_sink(
    sink: str,
    node_map: Mapping[str, PhysicalNode],
    *,
    environment: str,
) -> bool:
    node = node_map.get(sink)
    asset_name = node.asset_name if node is not None else sink
    return is_business_asset(asset_name, environment=environment)


def _is_technical_sink(
    sink: str,
    node_map: Mapping[str, PhysicalNode],
    *,
    environment: str,
) -> bool:
    node = node_map.get(sink)
    if node is not None and node.kind is PhysicalNodeKind.TEMPORARY_ASSET:
        return False
    asset_name = node.asset_name if node is not None else sink
    return is_technical_asset(asset_name, environment=environment)


def compute_lineage_issue_stable_key(
    environment: str,
    source_profile: str,
    program_name: str,
    issue_type: IssueType | str,
    *,
    node_key: str | None = None,
    branch_sink: str | None = None,
    cycle_nodes: Iterable[str] = (),
) -> str:
    """按 issue 语义计算跨进程稳定的 SHA-256 identity。

    Program-level issue 不把 sink 列表、evidence 或 message 放入 identity；
    branch、node 和 cycle issue 则分别使用 branch sink、node key 和 canonical
    sorted SCC node set 作为区分因子。
    """

    resolved_type = IssueType(issue_type)
    identity: dict[str, object] = {
        "environment": environment,
        "issue_type": resolved_type.value,
        "program_name": program_name,
        "scope": "program",
        "source_profile": source_profile,
    }
    if resolved_type in (
        IssueType.ORPHAN_BRANCH,
        IssueType.LINEAGE_BRANCH_BROKEN,
    ):
        identity["scope"] = "branch"
        identity["branch_sink"] = branch_sink
    elif resolved_type is IssueType.SELF_REFERENCE:
        identity["scope"] = "node"
        identity["node_key"] = node_key
    elif resolved_type is IssueType.CYCLE_DETECTED:
        identity["scope"] = "cycle"
        identity["cycle_nodes"] = sorted(set(cycle_nodes))
    return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def _make_fact(
    dag: ProgramPhysicalDAG,
    issue_type: IssueType,
    *,
    message: str,
    evidence: Mapping[str, object],
    node_key: str | None = None,
    branch_sink: str | None = None,
    cycle_nodes: Iterable[str] = (),
    confidence: AuditConfidence = AuditConfidence.HIGH,
    rule_version: str = AUDIT_RULE_VERSION,
) -> AuditFact:
    source = dag.program_source
    stable_key = compute_lineage_issue_stable_key(
        source.environment,
        source.source_profile,
        source.program_name,
        issue_type,
        node_key=node_key,
        branch_sink=branch_sink,
        cycle_nodes=cycle_nodes,
    )
    return AuditFact(
        environment=source.environment,
        source_profile=source.source_profile,
        program_name=source.program_name,
        issue_type=issue_type,
        message=message,
        node_key=node_key,
        branch_sink=branch_sink,
        evidence=dict(evidence),
        confidence=confidence,
        rule_version=rule_version,
        stable_key=stable_key,
    )


def _fact_sort_key(
    fact: AuditFact | LineageIssue,
) -> tuple[str, str, str, str, str]:
    cycle_sort_key = ""
    if IssueType(fact.issue_type) is IssueType.CYCLE_DETECTED:
        evidence = fact.evidence
        if isinstance(evidence, Mapping):
            cycle_nodes = evidence.get("cycle_nodes", ())
            if isinstance(cycle_nodes, (list, tuple)):
                cycle_sort_key = "\u0000".join(str(node) for node in cycle_nodes)
    return (
        IssueType(fact.issue_type).value,
        fact.branch_sink or "",
        fact.node_key or "",
        cycle_sort_key,
        fact.stable_key or "",
    )


def _issue_sort_key(issue: LineageIssue) -> tuple[str, str, str, str, str]:
    return _fact_sort_key(issue)


class ProgramLineageAuditor:
    """只运行 Physical DAG fact detector，不持有 severity/disposition policy。"""

    def _detect(
        self,
        dag: ProgramPhysicalDAG,
        observed_at: datetime | None = None,
        batch_id: str | None = None,
    ) -> LineageAuditResult:
        if not isinstance(dag, ProgramPhysicalDAG):
            raise TypeError("dag must be a ProgramPhysicalDAG")
        if observed_at is None:
            observed_at = datetime.now(timezone.utc)
        elif not isinstance(observed_at, datetime):
            raise TypeError("observed_at must be a datetime or None")

        edges = tuple(sorted(dag.edges, key=_edge_sort_key))
        node_map = {node.node_key: node for node in dag.nodes}
        graph_nodes = set(node_map)
        graph_nodes.update(edge.source for edge in edges)
        graph_nodes.update(edge.target for edge in edges)
        forward, reverse = _build_adjacency(graph_nodes, edges)

        raw_sinks = tuple(dag.sinks)
        sinks = _unique_sorted(raw_sinks)
        written_targets = _unique_sorted(
            [
                *(step.target for step in dag.steps if step.target is not None),
                *(edge.target for edge in edges),
                *sinks,
            ]
        )
        formal_sink_candidates = tuple(
            sink
            for sink in raw_sinks
            if _node_kind_value(sink, node_map) == PhysicalNodeKind.FORMAL_ASSET.value
        )
        formal_sinks = _unique_sorted(formal_sink_candidates)
        business_sink_candidates = tuple(
            sink
            for sink in formal_sink_candidates
            if _is_business_sink(
                sink,
                node_map,
                environment=dag.program_source.environment,
            )
        )
        business_sinks = _unique_sorted(business_sink_candidates)
        temporary_sinks = tuple(
            sink
            for sink in sinks
            if _node_kind_value(sink, node_map)
            == PhysicalNodeKind.TEMPORARY_ASSET.value
        )
        sink_kinds = {sink: _node_kind_value(sink, node_map) for sink in sinks}
        target_selection = select_materialization_target(
            authoritative_target=dag.expected_target,
            target_hint=dag.program_source.target_hint,
            formal_sinks=business_sink_candidates,
        )

        facts: list[AuditFact] = []

        self_edges: dict[str, PhysicalEdge] = {}
        for edge in edges:
            if edge.source == edge.target:
                self_edges.setdefault(edge.source, edge)
        for node_key in sorted(self_edges):
            edge = self_edges[node_key]
            edge_record = _edge_record(edge)
            evidence: dict[str, object] = {
                "node": node_key,
                "source": edge.source,
                "target": edge.target,
                "edge": edge_record,
                "statement_indices": edge_record["statement_indices"],
            }
            if "evidence" in edge_record:
                evidence["statement_evidence"] = edge_record["evidence"]
            facts.append(
                _make_fact(
                    dag,
                    IssueType.SELF_REFERENCE,
                    node_key=node_key,
                    message=(
                        f"Program {dag.program_source.program_name} contains a "
                        f"self-reference edge {node_key} -> {node_key}."
                    ),
                    evidence=evidence,
                )
            )

        components = _strongly_connected_components(graph_nodes, forward, reverse)
        for cycle_nodes in sorted(
            component for component in components if len(component) > 1
        ):
            cycle_node_set = set(cycle_nodes)
            cycle_edges = tuple(
                edge
                for edge in edges
                if edge.source in cycle_node_set and edge.target in cycle_node_set
            )
            cycle_records = [_edge_record(edge) for edge in cycle_edges]
            cycle_pairs = [[edge.source, edge.target] for edge in cycle_edges]
            facts.append(
                _make_fact(
                    dag,
                    IssueType.CYCLE_DETECTED,
                    message=(
                        f"Program {dag.program_source.program_name} contains a "
                        f"cycle involving {', '.join(cycle_nodes)}."
                    ),
                    evidence={
                        "cycle_nodes": list(cycle_nodes),
                        "cycle_edges": cycle_records,
                        "cycle_edge_pairs": cycle_pairs,
                    },
                    cycle_nodes=cycle_nodes,
                )
            )

        technical_sink_present = any(
            _is_technical_sink(
                sink,
                node_map,
                environment=dag.program_source.environment,
            )
            for sink in sinks
        )
        all_sinks_are_technical = bool(sinks) and all(
            _is_technical_sink(
                sink,
                node_map,
                environment=dag.program_source.environment,
            )
            for sink in sinks
        )
        if len(sinks) > 1 and not all_sinks_are_technical:
            expected_text = dag.expected_target or "unknown"
            if technical_sink_present:
                multi_sink_message = (
                    f"Program {dag.program_source.program_name} has "
                    f"{len(sinks)} candidate sinks ({', '.join(sinks)}); "
                    f"Business candidates are {', '.join(business_sinks) or 'none'}; "
                    f"expected target is {expected_text}."
                )
            else:
                multi_sink_message = (
                    f"Program {dag.program_source.program_name} has "
                    f"{len(sinks)} candidate sinks ({', '.join(sinks)}); "
                    f"expected target is {expected_text}."
                )
            multi_sink_evidence: dict[str, object] = {
                "sink_count": len(sinks),
                "sinks": list(sinks),
                "sorted_sinks": list(sinks),
                "formal_sinks": list(formal_sinks),
                "temporary_sinks": list(temporary_sinks),
                "sink_kinds": sink_kinds,
                "expected_target": dag.expected_target,
                "authoritative_target": target_selection.authoritative_target,
                "target_hint": target_selection.target_hint,
                "hint_match_count": target_selection.hint_match_count,
                "selected_materialization_target": (
                    target_selection.selected_materialization_target
                ),
                "selection_mode": target_selection.selection_mode.value,
            }
            if technical_sink_present:
                multi_sink_evidence.update(
                    {
                        "business_sinks": list(business_sinks),
                        "all_sinks": list(sinks),
                    }
                )
            facts.append(
                _make_fact(
                    dag,
                    IssueType.MULTI_SINK_CANDIDATE,
                    message=multi_sink_message,
                    evidence=multi_sink_evidence,
                )
            )

        expected_target = dag.expected_target
        expected_target_written = (
            expected_target is not None and expected_target in written_targets
        )
        expected_target_is_sink = (
            expected_target is not None and expected_target in sinks
        )
        actual_formal_sinks = list(formal_sinks)
        actual_business_sinks = list(business_sinks)
        # ``sinks`` is a graph-terminal fact.  A written target that is not
        # terminal must be explained by its graph facts, not relabeled as a
        # target mismatch.  In particular, a self-loop must not imply that the
        # expected target was written to the wrong place.
        if (
            expected_target is not None
            and not expected_target_is_sink
            and not expected_target_written
        ):
            if actual_business_sinks:
                facts.append(
                    _make_fact(
                        dag,
                        IssueType.TARGET_MISMATCH,
                        message=(
                            f"Program {dag.program_source.program_name} writes "
                            f"Business sink(s) {', '.join(actual_business_sinks)} "
                            f"instead of expected target {expected_target}."
                        ),
                        evidence={
                            "expected_target": expected_target,
                            "actual_formal_sinks": actual_formal_sinks,
                            "actual_business_sinks": actual_business_sinks,
                            "all_sinks": list(sinks),
                            "written_targets": list(written_targets),
                            "expected_target_written": False,
                            "expected_target_is_sink": False,
                        },
                    )
                )
            else:
                facts.append(
                    _make_fact(
                        dag,
                        IssueType.TARGET_NOT_FOUND,
                        message=(
                            f"Program {dag.program_source.program_name} did not "
                            f"write expected target {expected_target}; no Business "
                            "result sink was found."
                        ),
                        evidence={
                            "expected_target": expected_target,
                            "written_targets": list(written_targets),
                            "sinks": list(sinks),
                            "formal_sinks": actual_formal_sinks,
                            "business_sinks": actual_business_sinks,
                            "temporary_sinks": list(temporary_sinks),
                        },
                    )
                )

        target_reachable_nodes: tuple[str, ...] = ()
        selected_target_reachable_nodes: tuple[str, ...] = ()
        orphan_branch_sinks: tuple[str, ...] = ()
        selected_target = target_selection.selected_target
        if selected_target is not None and selected_target in written_targets:
            target_reachable = _reverse_reachable(selected_target, reverse)
            if expected_target is not None:
                target_reachable_nodes = tuple(sorted(target_reachable))
            else:
                selected_target_reachable_nodes = tuple(sorted(target_reachable))
            if expected_target is not None:
                orphan_branch_sinks = tuple(
                    sink for sink in sinks if sink not in target_reachable
                )
                for branch_sink in orphan_branch_sinks:
                    branch_nodes, branch_edges = _branch_for_sink(branch_sink, reverse)
                    branch_records = [_edge_record(edge) for edge in branch_edges]
                    branch_pairs = [[edge.source, edge.target] for edge in branch_edges]
                    entry_sources = _branch_entry_sources(branch_nodes, branch_edges)
                    branch_node_kinds = {
                        node: _node_kind_value(node, node_map) for node in branch_nodes
                    }
                    facts.append(
                        _make_fact(
                            dag,
                            IssueType.ORPHAN_BRANCH,
                            branch_sink=branch_sink,
                            message=(
                                f"Program {dag.program_source.program_name} has an "
                                f"orphan branch ending at {branch_sink} that cannot "
                                f"reach expected target {expected_target}."
                            ),
                            evidence={
                                "expected_target": expected_target,
                                "branch_sink": branch_sink,
                                "branch_nodes": list(branch_nodes),
                                "branch_edges": branch_records,
                                "branch_edge_pairs": branch_pairs,
                                "entry_sources": list(entry_sources),
                                "branch_roots": list(entry_sources),
                                "branch_node_kinds": branch_node_kinds,
                            },
                        )
                    )

        facts.sort(key=_fact_sort_key)
        return LineageAuditResult(
            dag=dag,
            issues=(),
            expected_target=expected_target,
            target_reachable_nodes=target_reachable_nodes,
            orphan_branch_sinks=orphan_branch_sinks,
            target_selection=target_selection,
            selected_target_reachable_nodes=selected_target_reachable_nodes,
            facts=tuple(facts),
        )

    def detect_facts(
        self,
        dag: ProgramPhysicalDAG,
        observed_at: datetime | None = None,
        batch_id: str | None = None,
    ) -> tuple[AuditFact, ...]:
        """只运行 detector，返回不含 severity/disposition 的 facts。"""

        return self._detect(
            dag,
            observed_at=observed_at,
            batch_id=batch_id,
        ).facts

    detect = detect_facts

    def audit(
        self,
        dag: ProgramPhysicalDAG,
        observed_at: datetime | None = None,
        batch_id: str | None = None,
        *,
        policy: AuditPolicy | None = None,
    ) -> LineageAuditResult:
        if policy is not None and not isinstance(policy, AuditPolicy):
            raise TypeError("policy must be an AuditPolicy or None")
        if observed_at is None:
            resolved_observed_at = datetime.now(timezone.utc)
        elif not isinstance(observed_at, datetime):
            raise TypeError("observed_at must be a datetime or None")
        else:
            resolved_observed_at = observed_at
        detected = self._detect(dag, observed_at=resolved_observed_at)
        resolved_policy = policy or DEFAULT_AUDIT_POLICY
        issues = resolved_policy.project(
            detected.facts,
            batch_id=batch_id,
            observed_at=resolved_observed_at,
        )
        return replace(
            detected,
            issues=issues,
            policy_version=resolved_policy.policy_version,
        )

    def __call__(
        self,
        dag: ProgramPhysicalDAG,
        observed_at: datetime | None = None,
        batch_id: str | None = None,
        *,
        policy: AuditPolicy | None = None,
    ) -> LineageAuditResult:
        return self.audit(
            dag,
            observed_at=observed_at,
            batch_id=batch_id,
            policy=policy,
        )


def detect_audit_facts(
    dag: ProgramPhysicalDAG,
    observed_at: datetime | None = None,
) -> tuple[AuditFact, ...]:
    """只运行 Audit fact detector，不读取或应用 severity/disposition policy。"""

    return ProgramLineageAuditor().detect_facts(dag, observed_at=observed_at)


def audit_program_physical_dag(
    dag: ProgramPhysicalDAG,
    observed_at: datetime | None = None,
    batch_id: str | None = None,
    *,
    policy: AuditPolicy | None = None,
) -> LineageAuditResult:
    """审计一个程序 Physical DAG；未传时间时在入口统一生成一次。"""

    return ProgramLineageAuditor().audit(
        dag,
        observed_at=observed_at,
        batch_id=batch_id,
        policy=policy,
    )


__all__ = [
    "AUDIT_POLICY_VERSION",
    "AUDIT_RULE_VERSION",
    "AuditFact",
    "AuditPolicy",
    "AuditPolicyResult",
    "AuditResult",
    "DEFAULT_AUDIT_POLICY",
    "ISSUE_SEVERITY_POLICY",
    "LineageAuditResult",
    "ProgramLineageAuditor",
    "TargetSelectionMode",
    "TargetSelectionResult",
    "apply_audit_policy",
    "audit_fact_from_issue",
    "audit_program_physical_dag",
    "compute_lineage_issue_stable_key",
    "detect_audit_facts",
    "issue_severity",
    "replay_audit_policy",
    "select_materialization_target",
]
