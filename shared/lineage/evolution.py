"""Phase 7 incremental planning, history lifecycle, and graph diff primitives.

The functions in this module are deliberately independent from SQLite and from
parsing.  They consume already materialized domain values so an executor can
reuse the Phase 3--5 builder/audit/materialization pipeline without doing work
for an unchanged program.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Any

from .audit import compute_lineage_issue_stable_key, issue_severity
from .domain import (
    IssueType,
    LineageEdge,
    LineageIssue,
    ProgramIdentity,
    ProgramSource,
    ProgramState,
)
from .lineage_builder import normalize_table_name


class IncrementalStatus(str, Enum):
    """状态 of a program in one provider snapshot."""

    NEW = "NEW"
    UNCHANGED = "UNCHANGED"
    CHANGED = "CHANGED"
    DELETED = "DELETED"


@dataclass(frozen=True, slots=True)
class SnapshotScope:
    """Provider 本轮完整扫描的 ``environment/source_profile`` 边界。"""

    environment: str
    source_profile: str

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())

    @property
    def key(self) -> tuple[str, str]:
        return (self.environment, self.source_profile)

    @classmethod
    def from_value(cls, value: Any) -> SnapshotScope:
        if isinstance(value, cls):
            return value
        if isinstance(value, ProgramIdentity):
            return cls(value.environment, value.source_profile)
        if isinstance(value, ProgramSource):
            return cls(value.environment, value.source_profile)
        if isinstance(value, (tuple, list)) and len(value) == 2:
            return cls(str(value[0]), str(value[1]))
        raise TypeError(
            "snapshot scope must be SnapshotScope, ProgramIdentity, "
            "ProgramSource, or a two-item tuple"
        )


@dataclass(frozen=True, slots=True)
class IncrementalPlan:
    """纯逻辑 planner 的确定性输出。"""

    new: tuple[ProgramSource, ...] = ()
    unchanged: tuple[ProgramSource, ...] = ()
    changed: tuple[ProgramSource, ...] = ()
    deleted: tuple[ProgramState, ...] = ()
    complete_snapshot: bool = False
    snapshot_scopes: tuple[SnapshotScope, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("new", "unchanged", "changed"):
            values = tuple(getattr(self, field_name))
            if any(not isinstance(value, ProgramSource) for value in values):
                raise TypeError(f"{field_name} must contain ProgramSource values")
            object.__setattr__(
                self,
                field_name,
                tuple(sorted(values, key=lambda item: item.identity.key)),
            )
        deleted = tuple(self.deleted)
        if any(not isinstance(value, ProgramState) for value in deleted):
            raise TypeError("deleted must contain ProgramState values")
        object.__setattr__(
            self,
            "deleted",
            tuple(sorted(deleted, key=lambda item: item.identity.key)),
        )
        scopes = tuple(self.snapshot_scopes)
        if any(not isinstance(scope, SnapshotScope) for scope in scopes):
            raise TypeError("snapshot_scopes must contain SnapshotScope values")
        object.__setattr__(
            self,
            "snapshot_scopes",
            tuple(sorted(scopes, key=lambda item: item.key)),
        )
        if not isinstance(self.complete_snapshot, bool):
            raise TypeError("complete_snapshot must be a boolean")

    @property
    def rebuild(self) -> tuple[ProgramSource, ...]:
        """需要进入 parser/DAG/audit/materialization 的程序。"""

        return tuple(
            sorted((*self.new, *self.changed), key=lambda item: item.identity.key)
        )

    @property
    def current(self) -> tuple[ProgramSource, ...]:
        return tuple(
            sorted(
                (*self.new, *self.unchanged, *self.changed),
                key=lambda item: item.identity.key,
            )
        )

    def status_for(self, identity: ProgramIdentity) -> IncrementalStatus | None:
        if not isinstance(identity, ProgramIdentity):
            raise TypeError("identity must be a ProgramIdentity")
        for source in self.new:
            if source.identity == identity:
                return IncrementalStatus.NEW
        for source in self.unchanged:
            if source.identity == identity:
                return IncrementalStatus.UNCHANGED
        for source in self.changed:
            if source.identity == identity:
                return IncrementalStatus.CHANGED
        for state in self.deleted:
            if state.identity == identity:
                return IncrementalStatus.DELETED
        return None


def _source_hash_is_available(value: str | None) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _normalize_scope_values(
    scopes: Iterable[SnapshotScope | ProgramIdentity | ProgramSource | tuple[str, str]]
    | None,
) -> tuple[SnapshotScope, ...]:
    if scopes is None:
        return ()
    values = {SnapshotScope.from_value(scope) for scope in scopes}
    return tuple(sorted(values, key=lambda item: item.key))


def _validate_unique_sources(sources: tuple[ProgramSource, ...]) -> None:
    identities: set[tuple[str, str, str]] = set()
    for source in sources:
        key = source.identity.key
        if key in identities:
            raise ValueError(
                "provider snapshot contains duplicate program identity: "
                f"{key[0]}/{key[1]}/{key[2]}"
            )
        identities.add(key)


def _active_state_map(
    states: tuple[ProgramState, ...],
) -> dict[ProgramIdentity, ProgramState]:
    result: dict[ProgramIdentity, ProgramState] = {}
    for state in states:
        if not isinstance(state, ProgramState):
            raise TypeError("previous_states must contain ProgramState values")
        if not state.is_active:
            continue
        identity = state.identity
        if identity in result:
            raise ValueError(
                "previous states contain duplicate active program identity: "
                f"{identity.environment}/{identity.source_profile}/{identity.program_name}"
            )
        result[identity] = state
    return result


def plan_incremental(
    current_sources: Iterable[ProgramSource],
    previous_states: Iterable[ProgramState] = (),
    *,
    complete_snapshot: bool = False,
    snapshot_scopes: Iterable[
        SnapshotScope | ProgramIdentity | ProgramSource | tuple[str, str]
    ]
    | None = None,
) -> IncrementalPlan:
    """根据当前 ProgramSource metadata 生成纯逻辑增量计划。

    ``source_hash`` 是唯一的 unchanged 依据。当前 hash 缺失或为空时永远
    不会返回 ``UNCHANGED``，即使旧 state 也没有 hash；这是保守的 rebuild
    策略。``DELETED`` 只在 ``complete_snapshot=True`` 且 identity 属于明确
    snapshot scope 时计算。
    """

    sources = tuple(current_sources)
    if any(not isinstance(source, ProgramSource) for source in sources):
        raise TypeError("current_sources must contain ProgramSource values")
    sources = tuple(sorted(sources, key=lambda item: item.identity.key))
    _validate_unique_sources(sources)

    states = tuple(previous_states)
    previous = _active_state_map(states)
    explicit_scopes = _normalize_scope_values(snapshot_scopes)
    inferred_scopes = {
        SnapshotScope(source.environment, source.source_profile) for source in sources
    }
    if complete_snapshot:
        if explicit_scopes:
            scopes = explicit_scopes
            outside = {
                source.identity.scope
                for source in sources
                if source.identity.scope not in {scope.key for scope in scopes}
            }
            if outside:
                raise ValueError(
                    "current source is outside the declared complete snapshot scope: "
                    f"{sorted(outside)!r}"
                )
        elif inferred_scopes:
            scopes = tuple(sorted(inferred_scopes, key=lambda item: item.key))
        else:
            raise ValueError(
                "complete snapshot with no programs requires snapshot_scopes"
            )
    else:
        scopes = explicit_scopes or tuple(
            sorted(inferred_scopes, key=lambda item: item.key)
        )

    current_map = {source.identity: source for source in sources}
    new: list[ProgramSource] = []
    unchanged: list[ProgramSource] = []
    changed: list[ProgramSource] = []
    for source in sources:
        previous_state = previous.get(source.identity)
        if previous_state is None:
            new.append(source)
            continue
        if (
            _source_hash_is_available(source.source_hash)
            and source.source_hash == previous_state.source_hash
        ):
            unchanged.append(source)
        else:
            changed.append(source)

    scope_keys = {scope.key for scope in scopes}
    deleted = [
        state
        for identity, state in previous.items()
        if complete_snapshot
        and identity.scope in scope_keys
        and identity not in current_map
    ]
    return IncrementalPlan(
        new=tuple(new),
        unchanged=tuple(unchanged),
        changed=tuple(changed),
        deleted=tuple(deleted),
        complete_snapshot=complete_snapshot,
        snapshot_scopes=scopes,
    )


def build_program_states(
    plan: IncrementalPlan,
    previous_states: Iterable[ProgramState],
    *,
    observed_at: datetime,
    batch_id: str | None = None,
) -> tuple[ProgramState, ...]:
    """把 planner 结果转换成 candidate batch 的完整 active program state。"""

    if not isinstance(plan, IncrementalPlan):
        raise TypeError("plan must be an IncrementalPlan")
    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime")
    previous = _active_state_map(tuple(previous_states))
    current = {source.identity: source for source in plan.current}
    scopes = {scope.key for scope in plan.snapshot_scopes}
    result: dict[ProgramIdentity, ProgramState] = {}

    for identity, state in previous.items():
        if identity in current:
            if plan.status_for(identity) is IncrementalStatus.UNCHANGED:
                result[identity] = replace(
                    state,
                    batch_id=batch_id,
                    last_seen_at=observed_at,
                    is_active=True,
                )
            continue
        if not plan.complete_snapshot or identity.scope not in scopes:
            result[identity] = replace(
                state,
                batch_id=batch_id,
                is_active=True,
            )

    for source in plan.current:
        identity = source.identity
        previous_state = previous.get(identity)
        status = plan.status_for(identity)
        if status is IncrementalStatus.UNCHANGED and previous_state is not None:
            result[identity] = replace(
                previous_state,
                source_hash=source.source_hash,
                batch_id=batch_id,
                last_seen_at=observed_at,
                is_active=True,
            )
            continue
        result[identity] = ProgramState.from_source(
            source,
            observed_at=observed_at,
            batch_id=batch_id,
            first_seen_at=(
                previous_state.first_seen_at
                if previous_state is not None
                else observed_at
            ),
            last_changed_at=observed_at,
        )

    return tuple(sorted(result.values(), key=lambda state: state.identity.key))


def program_identity_key(
    value: ProgramIdentity | ProgramSource | ProgramState,
) -> tuple[str, str, str]:
    """返回统一的 program identity key，供 executor 过滤旧 facts。"""

    if isinstance(value, (ProgramSource, ProgramState, ProgramIdentity)):
        return (
            value.identity.key if not isinstance(value, ProgramIdentity) else value.key
        )
    raise TypeError("value must be ProgramIdentity, ProgramSource, or ProgramState")


def issue_identity_key(issue: LineageIssue) -> tuple[str, str, str, str, str, str, str]:
    """跨 batch reconciliation 使用的稳定 issue identity。"""

    if not isinstance(issue, LineageIssue):
        raise TypeError("issue must be a LineageIssue")
    return (
        issue.environment,
        issue.source_profile,
        issue.program_name,
        IssueType(issue.issue_type).value,
        issue.stable_key or "",
        issue.node_key or "",
        issue.branch_sink or "",
    )


class IssueLifecycleStatus(str, Enum):
    NEW = "NEW"
    PERSISTING = "PERSISTING"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True, slots=True)
class IssueLifecycle:
    """一个 stable issue 在当前观察点的生命周期投影。"""

    issue: LineageIssue
    status: IssueLifecycleStatus
    first_seen_at: datetime
    last_seen_at: datetime
    resolved_at: datetime | None = None
    age_days: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.issue, LineageIssue):
            raise TypeError("issue must be a LineageIssue")
        if not isinstance(self.status, IssueLifecycleStatus):
            object.__setattr__(self, "status", IssueLifecycleStatus(self.status))
        if not isinstance(self.first_seen_at, datetime):
            raise TypeError("first_seen_at must be a datetime")
        if not isinstance(self.last_seen_at, datetime):
            raise TypeError("last_seen_at must be a datetime")
        if self.resolved_at is not None and not isinstance(self.resolved_at, datetime):
            raise TypeError("resolved_at must be a datetime or None")
        if self.age_days < 0:
            raise ValueError("age_days must be non-negative")

    @property
    def stable_key(self) -> str | None:
        return self.issue.stable_key


@dataclass(frozen=True, slots=True)
class IssueLifecycleResult:
    """当前 issue、持续 issue 和已解决 issue 的确定性 reconciliation 结果。"""

    records: tuple[IssueLifecycle, ...] = ()
    current_issues: tuple[LineageIssue, ...] = ()

    def __post_init__(self) -> None:
        records = tuple(self.records)
        if any(not isinstance(record, IssueLifecycle) for record in records):
            raise TypeError("records must contain IssueLifecycle values")
        object.__setattr__(
            self,
            "records",
            tuple(sorted(records, key=lambda item: issue_identity_key(item.issue))),
        )
        current = tuple(self.current_issues)
        if any(not isinstance(issue, LineageIssue) for issue in current):
            raise TypeError("current_issues must contain LineageIssue values")
        object.__setattr__(
            self,
            "current_issues",
            tuple(sorted(current, key=issue_identity_key)),
        )

    @property
    def newly_detected(self) -> tuple[IssueLifecycle, ...]:
        return tuple(
            record
            for record in self.records
            if record.status is IssueLifecycleStatus.NEW
        )

    @property
    def persisting(self) -> tuple[IssueLifecycle, ...]:
        return tuple(
            record
            for record in self.records
            if record.status is IssueLifecycleStatus.PERSISTING
        )

    @property
    def resolved(self) -> tuple[IssueLifecycle, ...]:
        return tuple(
            record
            for record in self.records
            if record.status is IssueLifecycleStatus.RESOLVED
        )


def _validate_unique_issues(issues: tuple[LineageIssue, ...], label: str) -> None:
    identities: set[tuple[str, str, str, str, str, str, str]] = set()
    for issue in issues:
        identity = issue_identity_key(issue)
        if identity in identities:
            raise ValueError(f"{label} contains duplicate issue identity: {identity!r}")
        identities.add(identity)


def _age_days(first_seen_at: datetime, observed_at: datetime) -> float:
    return max(0.0, (observed_at - first_seen_at).total_seconds() / 86400)


def reconcile_issue_lifecycle(
    previous_issues: Iterable[LineageIssue],
    current_issues: Iterable[LineageIssue],
    *,
    observed_at: datetime,
) -> IssueLifecycleResult:
    """跨 batch reconciliation；历史行不更新，resolved 由快照差异推导。"""

    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime")
    previous_values = tuple(previous_issues)
    current_values = tuple(current_issues)
    if any(not isinstance(issue, LineageIssue) for issue in previous_values):
        raise TypeError("previous_issues must contain LineageIssue values")
    if any(not isinstance(issue, LineageIssue) for issue in current_values):
        raise TypeError("current_issues must contain LineageIssue values")
    _validate_unique_issues(previous_values, "previous_issues")
    _validate_unique_issues(current_values, "current_issues")
    previous = {issue_identity_key(issue): issue for issue in previous_values}
    current_map = {issue_identity_key(issue): issue for issue in current_values}
    records: list[IssueLifecycle] = []
    reconciled: list[LineageIssue] = []

    for identity, issue in current_map.items():
        old = previous.get(identity)
        first_seen = (
            old.first_seen_at or observed_at
            if old is not None
            else issue.first_seen_at or observed_at
        )
        last_seen = (
            observed_at
            if issue.last_seen_at is None or issue.last_seen_at == observed_at
            else issue.last_seen_at
        )
        resolved_issue = replace(
            issue,
            first_seen_at=first_seen,
            last_seen_at=last_seen,
            is_active=True,
        )
        reconciled.append(resolved_issue)
        status = (
            IssueLifecycleStatus.PERSISTING
            if old is not None
            else IssueLifecycleStatus.NEW
        )
        records.append(
            IssueLifecycle(
                issue=resolved_issue,
                status=status,
                first_seen_at=first_seen,
                last_seen_at=last_seen,
                age_days=_age_days(first_seen, observed_at),
            )
        )

    for identity, old in previous.items():
        if identity in current_map:
            continue
        first_seen = old.first_seen_at or old.last_seen_at or observed_at
        resolved_issue = replace(old, is_active=False)
        records.append(
            IssueLifecycle(
                issue=resolved_issue,
                status=IssueLifecycleStatus.RESOLVED,
                first_seen_at=first_seen,
                last_seen_at=old.last_seen_at or first_seen,
                resolved_at=observed_at,
                age_days=_age_days(first_seen, observed_at),
            )
        )

    return IssueLifecycleResult(
        records=tuple(records), current_issues=tuple(reconciled)
    )


@dataclass(frozen=True, slots=True)
class BatchMetadata:
    """历史 batch 的只读元数据摘要。"""

    batch_id: str
    observed_at: datetime
    published_at: datetime | None
    edge_count: int
    issue_count: int
    program_count: int
    is_active: bool

    def __post_init__(self) -> None:
        if not isinstance(self.batch_id, str) or not self.batch_id.strip():
            raise ValueError("batch_id must be a non-empty string")
        if not isinstance(self.observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        if self.published_at is not None and not isinstance(
            self.published_at, datetime
        ):
            raise TypeError("published_at must be a datetime or None")
        for field_name in ("edge_count", "issue_count", "program_count"):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        if not isinstance(self.is_active, bool):
            raise TypeError("is_active must be a boolean")


@dataclass(frozen=True, slots=True)
class BusinessLineageEdge:
    """只包含业务图 identity 的 canonical edge；忽略 provenance。"""

    environment: str
    source_table: str
    target_table: str

    def __post_init__(self) -> None:
        if not isinstance(self.environment, str) or not self.environment.strip():
            raise ValueError("environment must be a non-empty string")
        source = _canonical_asset_name(self.source_table)
        target = _canonical_asset_name(self.target_table)
        if not source or not target:
            raise ValueError("source_table and target_table must be non-empty assets")
        object.__setattr__(self, "environment", self.environment.strip())
        object.__setattr__(self, "source_table", source)
        object.__setattr__(self, "target_table", target)

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.environment, self.source_table, self.target_table)

    def to_dict(self) -> dict[str, str]:
        return {
            "environment": self.environment,
            "source_table": self.source_table,
            "target_table": self.target_table,
        }


@dataclass(frozen=True, slots=True)
class LineageBatchDiff:
    added_edges: tuple[BusinessLineageEdge, ...] = ()
    removed_edges: tuple[BusinessLineageEdge, ...] = ()
    unchanged_edges: tuple[BusinessLineageEdge, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("added_edges", "removed_edges", "unchanged_edges"):
            values = tuple(getattr(self, field_name))
            if any(not isinstance(value, BusinessLineageEdge) for value in values):
                raise TypeError(f"{field_name} must contain BusinessLineageEdge values")
            object.__setattr__(
                self,
                field_name,
                tuple(sorted(values, key=lambda item: item.key)),
            )

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {
            "added_edges": [edge.to_dict() for edge in self.added_edges],
            "removed_edges": [edge.to_dict() for edge in self.removed_edges],
            "unchanged_edges": [edge.to_dict() for edge in self.unchanged_edges],
        }


@dataclass(frozen=True, slots=True)
class EnvironmentLineageDiff:
    only_in_dev: tuple[BusinessLineageEdge, ...] = ()
    only_in_prod: tuple[BusinessLineageEdge, ...] = ()
    unchanged_edges: tuple[BusinessLineageEdge, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("only_in_dev", "only_in_prod", "unchanged_edges"):
            values = tuple(getattr(self, field_name))
            if any(not isinstance(value, BusinessLineageEdge) for value in values):
                raise TypeError(f"{field_name} must contain BusinessLineageEdge values")
            object.__setattr__(
                self,
                field_name,
                tuple(sorted(values, key=lambda item: item.key)),
            )

    @property
    def common(self) -> tuple[BusinessLineageEdge, ...]:
        return self.unchanged_edges

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {
            "only_in_dev": [edge.to_dict() for edge in self.only_in_dev],
            "only_in_prod": [edge.to_dict() for edge in self.only_in_prod],
            "unchanged_edges": [edge.to_dict() for edge in self.unchanged_edges],
        }


def _business_edge(
    value: LineageEdge, *, environment: str | None = None
) -> BusinessLineageEdge:
    if not isinstance(value, LineageEdge):
        raise TypeError("edge collections must contain LineageEdge values")
    return BusinessLineageEdge(
        environment=environment or value.environment,
        source_table=value.source_table,
        target_table=value.target_table,
    )


def _business_edge_map(
    edges: Iterable[LineageEdge], *, environment: str | None = None
) -> dict[tuple[str, str, str], BusinessLineageEdge]:
    result: dict[tuple[str, str, str], BusinessLineageEdge] = {}
    for edge in edges:
        business_edge = _business_edge(edge, environment=environment)
        result[business_edge.key] = business_edge
    return result


def diff_lineage_batches(
    previous_edges: Iterable[LineageEdge],
    current_edges: Iterable[LineageEdge],
) -> LineageBatchDiff:
    """比较两个 batch 的正式业务 graph，不把 provenance 变化算作 edge 变化。"""

    previous = _business_edge_map(previous_edges)
    current = _business_edge_map(current_edges)
    previous_keys = set(previous)
    current_keys = set(current)
    return LineageBatchDiff(
        added_edges=tuple(current[key] for key in sorted(current_keys - previous_keys)),
        removed_edges=tuple(
            previous[key] for key in sorted(previous_keys - current_keys)
        ),
        unchanged_edges=tuple(
            current[key] for key in sorted(previous_keys & current_keys)
        ),
    )


def diff_environments(
    dev_edges: Iterable[LineageEdge],
    prod_edges: Iterable[LineageEdge],
    *,
    dev_environment: str = "DEV",
    prod_environment: str = "PROD",
    dev_source_profile: str | None = None,
    prod_source_profile: str | None = None,
) -> EnvironmentLineageDiff:
    """比较 canonical formal assets；默认忽略 source_profile 和 program provenance。"""

    dev_name = dev_environment.strip() if isinstance(dev_environment, str) else ""
    prod_name = prod_environment.strip() if isinstance(prod_environment, str) else ""
    if not dev_name or not prod_name:
        raise ValueError("environment names must be non-empty strings")
    for profile, field_name in (
        (dev_source_profile, "dev_source_profile"),
        (prod_source_profile, "prod_source_profile"),
    ):
        if profile is not None and (
            not isinstance(profile, str) or not profile.strip()
        ):
            raise ValueError(f"{field_name} must be a non-empty string or None")
    dev_profile = dev_source_profile.strip() if dev_source_profile else None
    prod_profile = prod_source_profile.strip() if prod_source_profile else None
    dev = _business_edge_map(
        (
            edge
            for edge in dev_edges
            if (
                isinstance(edge, LineageEdge)
                and edge.environment == dev_name
                and (dev_profile is None or edge.source_profile == dev_profile)
            )
        ),
        environment=dev_name,
    )
    prod = _business_edge_map(
        (
            edge
            for edge in prod_edges
            if (
                isinstance(edge, LineageEdge)
                and edge.environment == prod_name
                and (prod_profile is None or edge.source_profile == prod_profile)
            )
        ),
        environment=prod_name,
    )
    dev_graph_keys = {(edge.source_table, edge.target_table) for edge in dev.values()}
    prod_graph_keys = {(edge.source_table, edge.target_table) for edge in prod.values()}

    def matching_edge(
        graph_key: tuple[str, str],
        source: dict[tuple[str, str, str], BusinessLineageEdge],
        environment: str,
    ) -> BusinessLineageEdge:
        for edge in source.values():
            if (edge.source_table, edge.target_table) == graph_key:
                return BusinessLineageEdge(
                    environment, edge.source_table, edge.target_table
                )
        raise AssertionError("environment diff edge lookup failed")

    common_keys = dev_graph_keys & prod_graph_keys
    return EnvironmentLineageDiff(
        only_in_dev=tuple(
            matching_edge(key, dev, dev_name)
            for key in sorted(dev_graph_keys - prod_graph_keys)
        ),
        only_in_prod=tuple(
            matching_edge(key, prod, prod_name)
            for key in sorted(prod_graph_keys - dev_graph_keys)
        ),
        unchanged_edges=tuple(
            matching_edge(key, dev, dev_name) for key in sorted(common_keys)
        ),
    )


def _canonical_asset_name(value: str | None) -> str:
    return normalize_table_name(value or "")


def _issue_expected_target(issue: LineageIssue) -> str | None:
    if not isinstance(issue.evidence, dict):
        return None
    value = issue.evidence.get("expected_target")
    normalized = _canonical_asset_name(value if isinstance(value, str) else None)
    return normalized or None


def detect_broken_lineage_branches(
    previous_edges: Iterable[LineageEdge],
    current_edges: Iterable[LineageEdge],
    previous_issues: Iterable[LineageIssue],
    current_issues: Iterable[LineageIssue],
    *,
    observed_at: datetime,
    batch_id: str | None = None,
) -> tuple[LineageIssue, ...]:
    """只将“旧程序曾到达 target、当前同程序出现 orphan”提升为 broken。

    当前 active ``LINEAGE_BRANCH_BROKEN`` issue 也会作为证据继续 carry，避免
    第二个 batch 因旧 valid edge 已经消失而把 broken 状态错误降级为普通 orphan。
    """

    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime")
    old_edges = tuple(previous_edges)
    now_edges = tuple(current_edges)
    old_issues = tuple(previous_issues)
    now_issues = tuple(current_issues)
    old_valid: set[tuple[str, str, str, str]] = set()
    for edge in old_edges:
        if not isinstance(edge, LineageEdge) or edge.program_name is None:
            continue
        old_valid.add(
            (
                edge.environment,
                edge.source_profile,
                edge.program_name,
                _canonical_asset_name(edge.target_table),
            )
        )
    # Keep the current graph in the signature intentionally: it documents that
    # this check is a transition from a previous valid branch to a current
    # snapshot, while the orphan issue is the authoritative current evidence.
    _ = now_edges
    previous_broken = {
        (
            issue.environment,
            issue.source_profile,
            issue.program_name,
            issue.branch_sink or "",
        ): issue
        for issue in old_issues
        if IssueType(issue.issue_type) is IssueType.LINEAGE_BRANCH_BROKEN
    }
    generated: dict[tuple[str, str, str, str, str, str, str], LineageIssue] = {}
    for orphan in now_issues:
        if IssueType(orphan.issue_type) is not IssueType.ORPHAN_BRANCH:
            continue
        expected_target = _issue_expected_target(orphan)
        if not expected_target or not orphan.branch_sink:
            continue
        previous_key = (
            orphan.environment,
            orphan.source_profile,
            orphan.program_name,
            orphan.branch_sink,
        )
        had_previous_valid_target = (
            orphan.environment,
            orphan.source_profile,
            orphan.program_name,
            expected_target,
        ) in old_valid
        prior_broken = previous_broken.get(previous_key)
        if not had_previous_valid_target and prior_broken is None:
            continue
        previous_evidence = prior_broken.evidence if prior_broken is not None else None
        evidence: dict[str, object] = {
            "expected_target": expected_target,
            "branch_sink": orphan.branch_sink,
            "current_orphan_issue_key": orphan.stable_key,
            "previous_valid_target": had_previous_valid_target,
        }
        if previous_evidence is not None:
            evidence["previous_broken_evidence"] = previous_evidence
        stable_key = compute_lineage_issue_stable_key(
            orphan.environment,
            orphan.source_profile,
            orphan.program_name,
            IssueType.LINEAGE_BRANCH_BROKEN,
            branch_sink=orphan.branch_sink,
        )
        broken = LineageIssue(
            environment=orphan.environment,
            source_profile=orphan.source_profile,
            program_name=orphan.program_name,
            issue_type=IssueType.LINEAGE_BRANCH_BROKEN,
            severity=issue_severity(IssueType.LINEAGE_BRANCH_BROKEN),
            message=(
                f"Program {orphan.program_name} previously reached "
                f"{expected_target}, but branch {orphan.branch_sink} is now broken."
            ),
            branch_sink=orphan.branch_sink,
            evidence=evidence,
            batch_id=batch_id,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            is_active=True,
            stable_key=stable_key,
        )
        generated[issue_identity_key(broken)] = broken
    return tuple(sorted(generated.values(), key=issue_identity_key))


__all__ = [
    "BatchMetadata",
    "BusinessLineageEdge",
    "EnvironmentLineageDiff",
    "IncrementalPlan",
    "IncrementalStatus",
    "IssueLifecycle",
    "IssueLifecycleResult",
    "IssueLifecycleStatus",
    "LineageBatchDiff",
    "SnapshotScope",
    "build_program_states",
    "detect_broken_lineage_branches",
    "diff_environments",
    "diff_lineage_batches",
    "issue_identity_key",
    "plan_incremental",
    "program_identity_key",
    "reconcile_issue_lifecycle",
]
