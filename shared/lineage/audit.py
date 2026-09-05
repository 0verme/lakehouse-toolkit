"""基于 ``ProgramPhysicalDAG`` 的轻量血缘审计与异常检测。

本模块是 Physical DAG 的只读观察者：它只消费 Phase 3 已确认的节点、边、
steps 和 sinks，不重新解析 ``script_code``，也不修改图、不折叠 TMP、不生成
正式 ``LineageEdge``。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType

from shared.lineage.domain import (
    IssueType,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    is_temporary_asset,
)
from shared.lineage.physical_dag import ProgramPhysicalDAG

ISSUE_SEVERITY_POLICY: Mapping[IssueType, str] = MappingProxyType(
    {
        IssueType.TARGET_NOT_FOUND: "HIGH",
        IssueType.TARGET_MISMATCH: "HIGH",
        IssueType.CYCLE_DETECTED: "HIGH",
        IssueType.SELF_REFERENCE: "HIGH",
        IssueType.ORPHAN_BRANCH: "MEDIUM",
        IssueType.MULTI_SINK_CANDIDATE: "MEDIUM",
    }
)

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


@dataclass(frozen=True, slots=True)
class LineageAuditResult:
    """一次 Physical DAG audit 的结构化结果与可复用事实摘要。"""

    dag: ProgramPhysicalDAG
    issues: tuple[LineageIssue, ...]
    expected_target: str | None
    target_reachable_nodes: tuple[str, ...] = ()
    orphan_branch_sinks: tuple[str, ...] = ()

    @property
    def issue_types(self) -> tuple[IssueType, ...]:
        """按结果顺序返回 issue 类型，便于调用方做轻量统计。"""

        return tuple(IssueType(issue.issue_type) for issue in self.issues)

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)


# 便于只关心结果对象的调用方使用短名称；正式文档使用 LineageAuditResult。
AuditResult = LineageAuditResult


def issue_severity(issue_type: IssueType | str) -> str:
    """返回集中定义的、对同一 ``IssueType`` 稳定的 severity。"""

    resolved_type = IssueType(issue_type)
    return ISSUE_SEVERITY_POLICY[resolved_type]


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
    if is_temporary_asset(node_key):
        return PhysicalNodeKind.TEMPORARY_ASSET.value
    return PhysicalNodeKind.FORMAL_ASSET.value


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
    if resolved_type is IssueType.ORPHAN_BRANCH:
        identity["scope"] = "branch"
        identity["branch_sink"] = branch_sink
    elif resolved_type is IssueType.SELF_REFERENCE:
        identity["scope"] = "node"
        identity["node_key"] = node_key
    elif resolved_type is IssueType.CYCLE_DETECTED:
        identity["scope"] = "cycle"
        identity["cycle_nodes"] = sorted(set(cycle_nodes))
    return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def _make_issue(
    dag: ProgramPhysicalDAG,
    issue_type: IssueType,
    *,
    observed_at: datetime,
    batch_id: str | None,
    message: str,
    evidence: Mapping[str, object],
    node_key: str | None = None,
    branch_sink: str | None = None,
    cycle_nodes: Iterable[str] = (),
) -> LineageIssue:
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
    return LineageIssue(
        environment=source.environment,
        source_profile=source.source_profile,
        program_name=source.program_name,
        issue_type=issue_type,
        severity=issue_severity(issue_type),
        message=message,
        node_key=node_key,
        branch_sink=branch_sink,
        evidence=dict(evidence),
        batch_id=batch_id,
        first_seen_at=observed_at,
        last_seen_at=observed_at,
        is_active=True,
        stable_key=stable_key,
    )


def _issue_sort_key(issue: LineageIssue) -> tuple[str, str, str, str, str]:
    cycle_sort_key = ""
    if IssueType(issue.issue_type) is IssueType.CYCLE_DETECTED:
        evidence = issue.evidence
        if isinstance(evidence, Mapping):
            cycle_nodes = evidence.get("cycle_nodes", ())
            if isinstance(cycle_nodes, (list, tuple)):
                cycle_sort_key = "\u0000".join(str(node) for node in cycle_nodes)
    return (
        IssueType(issue.issue_type).value,
        issue.branch_sink or "",
        issue.node_key or "",
        cycle_sort_key,
        issue.stable_key or "",
    )


class ProgramLineageAuditor:
    """对一个 ``ProgramPhysicalDAG`` 执行只读、确定性的 Phase 4 audit。"""

    def audit(
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

        sinks = _unique_sorted(dag.sinks)
        written_targets = _unique_sorted(
            [
                *(step.target for step in dag.steps if step.target is not None),
                *(edge.target for edge in edges),
                *sinks,
            ]
        )
        formal_sinks = tuple(
            sink
            for sink in sinks
            if _node_kind_value(sink, node_map) == PhysicalNodeKind.FORMAL_ASSET.value
        )
        temporary_sinks = tuple(
            sink
            for sink in sinks
            if _node_kind_value(sink, node_map)
            == PhysicalNodeKind.TEMPORARY_ASSET.value
        )
        sink_kinds = {sink: _node_kind_value(sink, node_map) for sink in sinks}

        issues: list[LineageIssue] = []

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
            issues.append(
                _make_issue(
                    dag,
                    IssueType.SELF_REFERENCE,
                    observed_at=observed_at,
                    batch_id=batch_id,
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
            issues.append(
                _make_issue(
                    dag,
                    IssueType.CYCLE_DETECTED,
                    observed_at=observed_at,
                    batch_id=batch_id,
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

        if len(sinks) > 1:
            expected_text = dag.expected_target or "unknown"
            issues.append(
                _make_issue(
                    dag,
                    IssueType.MULTI_SINK_CANDIDATE,
                    observed_at=observed_at,
                    batch_id=batch_id,
                    message=(
                        f"Program {dag.program_source.program_name} has "
                        f"{len(sinks)} candidate sinks ({', '.join(sinks)}); "
                        f"expected target is {expected_text}."
                    ),
                    evidence={
                        "sink_count": len(sinks),
                        "sinks": list(sinks),
                        "sorted_sinks": list(sinks),
                        "formal_sinks": list(formal_sinks),
                        "temporary_sinks": list(temporary_sinks),
                        "sink_kinds": sink_kinds,
                        "expected_target": dag.expected_target,
                    },
                )
            )

        expected_target = dag.expected_target
        actual_formal_sinks = list(formal_sinks)
        if expected_target is not None and expected_target not in sinks:
            if expected_target in written_targets:
                issues.append(
                    _make_issue(
                        dag,
                        IssueType.TARGET_MISMATCH,
                        observed_at=observed_at,
                        batch_id=batch_id,
                        message=(
                            f"Program {dag.program_source.program_name} writes "
                            f"expected target {expected_target}, but it is not a "
                            "final sink."
                        ),
                        evidence={
                            "expected_target": expected_target,
                            "actual_formal_sinks": actual_formal_sinks,
                            "all_sinks": list(sinks),
                            "written_targets": list(written_targets),
                            "expected_target_written": True,
                            "expected_target_is_sink": False,
                        },
                    )
                )
            elif actual_formal_sinks:
                issues.append(
                    _make_issue(
                        dag,
                        IssueType.TARGET_MISMATCH,
                        observed_at=observed_at,
                        batch_id=batch_id,
                        message=(
                            f"Program {dag.program_source.program_name} writes "
                            f"formal sink(s) {', '.join(actual_formal_sinks)} "
                            f"instead of expected target {expected_target}."
                        ),
                        evidence={
                            "expected_target": expected_target,
                            "actual_formal_sinks": actual_formal_sinks,
                            "all_sinks": list(sinks),
                            "written_targets": list(written_targets),
                            "expected_target_written": False,
                            "expected_target_is_sink": False,
                        },
                    )
                )
            else:
                issues.append(
                    _make_issue(
                        dag,
                        IssueType.TARGET_NOT_FOUND,
                        observed_at=observed_at,
                        batch_id=batch_id,
                        message=(
                            f"Program {dag.program_source.program_name} did not "
                            f"write expected target {expected_target}; no formal "
                            "result sink was found."
                        ),
                        evidence={
                            "expected_target": expected_target,
                            "written_targets": list(written_targets),
                            "sinks": list(sinks),
                            "formal_sinks": actual_formal_sinks,
                            "temporary_sinks": list(temporary_sinks),
                        },
                    )
                )

        target_reachable_nodes: tuple[str, ...] = ()
        orphan_branch_sinks: tuple[str, ...] = ()
        if expected_target is not None and expected_target in written_targets:
            target_reachable = _reverse_reachable(expected_target, reverse)
            target_reachable_nodes = tuple(sorted(target_reachable))
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
                issues.append(
                    _make_issue(
                        dag,
                        IssueType.ORPHAN_BRANCH,
                        observed_at=observed_at,
                        batch_id=batch_id,
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

        issues.sort(key=_issue_sort_key)
        return LineageAuditResult(
            dag=dag,
            issues=tuple(issues),
            expected_target=expected_target,
            target_reachable_nodes=target_reachable_nodes,
            orphan_branch_sinks=orphan_branch_sinks,
        )

    def __call__(
        self,
        dag: ProgramPhysicalDAG,
        observed_at: datetime | None = None,
        batch_id: str | None = None,
    ) -> LineageAuditResult:
        return self.audit(dag, observed_at=observed_at, batch_id=batch_id)


def audit_program_physical_dag(
    dag: ProgramPhysicalDAG,
    observed_at: datetime | None = None,
    batch_id: str | None = None,
) -> LineageAuditResult:
    """审计一个程序 Physical DAG；未传时间时在入口统一生成一次。"""

    return ProgramLineageAuditor().audit(
        dag,
        observed_at=observed_at,
        batch_id=batch_id,
    )


__all__ = [
    "AuditResult",
    "ISSUE_SEVERITY_POLICY",
    "LineageAuditResult",
    "ProgramLineageAuditor",
    "audit_program_physical_dag",
    "compute_lineage_issue_stable_key",
    "issue_severity",
]
