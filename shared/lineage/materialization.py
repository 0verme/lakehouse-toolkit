"""Phase 5: 从 audited Physical DAG 派生正式业务血缘。

本模块只负责纯转换，不访问数据库。Physical DAG 保留完整的 TMP、cycle 和
orphan 事实；materialization 只沿 TMP 穿透到下一个正式资产，并把每条
``LineageEdge`` 的 provenance 压缩为可序列化的结构化 evidence。
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from shared.lineage.audit import (
    LineageAuditResult,
    audit_program_physical_dag,
)
from shared.lineage.audit import (
    _selected_statement_evidence as _audit_selected_statement_evidence,
)
from shared.lineage.audit import (
    _statement_indices as _audit_statement_indices,
)
from shared.lineage.audit import (
    _value_sort_key as _audit_value_sort_key,
)
from shared.lineage.domain import (
    IssueType,
    LineageEdge,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramState,
    is_temporary_asset,
)
from shared.lineage.physical_dag import ProgramPhysicalDAG


@dataclass(frozen=True, slots=True)
class ProgramMaterialization:
    """一个程序的正式 edge 与 audited issue。"""

    dag: ProgramPhysicalDAG
    audit: LineageAuditResult
    edges: tuple[LineageEdge, ...]
    issues: tuple[LineageIssue, ...]


@dataclass(frozen=True, slots=True)
class MaterializationBatch:
    """一次完整 materialization 计算的不可变 candidate snapshot。"""

    batch_id: str
    observed_at: datetime
    edges: tuple[LineageEdge, ...] = ()
    issues: tuple[LineageIssue, ...] = ()
    program_states: tuple[ProgramState, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.batch_id, str) or not self.batch_id.strip():
            raise ValueError("batch_id must be a non-empty string")
        if not isinstance(self.observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        object.__setattr__(self, "batch_id", self.batch_id.strip())
        object.__setattr__(self, "edges", tuple(self.edges))
        object.__setattr__(self, "issues", tuple(self.issues))
        object.__setattr__(self, "program_states", tuple(self.program_states))
        if any(not isinstance(state, ProgramState) for state in self.program_states):
            raise TypeError("program_states must contain ProgramState values")


def new_batch_id() -> str:
    """生成不依赖 Python ``hash()`` 的生产默认 batch identity。"""

    return f"batch-{uuid4().hex}"


def _resolve_batch_id(batch_id: str | None) -> str:
    if batch_id is None:
        return new_batch_id()
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id must be a non-empty string or None")
    return batch_id.strip()


def _resolve_observed_at(observed_at: datetime | None) -> datetime:
    if observed_at is None:
        return datetime.now(timezone.utc)
    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime or None")
    return observed_at


def _json_safe(value: object) -> object:
    """把 evidence 限制为 deterministic JSON-safe 结构，不保存对象 repr。"""

    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_json_safe(item) for item in value]
        return sorted(items, key=_canonical_json)
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(
        "evidence contains a value that cannot be represented as deterministic JSON"
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _unique_sorted_values(values: Iterable[object]) -> list[object]:
    unique: dict[str, object] = {}
    for value in values:
        unique[_canonical_json(value)] = value
    return sorted(unique.values(), key=_audit_value_sort_key)


def _as_items(value: object) -> list[object]:
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def _selected_physical_evidence(value: object) -> dict[str, object]:
    """复用 Phase 4 的轻量 evidence 白名单，并排除完整源码。"""

    selected = _audit_selected_statement_evidence(value)
    safe_value = _json_safe(selected)
    return safe_value if isinstance(safe_value, dict) else {}


def _statement_indices(evidence: object) -> list[object]:
    if not isinstance(evidence, Mapping):
        return []
    return _audit_statement_indices(evidence)


def _physical_edge_sort_key(edge: PhysicalEdge) -> tuple[str, str, str, str]:
    return (
        edge.source,
        edge.target,
        edge.evidence_type,
        _canonical_json(_selected_physical_evidence(edge.evidence)),
    )


def _physical_edge_summary(edge: PhysicalEdge) -> dict[str, object]:
    record: dict[str, object] = {
        "source": edge.source,
        "target": edge.target,
        "evidence_type": edge.evidence_type,
        "statement_indices": _statement_indices(edge.evidence),
    }
    details = _selected_physical_evidence(edge.evidence)
    if details:
        record["evidence"] = details
    return record


def _graph_nodes(dag: ProgramPhysicalDAG) -> set[str]:
    nodes = {node.node_key for node in dag.nodes}
    nodes.update(edge.source for edge in dag.edges)
    nodes.update(edge.target for edge in dag.edges)
    return nodes


def _node_map(dag: ProgramPhysicalDAG) -> dict[str, PhysicalNode]:
    return {node.node_key: node for node in dag.nodes}


def _is_temporary(node_key: str, node_map: Mapping[str, PhysicalNode]) -> bool:
    node = node_map.get(node_key)
    if node is not None:
        return node.kind is PhysicalNodeKind.TEMPORARY_ASSET
    return is_temporary_asset(node_key)


def _build_adjacency(
    edges: Iterable[PhysicalEdge],
) -> dict[str, tuple[PhysicalEdge, ...]]:
    adjacency: dict[str, list[PhysicalEdge]] = {}
    for edge in edges:
        adjacency.setdefault(edge.source, []).append(edge)
    return {
        source: tuple(sorted(items, key=_physical_edge_sort_key))
        for source, items in adjacency.items()
    }


def _included_nodes(audit: LineageAuditResult) -> set[str]:
    """只使用 Audit 已给出的 target-reaching facts，不重新推断 target。"""

    if audit.expected_target is None:
        # 没有权威 target 时，不判定任何 branch 为 orphan；只 materialize
        # Physical 图中已经明确存在的 formal-to-formal boundary。
        return _graph_nodes(audit.dag)
    return set(audit.target_reachable_nodes)


def _collapsed_paths(
    dag: ProgramPhysicalDAG,
    included_nodes: set[str],
) -> tuple[tuple[tuple[str, ...], tuple[PhysicalEdge, ...]], ...]:
    """沿 TMP 穿透，到达第一个 formal 节点后停止。"""

    node_map = _node_map(dag)
    adjacency = _build_adjacency(dag.edges)
    formal_starts = sorted(
        node for node in included_nodes if not _is_temporary(node, node_map)
    )
    paths: dict[tuple[str, ...], tuple[PhysicalEdge, ...]] = {}

    for start in formal_starts:
        pending: list[tuple[str, tuple[str, ...], tuple[PhysicalEdge, ...]]] = [
            (start, (start,), ())
        ]
        while pending:
            current, path, path_edges = pending.pop()
            for edge in adjacency.get(current, ()):
                next_node = edge.target
                if next_node not in included_nodes:
                    continue
                if _is_temporary(next_node, node_map):
                    if next_node in path:
                        # TMP cycle 没有新的 formal boundary；停止该路径。
                        continue
                    pending.append(
                        (next_node, path + (next_node,), path_edges + (edge,))
                    )
                    continue

                # Formal endpoint 是一条新的业务资产边界。即使它等于 start，
                # 也只输出一次 self edge，不再沿它继续展开。
                completed_path = path + (next_node,)
                paths.setdefault(completed_path, path_edges + (edge,))

    return tuple(
        sorted(
            paths.items(),
            key=lambda item: (
                _canonical_json(item[0]),
                tuple(_physical_edge_sort_key(edge) for edge in item[1]),
            ),
        )
    )


def _edge_identity(edge: LineageEdge) -> tuple[str, str, str, str, str, str]:
    return (
        edge.environment,
        edge.source_profile,
        edge.source_table,
        edge.target_table,
        edge.program_name or "",
        edge.job_key or "",
    )


def _path_evidence(
    path: tuple[str, ...],
    physical_edges: tuple[PhysicalEdge, ...],
) -> dict[str, object]:
    edge_records = [_physical_edge_summary(edge) for edge in physical_edges]
    # path[1:-1] 只包含 traversal 中实际穿透的 temporary 节点，
    # 也涵盖通过 CREATE TEMP 标记但名称不是 TMP 的节点。
    tmp_nodes = sorted(set(path[1:-1]))
    return {
        "nodes": list(path),
        "physical_edge_pairs": [[edge.source, edge.target] for edge in physical_edges],
        "physical_edges": edge_records,
        "collapsed_tmp_nodes": tmp_nodes,
    }


def _edge_evidence(
    paths: Iterable[tuple[tuple[str, ...], tuple[PhysicalEdge, ...]]],
) -> dict[str, object]:
    path_records = [
        _path_evidence(path, physical_edges) for path, physical_edges in paths
    ]
    path_records = sorted(
        path_records,
        key=_canonical_json,
    )
    physical_pairs = sorted(
        {
            (str(pair[0]), str(pair[1]))
            for record in path_records
            for pair in _as_items(record.get("physical_edge_pairs"))
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        }
    )
    statement_indices = _unique_sorted_values(
        index
        for record in path_records
        for edge in _as_items(record.get("physical_edges"))
        if isinstance(edge, Mapping)
        for index in _as_items(edge.get("statement_indices"))
    )
    collapsed_tmp_nodes = sorted(
        {
            str(node)
            for record in path_records
            for node in _as_items(record.get("collapsed_tmp_nodes"))
        }
    )
    return {
        "collapse": "tmp_until_formal_boundary",
        "collapsed_tmp_nodes": collapsed_tmp_nodes,
        "physical_edge_pairs": [list(pair) for pair in physical_pairs],
        "physical_paths": path_records,
        "path_count": len(path_records),
        "statement_indices": statement_indices,
    }


def _lineage_edge_from_path(
    dag: ProgramPhysicalDAG,
    path: tuple[str, ...],
    physical_edges: tuple[PhysicalEdge, ...],
    *,
    batch_id: str,
    observed_at: datetime,
    job_key: str | None,
) -> LineageEdge:
    source = dag.program_source
    return LineageEdge(
        environment=source.environment,
        source_profile=source.source_profile,
        source_table=path[0],
        target_table=path[-1],
        program_name=source.program_name,
        job_key=job_key,
        evidence_type="physical_dag",
        source_hash=source.source_hash,
        batch_id=batch_id,
        observed_at=observed_at,
        updated_at=observed_at,
        is_active=True,
        evidence=_edge_evidence(((path, physical_edges),)),
    )


def _merge_edge_evidence(
    first: Mapping[str, object] | str | None,
    second: Mapping[str, object] | str | None,
) -> dict[str, object]:
    first_value = _json_safe(first) if isinstance(first, Mapping) else {}
    second_value = _json_safe(second) if isinstance(second, Mapping) else {}
    first_paths = _as_items(
        first_value.get("physical_paths", []) if isinstance(first_value, dict) else []
    )
    second_paths = _as_items(
        second_value.get("physical_paths", []) if isinstance(second_value, dict) else []
    )
    paths = [
        item for item in (*first_paths, *second_paths) if isinstance(item, Mapping)
    ]
    unique_paths: dict[str, object] = {_canonical_json(path): path for path in paths}
    sorted_paths = [unique_paths[key] for key in sorted(unique_paths)]
    physical_pairs = sorted(
        {
            (str(pair[0]), str(pair[1]))
            for path in sorted_paths
            if isinstance(path, Mapping)
            for pair in path.get("physical_edge_pairs", [])
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        }
    )
    tmp_nodes = sorted(
        {
            str(node)
            for path in sorted_paths
            if isinstance(path, Mapping)
            for node in path.get("collapsed_tmp_nodes", [])
        }
    )
    statement_indices = _unique_sorted_values(
        index
        for path in sorted_paths
        if isinstance(path, Mapping)
        for edge in _as_items(path.get("physical_edges"))
        if isinstance(edge, Mapping)
        for index in _as_items(edge.get("statement_indices"))
    )
    return {
        "collapse": "tmp_until_formal_boundary",
        "collapsed_tmp_nodes": tmp_nodes,
        "physical_edge_pairs": [list(pair) for pair in physical_pairs],
        "physical_paths": sorted_paths,
        "path_count": len(sorted_paths),
        "statement_indices": statement_indices,
    }


def _collapse_paths_to_edges(
    dag: ProgramPhysicalDAG,
    paths: Iterable[tuple[tuple[str, ...], tuple[PhysicalEdge, ...]]],
    *,
    batch_id: str,
    observed_at: datetime,
    job_key: str | None,
) -> tuple[LineageEdge, ...]:
    grouped: dict[tuple[str, str, str, str, str, str], LineageEdge] = {}
    ordered_paths = sorted(
        paths,
        key=lambda item: (
            _canonical_json(item[0]),
            tuple(_physical_edge_sort_key(edge) for edge in item[1]),
        ),
    )
    for path, physical_edges in ordered_paths:
        edge = _lineage_edge_from_path(
            dag,
            path,
            physical_edges,
            batch_id=batch_id,
            observed_at=observed_at,
            job_key=job_key,
        )
        identity = _edge_identity(edge)
        existing = grouped.get(identity)
        if existing is None:
            grouped[identity] = edge
            continue
        grouped[identity] = replace(
            existing,
            evidence=_merge_edge_evidence(existing.evidence, edge.evidence),
        )

    return tuple(
        sorted(
            grouped.values(),
            key=lambda edge: (
                _edge_identity(edge),
                edge.source_hash or "",
                edge.evidence_type,
                _canonical_json(edge.evidence),
            ),
        )
    )


def _issue_identity(issue: LineageIssue) -> tuple[str, str, str, str, str, str, str]:
    return (
        issue.environment,
        issue.source_profile,
        issue.program_name,
        IssueType(issue.issue_type).value,
        issue.stable_key or "",
        issue.node_key or "",
        issue.branch_sink or "",
    )


def _prepare_issues(
    issues: Iterable[LineageIssue],
    *,
    batch_id: str,
    observed_at: datetime,
) -> tuple[LineageIssue, ...]:
    prepared: list[LineageIssue] = []
    for issue in issues:
        if not isinstance(issue, LineageIssue):
            raise TypeError("issues must contain LineageIssue values")
        prepared.append(
            replace(
                issue,
                batch_id=batch_id,
                first_seen_at=issue.first_seen_at or observed_at,
                last_seen_at=observed_at,
                is_active=True,
            )
        )

    grouped: dict[tuple[str, str, str, str, str, str, str], LineageIssue] = {}
    for issue in sorted(
        prepared,
        key=lambda item: (
            _issue_identity(item),
            _canonical_json(item.evidence),
            item.message,
        ),
    ):
        grouped.setdefault(_issue_identity(issue), issue)
    return tuple(
        sorted(
            grouped.values(),
            key=lambda item: (
                _issue_identity(item),
                _canonical_json(item.evidence),
                item.message,
            ),
        )
    )


def materialize_program(
    dag: ProgramPhysicalDAG,
    audit_result: LineageAuditResult | None = None,
    *,
    issues: Iterable[LineageIssue] | None = None,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_key: str | None = None,
) -> ProgramMaterialization:
    """把一个 audited DAG 转换为 direct formal ``LineageEdge``。

    ``audit_result`` 未提供时只调用既有 Phase 4 auditor，不在这里复制 detector。
    已知 expected target 时只使用 ``target_reachable_nodes``；未知 target 时不猜 sink，
    仅 materialize 图中已有的 formal-to-formal boundary。
    """

    if not isinstance(dag, ProgramPhysicalDAG):
        raise TypeError("dag must be a ProgramPhysicalDAG")
    if audit_result is not None:
        if not isinstance(audit_result, LineageAuditResult):
            raise TypeError("audit_result must be a LineageAuditResult or None")
        if audit_result.dag != dag:
            raise ValueError("audit_result must describe the supplied dag")

    resolved_batch_id = _resolve_batch_id(batch_id)
    resolved_observed_at = _resolve_observed_at(observed_at)
    audit = audit_result or audit_program_physical_dag(
        dag,
        observed_at=resolved_observed_at,
        batch_id=resolved_batch_id,
    )
    if job_key is not None and (not isinstance(job_key, str) or not job_key.strip()):
        raise ValueError("job_key must be a non-empty string or None")
    normalized_job_key = job_key.strip() if isinstance(job_key, str) else None
    selected_issues = audit.issues if issues is None else issues
    included_nodes = _included_nodes(audit)
    paths = _collapsed_paths(dag, included_nodes)
    edges = _collapse_paths_to_edges(
        dag,
        paths,
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
        job_key=normalized_job_key,
    )
    prepared_issues = _prepare_issues(
        selected_issues,
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
    )
    return ProgramMaterialization(
        dag=dag,
        audit=audit,
        edges=edges,
        issues=prepared_issues,
    )


def collapse_tmp_edges(
    dag: ProgramPhysicalDAG,
    audit_result: LineageAuditResult | None = None,
    *,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_key: str | None = None,
) -> tuple[LineageEdge, ...]:
    """纯 TMP collapse 入口；返回已带批次 metadata 的 direct edges。"""

    return materialize_program(
        dag,
        audit_result,
        batch_id=batch_id,
        observed_at=observed_at,
        job_key=job_key,
    ).edges


# 语义化兼容入口；使用同一签名，避免维护两份转换逻辑。
materialize_program_lineage = materialize_program


def _job_key_for(
    source_program_name: str,
    job_keys: Mapping[str, str] | None,
) -> str | None:
    if job_keys is None:
        return None
    value = job_keys.get(source_program_name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("job_keys values must be non-empty strings")
    return value.strip()


def materialize_batch(
    audits: Iterable[LineageAuditResult],
    *,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_keys: Mapping[str, str] | None = None,
) -> MaterializationBatch:
    """对多个既有 Audit 结果做完整、确定性的 candidate 计算。"""

    resolved_batch_id = _resolve_batch_id(batch_id)
    resolved_observed_at = _resolve_observed_at(observed_at)
    all_edges: list[LineageEdge] = []
    all_issues: list[LineageIssue] = []
    for audit in audits:
        if not isinstance(audit, LineageAuditResult):
            raise TypeError("audits must contain LineageAuditResult values")
        result = materialize_program(
            audit.dag,
            audit,
            batch_id=resolved_batch_id,
            observed_at=resolved_observed_at,
            job_key=_job_key_for(audit.dag.program_source.program_name, job_keys),
        )
        all_edges.extend(result.edges)
        all_issues.extend(result.issues)

    edge_groups: dict[tuple[str, str, str, str, str, str], LineageEdge] = {}
    for edge in sorted(
        all_edges,
        key=lambda item: (
            _edge_identity(item),
            item.source_hash or "",
            _canonical_json(item.evidence),
        ),
    ):
        identity = _edge_identity(edge)
        existing = edge_groups.get(identity)
        if existing is None:
            edge_groups[identity] = edge
        else:
            edge_groups[identity] = replace(
                existing,
                evidence=_merge_edge_evidence(existing.evidence, edge.evidence),
            )

    return MaterializationBatch(
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
        edges=tuple(
            sorted(
                edge_groups.values(),
                key=lambda item: (
                    _edge_identity(item),
                    item.source_hash or "",
                    _canonical_json(item.evidence),
                ),
            )
        ),
        issues=_prepare_issues(
            all_issues,
            batch_id=resolved_batch_id,
            observed_at=resolved_observed_at,
        ),
    )


build_materialization_batch = materialize_batch


__all__ = [
    "MaterializationBatch",
    "ProgramMaterialization",
    "build_materialization_batch",
    "collapse_tmp_edges",
    "materialize_batch",
    "materialize_program",
    "materialize_program_lineage",
    "new_batch_id",
]
