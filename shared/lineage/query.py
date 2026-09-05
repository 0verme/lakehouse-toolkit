"""Phase 6 query, Viewer contract, and downstream impact analysis.

The query layer consumes materialized ``LineageEdge`` facts only.  It does not
call providers, parse program SQL, or build a Physical DAG.  The edge reader is
a small protocol so SQLite is only one possible storage adapter.
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from .domain import LineageEdge, normalize_asset_name

DEFAULT_QUERY_DEPTH = 7
DEFAULT_QUERY_MAX_NODES = 300


class LineageDirection(str, Enum):
    """查询方向；领域边始终保持 ``source -> target``。"""

    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class LineageEdgeReader(Protocol):
    """只读 active ``lineage_edge`` 邻居的最小 repository 合约。"""

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
    ) -> Iterable[LineageEdge]:
        """返回 scope 内以 ``source_table`` 为 source 的 active edges。"""
        return ()

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
    ) -> Iterable[LineageEdge]:
        """返回 scope 内以 ``target_table`` 为 target 的 active edges。"""
        return ()


@dataclass(frozen=True, slots=True)
class LineageNode:
    """Viewer 中的正式资产节点；``depth`` 是距 root 的 edge distance。"""

    id: str
    table: str
    depth: int

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id:
            raise ValueError("node id must be a non-empty string")
        if not isinstance(self.table, str) or not self.table:
            raise ValueError("node table must be a non-empty string")
        if not isinstance(self.depth, int) or isinstance(self.depth, bool):
            raise TypeError("node depth must be an integer")
        if self.depth < 0:
            raise ValueError("node depth must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {"id": self.id, "table": self.table, "depth": self.depth}


@dataclass(frozen=True, slots=True)
class LineageGraphEdge:
    """Viewer projection of one deduplicated ``source -> target`` graph edge."""

    source: str
    target: str

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("edge source must be a non-empty string")
        if not isinstance(self.target, str) or not self.target:
            raise ValueError("edge target must be a non-empty string")

    def to_dict(self) -> dict[str, str]:
        return {"source": self.source, "target": self.target}


@dataclass(frozen=True, slots=True)
class LineageQueryResult:
    """稳定、可直接转换为 Viewer JSON 的 lineage projection。"""

    nodes: tuple[LineageNode, ...] = ()
    edges: tuple[LineageGraphEdge, ...] = ()
    truncated: bool = False

    def __post_init__(self) -> None:
        nodes = tuple(self.nodes)
        edges = tuple(self.edges)
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be a boolean")
        if any(not isinstance(node, LineageNode) for node in nodes):
            raise TypeError("nodes must contain LineageNode values")
        if any(not isinstance(edge, LineageGraphEdge) for edge in edges):
            raise TypeError("edges must contain LineageGraphEdge values")
        object.__setattr__(
            self,
            "nodes",
            tuple(sorted(nodes, key=lambda node: (node.depth, node.id, node.table))),
        )
        object.__setattr__(
            self,
            "edges",
            tuple(sorted(edges, key=lambda edge: (edge.source, edge.target))),
        )

    def to_viewer_dict(self) -> dict[str, object]:
        """返回不含 dataclass repr 或 SQLite row 的稳定 JSON-compatible dict。"""

        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
            "truncated": self.truncated,
        }

    as_dict = to_viewer_dict

    def to_json(self) -> str:
        """序列化为 deterministic Viewer JSON。"""

        return json.dumps(
            self.to_viewer_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
        )

    to_viewer_json = to_json


@dataclass(frozen=True, slots=True)
class BlastRadiusResult:
    """downstream-only impact summary；root 不计入 impact。"""

    root: str | None
    direct_impact: int
    indirect_impact: int
    total_impact: int
    max_depth: int
    truncated: bool = False

    def __post_init__(self) -> None:
        for field_name in (
            "direct_impact",
            "indirect_impact",
            "total_impact",
            "max_depth",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{field_name} must be an integer")
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        if self.total_impact != self.direct_impact + self.indirect_impact:
            raise ValueError("total_impact must equal direct plus indirect impact")
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be a boolean")

    def to_dict(self) -> dict[str, object]:
        return {
            "direct_impact": self.direct_impact,
            "indirect_impact": self.indirect_impact,
            "total_impact": self.total_impact,
            "max_depth": self.max_depth,
            "truncated": self.truncated,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class _QueryScope:
    environment: str
    source_profile: str | None


class LineageQueryService:
    """基于 materialized edge reader 的统一 BFS 查询服务。"""

    def __init__(self, edge_reader: LineageEdgeReader) -> None:
        if not callable(getattr(edge_reader, "read_outgoing_edges", None)):
            raise TypeError("edge_reader must provide read_outgoing_edges")
        if not callable(getattr(edge_reader, "read_incoming_edges", None)):
            raise TypeError("edge_reader must provide read_incoming_edges")
        self._edge_reader = edge_reader

    def query(
        self,
        table: str,
        environment: str,
        direction: LineageDirection | str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
    ) -> LineageQueryResult:
        """按固定 depth/max_nodes 语义执行 upstream 或 downstream BFS。"""

        scope = _build_scope(environment, source_profile)
        root = _normalize_table(table)
        resolved_direction = _resolve_direction(direction)
        resolved_depth = _validate_limit(depth, "depth", allow_zero=True)
        resolved_max_nodes = _validate_limit(max_nodes, "max_nodes", allow_zero=False)
        return self._traverse(
            root,
            scope,
            resolved_direction,
            depth=resolved_depth,
            max_nodes=resolved_max_nodes,
        )

    def query_upstream(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
    ) -> LineageQueryResult:
        return self._query_direction(
            table,
            environment,
            LineageDirection.UPSTREAM,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
        )

    def query_downstream(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
    ) -> LineageQueryResult:
        return self._query_direction(
            table,
            environment,
            LineageDirection.DOWNSTREAM,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
        )

    def _query_direction(
        self,
        table: str,
        environment: str,
        direction: LineageDirection,
        *,
        depth: int,
        max_nodes: int,
        source_profile: str | None,
    ) -> LineageQueryResult:
        # pi-lens-ignore: python-sql-injection
        return self.query(
            table,
            environment,
            direction,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
        )

    def analyze_blast_radius(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
    ) -> BlastRadiusResult:
        """消费同一 downstream traversal，计算 root 之外的唯一影响资产。"""

        resolved_root = _normalize_table(table)
        result = self.query_downstream(
            resolved_root,
            environment,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
        )
        if not result.nodes:
            return BlastRadiusResult(
                root=None,
                direct_impact=0,
                indirect_impact=0,
                total_impact=0,
                max_depth=0,
                truncated=result.truncated,
            )

        direct = sum(node.depth == 1 for node in result.nodes)
        indirect = sum(node.depth >= 2 for node in result.nodes)
        max_depth = max((node.depth for node in result.nodes), default=0)
        return BlastRadiusResult(
            root=resolved_root,
            direct_impact=direct,
            indirect_impact=indirect,
            total_impact=direct + indirect,
            max_depth=max_depth,
            truncated=result.truncated,
        )

    blast_radius = analyze_blast_radius

    def _traverse(
        self,
        root: str,
        scope: _QueryScope,
        direction: LineageDirection,
        *,
        depth: int,
        max_nodes: int,
    ) -> LineageQueryResult:
        first_neighbors = self._neighbors(root, scope, direction)
        if not first_neighbors:
            opposite = (
                LineageDirection.UPSTREAM
                if direction is LineageDirection.DOWNSTREAM
                else LineageDirection.DOWNSTREAM
            )
            if not self._neighbors(root, scope, opposite):
                # root 不在 active lineage projection 中，不能凭空声明已知资产。
                return LineageQueryResult()

        node_depth: dict[str, int] = {root: 0}
        edge_pairs: set[tuple[str, str]] = set()
        pending: deque[tuple[str, int]] = deque([(root, 0)])
        truncated = False

        while pending:
            current, current_depth = pending.popleft()
            neighbors = (
                first_neighbors
                if current == root
                else self._neighbors(current, scope, direction)
            )
            for source, target in neighbors:
                next_node = (
                    target if direction is LineageDirection.DOWNSTREAM else source
                )
                if next_node in node_depth:
                    # 既有节点之间的边（包括 cycle/self-reference）仍展示，
                    # 但不会再次入队。
                    edge_pairs.add((source, target))
                    continue
                if current_depth >= depth or len(node_depth) >= max_nodes:
                    # 这里存在可达的新节点，但它受 depth/max_nodes 限制未返回。
                    truncated = True
                    continue
                node_depth[next_node] = current_depth + 1
                edge_pairs.add((source, target))
                pending.append((next_node, current_depth + 1))

        nodes = tuple(
            LineageNode(id=node, table=node, depth=node_depth[node])
            for node in sorted(node_depth, key=lambda item: (node_depth[item], item))
        )
        edges = tuple(
            LineageGraphEdge(source=source, target=target)
            for source, target in sorted(edge_pairs)
        )
        return LineageQueryResult(nodes=nodes, edges=edges, truncated=truncated)

    def _neighbors(
        self,
        current: str,
        scope: _QueryScope,
        direction: LineageDirection,
    ) -> tuple[tuple[str, str], ...]:
        if direction is LineageDirection.DOWNSTREAM:
            raw_edges = self._edge_reader.read_outgoing_edges(
                environment=scope.environment,
                source_table=current,
                source_profile=scope.source_profile,
            )
        else:
            raw_edges = self._edge_reader.read_incoming_edges(
                environment=scope.environment,
                target_table=current,
                source_profile=scope.source_profile,
            )

        pairs: set[tuple[str, str]] = set()
        for edge in raw_edges:
            if not isinstance(edge, LineageEdge):
                raise TypeError("edge_reader must return LineageEdge values")
            if edge.environment != scope.environment:
                continue
            if (
                scope.source_profile is not None
                and edge.source_profile != scope.source_profile
            ):
                continue
            source = _normalize_table(edge.source_table)
            target = _normalize_table(edge.target_table)
            if (direction is LineageDirection.DOWNSTREAM and source == current) or (
                direction is LineageDirection.UPSTREAM and target == current
            ):
                pairs.add((source, target))
        return tuple(sorted(pairs))


def query_lineage(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    direction: LineageDirection | str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
) -> LineageQueryResult:
    """函数式 query facade，便于脚本和 adapter 测试使用。"""

    # pi-lens-ignore: python-sql-injection
    return LineageQueryService(edge_reader).query(
        table,
        environment,
        direction,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
    )


def query_upstream(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
) -> LineageQueryResult:
    return LineageQueryService(edge_reader).query_upstream(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
    )


def query_downstream(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
) -> LineageQueryResult:
    return LineageQueryService(edge_reader).query_downstream(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
    )


def analyze_blast_radius(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
) -> BlastRadiusResult:
    return LineageQueryService(edge_reader).analyze_blast_radius(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
    )


def _build_scope(environment: str, source_profile: str | None) -> _QueryScope:
    resolved_environment = _required_text(environment, "environment")
    if source_profile is None:
        resolved_profile = None
    else:
        resolved_profile = _required_text(source_profile, "source_profile")
    return _QueryScope(resolved_environment, resolved_profile)


def _normalize_table(table: str) -> str:
    text = _required_text(table, "table")
    normalized = normalize_asset_name(text)
    if not normalized:
        raise ValueError("table must contain a non-empty asset name")
    return normalized


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _validate_limit(value: int, field_name: str, *, allow_zero: bool) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0 or (value == 0 and not allow_zero):
        minimum = 0 if allow_zero else 1
        raise ValueError(f"{field_name} must be >= {minimum}")
    return value


def _resolve_direction(value: LineageDirection | str) -> LineageDirection:
    try:
        return LineageDirection(value)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(direction.value for direction in LineageDirection)
        raise ValueError(f"direction must be one of: {valid}") from exc


__all__ = [
    "DEFAULT_QUERY_DEPTH",
    "DEFAULT_QUERY_MAX_NODES",
    "BlastRadiusResult",
    "LineageDirection",
    "LineageEdgeReader",
    "LineageGraphEdge",
    "LineageNode",
    "LineageQueryResult",
    "LineageQueryService",
    "analyze_blast_radius",
    "query_downstream",
    "query_lineage",
    "query_upstream",
]
