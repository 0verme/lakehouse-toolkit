"""Bounded lineage query service and Viewer/domain graph projection.

The query layer consumes materialized lineage facts only.  It does not call
providers, parse program SQL, or build a Physical DAG.  Storage adapters may
expose a request scope so one Web query can reuse one DWS connection.
"""

from __future__ import annotations

import inspect
import json
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from time import perf_counter
from typing import Any, Protocol

from .domain import LineageEdge, canonicalize_dataset_name

DEFAULT_QUERY_DEPTH = 7
DEFAULT_QUERY_MAX_NODES = 300


class LineageDirection(str, Enum):
    """查询方向；领域边始终保持 ``source -> target``。"""

    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"


class LineageView(str, Enum):
    """正式 DWS lineage projection consumed by the query service."""

    BUSINESS = "business"
    PHYSICAL = "physical"


@dataclass(slots=True)
class LineageQueryTiming:
    """One bounded query's observability counters.

    ``render_preparation_ms`` is filled by a presentation adapter; the other
    fields are populated by the query service and a request-scoped reader.
    """

    connection_ms: int = 0
    active_batch_resolve_ms: int = 0
    edge_query_ms: int = 0
    edge_rows: int = 0
    traversal_ms: int = 0
    projection_ms: int = 0
    render_preparation_ms: int = 0
    total_ms: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "connection_ms": self.connection_ms,
            "active_batch_resolve_ms": self.active_batch_resolve_ms,
            "edge_query_ms": self.edge_query_ms,
            "edge_rows": self.edge_rows,
            "traversal_ms": self.traversal_ms,
            "projection_ms": self.projection_ms,
            "render_preparation_ms": self.render_preparation_ms,
            "total_ms": self.total_ms,
        }


@dataclass(frozen=True, slots=True)
class LineageReadEdge:
    """Storage-neutral edge projection usable by physical and business views.

    Unlike :class:`LineageEdge`, this read-side value object deliberately does
    not infer business/temporary semantics.  That lets a physical DWS edge
    retain an explicit temporary endpoint or a pre-business evidence endpoint
    without leaking DWS row classes into the query or UI layers.
    """

    environment: str
    source_profile: str
    source_table: str
    target_table: str
    batch_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("environment", "source_profile"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        for field_name in ("source_table", "target_table"):
            value = canonicalize_dataset_name(getattr(self, field_name))
            if value is None:
                raise ValueError(
                    f"{field_name} must be a qualified schema.table dataset reference"
                )
            object.__setattr__(self, field_name, value)
        if self.batch_id is not None:
            if not isinstance(self.batch_id, str) or not self.batch_id.strip():
                raise ValueError("batch_id must be a non-empty string or None")
            object.__setattr__(self, "batch_id", self.batch_id.strip())


class LineageEdgeReader(Protocol):
    """Read active neighbors from one explicit lineage projection.

    ``view`` and ``timing`` are optional additions to the original #8 reader
    contract.  Query Service keeps a compatibility path for older readers that
    do not declare them; new DWS readers should implement both and
    ``request_scope``.
    """

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> Iterable[LineageEdge | LineageReadEdge]:
        """返回 scope 内以 ``source_table`` 为 source 的 active edges。"""
        return ()

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> Iterable[LineageEdge | LineageReadEdge]:
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
    """Domain graph result with a stable three-field Viewer projection.

    The legacy ``to_viewer_dict`` contract intentionally remains exactly
    ``nodes`` / ``edges`` / ``truncated``.  Explorer metadata is available as
    typed fields and through ``to_explorer_dict``.
    """

    nodes: tuple[LineageNode, ...] = ()
    edges: tuple[LineageGraphEdge, ...] = ()
    truncated: bool = False
    root: str | None = None
    environment: str | None = None
    direction: LineageDirection | None = None
    view: LineageView = LineageView.BUSINESS
    depth: int | None = None
    max_nodes: int | None = None
    root_found: bool | None = None
    batch_id: str | None = None
    timing: LineageQueryTiming | None = None

    def __post_init__(self) -> None:
        nodes = tuple(self.nodes)
        edges = tuple(self.edges)
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be a boolean")
        if any(not isinstance(node, LineageNode) for node in nodes):
            raise TypeError("nodes must contain LineageNode values")
        if any(not isinstance(edge, LineageGraphEdge) for edge in edges):
            raise TypeError("edges must contain LineageGraphEdge values")
        if self.direction is not None:
            object.__setattr__(self, "direction", _resolve_direction(self.direction))
        object.__setattr__(self, "view", _resolve_view(self.view))
        if self.root_found is not None and not isinstance(self.root_found, bool):
            raise TypeError("root_found must be a boolean or None")
        if self.batch_id is not None and (
            not isinstance(self.batch_id, str) or not self.batch_id.strip()
        ):
            raise ValueError("batch_id must be a non-empty string or None")
        if self.timing is not None and not isinstance(self.timing, LineageQueryTiming):
            raise TypeError("timing must be LineageQueryTiming or None")
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

    @property
    def max_depth(self) -> int:
        """Return the maximum returned node depth, excluding an empty graph."""

        return max((node.depth for node in self.nodes), default=0)

    def to_viewer_dict(self) -> dict[str, object]:
        """返回不含 dataclass repr 或 SQLite row 的稳定 JSON-compatible dict。"""

        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
            "truncated": self.truncated,
        }

    as_dict = to_viewer_dict

    def to_explorer_dict(self) -> dict[str, object]:
        """Return the Viewer graph plus bounded Explorer metadata."""

        payload = self.to_viewer_dict()
        payload.update(
            {
                "root": self.root,
                "environment": self.environment,
                "direction": None if self.direction is None else self.direction.value,
                "view": self.view.value,
                "depth": self.depth,
                "max_nodes": self.max_nodes,
                "max_depth": self.max_depth,
                "root_found": self.root_found,
                "batch_id": self.batch_id,
                "timing": None if self.timing is None else self.timing.as_dict(),
            }
        )
        return payload

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
    """基于 materialized edge reader 的统一 bounded BFS 查询服务。"""

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
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> LineageQueryResult:
        """按固定 depth/max_nodes 语义执行一个或两个方向的 bounded BFS。"""

        scope = _build_scope(environment, source_profile)
        root = _normalize_table(table)
        resolved_direction = _resolve_direction(direction)
        resolved_view = _resolve_view(view)
        resolved_depth = _validate_limit(depth, "depth", allow_zero=True)
        resolved_max_nodes = _validate_limit(max_nodes, "max_nodes", allow_zero=False)
        return self._run_query(
            root,
            scope,
            resolved_direction,
            depth=resolved_depth,
            max_nodes=resolved_max_nodes,
            view=resolved_view,
            timing=timing,
        )

    def query_upstream(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> LineageQueryResult:
        return self.query(
            table,
            environment,
            LineageDirection.UPSTREAM,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
            view=view,
            timing=timing,
        )

    def query_downstream(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> LineageQueryResult:
        return self.query(
            table,
            environment,
            LineageDirection.DOWNSTREAM,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
            view=view,
            timing=timing,
        )

    def query_both(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> LineageQueryResult:
        return self.query(
            table,
            environment,
            LineageDirection.BOTH,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
            view=view,
            timing=timing,
        )

    def analyze_blast_radius(
        self,
        table: str,
        environment: str,
        *,
        depth: int = DEFAULT_QUERY_DEPTH,
        max_nodes: int = DEFAULT_QUERY_MAX_NODES,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> BlastRadiusResult:
        """消费同一 downstream traversal，计算 root 之外的唯一影响资产。"""

        resolved_root = _normalize_table(table)
        result = self.query_downstream(
            resolved_root,
            environment,
            depth=depth,
            max_nodes=max_nodes,
            source_profile=source_profile,
            view=view,
            timing=timing,
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

    def _run_query(
        self,
        root: str,
        scope: _QueryScope,
        direction: LineageDirection,
        *,
        depth: int,
        max_nodes: int,
        view: LineageView,
        timing: LineageQueryTiming | None,
    ) -> LineageQueryResult:
        started = perf_counter()
        supplied_timing = timing is not None
        query_timing = timing or LineageQueryTiming()
        with _reader_request_scope(self._edge_reader, query_timing) as reader:
            if direction is LineageDirection.BOTH:
                upstream = self._traverse(
                    root,
                    scope,
                    LineageDirection.UPSTREAM,
                    depth=depth,
                    max_nodes=max_nodes,
                    view=view,
                    reader=reader,
                    timing=query_timing,
                )
                downstream = self._traverse(
                    root,
                    scope,
                    LineageDirection.DOWNSTREAM,
                    depth=depth,
                    max_nodes=max_nodes,
                    view=view,
                    reader=reader,
                    timing=query_timing,
                )
                result = self._merge_both(
                    upstream,
                    downstream,
                    root=root,
                    scope=scope,
                    depth=depth,
                    max_nodes=max_nodes,
                    view=view,
                    timing=query_timing,
                )
            else:
                result = self._traverse(
                    root,
                    scope,
                    direction,
                    depth=depth,
                    max_nodes=max_nodes,
                    view=view,
                    reader=reader,
                    timing=query_timing,
                )
        query_timing.total_ms = int((perf_counter() - started) * 1000)
        if not supplied_timing:
            # Timing is opt-in so old query callers remain value-deterministic.
            result = _replace_result(result, timing=None)
        return result

    def _traverse(
        self,
        root: str,
        scope: _QueryScope,
        direction: LineageDirection,
        *,
        depth: int,
        max_nodes: int,
        view: LineageView,
        reader: Any,
        timing: LineageQueryTiming,
    ) -> LineageQueryResult:
        started = perf_counter()
        first_neighbors = self._neighbors(
            root, scope, direction, view=view, reader=reader, timing=timing
        )
        root_found: bool | None = None
        if not first_neighbors:
            opposite = (
                LineageDirection.UPSTREAM
                if direction is LineageDirection.DOWNSTREAM
                else LineageDirection.DOWNSTREAM
            )
            opposite_neighbors = self._neighbors(
                root, scope, opposite, view=view, reader=reader, timing=timing
            )
            if not opposite_neighbors:
                root_found = self._contains_node(
                    root, scope, view=view, reader=reader, timing=timing
                )
                if root_found is not True:
                    timing.traversal_ms += int((perf_counter() - started) * 1000)
                    return self._result(
                        root,
                        scope,
                        direction,
                        view,
                        depth,
                        max_nodes,
                        root_found=root_found,
                        reader=reader,
                        timing=timing,
                    )

        node_depth: dict[str, int] = {root: 0}
        edge_pairs: set[tuple[str, str]] = set()
        pending: deque[tuple[str, int]] = deque([(root, 0)])
        truncated = False

        while pending:
            current, current_depth = pending.popleft()
            neighbors = (
                first_neighbors
                if current == root
                else self._neighbors(
                    current,
                    scope,
                    direction,
                    view=view,
                    reader=reader,
                    timing=timing,
                )
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

        projection_started = perf_counter()
        nodes = tuple(
            LineageNode(id=node, table=node, depth=node_depth[node])
            for node in sorted(node_depth, key=lambda item: (node_depth[item], item))
        )
        edges = tuple(
            LineageGraphEdge(source=source, target=target)
            for source, target in sorted(edge_pairs)
        )
        timing.projection_ms += int((perf_counter() - projection_started) * 1000)
        timing.traversal_ms += int((perf_counter() - started) * 1000)
        return self._result(
            root,
            scope,
            direction,
            view,
            depth,
            max_nodes,
            nodes=nodes,
            edges=edges,
            truncated=truncated,
            root_found=True if root_found is None else root_found,
            reader=reader,
            timing=timing,
        )

    def _merge_both(
        self,
        upstream: LineageQueryResult,
        downstream: LineageQueryResult,
        *,
        root: str,
        scope: _QueryScope,
        depth: int,
        max_nodes: int,
        view: LineageView,
        timing: LineageQueryTiming,
    ) -> LineageQueryResult:
        found_values = [
            value for value in (upstream.root_found, downstream.root_found) if value is not None
        ]
        root_found: bool | None
        if any(value is True for value in found_values):
            root_found = True
        elif found_values and all(value is False for value in found_values):
            root_found = False
        else:
            root_found = None

        has_returned_nodes = bool(upstream.nodes or downstream.nodes)
        node_depth: dict[str, int] = (
            {}
            if root_found is False or (root_found is None and not has_returned_nodes)
            else {root: 0}
        )
        for result in (upstream, downstream):
            for node in result.nodes:
                node_depth[node.id] = min(node_depth.get(node.id, node.depth), node.depth)
        truncated = upstream.truncated or downstream.truncated
        ordered_ids = sorted(
            node_depth,
            key=lambda node_id: (node_depth[node_id], node_id),
        )
        if len(ordered_ids) > max_nodes:
            ordered_ids = ordered_ids[:max_nodes]
            allowed = set(ordered_ids)
            node_depth = {node_id: node_depth[node_id] for node_id in ordered_ids}
            truncated = True
        allowed = set(node_depth)
        edge_pairs = {
            (edge.source, edge.target)
            for result in (upstream, downstream)
            for edge in result.edges
            if edge.source in allowed and edge.target in allowed
        }
        projection_started = perf_counter()
        nodes = tuple(
            LineageNode(id=node_id, table=node_id, depth=node_depth[node_id])
            for node_id in ordered_ids
        )
        edges = tuple(
            LineageGraphEdge(source=source, target=target)
            for source, target in sorted(edge_pairs)
        )
        timing.projection_ms += int((perf_counter() - projection_started) * 1000)
        return LineageQueryResult(
            nodes=nodes,
            edges=edges,
            truncated=truncated,
            root=root,
            environment=scope.environment,
            direction=LineageDirection.BOTH,
            view=view,
            depth=depth,
            max_nodes=max_nodes,
            root_found=root_found,
            batch_id=upstream.batch_id or downstream.batch_id,
            timing=timing,
        )

    def _neighbors(
        self,
        current: str,
        scope: _QueryScope,
        direction: LineageDirection,
        *,
        view: LineageView,
        reader: Any,
        timing: LineageQueryTiming,
    ) -> tuple[tuple[str, str], ...]:
        if direction is LineageDirection.DOWNSTREAM:
            method = getattr(reader, "read_outgoing_edges")
            kwargs = {
                "environment": scope.environment,
                "source_table": current,
                "source_profile": scope.source_profile,
            }
        else:
            method = getattr(reader, "read_incoming_edges")
            kwargs = {
                "environment": scope.environment,
                "target_table": current,
                "source_profile": scope.source_profile,
            }
        raw_edges = _call_reader_method(method, kwargs, view=view, timing=timing)

        pairs: set[tuple[str, str]] = set()
        for edge in raw_edges:
            if not isinstance(edge, (LineageEdge, LineageReadEdge)):
                raise TypeError(
                    "edge_reader must return LineageEdge or LineageReadEdge values"
                )
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

    def _contains_node(
        self,
        table: str,
        scope: _QueryScope,
        *,
        view: LineageView,
        reader: Any,
        timing: LineageQueryTiming,
    ) -> bool | None:
        method = getattr(reader, "contains_node", None)
        if not callable(method):
            return None
        value = _call_reader_method(
            method,
            {
                "environment": scope.environment,
                "table": table,
                "source_profile": scope.source_profile,
            },
            view=view,
            timing=timing,
        )
        if not isinstance(value, bool):
            raise TypeError("edge_reader.contains_node must return bool")
        return value

    @staticmethod
    def _result(
        root: str,
        scope: _QueryScope,
        direction: LineageDirection,
        view: LineageView,
        depth: int,
        max_nodes: int,
        *,
        nodes: tuple[LineageNode, ...] = (),
        edges: tuple[LineageGraphEdge, ...] = (),
        truncated: bool = False,
        root_found: bool | None = None,
        reader: Any,
        timing: LineageQueryTiming,
    ) -> LineageQueryResult:
        return LineageQueryResult(
            nodes=nodes,
            edges=edges,
            truncated=truncated,
            root=root,
            environment=scope.environment,
            direction=direction,
            view=view,
            depth=depth,
            max_nodes=max_nodes,
            root_found=root_found,
            batch_id=_reader_batch_id(reader),
            timing=timing,
        )


def query_lineage(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    direction: LineageDirection | str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
    view: LineageView | str = LineageView.BUSINESS,
    timing: LineageQueryTiming | None = None,
) -> LineageQueryResult:
    """函数式 query facade，便于脚本和 adapter 测试使用。"""

    return LineageQueryService(edge_reader).query(
        table,
        environment,
        direction,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
        view=view,
        timing=timing,
    )


def query_upstream(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
    view: LineageView | str = LineageView.BUSINESS,
    timing: LineageQueryTiming | None = None,
) -> LineageQueryResult:
    return LineageQueryService(edge_reader).query_upstream(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
        view=view,
        timing=timing,
    )


def query_downstream(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
    view: LineageView | str = LineageView.BUSINESS,
    timing: LineageQueryTiming | None = None,
) -> LineageQueryResult:
    return LineageQueryService(edge_reader).query_downstream(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
        view=view,
        timing=timing,
    )


def query_both(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
    view: LineageView | str = LineageView.BUSINESS,
    timing: LineageQueryTiming | None = None,
) -> LineageQueryResult:
    return LineageQueryService(edge_reader).query_both(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
        view=view,
        timing=timing,
    )


def analyze_blast_radius(
    edge_reader: LineageEdgeReader,
    table: str,
    environment: str,
    *,
    depth: int = DEFAULT_QUERY_DEPTH,
    max_nodes: int = DEFAULT_QUERY_MAX_NODES,
    source_profile: str | None = None,
    view: LineageView | str = LineageView.BUSINESS,
    timing: LineageQueryTiming | None = None,
) -> BlastRadiusResult:
    return LineageQueryService(edge_reader).analyze_blast_radius(
        table,
        environment,
        depth=depth,
        max_nodes=max_nodes,
        source_profile=source_profile,
        view=view,
        timing=timing,
    )


@contextmanager
def _reader_request_scope(
    reader: Any,
    timing: LineageQueryTiming,
) -> Iterator[Any]:
    request_scope = getattr(reader, "request_scope", None)
    if not callable(request_scope):
        yield reader
        return
    try:
        parameters = inspect.signature(request_scope).parameters
    except (TypeError, ValueError):
        parameters = {}
    kwargs = {"timing": timing} if "timing" in parameters else {}
    with request_scope(**kwargs) as scoped_reader:
        if scoped_reader is None:
            raise RuntimeError("edge_reader.request_scope returned no reader")
        yield scoped_reader


def _call_reader_method(
    method: Any,
    kwargs: dict[str, object],
    *,
    view: LineageView,
    timing: LineageQueryTiming,
) -> Any:
    """Call old and new reader implementations without hiding inner errors."""

    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        parameters = {}
    accepts_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    supports_view = accepts_kwargs or "view" in parameters
    if view is LineageView.PHYSICAL and not supports_view:
        raise TypeError("edge_reader must support the explicit physical view")
    if supports_view:
        kwargs["view"] = view
    if accepts_kwargs or "timing" in parameters:
        kwargs["timing"] = timing
    return method(**kwargs)


def _reader_batch_id(reader: Any) -> str | None:
    value = getattr(reader, "active_batch_id", None)
    if callable(value):
        return None
    return value if isinstance(value, str) and value.strip() else None


def _replace_result(
    result: LineageQueryResult,
    *,
    timing: LineageQueryTiming | None,
) -> LineageQueryResult:
    return LineageQueryResult(
        nodes=result.nodes,
        edges=result.edges,
        truncated=result.truncated,
        root=result.root,
        environment=result.environment,
        direction=result.direction,
        view=result.view,
        depth=result.depth,
        max_nodes=result.max_nodes,
        root_found=result.root_found,
        batch_id=result.batch_id,
        timing=timing,
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
    normalized = canonicalize_dataset_name(text)
    if normalized is None:
        raise ValueError("table must be a qualified schema.table dataset reference")
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


def _resolve_view(value: LineageView | str) -> LineageView:
    try:
        return LineageView(value)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(view.value for view in LineageView)
        raise ValueError(f"view must be one of: {valid}") from exc


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
    "LineageQueryTiming",
    "LineageReadEdge",
    "LineageView",
    "analyze_blast_radius",
    "query_both",
    "query_downstream",
    "query_lineage",
    "query_upstream",
]
