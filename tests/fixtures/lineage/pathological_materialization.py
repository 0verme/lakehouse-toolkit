"""Synthetic Physical DAGs for bounded materialization regression tests."""

from __future__ import annotations

from shared.lineage.domain import (
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramSource,
)
from shared.lineage.physical_dag import ProgramPhysicalDAG


def make_parallel_tmp_dag(path_count: int) -> ProgramPhysicalDAG:
    """Create ``path_count`` distinct TMP routes for one formal edge."""

    if isinstance(path_count, bool) or path_count < 1:
        raise ValueError("path_count must be a positive integer")
    target = "DWA.DEMO_RESULT"
    source = ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name="DEMO_PATHOLOGICAL_MATERIALIZATION",
        script_code="",
        expected_target=target,
        source_hash="sha256:demo-pathological-materialization",
    )
    nodes = [PhysicalNode("ODS.DEMO_SOURCE", "ODS.DEMO_SOURCE")]
    edges: list[PhysicalEdge] = []
    for index in range(path_count):
        temporary = f"TMP_BRANCH_{index:04d}"
        nodes.append(
            PhysicalNode(
                temporary,
                temporary,
                PhysicalNodeKind.TEMPORARY_ASSET,
            )
        )
        edges.extend(
            (
                PhysicalEdge(
                    "ODS.DEMO_SOURCE",
                    temporary,
                    evidence={"statement_index": index * 2},
                ),
                PhysicalEdge(
                    temporary,
                    target,
                    evidence={"statement_index": index * 2 + 1},
                ),
            )
        )
    nodes.append(PhysicalNode(target, target))
    return ProgramPhysicalDAG(
        program_source=source,
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target,),
        expected_target=target,
    )


def _structural_source(program_name: str, target: str) -> ProgramSource:
    return ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name=program_name,
        script_code="",
        expected_target=target,
        source_hash=f"sha256:{program_name.lower()}",
    )


def make_linear_tmp_dag(chain_length: int) -> ProgramPhysicalDAG:
    """Create one formal edge through a linear TMP chain."""

    if isinstance(chain_length, bool) or chain_length < 1:
        raise ValueError("chain_length must be a positive integer")
    source_name = "ODS.DEMO_LINEAR_SOURCE"
    target_name = "DWA.DEMO_LINEAR_RESULT"
    temporary_names = [f"TMP_LINEAR_{index:03d}" for index in range(chain_length)]
    nodes = [PhysicalNode(source_name, source_name)]
    nodes.extend(
        PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
        for name in temporary_names
    )
    nodes.append(PhysicalNode(target_name, target_name))
    route = [source_name, *temporary_names, target_name]
    edges = [
        PhysicalEdge(
            predecessor,
            successor,
            evidence={"statement_index": index},
        )
        for index, (predecessor, successor) in enumerate(zip(route, route[1:]))
    ]
    return ProgramPhysicalDAG(
        program_source=_structural_source("DEMO_LINEAR_MATERIALIZATION", target_name),
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target_name,),
        expected_target=target_name,
    )


def make_high_fanout_dag(branch_count: int) -> ProgramPhysicalDAG:
    """Create one source with many TMP branches converging on one target."""

    if isinstance(branch_count, bool) or branch_count < 1:
        raise ValueError("branch_count must be a positive integer")
    source_name = "ODS.DEMO_FANOUT_SOURCE"
    target_name = "DWA.DEMO_FANOUT_RESULT"
    branch_names = [f"TMP_FANOUT_{index:03d}" for index in range(branch_count)]
    nodes = [PhysicalNode(source_name, source_name)]
    nodes.extend(
        PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
        for name in branch_names
    )
    nodes.append(PhysicalNode(target_name, target_name))
    edges = [
        PhysicalEdge(
            source_name,
            branch,
            evidence={"statement_index": index},
        )
        for index, branch in enumerate(branch_names)
    ]
    edges.extend(
        PhysicalEdge(
            branch,
            target_name,
            evidence={"statement_index": branch_count + index},
        )
        for index, branch in enumerate(branch_names)
    )
    return ProgramPhysicalDAG(
        program_source=_structural_source(
            "DEMO_HIGH_FANOUT_MATERIALIZATION", target_name
        ),
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target_name,),
        expected_target=target_name,
    )


def make_high_fanin_dag(source_count: int) -> ProgramPhysicalDAG:
    """Create many formal sources feeding one TMP merge and one target."""

    if isinstance(source_count, bool) or source_count < 1:
        raise ValueError("source_count must be a positive integer")
    target_name = "DWA.DEMO_FANIN_RESULT"
    merge_name = "TMP_FANIN_MERGE"
    source_names = [
        f"ODS.DEMO_FANIN_SOURCE_{index:03d}" for index in range(source_count)
    ]
    nodes = [PhysicalNode(name, name) for name in source_names]
    nodes.append(PhysicalNode(merge_name, merge_name, PhysicalNodeKind.TEMPORARY_ASSET))
    nodes.append(PhysicalNode(target_name, target_name))
    edges = [
        PhysicalEdge(
            source,
            merge_name,
            evidence={"statement_index": index},
        )
        for index, source in enumerate(source_names)
    ]
    edges.append(
        PhysicalEdge(
            merge_name,
            target_name,
            evidence={"statement_index": source_count},
        )
    )
    return ProgramPhysicalDAG(
        program_source=_structural_source(
            "DEMO_HIGH_FANIN_MATERIALIZATION", target_name
        ),
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target_name,),
        expected_target=target_name,
    )


def make_mixed_pathological_dag() -> ProgramPhysicalDAG:
    """Create a ~150-node/350-edge mixed graph with 20,480 collapsed paths.

    The reachable component combines a long linear TMP chain, a high fan-out
    entry, a dense branch/merge diamond core, and a high fan-in result.  A
    disconnected synthetic edge-density component keeps the total graph close
    to the observed production shape without changing the reachable path
    contract used by the benchmark.
    """

    source_name = "ODS.DEMO_MIXED_SOURCE"
    target_name = "DWA.DEMO_MIXED_RESULT"
    padding_names = [f"TMP_MIXED_LINEAR_{index:03d}" for index in range(80)]
    layer_widths = (4, 4, 4, 4, 4, 20)
    nodes = [PhysicalNode(source_name, source_name)]
    nodes.extend(
        PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
        for name in padding_names
    )
    edges: list[PhysicalEdge] = []
    route = [source_name, *padding_names]
    edges.extend(
        PhysicalEdge(
            predecessor,
            successor,
            evidence={"statement_index": index},
        )
        for index, (predecessor, successor) in enumerate(zip(route, route[1:]))
    )

    previous_layer = [padding_names[-1]]
    for stage, width in enumerate(layer_widths):
        current_layer = [
            f"TMP_MIXED_STAGE_{stage:02d}_{branch:02d}" for branch in range(width)
        ]
        nodes.extend(
            PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
            for name in current_layer
        )
        edges.extend(
            PhysicalEdge(
                predecessor,
                successor,
                evidence={"statement_index": stage + 80},
            )
            for predecessor in previous_layer
            for successor in current_layer
        )
        previous_layer = current_layer

    nodes.append(PhysicalNode(target_name, target_name))
    edges.extend(
        PhysicalEdge(
            predecessor,
            target_name,
            evidence={"statement_index": len(layer_widths) + 80},
        )
        for predecessor in previous_layer
    )

    orphan_left = [f"TMP_MIXED_ORPHAN_LEFT_{index:02d}" for index in range(10)]
    orphan_right = [f"TMP_MIXED_ORPHAN_RIGHT_{index:02d}" for index in range(10)]
    orphan_isolated = [f"TMP_MIXED_ORPHAN_ISOLATED_{index:02d}" for index in range(12)]
    nodes.extend(
        PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
        for name in [*orphan_left, *orphan_right, *orphan_isolated]
    )
    edges.extend(
        PhysicalEdge(
            left,
            right,
            evidence={"statement_index": 100 + index},
        )
        for index, (left, right) in enumerate(
            (left, right) for left in orphan_left for right in orphan_right
        )
    )
    edges.extend(
        (
            PhysicalEdge(
                orphan_isolated[0],
                orphan_isolated[1],
                evidence={"statement_index": 200},
            ),
            PhysicalEdge(
                orphan_isolated[1],
                orphan_isolated[2],
                evidence={"statement_index": 201},
            ),
        )
    )

    return ProgramPhysicalDAG(
        program_source=_structural_source(
            "DEMO_MIXED_PATHOLOGICAL_MATERIALIZATION", target_name
        ),
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target_name,),
        expected_target=target_name,
    )


def make_dense_pathological_dag() -> ProgramPhysicalDAG:
    """Create a 43-node/407-edge dense DAG that exceeds the path guard.

    The graph has unique physical edge pairs, a 36-way formal fan-out, a
    39-way formal fan-in at the result, and deterministic extra forward edges.
    It is intentionally small in node count but has enough branch/merge density
    to exercise exact DP counting instead of the old path-count failure mode.
    """

    source_name = "ODS.DEMO_DENSE_SOURCE"
    target_name = "DWA.DEMO_DENSE_RESULT"
    temporary_names = [f"TMP_DENSE_{index:02d}" for index in range(41)]
    nodes = [PhysicalNode(source_name, source_name)]
    nodes.extend(
        PhysicalNode(name, name, PhysicalNodeKind.TEMPORARY_ASSET)
        for name in temporary_names
    )
    nodes.append(PhysicalNode(target_name, target_name))

    edges: list[PhysicalEdge] = []
    edge_pairs: set[tuple[str, str]] = set()

    def add_edge(source: str, target: str) -> None:
        if (source, target) in edge_pairs:
            raise AssertionError("dense fixture generated a duplicate edge pair")
        edge_pairs.add((source, target))
        edges.append(
            PhysicalEdge(
                source,
                target,
                evidence={"statement_index": len(edges)},
            )
        )

    for temporary in temporary_names[:36]:
        add_edge(source_name, temporary)
    for index in range(34):
        add_edge(temporary_names[index], temporary_names[index + 1])
    for temporary in temporary_names[2:]:
        add_edge(temporary, target_name)

    # Keep the degree shape aligned with the anonymized dense-DAG evidence:
    # the source fans out to 36 TMP nodes, the result has 39 TMP predecessors,
    # and only the source plus the first 34 TMP nodes are branch nodes.  Dense
    # extra edges target TMP_DENSE_01..TMP_DENSE_35 and originate only in
    # TMP_DENSE_00..TMP_DENSE_33; five reachability edges connect the remaining
    # TMP nodes without making them branches, preserving 35 branch and 36 merge
    # nodes.
    extra_pairs = [
        (temporary_names[source_index], temporary_names[target_index])
        for source_index in range(34)
        for target_index in range(source_index + 2, 36)
    ]
    selected_extra_pairs = [
        (temporary_names[source_index], temporary_names[source_index + 2])
        for source_index in range(34)
    ]
    selected_extra_pairs.extend(
        pair for pair in extra_pairs if pair not in selected_extra_pairs
    )
    for source, target in selected_extra_pairs:
        if len(edges) >= 402:
            break
        add_edge(source, target)
    for source, target in zip(temporary_names[29:34], temporary_names[36:41]):
        add_edge(source, target)

    if len(edges) != 407:
        raise AssertionError(f"dense fixture has {len(edges)} edges, expected 407")

    return ProgramPhysicalDAG(
        program_source=_structural_source(
            "DEMO_DENSE_PATHOLOGICAL_MATERIALIZATION", target_name
        ),
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target_name,),
        expected_target=target_name,
    )


def make_diamond_dag(stage_count: int) -> ProgramPhysicalDAG:
    """Create a deterministic TMP diamond with ``2 ** stage_count`` paths."""

    if isinstance(stage_count, bool) or stage_count < 1:
        raise ValueError("stage_count must be a positive integer")
    target = "DWA.DEMO_RESULT"
    source = ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name="DEMO_DIAMOND_MATERIALIZATION",
        script_code="",
        expected_target=target,
        source_hash="sha256:demo-diamond-materialization",
    )
    nodes = [PhysicalNode("ODS.DEMO_SOURCE", "ODS.DEMO_SOURCE")]
    edges: list[PhysicalEdge] = []
    previous = ["ODS.DEMO_SOURCE"]
    for stage in range(stage_count):
        current = [f"TMP_DIAMOND_{stage:02d}_{branch}" for branch in ("A", "B")]
        nodes.extend(
            PhysicalNode(node, node, PhysicalNodeKind.TEMPORARY_ASSET)
            for node in current
        )
        edges.extend(
            PhysicalEdge(
                predecessor,
                successor,
                evidence={"statement_index": stage},
            )
            for predecessor in previous
            for successor in current
        )
        previous = current
    nodes.append(PhysicalNode(target, target))
    edges.extend(
        PhysicalEdge(node, target, evidence={"statement_index": stage_count})
        for node in previous
    )
    return ProgramPhysicalDAG(
        program_source=source,
        nodes=tuple(nodes),
        edges=tuple(edges),
        steps=(),
        sinks=(target,),
        expected_target=target,
    )


__all__ = [
    "make_dense_pathological_dag",
    "make_diamond_dag",
    "make_high_fanin_dag",
    "make_high_fanout_dag",
    "make_linear_tmp_dag",
    "make_mixed_pathological_dag",
    "make_parallel_tmp_dag",
]
