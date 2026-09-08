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


__all__ = ["make_diamond_dag", "make_parallel_tmp_dag"]
