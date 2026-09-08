"""Synthetic scaling benchmark for bounded lineage materialization evidence.

The benchmark is deterministic and uses only fictional DEMO assets.  It reports
operation counts instead of asserting machine-dependent wall-clock thresholds.
The structural cases cover linear, diamond, fan-out, fan-in, mixed 20k-path,
and small dense path-explosion graphs.
"""

from __future__ import annotations

import json
import platform
import sys
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import shared.lineage.materialization as materialization  # noqa: E402
from shared.lineage.audit import audit_program_physical_dag  # noqa: E402
from shared.lineage.domain import (  # noqa: E402
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramSource,
)
from shared.lineage.materialization import materialize_program  # noqa: E402
from shared.lineage.physical_dag import ProgramPhysicalDAG  # noqa: E402
from tests.fixtures.lineage.pathological_materialization import (  # noqa: E402
    make_dense_pathological_dag,
    make_diamond_dag,
    make_high_fanin_dag,
    make_high_fanout_dag,
    make_linear_tmp_dag,
    make_mixed_pathological_dag,
)

PATH_COUNTS = (10, 100, 500, 1000)
MAX_CANONICALIZATION_CALLS_PER_SAMPLE = 64
OBSERVED_AT = datetime(2026, 1, 5, tzinfo=timezone.utc)
STRUCTURAL_CASES: tuple[tuple[str, Callable[[], ProgramPhysicalDAG]], ...] = (
    ("linear", lambda: make_linear_tmp_dag(40)),
    ("diamond", lambda: make_diamond_dag(12)),
    ("high-fanout", lambda: make_high_fanout_dag(36)),
    ("high-fanin", lambda: make_high_fanin_dag(39)),
    ("mixed-20k", make_mixed_pathological_dag),
    ("dense-43x407", make_dense_pathological_dag),
)


def build_synthetic_dag(path_count: int) -> ProgramPhysicalDAG:
    """Build a flat parallel fixture retained for the small scaling series."""

    if isinstance(path_count, bool) or path_count < 1:
        raise ValueError("path_count must be a positive integer")
    target = "DWA.DEMO_RESULT"
    source = ProgramSource(
        environment="DEV",
        source_profile="benchmark",
        program_name="DEMO_MATERIALIZATION_BENCHMARK",
        script_code="",
        expected_target=target,
        source_hash="sha256:demo-materialization-benchmark",
    )
    nodes = [PhysicalNode("ODS.DEMO_SOURCE", "ODS.DEMO_SOURCE")]
    edges: list[PhysicalEdge] = []
    for index in range(path_count):
        temporary = f"TMP_BENCHMARK_{index:04d}"
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


def _measure_case(name: str, dag: ProgramPhysicalDAG) -> dict[str, Any]:
    audit = audit_program_physical_dag(
        dag,
        observed_at=OBSERVED_AT,
        batch_id="batch-benchmark",
    )
    counts: dict[str, Any] = {
        "path_evidence_calls": 0,
        "edge_evidence_calls": 0,
        "lineage_edge_from_path_calls": 0,
        "temporary_lineage_edge_objects": 0,
        "accumulator_add_path_calls": 0,
        "accumulator_add_evidence_calls": 0,
        "canonicalization_calls": 0,
        "explicit_path_yields": 0,
        "explicit_path_enumeration_ms": 0.0,
        "sample_path_yields": 0,
        "retained_path_count": 0,
        "peak_retained_path_count": 0,
    }
    originals = {
        "canonical_json_safe": materialization._canonical_json_safe,
        "collapsed_paths": materialization._collapsed_paths,
        "sample_acyclic_paths": materialization._sample_acyclic_paths,
        "path_evidence": materialization._path_evidence,
        "edge_evidence": materialization._edge_evidence,
        "lineage_edge_from_path": materialization._lineage_edge_from_path,
        "add_path": materialization._EdgeEvidenceAccumulator.add_path,
        "add_evidence": materialization._EdgeEvidenceAccumulator.add_evidence,
    }

    def counted_canonical_json_safe(value: object) -> str:
        counts["canonicalization_calls"] += 1
        return originals["canonical_json_safe"](value)

    def counted_collapsed_paths(*args, **kwargs):
        iterator = originals["collapsed_paths"](*args, **kwargs)

        def observed():
            while True:
                started = perf_counter()
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                counts["explicit_path_enumeration_ms"] += (
                    perf_counter() - started
                ) * 1000
                counts["explicit_path_yields"] += 1
                counts["retained_path_count"] += 1
                counts["peak_retained_path_count"] = max(
                    counts["peak_retained_path_count"],
                    counts["retained_path_count"],
                )
                try:
                    yield item
                finally:
                    counts["retained_path_count"] -= 1

        return observed()

    def counted_sample_paths(*args, **kwargs):
        for item in originals["sample_acyclic_paths"](*args, **kwargs):
            counts["sample_path_yields"] += 1
            counts["retained_path_count"] += 1
            counts["peak_retained_path_count"] = max(
                counts["peak_retained_path_count"],
                counts["retained_path_count"],
            )
            try:
                yield item
            finally:
                counts["retained_path_count"] -= 1

    def counted_path_evidence(*args, **kwargs):
        counts["path_evidence_calls"] += 1
        return originals["path_evidence"](*args, **kwargs)

    def counted_edge_evidence(*args, **kwargs):
        counts["edge_evidence_calls"] += 1
        return originals["edge_evidence"](*args, **kwargs)

    def counted_lineage_edge_from_path(*args, **kwargs):
        counts["lineage_edge_from_path_calls"] += 1
        edge = originals["lineage_edge_from_path"](*args, **kwargs)
        counts["temporary_lineage_edge_objects"] += 1
        return edge

    def counted_add_path(self, *args, **kwargs):
        counts["accumulator_add_path_calls"] += 1
        return originals["add_path"](self, *args, **kwargs)

    def counted_add_evidence(self, *args, **kwargs):
        counts["accumulator_add_evidence_calls"] += 1
        return originals["add_evidence"](self, *args, **kwargs)

    materialization._canonical_json_safe = counted_canonical_json_safe
    materialization._collapsed_paths = counted_collapsed_paths
    materialization._sample_acyclic_paths = counted_sample_paths
    materialization._path_evidence = counted_path_evidence
    materialization._edge_evidence = counted_edge_evidence
    materialization._lineage_edge_from_path = counted_lineage_edge_from_path
    materialization._EdgeEvidenceAccumulator.add_path = counted_add_path
    materialization._EdgeEvidenceAccumulator.add_evidence = counted_add_evidence
    try:
        started_at = perf_counter()
        result = materialize_program(
            dag,
            audit,
            batch_id="batch-benchmark",
            observed_at=OBSERVED_AT,
        )
        elapsed_ms = (perf_counter() - started_at) * 1000
    finally:
        materialization._canonical_json_safe = originals["canonical_json_safe"]
        materialization._collapsed_paths = originals["collapsed_paths"]
        materialization._sample_acyclic_paths = originals["sample_acyclic_paths"]
        materialization._path_evidence = originals["path_evidence"]
        materialization._edge_evidence = originals["edge_evidence"]
        materialization._lineage_edge_from_path = originals["lineage_edge_from_path"]
        materialization._EdgeEvidenceAccumulator.add_path = originals["add_path"]
        materialization._EdgeEvidenceAccumulator.add_evidence = originals[
            "add_evidence"
        ]

    if not result.edges:
        raise AssertionError(f"{name} must produce at least one formal lineage edge")
    evidence = result.edges[0].evidence
    if not isinstance(evidence, dict):
        raise AssertionError("benchmark evidence must be a JSON object")
    path_count = evidence["path_count"]
    if not isinstance(path_count, int):
        raise AssertionError("benchmark path_count must be an integer")
    physical_paths = evidence.get("physical_paths")
    if not isinstance(physical_paths, list):
        raise AssertionError("benchmark physical_paths must be a list")
    sample_count = len(physical_paths)
    if sample_count > materialization.MAX_PHYSICAL_PATHS:
        raise AssertionError("benchmark exceeded the physical path sample cap")
    canonical_budget = (sample_count + 1) * MAX_CANONICALIZATION_CALLS_PER_SAMPLE + len(
        dag.edges
    ) * MAX_CANONICALIZATION_CALLS_PER_SAMPLE
    if counts["canonicalization_calls"] > canonical_budget:
        raise AssertionError(
            "canonicalization work exceeded the bounded sample/graph budget: "
            f"{counts['canonicalization_calls']} > {canonical_budget}"
        )
    output_bytes = len(
        json.dumps(
            evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    total_path_count = 0
    for edge in result.edges:
        if not isinstance(edge.evidence, dict):
            continue
        edge_path_count = edge.evidence.get("path_count")
        if isinstance(edge_path_count, int):
            total_path_count += edge_path_count
    outgoing = Counter(edge.source for edge in dag.edges)
    incoming = Counter(edge.target for edge in dag.edges)
    unique_edge_pairs = {(edge.source, edge.target) for edge in dag.edges}
    return {
        "case": name,
        "nodes": len(dag.nodes),
        "edges": len(dag.edges),
        "max_out_degree": max(outgoing.values(), default=0),
        "max_in_degree": max(incoming.values(), default=0),
        "branch_nodes": sum(count > 1 for count in outgoing.values()),
        "merge_nodes": sum(count > 1 for count in incoming.values()),
        "edge_pairs": len(unique_edge_pairs),
        "duplicate_edge_count": len(dag.edges) - len(unique_edge_pairs),
        "collapsed_paths": total_path_count,
        "first_edge_path_count": path_count,
        "explicit_path_yields": counts["explicit_path_yields"],
        "explicit_path_enumeration_ms": round(
            counts["explicit_path_enumeration_ms"], 2
        ),
        "materialization_ms": round(elapsed_ms, 2),
        "path_evidence_calls": counts["path_evidence_calls"],
        "edge_evidence_calls": counts["edge_evidence_calls"],
        "lineage_edge_from_path_calls": counts["lineage_edge_from_path_calls"],
        "temporary_lineage_edge_objects": counts["temporary_lineage_edge_objects"],
        "final_lineage_edges": len(result.edges),
        "accumulator_add_path_calls": counts["accumulator_add_path_calls"],
        "accumulator_add_evidence_calls": counts["accumulator_add_evidence_calls"],
        "canonicalization_calls": counts["canonicalization_calls"],
        "sample_path_yields": counts["sample_path_yields"],
        "peak_retained_path_count": counts["peak_retained_path_count"],
        "sample_count": sample_count,
        "physical_paths_truncated": evidence["physical_paths_truncated"],
        "physical_edge_pairs_truncated": evidence["physical_edge_pairs_truncated"],
        "output_bytes": output_bytes,
    }


def benchmark_case(path_count: int) -> dict[str, Any]:
    return _measure_case(f"parallel-{path_count}", build_synthetic_dag(path_count))


def main() -> int:
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {platform.platform()}")
    print("Canonicalization budget: bounded sample + physical graph summaries")
    for path_count in PATH_COUNTS:
        print(json.dumps(benchmark_case(path_count), sort_keys=True))
    for name, builder in STRUCTURAL_CASES:
        print(json.dumps(_measure_case(name, builder()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
