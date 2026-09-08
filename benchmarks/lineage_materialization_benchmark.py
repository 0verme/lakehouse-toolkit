"""Synthetic scaling benchmark for bounded lineage materialization evidence.

The benchmark is deterministic and uses only fictional DEMO assets.  It fails on
canonicalization work that grows beyond a linear per-path budget rather than on a
machine-dependent wall-clock threshold.
"""

from __future__ import annotations

import json
import platform
import sys
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

PATH_COUNTS = (10, 100, 500, 1000)
MAX_CANONICALIZATIONS_PER_PATH = 64
OBSERVED_AT = datetime(2026, 1, 5, tzinfo=timezone.utc)


def build_synthetic_dag(path_count: int) -> ProgramPhysicalDAG:
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


def benchmark_case(path_count: int) -> dict[str, Any]:
    dag = build_synthetic_dag(path_count)
    audit = audit_program_physical_dag(
        dag,
        observed_at=OBSERVED_AT,
        batch_id="batch-benchmark",
    )
    original_canonical = materialization._canonical_json
    canonicalization_calls = 0

    def counted_canonical(value: object) -> str:
        nonlocal canonicalization_calls
        canonicalization_calls += 1
        return original_canonical(value)

    materialization._canonical_json = counted_canonical
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
        materialization._canonical_json = original_canonical

    if len(result.edges) != 1:
        raise AssertionError("benchmark must produce one formal lineage edge")
    evidence = result.edges[0].evidence
    if not isinstance(evidence, dict):
        raise AssertionError("benchmark evidence must be a JSON object")
    if evidence["path_count"] != path_count:
        raise AssertionError("benchmark lost physical path count")
    if canonicalization_calls > path_count * MAX_CANONICALIZATIONS_PER_PATH:
        raise AssertionError(
            "canonicalization work exceeded the linear scaling budget: "
            f"{canonicalization_calls} calls for {path_count} paths"
        )
    output_bytes = len(
        json.dumps(
            evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return {
        "paths": path_count,
        "elapsed_ms": round(elapsed_ms, 2),
        "canonicalization_calls": canonicalization_calls,
        "peak_evidence_count": len(evidence["physical_paths"]),
        "path_count": evidence["path_count"],
        "physical_paths_truncated": evidence["physical_paths_truncated"],
        "output_bytes": output_bytes,
    }


def main() -> int:
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {platform.platform()}")
    print(
        f"Linear canonicalization budget: {MAX_CANONICALIZATIONS_PER_PATH} calls/path"
    )
    for path_count in PATH_COUNTS:
        print(json.dumps(benchmark_case(path_count), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
