"""公开 synthetic benchmark for Phase 6 indexed-neighbor BFS.

This is an architecture-evidence script, not a CI timing gate. It generates only
fictional ``DEMO`` assets and never reads a production database.
"""

from __future__ import annotations

import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.lineage import (  # noqa: E402
    LineageEdge,
    LineageQueryService,
    MaterializationBatch,
    SQLiteMaterializationStore,
)

DEPTH = 7
MAX_NODES = 300
RUNS = 3


def node_name(index: int) -> str:
    return f"DWM.DEMO_NODE_{index:05d}"


def build_synthetic_edges(edge_count: int) -> tuple[LineageEdge, ...]:
    if edge_count < 0:
        raise ValueError("edge_count must be non-negative")
    edges: list[LineageEdge] = []
    for child_index in range(1, edge_count + 1):
        parent_index = (child_index - 1) // 2
        source = "ODS.DEMO_ROOT" if parent_index == 0 else node_name(parent_index)
        target = node_name(child_index)
        edges.append(
            LineageEdge(
                environment="DEV",
                source_profile="benchmark",
                source_table=source,
                target_table=target,
                program_name=f"PROGRAM_DEMO_BENCH_{child_index:05d}",
                evidence_type="synthetic_benchmark",
                evidence={"edge_index": child_index},
            )
        )
    return tuple(edges)


def benchmark(edge_count: int) -> float:
    store = SQLiteMaterializationStore(":memory:")
    store.publish(
        MaterializationBatch(
            batch_id=f"batch-benchmark-{edge_count}",
            observed_at=datetime.now(timezone.utc),
            edges=build_synthetic_edges(edge_count),
        )
    )
    service = LineageQueryService(store)
    start = perf_counter()
    for _ in range(RUNS):
        result = service.query_downstream(
            "ODS.DEMO_ROOT",
            "DEV",
            depth=DEPTH,
            max_nodes=MAX_NODES,
        )
        if not result.nodes or len(result.nodes) > MAX_NODES:
            raise AssertionError("benchmark query violated max_nodes contract")
    return (perf_counter() - start) / RUNS


def main() -> int:
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {platform.platform()}")
    for edge_count in (1_000, 10_000):
        elapsed = benchmark(edge_count)
        print(f"Edges: {edge_count}")
        print("Query: downstream from ODS.DEMO_ROOT")
        print(f"Depth: {DEPTH}")
        print(f"Max nodes: {MAX_NODES}")
        print(f"Runs: {RUNS}")
        print(f"Approx elapsed: {elapsed:.6f}s/run")
        print("Closure required: NO")
    print(
        "Hardware/environment caveat: local synthetic SQLite run; timing is indicative, not a CI gate."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
