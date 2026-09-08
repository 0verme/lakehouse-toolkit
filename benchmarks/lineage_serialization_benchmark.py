"""Synthetic scaling benchmark for lineage evidence and SQLite publish.

The benchmark uses only fictional DEMO assets. It reports aggregate stage timing
and operation counts for 1k, 10k, and 100k edge candidates; it has no wall-clock
CI threshold.
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
from shared.lineage.domain import LineageEdge  # noqa: E402
from shared.lineage.materialization import MaterializationBatch  # noqa: E402
from shared.lineage.materialization_sqlite import (  # noqa: E402
    SQLiteMaterializationStore,
    SQLitePublishMetrics,
)

EDGE_COUNTS = (1_000, 10_000, 100_000)
OBSERVED_AT = datetime(2026, 1, 5, tzinfo=timezone.utc)


def build_batch(edge_count: int) -> MaterializationBatch:
    if isinstance(edge_count, bool) or edge_count < 1:
        raise ValueError("edge_count must be a positive integer")
    edges = tuple(
        LineageEdge(
            environment="DEV",
            source_profile="benchmark",
            source_table=f"ODS.DEMO_SOURCE_{index:06d}",
            target_table=f"DWA.DEMO_TARGET_{index:06d}",
            program_name=f"PROGRAM_DEMO_{index:06d}",
            evidence={
                "path_count": 1,
                "physical_edge_pairs": [
                    [
                        f"ODS.DEMO_SOURCE_{index:06d}",
                        f"DWA.DEMO_TARGET_{index:06d}",
                    ]
                ],
                "physical_paths": [
                    {
                        "nodes": [
                            f"ODS.DEMO_SOURCE_{index:06d}",
                            f"DWA.DEMO_TARGET_{index:06d}",
                        ],
                        "physical_edge_pairs": [
                            [
                                f"ODS.DEMO_SOURCE_{index:06d}",
                                f"DWA.DEMO_TARGET_{index:06d}",
                            ]
                        ],
                    }
                ],
                "statement_indices": [index],
            },
        )
        for index in range(edge_count)
    )
    return MaterializationBatch(
        batch_id=f"batch-serialization-benchmark-{edge_count}",
        observed_at=OBSERVED_AT,
        edges=edges,
    )


def benchmark(edge_count: int) -> dict[str, Any]:
    build_started_at = perf_counter()
    batch = build_batch(edge_count)
    batch_build_ms = (perf_counter() - build_started_at) * 1000
    materialization_metrics = materialization._MaterializationMetrics()
    sqlite_metrics = SQLitePublishMetrics()
    store = SQLiteMaterializationStore(":memory:")

    publish_started_at = perf_counter()
    with materialization._capture_metrics(materialization_metrics):
        result = store.publish(batch, instrumentation=sqlite_metrics)
    publish_ms = (perf_counter() - publish_started_at) * 1000

    if result.edge_count != edge_count:
        raise AssertionError("publish result edge count changed")
    if store.get_active_batch_id() != batch.batch_id:
        raise AssertionError("benchmark batch is not active")
    if sqlite_metrics.prepared_edge_rows != edge_count:
        raise AssertionError("prepared edge count changed")
    if sqlite_metrics.validated_edge_rows != edge_count:
        raise AssertionError("validated edge count changed")
    if sqlite_metrics.evidence_serialization_calls != edge_count:
        raise AssertionError("evidence serialization was not prepared once")

    return {
        "edge_candidates": edge_count,
        "batch_build_ms": round(batch_build_ms, 2),
        "publish_ms": round(publish_ms, 2),
        "materialization_json_safe_calls": materialization_metrics.json_safe_calls,
        "materialization_canonical_json_calls": materialization_metrics.canonical_json_calls,
        "materialization_canonical_json_safe_calls": materialization_metrics.canonical_json_safe_calls,
        "materialization_json_dumps_calls": materialization_metrics.json_dumps_calls,
        "sqlite_prepare_ms": sqlite_metrics.prepare_ms,
        "sqlite_insert_ms": sqlite_metrics.insert_ms,
        "sqlite_validate_ms": sqlite_metrics.validate_ms,
        "sqlite_active_switch_ms": sqlite_metrics.active_switch_ms,
        "sqlite_commit_ms": sqlite_metrics.commit_ms,
        "sqlite_evidence_serialization_calls": sqlite_metrics.evidence_serialization_calls,
        "sqlite_prepared_edge_rows": sqlite_metrics.prepared_edge_rows,
        "sqlite_validated_edge_rows": sqlite_metrics.validated_edge_rows,
    }


def main() -> int:
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {platform.platform()}")
    print("Synthetic DEMO edge candidates; timing is indicative, not a CI gate.")
    for edge_count in EDGE_COUNTS:
        print(json.dumps(benchmark(edge_count), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
