from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import shared.lineage.materialization as materialization
import shared.lineage.materialization_sqlite as sqlite_materialization
from shared.lineage.audit import audit_program_physical_dag
from shared.lineage.domain import LineageEdge
from shared.lineage.materialization import MaterializationBatch, materialize_batch
from shared.lineage.materialization_sqlite import (
    SQLiteMaterializationStore,
    SQLitePublishMetrics,
)
from tests.fixtures.lineage.pathological_materialization import make_parallel_tmp_dag

OBSERVED_AT = datetime(2026, 1, 5, tzinfo=timezone.utc)


def audit_dag(dag):
    return audit_program_physical_dag(
        dag,
        observed_at=OBSERVED_AT,
        batch_id="batch-test",
    )


def build_batch(edge_count: int) -> MaterializationBatch:
    return MaterializationBatch(
        batch_id=f"batch-serialization-{edge_count}",
        observed_at=OBSERVED_AT,
        edges=tuple(
            LineageEdge(
                environment="DEV",
                source_profile="benchmark",
                source_table=f"ODS.DEMO_SOURCE_{index:06d}",
                target_table=f"DWA.DEMO_TARGET_{index:06d}",
                program_name=f"PROGRAM_DEMO_{index:06d}",
                evidence={"edge_index": index, "path_count": 1},
            )
            for index in range(edge_count)
        ),
    )


class EvidencePreparationTests(unittest.TestCase):
    def test_bounded_simple_keys_do_not_use_json_dumps(self):
        values = materialization._BoundedValues(cap=2)
        with patch.object(
            materialization.json,
            "dumps",
            side_effect=AssertionError("simple bounded key was serialized"),
        ):
            values.add_safe(("ODS.DEMO_B", "TMP.DEMO_B"))
            values.add_safe(("ODS.DEMO_A", "TMP.DEMO_A"))
            values.add_safe(("ODS.DEMO_C", "TMP.DEMO_C"))
            selected = values.sorted_values()

        self.assertEqual(
            selected,
            [("ODS.DEMO_A", "TMP.DEMO_A"), ("ODS.DEMO_B", "TMP.DEMO_B")],
        )
        self.assertTrue(values.truncated)

    def test_bounded_values_reuse_cached_key_for_final_sort(self):
        values = materialization._BoundedValues(cap=1)
        values.add_safe({"b": 2, "a": 1})

        with patch.object(
            materialization,
            "_canonical_json_safe",
            side_effect=AssertionError("cached bounded key was serialized again"),
        ):
            self.assertEqual(values.sorted_values(), [{"b": 2, "a": 1}])

    def test_materialization_batch_finalization_consumes_safe_evidence(self):
        dag = make_parallel_tmp_dag(3)
        metrics = materialization._MaterializationMetrics()

        with materialization._capture_metrics(metrics):
            result = materialize_batch(
                [audit_dag(dag)],
                batch_id="batch-safe-finalize",
                observed_at=OBSERVED_AT,
            )

        evidence = result.edges[0].evidence
        self.assertIsInstance(evidence, dict)
        self.assertEqual(evidence["path_count"], 3)
        self.assertEqual(metrics.accumulator_add_evidence_calls, 0)
        self.assertGreaterEqual(metrics.batch_finalize_ms, 0)
        self.assertGreater(metrics.canonical_json_safe_calls, 0)


class SQLiteEvidencePreparationTests(unittest.TestCase):
    def test_publish_serializes_each_evidence_once_and_reuses_rows(self):
        batch = build_batch(3)
        metrics = SQLitePublishMetrics()
        canonical_calls = 0
        original_canonical = sqlite_materialization._canonical_json

        def counted_canonical(value):
            nonlocal canonical_calls
            canonical_calls += 1
            return original_canonical(value)

        with patch.object(
            sqlite_materialization,
            "_canonical_json",
            side_effect=counted_canonical,
        ) as canonical:
            with patch.object(
                materialization,
                "_json_safe",
                wraps=materialization._json_safe,
            ) as json_safe:
                with TemporaryDirectory() as directory:
                    store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
                    result = store.publish(batch, instrumentation=metrics)
                    self.assertEqual(store.get_active_batch_id(), batch.batch_id)

        self.assertEqual(result.edge_count, 3)
        self.assertEqual(canonical.call_count, 3)
        self.assertEqual(canonical_calls, 3)
        self.assertEqual(json_safe.call_count, 3)
        self.assertEqual(metrics.evidence_serialization_calls, 3)
        self.assertEqual(metrics.prepared_edge_rows, 3)
        self.assertEqual(metrics.validated_edge_rows, 3)

    def test_publish_metrics_split_storage_stages_without_wall_clock_assertions(self):
        batch = build_batch(2)
        metrics = SQLitePublishMetrics()

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(batch, instrumentation=metrics)

        for value in (
            metrics.prepare_ms,
            metrics.insert_ms,
            metrics.validate_ms,
            metrics.active_switch_ms,
            metrics.commit_ms,
        ):
            self.assertGreaterEqual(value, 0)
        self.assertEqual(metrics.prepared_edge_rows, 2)
        self.assertEqual(metrics.validated_edge_rows, 2)

    def test_instrumented_publish_failure_rolls_back_active_switch(self):
        first_batch = build_batch(1)
        second_batch = build_batch(2)
        metrics = SQLitePublishMetrics()

        def fail_after_switch(stage: str) -> None:
            if stage == "after_active_switch":
                raise RuntimeError("injected instrumented publish failure")

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(first_batch)
            with self.assertRaisesRegex(
                RuntimeError, "injected instrumented publish failure"
            ):
                store.publish(
                    second_batch,
                    stage_hook=fail_after_switch,
                    instrumentation=metrics,
                )
            self.assertEqual(store.get_active_batch_id(), first_batch.batch_id)
            self.assertEqual(store.read_edges(batch_id=second_batch.batch_id), ())
            self.assertEqual(metrics.prepared_edge_rows, 2)
            self.assertEqual(metrics.validated_edge_rows, 2)

    def test_simple_canonical_key_matches_json_semantics(self):
        for value in (
            None,
            True,
            False,
            0,
            -12,
            "ODS.DEMO_A",
            ("ODS.DEMO_A", "TMP.DEMO_A"),
            ["ODS.DEMO_A", "TMP.DEMO_A"],
        ):
            with self.subTest(value=value):
                expected = json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self.assertEqual(materialization._canonical_json_safe(value), expected)


if __name__ == "__main__":
    unittest.main()
