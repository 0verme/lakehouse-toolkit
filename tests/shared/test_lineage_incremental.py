from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from jobs.crontab import imp_lineage_edge
from shared.lineage import SQLiteMaterializationStore
from shared.lineage.domain import ProgramIdentity, ProgramSource, ProgramState
from shared.lineage.incremental import (  # pyright: ignore[reportMissingImports]
    IncrementalStatus,
    SnapshotScope,
    plan_incremental,
)
from tests.fixtures.lineage.phase7_evolution import (  # pyright: ignore[reportMissingImports]
    VALID_PROGRAM,
    source,
)

OBSERVED_AT = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)


class IncrementalPlannerTests(unittest.TestCase):
    def test_classifies_new_unchanged_changed_and_deleted(self):
        unchanged = source("PROGRAM_DEMO_UNCHANGED")
        changed = source("PROGRAM_DEMO_CHANGED", source_hash="hash-old")
        deleted = source("PROGRAM_DEMO_DELETED")
        previous = tuple(
            ProgramState.from_source(
                item, observed_at=OBSERVED_AT, batch_id="batch-old"
            )
            for item in (unchanged, changed, deleted)
        )
        current_changed = source("PROGRAM_DEMO_CHANGED", source_hash="hash-new")
        new = source("PROGRAM_DEMO_NEW")

        plan = plan_incremental(
            [current_changed, unchanged, new],
            previous,
            complete_snapshot=True,
            snapshot_scopes=[SnapshotScope("DEV", "fixture")],
        )

        self.assertEqual([item.program_name for item in plan.new], ["PROGRAM_DEMO_NEW"])
        self.assertEqual(
            [item.program_name for item in plan.unchanged],
            ["PROGRAM_DEMO_UNCHANGED"],
        )
        self.assertEqual(
            [item.program_name for item in plan.changed], ["PROGRAM_DEMO_CHANGED"]
        )
        self.assertEqual(
            [item.program_name for item in plan.deleted], ["PROGRAM_DEMO_DELETED"]
        )
        self.assertEqual(
            plan.status_for(ProgramIdentity("DEV", "fixture", "PROGRAM_DEMO_NEW")),
            IncrementalStatus.NEW,
        )

    def test_missing_hash_is_conservatively_rebuild_required(self):
        old = source("PROGRAM_DEMO_HASH")
        current = ProgramSource(
            environment=old.environment,
            source_profile=old.source_profile,
            program_name=old.program_name,
            script_code=old.script_code,
            expected_target=old.expected_target,
            source_hash=None,
        )
        previous = (
            ProgramState.from_source(
                old, observed_at=OBSERVED_AT, batch_id="batch-old"
            ),
        )

        plan = plan_incremental([current], previous)

        self.assertEqual(plan.new, ())
        self.assertEqual(plan.unchanged, ())
        self.assertEqual(plan.changed, (current,))

    def test_incomplete_snapshot_does_not_mark_missing_program_deleted(self):
        old = source("PROGRAM_DEMO_PARTIAL")
        previous = (
            ProgramState.from_source(
                old, observed_at=OBSERVED_AT, batch_id="batch-old"
            ),
        )

        plan = plan_incremental([source("PROGRAM_DEMO_OTHER")], previous)

        self.assertEqual(plan.deleted, ())

    def test_duplicate_identity_fails_validation(self):
        duplicate_a = source("PROGRAM_DEMO_DUPLICATE")
        duplicate_b = source("PROGRAM_DEMO_DUPLICATE", source_hash="different")

        with self.assertRaisesRegex(ValueError, "duplicate program identity"):
            plan_incremental([duplicate_a, duplicate_b])

    def test_plan_order_is_deterministic(self):
        values = [source(f"PROGRAM_DEMO_{index}") for index in range(5)]

        first = plan_incremental(values)
        second = plan_incremental(reversed(values))

        self.assertEqual(first, second)


class IncrementalExecutionTests(unittest.TestCase):
    def test_unchanged_program_skips_physical_dag_builder(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            first = source("PROGRAM_DEMO_SKIP")
            imp_lineage_edge.materialize_sources(
                [first],
                db_path=db_path,
                batch_id="batch-skip-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                wraps=imp_lineage_edge.build_program_physical_dag,
            ) as builder:
                imp_lineage_edge.materialize_sources(
                    [first],
                    db_path=db_path,
                    batch_id="batch-skip-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                    complete_snapshot=True,
                )
                builder.assert_not_called()

            store = SQLiteMaterializationStore(db_path)
            self.assertEqual(store.get_active_batch_id(), "batch-skip-2")
            self.assertEqual(
                {edge.program_name for edge in store.read_edges(active_only=True)},
                {"PROGRAM_DEMO_SKIP"},
            )

    def test_mixed_complete_snapshot_reuses_unchanged_and_removes_deleted(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            initial = [source(f"PROGRAM_DEMO_{index:02d}") for index in range(10)]
            imp_lineage_edge.materialize_sources(
                initial,
                db_path=db_path,
                batch_id="batch-mixed-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            current = [source(f"PROGRAM_DEMO_{index:02d}") for index in range(5)]
            current.extend(
                source(
                    f"PROGRAM_DEMO_{index:02d}",
                    script_code=VALID_PROGRAM.replace("DEMO_A", "DEMO_B"),
                )
                for index in (5, 6)
            )
            current.extend(source(f"PROGRAM_DEMO_NEW_{index:02d}") for index in (1, 2))
            imp_lineage_edge.materialize_sources(
                current,
                db_path=db_path,
                batch_id="batch-mixed-2",
                observed_at=OBSERVED_AT.replace(day=2),
                complete_snapshot=True,
            )

            store = SQLiteMaterializationStore(db_path)
            active_names = {
                state.program_name
                for state in store.read_program_states(active_only=True)
            }
            self.assertEqual(
                active_names,
                {
                    *(f"PROGRAM_DEMO_{index:02d}" for index in range(7)),
                    "PROGRAM_DEMO_NEW_01",
                    "PROGRAM_DEMO_NEW_02",
                },
            )
            self.assertNotIn("PROGRAM_DEMO_07", active_names)
            self.assertNotIn("PROGRAM_DEMO_09", active_names)
            self.assertEqual(store.get_active_batch_id(), "batch-mixed-2")
            self.assertEqual(len(store.read_edges(batch_id="batch-mixed-1")), 10)

    def test_rebuild_failure_keeps_previous_active_batch(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            old = source("PROGRAM_DEMO_ROLLBACK")
            imp_lineage_edge.materialize_sources(
                [old],
                db_path=db_path,
                batch_id="batch-rollback-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            changed = source(
                "PROGRAM_DEMO_ROLLBACK",
                script_code=VALID_PROGRAM.replace("DEMO_A", "DEMO_C"),
            )
            real_builder = imp_lineage_edge.build_program_physical_dag

            def fail(_program_source):
                raise RuntimeError("synthetic rebuild failure")

            with (
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    side_effect=fail,
                ),
                self.assertRaisesRegex(RuntimeError, "synthetic rebuild failure"),
            ):
                imp_lineage_edge.materialize_sources(
                    [changed],
                    db_path=db_path,
                    batch_id="batch-rollback-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                    complete_snapshot=True,
                )

            self.assertIsNotNone(real_builder)
            store = SQLiteMaterializationStore(db_path)
            self.assertEqual(store.get_active_batch_id(), "batch-rollback-1")
            self.assertEqual(store.read_edges(batch_id="batch-rollback-2"), ())

    def test_complete_snapshot_deletes_program_but_keeps_historical_fact(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            first = source("PROGRAM_DEMO_DELETE")
            imp_lineage_edge.materialize_sources(
                [first],
                db_path=db_path,
                batch_id="batch-delete-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            imp_lineage_edge.materialize_sources(
                [],
                db_path=db_path,
                batch_id="batch-delete-2",
                observed_at=OBSERVED_AT.replace(day=2),
                complete_snapshot=True,
                snapshot_scopes=[SnapshotScope("DEV", "fixture")],
            )

            store = SQLiteMaterializationStore(db_path)
            self.assertEqual(store.read_edges(active_only=True), ())
            self.assertEqual(store.read_program_states(active_only=True), ())
            self.assertEqual(len(store.read_edges(batch_id="batch-delete-1")), 1)
            self.assertFalse(store.list_batches()[0].is_active)


if __name__ == "__main__":
    unittest.main()
