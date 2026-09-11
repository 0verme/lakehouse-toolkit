from __future__ import annotations

import io
import sqlite3
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from jobs.crontab import imp_lineage_edge
from jobs.crontab.imp_lineage_edge import materialize_sources
from shared.lineage import (
    LINEAGE_PIPELINE_VERSION,
    PipelineVersionMigrationRequired,
    SQLiteMaterializationStore,
)
from shared.lineage.incremental import SnapshotScope
from tests.fixtures.lineage.phase7_evolution import source

OBSERVED_AT = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)
STALE_PIPELINE_VERSION = "lineage-pipeline-v8-program-target-hint-selection"


class PipelineVersionMigrationPreflightTests(unittest.TestCase):
    @staticmethod
    def _sources():
        return (
            source(
                "PROGRAM_MIGRATION_A",
                source_profile="profile_a",
            ),
            source(
                "PROGRAM_MIGRATION_B",
                source_profile="profile_b",
            ),
        )

    @staticmethod
    def _scopes():
        return (
            SnapshotScope("DEV", "profile_a"),
            SnapshotScope("DEV", "profile_b"),
        )

    def _seed_snapshot(
        self,
        db_path: Path,
        *,
        stale_profiles: tuple[str, ...] = (),
    ) -> tuple:
        sources = self._sources()
        materialize_sources(
            sources,
            db_path=db_path,
            batch_id="batch-initial",
            observed_at=OBSERVED_AT,
            complete_snapshot=True,
            snapshot_scopes=self._scopes(),
        )
        if stale_profiles:
            placeholders = ",".join("?" for _ in stale_profiles)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "UPDATE lineage_program_state "
                    "SET pipeline_version = ? "
                    "WHERE is_active = 1 AND source_profile IN ("
                    f"{placeholders})",
                    (STALE_PIPELINE_VERSION, *stale_profiles),
                )
                connection.commit()
        return sources

    @staticmethod
    def _active_states(db_path: Path):
        store = SQLiteMaterializationStore(db_path)
        try:
            return {
                state.source_profile: state
                for state in store.read_program_states(active_only=True)
            }
        finally:
            store.close()

    @staticmethod
    def _active_program_names(db_path: Path):
        store = SQLiteMaterializationStore(db_path)
        try:
            return {
                edge.program_name
                for edge in store.read_edges(active_only=True)
            }
        finally:
            store.close()

    def test_partial_scoped_migration_fails_before_physical_dag_build(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_a", "profile_b"),
            )
            output = io.StringIO()
            with (
                redirect_stdout(output),
                redirect_stderr(output),
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    wraps=imp_lineage_edge.build_program_physical_dag,
                ) as builder,
                self.assertRaises(PipelineVersionMigrationRequired) as raised,
            ):
                materialize_sources(
                    [sources[0]],
                    db_path=db_path,
                    batch_id="batch-rejected",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=(self._scopes()[0],),
                    selected_profiles=("profile_a",),
                )

            builder.assert_not_called()
            self.assertEqual(raised.exception.stale_program_count, 2)
            self.assertEqual(
                raised.exception.stale_profiles,
                ("profile_a", "profile_b"),
            )
            self.assertIn(
                "current=lineage-pipeline-v10-sql-relation-context",
                str(raised.exception),
            )
            self.assertIn("stale_profiles=profile_a,profile_b", str(raised.exception))
            logs = output.getvalue()
            self.assertIn(
                "stage=pipeline_migration_preflight status=FAILED",
                logs,
            )
            self.assertIn("stale_programs=2", logs)
            self.assertNotIn("stage=build status=STARTED", logs)
            self.assertNotIn("stage=publish status=STARTED", logs)
            store = SQLiteMaterializationStore(db_path)
            try:
                self.assertEqual(store.get_active_batch_id(), "batch-initial")
                self.assertIsNone(store.get_batch_metadata("batch-rejected"))
            finally:
                store.close()

    def test_all_stale_scopes_can_migrate_in_one_complete_snapshot(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_a", "profile_b"),
            )
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                wraps=imp_lineage_edge.build_program_physical_dag,
            ) as builder:
                result = materialize_sources(
                    sources,
                    db_path=db_path,
                    batch_id="batch-migration",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=self._scopes(),
                    selected_profiles=("profile_a", "profile_b"),
                )

            self.assertEqual(builder.call_count, 2)
            self.assertEqual(result.batch_id, "batch-migration")
            states = self._active_states(db_path)
            self.assertEqual(set(states), {"profile_a", "profile_b"})
            self.assertTrue(
                all(
                    state.pipeline_version == LINEAGE_PIPELINE_VERSION
                    for state in states.values()
                )
            )

    def test_unified_version_keeps_other_profile_in_scoped_replay(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(db_path)
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                wraps=imp_lineage_edge.build_program_physical_dag,
            ) as builder:
                materialize_sources(
                    [sources[0]],
                    db_path=db_path,
                    batch_id="batch-scoped",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=(self._scopes()[0],),
                    selected_profiles=("profile_a",),
                )

            builder.assert_not_called()
            states = self._active_states(db_path)
            self.assertEqual(set(states), {"profile_a", "profile_b"})
            self.assertEqual(states["profile_b"].batch_id, "batch-scoped")
            self.assertTrue(
                all(
                    state.pipeline_version == LINEAGE_PIPELINE_VERSION
                    for state in states.values()
                )
            )
            self.assertEqual(
                self._active_program_names(db_path),
                {program_source.program_name for program_source in sources},
            )

    def test_selected_stale_scope_can_migrate_while_current_scope_is_retained(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_a",),
            )
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                wraps=imp_lineage_edge.build_program_physical_dag,
            ) as builder:
                materialize_sources(
                    [sources[0]],
                    db_path=db_path,
                    batch_id="batch-single-stale-scope",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=(self._scopes()[0],),
                    selected_profiles=("profile_a",),
                )

            builder.assert_called_once()
            states = self._active_states(db_path)
            self.assertEqual(set(states), {"profile_a", "profile_b"})
            self.assertTrue(
                all(
                    state.pipeline_version == LINEAGE_PIPELINE_VERSION
                    for state in states.values()
                )
            )

    def test_unselected_stale_scope_still_fails_fast(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_b",),
            )
            with (
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    wraps=imp_lineage_edge.build_program_physical_dag,
                ) as builder,
                self.assertRaises(PipelineVersionMigrationRequired),
            ):
                materialize_sources(
                    [sources[0]],
                    db_path=db_path,
                    batch_id="batch-rejected-stale-b",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=(self._scopes()[0],),
                    selected_profiles=("profile_a",),
                )

            builder.assert_not_called()

    def test_limit_cannot_obtain_pipeline_migration_authority(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_a", "profile_b"),
            )
            with (
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    wraps=imp_lineage_edge.build_program_physical_dag,
                ) as builder,
                self.assertRaises(PipelineVersionMigrationRequired),
            ):
                materialize_sources(
                    sources,
                    db_path=db_path,
                    batch_id="batch-rejected-limit",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=self._scopes(),
                    selected_profiles=("profile_a", "profile_b"),
                    limit=20,
                )

            builder.assert_not_called()

    def test_force_rebuild_cannot_bypass_migration_scope_safety(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            sources = self._seed_snapshot(
                db_path,
                stale_profiles=("profile_a", "profile_b"),
            )
            with (
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    wraps=imp_lineage_edge.build_program_physical_dag,
                ) as builder,
                self.assertRaises(PipelineVersionMigrationRequired),
            ):
                materialize_sources(
                    [sources[0]],
                    db_path=db_path,
                    batch_id="batch-rejected-force",
                    observed_at=OBSERVED_AT + timedelta(days=1),
                    complete_snapshot=True,
                    snapshot_scopes=(self._scopes()[0],),
                    selected_profiles=("profile_a",),
                    force_rebuild=True,
                )

            builder.assert_not_called()

    def test_first_run_without_previous_active_state_is_allowed(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            program_source = self._sources()[0]
            result = materialize_sources(
                [program_source],
                db_path=db_path,
                batch_id="batch-first-run",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                snapshot_scopes=(self._scopes()[0],),
                selected_profiles=("profile_a",),
            )

            self.assertEqual(result.batch_id, "batch-first-run")
            states = self._active_states(db_path)
            self.assertEqual(set(states), {"profile_a"})
            self.assertEqual(
                states["profile_a"].pipeline_version,
                LINEAGE_PIPELINE_VERSION,
            )


if __name__ == "__main__":
    unittest.main()
