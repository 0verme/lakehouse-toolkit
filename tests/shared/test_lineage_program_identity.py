from __future__ import annotations

import json
import unittest
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from jobs.crontab.imp_lineage_edge import materialize_sources
from shared.lineage import LINEAGE_PIPELINE_VERSION, SQLiteMaterializationStore
from shared.lineage.domain import LineageEdge, ProgramIdentity, ProgramState
from shared.lineage.incremental import (
    IncrementalStatus,
    SnapshotScope,
    build_program_states,
    plan_incremental,
)
from tests.fixtures.lineage.phase7_evolution import VALID_PROGRAM, source


OBSERVED_AT = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)


class ProgramIdentityContractTests(unittest.TestCase):
    def test_identity_has_only_canonical_three_fields_and_stable_payload(self):
        identity = ProgramIdentity(" DEV ", " profile_a ", " DEMO_JOB ")

        self.assertEqual(
            {field.name for field in fields(ProgramIdentity)},
            {"environment", "source_profile", "program_name"},
        )
        self.assertEqual(identity.key, ("DEV", "profile_a", "DEMO_JOB"))
        self.assertEqual(identity.scope, ("DEV", "profile_a"))
        self.assertEqual(
            identity.to_dict(),
            {
                "environment": "DEV",
                "source_profile": "profile_a",
                "program_name": "DEMO_JOB",
            },
        )
        self.assertEqual(
            json.dumps(
                identity.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            '{"environment":"DEV","program_name":"DEMO_JOB","source_profile":"profile_a"}',
        )

    def test_identity_dimensions_are_independent_and_case_is_preserved(self):
        canonical = ProgramIdentity("DEV", "profile_a", "DEMO_JOB")

        self.assertEqual(ProgramIdentity("DEV", "profile_a", "DEMO_JOB"), canonical)
        self.assertNotEqual(ProgramIdentity("PROD", "profile_a", "DEMO_JOB"), canonical)
        self.assertNotEqual(ProgramIdentity("DEV", "profile_b", "DEMO_JOB"), canonical)
        self.assertNotEqual(ProgramIdentity("DEV", "profile_a", "OTHER_JOB"), canonical)
        self.assertNotEqual(ProgramIdentity("dev", "profile_a", "DEMO_JOB"), canonical)

    def test_source_hash_is_not_program_identity(self):
        profile_a = source("DEMO_JOB", source_profile="profile_a")
        profile_b = source("DEMO_JOB", source_profile="profile_b")

        self.assertNotEqual(profile_a.identity, profile_b.identity)
        self.assertEqual(profile_a.source_hash, profile_b.source_hash)
        self.assertEqual(
            profile_a.identity, ProgramIdentity("DEV", "profile_a", "DEMO_JOB")
        )

    def test_source_and_state_expose_the_same_static_identity(self):
        program_source = source("DEMO_JOB")
        program_state = ProgramState.from_source(
            program_source,
            observed_at=OBSERVED_AT,
            batch_id="batch-identity-1",
        )

        same_program_in_next_batch = ProgramState.from_source(
            program_source,
            observed_at=OBSERVED_AT + timedelta(days=1),
            batch_id="batch-identity-2",
        )

        self.assertEqual(program_source.identity, program_state.identity)
        self.assertEqual(program_state.identity, same_program_in_next_batch.identity)
        self.assertNotEqual(program_state.batch_id, same_program_in_next_batch.batch_id)


class ProgramIdentityPlannerTests(unittest.TestCase):
    def test_same_identity_same_hash_and_pipeline_version_is_unchanged(self):
        current = source("DEMO_JOB_STABLE")
        previous = ProgramState.from_source(
            current,
            observed_at=OBSERVED_AT - timedelta(days=1),
            batch_id="batch-old",
        )

        plan = plan_incremental(
            [current],
            [previous],
            pipeline_version=LINEAGE_PIPELINE_VERSION,
        )

        self.assertEqual(plan.status_for(current.identity), IncrementalStatus.UNCHANGED)
        next_states = build_program_states(
            plan,
            [previous],
            observed_at=OBSERVED_AT,
            batch_id="batch-new",
        )

        self.assertEqual(plan.unchanged, (current,))
        self.assertEqual(plan.changed, ())
        self.assertEqual(next_states[0].identity, current.identity)
        self.assertEqual(next_states[0].source_hash, current.source_hash)
        self.assertEqual(next_states[0].first_seen_at, previous.first_seen_at)
        self.assertEqual(next_states[0].last_seen_at, OBSERVED_AT)
        self.assertEqual(next_states[0].last_changed_at, previous.last_changed_at)
        self.assertEqual(next_states[0].batch_id, "batch-new")

    def test_same_identity_changed_hash_is_changed(self):
        previous_source = source("DEMO_JOB_CHANGED")
        current = source(
            "DEMO_JOB_CHANGED",
            script_code=VALID_PROGRAM.replace("DEMO_A", "DEMO_B"),
        )
        previous = ProgramState.from_source(
            previous_source,
            observed_at=OBSERVED_AT,
            batch_id="batch-old",
        )

        self.assertEqual(previous.identity, current.identity)
        self.assertNotEqual(previous.source_hash, current.source_hash)
        plan = plan_incremental([current], [previous])

        self.assertEqual(plan.status_for(current.identity), IncrementalStatus.CHANGED)
        self.assertEqual(plan.unchanged, ())
        self.assertEqual(plan.changed, (current,))

    def test_same_identity_same_hash_pipeline_version_bump_requires_rebuild(self):
        current = source("DEMO_JOB_PIPELINE_BUMP")
        previous = ProgramState.from_source(
            current,
            observed_at=OBSERVED_AT,
            batch_id="batch-old",
            pipeline_version=LINEAGE_PIPELINE_VERSION,
        )

        plan = plan_incremental(
            [current],
            [previous],
            pipeline_version="lineage-pipeline-v2",
        )
        states = build_program_states(
            plan,
            [previous],
            observed_at=OBSERVED_AT + timedelta(days=1),
            batch_id="batch-new",
        )

        self.assertEqual(previous.source_hash, current.source_hash)
        self.assertEqual(plan.status_for(current.identity), IncrementalStatus.CHANGED)
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0].identity, current.identity)
        self.assertEqual(states[0].pipeline_version, "lineage-pipeline-v2")
        self.assertEqual(states[0].first_seen_at, OBSERVED_AT)
        self.assertEqual(states[0].last_seen_at, OBSERVED_AT + timedelta(days=1))
        self.assertEqual(states[0].last_changed_at, OBSERVED_AT + timedelta(days=1))

    def test_rename_is_old_deleted_and_new_identity_without_alias_inference(self):
        old_source = source("OLD_PROGRAM")
        new_source = source("NEW_PROGRAM")
        old_state = ProgramState.from_source(
            old_source,
            observed_at=OBSERVED_AT,
            batch_id="batch-old",
        )

        plan = plan_incremental(
            [new_source],
            [old_state],
            complete_snapshot=True,
            snapshot_scopes=[SnapshotScope("DEV", "fixture")],
        )
        candidate_states = build_program_states(
            plan,
            [old_state],
            observed_at=OBSERVED_AT + timedelta(days=1),
            batch_id="batch-new",
        )

        self.assertEqual(
            plan.status_for(old_source.identity), IncrementalStatus.DELETED
        )
        self.assertEqual(plan.status_for(new_source.identity), IncrementalStatus.NEW)
        self.assertEqual(
            [state.identity for state in candidate_states], [new_source.identity]
        )


class ProgramLifecycleContractTests(unittest.TestCase):
    def test_complete_snapshot_rename_deactivates_old_and_activates_new_identity(self):
        old_source = source("OLD_PROGRAM")
        new_source = source("NEW_PROGRAM")

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            materialize_sources(
                [old_source],
                db_path=db_path,
                batch_id="batch-rename-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            materialize_sources(
                [new_source],
                db_path=db_path,
                batch_id="batch-rename-2",
                observed_at=OBSERVED_AT + timedelta(days=1),
                complete_snapshot=True,
            )

            store = SQLiteMaterializationStore(db_path)
            active_states = store.read_program_states(active_only=True)
            old_history = store.read_program_states(batch_id="batch-rename-1")

        self.assertEqual(
            [state.identity for state in active_states], [new_source.identity]
        )
        self.assertTrue(active_states[0].is_active)
        self.assertEqual(
            active_states[0].first_seen_at, OBSERVED_AT + timedelta(days=1)
        )
        self.assertEqual(
            [state.identity for state in old_history], [old_source.identity]
        )
        self.assertFalse(old_history[0].is_active)

    def test_complete_delete_then_restore_preserves_history_and_reappears_active(self):
        program_source = source("DEMO_JOB_DELETE_RESTORE")
        deleted_at = OBSERVED_AT + timedelta(days=1)
        restored_at = OBSERVED_AT + timedelta(days=2)

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            materialize_sources(
                [program_source],
                db_path=db_path,
                batch_id="batch-delete-restore-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            materialize_sources(
                [],
                db_path=db_path,
                batch_id="batch-delete-restore-2",
                observed_at=deleted_at,
                complete_snapshot=True,
                snapshot_scopes=[SnapshotScope("DEV", "fixture")],
            )

            store = SQLiteMaterializationStore(db_path)
            deleted_history = store.read_program_states(
                batch_id="batch-delete-restore-1"
            )
            self.assertEqual(len(deleted_history), 1)
            self.assertFalse(deleted_history[0].is_active)
            self.assertEqual(store.read_program_states(active_only=True), ())

            materialize_sources(
                [program_source],
                db_path=db_path,
                batch_id="batch-delete-restore-3",
                observed_at=restored_at,
                complete_snapshot=True,
            )

            active_states = store.read_program_states(active_only=True)
            self.assertEqual(len(active_states), 1)
            self.assertTrue(active_states[0].is_active)
            self.assertEqual(active_states[0].identity, program_source.identity)
            self.assertEqual(active_states[0].batch_id, "batch-delete-restore-3")
            self.assertEqual(active_states[0].first_seen_at, restored_at)
            self.assertFalse(
                store.read_program_states(batch_id="batch-delete-restore-1")[
                    0
                ].is_active
            )
            self.assertEqual(
                len(store.read_edges(batch_id="batch-delete-restore-1")), 1
            )

    def test_partial_replay_keeps_program_not_read_in_the_snapshot(self):
        observed_program = source("DEMO_JOB_REPLAY_OBSERVED")
        omitted_program = source("DEMO_JOB_REPLAY_OMITTED")

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            materialize_sources(
                [observed_program, omitted_program],
                db_path=db_path,
                batch_id="batch-partial-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            materialize_sources(
                [observed_program],
                db_path=db_path,
                batch_id="batch-partial-2",
                observed_at=OBSERVED_AT + timedelta(days=1),
                complete_snapshot=False,
            )

            store = SQLiteMaterializationStore(db_path)
            active_states = store.read_program_states(active_only=True)
            active_names = {state.program_name for state in active_states}
            omitted_state = next(
                state
                for state in active_states
                if state.program_name == omitted_program.program_name
            )

            self.assertEqual(
                active_names,
                {observed_program.program_name, omitted_program.program_name},
            )
            self.assertEqual(omitted_state.batch_id, "batch-partial-2")
            self.assertTrue(
                any(
                    edge.program_name == omitted_program.program_name
                    for edge in store.read_edges(active_only=True)
                )
            )

    def test_job_key_is_optional_provenance_and_not_static_identity(self):
        first = LineageEdge(
            environment="DEV",
            source_profile="fixture",
            source_table="ODS.DEMO_A",
            target_table="DWM.DEMO_B",
            program_name="DEMO_JOB_PROVENANCE",
            job_key="DEMO_JOB_KEY_A",
        )
        second = LineageEdge(
            environment="DEV",
            source_profile="fixture",
            source_table="ODS.DEMO_A",
            target_table="DWM.DEMO_B",
            program_name="DEMO_JOB_PROVENANCE",
            job_key="DEMO_JOB_KEY_B",
        )

        self.assertEqual(
            ProgramIdentity(
                first.environment, first.source_profile, first.program_name
            ),
            ProgramIdentity(
                second.environment, second.source_profile, second.program_name
            ),
        )
        self.assertNotEqual(first, second)
        self.assertEqual(first.job_key, "DEMO_JOB_KEY_A")
        self.assertEqual(second.job_key, "DEMO_JOB_KEY_B")


if __name__ == "__main__":
    unittest.main()
