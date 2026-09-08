from __future__ import annotations

import io
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from jobs.crontab import imp_lineage_edge
from tests.fixtures.lineage.phase7_evolution import source

OBSERVED_AT = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)


class FixtureProvider:
    def __init__(self, sources):
        self._sources = tuple(sources)

    def iter_program_sources(self):
        yield from self._sources


def run_with_output(callable_, *args, **kwargs):
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        result = callable_(*args, **kwargs)
    return result, output.getvalue()


class LineageJobObservabilityTests(unittest.TestCase):
    def test_job_emits_stage_logs_and_flushes_successfully(self):
        program_source = source("PROGRAM_DEMO_OBSERVABILITY")
        with TemporaryDirectory() as directory:
            result, output = run_with_output(
                imp_lineage_edge.main,
                [FixtureProvider([program_source])],
                db_path=Path(directory) / "lineage.db",
                batch_id="batch-observability-1",
                observed_at=OBSERVED_AT,
                coverage_report_path=None,
            )

        self.assertEqual(result, 0)
        self.assertIn("stage=job status=STARTED providers=1", output)
        self.assertIn("stage=source_load status=SUCCESS sources=1", output)
        self.assertIn(
            "stage=incremental_plan status=SUCCESS total=1 new=1 changed=0 "
            "unchanged=0 deleted=0 rebuild=1",
            output,
        )
        self.assertIn("stage=build status=STARTED total=1", output)
        self.assertIn("stage=build status=SUCCESS processed=1", output)
        self.assertNotIn("stage=build status=RUNNING", output)
        self.assertIn("stage=publish status=STARTED", output)
        self.assertIn(
            "stage=publish status=SUCCESS batch_id=batch-observability-1",
            output,
        )
        for field in (
            "program_computation_ms=",
            "program_materialization_ms=",
            "batch_finalize_ms=",
            "candidate_finalize_ms=",
            "canonicalization_calls=",
            "serialization_calls=",
        ):
            self.assertIn(field, output)
        for field in (
            "prepare_ms=",
            "insert_ms=",
            "validate_ms=",
            "active_switch_ms=",
            "commit_ms=",
            "prepared_edges=",
            "validated_edges=",
        ):
            self.assertIn(field, output)
        self.assertIn("stage=coverage environment=DEV source_profile=fixture", output)
        self.assertIn("stage=job status=SUCCESS", output)

    def test_source_load_failure_logs_safe_classification(self):
        class FailingSources:
            def __iter__(self):
                raise RuntimeError("host=db.internal password=secret")

        with TemporaryDirectory() as directory:
            output = io.StringIO()
            with (
                redirect_stdout(output),
                redirect_stderr(output),
                self.assertRaisesRegex(RuntimeError, "host=db.internal"),
            ):
                imp_lineage_edge.materialize_sources(
                    FailingSources(),
                    db_path=Path(directory) / "lineage.db",
                    batch_id="batch-source-failure",
                    observed_at=OBSERVED_AT,
                )

        logs = output.getvalue()
        self.assertIn("stage=source_load status=STARTED", logs)
        self.assertIn("stage=source_load status=FAILED exception=RuntimeError", logs)
        self.assertNotIn("host=db.internal", logs)
        self.assertNotIn("password=secret", logs)

    def test_unsafe_batch_id_is_redacted_in_publish_log(self):
        with TemporaryDirectory() as directory:
            _, output = run_with_output(
                imp_lineage_edge.materialize_sources,
                [source("PROGRAM_BATCH_ID_REDACTION")],
                db_path=Path(directory) / "lineage.db",
                batch_id="host=db.internal password=secret",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )

        self.assertIn("stage=publish status=SUCCESS batch_id=<redacted>", output)
        self.assertNotIn("host=db.internal", output)
        self.assertNotIn("password=secret", output)

    def test_build_progress_uses_rebuild_workload_and_count_threshold(self):
        sources = [source(f"PROGRAM_DEMO_PROGRESS_{index}") for index in range(5)]
        with TemporaryDirectory() as directory:
            _, output = run_with_output(
                imp_lineage_edge.materialize_sources,
                sources,
                db_path=Path(directory) / "lineage.db",
                batch_id="batch-progress-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                progress_every=2,
            )

        self.assertIn("stage=build status=STARTED total=5", output)
        self.assertIn(
            "stage=build status=RUNNING processed=2 total=5 percent=40",
            output,
        )
        self.assertIn(
            "stage=build status=RUNNING processed=4 total=5 percent=80",
            output,
        )
        self.assertIn("stage=build status=SUCCESS processed=5", output)
        self.assertNotIn("PROGRAM_DEMO_PROGRESS_", output)

    def test_controlled_replay_filters_profile_and_limit_deterministically(self):
        sources = [
            source("PROGRAM_PROFILE_A_03", source_profile="profile_a"),
            source("PROGRAM_PROFILE_A_01", source_profile="profile_a"),
            source("PROGRAM_PROFILE_A_02", source_profile="profile_a"),
            source("PROGRAM_PROFILE_B_01", source_profile="profile_b"),
        ]
        provider = FixtureProvider(sources)
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            run_with_output(
                imp_lineage_edge.main,
                [provider],
                db_path=db_path,
                batch_id="batch-replay-initial",
                observed_at=OBSERVED_AT,
                coverage_report_path=None,
            )
            rebuilt_names = []
            real_builder = imp_lineage_edge.build_program_physical_dag

            def record_builder(program_source):
                rebuilt_names.append(program_source.program_name)
                return real_builder(program_source)

            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                side_effect=record_builder,
            ):
                result, output = run_with_output(
                    imp_lineage_edge.main,
                    [provider],
                    db_path=db_path,
                    batch_id="batch-replay-sample",
                    observed_at=OBSERVED_AT.replace(day=2),
                    coverage_report_path=None,
                    selected_profiles=("profile_a",),
                    limit=2,
                    force_rebuild=True,
                )

            store = imp_lineage_edge.SQLiteMaterializationStore(db_path)
            active_names = {
                state.program_name
                for state in store.read_program_states(active_only=True)
            }

        self.assertEqual(result, 0)
        self.assertEqual(
            rebuilt_names,
            ["PROGRAM_PROFILE_A_01", "PROGRAM_PROFILE_A_02"],
        )
        self.assertEqual(
            active_names,
            {
                "PROGRAM_PROFILE_A_01",
                "PROGRAM_PROFILE_A_02",
                "PROGRAM_PROFILE_A_03",
                "PROGRAM_PROFILE_B_01",
            },
        )
        self.assertIn(
            "stage=replay status=SELECTED "
            "replay_mode=controlled_profile_limit selected_profiles=profile_a "
            "source_total=3 replay_total=2 limit=2 force_rebuild=True "
            "partial_snapshot=True",
            output,
        )
        self.assertIn("stage=incremental_plan status=SUCCESS total=2", output)
        self.assertIn("changed=2 unchanged=0 deleted=0 rebuild=2", output)

    def test_slow_program_logs_stage_timings_without_sensitive_details(self):
        program_source = source("PROGRAM_SLOW_OBSERVABILITY")
        real_builder = imp_lineage_edge.build_program_physical_dag

        def slow_builder(value):
            time.sleep(0.02)
            return real_builder(value)

        with TemporaryDirectory() as directory:
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                side_effect=slow_builder,
            ):
                _, output = run_with_output(
                    imp_lineage_edge.materialize_sources,
                    [program_source],
                    db_path=Path(directory) / "lineage.db",
                    batch_id="batch-slow-observability",
                    observed_at=OBSERVED_AT,
                    complete_snapshot=True,
                    slow_threshold_ms=1,
                )

        self.assertIn("stage=build_program status=SLOW", output)
        self.assertIn("build_program_physical_dag_ms=", output)
        self.assertIn("audit_program_physical_dag_ms=", output)
        self.assertIn("single_program_total_ms=", output)
        self.assertIn("stage=build status=SUCCESS processed=1 slow_programs=1", output)
        for secret in (
            "PROGRAM_SLOW_OBSERVABILITY",
            "INSERT INTO",
            "DWA.DEMO_TARGET",
        ):
            self.assertNotIn(secret, output)

    def test_fast_program_does_not_emit_slow_log(self):
        program_source = source("PROGRAM_FAST_OBSERVABILITY")
        with TemporaryDirectory() as directory:
            _, output = run_with_output(
                imp_lineage_edge.materialize_sources,
                [program_source],
                db_path=Path(directory) / "lineage.db",
                batch_id="batch-fast-observability",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                slow_threshold_ms=10_000,
            )

        self.assertNotIn("stage=build_program status=SLOW", output)
        self.assertIn("slow_programs=0", output)
        self.assertIn("max_program_elapsed_ms=", output)
        self.assertIn("avg_program_elapsed_ms=", output)

    def test_default_parser_arguments_keep_production_defaults(self):
        args = imp_lineage_edge.build_parser().parse_args([])

        self.assertIsNone(args.profile)
        self.assertIsNone(args.limit)
        self.assertFalse(args.force_rebuild)
        self.assertEqual(args.progress_every, imp_lineage_edge.DEFAULT_PROGRESS_EVERY)
        self.assertEqual(
            args.slow_threshold_ms,
            imp_lineage_edge.DEFAULT_SLOW_THRESHOLD_MS,
        )
        self.assertFalse(args.diagnostic)

        controlled = imp_lineage_edge.build_parser().parse_args(
            [
                "--profile",
                "profile_a",
                "--profile",
                "profile_b",
                "--limit",
                "100",
                "--force-rebuild",
                "--progress-every",
                "10",
                "--slow-threshold-ms",
                "5000",
                "--diagnostic",
            ]
        )
        self.assertEqual(controlled.profile, ["profile_a", "profile_b"])
        self.assertEqual(controlled.limit, 100)
        self.assertTrue(controlled.force_rebuild)
        self.assertEqual(controlled.progress_every, 10)
        self.assertEqual(controlled.slow_threshold_ms, 5000)
        self.assertTrue(controlled.diagnostic)

    def test_diagnostic_logs_started_and_success_with_materialization_metrics(self):
        program_source = source("PROGRAM_DIAGNOSTIC_OBSERVABILITY")
        with TemporaryDirectory() as directory:
            _, output = run_with_output(
                imp_lineage_edge.materialize_sources,
                [program_source],
                db_path=Path(directory) / "lineage.db",
                batch_id="batch-diagnostic-observability",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                diagnostic=True,
            )

        self.assertIn(
            "stage=build_program status=STARTED program_id=",
            output,
        )
        self.assertIn("source_profile=fixture ordinal=1", output)
        self.assertIn("stage=build_program status=SUCCESS", output)
        for field in (
            "elapsed_ms=",
            "dag_ms=",
            "audit_ms=",
            "materialization_ms=",
            "physical_nodes=",
            "physical_edges=",
            "lineage_edges=",
            "issues=",
        ):
            self.assertIn(field, output)
        self.assertNotIn("PROGRAM_DIAGNOSTIC_OBSERVABILITY", output)
        self.assertNotIn("INSERT INTO", output)

    def test_force_rebuild_reparses_an_unchanged_program(self):
        program_source = source("PROGRAM_DEMO_FORCE_REBUILD")
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            run_with_output(
                imp_lineage_edge.main,
                [FixtureProvider([program_source])],
                db_path=db_path,
                batch_id="batch-force-1",
                observed_at=OBSERVED_AT,
                coverage_report_path=None,
            )
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                wraps=imp_lineage_edge.build_program_physical_dag,
            ) as builder:
                result, output = run_with_output(
                    imp_lineage_edge.main,
                    [FixtureProvider([program_source])],
                    db_path=db_path,
                    batch_id="batch-force-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                    coverage_report_path=None,
                    force_rebuild=True,
                )

        self.assertEqual(result, 0)
        builder.assert_called_once()
        self.assertIn("unchanged=0", output)
        self.assertIn("rebuild=1", output)

    def test_second_run_reports_unchanged_and_zero_rebuild_workload(self):
        sources = [source(f"PROGRAM_DEMO_UNCHANGED_{index}") for index in range(3)]
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            run_with_output(
                imp_lineage_edge.main,
                [FixtureProvider(sources)],
                db_path=db_path,
                batch_id="batch-unchanged-1",
                observed_at=OBSERVED_AT,
                coverage_report_path=None,
            )
            _, output = run_with_output(
                imp_lineage_edge.main,
                [FixtureProvider(sources)],
                db_path=db_path,
                batch_id="batch-unchanged-2",
                observed_at=OBSERVED_AT.replace(day=2),
                coverage_report_path=None,
            )

        self.assertIn(
            "stage=incremental_plan status=SUCCESS total=3 new=0 changed=0 "
            "unchanged=3 deleted=0 rebuild=0",
            output,
        )
        self.assertIn("stage=build status=STARTED total=0", output)
        self.assertIn("stage=build status=SUCCESS processed=0", output)
        self.assertNotIn("stage=build status=RUNNING", output)

    def test_failure_logs_only_exception_class_and_propagates(self):
        program_source = source("PROGRAM_SECRET_NAME")
        error_text = (
            "host=db.internal password=secret SQL=select script_code from secret_table"
        )
        with TemporaryDirectory() as directory:
            output = io.StringIO()
            with (
                redirect_stdout(output),
                redirect_stderr(output),
                patch(
                    "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                    side_effect=RuntimeError(error_text),
                ),
                self.assertRaisesRegex(RuntimeError, "host=db.internal"),
            ):
                imp_lineage_edge.main(
                    [FixtureProvider([program_source])],
                    db_path=Path(directory) / "lineage.db",
                    batch_id="batch-failure-1",
                    observed_at=OBSERVED_AT,
                    coverage_report_path=None,
                )

        logs = output.getvalue()
        self.assertIn("stage=build status=FAILED exception=RuntimeError", logs)
        self.assertIn("stage=job status=FAILED exception=RuntimeError", logs)
        for secret in (
            "PROGRAM_SECRET_NAME",
            "host=db.internal",
            "password=secret",
            "script_code",
            "secret_table",
        ):
            self.assertNotIn(secret, logs)

    def test_publish_failure_logs_failed_and_keeps_exception_behavior(self):
        error_text = "host=db.internal SQL=insert into lineage_edge password=secret"
        with TemporaryDirectory() as directory:
            output = io.StringIO()
            with (
                redirect_stdout(output),
                redirect_stderr(output),
                patch(
                    "jobs.crontab.imp_lineage_edge.SQLiteMaterializationStore.publish",
                    side_effect=RuntimeError(error_text),
                ),
                self.assertRaisesRegex(RuntimeError, "SQL=insert"),
            ):
                imp_lineage_edge.main(
                    [FixtureProvider([source("PROGRAM_PUBLISH_FAILURE")])],
                    db_path=Path(directory) / "lineage.db",
                    batch_id="batch-publish-failure",
                    observed_at=OBSERVED_AT,
                    coverage_report_path=None,
                )

        logs = output.getvalue()
        self.assertIn("stage=publish status=STARTED", logs)
        self.assertIn("stage=publish status=FAILED exception=RuntimeError", logs)
        self.assertIn("stage=job status=FAILED exception=RuntimeError", logs)
        self.assertNotIn("host=db.internal", logs)
        self.assertNotIn("SQL=insert", logs)
        self.assertNotIn("password=secret", logs)


if __name__ == "__main__":
    unittest.main()
