from __future__ import annotations

import io
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
            )
            _, output = run_with_output(
                imp_lineage_edge.main,
                [FixtureProvider(sources)],
                db_path=db_path,
                batch_id="batch-unchanged-2",
                observed_at=OBSERVED_AT.replace(day=2),
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
