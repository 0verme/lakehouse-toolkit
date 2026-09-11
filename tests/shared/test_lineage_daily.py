from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import jobs.crontab.imp_lineage_daily as imp_lineage_daily  # pyright: ignore[reportMissingImports]
import jobs.crontab.imp_lineage_suppression as imp_lineage_suppression
from shared.lineage.environment_scope import (
    DISABLED_LINEAGE_ENVIRONMENT,
    UNKNOWN_LINEAGE_ENVIRONMENT,
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
)

OBSERVED_AT = datetime(2026, 9, 11, 8, 0, tzinfo=timezone.utc)


def make_scope(environment: str, *, enabled: bool = True) -> LineageEnvironmentScope:
    suffix = environment.lower().replace("_", "-")
    return LineageEnvironmentScope(
        name=f"scope_{suffix}",
        environment=environment,
        sql_source_profile=f"sql_{suffix}",
        schedule_source_profile=f"schedule_{suffix}",
        label=f"示例 {environment}",
        dws_profile=f"dws_{suffix}",
        enabled=enabled,
    )


def successful_runner(prefix: str, calls: list[str], *, rows: int | None = None):
    def execute(scope: LineageEnvironmentScope):
        calls.append(scope.environment)
        return SimpleNamespace(
            batch_id=f"batch-{prefix.lower()}-{scope.environment.lower()}",
            suppression_count=rows,
        )

    return execute


def failing_runner(prefix: str, calls: list[str], environments: set[str]):
    def execute(scope: LineageEnvironmentScope):
        calls.append(scope.environment)
        if scope.environment in environments:
            raise RuntimeError("host=db.internal password=secret")
        return SimpleNamespace(
            batch_id=f"batch-{prefix.lower()}-{scope.environment.lower()}"
        )

    return execute


class LineageDailySelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enabled = make_scope("DEV214")
        self.other = make_scope("DEV215")
        self.disabled = make_scope("DISABLED", enabled=False)

    def test_single_environment_runs_all_steps_and_keeps_batch_ids(self):
        sql_calls: list[str] = []
        schedule_calls: list[str] = []
        suppression_calls: list[str] = []
        output = io.StringIO()

        with redirect_stdout(output):
            result = imp_lineage_daily.run(
                (self.enabled,),
                observed_at=OBSERVED_AT,
                sql_runner=successful_runner("SQL", sql_calls),
                schedule_runner=successful_runner("SCHEDULE", schedule_calls),
                suppression_runner=successful_runner(
                    "SUPPRESSION", suppression_calls, rows=7764
                ),
            )

        environment = result.environments[0]
        self.assertTrue(environment.succeeded)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(environment.sql.batch_id, "batch-sql-dev214")
        self.assertEqual(environment.schedule.batch_id, "batch-schedule-dev214")
        self.assertEqual(environment.suppression.rows, 7764)
        self.assertEqual(sql_calls, ["DEV214"])
        self.assertEqual(schedule_calls, ["DEV214"])
        self.assertEqual(suppression_calls, ["DEV214"])
        self.assertIn("DEV214 SQL          SUCCESS", output.getvalue())
        self.assertIn("DEV214 SCHEDULE     SUCCESS", output.getvalue())
        self.assertIn("DEV214 SUPPRESSION  SUCCESS", output.getvalue())
        self.assertIn("rows=7764", output.getvalue())
        self.assertIn("summary environments=1 success=1 failed=0", output.getvalue())

    def test_disabled_scope_is_not_selected_without_environment(self):
        calls: list[str] = []

        result = imp_lineage_daily.run(
            (self.enabled, self.disabled),
            observed_at=OBSERVED_AT,
            sql_runner=successful_runner("SQL", calls),
            schedule_runner=successful_runner("SCHEDULE", calls),
            suppression_runner=successful_runner("SUPPRESSION", calls),
        )

        self.assertEqual([item.environment for item in result.environments], ["DEV214"])
        self.assertNotIn("DISABLED", calls)

    def test_disabled_environment_is_rejected_when_requested(self):
        with self.assertRaises(LineageEnvironmentScopeError) as context:
            imp_lineage_daily.run(
                (self.enabled, self.disabled),
                environment="DISABLED",
                observed_at=OBSERVED_AT,
                sql_runner=Mock(),
                schedule_runner=Mock(),
                suppression_runner=Mock(),
            )

        self.assertEqual(context.exception.code, DISABLED_LINEAGE_ENVIRONMENT)

    def test_environment_filter_only_selects_requested_scope(self):
        calls: list[str] = []

        result = imp_lineage_daily.run(
            (self.enabled, self.other),
            environment=" DEV215 ",
            observed_at=OBSERVED_AT,
            sql_runner=successful_runner("SQL", calls),
            schedule_runner=successful_runner("SCHEDULE", calls),
            suppression_runner=successful_runner("SUPPRESSION", calls),
        )

        self.assertEqual([item.environment for item in result.environments], ["DEV215"])
        self.assertEqual(calls, ["DEV215", "DEV215", "DEV215"])

    def test_unknown_environment_has_formal_error(self):
        with self.assertRaises(LineageEnvironmentScopeError) as context:
            imp_lineage_daily.run(
                (self.enabled,),
                environment="UNKNOWN",
                observed_at=OBSERVED_AT,
                sql_runner=Mock(),
                schedule_runner=Mock(),
                suppression_runner=Mock(),
            )

        self.assertEqual(context.exception.code, UNKNOWN_LINEAGE_ENVIRONMENT)


class LineageDailyDependencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dev214 = make_scope("DEV214")
        self.dev215 = make_scope("DEV215")

    def _run(
        self,
        *,
        sql_runner,
        schedule_runner,
        suppression_runner,
    ):
        return imp_lineage_daily.run(
            (self.dev214, self.dev215),
            observed_at=OBSERVED_AT,
            sql_runner=sql_runner,
            schedule_runner=schedule_runner,
            suppression_runner=suppression_runner,
        )

    def test_sql_failure_skips_suppression_but_schedule_still_runs(self):
        sql_calls: list[str] = []
        schedule_calls: list[str] = []
        suppression = Mock()
        result = self._run(
            sql_runner=failing_runner("SQL", sql_calls, {"DEV214"}),
            schedule_runner=successful_runner("SCHEDULE", schedule_calls),
            suppression_runner=suppression,
        )

        failed = result.environments[0]
        successful = result.environments[1]
        self.assertEqual(failed.sql.status, imp_lineage_daily.StepStatus.FAILED)
        self.assertEqual(failed.schedule.status, imp_lineage_daily.StepStatus.SUCCESS)
        self.assertEqual(
            failed.suppression.status, imp_lineage_daily.StepStatus.SKIPPED
        )
        self.assertEqual(failed.suppression.message, "upstream_failed")
        self.assertTrue(successful.succeeded)
        self.assertEqual(sql_calls, ["DEV214", "DEV215"])
        self.assertEqual(schedule_calls, ["DEV214", "DEV215"])
        suppression.assert_called_once_with(self.dev215)
        self.assertEqual(result.exit_code, 1)

    def test_schedule_failure_skips_suppression(self):
        suppression = Mock()
        result = imp_lineage_daily.run(
            (self.dev214,),
            observed_at=OBSERVED_AT,
            sql_runner=successful_runner("SQL", []),
            schedule_runner=failing_runner("SCHEDULE", [], {"DEV214"}),
            suppression_runner=suppression,
        )

        environment = result.environments[0]
        self.assertEqual(environment.sql.status, imp_lineage_daily.StepStatus.SUCCESS)
        self.assertEqual(
            environment.schedule.status, imp_lineage_daily.StepStatus.FAILED
        )
        self.assertEqual(
            environment.suppression.status, imp_lineage_daily.StepStatus.SKIPPED
        )
        suppression.assert_not_called()
        self.assertEqual(result.exit_code, 1)

    def test_both_upstreams_failed_still_skip_suppression(self):
        suppression = Mock()
        result = imp_lineage_daily.run(
            (self.dev214,),
            observed_at=OBSERVED_AT,
            sql_runner=failing_runner("SQL", [], {"DEV214"}),
            schedule_runner=failing_runner("SCHEDULE", [], {"DEV214"}),
            suppression_runner=suppression,
        )

        environment = result.environments[0]
        self.assertTrue(environment.failed)
        self.assertEqual(
            environment.suppression.status, imp_lineage_daily.StepStatus.SKIPPED
        )
        suppression.assert_not_called()

    def test_suppression_failure_marks_environment_failed(self):
        result = imp_lineage_daily.run(
            (self.dev214,),
            observed_at=OBSERVED_AT,
            sql_runner=successful_runner("SQL", []),
            schedule_runner=successful_runner("SCHEDULE", []),
            suppression_runner=failing_runner("SUPPRESSION", [], {"DEV214"}),
        )

        environment = result.environments[0]
        self.assertEqual(
            environment.suppression.status, imp_lineage_daily.StepStatus.FAILED
        )
        self.assertEqual(environment.suppression.error_code, "RuntimeError")
        self.assertEqual(result.exit_code, 1)

    def test_failed_environment_does_not_block_next_environment(self):
        calls: list[tuple[str, str]] = []

        def sql(scope):
            calls.append(("SQL", scope.environment))
            if scope.environment == "DEV214":
                raise RuntimeError("DEV214 failed")
            return SimpleNamespace(batch_id="batch-sql-dev215")

        def schedule(scope):
            calls.append(("SCHEDULE", scope.environment))
            return SimpleNamespace(
                batch_id=f"batch-schedule-{scope.environment.lower()}"
            )

        def suppression(scope):
            calls.append(("SUPPRESSION", scope.environment))
            return SimpleNamespace(suppression_count=1)

        result = self._run(
            sql_runner=sql,
            schedule_runner=schedule,
            suppression_runner=suppression,
        )

        self.assertEqual(
            calls,
            [
                ("SQL", "DEV214"),
                ("SCHEDULE", "DEV214"),
                ("SQL", "DEV215"),
                ("SCHEDULE", "DEV215"),
                ("SUPPRESSION", "DEV215"),
            ],
        )
        self.assertFalse(result.environments[0].succeeded)
        self.assertTrue(result.environments[1].succeeded)
        self.assertEqual(result.success_count, 1)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.exit_code, 1)

    def test_failure_logs_do_not_include_exception_details(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = imp_lineage_daily.run(
                (self.dev214,),
                observed_at=OBSERVED_AT,
                sql_runner=failing_runner("SQL", [], {"DEV214"}),
                schedule_runner=successful_runner("SCHEDULE", []),
                suppression_runner=Mock(),
            )

        text = output.getvalue()
        self.assertEqual(result.exit_code, 1)
        self.assertIn("error_code=RuntimeError", text)
        self.assertIn("reason=upstream_failed", text)
        self.assertNotIn("db.internal", text)
        self.assertNotIn("password=secret", text)


class LineageDailyDefaultRunnerTests(unittest.TestCase):
    def test_default_runners_call_existing_python_apis_with_scope_profiles(self):
        scope = make_scope("DEV214")
        sql_result = SimpleNamespace(batch_id="batch-sql-default")
        schedule_result = SimpleNamespace(batch_id="batch-schedule-default")
        suppression_result = (SimpleNamespace(suppression_count=4),)

        with (
            patch.object(
                imp_lineage_daily.imp_lineage_edge,
                "load_default_providers",
                return_value=("provider",),
            ) as load_sql,
            patch.object(
                imp_lineage_daily.imp_lineage_edge,
                "run",
                return_value=sql_result,
            ) as sql_run,
            patch.object(
                imp_lineage_daily.imp_schedule_lineage,
                "load_mysql_process_profiles",
                return_value=("profile",),
            ) as load_schedule,
            patch.object(
                imp_lineage_daily.imp_schedule_lineage,
                "run",
                return_value=schedule_result,
            ) as schedule_run,
            patch.object(
                imp_lineage_daily.imp_lineage_suppression,
                "run",
                return_value=suppression_result,
            ) as suppression_run,
        ):
            result = imp_lineage_daily.run(
                (scope,),
                config_path=Path("configs/lineage_providers.local.yaml"),
                observed_at=OBSERVED_AT,
            )

        load_sql.assert_called_once_with(Path("configs/lineage_providers.local.yaml"))
        load_schedule.assert_called_once_with(
            Path("configs/lineage_providers.local.yaml")
        )
        sql_kwargs = sql_run.call_args.kwargs
        self.assertEqual(sql_run.call_args.args, (("provider",),))
        self.assertEqual(sql_kwargs["selected_profiles"], ("sql_dev214",))
        self.assertEqual(sql_kwargs["store_backend"], "dws")
        self.assertEqual(sql_kwargs["dws_profile"], "dws_dev214")
        self.assertEqual(schedule_run.call_args.args, (("profile",),))
        self.assertEqual(
            schedule_run.call_args.kwargs["selected_profiles"], ("schedule_dev214",)
        )
        suppression_run.assert_called_once()
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.environments[0].sql.batch_id, "batch-sql-default")
        self.assertEqual(
            result.environments[0].schedule.batch_id, "batch-schedule-default"
        )
        self.assertEqual(result.environments[0].suppression.rows, 4)

    def test_main_returns_nonzero_for_failed_environment(self):
        code = imp_lineage_daily.main(
            scopes=(make_scope("DEV214"),),
            observed_at=OBSERVED_AT,
            sql_runner=failing_runner("SQL", [], {"DEV214"}),
            schedule_runner=successful_runner("SCHEDULE", []),
            suppression_runner=Mock(),
        )

        self.assertEqual(code, 1)


class LineageDailyCompatibilityTests(unittest.TestCase):
    def test_suppression_parser_keeps_environment_and_dry_run(self):
        args = imp_lineage_suppression.build_parser().parse_args(
            ["--environment", "DEV214", "--dry-run"]
        )

        self.assertEqual(args.environment, "DEV214")
        self.assertTrue(args.dry_run)

    def test_daily_parser_has_no_ambiguous_dry_run_option(self):
        parser = imp_lineage_daily.build_parser()
        args = parser.parse_args(["--environment", "DEV214"])
        self.assertEqual(args.environment, "DEV214")
        self.assertIsNone(args.config)

        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["--dry-run"])

    def test_production_suppression_entrypoint_is_under_crontab(self):
        project_root = Path(__file__).resolve().parents[2]

        self.assertTrue(
            (project_root / "jobs" / "crontab" / "imp_lineage_suppression.py").exists()
        )


if __name__ == "__main__":
    unittest.main()
