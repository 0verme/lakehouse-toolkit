from __future__ import annotations

import unittest
from datetime import datetime, timezone

from shared.lineage.domain import LineageEdge
from shared.lineage.environment_scope import LineageEnvironmentScope
from shared.lineage.reconciliation import (
    ActiveSnapshotNotFoundError,
    ReconciliationStatus,
    SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
    SQLBusinessLineageSnapshot,
    ScheduleLineageSnapshot,
    TargetSummaryStatus,
    reconcile_lineage_snapshots,
)
from shared.lineage.schedule import ScheduleLineageEdge
from tools.lineage.reconcile_sql_schedule_web import (
    build_environment_options,
    build_reconciliation_view_model,
    build_summary,
    map_reconciliation_error,
    parse_target_tables,
    reconcile_target,
    reconcile_targets,
    sort_rows,
    status_to_label,
)

ENVIRONMENT = "DEMO_DEV"
SQL_PROFILE = "DEMO_SQL_PROFILE"
SCHEDULE_PROFILE = "DEMO_SCHEDULE_PROFILE"
OBSERVED_AT = datetime(2026, 9, 10, 8, 9, 10, tzinfo=timezone.utc)


def make_scope() -> LineageEnvironmentScope:
    return LineageEnvironmentScope(
        name="demo_dev",
        environment=ENVIRONMENT,
        sql_source_profile=SQL_PROFILE,
        schedule_source_profile=SCHEDULE_PROFILE,
        label="示例开发环境",
        dws_profile="demo",
    )


def make_result(target: str = "DWM.RESULT"):
    sql_edges = tuple(
        LineageEdge(
            environment=ENVIRONMENT,
            source_profile=SQL_PROFILE,
            source_table=source,
            target_table=target,
            program_name=f"DEMO_SQL_{source.rsplit('.', 1)[-1]}",
            batch_id="batch-sql",
            observed_at=OBSERVED_AT,
        )
        for source in ("DWF.A", "DWF.B")
    )
    schedule_edges = tuple(
        ScheduleLineageEdge(
            environment=ENVIRONMENT,
            source_profile=SCHEDULE_PROFILE,
            process_name=f"DEMO_SCHEDULE_{source.rsplit('.', 1)[-1]}",
            project_version_key="DEMO_PROJECT:1.0",
            raw_source_table=source,
            raw_target_table=target,
            source_table=source,
            target_table=target,
        )
        for source in ("DWF.A", "DWF.C")
    )
    return reconcile_lineage_snapshots(
        SQLBusinessLineageSnapshot(
            batch_id="batch-sql",
            edges=sql_edges,
            observed_at=OBSERVED_AT,
            snapshot_scope=((ENVIRONMENT, SQL_PROFILE),),
        ),
        ScheduleLineageSnapshot(
            batch_id="batch-schedule",
            edges=schedule_edges,
            observed_at=OBSERVED_AT,
        ),
        environment=ENVIRONMENT,
        sql_source_profile=SQL_PROFILE,
        schedule_source_profile=SCHEDULE_PROFILE,
        target_table=target,
    )


class ReconcileSqlScheduleWebTests(unittest.TestCase):
    def test_environment_options_expose_only_enabled_environment_values(self):
        scope = make_scope()
        disabled = LineageEnvironmentScope(
            name="disabled",
            environment="DEMO_DISABLED",
            sql_source_profile="DEMO_SQL_DISABLED",
            schedule_source_profile="DEMO_SCHEDULE_DISABLED",
            label="停用环境",
            dws_profile="demo",
            enabled=False,
        )

        options = build_environment_options((disabled, scope))

        self.assertEqual(len(options), 1)
        self.assertEqual(
            options[0].as_pywebio_option(),
            {
                "label": "示例开发环境",
                "value": ENVIRONMENT,
            },
        )

    def test_status_labels_keep_domain_status_values(self):
        self.assertEqual(status_to_label(ReconciliationStatus.MATCH), "两边一致")
        self.assertEqual(
            status_to_label(ReconciliationStatus.SQL_ONLY),
            "SQL实际调用但调度未配置",
        )
        self.assertEqual(
            status_to_label(ReconciliationStatus.SCHEDULE_ONLY),
            "调度已配置但SQL未调用",
        )

    def test_view_model_sorts_differences_first_and_builds_summary(self):
        result = make_result()

        view_model = build_reconciliation_view_model(result)

        self.assertEqual(view_model.status, TargetSummaryStatus.DIFFERENT)
        self.assertEqual(view_model.status_label, "有差异")
        self.assertEqual(
            [row.status for row in view_model.rows],
            [
                ReconciliationStatus.SQL_ONLY,
                ReconciliationStatus.SCHEDULE_ONLY,
                ReconciliationStatus.MATCH,
            ],
        )
        self.assertEqual(view_model.sql_source_profile, SQL_PROFILE)
        self.assertEqual(view_model.schedule_source_profile, SCHEDULE_PROFILE)
        self.assertEqual(view_model.sql_batch_id, "batch-sql")
        self.assertEqual(view_model.schedule_batch_id, "batch-schedule")
        self.assertEqual(
            view_model.summary,
            build_summary(result.rows),
        )
        self.assertEqual(view_model.summary.sql_actual_count, 2)
        self.assertEqual(view_model.summary.schedule_configured_count, 2)
        self.assertEqual(view_model.summary.match_count, 1)
        self.assertEqual(view_model.summary.sql_only_count, 1)
        self.assertEqual(view_model.summary.schedule_only_count, 1)

    def test_sort_rows_prioritizes_sql_only_schedule_only_then_match(self):
        rows = make_result().rows

        sorted_rows = sort_rows(rows)

        self.assertEqual(
            [row.status for row in sorted_rows],
            [
                ReconciliationStatus.SQL_ONLY,
                ReconciliationStatus.SCHEDULE_ONLY,
                ReconciliationStatus.MATCH,
            ],
        )

    def test_reconcile_target_passes_resolved_split_scope_to_formal_runner(self):
        captured = {}

        def runner(**kwargs):
            captured.update(kwargs)
            return make_result(kwargs["target_table"])

        reconcile_target(make_scope(), " DWM.RESULT ", runner=runner)

        self.assertEqual(
            captured,
            {
                "dws_profile": "demo",
                "environment": ENVIRONMENT,
                "sql_source_profile": SQL_PROFILE,
                "schedule_source_profile": SCHEDULE_PROFILE,
                "target_table": "DWM.RESULT",
            },
        )

    def test_multiple_targets_isolate_one_failure(self):
        calls: list[str] = []

        def runner(**kwargs):
            target = kwargs["target_table"]
            calls.append(target)
            if target == "DWM.FAIL":
                raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
            return make_result(target)

        outcomes = reconcile_targets(
            make_scope(),
            ("DWM.RESULT_A", "DWM.FAIL", "DWA.RESULT_B"),
            runner=runner,
        )

        self.assertEqual(calls, ["DWM.RESULT_A", "DWM.FAIL", "DWA.RESULT_B"])
        self.assertEqual(len(outcomes), 3)
        self.assertTrue(outcomes[0].succeeded)
        self.assertFalse(outcomes[1].succeeded)
        self.assertIsNotNone(outcomes[1].error)
        self.assertEqual(
            outcomes[1].error.error_code,  # type: ignore[union-attr]
            SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
        )
        self.assertTrue(outcomes[2].succeeded)

    def test_fail_closed_error_mapping_preserves_both_snapshot_codes(self):
        sql_error = map_reconciliation_error(
            ActiveSnapshotNotFoundError("SQL_ACTIVE_SNAPSHOT_NOT_FOUND")
        )
        schedule_error = map_reconciliation_error(
            ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
        )

        self.assertEqual(sql_error.error_code, "SQL_ACTIVE_SNAPSHOT_NOT_FOUND")
        self.assertIn("SQL active snapshot", sql_error.message)
        self.assertEqual(
            schedule_error.error_code,
            SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
        )
        self.assertIn("Schedule active snapshot", schedule_error.message)

    def test_parse_target_tables_keeps_one_target_per_nonempty_line(self):
        self.assertEqual(
            parse_target_tables("\n DWM.TABLE_A \n\tDWM.TABLE_B\n"),
            ("DWM.TABLE_A", "DWM.TABLE_B"),
        )


if __name__ == "__main__":
    unittest.main()
