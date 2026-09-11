from __future__ import annotations

import unittest
from datetime import datetime, timezone
from io import BytesIO

from openpyxl import load_workbook

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
    EXPORT_HEADERS,
    EXPORT_SHEET_TITLE,
    ReconciliationErrorView,
    TargetReconciliationOutcome,
    _render_rows_html,
    build_environment_options,
    build_excel_bytes,
    build_export_filename,
    build_export_rows,
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


def make_result(
    target: str = "DWM.RESULT",
    *,
    sql_sources: tuple[str, ...] = ("DWF.A", "DWF.B"),
    schedule_sources: tuple[str, ...] = ("DWF.A", "DWF.C"),
):
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
        for source in sql_sources
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
        for source in schedule_sources
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


def make_empty_result(target: str = "DWM.RESULT"):
    return make_result(target, sql_sources=(), schedule_sources=())


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

    def test_export_rows_keep_all_statuses_and_sort_each_status_deterministically(self):
        result = make_result(
            sql_sources=("DWF.Z", "DWF.A"),
            schedule_sources=("DWF.Y", "DWF.B"),
        )
        view_model = build_reconciliation_view_model(result)
        outcome = TargetReconciliationOutcome(
            target_table=view_model.target_table,
            view_model=view_model,
        )

        export_rows = build_export_rows((outcome,))

        self.assertEqual(
            [(row.status, row.source_table) for row in export_rows],
            [
                (ReconciliationStatus.SQL_ONLY, "DWF.A"),
                (ReconciliationStatus.SQL_ONLY, "DWF.Z"),
                (ReconciliationStatus.SCHEDULE_ONLY, "DWF.B"),
                (ReconciliationStatus.SCHEDULE_ONLY, "DWF.Y"),
            ],
        )

    def test_all_match_view_and_export_keep_complete_rows(self):
        result = make_result(
            sql_sources=("DWF.A", "DWF.B"),
            schedule_sources=("DWF.A", "DWF.B"),
        )
        view_model = build_reconciliation_view_model(result)
        outcome = TargetReconciliationOutcome(
            target_table=view_model.target_table,
            view_model=view_model,
        )

        self.assertEqual(len(view_model.rows), 2)
        self.assertTrue(
            all(row.status is ReconciliationStatus.MATCH for row in view_model.rows)
        )
        self.assertEqual(_render_rows_html(view_model).count("两边一致"), 2)
        self.assertEqual(len(build_export_rows((outcome,))), 2)

    def test_excel_contains_fixed_headers_all_statuses_and_chinese_values(self):
        result = make_result()
        view_model = build_reconciliation_view_model(result)
        outcome = TargetReconciliationOutcome(
            target_table=view_model.target_table,
            view_model=view_model,
        )

        workbook = load_workbook(BytesIO(build_excel_bytes(build_export_rows((outcome,)))))
        sheet = workbook[EXPORT_SHEET_TITLE]
        values = list(sheet.values)
        workbook.close()

        self.assertEqual(sheet.title, EXPORT_SHEET_TITLE)
        self.assertEqual(values[0], EXPORT_HEADERS)
        self.assertEqual(
            values[1:],
            [
                (
                    "DWM.RESULT",
                    "DWF.B",
                    "是",
                    "否",
                    "SQL实际调用但调度未配置",
                ),
                (
                    "DWM.RESULT",
                    "DWF.C",
                    "否",
                    "是",
                    "调度已配置但SQL未调用",
                ),
                ("DWM.RESULT", "DWF.A", "是", "是", "两边一致"),
            ],
        )

    def test_multi_target_export_uses_one_sheet_and_excludes_failed_target(self):
        first = build_reconciliation_view_model(make_result("DWM.RESULT_A"))
        second = build_reconciliation_view_model(
            make_result("DWM.RESULT_B", sql_sources=("DWF.X",), schedule_sources=("DWF.X",))
        )
        outcomes = (
            TargetReconciliationOutcome(
                target_table=first.target_table,
                view_model=first,
            ),
            TargetReconciliationOutcome(
                target_table="DWM.FAIL",
                error=ReconciliationErrorView(
                    error_code=SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
                    message="失败",
                ),
            ),
            TargetReconciliationOutcome(
                target_table=second.target_table,
                view_model=second,
            ),
        )

        export_rows = build_export_rows(outcomes)
        workbook = load_workbook(BytesIO(build_excel_bytes(export_rows)))
        sheet = workbook[EXPORT_SHEET_TITLE]
        values = list(sheet.values)
        workbook.close()

        self.assertEqual(workbook.sheetnames, [EXPORT_SHEET_TITLE])
        target_values = [row[0] for row in values[1:]]
        self.assertCountEqual(
            target_values,
            ["DWM.RESULT_A", "DWM.RESULT_A", "DWM.RESULT_A", "DWM.RESULT_B"],
        )
        self.assertNotIn("DWM.FAIL", target_values)

    def test_no_rows_means_no_export_payload(self):
        empty_view = build_reconciliation_view_model(make_empty_result())
        outcomes = (
            TargetReconciliationOutcome(
                target_table=empty_view.target_table,
                view_model=empty_view,
            ),
            TargetReconciliationOutcome(
                target_table="DWM.FAIL",
                error=ReconciliationErrorView(
                    error_code=SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
                    message="失败",
                ),
            ),
        )

        self.assertEqual(build_export_rows(outcomes), ())

    def test_export_filename_is_safe_and_omits_profiles_and_batches(self):
        generated_at = datetime(2026, 9, 11, 15, 5, 0)

        single = build_export_filename(
            "DEV214",
            ("DWM.M_JJQD_LIST",),
            generated_at=generated_at,
        )
        multi = build_export_filename(
            "DEV214",
            ("DWM.RESULT_A", "DWM.RESULT_B"),
            generated_at=generated_at,
        )
        unsafe = build_export_filename(
            "DEV:214",
            ("DWM/M?JJQD|LIST",),
            generated_at="2026/09/11 15:05:00",
        )

        self.assertEqual(
            single,
            "lineage_reconciliation_DEV214_DWM_M_JJQD_LIST_20260911_150500.xlsx",
        )
        self.assertEqual(
            multi,
            "lineage_reconciliation_DEV214_20260911_150500.xlsx",
        )
        self.assertEqual(
            unsafe,
            "lineage_reconciliation_DEV_214_DWM_M_JJQD_LIST_2026_09_11_15_05_00.xlsx",
        )
        self.assertFalse(any(character in unsafe for character in ':\\/*?"<>|'))

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
