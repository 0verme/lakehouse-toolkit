from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from shared.lineage.domain import LineageEdge
from shared.lineage.reconciliation import (
    ActiveSnapshotNotFoundError,
    ReconciliationStatus,
    SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
    SQL_ACTIVE_SNAPSHOT_NOT_FOUND,
    ScheduleLineageSnapshot,
    TargetSummaryStatus,
    normalize_lineage_comparison_table_key,
    read_active_schedule_snapshot,
    read_active_sql_business_snapshot,
    reconcile_active_dws_lineage,
    reconcile_lineage_snapshots,
)
from shared.lineage.schedule import ScheduleLineageEdge
from tools.lineage.reconcile_sql_schedule import (
    build_parser,
    render_csv,
    render_json,
    render_table,
)

ENVIRONMENT = "DEMO_DEV"
PROFILE = "DEMO_PROFILE"
SQL_PROFILE = "DEMO_SQL_PROFILE"
SCHEDULE_PROFILE = "DEMO_SCHEDULE_PROFILE"
OBSERVED_AT = datetime(2026, 9, 10, 8, 9, 10, tzinfo=timezone.utc)


@dataclass(frozen=True)
class FakeScheduleRow:
    edge: ScheduleLineageEdge
    batch_id: str
    observed_at: datetime = OBSERVED_AT
    is_active: bool = True


class FakeSQLReader:
    def __init__(
        self,
        edges: tuple[LineageEdge, ...] = (),
        *,
        batch_id: str | None = "batch-sql",
        scopes: tuple[tuple[str, str], ...] = ((ENVIRONMENT, PROFILE),),
    ) -> None:
        self.edges = edges
        self.batch_id = batch_id
        self.scopes = scopes
        self.calls: list[tuple[str | None, bool]] = []

    def get_active_batch_id(self) -> str | None:
        return self.batch_id

    def get_batch_metadata(self, batch_id: str):
        if self.batch_id != batch_id:
            return None
        return SimpleNamespace(is_active=True, observed_at=OBSERVED_AT)

    def get_active_snapshot_scope(self) -> tuple[tuple[str, str], ...]:
        return self.scopes

    def read_edges(
        self, *, batch_id: str | None = None, active_only: bool = False
    ) -> tuple[LineageEdge, ...]:
        self.calls.append((batch_id, active_only))
        return self.edges


class FakeScheduleReader:
    def __init__(
        self,
        rows: tuple[FakeScheduleRow, ...] = (),
        *,
        batch_id: str | None = "batch-schedule",
    ) -> None:
        self.rows = rows
        self.batch_id = batch_id
        self.calls: list[tuple[str | None, bool]] = []

    def get_active_batch_id(self) -> str | None:
        return self.batch_id

    def read_rows(
        self, *, batch_id: str | None = None, active_only: bool = False
    ) -> tuple[FakeScheduleRow, ...]:
        self.calls.append((batch_id, active_only))
        return self.rows


def sql_edge(
    source: str,
    target: str,
    *,
    program: str = "DEMO_PROGRAM_A",
    environment: str = ENVIRONMENT,
    profile: str = PROFILE,
    batch_id: str | None = None,
) -> LineageEdge:
    return LineageEdge(
        environment=environment,
        source_profile=profile,
        source_table=source,
        target_table=target,
        program_name=program,
        batch_id=batch_id,
        observed_at=OBSERVED_AT,
    )


def schedule_edge(
    source: str,
    target: str,
    *,
    process: str = "DEMO_PROCESS_A",
    project: str = "DEMO_PROJECT:1.0",
    environment: str = ENVIRONMENT,
    profile: str = PROFILE,
) -> ScheduleLineageEdge:
    return ScheduleLineageEdge(
        environment=environment,
        source_profile=profile,
        process_name=process,
        project_version_key=project,
        raw_source_table=source,
        raw_target_table=target,
    )


def sql_snapshot(
    *edges: LineageEdge,
    batch_id: str = "batch-sql",
    profile: str = PROFILE,
):
    from shared.lineage.reconciliation import SQLBusinessLineageSnapshot

    return SQLBusinessLineageSnapshot(
        batch_id=batch_id,
        edges=edges,
        observed_at=OBSERVED_AT,
        snapshot_scope=((ENVIRONMENT, profile),),
    )


def schedule_snapshot(
    *edges: ScheduleLineageEdge, batch_id: str = "batch-schedule"
) -> ScheduleLineageSnapshot:
    return ScheduleLineageSnapshot(
        batch_id=batch_id,
        edges=edges,
        observed_at=OBSERVED_AT,
    )


class ComparisonNormalizationTests(unittest.TestCase):
    def test_explicit_dws_namespace_registry_is_idempotent(self):
        values = {
            "DWS_DWF.A": "DWF.A",
            "DWS_DWM.B": "DWM.B",
            "DWS_DWD.C": "DWD.C",
            "DWS_DWP.D": "DWP.D",
            "DWS_DWA.E": "DWA.E",
            "DWS_DM.F": "DM.F",
            "DWS_DWUPRR.G": "DWUPRR.G",
        }
        for raw, expected in values.items():
            with self.subTest(raw=raw):
                normalized = normalize_lineage_comparison_table_key(raw)
                self.assertEqual(normalized, expected)
                self.assertEqual(
                    normalize_lineage_comparison_table_key(normalized), expected
                )

    def test_unknown_namespace_and_same_basename_remain_distinct(self):
        self.assertEqual(
            normalize_lineage_comparison_table_key("DWS_UNKNOWN.TABLE_X"),
            "DWS_UNKNOWN.TABLE_X",
        )
        self.assertNotEqual(
            normalize_lineage_comparison_table_key("SCHEMA_A.TABLE_X"),
            normalize_lineage_comparison_table_key("SCHEMA_B.TABLE_X"),
        )


class ReconciliationDomainTests(unittest.TestCase):
    def test_three_states_for_one_target_and_different_summary(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DEMO_DWF.A", "DEMO_DWM.RESULT_A"),
                sql_edge(
                    "DEMO_DWF.B",
                    "DEMO_DWM.RESULT_A",
                    program="DEMO_PROGRAM_B",
                ),
            ),
            schedule_snapshot(
                schedule_edge("DEMO_DWF.A", "DEMO_DWM.RESULT_A"),
                schedule_edge(
                    "DEMO_DWF.C",
                    "DEMO_DWM.RESULT_A",
                    process="DEMO_PROCESS_C",
                ),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
            target_table="DEMO_DWM.RESULT_A",
        )

        self.assertEqual(
            [(row.source_table, row.status) for row in result.rows],
            [
                ("DEMO_DWF.A", ReconciliationStatus.MATCH),
                ("DEMO_DWF.B", ReconciliationStatus.SQL_ONLY),
                ("DEMO_DWF.C", ReconciliationStatus.SCHEDULE_ONLY),
            ],
        )
        self.assertEqual(result.match_count, 1)
        self.assertEqual(result.sql_only_count, 1)
        self.assertEqual(result.schedule_only_count, 1)
        summary = result.target_summaries[0]
        self.assertEqual(summary.sql_source_count, 2)
        self.assertEqual(summary.schedule_source_count, 2)
        self.assertEqual(summary.match_count, 1)
        self.assertEqual(summary.sql_only_count, 1)
        self.assertEqual(summary.schedule_only_count, 1)
        self.assertEqual(summary.status, TargetSummaryStatus.DIFFERENT)

    def test_different_source_profiles_keep_three_state_edge_reconciliation(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge(
                    "DEMO_DWF.A",
                    "DEMO_DWM.RESULT",
                    profile=SQL_PROFILE,
                ),
                sql_edge(
                    "DEMO_DWF.B",
                    "DEMO_DWM.RESULT",
                    profile=SQL_PROFILE,
                    program="DEMO_PROGRAM_B",
                ),
                profile=SQL_PROFILE,
            ),
            schedule_snapshot(
                schedule_edge(
                    "DEMO_DWF.A",
                    "DEMO_DWM.RESULT",
                    profile=SCHEDULE_PROFILE,
                ),
                schedule_edge(
                    "DEMO_DWF.C",
                    "DEMO_DWM.RESULT",
                    profile=SCHEDULE_PROFILE,
                    process="DEMO_PROCESS_C",
                ),
            ),
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_table="DEMO_DWM.RESULT",
        )

        self.assertNotEqual(SQL_PROFILE, SCHEDULE_PROFILE)
        self.assertEqual(
            [(row.source_table, row.status) for row in result.rows],
            [
                ("DEMO_DWF.A", ReconciliationStatus.MATCH),
                ("DEMO_DWF.B", ReconciliationStatus.SQL_ONLY),
                ("DEMO_DWF.C", ReconciliationStatus.SCHEDULE_ONLY),
            ],
        )
        self.assertEqual(result.sql_source_profile, SQL_PROFILE)
        self.assertEqual(result.schedule_source_profile, SCHEDULE_PROFILE)
        self.assertIsNone(result.source_profile)
        self.assertIsNone(result.rows[0].source_profile)
        summary = result.target_summaries[0]
        self.assertEqual(summary.sql_source_profile, SQL_PROFILE)
        self.assertEqual(summary.schedule_source_profile, SCHEDULE_PROFILE)
        self.assertEqual(summary.status, TargetSummaryStatus.DIFFERENT)
        self.assertEqual(summary.sql_only_count, 1)
        self.assertEqual(summary.schedule_only_count, 1)

        payload = result.to_dict()
        self.assertEqual(payload["sql_source_profile"], SQL_PROFILE)
        self.assertEqual(payload["schedule_source_profile"], SCHEDULE_PROFILE)
        self.assertNotIn("source_profile", payload)
        rows = payload["rows"]
        assert isinstance(rows, list)
        first_row = rows[0]
        assert isinstance(first_row, dict)
        self.assertNotIn("source_profile", first_row)
        self.assertEqual(first_row["sql_source_profile"], SQL_PROFILE)
        self.assertEqual(first_row["schedule_source_profile"], SCHEDULE_PROFILE)

    def test_profile_is_not_part_of_business_edge_identity(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DEMO_DWF.A", "DEMO_DWM.RESULT", profile=SQL_PROFILE),
                profile=SQL_PROFILE,
            ),
            schedule_snapshot(
                schedule_edge(
                    "DEMO_DWF.A",
                    "DEMO_DWM.RESULT",
                    profile=SCHEDULE_PROFILE,
                )
            ),
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
        )

        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].status, ReconciliationStatus.MATCH)
        self.assertEqual(result.rows[0].source_table, "DEMO_DWF.A")
        self.assertEqual(result.rows[0].target_table, "DEMO_DWM.RESULT")

    def test_all_match_has_consistent_target_summary(self):
        edge = sql_edge("DEMO_DWF.A", "DEMO_DWM.RESULT_A")
        result = reconcile_lineage_snapshots(
            sql_snapshot(edge),
            schedule_snapshot(schedule_edge("DEMO_DWF.A", "DEMO_DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(result.rows[0].status, ReconciliationStatus.MATCH)
        self.assertEqual(
            result.target_summaries[0].status,
            TargetSummaryStatus.CONSISTENT,
        )
        self.assertEqual(result.sql_only_count, 0)
        self.assertEqual(result.schedule_only_count, 0)
        self.assertEqual(result.sql_source_profile, PROFILE)
        self.assertEqual(result.schedule_source_profile, PROFILE)
        self.assertEqual(result.source_profile, PROFILE)

    def test_new_profile_arguments_require_both_sides(self):
        with self.assertRaisesRegex(
            ValueError,
            "sql_source_profile and schedule_source_profile must both be provided",
        ):
            reconcile_lineage_snapshots(
                sql_snapshot(),
                schedule_snapshot(),
                environment=ENVIRONMENT,
                sql_source_profile=SQL_PROFILE,
            )

    def test_legacy_and_new_profile_conflict_fails_fast(self):
        with self.assertRaisesRegex(
            ValueError, "source_profile conflicts with sql_source_profile"
        ):
            reconcile_lineage_snapshots(
                sql_snapshot(),
                schedule_snapshot(),
                environment=ENVIRONMENT,
                source_profile=PROFILE,
                sql_source_profile=SQL_PROFILE,
                schedule_source_profile=PROFILE,
            )

    def test_legacy_profile_shorthand_resolves_both_sides(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(sql_edge("DWF.A", "DWM.RESULT_A")),
            schedule_snapshot(schedule_edge("DWF.A", "DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(result.sql_source_profile, PROFILE)
        self.assertEqual(result.schedule_source_profile, PROFILE)
        self.assertEqual(result.source_profile, PROFILE)

    def test_legacy_dws_wrapper_matches_unwrapped_identity_in_both_directions(self):
        first = reconcile_lineage_snapshots(
            sql_snapshot(sql_edge("DWS_DWF.A", "DWS_DWM.RESULT_A")),
            schedule_snapshot(schedule_edge("DWF.A", "DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        second = reconcile_lineage_snapshots(
            sql_snapshot(sql_edge("DWF.A", "DWM.RESULT_A")),
            schedule_snapshot(schedule_edge("DWS_DWF.A", "DWS_DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(first.rows[0].status, ReconciliationStatus.MATCH)
        self.assertEqual(second.rows[0].status, ReconciliationStatus.MATCH)

    def test_unknown_namespace_does_not_get_guessed(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(sql_edge("UNKNOWN.TABLE_X", "DWM.RESULT_A")),
            schedule_snapshot(schedule_edge("DWS_UNKNOWN.TABLE_X", "DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(
            {row.source_table: row.status for row in result.rows},
            {
                "UNKNOWN.TABLE_X": ReconciliationStatus.SQL_ONLY,
                "DWS_UNKNOWN.TABLE_X": ReconciliationStatus.SCHEDULE_ONLY,
            },
        )

    def test_multiple_provenance_values_make_one_business_comparison_row(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DWF.A", "DWM.RESULT_A", program="DEMO_PROGRAM_A"),
                sql_edge("DWF.A", "DWM.RESULT_A", program="DEMO_PROGRAM_B"),
            ),
            schedule_snapshot(
                schedule_edge("DWF.A", "DWM.RESULT_A", process="DEMO_PROCESS_A"),
                schedule_edge("DWF.A", "DWM.RESULT_A", process="DEMO_PROCESS_B"),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(len(result.rows), 1)
        row = result.rows[0]
        self.assertEqual(row.status, ReconciliationStatus.MATCH)
        self.assertEqual(row.sql_fact_count, 2)
        self.assertEqual(row.schedule_fact_count, 2)
        self.assertEqual(row.sql_program_count, 2)
        self.assertEqual(row.schedule_process_count, 2)

    def test_environment_and_profile_are_strictly_isolated(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DWF.A", "DWM.RESULT_A"),
                sql_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    environment="DEMO_OTHER_ENV",
                    profile=PROFILE,
                ),
                sql_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    profile="DEMO_OTHER_PROFILE",
                ),
            ),
            schedule_snapshot(
                schedule_edge("DWF.A", "DWM.RESULT_A"),
                schedule_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    environment="DEMO_OTHER_ENV",
                    profile=PROFILE,
                    process="DEMO_PROCESS_OTHER_ENV",
                ),
                schedule_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    profile="DEMO_OTHER_PROFILE",
                    process="DEMO_PROCESS_OTHER_PROFILE",
                ),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].status, ReconciliationStatus.MATCH)
        self.assertEqual(result.rows[0].sql_fact_count, 1)
        self.assertEqual(result.rows[0].schedule_fact_count, 1)

    def test_target_filter_returns_only_requested_target(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DWF.A", "DWM.RESULT_A"),
                sql_edge("DWF.B", "DWM.RESULT_B"),
            ),
            schedule_snapshot(
                schedule_edge("DWF.A", "DWM.RESULT_A"),
                schedule_edge("DWF.B", "DWM.RESULT_B"),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
            target_table="DWM.RESULT_B",
        )
        self.assertEqual({row.target_table for row in result.rows}, {"DWM.RESULT_B"})
        self.assertEqual(
            {summary.target_table for summary in result.target_summaries},
            {"DWM.RESULT_B"},
        )

    def test_deterministic_target_then_source_ordering(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge("DWF.Z", "DWM.RESULT_B"),
                sql_edge("DWF.B", "DWM.RESULT_A"),
                sql_edge("DWF.A", "DWM.RESULT_A"),
            ),
            schedule_snapshot(
                schedule_edge("DWF.Z", "DWM.RESULT_B"),
                schedule_edge("DWF.B", "DWM.RESULT_A"),
                schedule_edge("DWF.A", "DWM.RESULT_A"),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(
            [(row.target_table, row.source_table) for row in result.rows],
            [
                ("DWM.RESULT_A", "DWF.A"),
                ("DWM.RESULT_A", "DWF.B"),
                ("DWM.RESULT_B", "DWF.Z"),
            ],
        )

    def test_technical_only_schedule_fact_is_not_reintroduced(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(),
            schedule_snapshot(
                schedule_edge("DLO.TECHNICAL_ONLY", "DWF.RESULT_A"),
                schedule_edge("DWF.TMP_1", "DWF.RESULT_A"),
            ),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
            target_table="DWF.RESULT_A",
        )
        self.assertEqual(result.rows, ())
        self.assertEqual(
            result.target_summaries[0].status, TargetSummaryStatus.CONSISTENT
        )

    def test_snapshot_metadata_is_returned_without_freshness_policy(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(batch_id="batch-sql-DEMO"),
            schedule_snapshot(batch_id="batch-schedule-DEMO"),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(result.sql_batch_id, "batch-sql-DEMO")
        self.assertEqual(result.schedule_batch_id, "batch-schedule-DEMO")
        self.assertEqual(result.sql_observed_at, OBSERVED_AT)
        self.assertEqual(result.schedule_observed_at, OBSERVED_AT)

    def test_result_serialization_contains_rows_and_target_summary(self):
        result = reconcile_lineage_snapshots(
            sql_snapshot(sql_edge("DWF.A", "DWM.RESULT_A")),
            schedule_snapshot(schedule_edge("DWF.A", "DWM.RESULT_A")),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        payload = result.to_dict()
        self.assertEqual(payload["match"], 1)
        rows = payload["rows"]
        summaries = payload["target_summaries"]
        assert isinstance(rows, list)
        assert isinstance(summaries, list)
        assert isinstance(rows[0], dict)
        assert isinstance(summaries[0], dict)
        self.assertEqual(rows[0]["status"], "MATCH")
        self.assertEqual(summaries[0]["status"], "CONSISTENT")


class ActiveReaderContractTests(unittest.TestCase):
    def test_active_sql_reader_uses_batch_and_active_join_contract(self):
        edge = sql_edge("DWF.A", "DWM.RESULT_A", batch_id="batch-sql")
        reader = FakeSQLReader((edge,))
        snapshot = read_active_sql_business_snapshot(
            reader,
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(snapshot.batch_id, "batch-sql")
        self.assertEqual(snapshot.edges, (edge,))
        self.assertEqual(reader.calls, [("batch-sql", True)])

    def test_active_sql_reader_rejects_missing_declared_scope(self):
        reader = FakeSQLReader(scopes=(("DEMO_OTHER_ENV", "DEMO_OTHER_PROFILE"),))
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_sql_business_snapshot(
                reader,
                environment=ENVIRONMENT,
                source_profile=PROFILE,
            )
        self.assertEqual(context.exception.code, SQL_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_active_sql_reader_returns_only_requested_profile_scope(self):
        selected = sql_edge(
            "DWF.A",
            "DWM.RESULT_A",
            profile=SQL_PROFILE,
            batch_id="batch-sql",
        )
        ignored = sql_edge(
            "DWF.B",
            "DWM.RESULT_A",
            profile=SCHEDULE_PROFILE,
            batch_id="batch-sql",
        )
        reader = FakeSQLReader(
            (ignored, selected),
            scopes=((ENVIRONMENT, SQL_PROFILE), (ENVIRONMENT, SCHEDULE_PROFILE)),
        )

        snapshot = read_active_sql_business_snapshot(
            reader,
            environment=ENVIRONMENT,
            source_profile=SQL_PROFILE,
        )

        self.assertEqual(snapshot.edges, (selected,))

    def test_missing_sql_active_snapshot_fails_closed(self):
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_sql_business_snapshot(
                FakeSQLReader(batch_id=None),
                environment=ENVIRONMENT,
                source_profile=PROFILE,
            )
        self.assertEqual(context.exception.code, SQL_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_missing_sql_profile_scope_fails_closed(self):
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_sql_business_snapshot(
                FakeSQLReader(scopes=((ENVIRONMENT, SCHEDULE_PROFILE),)),
                environment=ENVIRONMENT,
                source_profile=SQL_PROFILE,
            )
        self.assertEqual(context.exception.code, SQL_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_active_schedule_reader_requires_rows_in_requested_scope(self):
        edge = schedule_edge("DWF.A", "DWM.RESULT_A")
        reader = FakeScheduleReader((FakeScheduleRow(edge, "batch-schedule"),))
        snapshot = read_active_schedule_snapshot(
            reader,
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(snapshot.batch_id, "batch-schedule")
        self.assertEqual(snapshot.edges, (edge,))
        self.assertEqual(reader.calls, [("batch-schedule", True)])

    def test_active_schedule_reader_returns_only_requested_profile_scope(self):
        selected = schedule_edge(
            "DWF.A",
            "DWM.RESULT_A",
            profile=SCHEDULE_PROFILE,
        )
        ignored = schedule_edge(
            "DWF.B",
            "DWM.RESULT_A",
            profile=SQL_PROFILE,
        )
        snapshot = read_active_schedule_snapshot(
            FakeScheduleReader(
                (
                    FakeScheduleRow(ignored, "batch-schedule"),
                    FakeScheduleRow(selected, "batch-schedule"),
                )
            ),
            environment=ENVIRONMENT,
            source_profile=SCHEDULE_PROFILE,
        )
        self.assertEqual(snapshot.edges, (selected,))

    def test_schedule_rows_from_other_scope_fail_closed(self):
        other = schedule_edge(
            "DWF.A",
            "DWM.RESULT_A",
            environment="DEMO_OTHER_ENV",
            profile="DEMO_OTHER_PROFILE",
        )
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_schedule_snapshot(
                FakeScheduleReader((FakeScheduleRow(other, "batch-schedule"),)),
                environment=ENVIRONMENT,
                source_profile=PROFILE,
            )
        self.assertEqual(context.exception.code, SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_missing_schedule_snapshot_fails_closed_instead_of_marking_sql_only(self):
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_schedule_snapshot(
                FakeScheduleReader(batch_id=None),
                environment=ENVIRONMENT,
                source_profile=PROFILE,
            )
        self.assertEqual(context.exception.code, SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_missing_schedule_profile_scope_fails_closed(self):
        other = schedule_edge(
            "DWF.A",
            "DWM.RESULT_A",
            profile=SQL_PROFILE,
        )
        with self.assertRaises(ActiveSnapshotNotFoundError) as context:
            read_active_schedule_snapshot(
                FakeScheduleReader((FakeScheduleRow(other, "batch-schedule"),)),
                environment=ENVIRONMENT,
                source_profile=SCHEDULE_PROFILE,
            )
        self.assertEqual(context.exception.code, SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)

    def test_combined_active_reader_reconciliation_returns_both_snapshot_ids(self):
        sql = sql_edge("DWF.A", "DWM.RESULT_A", batch_id="batch-sql")
        schedule = schedule_edge("DWF.A", "DWM.RESULT_A")
        result = reconcile_active_dws_lineage(
            FakeSQLReader((sql,)),
            FakeScheduleReader((FakeScheduleRow(schedule, "batch-schedule"),)),
            environment=ENVIRONMENT,
            source_profile=PROFILE,
        )
        self.assertEqual(result.sql_batch_id, "batch-sql")
        self.assertEqual(result.schedule_batch_id, "batch-schedule")
        self.assertEqual(result.rows[0].status, ReconciliationStatus.MATCH)

    def test_combined_active_reader_uses_independent_profile_scopes(self):
        sql = sql_edge(
            "DWF.A",
            "DWM.RESULT_A",
            profile=SQL_PROFILE,
            batch_id="batch-sql",
        )
        schedule = schedule_edge(
            "DWF.A",
            "DWM.RESULT_A",
            profile=SCHEDULE_PROFILE,
        )
        result = reconcile_active_dws_lineage(
            FakeSQLReader(
                (sql,),
                scopes=((ENVIRONMENT, SQL_PROFILE),),
            ),
            FakeScheduleReader((FakeScheduleRow(schedule, "batch-schedule"),)),
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
        )
        self.assertEqual(result.rows[0].status, ReconciliationStatus.MATCH)
        self.assertEqual(result.sql_source_profile, SQL_PROFILE)
        self.assertEqual(result.schedule_source_profile, SCHEDULE_PROFILE)


class CliReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.result = reconcile_lineage_snapshots(
            sql_snapshot(
                sql_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    profile=SQL_PROFILE,
                ),
                profile=SQL_PROFILE,
            ),
            schedule_snapshot(
                schedule_edge(
                    "DWF.A",
                    "DWM.RESULT_A",
                    profile=SCHEDULE_PROFILE,
                )
            ),
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
        )

    def test_cli_parser_supports_scope_target_and_formats(self):
        args = build_parser().parse_args(
            [
                "--dws-profile",
                "DEMO_DWS_PROFILE",
                "--environment",
                ENVIRONMENT,
                "--profile",
                PROFILE,
                "--target",
                "DWM.RESULT_A",
                "--format",
                "json",
            ]
        )
        self.assertEqual(args.dws_profile, "DEMO_DWS_PROFILE")
        self.assertEqual(args.source_profile, PROFILE)
        self.assertEqual(args.sql_source_profile, PROFILE)
        self.assertEqual(args.schedule_source_profile, PROFILE)
        self.assertEqual(args.output_format, "json")

    def test_cli_parser_supports_different_side_profiles(self):
        args = build_parser().parse_args(
            [
                "--environment",
                ENVIRONMENT,
                "--sql-profile",
                SQL_PROFILE,
                "--schedule-profile",
                SCHEDULE_PROFILE,
            ]
        )
        self.assertIsNone(args.source_profile)
        self.assertEqual(args.sql_source_profile, SQL_PROFILE)
        self.assertEqual(args.schedule_source_profile, SCHEDULE_PROFILE)

    def test_cli_parser_rejects_missing_profile_scope(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["--environment", ENVIRONMENT])

    def test_cli_parser_rejects_conflicting_legacy_profile(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                [
                    "--environment",
                    ENVIRONMENT,
                    "--profile",
                    PROFILE,
                    "--sql-profile",
                    SQL_PROFILE,
                    "--schedule-profile",
                    PROFILE,
                ]
            )

    def test_cli_parser_allows_matching_legacy_and_explicit_profiles(self):
        args = build_parser().parse_args(
            [
                "--environment",
                ENVIRONMENT,
                "--profile",
                PROFILE,
                "--sql-profile",
                PROFILE,
                "--schedule-profile",
                PROFILE,
            ]
        )
        self.assertEqual(args.sql_source_profile, PROFILE)
        self.assertEqual(args.schedule_source_profile, PROFILE)

    def test_target_table_output_matches_requested_shape(self):
        output = render_table(
            self.result,
            target_table="DWM.RESULT_A",
            elapsed_ms=3,
        )
        self.assertIn("Target: DWM.RESULT_A", output)
        self.assertIn("Status: CONSISTENT", output)
        self.assertIn(f"SQL profile: {SQL_PROFILE}", output)
        self.assertIn(f"Schedule profile: {SCHEDULE_PROFILE}", output)
        self.assertIn("DWF.A", output)
        self.assertIn("MATCH", output)

    def test_full_scope_table_is_aggregate_and_json_is_machine_readable(self):
        table = render_table(self.result, elapsed_ms=3)
        self.assertIn("reconciliation_rows=1", table)
        self.assertIn(f"sql_profile={SQL_PROFILE}", table)
        self.assertIn(f"schedule_profile={SCHEDULE_PROFILE}", table)
        self.assertNotIn("\nprofile=", table)
        self.assertNotIn("DWF.A", table)
        payload = json.loads(render_json(self.result, elapsed_ms=3))
        self.assertEqual(payload["elapsed_ms"], 3)
        self.assertEqual(payload["sql_batch_id"], "batch-sql")
        self.assertEqual(payload["sql_source_profile"], SQL_PROFILE)
        self.assertEqual(payload["schedule_source_profile"], SCHEDULE_PROFILE)
        self.assertNotIn("source_profile", payload)
        self.assertEqual(len(payload["rows"]), 1)

    def test_csv_contains_deterministic_row_contract(self):
        output = render_csv(self.result)
        self.assertTrue(
            output.startswith(
                "ENVIRONMENT,SQL_SOURCE_PROFILE,SCHEDULE_SOURCE_PROFILE,TARGET_TABLE"
            )
        )
        self.assertNotIn("SOURCE_PROFILE", output.splitlines()[0].split(","))
        self.assertIn(f"{SQL_PROFILE},{SCHEDULE_PROFILE},DWM.RESULT_A,DWF.A", output)
        self.assertIn(",MATCH,", output)


if __name__ == "__main__":
    unittest.main()
