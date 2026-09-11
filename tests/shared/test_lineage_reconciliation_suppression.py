from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from shared.lineage.domain import LineageEdge
from shared.lineage.dws_timestamp import TIMESTAMPTZ_PARAM_SQL
from shared.lineage.environment_scope import LineageEnvironmentScope
from shared.lineage.reconciliation import (
    ReconciliationStatus,
    ScheduleLineageSnapshot,
    SQLBusinessLineageSnapshot,
    reconcile_lineage_snapshots,
)
from shared.lineage.reconciliation_suppression import (
    DWSReconciliationSuppressionStore,
    ReconciliationSuppression,
    ReconciliationSuppressionReason,
    SUPPRESSION_CLASSIFIER_VERSION,
    classify_reconciliation_suppressions,
    compute_reconciliation_suppression_key,
    compute_reconciliation_suppression_row_key,
    usable_suppressed_edge_keys,
)
from shared.lineage.schedule import ScheduleLineageEdge
from jobs.crontab.imp_lineage_suppression import run

ENVIRONMENT = "DEMO_DEV"
OTHER_ENVIRONMENT = "DEMO_OTHER"
SQL_PROFILE = "DEMO_SQL_PROFILE"
SCHEDULE_PROFILE = "DEMO_SCHEDULE_PROFILE"
OTHER_SQL_PROFILE = "DEMO_OTHER_SQL_PROFILE"
OTHER_SCHEDULE_PROFILE = "DEMO_OTHER_SCHEDULE_PROFILE"
OBSERVED_AT = datetime(2026, 9, 10, 8, 9, 10, tzinfo=timezone.utc)

SUPPRESSION_SCHEMA_SQL = """
CREATE TABLE dwp.lineage_reconciliation_suppression (
    row_key TEXT NOT NULL,
    suppression_key TEXT NOT NULL,
    environment TEXT NOT NULL,
    sql_source_profile TEXT NOT NULL,
    schedule_source_profile TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    raw_status TEXT NOT NULL,
    suppression_reason TEXT NOT NULL,
    sql_batch_id TEXT NOT NULL,
    schedule_batch_id TEXT NOT NULL,
    classifier_version TEXT NOT NULL,
    observed_at TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    is_active INTEGER DEFAULT FALSE,
    created_at TEXT,
    updated_at TEXT
)
"""


def sql_edge(
    source: str,
    target: str,
    *,
    environment: str = ENVIRONMENT,
    profile: str = SQL_PROFILE,
    batch_id: str | None = None,
) -> LineageEdge:
    return LineageEdge(
        environment=environment,
        source_profile=profile,
        source_table=source,
        target_table=target,
        program_name="DEMO_PROGRAM",
        batch_id=batch_id,
        observed_at=OBSERVED_AT,
    )


def schedule_edge(
    source: str,
    target: str,
    *,
    environment: str = ENVIRONMENT,
    profile: str = SCHEDULE_PROFILE,
) -> ScheduleLineageEdge:
    return ScheduleLineageEdge(
        environment=environment,
        source_profile=profile,
        process_name="DEMO_PROCESS",
        project_version_key="DEMO_PROJECT:1.0",
        raw_source_table=source,
        raw_target_table=target,
    )


def make_sql_snapshot(
    *edges: LineageEdge,
    batch_id: str = "batch-sql-1",
    profile: str = SQL_PROFILE,
) -> SQLBusinessLineageSnapshot:
    return SQLBusinessLineageSnapshot(
        batch_id=batch_id,
        edges=edges,
        observed_at=OBSERVED_AT,
        snapshot_scope=((ENVIRONMENT, profile),),
    )


def make_schedule_snapshot(
    *edges: ScheduleLineageEdge,
    batch_id: str = "batch-schedule-1",
) -> ScheduleLineageSnapshot:
    return ScheduleLineageSnapshot(
        batch_id=batch_id,
        edges=edges,
        observed_at=OBSERVED_AT,
    )


def make_result(
    sql_snapshot: SQLBusinessLineageSnapshot,
    schedule_snapshot: ScheduleLineageSnapshot,
):
    return reconcile_lineage_snapshots(
        sql_snapshot,
        schedule_snapshot,
        environment=ENVIRONMENT,
        sql_source_profile=SQL_PROFILE,
        schedule_source_profile=SCHEDULE_PROFILE,
        target_table="DEMO_DWM.RESULT_A",
    )


def make_no_producer_result():
    sql = make_sql_snapshot(
        sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A", batch_id="batch-sql-1")
    )
    schedule = make_schedule_snapshot(
        schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT")
    )
    return sql, schedule, make_result(sql, schedule)


class ReconciliationSuppressionClassifierTests(unittest.TestCase):
    def test_sql_only_without_either_internal_producer_is_suppressed(self):
        sql, schedule, result = make_no_producer_result()

        suppressions = classify_reconciliation_suppressions(result, sql, schedule)

        self.assertEqual(len(suppressions), 1)
        candidate = suppressions[0]
        self.assertEqual(candidate.source_table, "DEMO_DWF.REFERENCE_A")
        self.assertEqual(candidate.target_table, "DEMO_DWM.RESULT_A")
        self.assertEqual(candidate.raw_status, ReconciliationStatus.SQL_ONLY)
        self.assertEqual(
            candidate.suppression_reason,
            ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
        )
        self.assertEqual(candidate.classifier_version, SUPPRESSION_CLASSIFIER_VERSION)

    def test_sql_producer_prevents_suppression(self):
        sql = make_sql_snapshot(
            sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A"),
            sql_edge("DEMO_DWF.INTERNAL_A", "DEMO_DWF.REFERENCE_A"),
        )
        schedule = make_schedule_snapshot(
            schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT")
        )

        self.assertEqual(
            classify_reconciliation_suppressions(
                make_result(sql, schedule), sql, schedule
            ),
            (),
        )

    def test_schedule_producer_prevents_suppression(self):
        sql = make_sql_snapshot(sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A"))
        schedule = make_schedule_snapshot(
            schedule_edge("DEMO_DWF.INTERNAL_A", "DEMO_DWF.REFERENCE_A"),
            schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),
        )

        self.assertEqual(
            classify_reconciliation_suppressions(
                make_result(sql, schedule), sql, schedule
            ),
            (),
        )

    def test_both_producer_types_prevent_suppression(self):
        sql = make_sql_snapshot(
            sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A"),
            sql_edge("DEMO_DWF.SQL_PRODUCER", "DEMO_DWF.REFERENCE_A"),
        )
        schedule = make_schedule_snapshot(
            schedule_edge("DEMO_DWF.SCHEDULE_PRODUCER", "DEMO_DWF.REFERENCE_A"),
            schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),
        )

        self.assertEqual(
            classify_reconciliation_suppressions(
                make_result(sql, schedule), sql, schedule
            ),
            (),
        )

    def test_producer_target_uses_formal_comparison_normalization(self):
        sql = make_sql_snapshot(
            sql_edge("DWF.REFERENCE_A", "DWM.RESULT_A"),
            sql_edge("DWF.INTERNAL_A", "DWS_DWF.REFERENCE_A"),
        )
        schedule = make_schedule_snapshot(
            schedule_edge("DWF.OTHER_A", "DWM.OTHER_RESULT")
        )
        result = reconcile_lineage_snapshots(
            sql,
            schedule,
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_table="DWM.RESULT_A",
        )

        self.assertEqual(
            classify_reconciliation_suppressions(result, sql, schedule), ()
        )

    def test_match_and_schedule_only_are_never_suppressed(self):
        sql = make_sql_snapshot(
            sql_edge("DEMO_DWF.MATCH_A", "DEMO_DWM.RESULT_A"),
            sql_edge("DEMO_DWF.SQL_ONLY_A", "DEMO_DWM.RESULT_A"),
        )
        schedule = make_schedule_snapshot(
            schedule_edge("DEMO_DWF.MATCH_A", "DEMO_DWM.RESULT_A"),
            schedule_edge("DEMO_DWF.SCHEDULE_ONLY_A", "DEMO_DWM.RESULT_A"),
            schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),
        )
        result = make_result(sql, schedule)

        self.assertEqual(
            [row.status for row in result.rows],
            [
                ReconciliationStatus.MATCH,
                ReconciliationStatus.SCHEDULE_ONLY,
                ReconciliationStatus.SQL_ONLY,
            ],
        )
        suppressions = classify_reconciliation_suppressions(result, sql, schedule)
        self.assertEqual(
            [(item.source_table, item.target_table) for item in suppressions],
            [("DEMO_DWF.SQL_ONLY_A", "DEMO_DWM.RESULT_A")],
        )

    def test_identity_environment_and_profiles_are_strictly_isolated(self):
        sql = make_sql_snapshot(
            sql_edge("SCHEMA_A.TABLE_X", "DEMO_DWM.RESULT_A"),
            sql_edge(
                "DEMO_DWF.OTHER_ENV_PRODUCER",
                "SCHEMA_A.TABLE_X",
                environment=OTHER_ENVIRONMENT,
            ),
            sql_edge(
                "DEMO_DWF.OTHER_PROFILE_PRODUCER",
                "SCHEMA_A.TABLE_X",
                profile=OTHER_SQL_PROFILE,
            ),
            sql_edge("SCHEMA_B.TABLE_X", "DEMO_DWM.OTHER_RESULT"),
        )
        schedule = make_schedule_snapshot(
            schedule_edge(
                "DEMO_DWF.OTHER_ENV_SCHEDULE",
                "SCHEMA_A.TABLE_X",
                environment=OTHER_ENVIRONMENT,
            ),
            schedule_edge(
                "DEMO_DWF.OTHER_PROFILE_SCHEDULE",
                "SCHEMA_A.TABLE_X",
                profile=OTHER_SCHEDULE_PROFILE,
            ),
            schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),
        )
        result = make_result(sql, schedule)

        suppressions = classify_reconciliation_suppressions(result, sql, schedule)

        self.assertEqual(
            [(item.source_table, item.target_table) for item in suppressions],
            [("SCHEMA_A.TABLE_X", "DEMO_DWM.RESULT_A")],
        )
        self.assertNotEqual(
            "SCHEMA_A.TABLE_X",
            "SCHEMA_B.TABLE_X",
            "different qualified schemas are not interchangeable",
        )

    def test_classifier_is_deterministic_and_does_not_mutate_raw_result(self):
        sql, schedule, result = make_no_producer_result()
        original_rows = result.rows

        first = classify_reconciliation_suppressions(result, sql, schedule)
        second = classify_reconciliation_suppressions(result, sql, schedule)

        self.assertEqual(first, second)
        self.assertEqual(result.rows, original_rows)
        self.assertEqual(result.rows[0].status, ReconciliationStatus.SQL_ONLY)

    def test_missing_scope_or_batch_provenance_fails_open(self):
        sql, schedule, result = make_no_producer_result()
        incomplete_sql = SQLBusinessLineageSnapshot(
            batch_id=sql.batch_id,
            edges=sql.edges,
            observed_at=sql.observed_at,
        )
        with self.assertRaisesRegex(Exception, "scope"):
            classify_reconciliation_suppressions(result, incomplete_sql, schedule)

        stale_schedule = make_schedule_snapshot(
            *schedule.edges,
            batch_id="batch-schedule-old",
        )
        with self.assertRaisesRegex(Exception, "batch"):
            classify_reconciliation_suppressions(result, sql, stale_schedule)

    def test_stable_suppression_identity_excludes_batch_ids(self):
        first = ReconciliationSuppression(
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            source_table="DEMO_DWF.REFERENCE_A",
            target_table="DEMO_DWM.RESULT_A",
            raw_status=ReconciliationStatus.SQL_ONLY,
            suppression_reason=ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
            sql_batch_id="batch-sql-1",
            schedule_batch_id="batch-schedule-1",
            observed_at=OBSERVED_AT,
        )
        second = ReconciliationSuppression(
            environment=first.environment,
            sql_source_profile=first.sql_source_profile,
            schedule_source_profile=first.schedule_source_profile,
            source_table=first.source_table,
            target_table=first.target_table,
            raw_status=first.raw_status,
            suppression_reason=first.suppression_reason,
            sql_batch_id="batch-sql-2",
            schedule_batch_id="batch-schedule-2",
            classifier_version=first.classifier_version,
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(first.suppression_key, second.suppression_key)
        self.assertNotEqual(first.row_key, second.row_key)
        self.assertEqual(
            first.suppression_key,
            compute_reconciliation_suppression_key(
                environment=ENVIRONMENT,
                sql_source_profile=SQL_PROFILE,
                schedule_source_profile=SCHEDULE_PROFILE,
                source_table="DEMO_DWF.REFERENCE_A",
                target_table="DEMO_DWM.RESULT_A",
                suppression_reason=ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
            ),
        )
        self.assertEqual(
            first.row_key,
            compute_reconciliation_suppression_row_key(
                suppression_key=first.suppression_key,
                sql_batch_id="batch-sql-1",
                schedule_batch_id="batch-schedule-1",
            ),
        )


class _TimestampBoundaryCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, parameters=()):
        return self._cursor.execute(
            sql.replace(TIMESTAMPTZ_PARAM_SQL, "?"), tuple(parameters)
        )

    def executemany(self, sql, rows):
        return self._cursor.executemany(
            sql.replace(TIMESTAMPTZ_PARAM_SQL, "?"), tuple(tuple(row) for row in rows)
        )

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _TimestampBoundaryConnection:
    def __init__(self, database):
        self.database = database

    def cursor(self):
        return _TimestampBoundaryCursor(self.database.cursor())

    def __getattr__(self, name):
        return getattr(self.database, name)


class ReconciliationSuppressionStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = sqlite3.connect(":memory:")
        self.database.execute("ATTACH DATABASE ':memory:' AS dwp")
        self.database.executescript(SUPPRESSION_SCHEMA_SQL)
        self.connection = _TimestampBoundaryConnection(self.database)
        self.store = DWSReconciliationSuppressionStore(connection=self.connection)
        self.sql, self.schedule, self.result = make_no_producer_result()
        self.candidates = classify_reconciliation_suppressions(
            self.result, self.sql, self.schedule, observed_at=OBSERVED_AT
        )

    def tearDown(self) -> None:
        self.database.close()

    def publish(
        self,
        candidates,
        *,
        sql_batch="batch-sql-1",
        schedule_batch="batch-schedule-1",
        environment=ENVIRONMENT,
        sql_profile=SQL_PROFILE,
        schedule_profile=SCHEDULE_PROFILE,
        observed_at=OBSERVED_AT,
    ):
        adjusted = tuple(
            ReconciliationSuppression(
                environment=item.environment,
                sql_source_profile=item.sql_source_profile,
                schedule_source_profile=item.schedule_source_profile,
                source_table=item.source_table,
                target_table=item.target_table,
                raw_status=item.raw_status,
                suppression_reason=item.suppression_reason,
                sql_batch_id=sql_batch,
                schedule_batch_id=schedule_batch,
                classifier_version=item.classifier_version,
                observed_at=observed_at,
            )
            for item in candidates
        )
        return self.store.publish(
            adjusted,
            environment=environment,
            sql_source_profile=sql_profile,
            schedule_source_profile=schedule_profile,
            observed_at=observed_at,
        )

    def test_publish_reads_audit_row_and_preserves_provenance(self):
        published = self.publish(self.candidates)

        self.assertEqual(published.suppression_count, 1)
        active = self.store.read_rows(active_only=True)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].raw_status, ReconciliationStatus.SQL_ONLY)
        self.assertEqual(
            active[0].suppression_reason,
            ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
        )
        self.assertEqual(active[0].sql_batch_id, "batch-sql-1")
        self.assertEqual(active[0].schedule_batch_id, "batch-schedule-1")
        self.assertTrue(active[0].is_active)

    def test_new_snapshot_reuses_stable_identity_and_retires_old_active_row(self):
        self.publish(self.candidates)
        first = self.store.read_rows(active_only=True)[0]
        next_observed = OBSERVED_AT + timedelta(days=1)

        self.publish(
            self.candidates,
            sql_batch="batch-sql-2",
            schedule_batch="batch-schedule-2",
            observed_at=next_observed,
        )
        active = self.store.read_rows(active_only=True)
        history = self.store.read_rows()

        self.assertEqual(len(active), 1)
        self.assertEqual(len(history), 2)
        self.assertEqual(active[0].suppression_key, first.suppression_key)
        self.assertNotEqual(active[0].row_key, first.row_key)
        self.assertEqual(active[0].sql_batch_id, "batch-sql-2")
        self.assertEqual(active[0].schedule_batch_id, "batch-schedule-2")
        self.assertEqual(active[0].first_seen_at, first.first_seen_at)
        self.assertFalse(
            next(row for row in history if row.row_key == first.row_key).is_active
        )

    def test_empty_new_snapshot_retires_stale_rows_without_deleting_history(self):
        self.publish(self.candidates)
        self.publish((), sql_batch="batch-sql-2", schedule_batch="batch-schedule-2")

        self.assertEqual(self.store.read_rows(active_only=True), ())
        self.assertEqual(len(self.store.read_rows()), 1)
        self.assertFalse(self.store.read_rows()[0].is_active)

    def test_scope_lifecycle_does_not_retire_other_scope(self):
        other_candidate = ReconciliationSuppression(
            environment=OTHER_ENVIRONMENT,
            sql_source_profile=OTHER_SQL_PROFILE,
            schedule_source_profile=OTHER_SCHEDULE_PROFILE,
            source_table="DEMO_DWF.OTHER_REFERENCE",
            target_table="DEMO_DWM.RESULT_A",
            raw_status=ReconciliationStatus.SQL_ONLY,
            suppression_reason=ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
            sql_batch_id="other-sql",
            schedule_batch_id="other-schedule",
            observed_at=OBSERVED_AT,
        )
        self.store.publish(
            (other_candidate,),
            environment=OTHER_ENVIRONMENT,
            sql_source_profile=OTHER_SQL_PROFILE,
            schedule_source_profile=OTHER_SCHEDULE_PROFILE,
            observed_at=OBSERVED_AT,
        )
        self.publish(())

        active = self.store.read_rows(active_only=True)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].environment, OTHER_ENVIRONMENT)

    def test_stale_audit_row_is_not_usable_for_new_result(self):
        self.publish(self.candidates)
        active = self.store.read_rows(active_only=True)
        self.assertEqual(
            usable_suppressed_edge_keys(self.result, active),
            frozenset({("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A")}),
        )
        stale_result = reconcile_lineage_snapshots(
            make_sql_snapshot(
                sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A", batch_id="new"),
                batch_id="batch-sql-new",
            ),
            make_schedule_snapshot(
                schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),
                batch_id="batch-schedule-new",
            ),
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_table="DEMO_DWM.RESULT_A",
        )
        self.assertEqual(usable_suppressed_edge_keys(stale_result, active), frozenset())


@dataclass(frozen=True, slots=True)
class _FakeScheduleRow:
    edge: ScheduleLineageEdge
    batch_id: str
    observed_at: datetime = OBSERVED_AT
    is_active: bool = True


class _FakeSQLReader:
    def __init__(self, edges):
        self.edges = tuple(edges)

    def get_active_batch_id(self):
        return "batch-sql-command"

    def get_batch_metadata(self, batch_id):
        return SimpleNamespace(is_active=True, observed_at=OBSERVED_AT)

    def get_active_snapshot_scope(self):
        return ((ENVIRONMENT, SQL_PROFILE),)

    def read_edges(self, *, batch_id=None, active_only=False):
        return self.edges


class _FakeScheduleReader:
    def __init__(self, edges):
        self.rows = tuple(
            _FakeScheduleRow(edge, "batch-schedule-command") for edge in edges
        )

    def get_active_batch_id(self):
        return "batch-schedule-command"

    def read_rows(self, *, batch_id=None, active_only=False):
        return self.rows


class MaterializationCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scope = LineageEnvironmentScope(
            name="demo_dev",
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            label="示例开发环境",
            dws_profile="demo",
        )
        self.other_scope = LineageEnvironmentScope(
            name="demo_other",
            environment=OTHER_ENVIRONMENT,
            sql_source_profile=OTHER_SQL_PROFILE,
            schedule_source_profile=OTHER_SCHEDULE_PROFILE,
            label="其它示例环境",
            dws_profile="demo-other",
        )
        self.sql_factory = Mock(
            return_value=_FakeSQLReader(
                (sql_edge("DEMO_DWF.REFERENCE_A", "DEMO_DWM.RESULT_A"),)
            )
        )
        self.schedule_factory = Mock(
            return_value=_FakeScheduleReader(
                (schedule_edge("DEMO_DWF.OTHER_A", "DEMO_DWM.OTHER_RESULT"),)
            )
        )
        self.suppression_factory = Mock()
        self.suppression_store = Mock()
        self.suppression_factory.return_value = self.suppression_store

    def test_dry_run_classifies_without_writing_dws(self):
        summaries = run(
            (self.scope,),
            dry_run=True,
            observed_at=OBSERVED_AT,
            sql_store_factory=self.sql_factory,
            schedule_store_factory=self.schedule_factory,
            suppression_store_factory=self.suppression_factory,
        )

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].raw_sql_only_count, 1)
        self.assertEqual(summaries[0].suppressed_count, 1)
        self.assertEqual(summaries[0].actionable_sql_only_count, 0)
        self.suppression_factory.assert_not_called()

    def test_environment_filter_only_materializes_requested_scope(self):
        summaries = run(
            (self.scope, self.other_scope),
            environment=ENVIRONMENT,
            observed_at=OBSERVED_AT,
            sql_store_factory=self.sql_factory,
            schedule_store_factory=self.schedule_factory,
            suppression_store_factory=self.suppression_factory,
        )

        self.assertEqual([item.environment for item in summaries], [ENVIRONMENT])
        self.suppression_store.publish.assert_called_once()
        self.sql_factory.assert_called_once_with(self.scope)
        self.schedule_factory.assert_called_once_with(self.scope)

    def test_jobs_crontab_entrypoint_main_remains_independently_runnable(self):
        from jobs.crontab.imp_lineage_suppression import main

        result = main(
            scopes=(self.scope,),
            observed_at=OBSERVED_AT,
            sql_store_factory=self.sql_factory,
            schedule_store_factory=self.schedule_factory,
            suppression_store_factory=self.suppression_factory,
        )

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
