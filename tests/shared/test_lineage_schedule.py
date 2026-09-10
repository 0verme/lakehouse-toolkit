from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

from shared.lineage.providers import (
    MySQLProcessProfile,
    ProviderError,
    ScheduleLineageConfig,
)
from shared.lineage.schedule import (
    MySQLScheduleLineageProvider,
    ScheduleLineageEdge,
    ScheduleLineageLoadResult,
    ScheduleLineageLoadStats,
    deduplicate_schedule_edges,
    normalize_schedule_table_key,
)
from shared.lineage.dws_timestamp import TIMESTAMPTZ_PARAM_SQL
from shared.lineage.schedule_materialization import (
    DWSScheduleLineageStore,
    INSERT_SCHEDULE_EDGE_SQL,
    SELECT_SCHEDULE_EDGE_SQL,
)

OBSERVED_AT = datetime(2026, 9, 10, 8, 9, 10, tzinfo=timezone.utc)
SCHEDULE_SCHEMA_SQL = """
CREATE TABLE dwp.lineage_schedule_edge (
    row_key TEXT NOT NULL,
    schedule_edge_key TEXT NOT NULL,
    environment TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    process_name TEXT NOT NULL,
    project_version_key TEXT NOT NULL,
    raw_source_table TEXT NOT NULL,
    raw_target_table TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    observed_at TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    last_changed_at TEXT,
    is_active INTEGER DEFAULT FALSE,
    created_at TEXT,
    updated_at TEXT
)
"""


class FakeScheduleDwsCursor:
    """SQLite cursor that emulates the DWS timestamp binding boundary."""

    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, parameters=()):
        translated = sql.replace(TIMESTAMPTZ_PARAM_SQL, "?")
        return self._cursor.execute(translated, tuple(parameters))

    def executemany(self, sql, rows):
        translated = sql.replace(TIMESTAMPTZ_PARAM_SQL, "?")
        return self._cursor.executemany(translated, tuple(tuple(row) for row in rows))

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class FakeScheduleDwsConnection:
    """Connection proxy for schedule tests without a real DWS."""

    def __init__(self, connection):
        self._connection = connection

    def cursor(self):
        return FakeScheduleDwsCursor(self._connection.cursor())

    def __getattr__(self, name):
        return getattr(self._connection, name)


class FakeScheduleCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.offset = 0
        self.execute_calls: list[tuple[object, ...]] = []
        self.fetchmany_calls: list[int] = []
        self.closed = False

    def execute(self, *args):
        self.execute_calls.append(args)

    def fetchmany(self, size: int):
        self.fetchmany_calls.append(size)
        batch = self.rows[self.offset : self.offset + size]
        self.offset += len(batch)
        return batch

    def close(self):
        self.closed = True


class FakeScheduleConnection:
    def __init__(self, cursor: FakeScheduleCursor):
        self.cursor_instance = cursor
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def make_profile(
    name: str = "mysql_dev_a",
    *,
    environment: str = "DEV",
    schedule_lineage: ScheduleLineageConfig | None = None,
) -> MySQLProcessProfile:
    return MySQLProcessProfile(
        name=name,
        environment=environment,
        connection={
            "host": "127.0.0.1",
            "port": 3306,
            "user": "DEMO_USER",
            "password": "DEMO_PASSWORD",
            "database": "demo_meta",
        },
        process_table="demo_meta.processes",
        program_name_column="process_name",
        script_code_column="script_code",
        schedule_lineage=schedule_lineage
        or ScheduleLineageConfig.from_mapping(
            {
                "enabled": True,
                "table": "demo_meta.schedule_rel",
            }
        ),
    )


def make_edge(
    process_name: str = "DEMO_PROCESS_A",
    *,
    source: str = "DWS_DWF.A",
    target: str = "DWS_DWM.B",
    project: str = "DWD:1.0",
    profile: str = "mysql_dev_a",
) -> ScheduleLineageEdge:
    return ScheduleLineageEdge(
        environment="DEV",
        source_profile=profile,
        process_name=process_name,
        project_version_key=project,
        raw_source_table=source,
        raw_target_table=target,
    )


class ScheduleLineageProviderTests(unittest.TestCase):
    def test_schedule_namespace_normalization_is_scoped_and_strict(self):
        self.assertEqual(
            normalize_schedule_table_key("DWS_DWF.A"),
            "DWF.A",
        )
        self.assertEqual(
            normalize_schedule_table_key("DWS_DWUPRR.C"),
            "DWUPRR.C",
        )
        self.assertEqual(
            normalize_schedule_table_key("DWS_DWM.B"),
            "DWM.B",
        )
        self.assertEqual(
            normalize_schedule_table_key("DEMO_DWF.A"),
            "DEMO_DWF.A",
        )
        self.assertEqual(
            normalize_schedule_table_key("DWF.DWS_TABLE"),
            "DWF.DWS_TABLE",
        )

    def test_config_reuses_process_profile_connection_and_scope(self):
        profile = MySQLProcessProfile.from_mapping(
            {
                "name": "mysql_dev_a",
                "environment": "DEV",
                "connection": {
                    "host": "127.0.0.1",
                    "port": 3306,
                    "user": "DEMO_USER",
                    "password": "DEMO_PASSWORD",
                    "database": "demo_meta",
                },
                "table": "demo_meta.processes",
                "schedule_lineage": {
                    "enabled": True,
                    "table": "demo_meta.demo_schedule_rel",
                    "process_name_column": "process_name",
                    "project_version_column": "project_version_key",
                    "source_table_column": "src_table_key",
                    "target_table_column": "tar_table_key",
                    "include_projects": ["DWD:1.0", "DWM:1.0"],
                    "conditional_projects": [
                        {
                            "project": "DWUPRR:1.0",
                            "target_schema_prefix": "DWS_DWUPRR.",
                        }
                    ],
                },
            }
        )
        self.assertIsNotNone(profile.connection)
        self.assertIsNotNone(profile.schedule_lineage)
        assert profile.schedule_lineage is not None
        self.assertEqual(
            profile.schedule_lineage.include_projects, ("DWD:1.0", "DWM:1.0")
        )
        self.assertEqual(
            profile.schedule_lineage.conditional_projects[0].target_schema_prefix,
            "DWS_DWUPRR.",
        )

    def test_config_rejects_projects_outside_v1_scope(self):
        with self.assertRaisesRegex(ValueError, "unsupported V1 project"):
            ScheduleLineageConfig.from_mapping(
                {
                    "enabled": True,
                    "table": "demo_meta.schedule_rel",
                    "include_projects": ["DW_PROJECT:1.0"],
                }
            )
        with self.assertRaisesRegex(ValueError, "unsupported V1 project"):
            ScheduleLineageConfig.from_mapping(
                {
                    "enabled": True,
                    "table": "demo_meta.schedule_rel",
                    "include_projects": ["DWUPRR:1.0"],
                }
            )

    def test_provider_accepts_existing_dev_environment_names(self):
        for environment in ("DEV", "DEV200", "DEV214", "DEV203", "DEV224"):
            with self.subTest(environment=environment):
                provider = MySQLScheduleLineageProvider(
                    make_profile(environment=environment),
                    connection_factory=lambda settings: None,
                )
                self.assertEqual(provider.environment, environment)

    def test_provider_rejects_prod_and_unrelated_dev_prefixes(self):
        for environment in ("PROD", "PROD214", "DEVICE", "DEV_PROD"):
            with self.subTest(environment=environment):
                with self.assertRaisesRegex(
                    ValueError, "schedule lineage only supports DEV profiles"
                ):
                    MySQLScheduleLineageProvider(make_profile(environment=environment))

    def test_provider_filters_projects_normalizes_and_deduplicates(self):
        profile = make_profile()
        rows = [
            ("DEMO_PROCESS_A", "DWD:1.0", "DWS_DWF.A", "DWS_DWM.B"),
            ("DEMO_PROCESS_UPRR", "DWUPRR:1.0", "DWS_DWUPRR.C", "DWS_DWUPRR.D"),
            ("DEMO_PROCESS_UPRR", "DWUPRR:1.0", "DWS_DWUPRR.C", "DWS_DWUPRR.D"),
            ("DEMO_FILE_SEND", "DWUPRR:1.0", "LOCAL_SOURCE", "KDW_FILE"),
            ("DEMO_EXCLUDED", "DW_PROJECT:1.0", "DWS_DWF.X", "DWS_DWM.Y"),
            ("DEMO_PROCESS_DWM", "DWM:1.0", "DEMO_DWF.SOURCE", "DEMO_DWM.TARGET"),
            ("DEMO_PROCESS_B", "DWD:1.0", "DWS_DWF.A", "DWS_DWM.B"),
        ]
        cursor = FakeScheduleCursor(rows)
        connection = FakeScheduleConnection(cursor)
        provider = MySQLScheduleLineageProvider(
            profile,
            connection_factory=lambda settings: connection,
        )

        result = provider.load()

        self.assertEqual(result.stats.source_rows, 7)
        self.assertEqual(result.stats.accepted_rows, 5)
        self.assertEqual(result.stats.rejected_rows, 2)
        self.assertEqual(result.stats.normalized_edges, 5)
        self.assertEqual(result.stats.deduplicated_edges, 4)
        self.assertEqual(result.stats.selected_edges, 4)
        self.assertTrue(result.stats.source_complete)
        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in result.edges},
            {
                ("DWF.A", "DWM.B"),
                ("DWUPRR.C", "DWUPRR.D"),
                ("DEMO_DWF.SOURCE", "DEMO_DWM.TARGET"),
            },
        )
        self.assertEqual(
            sum(edge.process_name == "DEMO_PROCESS_A" for edge in result.edges), 1
        )
        self.assertEqual(
            sum(edge.process_name == "DEMO_PROCESS_B" for edge in result.edges), 1
        )
        self.assertEqual(cursor.execute_calls[0], (provider.query,))
        self.assertEqual(cursor.fetchmany_calls, [200, 200])
        self.assertTrue(cursor.closed)
        self.assertTrue(connection.closed)

    def test_provider_invalid_in_scope_row_is_fail_closed(self):
        profile = make_profile()
        cursor = FakeScheduleCursor(
            [("DEMO_BAD", "DWD:1.0", "UNQUALIFIED", "DWS_DWM.B")]
        )
        provider = MySQLScheduleLineageProvider(
            profile,
            connection_factory=lambda settings: FakeScheduleConnection(cursor),
        )

        result = provider.load()

        self.assertFalse(result.stats.source_complete)
        self.assertEqual(result.stats.accepted_rows, 0)
        self.assertEqual(result.stats.rejected_rows, 1)
        self.assertEqual(result.stats.invalid_rows, 1)
        self.assertEqual(result.edges, ())

    def test_stable_key_is_deterministic_and_provenance_sensitive(self):
        first = make_edge()
        same = make_edge()
        other_process = make_edge("DEMO_PROCESS_B")

        self.assertEqual(first.schedule_edge_key, same.schedule_edge_key)
        self.assertNotEqual(first.schedule_edge_key, other_process.schedule_edge_key)
        self.assertEqual(
            len(deduplicate_schedule_edges((first, same, other_process))),
            2,
        )


class ScheduleLineageMaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        database = sqlite3.connect(":memory:")
        database.execute("ATTACH DATABASE ':memory:' AS dwp")
        database.executescript(SCHEDULE_SCHEMA_SQL)
        self.connection = FakeScheduleDwsConnection(database)
        self.store = DWSScheduleLineageStore(connection=self.connection)

    def tearDown(self) -> None:
        self.connection.close()

    def publish(
        self,
        edges,
        batch_id: str,
        *,
        observed_at: datetime = OBSERVED_AT,
        complete_snapshot: bool = True,
        scopes=(("DEV", "mysql_dev_a"),),
        stage_hook=None,
    ):
        return self.store.publish(
            edges,
            batch_id=batch_id,
            observed_at=observed_at,
            complete_snapshot=complete_snapshot,
            snapshot_scopes=scopes,
            stage_hook=stage_hook,
        )

    def test_publish_preserves_raw_and_comparison_identity(self):
        edge = make_edge()
        result = self.publish((edge,), "batch-schedule-1")

        self.assertEqual(result.edge_count, 1)
        row = self.store.read_rows(active_only=True)[0]
        self.assertEqual(row.raw_source_table, "DWS_DWF.A")
        self.assertEqual(row.raw_target_table, "DWS_DWM.B")
        self.assertEqual(row.source_table, "DWF.A")
        self.assertEqual(row.target_table, "DWM.B")
        self.assertTrue(row.is_active)
        self.assertEqual(self.store.get_active_batch_id(), "batch-schedule-1")

    def test_repeat_publish_keeps_stable_key_and_updates_history(self):
        edge = make_edge()
        first_observed_at = OBSERVED_AT
        second_observed_at = OBSERVED_AT + timedelta(days=1)
        self.publish(
            (edge,),
            "batch-schedule-1",
            observed_at=first_observed_at,
        )
        first = self.store.read_rows(batch_id="batch-schedule-1")[0]

        self.publish(
            (edge,),
            "batch-schedule-2",
            observed_at=second_observed_at,
        )
        second = self.store.read_rows(batch_id="batch-schedule-2")[0]

        self.assertEqual(first.schedule_edge_key, second.schedule_edge_key)
        self.assertNotEqual(first.row_key, second.row_key)
        self.assertEqual(first.first_seen_at, second.first_seen_at)
        self.assertEqual(second.first_seen_at, first_observed_at)
        self.assertEqual(second.last_seen_at, second_observed_at)
        self.assertEqual(first.updated_at, first_observed_at)
        self.assertEqual(second.updated_at, second_observed_at)
        self.assertEqual(len(self.store.read_rows()), 2)
        self.assertEqual(len(self.store.read_rows(active_only=True)), 1)
        self.assertEqual(self.store.get_active_batch_id(), "batch-schedule-2")

    def test_duplicate_configuration_is_one_fact_but_process_provenance_survives(self):
        first = make_edge("DEMO_PROCESS_A")
        duplicate = make_edge("DEMO_PROCESS_A")
        other_process = make_edge("DEMO_PROCESS_B")

        result = self.publish((first, duplicate, other_process), "batch-schedule-dedup")

        self.assertEqual(result.edge_count, 2)
        self.assertEqual(
            {row.process_name for row in self.store.read_rows(active_only=True)},
            {"DEMO_PROCESS_A", "DEMO_PROCESS_B"},
        )

    def test_profile_scope_and_partial_replay_do_not_delete_other_edges(self):
        profile_b_edge = ScheduleLineageEdge(
            environment="DEV",
            source_profile="mysql_dev_b",
            process_name="DEMO_PROCESS_B",
            project_version_key="DWD:1.0",
            raw_source_table="DWS_DWF.B",
            raw_target_table="DWS_DWM.C",
        )
        edge_a = make_edge("DEMO_PROCESS_A")
        self.publish(
            (edge_a, profile_b_edge),
            "batch-schedule-all",
            scopes=(("DEV", "mysql_dev_a"), ("DEV", "mysql_dev_b")),
        )
        replacement_a = make_edge(
            "DEMO_PROCESS_A2",
            source="DWS_DWF.NEW_A",
            target="DWS_DWM.NEW_B",
        )
        self.publish(
            (replacement_a,), "batch-schedule-profile", scopes=(("DEV", "mysql_dev_a"),)
        )
        active_after_profile = self.store.read_rows(active_only=True)
        self.assertEqual(
            {(row.source_profile, row.process_name) for row in active_after_profile},
            {("mysql_dev_a", "DEMO_PROCESS_A2"), ("mysql_dev_b", "DEMO_PROCESS_B")},
        )

        partial_a = make_edge(
            "DEMO_PROCESS_PARTIAL",
            source="DWS_DWF.PARTIAL_A",
            target="DWS_DWM.PARTIAL_B",
        )
        self.publish(
            (partial_a,),
            "batch-schedule-partial",
            complete_snapshot=False,
            scopes=(("DEV", "mysql_dev_a"),),
        )
        self.assertEqual(
            len(self.store.read_rows(active_only=True)),
            3,
            "partial replay must retain the other active facts",
        )

    def test_candidate_validation_failure_rolls_back_previous_active_snapshot(self):
        first = make_edge()
        self.publish((first,), "batch-schedule-1")
        replacement = make_edge(
            "DEMO_PROCESS_NEW",
            source="DWS_DWF.NEW_A",
            target="DWS_DWM.NEW_B",
        )

        def corrupt_candidate(stage: str) -> None:
            if stage == "after_candidate_insert":
                self.connection.execute(
                    "UPDATE dwp.lineage_schedule_edge SET source_table = ? WHERE batch_id = ?",
                    ("DWF.CORRUPTED", "batch-schedule-2"),
                )

        with self.assertRaisesRegex(ValueError, "stable identity"):
            self.publish(
                (replacement,),
                "batch-schedule-2",
                stage_hook=corrupt_candidate,
            )

        self.assertEqual(self.store.get_active_batch_id(), "batch-schedule-1")
        self.assertEqual(
            self.store.read_rows(batch_id="batch-schedule-2"),
            (),
        )
        self.assertEqual(
            self.store.read_rows(active_only=True)[0].process_name,
            "DEMO_PROCESS_A",
        )

    def test_publish_failure_after_switch_preserves_previous_active_snapshot(self):
        first = make_edge()
        self.publish((first,), "batch-schedule-1")
        replacement = make_edge(
            "DEMO_PROCESS_NEW",
            source="DWS_DWF.NEW_A",
            target="DWS_DWM.NEW_B",
        )

        def fail_after_switch(stage: str) -> None:
            if stage == "after_active_switch":
                raise RuntimeError("controlled schedule publish failure")

        with self.assertRaisesRegex(
            RuntimeError, "controlled schedule publish failure"
        ):
            self.publish(
                (replacement,),
                "batch-schedule-2",
                stage_hook=fail_after_switch,
            )

        self.assertEqual(self.store.get_active_batch_id(), "batch-schedule-1")
        self.assertEqual(self.store.read_rows(batch_id="batch-schedule-2"), ())

    def test_empty_full_snapshot_commits_without_schedule_batch_table(self):
        self.publish((make_edge(),), "batch-schedule-1")

        result = self.publish((), "batch-schedule-empty")

        self.assertEqual(result.edge_count, 0)
        self.assertEqual(self.store.read_rows(active_only=True), ())
        self.assertIsNone(self.store.get_active_batch_id())

    def test_schedule_write_and_read_use_shared_timestamp_contract(self):
        self.assertEqual(INSERT_SCHEDULE_EDGE_SQL.count(TIMESTAMPTZ_PARAM_SQL), 6)
        self.assertIn(
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            f"{TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, "
            f"{TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, ?, "
            f"{TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL})",
            " ".join(INSERT_SCHEDULE_EDGE_SQL.split()),
        )
        for column in (
            "observed_at",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "created_at",
            "updated_at",
        ):
            with self.subTest(column=column):
                self.assertRegex(
                    SELECT_SCHEDULE_EDGE_SQL,
                    rf"CAST\(\s*{column}\s+AS VARCHAR\(128\)\s*\)\s+AS\s+{column}\b",
                )

    def test_schedule_candidate_round_trip_handles_truncated_fractional_seconds(self):
        self.publish((make_edge(),), "batch-schedule-round-trip")
        dws_text = "2026-09-10 18:56:48.5814+08:00"
        self.connection.execute(
            "UPDATE dwp.lineage_schedule_edge SET "
            "observed_at = ?, first_seen_at = ?, last_seen_at = ?, "
            "last_changed_at = ?, created_at = ?, updated_at = ? "
            "WHERE batch_id = ?",
            (dws_text,) * 6 + ("batch-schedule-round-trip",),
        )
        self.connection.commit()

        row = self.store.read_rows(batch_id="batch-schedule-round-trip")[0]
        timestamps = (
            row.observed_at,
            row.first_seen_at,
            row.last_seen_at,
            row.last_changed_at,
            row.created_at,
            row.updated_at,
        )
        for timestamp in timestamps:
            self.assertEqual(timestamp.microsecond, 581400)
            self.assertEqual(timestamp.utcoffset(), timedelta(hours=8))
            self.assertIsNotNone(timestamp.tzinfo)

    def test_writer_uses_parameter_binding_for_schedule_values(self):
        self.assertIn("?", INSERT_SCHEDULE_EDGE_SQL)
        self.assertNotIn("DWS_DWF.A", INSERT_SCHEDULE_EDGE_SQL)


class ScheduleSourceFailureTests(unittest.TestCase):
    def test_cli_accepts_profile_and_dws_profile(self):
        from jobs.crontab.imp_schedule_lineage import build_parser

        args = build_parser().parse_args(
            [
                "--profile",
                "mysql_dev_a",
                "--dws-profile",
                "DEMO_DWS_PROFILE",
            ]
        )

        self.assertEqual(args.profile, ["mysql_dev_a"])
        self.assertEqual(args.dws_profile, "DEMO_DWS_PROFILE")

    def test_job_orchestrates_one_profile_into_dws_store(self):
        from jobs.crontab.imp_schedule_lineage import run

        profile = make_profile()
        edge = make_edge()

        class StaticProvider:
            def __init__(self, selected_profile):
                self.selected_profile = selected_profile

            def load(self, limit=None):
                selected = () if limit == 0 else (edge,)
                return ScheduleLineageLoadResult(
                    edges=selected,
                    stats=ScheduleLineageLoadStats(
                        source_rows=1,
                        accepted_rows=1,
                        normalized_edges=1,
                        deduplicated_edges=1,
                        selected_edges=len(selected),
                    ),
                )

        database = sqlite3.connect(":memory:")
        database.execute("ATTACH DATABASE ':memory:' AS dwp")
        database.executescript(SCHEDULE_SCHEMA_SQL)
        connection = FakeScheduleDwsConnection(database)
        store = DWSScheduleLineageStore(connection=connection)

        result = run(
            [profile],
            dws_profile=None,
            batch_id="batch-schedule-job",
            observed_at=OBSERVED_AT,
            provider_factory=StaticProvider,
            store=store,
        )

        self.assertEqual(result.batch_id, "batch-schedule-job")
        self.assertEqual(len(store.read_edges(active_only=True)), 1)
        connection.close()

    def test_source_read_failure_happens_before_any_dws_publish(self):
        from jobs.crontab.imp_schedule_lineage import run

        profile = make_profile()

        class FailingProvider:
            def __init__(self, selected_profile):
                self.selected_profile = selected_profile

            def load(self, limit=None):
                raise ProviderError("source read failed")

        database = sqlite3.connect(":memory:")
        database.execute("ATTACH DATABASE ':memory:' AS dwp")
        database.executescript(SCHEDULE_SCHEMA_SQL)
        connection = FakeScheduleDwsConnection(database)
        store = DWSScheduleLineageStore(connection=connection)
        store.publish(
            (make_edge(),),
            batch_id="batch-schedule-existing",
            observed_at=OBSERVED_AT,
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "mysql_dev_a"),),
        )

        with self.assertRaises(ProviderError):
            run(
                [profile],
                dws_profile=None,
                provider_factory=FailingProvider,
                store=store,
            )

        self.assertEqual(store.get_active_batch_id(), "batch-schedule-existing")
        connection.close()


if __name__ == "__main__":
    unittest.main()
