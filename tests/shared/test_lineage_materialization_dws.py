from __future__ import annotations

import re
import sqlite3
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from shared.lineage.domain import (
    IssueDisposition,
    LineageIssue,
    ProgramState,
    ProgramSource,
)
from shared.lineage.materialization import MaterializationBatch, materialize_program
from shared.lineage.materialization_dws import (
    ACTIVATE_BATCH_SQL,
    BATCH_SELECT_SQL,
    BUSINESS_EDGE_SELECT_SQL,
    INSERT_BATCH_SQL,
    INSERT_BUSINESS_EDGE_SQL,
    INSERT_ISSUE_SQL,
    INSERT_PHYSICAL_EDGE_SQL,
    INSERT_PROGRAM_STATE_SQL,
    ISSUE_SELECT_SQL,
    PHYSICAL_EDGE_SELECT_SQL,
    PROGRAM_STATE_SELECT_SQL,
    RETIRE_BATCH_SQL,
    DWSMaterializationStore,
    DWSPublishResult,
    TIMESTAMPTZ_PARAM_SQL,
    _begin_transaction,
    _parse_datetime,
    _timestamp_param,
)
from shared.lineage.physical_dag import ProgramPhysicalDAG, build_program_physical_dag
from shared.lineage.version import LINEAGE_PIPELINE_VERSION

OBSERVED_AT = datetime(2026, 2, 1, 8, 9, 10, tzinfo=timezone.utc)

DWS_SMOKE_SCHEMA_SQL = """
CREATE TABLE dwp.lineage_batch (
    batch_id TEXT NOT NULL, snapshot_mode TEXT NOT NULL,
    complete_snapshot INTEGER NOT NULL, snapshot_scope TEXT,
    pipeline_version TEXT, observed_at TEXT, previous_batch_id TEXT,
    publish_status TEXT, published_at TEXT, program_count INTEGER,
    edge_count INTEGER, issue_count INTEGER, is_active INTEGER,
    created_at TEXT, updated_at TEXT
);
CREATE TABLE dwp.lineage_program_state (
    row_key TEXT, program_key TEXT, environment TEXT, source_profile TEXT,
    program_name TEXT, source_hash TEXT, pipeline_version TEXT, batch_id TEXT,
    first_seen_at TEXT, last_seen_at TEXT, last_changed_at TEXT,
    is_active INTEGER, created_at TEXT, updated_at TEXT
);
CREATE TABLE dwp.lineage_edge (
    row_key TEXT, edge_key TEXT, environment TEXT, source_profile TEXT,
    program_key TEXT, program_name TEXT, source_table TEXT, target_table TEXT,
    source_node_kind TEXT, target_node_kind TEXT, source_dataset_key TEXT,
    target_dataset_key TEXT, evidence_type TEXT, evidence_json TEXT,
    source_hash TEXT, pipeline_version TEXT, batch_id TEXT, observed_at TEXT,
    first_seen_at TEXT, last_seen_at TEXT, last_changed_at TEXT,
    is_active INTEGER, created_at TEXT, updated_at TEXT
);
CREATE TABLE dwp.lineage_business_edge (
    row_key TEXT, business_edge_key TEXT, environment TEXT,
    source_profile TEXT, program_key TEXT, program_name TEXT,
    source_dataset_key TEXT, source_table TEXT, target_dataset_key TEXT,
    target_table TEXT, collapse_depth INTEGER, physical_derivation_hash TEXT,
    source_hash TEXT, pipeline_version TEXT, batch_id TEXT, observed_at TEXT,
    first_seen_at TEXT, last_seen_at TEXT, last_changed_at TEXT,
    is_active INTEGER, created_at TEXT, updated_at TEXT
);
CREATE TABLE dwp.lineage_issue (
    row_key TEXT, stable_issue_key TEXT, environment TEXT, source_profile TEXT,
    program_key TEXT, program_name TEXT, issue_type TEXT, confidence TEXT,
    rule_version TEXT, severity TEXT, disposition TEXT, policy_version TEXT,
    node_key TEXT, branch_sink TEXT, message TEXT, evidence_json TEXT,
    batch_id TEXT, first_seen_at TEXT, last_seen_at TEXT, last_changed_at TEXT,
    disposition_updated_at TEXT, disposition_updated_by TEXT, is_active INTEGER,
    created_at TEXT, updated_at TEXT
);
"""

_TIMESTAMP_COLUMNS = {
    "observed_at",
    "published_at",
    "first_seen_at",
    "last_seen_at",
    "last_changed_at",
    "disposition_updated_at",
    "created_at",
    "updated_at",
}
_WRITE_SQL = (
    INSERT_BATCH_SQL,
    INSERT_PROGRAM_STATE_SQL,
    INSERT_PHYSICAL_EDGE_SQL,
    INSERT_BUSINESS_EDGE_SQL,
    INSERT_ISSUE_SQL,
    RETIRE_BATCH_SQL,
    ACTIVATE_BATCH_SQL,
)
_WRITE_SQL_BY_NORMALIZED = {" ".join(sql.split()): sql for sql in _WRITE_SQL}
_READ_TIMESTAMP_PROJECTIONS = {
    "lineage_batch": (
        BATCH_SELECT_SQL,
        ("observed_at", "published_at", "created_at", "updated_at"),
    ),
    "lineage_program_state": (
        PROGRAM_STATE_SELECT_SQL,
        (
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "created_at",
            "updated_at",
        ),
    ),
    "lineage_edge": (
        PHYSICAL_EDGE_SELECT_SQL,
        (
            "observed_at",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "created_at",
            "updated_at",
        ),
    ),
    "lineage_business_edge": (
        BUSINESS_EDGE_SELECT_SQL,
        (
            "observed_at",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "created_at",
            "updated_at",
        ),
    ),
    "lineage_issue": (
        ISSUE_SELECT_SQL,
        (
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "disposition_updated_at",
            "created_at",
            "updated_at",
        ),
    ),
}


def _split_sql_arguments(expression: str) -> tuple[str, ...]:
    arguments: list[str] = []
    start = 0
    depth = 0
    quoted = False
    for index, character in enumerate(expression):
        if character == "'":
            quoted = not quoted
        elif not quoted and character == "(":
            depth += 1
        elif not quoted and character == ")":
            depth -= 1
        elif not quoted and character == "," and depth == 0:
            arguments.append(expression[start:index].strip())
            start = index + 1
    arguments.append(expression[start:].strip())
    return tuple(arguments)


def _insert_contract(sql: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    match = re.search(
        r"INSERT INTO\s+[^()]+\((.*?)\)\s*VALUES\s*\((.*)\)\s*$",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"cannot parse INSERT SQL: {sql!r}")
    return (
        _split_sql_arguments(match.group(1)),
        _split_sql_arguments(match.group(2)),
    )


class _FakeJDBCCursor:
    """SQLite-backed cursor that enforces the DWS JDBC parameter boundary."""

    def __init__(self, cursor: Any, calls: list[tuple[str, str, tuple[Any, ...]]]):
        self._cursor = cursor
        self._calls = calls

    @staticmethod
    def _normalized(sql: str) -> str:
        return " ".join(sql.split())

    def _assert_timestamp_contract(
        self, sql: str, row: tuple[Any, ...]
    ) -> None:
        normalized = self._normalized(sql)
        if normalized not in _WRITE_SQL_BY_NORMALIZED:
            return

        if normalized in {
            self._normalized(INSERT_BATCH_SQL),
            self._normalized(INSERT_PROGRAM_STATE_SQL),
            self._normalized(INSERT_PHYSICAL_EDGE_SQL),
            self._normalized(INSERT_BUSINESS_EDGE_SQL),
            self._normalized(INSERT_ISSUE_SQL),
        }:
            columns, expressions = _insert_contract(sql)
            if len(columns) != len(expressions) or len(row) != len(columns):
                raise AssertionError(
                    "DWS INSERT columns, expressions, and parameters differ"
                )
            for index, (column, expression, value) in enumerate(
                zip(columns, expressions, row)
            ):
                if column in _TIMESTAMP_COLUMNS:
                    if expression != TIMESTAMPTZ_PARAM_SQL:
                        raise AssertionError(
                            "VARCHAR directly bound to timestamptz: "
                            f"{column} uses {expression!r}"
                        )
                    if value is not None and not isinstance(value, str):
                        raise AssertionError(
                            f"timestamp parameter {column} is not ISO text"
                        )
                elif expression != "?":
                    raise AssertionError(
                        f"non-timestamp column {column} has unexpected expression"
                    )
            return

        if normalized == self._normalized(RETIRE_BATCH_SQL):
            if not re.search(
                rf"updated_at\s*=\s*{re.escape(TIMESTAMPTZ_PARAM_SQL)}",
                sql,
                flags=re.IGNORECASE,
            ):
                raise AssertionError("retire batch timestamp is not explicitly cast")
            if len(row) != 1 or (row[0] is not None and not isinstance(row[0], str)):
                raise AssertionError("retire batch timestamp binding is invalid")
            return

        if normalized == self._normalized(ACTIVATE_BATCH_SQL):
            for column in ("published_at", "updated_at"):
                if not re.search(
                    rf"{column}\s*=\s*{re.escape(TIMESTAMPTZ_PARAM_SQL)}",
                    sql,
                    flags=re.IGNORECASE,
                ):
                    raise AssertionError(
                        f"activate batch timestamp {column} is not explicitly cast"
                    )
            if len(row) != 3:
                raise AssertionError("activate batch parameter count is invalid")
            for value in row[:2]:
                if value is not None and not isinstance(value, str):
                    raise AssertionError("activate batch timestamp is not ISO text")

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        row = tuple(parameters)
        self._assert_timestamp_contract(sql, row)
        self._calls.append(("execute", sql, row))
        translated = sql.replace(TIMESTAMPTZ_PARAM_SQL, "?")
        return self._cursor.execute(translated, row)

    def executemany(
        self, sql: str, rows: tuple[tuple[Any, ...], ...]
    ) -> Any:
        materialized_rows = tuple(tuple(row) for row in rows)
        for row in materialized_rows:
            self._assert_timestamp_contract(sql, row)
        self._calls.append(("executemany", sql, materialized_rows))
        translated = sql.replace(TIMESTAMPTZ_PARAM_SQL, "?")
        return self._cursor.executemany(translated, materialized_rows)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _FakeJDBCConnection:
    """Connection proxy for tests without connecting to a real DWS."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    def cursor(self) -> _FakeJDBCCursor:
        return _FakeJDBCCursor(self._connection.cursor(), self.calls)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class DWSMaterializationStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute("ATTACH DATABASE ':memory:' AS dwp")
        self.connection.executescript(DWS_SMOKE_SCHEMA_SQL)
        self.jdbc_boundary = _FakeJDBCConnection(self.connection)
        self.store = DWSMaterializationStore(connection=self.jdbc_boundary)

    def tearDown(self) -> None:
        self.connection.close()

    @staticmethod
    def make_batch(
        source: ProgramSource,
        *,
        batch_id: str,
        observed_at: datetime,
        dag: ProgramPhysicalDAG | None = None,
    ) -> tuple[MaterializationBatch, ProgramPhysicalDAG]:
        dag = dag or build_program_physical_dag(source)
        materialization = materialize_program(
            dag,
            batch_id=batch_id,
            observed_at=observed_at,
        )
        state = ProgramState(
            environment=source.environment,
            source_profile=source.source_profile,
            program_name=source.program_name,
            source_hash=source.source_hash,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            last_changed_at=observed_at,
            batch_id=batch_id,
            pipeline_version=LINEAGE_PIPELINE_VERSION,
        )
        return (
            MaterializationBatch(
                batch_id=batch_id,
                observed_at=observed_at,
                edges=materialization.edges,
                issues=materialization.issues,
                program_states=(state,),
            ),
            dag,
        )

    def test_transaction_helpers_restore_dbapi_and_jdbc_autocommit(self) -> None:
        class DbApiConnection:
            autocommit = True

        dbapi_connection = DbApiConnection()
        restore_dbapi = _begin_transaction(dbapi_connection)
        self.assertFalse(dbapi_connection.autocommit)
        restore_dbapi()
        self.assertTrue(dbapi_connection.autocommit)

        class JdbcConnection:
            def __init__(self) -> None:
                self.autocommit = True
                self.calls: list[bool] = []

            def getAutoCommit(self) -> bool:
                return self.autocommit

            def setAutoCommit(self, value: bool) -> None:
                self.calls.append(value)
                self.autocommit = value

        class JdbcWrapper:
            def __init__(self, jconn: JdbcConnection) -> None:
                self.jconn = jconn

        jdbc_connection = JdbcConnection()
        restore_jdbc = _begin_transaction(JdbcWrapper(jdbc_connection))
        self.assertFalse(jdbc_connection.autocommit)
        restore_jdbc()
        self.assertTrue(jdbc_connection.autocommit)
        self.assertEqual(jdbc_connection.calls, [False, True])

        class BeginConnection:
            def __init__(self) -> None:
                self.begun = False

            def begin(self) -> None:
                self.begun = True

        begin_connection = BeginConnection()
        restore_begin = _begin_transaction(begin_connection)
        self.assertTrue(begin_connection.begun)
        restore_begin()

    def test_timestamp_param_preserves_offset_and_null(self) -> None:
        offset = timezone(timedelta(hours=5, minutes=30))
        value = datetime(2026, 2, 1, 8, 9, 10, 123456, tzinfo=offset)

        self.assertEqual(_timestamp_param(value, "observed_at"), value.isoformat())
        self.assertIsNone(_timestamp_param(None, "disposition_updated_at"))

    def test_parse_datetime_supports_python_310_dws_offsets(self) -> None:
        expected_utc = datetime(
            2026, 1, 15, 3, 4, 5, 123456, tzinfo=timezone.utc
        )
        expected_shanghai = expected_utc.astimezone(timezone(timedelta(hours=8)))
        expected_minus_5 = datetime(
            2026,
            1,
            15,
            11,
            4,
            5,
            123456,
            tzinfo=timezone(timedelta(hours=-5)),
        )
        expected_minus_530 = datetime(
            2026,
            1,
            15,
            11,
            4,
            5,
            123456,
            tzinfo=timezone(timedelta(hours=-5, minutes=-30)),
        )
        values = {
            "2026-01-15 11:04:05.123456+08": expected_shanghai,
            "2026-01-15 03:04:05.123456+00": expected_utc,
            "2026-01-15 11:04:05.123456+08:00": expected_shanghai,
            "2026-01-15 11:04:05.123456+0800": expected_shanghai,
            "2026-01-15 11:04:05.123456-05": expected_minus_5,
            "2026-01-15 11:04:05.123456-0530": expected_minus_530,
            "2026-01-15T03:04:05.123456Z": expected_utc,
        }
        for text, expected in values.items():
            with self.subTest(text=text):
                parsed = _parse_datetime(text, "observed_at")
                self.assertEqual(parsed, expected)
                self.assertIsNotNone(parsed.tzinfo)
                self.assertIsNotNone(parsed.utcoffset())

        aware = datetime(2026, 1, 15, 3, 4, 5, tzinfo=timezone.utc)
        self.assertIs(_parse_datetime(aware, "observed_at"), aware)
        with self.assertRaisesRegex(ValueError, "timezone offset"):
            _parse_datetime(datetime(2026, 1, 15, 3, 4, 5), "observed_at")
        with self.assertRaisesRegex(ValueError, "timezone offset"):
            _parse_datetime("2026-01-15 03:04:05", "observed_at")

        same_instant = _parse_datetime(
            "2026-01-15 03:04:05.123456+00", "observed_at"
        )
        same_instant_local = _parse_datetime(
            "2026-01-15 11:04:05.123456+08", "observed_at"
        )
        different_instant = _parse_datetime(
            "2026-01-15 03:04:06.123456+00", "observed_at"
        )
        self.assertEqual(same_instant, same_instant_local)
        self.assertNotEqual(same_instant, different_instant)

    def test_all_dws_read_timestamps_use_explicit_text_projection(self) -> None:
        for table_name, (sql, columns) in _READ_TIMESTAMP_PROJECTIONS.items():
            for column in columns:
                with self.subTest(table=table_name, column=column):
                    self.assertRegex(
                        sql,
                        rf"CAST\(\s*(?:[a-z]+\.)?{column}\s+AS "
                        rf"VARCHAR\(128\)\s*\)\s+AS\s+{column}\b",
                    )

    def test_all_dws_projection_and_switch_timestamps_use_cast_boundary(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-timestamptz-contract",
            observed_at=OBSERVED_AT,
        )
        batch = replace(
            batch,
            issues=(
                LineageIssue(
                    environment="DEV",
                    source_profile="fixture",
                    program_name="DEMO_PROGRAM",
                    issue_type="ORPHAN_BRANCH",
                    severity="MEDIUM",
                    message="timestamp binding contract issue",
                    disposition_updated_at=None,
                ),
            ),
        )

        self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        observed_sql = {
            " ".join(sql.split()) for _, sql, _ in self.jdbc_boundary.calls
        }
        for sql in _WRITE_SQL:
            with self.subTest(sql=sql.splitlines()[1].strip()):
                self.assertIn(" ".join(sql.split()), observed_sql)

        issue_call = next(
            call
            for call in self.jdbc_boundary.calls
            if " ".join(call[1].split()) == " ".join(INSERT_ISSUE_SQL.split())
        )
        self.assertEqual(issue_call[0], "executemany")
        issue_columns, _ = _insert_contract(issue_call[1])
        issue_row = issue_call[2][0]
        self.assertIsNone(issue_row[issue_columns.index("disposition_updated_at")])

        batch_call = next(
            call
            for call in self.jdbc_boundary.calls
            if " ".join(call[1].split()) == " ".join(INSERT_BATCH_SQL.split())
        )
        batch_columns, _ = _insert_contract(batch_call[1])
        self.assertIsNone(batch_call[2][batch_columns.index("published_at")])

    def test_dws_read_round_trip_preserves_aware_instants_for_all_entities(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-read-round-trip",
            observed_at=OBSERVED_AT,
        )
        batch = replace(
            batch,
            issues=(
                LineageIssue(
                    environment="DEV",
                    source_profile="fixture",
                    program_name="DEMO_PROGRAM",
                    issue_type="ORPHAN_BRANCH",
                    severity="MEDIUM",
                    message="round-trip regression issue",
                    disposition_updated_at=OBSERVED_AT,
                    disposition_updated_by="DEMO_TESTER",
                ),
            ),
        )
        self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(
            self.store.get_active_snapshot_scope(),
            (("DEV", "fixture"),),
        )
        metadata = self.store.get_batch_metadata(batch.batch_id)
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.observed_at, OBSERVED_AT)
        self.assertEqual(metadata.published_at, OBSERVED_AT)

        states = self.store.read_program_states(
            batch_id=batch.batch_id,
            environment="DEV",
        )
        self.assertEqual(len(states), 1)
        self.assertEqual(
            self.store.read_program_states(
                batch_id=batch.batch_id,
                environment="DEMO_OTHER",
            ),
            (),
        )
        self.assertEqual(states[0].first_seen_at, OBSERVED_AT)
        self.assertEqual(states[0].last_seen_at, OBSERVED_AT)
        self.assertEqual(states[0].last_changed_at, OBSERVED_AT)

        physical = self.store.read_physical_edges(batch_id=batch.batch_id)
        self.assertGreater(len(physical), 0)
        for row in physical:
            self.assertEqual(
                (
                    row.observed_at,
                    row.first_seen_at,
                    row.last_seen_at,
                    row.last_changed_at,
                    row.created_at,
                    row.updated_at,
                ),
                (OBSERVED_AT,) * 6,
            )

        business = self.store._fetch_business_rows(
            self.connection, batch_id=batch.batch_id
        )
        self.assertEqual(len(business), 1)
        self.assertEqual(
            (
                business[0].observed_at,
                business[0].first_seen_at,
                business[0].last_seen_at,
                business[0].last_changed_at,
                business[0].created_at,
                business[0].updated_at,
            ),
            (OBSERVED_AT,) * 6,
        )

        issues = self.store._fetch_issue_rows(self.connection, batch_id=batch.batch_id)
        self.assertEqual(len(issues), 1)
        self.assertEqual(
            (
                issues[0].first_seen_at,
                issues[0].last_seen_at,
                issues[0].last_changed_at,
                issues[0].disposition_updated_at,
                issues[0].created_at,
                issues[0].updated_at,
            ),
            (OBSERVED_AT,) * 6,
        )

        with self.store._connection_scope() as connection:
            raw_states = self.store._fetch_program_state_rows(
                connection, batch_id=batch.batch_id
            )
        self.assertTrue(raw_states)
        for index in (8, 9, 10, 12, 13):
            self.assertIsInstance(raw_states[0][index], str)
            self.assertEqual(
                _parse_datetime(raw_states[0][index], "program_state_timestamp"),
                OBSERVED_AT,
            )

    def test_writes_physical_tmp_rows_and_collapsed_formal_business_row(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "CREATE TEMPORARY TABLE TMP_STAGE AS SELECT * FROM DWF.SOURCE;"
            " INSERT INTO DWM.RESULT SELECT * FROM TMP_STAGE;",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-1",
            observed_at=OBSERVED_AT,
        )

        result = self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(result.edge_count, 2)
        self.assertEqual(result.business_edge_count, 1)
        physical = self.store.read_physical_edges(active_only=True)
        self.assertEqual(len(physical), 2)
        self.assertTrue(
            any(
                row.source_node_kind == "temporary_asset"
                or row.target_node_kind == "temporary_asset"
                for row in physical
            )
        )
        business = self.store.read_edges(active_only=True)
        self.assertEqual(len(business), 1)
        self.assertEqual(
            (business[0].source_table, business[0].target_table),
            ("DWF.SOURCE", "DWM.RESULT"),
        )
        self.assertIsInstance(business[0].evidence, dict)
        assert isinstance(business[0].evidence, dict)
        self.assertEqual(business[0].evidence["collapse_depth"], 2)
        self.assertEqual(
            self.connection.execute(
                "SELECT edge_count FROM dwp.lineage_batch WHERE batch_id = ?",
                ("batch-dws-1",),
            ).fetchone()[0],
            2,
        )

    def test_business_edge_reads_push_target_predicate_to_dws(self) -> None:
        source_a = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_TARGET_A",
            "INSERT INTO DWM.RESULT_A SELECT * FROM DWF.SOURCE_A;",
            expected_target="DWM.RESULT_A",
            source_hash="sha256:target-a",
        )
        source_b = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_TARGET_B",
            "INSERT INTO DWM.RESULT_B SELECT * FROM DWF.SOURCE_B;",
            expected_target="DWM.RESULT_B",
            source_hash="sha256:target-b",
        )
        batch_a, dag_a = self.make_batch(
            source_a,
            batch_id="batch-dws-targets",
            observed_at=OBSERVED_AT,
        )
        batch_b, dag_b = self.make_batch(
            source_b,
            batch_id="batch-dws-targets",
            observed_at=OBSERVED_AT,
        )
        batch = MaterializationBatch(
            batch_id="batch-dws-targets",
            observed_at=OBSERVED_AT,
            edges=(*batch_a.edges, *batch_b.edges),
            issues=(*batch_a.issues, *batch_b.issues),
            program_states=(*batch_a.program_states, *batch_b.program_states),
        )
        self.store.publish(
            batch,
            physical_dags=(dag_a, dag_b),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        all_edges = self.store.read_edges(active_only=True)
        filtered = self.store.read_edges(
            active_only=True,
            target_tables=("DWS_DWM.RESULT_A", "DWM.RESULT_A"),
        )

        self.assertEqual({edge.target_table for edge in all_edges}, {"DWM.RESULT_A", "DWM.RESULT_B"})
        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in filtered},
            {("DWF.SOURCE_A", "DWM.RESULT_A")},
        )

    def test_business_boundary_keeps_technical_physical_rows_out_of_business_rows(
        self,
    ) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_BUSINESS_BOUNDARY",
            "INSERT INTO DLO.TECH_STAGE SELECT * FROM DWF.SOURCE;"
            " INSERT INTO DWO.TECH_STAGE_2 SELECT * FROM DLO.TECH_STAGE;"
            " INSERT INTO DWM.RESULT SELECT * FROM DWO.TECH_STAGE_2;",
            expected_target="DWM.RESULT",
            source_hash="sha256:business-boundary",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-business-boundary",
            observed_at=OBSERVED_AT,
        )

        result = self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(result.edge_count, 3)
        self.assertEqual(result.business_edge_count, 1)
        physical = self.store.read_physical_edges(active_only=True)
        self.assertEqual(len(physical), 3)
        self.assertTrue(
            any(
                row.source_table.startswith(("DLO.", "DWO."))
                or row.target_table.startswith(("DLO.", "DWO."))
                for row in physical
            )
        )
        business = self.store.read_edges(active_only=True)
        self.assertEqual(
            [(edge.source_table, edge.target_table) for edge in business],
            [("DWF.SOURCE", "DWM.RESULT")],
        )
        self.assertEqual(business[0].source_table, "DWF.SOURCE")
        self.assertEqual(business[0].target_table, "DWM.RESULT")

    def test_legacy_technical_business_rows_are_hidden_from_dws_reads(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_LEGACY_TECHNICAL",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE;",
            expected_target="DWM.RESULT",
            source_hash="sha256:legacy-technical",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-legacy-technical",
            observed_at=OBSERVED_AT,
        )
        self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )
        self.connection.execute(
            "UPDATE dwp.lineage_business_edge SET source_table = ?, target_table = ?",
            ("DLO.LEGACY_SOURCE", "DWF.LEGACY_TARGET"),
        )
        self.connection.commit()

        self.assertEqual(self.store.read_edges(active_only=True), ())
        self.assertEqual(
            self.store.read_outgoing_edges(
                environment="DEV",
                source_table="DLO.LEGACY_SOURCE",
            ),
            (),
        )

        second_batch, second_dag = self.make_batch(
            source,
            batch_id="batch-dws-legacy-technical-cleanup",
            observed_at=OBSERVED_AT.replace(day=2),
        )
        self.store.publish(
            second_batch,
            physical_dags=(second_dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )
        active_edges = self.store.read_edges(active_only=True)
        self.assertEqual(
            [(edge.source_table, edge.target_table) for edge in active_edges],
            [("DWF.SOURCE", "DWM.RESULT")],
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM dwp.lineage_business_edge "
                "WHERE is_active = TRUE"
            ).fetchone()[0],
            1,
        )

    def test_pipeline_version_invariant_remains_defensive(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-mixed-version",
            observed_at=OBSERVED_AT,
        )
        mixed_state = ProgramState(
            environment="DEV",
            source_profile="profile_b",
            program_name="DEMO_OTHER",
            source_hash="sha256:other",
            first_seen_at=OBSERVED_AT,
            last_seen_at=OBSERVED_AT,
            last_changed_at=OBSERVED_AT,
            batch_id=batch.batch_id,
            pipeline_version="lineage-pipeline-v8-program-target-hint-selection",
        )
        mixed_batch = replace(
            batch,
            program_states=(*batch.program_states, mixed_state),
        )

        with self.assertRaisesRegex(
            ValueError,
            "all program states in a DWS batch must share pipeline_version",
        ):
            self.store.publish(
                mixed_batch,
                physical_dags=(dag,),
                complete_snapshot=True,
                snapshot_scopes=(
                    ("DEV", "fixture"),
                    ("DEV", "profile_b"),
                ),
            )

    def test_complete_snapshot_requires_explicit_nonempty_scope(self) -> None:
        batch = MaterializationBatch(
            batch_id="batch-dws-scope-required",
            observed_at=OBSERVED_AT,
        )

        with self.assertRaisesRegex(ValueError, "explicit snapshot_scopes"):
            self.store.publish(batch, complete_snapshot=True)
        with self.assertRaisesRegex(ValueError, "non-empty snapshot scope"):
            self.store.publish(
                batch,
                complete_snapshot=True,
                snapshot_scopes=(),
            )

    def test_empty_snapshot_is_a_published_active_batch(self) -> None:
        batch = MaterializationBatch(
            batch_id="batch-dws-empty",
            observed_at=OBSERVED_AT,
        )

        result = self.store.publish(
            batch,
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(result.edge_count, 0)
        self.assertEqual(result.business_edge_count, 0)
        self.assertEqual(result.issue_count, 0)
        self.assertEqual(self.store.get_active_batch_id(), "batch-dws-empty")
        self.assertEqual(self.store.read_physical_edges(active_only=True), ())
        self.assertEqual(self.store.read_edges(active_only=True), ())
        self.assertEqual(self.store.read_issues(active_only=True), ())
        metadata = self.store.get_batch_metadata("batch-dws-empty")
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertTrue(metadata.is_active)
        self.assertEqual(metadata.edge_count, 0)

    def test_history_isolation_and_rebase_preserve_stable_keys(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        first, first_dag = self.make_batch(
            source,
            batch_id="batch-dws-history-1",
            observed_at=OBSERVED_AT,
        )
        self.store.publish(
            first,
            physical_dags=(first_dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )
        second, second_dag = self.make_batch(
            source,
            batch_id="batch-dws-history-2",
            observed_at=OBSERVED_AT.replace(day=2),
        )
        self.store.publish(
            second,
            physical_dags=(second_dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(self.store.get_active_batch_id(), "batch-dws-history-2")
        first_physical = self.store.read_physical_edges(batch_id=first.batch_id)
        second_physical = self.store.read_physical_edges(batch_id=second.batch_id)
        self.assertEqual(
            tuple(row.edge_key for row in first_physical),
            tuple(row.edge_key for row in second_physical),
        )
        self.assertNotEqual(
            tuple(row.row_key for row in first_physical),
            tuple(row.row_key for row in second_physical),
        )
        self.assertEqual(
            len(self.store.read_edges(batch_id=first.batch_id)),
            1,
        )
        self.assertEqual(len(self.store.read_edges(active_only=True)), 1)
        first_metadata = self.store.get_batch_metadata(first.batch_id)
        self.assertIsNotNone(first_metadata)
        assert first_metadata is not None
        self.assertFalse(first_metadata.is_active)

    def test_resolved_issue_can_survive_scoped_program_disappearance(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        first, first_dag = self.make_batch(
            source,
            batch_id="batch-dws-issue-1",
            observed_at=OBSERVED_AT,
        )
        first = replace(
            first,
            issues=(
                LineageIssue(
                    environment="DEV",
                    source_profile="fixture",
                    program_name="DEMO_PROGRAM",
                    issue_type="ORPHAN_BRANCH",
                    severity="MEDIUM",
                    message="demo issue",
                ),
            ),
        )
        self.store.publish(
            first,
            physical_dags=(first_dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        second = MaterializationBatch(
            batch_id="batch-dws-issue-2",
            observed_at=OBSERVED_AT.replace(day=2),
        )
        self.store.publish(
            second,
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertEqual(self.store.read_issues(active_only=True), ())
        resolved = self.store.read_issues(batch_id=second.batch_id)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].disposition, IssueDisposition.RESOLVED)
        self.assertFalse(resolved[0].is_active)
        self.assertEqual(self.store.read_program_states(active_only=True), ())

    def test_failed_active_switch_rolls_back_candidate_and_preserves_previous(
        self,
    ) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        first, dag = self.make_batch(
            source,
            batch_id="batch-dws-1",
            observed_at=OBSERVED_AT,
        )
        self.store.publish(
            first,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )
        second, second_dag = self.make_batch(
            source,
            batch_id="batch-dws-2",
            observed_at=OBSERVED_AT.replace(day=2),
        )

        def fail_after_validation(stage: str) -> None:
            if stage == "after_validate":
                raise RuntimeError("controlled publish failure")

        with self.assertRaisesRegex(RuntimeError, "controlled publish failure"):
            self.store.publish(
                second,
                physical_dags=(second_dag,),
                complete_snapshot=True,
                snapshot_scopes=(("DEV", "fixture"),),
                stage_hook=fail_after_validation,
            )

        self.assertEqual(self.store.get_active_batch_id(), "batch-dws-1")
        self.assertIsNone(self.store.get_batch_metadata("batch-dws-2"))
        for table in (
            "lineage_program_state",
            "lineage_edge",
            "lineage_business_edge",
            "lineage_issue",
        ):
            self.assertEqual(
                self.connection.execute(
                    f"SELECT COUNT(*) FROM dwp.{table} WHERE batch_id = ?",
                    ("batch-dws-2",),
                ).fetchone()[0],
                0,
            )
        self.assertEqual(len(self.store.read_edges(active_only=True)), 1)

    def test_job_publishes_through_injected_dws_backend(self) -> None:
        from jobs.crontab.imp_lineage_edge import materialize_sources

        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "CREATE TEMPORARY TABLE TMP_STAGE AS SELECT * FROM DWF.SOURCE;"
            " INSERT INTO DWM.RESULT SELECT * FROM TMP_STAGE;",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )

        result = materialize_sources(
            [source],
            store=self.store,
            batch_id="batch-dws-job",
            observed_at=OBSERVED_AT,
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertIsInstance(result, DWSPublishResult)
        self.assertEqual(result.edge_count, 2)
        self.assertEqual(len(self.store.read_physical_edges(active_only=True)), 2)
        self.assertEqual(len(self.store.read_edges(active_only=True)), 1)
        first_hash = self.connection.execute(
            "SELECT physical_derivation_hash FROM dwp.lineage_business_edge "
            "WHERE batch_id = ?",
            ("batch-dws-job",),
        ).fetchone()[0]

        second = materialize_sources(
            [source],
            store=self.store,
            batch_id="batch-dws-job-rebase",
            observed_at=OBSERVED_AT.replace(day=2),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
        )

        self.assertIsInstance(second, DWSPublishResult)
        self.assertEqual(second.edge_count, 2)
        self.assertEqual(len(self.store.read_physical_edges(active_only=True)), 2)
        self.assertEqual(len(self.store.read_edges(active_only=True)), 1)
        second_hash = self.connection.execute(
            "SELECT physical_derivation_hash FROM dwp.lineage_business_edge "
            "WHERE batch_id = ?",
            ("batch-dws-job-rebase",),
        ).fetchone()[0]
        self.assertEqual(first_hash, second_hash)

    def test_public_candidate_validation_rechecks_inserted_projection(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-validation",
            observed_at=OBSERVED_AT,
        )

        def validate_after_insert(stage: str) -> None:
            if stage == "after_candidate_insert":
                self.store.validate_candidate(batch.batch_id)

        result = self.store.publish(
            batch,
            physical_dags=(dag,),
            complete_snapshot=True,
            snapshot_scopes=(("DEV", "fixture"),),
            stage_hook=validate_after_insert,
        )

        self.assertEqual(result.batch_id, batch.batch_id)
        self.assertEqual(self.store.get_active_batch_id(), batch.batch_id)

    def test_duplicate_physical_identity_fails_closed(self) -> None:
        source = ProgramSource(
            "DEV",
            "fixture",
            "DEMO_PROGRAM",
            "INSERT INTO DWM.RESULT SELECT * FROM DWF.SOURCE",
            expected_target="DWM.RESULT",
            source_hash="sha256:demo",
        )
        batch, dag = self.make_batch(
            source,
            batch_id="batch-dws-duplicate",
            observed_at=OBSERVED_AT,
        )
        duplicate_dag = replace(dag, edges=dag.edges + dag.edges[:1])

        with self.assertRaisesRegex(ValueError, r"duplicate (row_key|stable identity)"):
            self.store.publish(
                batch,
                physical_dags=(duplicate_dag,),
                complete_snapshot=True,
                snapshot_scopes=(("DEV", "fixture"),),
            )

        self.assertIsNone(self.store.get_active_batch_id())
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM dwp.lineage_edge WHERE batch_id = ?",
                ("batch-dws-duplicate",),
            ).fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
