"""SQLite reference store for Phase 5 materialization batches.

SQLite is a public/demo adapter only.  The pure transformation lives in
``shared.lineage.materialization`` so a future production repository can reuse the
same candidate and publish contract without depending on SQLite.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from shared.lineage.audit import AuditFact, AuditPolicy, replay_audit_policy
from shared.lineage.domain import (
    AuditConfidence,
    IssueDisposition,
    IssueType,
    LEGACY_AUDIT_POLICY_VERSION,
    LEGACY_AUDIT_RULE_VERSION,
    LineageEdge,
    LineageIssue,
    ProgramState,
    canonicalize_dataset_name,
)

from .evolution import (  # pyright: ignore[reportMissingImports]
    BatchMetadata,
    diff_environments,
    diff_lineage_batches,
    reconcile_issue_lifecycle,
)
from .materialization import (  # pyright: ignore[reportMissingImports]
    MaterializationBatch,
    _canonical_json,
    _edge_identity,
    _issue_identity,
    new_batch_id,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_MATERIALIZATION_DB_PATH = (
    ROOT_DIR / "runtime" / "sqlite" / "lineage_materialization.db"
)
CURRENT_SCHEMA_VERSION = 3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS lineage_batch (
    batch_id TEXT PRIMARY KEY,
    observed_at TEXT NOT NULL,
    published_at TEXT,
    edge_count INTEGER NOT NULL,
    issue_count INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0, 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_batch_active
    ON lineage_batch(is_active)
    WHERE is_active = 1;

CREATE TABLE IF NOT EXISTS lineage_edge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    program_name TEXT,
    job_key TEXT,
    evidence_type TEXT NOT NULL,
    evidence TEXT NOT NULL,
    source_hash TEXT,
    batch_id TEXT NOT NULL REFERENCES lineage_batch(batch_id),
    observed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    is_active INTEGER NOT NULL CHECK (is_active IN (0, 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_edge_batch_identity
    ON lineage_edge(
        batch_id,
        environment,
        source_profile,
        source_table,
        target_table,
        IFNULL(program_name, ''),
        IFNULL(job_key, '')
    );

CREATE INDEX IF NOT EXISTS idx_lineage_edge_source
    ON lineage_edge(environment, source_profile, source_table, is_active);

CREATE INDEX IF NOT EXISTS idx_lineage_edge_target
    ON lineage_edge(environment, source_profile, target_table, is_active);

CREATE INDEX IF NOT EXISTS idx_lineage_edge_batch_active
    ON lineage_edge(batch_id, is_active);

CREATE TABLE IF NOT EXISTS lineage_issue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    program_name TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    stable_key TEXT,
    node_key TEXT,
    branch_sink TEXT,
    message TEXT NOT NULL,
    evidence TEXT NOT NULL,
    batch_id TEXT NOT NULL REFERENCES lineage_batch(batch_id),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    is_active INTEGER NOT NULL CHECK (is_active IN (0, 1)),
    confidence TEXT NOT NULL DEFAULT 'UNKNOWN',
    rule_version TEXT NOT NULL DEFAULT 'audit-rule-legacy',
    disposition TEXT NOT NULL DEFAULT 'OPEN',
    policy_version TEXT NOT NULL DEFAULT 'audit-policy-legacy',
    disposition_updated_at TEXT,
    disposition_updated_by TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_issue_batch_identity
    ON lineage_issue(
        batch_id,
        environment,
        source_profile,
        program_name,
        issue_type,
        IFNULL(stable_key, ''),
        IFNULL(node_key, ''),
        IFNULL(branch_sink, '')
    );

CREATE INDEX IF NOT EXISTS idx_lineage_issue_stable_key
    ON lineage_issue(stable_key);

CREATE INDEX IF NOT EXISTS idx_lineage_issue_batch_active
    ON lineage_issue(batch_id, is_active);

CREATE INDEX IF NOT EXISTS idx_lineage_issue_scope
    ON lineage_issue(
        environment,
        source_profile,
        program_name,
        issue_type,
        is_active
    );

CREATE TABLE IF NOT EXISTS lineage_program_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    program_name TEXT NOT NULL,
    source_hash TEXT,
    pipeline_version TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_changed_at TEXT,
    batch_id TEXT NOT NULL REFERENCES lineage_batch(batch_id),
    is_active INTEGER NOT NULL CHECK (is_active IN (0, 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_program_state_batch_identity
    ON lineage_program_state(
        batch_id,
        environment,
        source_profile,
        program_name
    );

CREATE UNIQUE INDEX IF NOT EXISTS uq_lineage_program_state_active_identity
    ON lineage_program_state(environment, source_profile, program_name)
    WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_lineage_program_state_batch_active
    ON lineage_program_state(batch_id, is_active);

CREATE INDEX IF NOT EXISTS idx_lineage_program_state_scope
    ON lineage_program_state(environment, source_profile, is_active);
"""

EDGE_SELECT_SQL = (
    "SELECT environment, source_profile, source_table, target_table, "
    "program_name, job_key, evidence_type, evidence, source_hash, "
    "batch_id, observed_at, updated_at, is_active FROM lineage_edge"
)
EDGE_ORDER_SQL = (
    " ORDER BY environment, source_profile, source_table, target_table, "
    "IFNULL(program_name, ''), IFNULL(job_key, ''), id"
)
ISSUE_SELECT_SQL = (
    "SELECT environment, source_profile, program_name, issue_type, severity, "
    "stable_key, node_key, branch_sink, message, evidence, batch_id, "
    "first_seen_at, last_seen_at, is_active, confidence, rule_version, "
    "disposition, policy_version, disposition_updated_at, disposition_updated_by "
    "FROM lineage_issue"
)
ISSUE_ORDER_SQL = (
    " ORDER BY environment, source_profile, program_name, issue_type, "
    "IFNULL(stable_key, ''), IFNULL(node_key, ''), IFNULL(branch_sink, ''), id"
)
PROGRAM_STATE_SELECT_SQL = (
    "SELECT environment, source_profile, program_name, source_hash, "
    "pipeline_version, first_seen_at, last_seen_at, last_changed_at, "
    "batch_id, is_active FROM lineage_program_state"
)
PROGRAM_STATE_ORDER_SQL = " ORDER BY environment, source_profile, program_name, id"
ACTIVE_ISSUE_SELECT_SQL = """
    SELECT environment, source_profile, program_name, issue_type, severity,
           stable_key, node_key, branch_sink, message, evidence, batch_id,
           first_seen_at, last_seen_at, is_active, confidence, rule_version,
           disposition, policy_version, disposition_updated_at,
           disposition_updated_by
    FROM lineage_issue
    WHERE is_active = 1
    ORDER BY environment, source_profile, program_name, issue_type,
             IFNULL(stable_key, ''), IFNULL(node_key, ''), IFNULL(branch_sink, ''), id
"""
BATCH_METADATA_SELECT_SQL = """
    SELECT
        batch_id,
        observed_at,
        published_at,
        edge_count,
        issue_count,
        is_active,
        (
            SELECT COUNT(*)
            FROM lineage_program_state AS state
            WHERE state.batch_id = batch.batch_id
        ) AS program_count
    FROM lineage_batch AS batch
    ORDER BY observed_at, batch_id
"""

EDGE_OUTGOING_NEIGHBOR_SQL = (
    EDGE_SELECT_SQL
    + " WHERE environment = ? AND source_table = ? AND is_active = 1"
    + EDGE_ORDER_SQL
)
EDGE_OUTGOING_PROFILE_NEIGHBOR_SQL = (
    EDGE_SELECT_SQL
    + " WHERE environment = ? AND source_table = ? AND is_active = 1"
    + " AND source_profile = ?"
    + EDGE_ORDER_SQL
)
EDGE_INCOMING_NEIGHBOR_SQL = (
    EDGE_SELECT_SQL
    + " WHERE environment = ? AND target_table = ? AND is_active = 1"
    + EDGE_ORDER_SQL
)
EDGE_INCOMING_PROFILE_NEIGHBOR_SQL = (
    EDGE_SELECT_SQL
    + " WHERE environment = ? AND target_table = ? AND is_active = 1"
    + " AND source_profile = ?"
    + EDGE_ORDER_SQL
)


@dataclass(frozen=True, slots=True)
class PublishResult:
    """一次成功 publish 的最小结果摘要。"""

    batch_id: str
    edge_count: int
    issue_count: int
    previous_batch_id: str | None
    program_count: int = 0


@dataclass(slots=True)
class SQLitePublishMetrics:
    """Optional aggregate timings and row counts for SQLite publish diagnostics."""

    prepare_ms: int = 0
    insert_ms: int = 0
    validate_ms: int = 0
    active_switch_ms: int = 0
    commit_ms: int = 0
    prepared_edge_rows: int = 0
    prepared_issue_rows: int = 0
    prepared_program_rows: int = 0
    validated_edge_rows: int = 0
    validated_issue_rows: int = 0
    validated_program_rows: int = 0
    evidence_serialization_calls: int = 0


@dataclass(frozen=True, slots=True)
class _PreparedCandidate:
    batch: MaterializationBatch
    edge_rows: tuple[tuple[object, ...], ...]
    issue_rows: tuple[tuple[object, ...], ...]
    program_state_rows: tuple[tuple[object, ...], ...]


def _migrate_schema(connection: sqlite3.Connection) -> None:
    """将旧 reference schema 升级到当前版本且不改写历史 facts。"""

    columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(lineage_program_state)"
        ).fetchall()
    }
    if columns and "pipeline_version" not in columns:
        connection.execute(
            "ALTER TABLE lineage_program_state ADD COLUMN pipeline_version TEXT"
        )

    issue_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lineage_issue)").fetchall()
    }
    issue_migrations = (
        ("confidence", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
        ("rule_version", "TEXT NOT NULL DEFAULT 'audit-rule-legacy'"),
        ("disposition", "TEXT NOT NULL DEFAULT 'OPEN'"),
        ("policy_version", "TEXT NOT NULL DEFAULT 'audit-policy-legacy'"),
        ("disposition_updated_at", "TEXT"),
        ("disposition_updated_by", "TEXT"),
    )
    for column_name, definition in issue_migrations:
        if issue_columns and column_name not in issue_columns:
            # Both values come from the fixed migration table above, never input.
            # pi-lens-ignore: python-sql-injection
            try:
                connection.execute(
                    f"ALTER TABLE lineage_issue ADD COLUMN {column_name} {definition}"
                )
            except sqlite3.Error as exc:
                raise RuntimeError(
                    f"failed to migrate lineage_issue column {column_name}"
                ) from exc

    version_row = connection.execute("PRAGMA user_version").fetchone()
    current_version = 0 if version_row is None else int(version_row[0])
    if current_version < CURRENT_SCHEMA_VERSION:
        # The version is a fixed source-code migration constant, never user input.
        # pi-lens-ignore: python-sql-injection
        connection.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")


def _datetime_text(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be a datetime")
    return value.isoformat()


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _decode_evidence(value: str) -> Mapping[str, object] | str | None:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("stored evidence is not valid JSON") from exc
    if decoded is None or isinstance(decoded, (str, Mapping)):
        return decoded
    raise ValueError("stored evidence must be a JSON object, string, or null")


def _edge_from_row(row: Any) -> LineageEdge:
    return LineageEdge(
        environment=str(row[0]),
        source_profile=str(row[1]),
        source_table=str(row[2]),
        target_table=str(row[3]),
        program_name=row[4],
        job_key=row[5],
        evidence_type=str(row[6]),
        evidence=_decode_evidence(str(row[7])),
        source_hash=row[8],
        batch_id=str(row[9]),
        observed_at=_parse_datetime(str(row[10])),
        updated_at=_parse_datetime(str(row[11])),
        is_active=bool(row[12]),
    )


def _program_state_from_row(row: Any) -> ProgramState:
    return ProgramState(
        environment=str(row[0]),
        source_profile=str(row[1]),
        program_name=str(row[2]),
        source_hash=row[3],
        pipeline_version=row[4],
        first_seen_at=_parse_datetime(str(row[5])),
        last_seen_at=_parse_datetime(str(row[6])),
        last_changed_at=(None if row[7] is None else _parse_datetime(str(row[7]))),
        batch_id=str(row[8]),
        is_active=bool(row[9]),
    )


def _batch_metadata_from_row(row: Any) -> BatchMetadata:
    try:
        return BatchMetadata(
            batch_id=str(row[0]),
            observed_at=_parse_datetime(str(row[1])),
            published_at=(None if row[2] is None else _parse_datetime(str(row[2]))),
            edge_count=int(row[3]),
            issue_count=int(row[4]),
            program_count=int(row[6]),
            is_active=bool(row[5]),
        )
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored batch metadata is invalid") from exc


def _issue_from_row(row: Any) -> LineageIssue:
    row_length = len(row)
    confidence = (
        row[14]
        if row_length > 14 and row[14] is not None
        else AuditConfidence.UNKNOWN.value
    )
    rule_version = (
        row[15]
        if row_length > 15 and row[15] is not None
        else LEGACY_AUDIT_RULE_VERSION
    )
    disposition = (
        row[16]
        if row_length > 16 and row[16] is not None
        else IssueDisposition.OPEN.value
    )
    policy_version = (
        row[17]
        if row_length > 17 and row[17] is not None
        else LEGACY_AUDIT_POLICY_VERSION
    )
    disposition_updated_at = (
        None
        if row_length <= 18 or row[18] is None
        else _parse_datetime(str(row[18]))
    )
    disposition_updated_by = (
        None if row_length <= 19 else row[19]
    )
    return LineageIssue(
        environment=str(row[0]),
        source_profile=str(row[1]),
        program_name=str(row[2]),
        issue_type=IssueType(str(row[3])),
        severity=str(row[4]),
        stable_key=row[5],
        node_key=row[6],
        branch_sink=row[7],
        message=str(row[8]),
        evidence=_decode_evidence(str(row[9])),
        batch_id=str(row[10]),
        first_seen_at=_parse_datetime(str(row[11])),
        last_seen_at=_parse_datetime(str(row[12])),
        is_active=bool(row[13]),
        confidence=confidence,
        rule_version=str(rule_version),
        disposition=disposition,
        policy_version=str(policy_version),
        disposition_updated_at=disposition_updated_at,
        disposition_updated_by=disposition_updated_by,
    )


def _prepare_batch(batch: MaterializationBatch) -> MaterializationBatch:
    if not isinstance(batch, MaterializationBatch):
        raise TypeError("batch must be a MaterializationBatch")

    edges: list[LineageEdge] = []
    for edge in batch.edges:
        if not isinstance(edge, LineageEdge):
            raise TypeError("batch.edges must contain LineageEdge values")
        edges.append(
            replace(
                edge,
                batch_id=batch.batch_id,
                observed_at=batch.observed_at,
                updated_at=batch.observed_at,
                is_active=False,
            )
        )

    issues: list[LineageIssue] = []
    for issue in batch.issues:
        if not isinstance(issue, LineageIssue):
            raise TypeError("batch.issues must contain LineageIssue values")
        issues.append(
            replace(
                issue,
                batch_id=batch.batch_id,
                first_seen_at=issue.first_seen_at or batch.observed_at,
                last_seen_at=batch.observed_at,
                is_active=False,
            )
        )

    program_states: list[ProgramState] = []
    for state in batch.program_states:
        if not isinstance(state, ProgramState):
            raise TypeError("batch.program_states must contain ProgramState values")
        program_states.append(
            replace(
                state,
                batch_id=batch.batch_id,
                is_active=False,
            )
        )

    return MaterializationBatch(
        batch_id=batch.batch_id,
        observed_at=batch.observed_at,
        edges=tuple(edges),
        issues=tuple(issues),
        program_states=tuple(program_states),
    )


def _edge_row(edge: LineageEdge, batch: MaterializationBatch) -> tuple[object, ...]:
    return (
        edge.environment,
        edge.source_profile,
        edge.source_table,
        edge.target_table,
        edge.program_name,
        edge.job_key,
        edge.evidence_type,
        _canonical_json(edge.evidence),
        edge.source_hash,
        batch.batch_id,
        _datetime_text(batch.observed_at),
        _datetime_text(edge.updated_at or batch.observed_at),
        0,
    )


def _issue_row(issue: LineageIssue, batch: MaterializationBatch) -> tuple[object, ...]:
    return (
        issue.environment,
        issue.source_profile,
        issue.program_name,
        IssueType(issue.issue_type).value,
        issue.severity,
        issue.stable_key,
        issue.node_key,
        issue.branch_sink,
        issue.message,
        _canonical_json(issue.evidence),
        batch.batch_id,
        _datetime_text(issue.first_seen_at or batch.observed_at),
        _datetime_text(issue.last_seen_at or batch.observed_at),
        0,
        AuditConfidence(issue.confidence).value,
        issue.rule_version,
        IssueDisposition(issue.disposition).value,
        issue.policy_version,
        (
            None
            if issue.disposition_updated_at is None
            else _datetime_text(issue.disposition_updated_at)
        ),
        issue.disposition_updated_by,
    )


def _program_state_row(
    state: ProgramState, batch: MaterializationBatch
) -> tuple[object, ...]:
    return (
        state.environment,
        state.source_profile,
        state.program_name,
        state.source_hash,
        state.pipeline_version,
        _datetime_text(state.first_seen_at),
        _datetime_text(state.last_seen_at),
        None
        if state.last_changed_at is None
        else _datetime_text(state.last_changed_at),
        batch.batch_id,
        0,
    )


def _prepare_candidate(
    batch: MaterializationBatch,
    instrumentation: SQLitePublishMetrics | None = None,
) -> _PreparedCandidate:
    prepared_batch = _prepare_batch(batch)
    edge_rows = tuple(_edge_row(edge, prepared_batch) for edge in prepared_batch.edges)
    issue_rows = tuple(
        _issue_row(issue, prepared_batch) for issue in prepared_batch.issues
    )
    program_state_rows = tuple(
        _program_state_row(state, prepared_batch)
        for state in prepared_batch.program_states
    )
    if instrumentation is not None:
        instrumentation.prepared_edge_rows = len(edge_rows)
        instrumentation.prepared_issue_rows = len(issue_rows)
        instrumentation.prepared_program_rows = len(program_state_rows)
        instrumentation.evidence_serialization_calls = len(edge_rows) + len(issue_rows)
    return _PreparedCandidate(
        batch=prepared_batch,
        edge_rows=edge_rows,
        issue_rows=issue_rows,
        program_state_rows=program_state_rows,
    )


class SQLiteMaterializationStore:
    """保存 candidate 并以单个 SQLite transaction 切换 active batch。"""

    def __init__(
        self,
        db_path: str | Path | sqlite3.Connection = DEFAULT_MATERIALIZATION_DB_PATH,
    ) -> None:
        self._owns_connection = False
        self._connection: sqlite3.Connection | None = None
        if isinstance(db_path, sqlite3.Connection):
            self._connection = db_path
            self.db_path: Path | None = None
        else:
            if not isinstance(db_path, (str, Path)) or not str(db_path).strip():
                raise ValueError("db_path must be a non-empty path or connection")
            if str(db_path) == ":memory:":
                self._connection = sqlite3.connect(":memory:")
                self._owns_connection = True
                self.db_path = None
            else:
                self.db_path = Path(db_path).expanduser()
        self.initialize_schema()

    @property
    def connection(self) -> sqlite3.Connection | None:
        """返回注入或 memory connection；文件数据库按操作短连接打开。"""

        return self._connection

    @contextmanager
    def _connection_scope(self) -> Iterator[sqlite3.Connection]:
        if self._connection is not None:
            self._configure_connection(self._connection)
            yield self._connection
            return
        if self.db_path is None:
            raise RuntimeError("materialization store has no database path")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            self._configure_connection(connection)
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _configure_connection(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA foreign_keys = ON")

    def initialize_schema(self) -> None:
        with self._connection_scope() as connection:
            connection.executescript(SCHEMA_SQL)
            _migrate_schema(connection)
            connection.commit()

    initialize = initialize_schema

    def close(self) -> None:
        if self._owns_connection and self._connection is not None:
            self._connection.close()
            self._connection = None

    def _candidate_counts(
        self,
        connection: sqlite3.Connection,
        batch_id: str,
    ) -> tuple[int, int, int]:
        edge_row = connection.execute(
            "SELECT COUNT(*) FROM lineage_edge WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        issue_row = connection.execute(
            "SELECT COUNT(*) FROM lineage_issue WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        program_row = connection.execute(
            "SELECT COUNT(*) FROM lineage_program_state WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if edge_row is None or issue_row is None or program_row is None:
            raise ValueError("candidate count query returned no row")
        try:
            return int(edge_row[0]), int(issue_row[0]), int(program_row[0])
        except (IndexError, TypeError, ValueError) as exc:
            raise ValueError("candidate count query returned invalid data") from exc

    def _validate_candidate_in_transaction(
        self,
        connection: sqlite3.Connection,
        candidate: _PreparedCandidate,
        instrumentation: SQLitePublishMetrics | None = None,
    ) -> None:
        batch = candidate.batch
        edge_identities: set[tuple[str, str, str, str, str, str]] = set()
        for edge in batch.edges:
            identity = _edge_identity(edge)
            if identity in edge_identities:
                raise ValueError("candidate batch contains duplicate LineageEdge facts")
            edge_identities.add(identity)

        issue_identities: set[tuple[str, str, str, str, str, str, str]] = set()
        for issue in batch.issues:
            identity = _issue_identity(issue)
            if identity in issue_identities:
                raise ValueError(
                    "candidate batch contains duplicate LineageIssue facts"
                )
            issue_identities.add(identity)

        program_identities: set[tuple[str, str, str]] = set()
        for state in batch.program_states:
            identity = (
                state.environment,
                state.source_profile,
                state.program_name,
            )
            if identity in program_identities:
                raise ValueError(
                    "candidate batch contains duplicate ProgramState facts"
                )
            program_identities.add(identity)

        if instrumentation is not None:
            instrumentation.validated_edge_rows = len(batch.edges)
            instrumentation.validated_issue_rows = len(batch.issues)
            instrumentation.validated_program_rows = len(batch.program_states)

        stored_edge_count, stored_issue_count, stored_program_count = (
            self._candidate_counts(connection, batch.batch_id)
        )
        if stored_edge_count != len(batch.edges):
            raise ValueError("candidate lineage_edge count does not match batch")
        if stored_issue_count != len(batch.issues):
            raise ValueError("candidate lineage_issue count does not match batch")
        if stored_program_count != len(batch.program_states):
            raise ValueError(
                "candidate lineage_program_state count does not match batch"
            )

        batch_row = connection.execute(
            "SELECT is_active, edge_count, issue_count FROM lineage_batch WHERE batch_id = ?",
            (batch.batch_id,),
        ).fetchone()
        if batch_row is None:
            raise ValueError("candidate batch metadata is missing")
        if bool(batch_row[0]):
            raise ValueError("candidate batch is already active")
        try:
            metadata_counts = (int(batch_row[1]), int(batch_row[2]))
        except (IndexError, TypeError, ValueError) as exc:
            raise ValueError("candidate batch metadata counts are invalid") from exc
        if metadata_counts != (len(batch.edges), len(batch.issues)):
            raise ValueError("candidate batch metadata counts do not match rows")

    def _insert_candidate(
        self,
        connection: sqlite3.Connection,
        candidate: _PreparedCandidate,
    ) -> None:
        batch = candidate.batch
        connection.execute(
            """
            INSERT INTO lineage_batch(
                batch_id, observed_at, published_at, edge_count, issue_count, is_active
            ) VALUES (?, ?, NULL, ?, ?, 0)
            """,
            (
                batch.batch_id,
                _datetime_text(batch.observed_at),
                len(batch.edges),
                len(batch.issues),
            ),
        )
        connection.executemany(
            """
            INSERT INTO lineage_edge(
                environment, source_profile, source_table, target_table,
                program_name, job_key, evidence_type, evidence, source_hash,
                batch_id, observed_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            candidate.edge_rows,
        )
        connection.executemany(
            """
            INSERT INTO lineage_issue(
                environment, source_profile, program_name, issue_type, severity,
                stable_key, node_key, branch_sink, message, evidence, batch_id,
                first_seen_at, last_seen_at, is_active, confidence, rule_version,
                disposition, policy_version, disposition_updated_at,
                disposition_updated_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            candidate.issue_rows,
        )
        connection.executemany(
            """
            INSERT INTO lineage_program_state(
                environment, source_profile, program_name, source_hash,
                pipeline_version, first_seen_at, last_seen_at, last_changed_at,
                batch_id, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            candidate.program_state_rows,
        )

    @staticmethod
    def _active_batch_id(connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            "SELECT batch_id FROM lineage_batch WHERE is_active = 1"
        ).fetchone()
        return None if row is None else str(row[0])

    @staticmethod
    def _call_stage_hook(
        stage_hook: Callable[[str], Any] | None,
        stage: str,
    ) -> None:
        if stage_hook is not None:
            stage_hook(stage)

    def publish(
        self,
        batch: MaterializationBatch,
        *,
        stage_hook: Callable[[str], Any] | None = None,
        instrumentation: SQLitePublishMetrics | None = None,
    ) -> PublishResult:
        """在一个 transaction 内完成 prepare、insert、validation 和 active switch。

        ``stage_hook`` 仅用于公开测试/demo 注入故障；任何异常都会 rollback，
        因此旧 active batch 不会被切成空表或半成品。candidate row payload 在
        prepare 阶段只生成一次，insert 与 validation 复用同一份 canonical JSON。
        """

        if not isinstance(batch, MaterializationBatch):
            raise TypeError("batch must be a MaterializationBatch")
        if instrumentation is not None and not isinstance(
            instrumentation, SQLitePublishMetrics
        ):
            raise TypeError("instrumentation must be SQLitePublishMetrics or None")
        with self._connection_scope() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                previous_batch_id = self._active_batch_id(connection)
                prepare_started_at = (
                    perf_counter() if instrumentation is not None else None
                )
                # SQL is selected from a fixed module constant.
                # pi-lens-ignore: python-sql-injection
                previous_issue_rows = connection.execute(
                    ACTIVE_ISSUE_SELECT_SQL
                ).fetchall()
                previous_issues = tuple(
                    _issue_from_row(row) for row in previous_issue_rows
                )
                reconciled = reconcile_issue_lifecycle(
                    previous_issues,
                    batch.issues,
                    observed_at=batch.observed_at,
                )
                reconciled_issues = tuple(reconciled.current_issues) + tuple(
                    record.issue for record in reconciled.resolved
                )
                prepared = _prepare_candidate(
                    replace(batch, issues=reconciled_issues),
                    instrumentation,
                )
                if instrumentation is not None and prepare_started_at is not None:
                    instrumentation.prepare_ms = int(
                        (perf_counter() - prepare_started_at) * 1000
                    )

                insert_started_at = (
                    perf_counter() if instrumentation is not None else None
                )
                self._insert_candidate(connection, prepared)
                if instrumentation is not None and insert_started_at is not None:
                    instrumentation.insert_ms = int(
                        (perf_counter() - insert_started_at) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_candidate_insert")

                validate_started_at = (
                    perf_counter() if instrumentation is not None else None
                )
                self._validate_candidate_in_transaction(
                    connection,
                    prepared,
                    instrumentation,
                )
                if instrumentation is not None and validate_started_at is not None:
                    instrumentation.validate_ms = int(
                        (perf_counter() - validate_started_at) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_validate")

                active_switch_started_at = (
                    perf_counter() if instrumentation is not None else None
                )
                connection.execute(
                    "UPDATE lineage_edge SET is_active = 0 WHERE is_active = 1"
                )
                connection.execute(
                    "UPDATE lineage_issue SET is_active = 0 WHERE is_active = 1"
                )
                connection.execute(
                    "UPDATE lineage_program_state SET is_active = 0 WHERE is_active = 1"
                )
                connection.execute(
                    "UPDATE lineage_batch SET is_active = 0 WHERE is_active = 1"
                )
                connection.execute(
                    """
                    UPDATE lineage_batch
                    SET is_active = 1, published_at = ?
                    WHERE batch_id = ?
                    """,
                    (
                        _datetime_text(prepared.batch.observed_at),
                        prepared.batch.batch_id,
                    ),
                )
                connection.execute(
                    "UPDATE lineage_edge SET is_active = 1 WHERE batch_id = ?",
                    (prepared.batch.batch_id,),
                )
                connection.execute(
                    """
                    UPDATE lineage_issue
                    SET is_active = CASE
                        WHEN disposition = 'RESOLVED' THEN 0
                        ELSE 1
                    END
                    WHERE batch_id = ?
                    """,
                    (prepared.batch.batch_id,),
                )
                connection.execute(
                    "UPDATE lineage_program_state SET is_active = 1 WHERE batch_id = ?",
                    (prepared.batch.batch_id,),
                )
                if instrumentation is not None and active_switch_started_at is not None:
                    instrumentation.active_switch_ms = int(
                        (perf_counter() - active_switch_started_at) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_active_switch")

                commit_started_at = (
                    perf_counter() if instrumentation is not None else None
                )
                connection.commit()
                if instrumentation is not None and commit_started_at is not None:
                    instrumentation.commit_ms = int(
                        (perf_counter() - commit_started_at) * 1000
                    )
            except Exception:
                connection.rollback()
                raise

        prepared_batch = prepared.batch
        return PublishResult(
            batch_id=prepared_batch.batch_id,
            edge_count=len(prepared_batch.edges),
            issue_count=len(prepared_batch.issues),
            previous_batch_id=previous_batch_id,
            program_count=len(prepared_batch.program_states),
        )

    publish_batch = publish

    def validate_candidate(self, batch_id: str) -> None:
        """校验已写入的 inactive candidate，不改变 active 状态。"""

        if not isinstance(batch_id, str) or not batch_id.strip():
            raise ValueError("batch_id must be a non-empty string")
        with self._connection_scope() as connection:
            row = connection.execute(
                "SELECT observed_at, edge_count, issue_count FROM lineage_batch WHERE batch_id = ?",
                (batch_id.strip(),),
            ).fetchone()
            if row is None:
                raise ValueError("candidate batch does not exist")
            candidate_id = batch_id.strip()
            edges = self.read_edges(batch_id=candidate_id, active_only=False)
            issues = self.read_issues(batch_id=candidate_id, active_only=False)
            program_states = self.read_program_states(
                batch_id=candidate_id, active_only=False
            )
            self._validate_candidate_in_transaction(
                connection,
                _prepare_candidate(
                    MaterializationBatch(
                        batch_id=candidate_id,
                        observed_at=_parse_datetime(str(row[0])),
                        edges=edges,
                        issues=issues,
                        program_states=program_states,
                    )
                ),
            )

    def get_active_batch_id(self) -> str | None:
        with self._connection_scope() as connection:
            return self._active_batch_id(connection)

    def _read_rows(
        self,
        select_sql: str,
        order_sql: str,
        *,
        batch_id: str | None,
        active_only: bool,
    ) -> list[Any]:
        if batch_id is not None and active_only:
            sql = select_sql + " WHERE batch_id = ? AND is_active = 1" + order_sql
            params: tuple[object, ...] = (batch_id,)
        elif batch_id is not None:
            sql = select_sql + " WHERE batch_id = ?" + order_sql
            params = (batch_id,)
        elif active_only:
            sql = select_sql + " WHERE is_active = 1" + order_sql
            params = ()
        else:
            sql = select_sql + order_sql
            params = ()
        with self._connection_scope() as connection:
            # SQL is selected from fixed branches above; values remain parameters.
            # pi-lens-ignore: python-sql-injection
            return connection.execute(sql, params).fetchall()

    def _read_neighbor_edges(
        self,
        *,
        environment: str,
        table: str,
        source_profile: str | None,
        outgoing: bool,
    ) -> tuple[LineageEdge, ...]:
        for value, field_name in ((environment, "environment"), (table, "table")):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if source_profile is not None and (
            not isinstance(source_profile, str) or not source_profile.strip()
        ):
            raise ValueError("source_profile must be a non-empty string or None")

        if outgoing:
            if source_profile is None:
                query_sql = EDGE_OUTGOING_NEIGHBOR_SQL
            else:
                query_sql = EDGE_OUTGOING_PROFILE_NEIGHBOR_SQL
        elif source_profile is None:
            query_sql = EDGE_INCOMING_NEIGHBOR_SQL
        else:
            query_sql = EDGE_INCOMING_PROFILE_NEIGHBOR_SQL

        canonical_table = canonicalize_dataset_name(table)
        if canonical_table is None:
            raise ValueError("table must be a qualified schema.table dataset reference")
        params: list[object] = [environment.strip(), canonical_table]
        if source_profile is not None:
            params.append(source_profile.strip())

        with self._connection_scope() as connection:
            # SQL is selected from fixed source/target/profile branches above;
            # table, environment and profile values remain parameters.
            # pi-lens-ignore: python-sql-injection
            rows = connection.execute(query_sql, tuple(params)).fetchall()
        return tuple(_edge_from_row(row) for row in rows)

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        """只读 active snapshot 中从 source_table 出发的窄 edge 集合。"""

        return self._read_neighbor_edges(
            environment=environment,
            table=source_table,
            source_profile=source_profile,
            outgoing=True,
        )

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        """只读 active snapshot 中指向 target_table 的窄 edge 集合。"""

        return self._read_neighbor_edges(
            environment=environment,
            table=target_table,
            source_profile=source_profile,
            outgoing=False,
        )

    def read_edges(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[LineageEdge, ...]:
        rows = self._read_rows(
            EDGE_SELECT_SQL,
            EDGE_ORDER_SQL,
            batch_id=batch_id,
            active_only=active_only,
        )
        return tuple(_edge_from_row(row) for row in rows)

    def read_program_states(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[ProgramState, ...]:
        rows = self._read_rows(
            PROGRAM_STATE_SELECT_SQL,
            PROGRAM_STATE_ORDER_SQL,
            batch_id=batch_id,
            active_only=active_only,
        )
        return tuple(_program_state_from_row(row) for row in rows)

    def list_batch_metadata(self) -> tuple[BatchMetadata, ...]:
        """按 observed_at 返回所有历史 batch，旧 batch 只读不修改。"""

        with self._connection_scope() as connection:
            # SQL is selected from a fixed module constant.
            # pi-lens-ignore: python-sql-injection
            rows = connection.execute(BATCH_METADATA_SELECT_SQL).fetchall()
        return tuple(_batch_metadata_from_row(row) for row in rows)

    list_batches = list_batch_metadata

    def get_batch_metadata(self, batch_id: str) -> BatchMetadata | None:
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise ValueError("batch_id must be a non-empty string")
        return next(
            (
                item
                for item in self.list_batch_metadata()
                if item.batch_id == batch_id.strip()
            ),
            None,
        )

    def reconcile_issue_lifecycle(
        self,
        previous_batch_id: str,
        current_batch_id: str,
    ):
        current_metadata = self.get_batch_metadata(current_batch_id)
        if current_metadata is None:
            raise ValueError("current batch does not exist")
        current_issues = tuple(
            issue
            for issue in self.read_issues(batch_id=current_batch_id)
            if issue.disposition is not IssueDisposition.RESOLVED
        )
        return reconcile_issue_lifecycle(
            self.read_issues(batch_id=previous_batch_id),
            current_issues,
            observed_at=current_metadata.observed_at,
        )

    def replay_issue_policy(
        self,
        policy: AuditPolicy,
        *,
        source_batch_id: str | None = None,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> PublishResult:
        """从既有 batch 重放 policy；不重建 parser/DAG，也不改写旧 batch。"""

        if not isinstance(policy, AuditPolicy):
            raise TypeError("policy must be an AuditPolicy")
        source_id = source_batch_id or self.get_active_batch_id()
        if source_id is None:
            raise ValueError("no source batch is available for policy replay")
        source_metadata = self.get_batch_metadata(source_id)
        if source_metadata is None:
            raise ValueError("source batch does not exist")
        resolved_batch_id = batch_id or new_batch_id()
        if not isinstance(resolved_batch_id, str) or not resolved_batch_id.strip():
            raise ValueError("batch_id must be a non-empty string or None")
        resolved_observed_at = observed_at or source_metadata.observed_at
        if not isinstance(resolved_observed_at, datetime):
            raise TypeError("observed_at must be a datetime or None")
        return self.publish(
            MaterializationBatch(
                batch_id=resolved_batch_id.strip(),
                observed_at=resolved_observed_at,
                edges=self.read_edges(batch_id=source_id),
                issues=replay_audit_policy(
                    self.read_issues(batch_id=source_id),
                    policy,
                    batch_id=resolved_batch_id.strip(),
                    observed_at=resolved_observed_at,
                ),
                program_states=self.read_program_states(batch_id=source_id),
            )
        )

    def set_issue_disposition(
        self,
        stable_key: str,
        disposition: IssueDisposition | str,
        *,
        source_batch_id: str | None = None,
        batch_id: str | None = None,
        observed_at: datetime | None = None,
        updated_by: str | None = None,
    ) -> PublishResult:
        """以新 batch 记录人工 disposition，保留原 batch/history 不变。"""

        if not isinstance(stable_key, str) or not stable_key.strip():
            raise ValueError("stable_key must be a non-empty string")
        resolved_disposition = IssueDisposition(disposition)
        source_id = source_batch_id or self.get_active_batch_id()
        if source_id is None:
            raise ValueError("no source batch is available for disposition update")
        source_metadata = self.get_batch_metadata(source_id)
        if source_metadata is None:
            raise ValueError("source batch does not exist")
        issues = self.read_issues(batch_id=source_id)
        requested_key = stable_key.strip()
        matches: list[LineageIssue] = []
        for issue in issues:
            issue_key = issue.stable_key
            if issue_key is None:
                issue_key = AuditFact.from_issue(issue).stable_issue_identity
            if issue_key == requested_key:
                matches.append(issue)
        if len(matches) != 1:
            raise ValueError("stable_key must identify exactly one issue")
        resolved_batch_id = batch_id or new_batch_id()
        resolved_observed_at = observed_at or source_metadata.observed_at
        updated = matches[0].with_disposition(
            resolved_disposition,
            updated_at=resolved_observed_at,
            updated_by=updated_by,
        )
        if updated.stable_key is None:
            updated = replace(updated, stable_key=requested_key)
        updated_issues = tuple(
            updated if issue is matches[0] else issue
            for issue in issues
        )
        return self.publish(
            MaterializationBatch(
                batch_id=resolved_batch_id,
                observed_at=resolved_observed_at,
                edges=self.read_edges(batch_id=source_id),
                issues=updated_issues,
                program_states=self.read_program_states(batch_id=source_id),
            )
        )

    def diff_lineage_batches(
        self,
        previous_batch_id: str,
        current_batch_id: str,
    ):
        return diff_lineage_batches(
            self.read_edges(batch_id=previous_batch_id),
            self.read_edges(batch_id=current_batch_id),
        )

    compare_batches = diff_lineage_batches

    def diff_environments(
        self,
        dev_batch_id: str | None = None,
        prod_batch_id: str | None = None,
        *,
        dev_environment: str = "DEV",
        prod_environment: str = "PROD",
        dev_source_profile: str | None = None,
        prod_source_profile: str | None = None,
    ):
        return diff_environments(
            self.read_edges(batch_id=dev_batch_id, active_only=dev_batch_id is None),
            self.read_edges(
                batch_id=prod_batch_id,
                active_only=prod_batch_id is None,
            ),
            dev_environment=dev_environment,
            prod_environment=prod_environment,
            dev_source_profile=dev_source_profile,
            prod_source_profile=prod_source_profile,
        )

    def read_issues(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[LineageIssue, ...]:
        rows = self._read_rows(
            ISSUE_SELECT_SQL,
            ISSUE_ORDER_SQL,
            batch_id=batch_id,
            active_only=active_only,
        )
        return tuple(_issue_from_row(row) for row in rows)


MaterializationSQLiteStore = SQLiteMaterializationStore


def initialize_materialization_schema(
    db_path: str | Path | sqlite3.Connection = DEFAULT_MATERIALIZATION_DB_PATH,
) -> None:
    """初始化 SQLite reference schema。"""

    SQLiteMaterializationStore(db_path).initialize_schema()


def publish_materialization_batch(
    batch: MaterializationBatch,
    db_path: str | Path | sqlite3.Connection = DEFAULT_MATERIALIZATION_DB_PATH,
    *,
    stage_hook: Callable[[str], Any] | None = None,
    instrumentation: SQLitePublishMetrics | None = None,
) -> PublishResult:
    """函数式 publish facade，供 crontab 入口和 demo 使用。"""

    return SQLiteMaterializationStore(db_path).publish(
        batch,
        stage_hook=stage_hook,
        instrumentation=instrumentation,
    )


__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "DEFAULT_MATERIALIZATION_DB_PATH",
    "MaterializationSQLiteStore",
    "PublishResult",
    "SCHEMA_SQL",
    "SQLiteMaterializationStore",
    "SQLitePublishMetrics",
    "initialize_materialization_schema",
    "publish_materialization_batch",
]
