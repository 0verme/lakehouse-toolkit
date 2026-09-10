"""DWS repository for configured schedule lineage facts.

The schedule table owns its snapshot rows because Issue #84's lineage_batch has
one global active boundary for the SQL lineage projections. Reusing that batch
control would retire an SQL snapshot every time the independent schedule job
runs. This repository therefore reuses Issue #84's connection and transaction
helpers, while keeping schedule ``batch_id`` / ``is_active`` history isolated
inside ``dwp.lineage_schedule_edge``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any

from shared.lineage.evolution import SnapshotScope
from shared.lineage.materialization_dws import (
    DWSMaterializationStore,
    _begin_transaction,
    _commit,
    _rollback,
)
from shared.lineage.schedule import (
    ScheduleLineageEdge,
    deduplicate_schedule_edges,
    normalize_schedule_table_key,
    schedule_edge_key,
    schedule_row_key,
)

INSERT_SCHEDULE_EDGE_SQL = """
    INSERT INTO dwp.lineage_schedule_edge(
        row_key, schedule_edge_key, environment, source_profile, process_name,
        project_version_key, raw_source_table, raw_target_table, source_table,
        target_table, batch_id, observed_at, first_seen_at, last_seen_at,
        last_changed_at, is_active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
SELECT_SCHEDULE_EDGE_SQL = """
    SELECT row_key, schedule_edge_key, environment, source_profile, process_name,
           project_version_key, raw_source_table, raw_target_table, source_table,
           target_table, batch_id, observed_at, first_seen_at, last_seen_at,
           last_changed_at, is_active, created_at, updated_at
    FROM dwp.lineage_schedule_edge
"""
DEACTIVATE_SCHEDULE_EDGE_SQL = (
    "UPDATE dwp.lineage_schedule_edge SET is_active = FALSE WHERE is_active = TRUE"
)
ACTIVATE_SCHEDULE_EDGE_SQL = """
    UPDATE dwp.lineage_schedule_edge
    SET is_active = TRUE
    WHERE batch_id = ?
"""

_MAX_STORED_TEXT = 512


def _required_text(value: object, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} must not be NULL")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(text) > _MAX_STORED_TEXT:
        raise ValueError(f"{field_name} exceeds DWS column length")
    return text


def _key_text(value: object, field_name: str) -> str:
    text = _required_text(value, field_name)
    if len(text) > 128:
        raise ValueError(f"{field_name} exceeds DWS key length")
    if "\x1f" in text:
        raise ValueError(f"{field_name} contains the stable-key separator")
    return text


def _timestamp_param(value: datetime | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None")
    return value.isoformat()


def _parse_timestamp(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        raise ValueError(f"{field_name} must not be NULL")
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a valid timestamp") from exc


def _stored_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().upper() in {"TRUE", "1", "T"}:
        return True
    if isinstance(value, str) and value.strip().upper() in {"FALSE", "0", "F"}:
        return False
    raise ValueError(f"{field_name} is not a valid boolean")


@dataclass(frozen=True, slots=True)
class DWSScheduleLineageRow:
    """One persisted schedule fact, including snapshot/history fields."""

    row_key: str
    schedule_edge_key: str
    environment: str
    source_profile: str
    process_name: str
    project_version_key: str
    raw_source_table: str
    raw_target_table: str
    source_table: str
    target_table: str
    batch_id: str
    observed_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @property
    def edge(self) -> ScheduleLineageEdge:
        return ScheduleLineageEdge(
            environment=self.environment,
            source_profile=self.source_profile,
            process_name=self.process_name,
            project_version_key=self.project_version_key,
            raw_source_table=self.raw_source_table,
            raw_target_table=self.raw_target_table,
            source_table=self.source_table,
            target_table=self.target_table,
        )

    @property
    def scope(self) -> tuple[str, str]:
        return (self.environment, self.source_profile)


@dataclass(frozen=True, slots=True)
class DWSSchedulePublishResult:
    batch_id: str
    edge_count: int
    previous_batch_id: str | None


@dataclass(slots=True)
class DWSSchedulePublishMetrics:
    prepare_ms: int = 0
    insert_ms: int = 0
    validate_ms: int = 0
    active_switch_ms: int = 0
    commit_ms: int = 0
    prepared_edge_rows: int = 0
    validated_edge_rows: int = 0


@dataclass(frozen=True, slots=True)
class _PreparedScheduleCandidate:
    batch_id: str
    observed_at: datetime
    previous_batch_id: str | None
    previous_active_keys: frozenset[str]
    rows: tuple[DWSScheduleLineageRow, ...]


def _scope_key(scope: object) -> tuple[str, str]:
    if isinstance(scope, SnapshotScope):
        environment, source_profile = scope.key
    elif isinstance(scope, ScheduleLineageEdge):
        return scope.scope
    elif isinstance(scope, tuple) and len(scope) == 2:
        environment, source_profile = scope
    else:
        raise TypeError("schedule snapshot scope must be a SnapshotScope or pair")
    return (
        _required_text(environment, "scope environment"),
        _required_text(source_profile, "scope source_profile"),
    )


def _scope_keys(scopes: Iterable[object] | None) -> frozenset[tuple[str, str]]:
    if scopes is None:
        return frozenset()
    return frozenset(_scope_key(scope) for scope in scopes)


def _row_from_edge(
    edge: ScheduleLineageEdge,
    *,
    batch_id: str,
    observed_at: datetime,
    previous: DWSScheduleLineageRow | None,
    observed_now: bool,
) -> DWSScheduleLineageRow:
    changed = previous is None or (
        previous.environment != edge.environment
        or previous.source_profile != edge.source_profile
        or previous.process_name != edge.process_name
        or previous.project_version_key != edge.project_version_key
        or previous.raw_source_table != edge.raw_source_table
        or previous.raw_target_table != edge.raw_target_table
        or previous.source_table != edge.source_table
        or previous.target_table != edge.target_table
    )
    first_seen_at = observed_at if previous is None else previous.first_seen_at
    last_seen_at = (
        observed_at if observed_now or previous is None else previous.last_seen_at
    )
    last_changed_at = (
        observed_at if changed or previous is None else previous.last_changed_at
    )
    return DWSScheduleLineageRow(
        row_key=schedule_row_key(batch_id, edge.schedule_edge_key),
        schedule_edge_key=edge.schedule_edge_key,
        environment=edge.environment,
        source_profile=edge.source_profile,
        process_name=edge.process_name,
        project_version_key=edge.project_version_key,
        raw_source_table=edge.raw_source_table,
        raw_target_table=edge.raw_target_table,
        source_table=edge.source_table,
        target_table=edge.target_table,
        batch_id=batch_id,
        observed_at=observed_at,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        last_changed_at=last_changed_at,
        is_active=False,
        created_at=observed_at if previous is None else previous.created_at,
        updated_at=observed_at,
    )


def _rebase_row(
    row: DWSScheduleLineageRow,
    *,
    batch_id: str,
    observed_at: datetime,
) -> DWSScheduleLineageRow:
    return DWSScheduleLineageRow(
        row_key=schedule_row_key(batch_id, row.schedule_edge_key),
        schedule_edge_key=row.schedule_edge_key,
        environment=row.environment,
        source_profile=row.source_profile,
        process_name=row.process_name,
        project_version_key=row.project_version_key,
        raw_source_table=row.raw_source_table,
        raw_target_table=row.raw_target_table,
        source_table=row.source_table,
        target_table=row.target_table,
        batch_id=batch_id,
        observed_at=observed_at,
        first_seen_at=row.first_seen_at,
        last_seen_at=row.last_seen_at,
        last_changed_at=row.last_changed_at,
        is_active=False,
        created_at=row.created_at,
        updated_at=observed_at,
    )


def _row_values(row: DWSScheduleLineageRow) -> tuple[object, ...]:
    return (
        row.row_key,
        row.schedule_edge_key,
        row.environment,
        row.source_profile,
        row.process_name,
        row.project_version_key,
        row.raw_source_table,
        row.raw_target_table,
        row.source_table,
        row.target_table,
        row.batch_id,
        _timestamp_param(row.observed_at, "observed_at"),
        _timestamp_param(row.first_seen_at, "first_seen_at"),
        _timestamp_param(row.last_seen_at, "last_seen_at"),
        _timestamp_param(row.last_changed_at, "last_changed_at"),
        row.is_active,
        _timestamp_param(row.created_at, "created_at"),
        _timestamp_param(row.updated_at, "updated_at"),
    )


def _stored_row(raw: object) -> DWSScheduleLineageRow:
    if not isinstance(raw, Sequence):
        raise ValueError("stored schedule row is not a sequence")
    values = tuple(raw)
    if len(values) != 18:
        raise ValueError("stored schedule row has an unexpected column count")
    return DWSScheduleLineageRow(
        row_key=_key_text(values[0], "row_key"),
        schedule_edge_key=_key_text(values[1], "schedule_edge_key"),
        environment=_required_text(values[2], "environment"),
        source_profile=_required_text(values[3], "source_profile"),
        process_name=_required_text(values[4], "process_name"),
        project_version_key=_required_text(values[5], "project_version_key"),
        raw_source_table=_required_text(values[6], "raw_source_table"),
        raw_target_table=_required_text(values[7], "raw_target_table"),
        source_table=_required_text(values[8], "source_table"),
        target_table=_required_text(values[9], "target_table"),
        batch_id=_key_text(values[10], "batch_id"),
        observed_at=_parse_timestamp(values[11], "observed_at"),
        first_seen_at=_parse_timestamp(values[12], "first_seen_at"),
        last_seen_at=_parse_timestamp(values[13], "last_seen_at"),
        last_changed_at=_parse_timestamp(values[14], "last_changed_at"),
        is_active=_stored_bool(values[15], "is_active"),
        created_at=_parse_timestamp(values[16], "created_at"),
        updated_at=_parse_timestamp(values[17], "updated_at"),
    )


def _active_batch_id(rows: Iterable[DWSScheduleLineageRow]) -> str | None:
    batch_ids = {row.batch_id for row in rows}
    if len(batch_ids) > 1:
        raise ValueError("schedule active rows contain more than one batch")
    return next(iter(batch_ids), None)


class DWSScheduleLineageStore:
    """Atomic repository for ``dwp.lineage_schedule_edge``.

    ``DWSMaterializationStore`` is composed as the #84 connection boundary.
    Only schedule-specific row preparation and validation live here; no second
    password loader, JDBC connector, or batch-control table is introduced.
    """

    backend_name = "dws-schedule"

    def __init__(
        self,
        profile: str | None = None,
        *,
        connection: Any | None = None,
        connection_factory: Any | None = None,
    ) -> None:
        self._connection_boundary = DWSMaterializationStore(
            profile=profile,
            connection=connection,
            connection_factory=connection_factory,
        )

    @property
    def connection(self) -> Any | None:
        return self._connection_boundary.connection

    @contextmanager
    def _connection_scope(self) -> Iterator[Any]:
        with self._connection_boundary._connection_scope() as connection:
            yield connection

    @staticmethod
    @contextmanager
    def _cursor_scope(connection: Any) -> Iterator[Any]:
        with DWSMaterializationStore._cursor_scope(connection) as cursor:
            yield cursor

    @staticmethod
    def _execute(cursor: Any, sql: str, params: Iterable[object] = ()) -> Any:
        return DWSMaterializationStore._execute(cursor, sql, params)

    @staticmethod
    def _executemany(cursor: Any, sql: str, rows: Iterable[Iterable[object]]) -> None:
        DWSMaterializationStore._executemany(cursor, sql, rows)

    def _fetch_rows(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSScheduleLineageRow, ...]:
        conditions: list[str] = []
        params: list[object] = []
        if batch_id is not None:
            _key_text(batch_id, "batch_id")
            conditions.append("batch_id = ?")
            params.append(batch_id)
        if active_only:
            conditions.append("is_active = TRUE")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        with self._cursor_scope(connection) as cursor:
            self._execute(
                cursor,
                SELECT_SCHEDULE_EDGE_SQL
                + where
                + " ORDER BY schedule_edge_key, row_key",
                params,
            )
            raw_rows = cursor.fetchall()
        return tuple(_stored_row(row) for row in raw_rows)

    @staticmethod
    def _count(connection: Any, batch_id: str) -> int:
        cursor = connection.cursor()
        try:
            # Fixed table SQL; batch_id is always bound.
            cursor.execute(
                "SELECT COUNT(*) FROM dwp.lineage_schedule_edge WHERE batch_id = ?",
                (batch_id,),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
        if row is None:
            raise ValueError("schedule candidate count returned no row")
        return int(row[0])

    def _prepare_candidate(
        self,
        edges: Iterable[ScheduleLineageEdge],
        *,
        batch_id: str,
        observed_at: datetime,
        previous_rows: tuple[DWSScheduleLineageRow, ...],
        complete_snapshot: bool,
        snapshot_scopes: Iterable[object] | None,
    ) -> _PreparedScheduleCandidate:
        _key_text(batch_id, "batch_id")
        if not isinstance(observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        if not isinstance(complete_snapshot, bool):
            raise TypeError("complete_snapshot must be a boolean")
        scopes = _scope_keys(snapshot_scopes)
        if complete_snapshot and not scopes:
            raise ValueError("complete schedule snapshot requires explicit scopes")
        incoming = deduplicate_schedule_edges(edges)
        if complete_snapshot and any(edge.scope not in scopes for edge in incoming):
            raise ValueError("schedule edge is outside the declared snapshot scope")

        previous_by_key: dict[str, DWSScheduleLineageRow] = {}
        for row in previous_rows:
            if row.schedule_edge_key in previous_by_key:
                raise ValueError("active schedule rows contain duplicate identity")
            if row.schedule_edge_key != schedule_edge_key(row.edge):
                raise ValueError("stored schedule stable identity is inconsistent")
            if row.row_key != schedule_row_key(row.batch_id, row.schedule_edge_key):
                raise ValueError("stored schedule row_key is inconsistent")
            previous_by_key[row.schedule_edge_key] = row
        previous_batch_id = _active_batch_id(previous_rows)
        previous_active_keys = frozenset(previous_by_key)

        incoming_by_key = {edge.schedule_edge_key: edge for edge in incoming}
        prepared: dict[str, DWSScheduleLineageRow] = {}
        for key, previous in previous_by_key.items():
            if (
                complete_snapshot
                and previous.scope in scopes
                and key not in incoming_by_key
            ):
                continue
            prepared[key] = _rebase_row(
                previous,
                batch_id=batch_id,
                observed_at=observed_at,
            )
        for key, edge in incoming_by_key.items():
            prepared[key] = _row_from_edge(
                edge,
                batch_id=batch_id,
                observed_at=observed_at,
                previous=previous_by_key.get(key),
                observed_now=True,
            )
        rows = tuple(sorted(prepared.values(), key=lambda row: row.schedule_edge_key))
        return _PreparedScheduleCandidate(
            batch_id=batch_id,
            observed_at=observed_at,
            previous_batch_id=previous_batch_id,
            previous_active_keys=previous_active_keys,
            rows=rows,
        )

    def _insert_candidate(
        self,
        connection: Any,
        candidate: _PreparedScheduleCandidate,
    ) -> None:
        with self._cursor_scope(connection) as cursor:
            self._executemany(
                cursor,
                INSERT_SCHEDULE_EDGE_SQL,
                (_row_values(row) for row in candidate.rows),
            )

    def _validate_candidate(
        self,
        connection: Any,
        candidate: _PreparedScheduleCandidate,
        instrumentation: DWSSchedulePublishMetrics | None = None,
    ) -> None:
        row_keys: set[str] = set()
        stable_keys: set[str] = set()
        for row in candidate.rows:
            _key_text(row.row_key, "row_key")
            _key_text(row.schedule_edge_key, "schedule_edge_key")
            if row.row_key in row_keys or row.schedule_edge_key in stable_keys:
                raise ValueError("candidate contains duplicate schedule identity")
            row_keys.add(row.row_key)
            stable_keys.add(row.schedule_edge_key)
            if row.batch_id != candidate.batch_id or row.is_active:
                raise ValueError("candidate schedule row has invalid batch lifecycle")
            edge = row.edge
            if row.schedule_edge_key != edge.schedule_edge_key:
                raise ValueError("schedule stable identity is inconsistent")
            if row.row_key != schedule_row_key(
                candidate.batch_id, row.schedule_edge_key
            ):
                raise ValueError("schedule row_key is inconsistent")
            if row.source_table != normalize_schedule_table_key(row.raw_source_table):
                raise ValueError("schedule source comparison identity is inconsistent")
            if row.target_table != normalize_schedule_table_key(row.raw_target_table):
                raise ValueError("schedule target comparison identity is inconsistent")

        stored_rows = self._fetch_rows(connection, batch_id=candidate.batch_id)
        for stored, prepared in zip(stored_rows, candidate.rows):
            if stored.schedule_edge_key != stored.edge.schedule_edge_key:
                raise ValueError("stored schedule stable identity is inconsistent")
            if stored != prepared:
                raise ValueError(
                    "stored schedule candidate does not match prepared rows"
                )
        if len(stored_rows) != len(candidate.rows) or self._count(
            connection, candidate.batch_id
        ) != len(candidate.rows):
            raise ValueError(
                "schedule candidate row count does not match prepared rows"
            )
        active_rows = self._fetch_rows(connection, active_only=True)
        active_batch_id = _active_batch_id(active_rows)
        active_keys = frozenset(row.schedule_edge_key for row in active_rows)
        if active_batch_id != candidate.previous_batch_id:
            raise ValueError("previous active schedule snapshot changed")
        if active_keys != candidate.previous_active_keys:
            raise ValueError("previous active schedule rows changed")
        if instrumentation is not None:
            instrumentation.validated_edge_rows = len(candidate.rows)

    def _active_switch(
        self,
        connection: Any,
        candidate: _PreparedScheduleCandidate,
    ) -> None:
        with self._cursor_scope(connection) as cursor:
            self._execute(cursor, DEACTIVATE_SCHEDULE_EDGE_SQL)
            self._execute(cursor, ACTIVATE_SCHEDULE_EDGE_SQL, (candidate.batch_id,))
        active_rows = self._fetch_rows(connection, active_only=True)
        active_batch_id = _active_batch_id(active_rows)
        active_keys = frozenset(row.schedule_edge_key for row in active_rows)
        expected_keys = frozenset(row.schedule_edge_key for row in candidate.rows)
        if expected_keys:
            published = (
                active_batch_id == candidate.batch_id and active_keys == expected_keys
            )
        else:
            # With no schedule_batch control table, an empty active snapshot is
            # represented by zero active fact rows; the publish still commits
            # atomically and the returned batch id remains run provenance.
            published = active_batch_id is None and not active_keys
        if not published:
            raise ValueError("active schedule switch did not publish the candidate")

    @staticmethod
    def _call_stage_hook(stage_hook: Any, stage: str) -> None:
        if stage_hook is not None:
            stage_hook(stage)

    def publish(
        self,
        edges: Iterable[ScheduleLineageEdge],
        *,
        batch_id: str,
        observed_at: datetime,
        complete_snapshot: bool = False,
        snapshot_scopes: Iterable[object] | None = None,
        stage_hook: Any = None,
        instrumentation: DWSSchedulePublishMetrics | None = None,
    ) -> DWSSchedulePublishResult:
        """Insert, validate, switch active rows and commit in one transaction."""

        if instrumentation is not None and not isinstance(
            instrumentation, DWSSchedulePublishMetrics
        ):
            raise TypeError("instrumentation must be DWSSchedulePublishMetrics or None")
        with self._connection_scope() as connection:
            restore_autocommit = _begin_transaction(connection)
            try:
                previous_rows = self._fetch_rows(connection, active_only=True)
                prepare_started = perf_counter() if instrumentation else None
                candidate = self._prepare_candidate(
                    edges,
                    batch_id=batch_id,
                    observed_at=observed_at,
                    previous_rows=previous_rows,
                    complete_snapshot=complete_snapshot,
                    snapshot_scopes=snapshot_scopes,
                )
                if instrumentation is not None:
                    instrumentation.prepared_edge_rows = len(candidate.rows)
                    if prepare_started is not None:
                        instrumentation.prepare_ms = int(
                            (perf_counter() - prepare_started) * 1000
                        )
                insert_started = perf_counter() if instrumentation else None
                self._insert_candidate(connection, candidate)
                if instrumentation is not None and insert_started is not None:
                    instrumentation.insert_ms = int(
                        (perf_counter() - insert_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_candidate_insert")
                validate_started = perf_counter() if instrumentation else None
                self._validate_candidate(connection, candidate, instrumentation)
                if instrumentation is not None and validate_started is not None:
                    instrumentation.validate_ms = int(
                        (perf_counter() - validate_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_validate")
                switch_started = perf_counter() if instrumentation else None
                self._active_switch(connection, candidate)
                if instrumentation is not None and switch_started is not None:
                    instrumentation.active_switch_ms = int(
                        (perf_counter() - switch_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_active_switch")
                commit_started = perf_counter() if instrumentation else None
                _commit(connection)
                if instrumentation is not None and commit_started is not None:
                    instrumentation.commit_ms = int(
                        (perf_counter() - commit_started) * 1000
                    )
            except Exception:
                _rollback(connection)
                raise
            finally:
                restore_autocommit()
        return DWSSchedulePublishResult(
            batch_id=candidate.batch_id,
            edge_count=len(candidate.rows),
            previous_batch_id=candidate.previous_batch_id,
        )

    publish_batch = publish

    def validate_candidate(self, batch_id: str) -> None:
        """Recheck an inserted inactive candidate without activating it."""

        with self._connection_scope() as connection:
            rows = self._fetch_rows(connection, batch_id=batch_id)
            if not rows:
                raise ValueError("schedule candidate does not exist")
            active_rows = self._fetch_rows(connection, active_only=True)
            candidate = _PreparedScheduleCandidate(
                batch_id=batch_id,
                observed_at=rows[0].observed_at,
                previous_batch_id=_active_batch_id(active_rows),
                previous_active_keys=frozenset(
                    row.schedule_edge_key for row in active_rows
                ),
                rows=rows,
            )
            self._validate_candidate(connection, candidate)

    def read_rows(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSScheduleLineageRow, ...]:
        with self._connection_scope() as connection:
            return self._fetch_rows(
                connection,
                batch_id=batch_id,
                active_only=active_only,
            )

    def read_edges(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[ScheduleLineageEdge, ...]:
        return tuple(
            row.edge
            for row in self.read_rows(batch_id=batch_id, active_only=active_only)
        )

    def get_active_batch_id(self) -> str | None:
        rows = self.read_rows(active_only=True)
        return _active_batch_id(rows)


DWSScheduleLineageWriter = DWSScheduleLineageStore


__all__ = [
    "DWSScheduleLineageRow",
    "DWSScheduleLineageStore",
    "DWSScheduleLineageWriter",
    "DWSSchedulePublishMetrics",
    "DWSSchedulePublishResult",
]
