"""Presentation suppression projection for SQL/schedule reconciliation.

The raw reconciliation contract remains the three-state
``MATCH``/``SQL_ONLY``/``SCHEDULE_ONLY`` result in
:mod:`shared.lineage.reconciliation`.  This module adds a conservative,
queryable projection on top of that result.  It never changes a raw row and it
never infers a table category from a table name.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Any

from shared.lineage.dws_timestamp import (
    TIMESTAMPTZ_PARAM_SQL,
    dws_timestamp_param,
    dws_timestamp_projection,
    parse_dws_timestamp,
)
from shared.lineage.materialization_dws import (
    DWSMaterializationStore,
    _begin_transaction,
    _commit,
    _rollback,
)
from shared.lineage.reconciliation import (
    LineageReconciliationResult,
    ReconciliationStatus,
    ScheduleLineageSnapshot,
    SQLBusinessLineageSnapshot,
    normalize_lineage_comparison_table_key,
)
from shared.lineage.domain import LineageEdge
from shared.lineage.schedule import ScheduleLineageEdge

SUPPRESSION_TABLE_NAME = "lineage_reconciliation_suppression"
SUPPRESSION_CLASSIFIER_VERSION = "reconciliation-suppression-v1"
SUPPRESSION_KEY_SEPARATOR = "\x1f"
DWS_KEY_MAX_LENGTH = 128


class ReconciliationSuppressionReason(str, Enum):
    """V1 suppression reasons kept separate from raw reconciliation status."""

    NO_INTERNAL_PRODUCER = "NO_INTERNAL_PRODUCER"


class ReconciliationSuppressionError(RuntimeError):
    """Raised when suppression evidence cannot be verified safely."""


@dataclass(frozen=True, slots=True)
class ReconciliationSuppression:
    """One pure-domain suppression candidate for a raw ``SQL_ONLY`` row."""

    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    source_table: str
    target_table: str
    raw_status: ReconciliationStatus | str
    suppression_reason: ReconciliationSuppressionReason | str
    sql_batch_id: str
    schedule_batch_id: str
    classifier_version: str = SUPPRESSION_CLASSIFIER_VERSION
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "environment",
            "sql_source_profile",
            "schedule_source_profile",
            "sql_batch_id",
            "schedule_batch_id",
            "classifier_version",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            value = value.strip()
            if len(value) > 512:
                raise ValueError(f"{field_name} exceeds suppression column length")
            object.__setattr__(self, field_name, value)

        source = normalize_lineage_comparison_table_key(self.source_table)
        target = normalize_lineage_comparison_table_key(self.target_table)
        if len(source) > 512 or len(target) > 512:
            raise ValueError("comparison table exceeds suppression column length")
        object.__setattr__(self, "source_table", source)
        object.__setattr__(self, "target_table", target)

        try:
            raw_status = ReconciliationStatus(self.raw_status)
        except (TypeError, ValueError) as exc:
            raise ValueError("raw_status is not a valid reconciliation status") from exc
        if raw_status is not ReconciliationStatus.SQL_ONLY:
            raise ValueError("suppression raw_status must be SQL_ONLY")
        object.__setattr__(self, "raw_status", raw_status)

        reason = _reason_value(self.suppression_reason)
        if reason != ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER.value:
            raise ValueError("unsupported reconciliation suppression reason")
        object.__setattr__(
            self,
            "suppression_reason",
            ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
        )

        if self.observed_at is not None:
            _validate_timestamp(self.observed_at, "observed_at")

    @property
    def suppression_key(self) -> str:
        """Return the batch-independent identity of the suppressed edge."""

        return compute_reconciliation_suppression_key(
            environment=self.environment,
            sql_source_profile=self.sql_source_profile,
            schedule_source_profile=self.schedule_source_profile,
            source_table=self.source_table,
            target_table=self.target_table,
            suppression_reason=self.suppression_reason,
        )

    @property
    def row_key(self) -> str:
        """Return one materialized-observation row identity."""

        return compute_reconciliation_suppression_row_key(
            suppression_key=self.suppression_key,
            sql_batch_id=self.sql_batch_id,
            schedule_batch_id=self.schedule_batch_id,
        )

    @property
    def edge_identity(self) -> tuple[str, str]:
        """Return the normalized ``source_table -> target_table`` identity."""

        return (self.source_table, self.target_table)


def _validate_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return value


def _required_text(value: object, field_name: str, *, max_length: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    text = value.strip()
    if len(text) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length")
    if SUPPRESSION_KEY_SEPARATOR in text:
        raise ValueError(f"{field_name} contains the stable-key separator")
    return text


def _key_text(value: object, field_name: str) -> str:
    return _required_text(value, field_name, max_length=DWS_KEY_MAX_LENGTH)


def _stable_hash(prefix: str, *parts: str) -> str:
    values = (_required_text(prefix, "key prefix"),)
    values += tuple(_required_text(part, "key part") for part in parts)
    return hashlib.sha256(
        SUPPRESSION_KEY_SEPARATOR.join(values).encode("utf-8")
    ).hexdigest()


def _reason_value(value: ReconciliationSuppressionReason | str) -> str:
    return (
        value.value
        if isinstance(value, ReconciliationSuppressionReason)
        else str(value).strip()
    )


def _status_value(value: ReconciliationStatus | str) -> str:
    return (
        value.value if isinstance(value, ReconciliationStatus) else str(value).strip()
    )


def compute_reconciliation_suppression_key(
    *,
    environment: str,
    sql_source_profile: str,
    schedule_source_profile: str,
    source_table: str,
    target_table: str,
    suppression_reason: ReconciliationSuppressionReason | str,
) -> str:
    """Compute a stable identity that deliberately excludes both batch IDs."""

    reason = _reason_value(suppression_reason)
    if reason != ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER.value:
        raise ValueError("unsupported reconciliation suppression reason")
    source = normalize_lineage_comparison_table_key(source_table)
    target = normalize_lineage_comparison_table_key(target_table)
    return _stable_hash(
        "reconciliation-suppression",
        _required_text(environment, "environment"),
        _required_text(sql_source_profile, "sql_source_profile"),
        _required_text(schedule_source_profile, "schedule_source_profile"),
        source,
        target,
        reason,
    )


def compute_reconciliation_suppression_row_key(
    *,
    suppression_key: str,
    sql_batch_id: str,
    schedule_batch_id: str,
) -> str:
    """Compute one observation row key from stable identity and provenance."""

    return _stable_hash(
        "reconciliation-suppression-row",
        _key_text(suppression_key, "suppression_key"),
        _required_text(sql_batch_id, "sql_batch_id"),
        _required_text(schedule_batch_id, "schedule_batch_id"),
    )


def _snapshot_observation_time(
    result: LineageReconciliationResult,
    observed_at: datetime | None,
) -> datetime:
    sql_observed_at = result.sql_observed_at
    schedule_observed_at = result.schedule_observed_at
    if sql_observed_at is None or schedule_observed_at is None:
        raise ReconciliationSuppressionError(
            "suppression snapshot observation time is incomplete"
        )
    _validate_timestamp(sql_observed_at, "sql_observed_at")
    _validate_timestamp(schedule_observed_at, "schedule_observed_at")
    if observed_at is not None:
        return _validate_timestamp(observed_at, "observed_at")
    return max(sql_observed_at, schedule_observed_at)


def _validate_classifier_inputs(
    result: LineageReconciliationResult,
    sql_snapshot: SQLBusinessLineageSnapshot,
    schedule_snapshot: ScheduleLineageSnapshot,
) -> tuple[str, str, str]:
    if not isinstance(result, LineageReconciliationResult):
        raise TypeError("result must be a LineageReconciliationResult")
    if not isinstance(sql_snapshot, SQLBusinessLineageSnapshot):
        raise TypeError("sql_snapshot must be a SQLBusinessLineageSnapshot")
    if not isinstance(schedule_snapshot, ScheduleLineageSnapshot):
        raise TypeError("schedule_snapshot must be a ScheduleLineageSnapshot")
    if sql_snapshot.batch_id != result.sql_batch_id:
        raise ReconciliationSuppressionError("SQL snapshot batch provenance is stale")
    if schedule_snapshot.batch_id != result.schedule_batch_id:
        raise ReconciliationSuppressionError(
            "schedule snapshot batch provenance is stale"
        )

    scope = (
        result.environment,
        result.sql_source_profile,
        result.schedule_source_profile,
    )
    sql_scope = (scope[0], scope[1])
    if not sql_snapshot.snapshot_scope or sql_scope not in sql_snapshot.snapshot_scope:
        raise ReconciliationSuppressionError(
            "SQL snapshot scope is incomplete for suppression"
        )
    if not any(
        edge.environment == scope[0] and edge.source_profile == scope[2]
        for edge in schedule_snapshot.edges
    ):
        raise ReconciliationSuppressionError(
            "schedule snapshot scope is incomplete for suppression"
        )
    return scope


def classify_reconciliation_suppressions(
    result: LineageReconciliationResult,
    sql_snapshot: SQLBusinessLineageSnapshot,
    schedule_snapshot: ScheduleLineageSnapshot,
    *,
    observed_at: datetime | None = None,
) -> tuple[ReconciliationSuppression, ...]:
    """Classify conservative ``NO_INTERNAL_PRODUCER`` candidates.

    This is a pure function: it only consumes already-verified snapshots and a
    reconciliation result.  It does not connect to DWS, load configuration, or
    mutate the result.  A missing/ambiguous snapshot boundary raises
    :class:`ReconciliationSuppressionError`; callers must fail open and keep
    the raw ``SQL_ONLY`` rows visible.
    """

    environment, sql_profile, schedule_profile = _validate_classifier_inputs(
        result, sql_snapshot, schedule_snapshot
    )
    effective_observed_at = _snapshot_observation_time(result, observed_at)

    sql_produced_targets: set[str] = set()
    for edge in sql_snapshot.edges:
        if edge.environment != environment or edge.source_profile != sql_profile:
            continue
        if not isinstance(edge, LineageEdge):
            raise ReconciliationSuppressionError("SQL producer edge type is invalid")
        sql_produced_targets.add(
            normalize_lineage_comparison_table_key(edge.target_table)
        )

    schedule_produced_targets: set[str] = set()
    for edge in schedule_snapshot.edges:
        if edge.environment != environment or edge.source_profile != schedule_profile:
            continue
        if not isinstance(edge, ScheduleLineageEdge):
            raise ReconciliationSuppressionError(
                "schedule producer edge type is invalid"
            )
        schedule_produced_targets.add(
            normalize_lineage_comparison_table_key(edge.target_table)
        )

    candidates: list[ReconciliationSuppression] = []
    for row in result.rows:
        if row.status is not ReconciliationStatus.SQL_ONLY:
            continue
        source = normalize_lineage_comparison_table_key(row.source_table)
        if source in sql_produced_targets or source in schedule_produced_targets:
            continue
        candidates.append(
            ReconciliationSuppression(
                environment=environment,
                sql_source_profile=sql_profile,
                schedule_source_profile=schedule_profile,
                source_table=source,
                target_table=row.target_table,
                raw_status=row.status,
                suppression_reason=ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER,
                sql_batch_id=result.sql_batch_id,
                schedule_batch_id=result.schedule_batch_id,
                classifier_version=SUPPRESSION_CLASSIFIER_VERSION,
                observed_at=effective_observed_at,
            )
        )
    return tuple(
        sorted(candidates, key=lambda item: (item.target_table, item.source_table))
    )


@dataclass(frozen=True, slots=True)
class DWSReconciliationSuppressionRow:
    """Persistence projection for ``dwp.lineage_reconciliation_suppression``."""

    row_key: str
    suppression_key: str
    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    source_table: str
    target_table: str
    raw_status: ReconciliationStatus | str
    suppression_reason: ReconciliationSuppressionReason | str
    sql_batch_id: str
    schedule_batch_id: str
    classifier_version: str
    observed_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        candidate = ReconciliationSuppression(
            environment=self.environment,
            sql_source_profile=self.sql_source_profile,
            schedule_source_profile=self.schedule_source_profile,
            source_table=self.source_table,
            target_table=self.target_table,
            raw_status=self.raw_status,
            suppression_reason=self.suppression_reason,
            sql_batch_id=self.sql_batch_id,
            schedule_batch_id=self.schedule_batch_id,
            classifier_version=self.classifier_version,
            observed_at=self.observed_at,
        )
        if self.suppression_key != candidate.suppression_key:
            raise ValueError("suppression_key is inconsistent with the row identity")
        if self.row_key != candidate.row_key:
            raise ValueError("row_key is inconsistent with the observation identity")
        for field_name in (
            "environment",
            "sql_source_profile",
            "schedule_source_profile",
            "source_table",
            "target_table",
            "sql_batch_id",
            "schedule_batch_id",
            "classifier_version",
        ):
            object.__setattr__(self, field_name, getattr(candidate, field_name))
        object.__setattr__(self, "raw_status", candidate.raw_status)
        object.__setattr__(self, "suppression_reason", candidate.suppression_reason)
        _key_text(self.row_key, "row_key")
        _key_text(self.suppression_key, "suppression_key")
        if not isinstance(self.is_active, bool):
            raise TypeError("is_active must be a boolean")
        for field_name in ("first_seen_at", "last_seen_at", "created_at", "updated_at"):
            _validate_timestamp(getattr(self, field_name), field_name)

    @property
    def edge_identity(self) -> tuple[str, str]:
        return (self.source_table, self.target_table)

    def as_candidate(self) -> ReconciliationSuppression:
        return ReconciliationSuppression(
            environment=self.environment,
            sql_source_profile=self.sql_source_profile,
            schedule_source_profile=self.schedule_source_profile,
            source_table=self.source_table,
            target_table=self.target_table,
            raw_status=self.raw_status,
            suppression_reason=self.suppression_reason,
            sql_batch_id=self.sql_batch_id,
            schedule_batch_id=self.schedule_batch_id,
            classifier_version=self.classifier_version,
            observed_at=self.observed_at,
        )


def usable_suppressed_edge_keys(
    result: LineageReconciliationResult,
    rows: Iterable[DWSReconciliationSuppressionRow],
) -> frozenset[tuple[str, str]]:
    """Return only active, current-snapshot rows safe for presentation hiding.

    A row with another scope, another batch pair, another classifier version or
    another raw/reason value is ignored.  A caller that cannot read the audit
    table at all must catch that read error and return no keys instead.
    """

    if not isinstance(result, LineageReconciliationResult):
        raise TypeError("result must be a LineageReconciliationResult")
    keys: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, DWSReconciliationSuppressionRow):
            raise TypeError("rows must contain DWSReconciliationSuppressionRow values")
        if not row.is_active:
            continue
        if (
            row.environment != result.environment
            or row.sql_source_profile != result.sql_source_profile
            or row.schedule_source_profile != result.schedule_source_profile
            or row.sql_batch_id != result.sql_batch_id
            or row.schedule_batch_id != result.schedule_batch_id
            or row.classifier_version != SUPPRESSION_CLASSIFIER_VERSION
            or row.raw_status is not ReconciliationStatus.SQL_ONLY
            or row.suppression_reason
            != ReconciliationSuppressionReason.NO_INTERNAL_PRODUCER
        ):
            continue
        keys.add(row.edge_identity)
    return frozenset(keys)


INSERT_SUPPRESSION_SQL = f"""
    INSERT INTO dwp.{SUPPRESSION_TABLE_NAME}(
        row_key, suppression_key, environment, sql_source_profile,
        schedule_source_profile, source_table, target_table, raw_status,
        suppression_reason, sql_batch_id, schedule_batch_id, classifier_version,
        observed_at, first_seen_at, last_seen_at, is_active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL}, ?, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL})
"""
UPDATE_SUPPRESSION_SQL = f"""
    UPDATE dwp.{SUPPRESSION_TABLE_NAME}
    SET suppression_key = ?, environment = ?, sql_source_profile = ?,
        schedule_source_profile = ?, source_table = ?, target_table = ?,
        raw_status = ?, suppression_reason = ?, sql_batch_id = ?,
        schedule_batch_id = ?, classifier_version = ?,
        observed_at = {TIMESTAMPTZ_PARAM_SQL},
        first_seen_at = {TIMESTAMPTZ_PARAM_SQL},
        last_seen_at = {TIMESTAMPTZ_PARAM_SQL}, is_active = ?,
        created_at = {TIMESTAMPTZ_PARAM_SQL},
        updated_at = {TIMESTAMPTZ_PARAM_SQL}
    WHERE row_key = ?
"""
SELECT_SUPPRESSION_SQL = f"""
    SELECT row_key, suppression_key, environment, sql_source_profile,
           schedule_source_profile, source_table, target_table, raw_status,
           suppression_reason, sql_batch_id, schedule_batch_id,
           classifier_version,
           {dws_timestamp_projection("observed_at")},
           {dws_timestamp_projection("first_seen_at")},
           {dws_timestamp_projection("last_seen_at")}, is_active,
           {dws_timestamp_projection("created_at")},
           {dws_timestamp_projection("updated_at")}
    FROM dwp.{SUPPRESSION_TABLE_NAME}
"""
RETIRE_SCOPE_SQL = f"""
    UPDATE dwp.{SUPPRESSION_TABLE_NAME}
    SET is_active = FALSE, updated_at = {TIMESTAMPTZ_PARAM_SQL}
    WHERE is_active = TRUE
      AND environment = ?
      AND sql_source_profile = ?
      AND schedule_source_profile = ?
"""
ACTIVATE_SUPPRESSION_SQL = f"""
    UPDATE dwp.{SUPPRESSION_TABLE_NAME}
    SET is_active = TRUE
    WHERE row_key = ?
      AND environment = ?
      AND sql_source_profile = ?
      AND schedule_source_profile = ?
"""


class DWSReconciliationSuppressionStore:
    """Atomic, scope-local repository for suppression audit observations."""

    backend_name = "dws-reconciliation-suppression"

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
    def _scope_values(
        environment: str, sql_source_profile: str, schedule_source_profile: str
    ) -> tuple[str, str, str]:
        return (
            _required_text(environment, "environment"),
            _required_text(sql_source_profile, "sql_source_profile"),
            _required_text(schedule_source_profile, "schedule_source_profile"),
        )

    def _fetch_rows(
        self,
        connection: Any,
        *,
        environment: str | None = None,
        sql_source_profile: str | None = None,
        schedule_source_profile: str | None = None,
        sql_batch_id: str | None = None,
        schedule_batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSReconciliationSuppressionRow, ...]:
        conditions: list[str] = []
        params: list[object] = []
        filters = (
            ("environment", environment),
            ("sql_source_profile", sql_source_profile),
            ("schedule_source_profile", schedule_source_profile),
            ("sql_batch_id", sql_batch_id),
            ("schedule_batch_id", schedule_batch_id),
        )
        for field_name, value in filters:
            if value is not None:
                conditions.append(f"{field_name} = ?")
                params.append(_required_text(value, field_name))
        if active_only:
            conditions.append("is_active = TRUE")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        with self._cursor_scope(connection) as cursor:
            self._execute(
                cursor,
                SELECT_SUPPRESSION_SQL
                + where
                + " ORDER BY environment, sql_source_profile, "
                "schedule_source_profile, target_table, source_table, row_key",
                params,
            )
            raw_rows = cursor.fetchall()
        return tuple(_stored_suppression_row(row) for row in raw_rows)

    @staticmethod
    def _row_values(row: DWSReconciliationSuppressionRow) -> tuple[object, ...]:
        return (
            row.row_key,
            row.suppression_key,
            row.environment,
            row.sql_source_profile,
            row.schedule_source_profile,
            row.source_table,
            row.target_table,
            _status_value(row.raw_status),
            _reason_value(row.suppression_reason),
            row.sql_batch_id,
            row.schedule_batch_id,
            row.classifier_version,
            dws_timestamp_param(row.observed_at, "observed_at"),
            dws_timestamp_param(row.first_seen_at, "first_seen_at"),
            dws_timestamp_param(row.last_seen_at, "last_seen_at"),
            row.is_active,
            dws_timestamp_param(row.created_at, "created_at"),
            dws_timestamp_param(row.updated_at, "updated_at"),
        )

    @staticmethod
    def _update_values(row: DWSReconciliationSuppressionRow) -> tuple[object, ...]:
        values = DWSReconciliationSuppressionStore._row_values(row)
        # UPDATE excludes row_key, which is the lookup identity.
        return (*values[1:], values[0])

    def _prepare_rows(
        self,
        suppressions: Iterable[ReconciliationSuppression],
        *,
        scope: tuple[str, str, str],
        observed_at: datetime,
        existing_rows: tuple[DWSReconciliationSuppressionRow, ...],
    ) -> tuple[DWSReconciliationSuppressionRow, ...]:
        _validate_timestamp(observed_at, "observed_at")
        candidates = tuple(suppressions)
        if any(not isinstance(item, ReconciliationSuppression) for item in candidates):
            raise TypeError(
                "suppressions must contain ReconciliationSuppression values"
            )
        seen_keys: set[str] = set()
        existing_by_row_key: dict[str, DWSReconciliationSuppressionRow] = {}
        by_suppression_key: dict[str, list[DWSReconciliationSuppressionRow]] = {}
        for row in existing_rows:
            if row.row_key in existing_by_row_key:
                raise ValueError("suppression history contains duplicate row_key")
            existing_by_row_key[row.row_key] = row
            by_suppression_key.setdefault(row.suppression_key, []).append(row)

        prepared: list[DWSReconciliationSuppressionRow] = []
        for candidate in sorted(
            candidates, key=lambda item: (item.target_table, item.source_table)
        ):
            if (
                candidate.environment,
                candidate.sql_source_profile,
                candidate.schedule_source_profile,
            ) != scope:
                raise ValueError("suppression candidate is outside the requested scope")
            if candidate.classifier_version != SUPPRESSION_CLASSIFIER_VERSION:
                raise ValueError("unsupported suppression classifier version")
            if candidate.suppression_key in seen_keys:
                raise ValueError("suppression candidates contain duplicate identity")
            seen_keys.add(candidate.suppression_key)
            history = by_suppression_key.get(candidate.suppression_key, [])
            first_seen = min(
                (row.first_seen_at for row in history),
                default=observed_at,
            )
            created_at = min(
                (row.created_at for row in history),
                default=observed_at,
            )
            effective = replace(candidate, observed_at=observed_at)
            row = DWSReconciliationSuppressionRow(
                row_key=effective.row_key,
                suppression_key=effective.suppression_key,
                environment=effective.environment,
                sql_source_profile=effective.sql_source_profile,
                schedule_source_profile=effective.schedule_source_profile,
                source_table=effective.source_table,
                target_table=effective.target_table,
                raw_status=effective.raw_status,
                suppression_reason=effective.suppression_reason,
                sql_batch_id=effective.sql_batch_id,
                schedule_batch_id=effective.schedule_batch_id,
                classifier_version=effective.classifier_version,
                observed_at=observed_at,
                first_seen_at=first_seen,
                last_seen_at=observed_at,
                is_active=False,
                created_at=created_at,
                updated_at=observed_at,
            )
            old = existing_by_row_key.get(row.row_key)
            if old is not None and old.suppression_key != row.suppression_key:
                raise ValueError("suppression row_key collides with another identity")
            prepared.append(row)
        return tuple(prepared)

    def publish(
        self,
        suppressions: Iterable[ReconciliationSuppression],
        *,
        environment: str,
        sql_source_profile: str,
        schedule_source_profile: str,
        observed_at: datetime,
    ) -> "DWSSuppressionPublishResult":
        """Publish one scope's candidates and retire stale active rows atomically."""

        scope = self._scope_values(
            environment, sql_source_profile, schedule_source_profile
        )
        with self._connection_scope() as connection:
            restore_autocommit = _begin_transaction(connection)
            try:
                existing_rows = self._fetch_rows(
                    connection,
                    environment=scope[0],
                    sql_source_profile=scope[1],
                    schedule_source_profile=scope[2],
                )
                previous_active = tuple(row for row in existing_rows if row.is_active)
                prepared = self._prepare_rows(
                    suppressions,
                    scope=scope,
                    observed_at=observed_at,
                    existing_rows=existing_rows,
                )
                existing_by_row_key = {row.row_key for row in existing_rows}
                with self._cursor_scope(connection) as cursor:
                    for row in prepared:
                        if row.row_key in existing_by_row_key:
                            self._execute(
                                cursor,
                                UPDATE_SUPPRESSION_SQL,
                                self._update_values(row),
                            )
                        else:
                            self._execute(
                                cursor,
                                INSERT_SUPPRESSION_SQL,
                                self._row_values(row),
                            )
                    self._execute(
                        cursor,
                        RETIRE_SCOPE_SQL,
                        (
                            dws_timestamp_param(observed_at, "updated_at"),
                            scope[0],
                            scope[1],
                            scope[2],
                        ),
                    )
                    for row in prepared:
                        self._execute(
                            cursor,
                            ACTIVATE_SUPPRESSION_SQL,
                            (row.row_key, scope[0], scope[1], scope[2]),
                        )

                active_after = self._fetch_rows(
                    connection,
                    environment=scope[0],
                    sql_source_profile=scope[1],
                    schedule_source_profile=scope[2],
                    active_only=True,
                )
                if {row.row_key for row in active_after} != {
                    row.row_key for row in prepared
                }:
                    raise ValueError("active suppression lifecycle is inconsistent")
                _commit(connection)
            except Exception:
                _rollback(connection)
                raise
            finally:
                restore_autocommit()

        previous_keys = {row.suppression_key for row in previous_active}
        active_keys = {row.suppression_key for row in prepared}
        return DWSSuppressionPublishResult(
            environment=scope[0],
            sql_source_profile=scope[1],
            schedule_source_profile=scope[2],
            suppression_count=len(prepared),
            retired_count=len(previous_keys - active_keys),
            previous_active_count=len(previous_active),
        )

    publish_batch = publish

    def read_rows(
        self,
        *,
        environment: str | None = None,
        sql_source_profile: str | None = None,
        schedule_source_profile: str | None = None,
        sql_batch_id: str | None = None,
        schedule_batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSReconciliationSuppressionRow, ...]:
        with self._connection_scope() as connection:
            return self._fetch_rows(
                connection,
                environment=environment,
                sql_source_profile=sql_source_profile,
                schedule_source_profile=schedule_source_profile,
                sql_batch_id=sql_batch_id,
                schedule_batch_id=schedule_batch_id,
                active_only=active_only,
            )

    read_suppressions = read_rows


@dataclass(frozen=True, slots=True)
class DWSSuppressionPublishResult:
    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    suppression_count: int
    retired_count: int
    previous_active_count: int


def _parse_optional_timestamp(value: object, field_name: str) -> datetime:
    if value is None:
        raise ValueError(f"{field_name} must not be NULL")
    return parse_dws_timestamp(value, field_name)


def _stored_suppression_row(raw: object) -> DWSReconciliationSuppressionRow:
    try:
        values = tuple(raw)  # type: ignore[arg-type]
        if len(values) != 18:
            raise ValueError("stored suppression row has an unexpected column count")
        return DWSReconciliationSuppressionRow(
            row_key=_key_text(values[0], "row_key"),
            suppression_key=_key_text(values[1], "suppression_key"),
            environment=_required_text(values[2], "environment"),
            sql_source_profile=_required_text(values[3], "sql_source_profile"),
            schedule_source_profile=_required_text(
                values[4], "schedule_source_profile"
            ),
            source_table=normalize_lineage_comparison_table_key(
                _required_text(values[5], "source_table")
            ),
            target_table=normalize_lineage_comparison_table_key(
                _required_text(values[6], "target_table")
            ),
            raw_status=_required_text(values[7], "raw_status", max_length=32),
            suppression_reason=_required_text(
                values[8], "suppression_reason", max_length=64
            ),
            sql_batch_id=_required_text(values[9], "sql_batch_id"),
            schedule_batch_id=_required_text(values[10], "schedule_batch_id"),
            classifier_version=_required_text(
                values[11], "classifier_version", max_length=128
            ),
            observed_at=_parse_optional_timestamp(values[12], "observed_at"),
            first_seen_at=_parse_optional_timestamp(values[13], "first_seen_at"),
            last_seen_at=_parse_optional_timestamp(values[14], "last_seen_at"),
            is_active=_stored_bool(values[15], "is_active"),
            created_at=_parse_optional_timestamp(values[16], "created_at"),
            updated_at=_parse_optional_timestamp(values[17], "updated_at"),
        )
    except (IndexError, TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(
            "stored suppression row has"
        ):
            raise
        raise ValueError("stored DWS suppression row is invalid") from exc


def _stored_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"TRUE", "T", "1"}:
            return True
        if normalized in {"FALSE", "F", "0"}:
            return False
    raise ValueError(f"{field_name} is not a valid boolean")


__all__ = [
    "ACTIVATE_SUPPRESSION_SQL",
    "DWSReconciliationSuppressionRow",
    "DWSReconciliationSuppressionStore",
    "DWSSuppressionPublishResult",
    "INSERT_SUPPRESSION_SQL",
    "ReconciliationSuppression",
    "ReconciliationSuppressionError",
    "ReconciliationSuppressionReason",
    "RETIRE_SCOPE_SQL",
    "SELECT_SUPPRESSION_SQL",
    "SUPPRESSION_CLASSIFIER_VERSION",
    "SUPPRESSION_TABLE_NAME",
    "UPDATE_SUPPRESSION_SQL",
    "classify_reconciliation_suppressions",
    "compute_reconciliation_suppression_key",
    "compute_reconciliation_suppression_row_key",
    "usable_suppressed_edge_keys",
]
