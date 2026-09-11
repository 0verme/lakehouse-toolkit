"""SQL business lineage versus configured schedule reconciliation.

This module consumes already-materialized facts only.  SQL facts come from the
DWS ``lineage_business_edge`` projection and schedule facts come from the DWS
``lineage_schedule_edge`` projection; neither parser, physical DAG, MySQL
provider, nor TMP collapse is part of this comparison boundary.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from shared.lineage.domain import (
    LineageEdge,
    canonicalize_dataset_name,
    decode_code,
    is_business_asset,
    normalize_asset_name,
    normalize_legacy_program_namespace,
)
from shared.lineage.schedule import ScheduleLineageEdge

SQL_ACTIVE_SNAPSHOT_NOT_FOUND = "SQL_ACTIVE_SNAPSHOT_NOT_FOUND"
SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND = "SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND"


class ReconciliationStatus(str, Enum):
    """Row-level comparison result for one business table edge."""

    MATCH = "MATCH"
    SQL_ONLY = "SQL_ONLY"
    SCHEDULE_ONLY = "SCHEDULE_ONLY"


class TargetSummaryStatus(str, Enum):
    """Target-level result derived from row-level reconciliation statuses."""

    CONSISTENT = "CONSISTENT"
    DIFFERENT = "DIFFERENT"


# The longer name is useful to callers that have more than one target status.
ReconciliationTargetStatus = TargetSummaryStatus


class LineageReconciliationError(RuntimeError):
    """Base error for a reconciliation request that cannot be verified."""

    code = "LINEAGE_RECONCILIATION_FAILED"


class ActiveSnapshotNotFoundError(LineageReconciliationError):
    """Raised when one side has no verifiable current active snapshot."""

    def __init__(self, code: str) -> None:
        if code not in {
            SQL_ACTIVE_SNAPSHOT_NOT_FOUND,
            SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
        }:
            raise ValueError("unsupported active snapshot error code")
        self.code = code
        super().__init__(code)


def normalize_lineage_comparison_table_key(value: object) -> str:
    """Normalize a table key only at the SQL-vs-schedule comparison boundary.

    The explicit legacy namespace registry is reused, while the physical
    ``DatasetIdentity`` and stored DWS facts remain untouched.  Unknown
    namespaces are canonicalized only (never guessed or matched by basename).
    """

    canonical = canonicalize_dataset_name(normalize_asset_name(decode_code(value)))
    if canonical is None:
        raise ValueError("comparison table must be a qualified schema.table")
    mapped = normalize_legacy_program_namespace(canonical)
    return canonical if mapped is None else mapped


@dataclass(frozen=True, slots=True)
class SQLBusinessLineageSnapshot:
    """One verified active SQL business-lineage snapshot."""

    batch_id: str
    edges: tuple[LineageEdge, ...]
    observed_at: datetime | None = None
    snapshot_scope: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        _required_text(self.batch_id, "sql batch_id")
        edges = tuple(self.edges)
        if any(not isinstance(edge, LineageEdge) for edge in edges):
            raise TypeError("SQL snapshot edges must contain LineageEdge values")
        object.__setattr__(self, "edges", edges)
        if self.observed_at is not None and not isinstance(self.observed_at, datetime):
            raise TypeError("sql observed_at must be a datetime or None")
        object.__setattr__(
            self, "snapshot_scope", _normalize_snapshot_scopes(self.snapshot_scope)
        )


@dataclass(frozen=True, slots=True)
class ScheduleLineageSnapshot:
    """One verified active configured-schedule snapshot for a scope."""

    batch_id: str
    edges: tuple[ScheduleLineageEdge, ...]
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        _required_text(self.batch_id, "schedule batch_id")
        edges = tuple(self.edges)
        if any(not isinstance(edge, ScheduleLineageEdge) for edge in edges):
            raise TypeError(
                "schedule snapshot edges must contain ScheduleLineageEdge values"
            )
        object.__setattr__(self, "edges", edges)
        if self.observed_at is not None and not isinstance(self.observed_at, datetime):
            raise TypeError("schedule observed_at must be a datetime or None")


@dataclass(frozen=True, slots=True)
class LineageReconciliationRow:
    """One deduplicated business ``source_table -> target_table`` comparison."""

    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    source_table: str
    target_table: str
    sql_present: bool
    schedule_present: bool
    status: ReconciliationStatus
    sql_fact_count: int
    schedule_fact_count: int
    sql_program_count: int = 0
    schedule_process_count: int = 0

    def __post_init__(self) -> None:
        _required_text(self.environment, "environment")
        _required_text(self.sql_source_profile, "sql_source_profile")
        _required_text(self.schedule_source_profile, "schedule_source_profile")
        source = normalize_lineage_comparison_table_key(self.source_table)
        target = normalize_lineage_comparison_table_key(self.target_table)
        object.__setattr__(self, "environment", self.environment.strip())
        object.__setattr__(self, "sql_source_profile", self.sql_source_profile.strip())
        object.__setattr__(
            self,
            "schedule_source_profile",
            self.schedule_source_profile.strip(),
        )
        object.__setattr__(self, "source_table", source)
        object.__setattr__(self, "target_table", target)
        if not isinstance(self.sql_present, bool) or not isinstance(
            self.schedule_present, bool
        ):
            raise TypeError("presence flags must be booleans")
        status = _resolve_status(self.sql_present, self.schedule_present)
        if not isinstance(self.status, ReconciliationStatus):
            try:
                object.__setattr__(self, "status", ReconciliationStatus(self.status))
            except (TypeError, ValueError) as exc:
                raise ValueError("status is not a valid reconciliation status") from exc
        if self.status is not status:
            raise ValueError("status does not match presence flags")
        for field_name in (
            "sql_fact_count",
            "schedule_fact_count",
            "sql_program_count",
            "schedule_process_count",
        ):
            _non_negative_int(getattr(self, field_name), field_name)
        if self.sql_present != (self.sql_fact_count > 0):
            raise ValueError("sql_present does not match sql_fact_count")
        if self.schedule_present != (self.schedule_fact_count > 0):
            raise ValueError("schedule_present does not match schedule_fact_count")

    @property
    def source_profile(self) -> str | None:
        """Return the legacy profile only when both sides use the same value."""

        return (
            self.sql_source_profile
            if self.sql_source_profile == self.schedule_source_profile
            else None
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "sql_source_profile": self.sql_source_profile,
            "schedule_source_profile": self.schedule_source_profile,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "sql_present": self.sql_present,
            "schedule_present": self.schedule_present,
            "status": self.status.value,
            "sql_fact_count": self.sql_fact_count,
            "schedule_fact_count": self.schedule_fact_count,
            "sql_program_count": self.sql_program_count,
            "schedule_process_count": self.schedule_process_count,
        }


@dataclass(frozen=True, slots=True)
class LineageReconciliationTargetSummary:
    """Target-centric projection of row-level reconciliation results."""

    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    target_table: str
    sql_source_count: int
    schedule_source_count: int
    match_count: int
    sql_only_count: int
    schedule_only_count: int
    status: TargetSummaryStatus

    def __post_init__(self) -> None:
        _required_text(self.environment, "environment")
        _required_text(self.sql_source_profile, "sql_source_profile")
        _required_text(self.schedule_source_profile, "schedule_source_profile")
        object.__setattr__(self, "environment", self.environment.strip())
        object.__setattr__(self, "sql_source_profile", self.sql_source_profile.strip())
        object.__setattr__(
            self,
            "schedule_source_profile",
            self.schedule_source_profile.strip(),
        )
        object.__setattr__(
            self,
            "target_table",
            normalize_lineage_comparison_table_key(self.target_table),
        )
        for field_name in (
            "sql_source_count",
            "schedule_source_count",
            "match_count",
            "sql_only_count",
            "schedule_only_count",
        ):
            _non_negative_int(getattr(self, field_name), field_name)
        expected = (
            TargetSummaryStatus.CONSISTENT
            if self.sql_only_count == 0 and self.schedule_only_count == 0
            else TargetSummaryStatus.DIFFERENT
        )
        if not isinstance(self.status, TargetSummaryStatus):
            try:
                object.__setattr__(self, "status", TargetSummaryStatus(self.status))
            except (TypeError, ValueError) as exc:
                raise ValueError("target summary status is invalid") from exc
        if self.status is not expected:
            raise ValueError("target summary status does not match row counts")

    @property
    def source_profile(self) -> str | None:
        """Return the legacy profile only when both sides use the same value."""

        return (
            self.sql_source_profile
            if self.sql_source_profile == self.schedule_source_profile
            else None
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "sql_source_profile": self.sql_source_profile,
            "schedule_source_profile": self.schedule_source_profile,
            "target_table": self.target_table,
            "sql_source_count": self.sql_source_count,
            "schedule_source_count": self.schedule_source_count,
            "match_count": self.match_count,
            "sql_only_count": self.sql_only_count,
            "schedule_only_count": self.schedule_only_count,
            "status": self.status.value,
        }


@dataclass(frozen=True, slots=True)
class LineageReconciliationResult:
    """Deterministic row and target summary result for two active snapshots."""

    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    rows: tuple[LineageReconciliationRow, ...]
    target_summaries: tuple[LineageReconciliationTargetSummary, ...]
    sql_batch_id: str
    schedule_batch_id: str
    sql_observed_at: datetime | None = None
    schedule_observed_at: datetime | None = None
    sql_edge_count: int = 0
    schedule_edge_count: int = 0

    def __post_init__(self) -> None:
        _required_text(self.environment, "environment")
        _required_text(self.sql_source_profile, "sql_source_profile")
        _required_text(self.schedule_source_profile, "schedule_source_profile")
        _required_text(self.sql_batch_id, "sql_batch_id")
        _required_text(self.schedule_batch_id, "schedule_batch_id")
        object.__setattr__(self, "environment", self.environment.strip())
        object.__setattr__(self, "sql_source_profile", self.sql_source_profile.strip())
        object.__setattr__(
            self,
            "schedule_source_profile",
            self.schedule_source_profile.strip(),
        )
        rows = tuple(self.rows)
        summaries = tuple(self.target_summaries)
        if any(not isinstance(row, LineageReconciliationRow) for row in rows):
            raise TypeError("rows must contain LineageReconciliationRow values")
        if any(
            not isinstance(summary, LineageReconciliationTargetSummary)
            for summary in summaries
        ):
            raise TypeError(
                "target_summaries must contain LineageReconciliationTargetSummary values"
            )
        row_keys = {(row.source_table, row.target_table) for row in rows}
        if len(row_keys) != len(rows):
            raise ValueError("reconciliation rows contain duplicate table edges")
        summary_targets = {summary.target_table for summary in summaries}
        if len(summary_targets) != len(summaries):
            raise ValueError("target summaries contain duplicate target tables")
        if any(
            row.environment != self.environment
            or row.sql_source_profile != self.sql_source_profile
            or row.schedule_source_profile != self.schedule_source_profile
            for row in rows
        ):
            raise ValueError("reconciliation row is outside the requested scope")
        if any(
            summary.environment != self.environment
            or summary.sql_source_profile != self.sql_source_profile
            or summary.schedule_source_profile != self.schedule_source_profile
            for summary in summaries
        ):
            raise ValueError("target summary is outside the requested scope")
        object.__setattr__(
            self,
            "rows",
            tuple(sorted(rows, key=_row_sort_key)),
        )
        object.__setattr__(
            self,
            "target_summaries",
            tuple(sorted(summaries, key=lambda item: item.target_table)),
        )
        if self.sql_observed_at is not None and not isinstance(
            self.sql_observed_at, datetime
        ):
            raise TypeError("sql_observed_at must be a datetime or None")
        if self.schedule_observed_at is not None and not isinstance(
            self.schedule_observed_at, datetime
        ):
            raise TypeError("schedule_observed_at must be a datetime or None")
        _non_negative_int(self.sql_edge_count, "sql_edge_count")
        _non_negative_int(self.schedule_edge_count, "schedule_edge_count")
        if self.sql_edge_count != sum(row.sql_fact_count for row in rows):
            raise ValueError("sql_edge_count does not match reconciliation rows")
        if self.schedule_edge_count != sum(row.schedule_fact_count for row in rows):
            raise ValueError("schedule_edge_count does not match reconciliation rows")

    @property
    def source_profile(self) -> str | None:
        """Return the legacy profile only when both sides use the same value."""

        return (
            self.sql_source_profile
            if self.sql_source_profile == self.schedule_source_profile
            else None
        )

    @property
    def match_count(self) -> int:
        return sum(row.status is ReconciliationStatus.MATCH for row in self.rows)

    @property
    def sql_only_count(self) -> int:
        return sum(row.status is ReconciliationStatus.SQL_ONLY for row in self.rows)

    @property
    def schedule_only_count(self) -> int:
        return sum(
            row.status is ReconciliationStatus.SCHEDULE_ONLY for row in self.rows
        )

    @property
    def consistent_target_count(self) -> int:
        return sum(
            summary.status is TargetSummaryStatus.CONSISTENT
            for summary in self.target_summaries
        )

    @property
    def different_target_count(self) -> int:
        return sum(
            summary.status is TargetSummaryStatus.DIFFERENT
            for summary in self.target_summaries
        )

    def aggregate_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "sql_source_profile": self.sql_source_profile,
            "schedule_source_profile": self.schedule_source_profile,
            "sql_edges": self.sql_edge_count,
            "schedule_edges": self.schedule_edge_count,
            "reconciliation_rows": len(self.rows),
            "match": self.match_count,
            "sql_only": self.sql_only_count,
            "schedule_only": self.schedule_only_count,
            "targets": len(self.target_summaries),
            "consistent_targets": self.consistent_target_count,
            "different_targets": self.different_target_count,
            "sql_batch_id": self.sql_batch_id,
            "schedule_batch_id": self.schedule_batch_id,
            "sql_observed_at": _timestamp_value(self.sql_observed_at),
            "schedule_observed_at": _timestamp_value(self.schedule_observed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self.aggregate_dict(),
            "rows": [row.to_dict() for row in self.rows],
            "target_summaries": [
                summary.to_dict() for summary in self.target_summaries
            ],
        }


class SQLBusinessLineageReader(Protocol):
    """Minimal existing DWS SQL business reader contract."""

    def get_active_batch_id(self) -> str | None: ...

    def get_batch_metadata(self, batch_id: str) -> Any | None: ...

    def read_edges(
        self, *, batch_id: str | None = None, active_only: bool = False
    ) -> Iterable[LineageEdge]: ...


class ScheduleLineageReader(Protocol):
    """Minimal existing DWS schedule reader contract."""

    def get_active_batch_id(self) -> str | None: ...

    def read_rows(
        self, *, batch_id: str | None = None, active_only: bool = False
    ) -> Iterable[Any]: ...


def read_active_sql_business_snapshot(
    reader: SQLBusinessLineageReader,
    *,
    environment: str,
    source_profile: str,
) -> SQLBusinessLineageSnapshot:
    """Read SQL facts through the active ``lineage_batch`` reader contract."""

    scope = _validate_scope(environment, source_profile)
    active_batch_id = reader.get_active_batch_id()
    if not isinstance(active_batch_id, str) or not active_batch_id.strip():
        raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)
    active_batch_id = active_batch_id.strip()
    metadata = reader.get_batch_metadata(active_batch_id)
    if metadata is None or not _is_true(getattr(metadata, "is_active", None)):
        raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)
    observed_at = getattr(metadata, "observed_at", None)
    if not isinstance(observed_at, datetime):
        raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)

    snapshot_scope: tuple[tuple[str, str], ...] = ()
    scope_reader = getattr(reader, "get_active_snapshot_scope", None)
    if callable(scope_reader):
        try:
            raw_scope = scope_reader()
            if not isinstance(raw_scope, (tuple, list, set, frozenset)):
                raise TypeError("snapshot scope reader must return an iterable")
            snapshot_scope = _normalize_snapshot_scopes(raw_scope)
        except (TypeError, ValueError) as exc:
            raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND) from exc
        if scope not in snapshot_scope:
            raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)

    edges = tuple(reader.read_edges(batch_id=active_batch_id, active_only=True))
    if any(not isinstance(edge, LineageEdge) for edge in edges):
        raise TypeError("SQL active reader must return LineageEdge values")
    if any(
        edge.batch_id is not None and edge.batch_id != active_batch_id for edge in edges
    ):
        raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)
    scoped_edges = tuple(
        edge
        for edge in edges
        if edge.environment == scope[0] and edge.source_profile == scope[1]
    )
    return SQLBusinessLineageSnapshot(
        batch_id=active_batch_id,
        edges=scoped_edges,
        observed_at=observed_at,
        snapshot_scope=snapshot_scope,
    )


def read_active_schedule_snapshot(
    reader: ScheduleLineageReader,
    *,
    environment: str,
    source_profile: str,
) -> ScheduleLineageSnapshot:
    """Read schedule rows from the current DWS active fact snapshot.

    ``lineage_schedule_edge`` has no independent control table.  Consequently
    no active rows for the requested scope are treated as unverifiable rather
    than as a valid empty schedule snapshot.
    """

    scope = _validate_scope(environment, source_profile)
    active_batch_id = reader.get_active_batch_id()
    if not isinstance(active_batch_id, str) or not active_batch_id.strip():
        raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
    active_batch_id = active_batch_id.strip()
    rows = tuple(reader.read_rows(batch_id=active_batch_id, active_only=True))
    if not rows:
        raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)

    scoped_rows: list[Any] = []
    observed_values: set[datetime] = set()
    for row in rows:
        if getattr(row, "batch_id", None) != active_batch_id:
            raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
        if not _is_true(getattr(row, "is_active", None)):
            raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
        edge = getattr(row, "edge", None)
        if not isinstance(edge, ScheduleLineageEdge):
            raise TypeError(
                "schedule active reader rows must expose ScheduleLineageEdge"
            )
        if edge.scope == scope:
            scoped_rows.append(row)
            observed_at = getattr(row, "observed_at", None)
            if not isinstance(observed_at, datetime):
                raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
            observed_values.add(observed_at)

    if not scoped_rows or len(observed_values) != 1:
        raise ActiveSnapshotNotFoundError(SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND)
    observed_at = next(iter(observed_values))
    edges = tuple(row.edge for row in scoped_rows)
    return ScheduleLineageSnapshot(
        batch_id=active_batch_id,
        edges=edges,
        observed_at=observed_at,
    )


def reconcile_active_dws_lineage(
    sql_reader: SQLBusinessLineageReader,
    schedule_reader: ScheduleLineageReader,
    *,
    environment: str,
    sql_source_profile: str | None = None,
    schedule_source_profile: str | None = None,
    source_profile: str | None = None,
    target_table: str | None = None,
) -> LineageReconciliationResult:
    """Reconcile two existing DWS active readers without touching source systems."""

    sql_profile, schedule_profile = _resolve_source_profiles(
        source_profile=source_profile,
        sql_source_profile=sql_source_profile,
        schedule_source_profile=schedule_source_profile,
    )
    sql_snapshot = read_active_sql_business_snapshot(
        sql_reader,
        environment=environment,
        source_profile=sql_profile,
    )
    schedule_snapshot = read_active_schedule_snapshot(
        schedule_reader,
        environment=environment,
        source_profile=schedule_profile,
    )
    return reconcile_lineage_snapshots(
        sql_snapshot,
        schedule_snapshot,
        environment=environment,
        sql_source_profile=sql_profile,
        schedule_source_profile=schedule_profile,
        target_table=target_table,
    )


def reconcile_lineage_snapshots(
    sql_snapshot: SQLBusinessLineageSnapshot,
    schedule_snapshot: ScheduleLineageSnapshot,
    *,
    environment: str,
    sql_source_profile: str | None = None,
    schedule_source_profile: str | None = None,
    source_profile: str | None = None,
    target_table: str | None = None,
) -> LineageReconciliationResult:
    """Perform deterministic set reconciliation for one strict environment."""

    sql_profile, schedule_profile = _resolve_source_profiles(
        source_profile=source_profile,
        sql_source_profile=sql_source_profile,
        schedule_source_profile=schedule_source_profile,
    )
    sql_scope = _validate_scope(environment, sql_profile)
    schedule_scope = _validate_scope(environment, schedule_profile)
    if not isinstance(sql_snapshot, SQLBusinessLineageSnapshot):
        raise TypeError("sql_snapshot must be SQLBusinessLineageSnapshot")
    if not isinstance(schedule_snapshot, ScheduleLineageSnapshot):
        raise TypeError("schedule_snapshot must be ScheduleLineageSnapshot")
    if sql_snapshot.snapshot_scope and sql_scope not in sql_snapshot.snapshot_scope:
        raise ActiveSnapshotNotFoundError(SQL_ACTIVE_SNAPSHOT_NOT_FOUND)
    resolved_target = (
        None
        if target_table is None
        else normalize_lineage_comparison_table_key(target_table)
    )

    sql_values: dict[tuple[str, str, str], _ComparisonAccumulator] = {}
    schedule_values: dict[tuple[str, str, str], _ComparisonAccumulator] = {}
    for edge in sql_snapshot.edges:
        if not isinstance(edge, LineageEdge):
            raise TypeError("SQL snapshot edges must contain LineageEdge values")
        if edge.environment != sql_scope[0] or edge.source_profile != sql_scope[1]:
            continue
        key = _comparison_key(
            edge.environment,
            edge.source_table,
            edge.target_table,
        )
        if key is None or (resolved_target is not None and key[1] != resolved_target):
            continue
        aggregate = sql_values.setdefault(key, _ComparisonAccumulator())
        aggregate.sql_fact_count += 1
        if edge.program_name:
            aggregate.sql_programs.add(edge.program_name)

    for edge in schedule_snapshot.edges:
        if not isinstance(edge, ScheduleLineageEdge):
            raise TypeError(
                "schedule snapshot edges must contain ScheduleLineageEdge values"
            )
        if (
            edge.environment != schedule_scope[0]
            or edge.source_profile != schedule_scope[1]
        ):
            continue
        key = _comparison_key(
            edge.environment,
            edge.source_table,
            edge.target_table,
        )
        if key is None or (resolved_target is not None and key[1] != resolved_target):
            continue
        aggregate = schedule_values.setdefault(key, _ComparisonAccumulator())
        aggregate.schedule_fact_count += 1
        aggregate.schedule_processes.add(edge.process_name)

    all_keys = sorted(set(sql_values) | set(schedule_values), key=_key_sort_key)
    rows: list[LineageReconciliationRow] = []
    for key in all_keys:
        sql_aggregate = sql_values.get(key, _ComparisonAccumulator())
        schedule_aggregate = schedule_values.get(key, _ComparisonAccumulator())
        sql_present = sql_aggregate.sql_fact_count > 0
        schedule_present = schedule_aggregate.schedule_fact_count > 0
        rows.append(
            LineageReconciliationRow(
                environment=key[0],
                sql_source_profile=sql_profile,
                schedule_source_profile=schedule_profile,
                source_table=key[2],
                target_table=key[1],
                sql_present=sql_present,
                schedule_present=schedule_present,
                status=_resolve_status(sql_present, schedule_present),
                sql_fact_count=sql_aggregate.sql_fact_count,
                schedule_fact_count=schedule_aggregate.schedule_fact_count,
                sql_program_count=len(sql_aggregate.sql_programs),
                schedule_process_count=len(schedule_aggregate.schedule_processes),
            )
        )

    summaries = _build_target_summaries(
        rows,
        environment=sql_scope[0],
        sql_source_profile=sql_profile,
        schedule_source_profile=schedule_profile,
        target_table=resolved_target,
    )
    return LineageReconciliationResult(
        environment=sql_scope[0],
        sql_source_profile=sql_profile,
        schedule_source_profile=schedule_profile,
        rows=tuple(rows),
        target_summaries=summaries,
        sql_batch_id=sql_snapshot.batch_id,
        schedule_batch_id=schedule_snapshot.batch_id,
        sql_observed_at=sql_snapshot.observed_at,
        schedule_observed_at=schedule_snapshot.observed_at,
        sql_edge_count=sum(row.sql_fact_count for row in rows),
        schedule_edge_count=sum(row.schedule_fact_count for row in rows),
    )


@dataclass(slots=True)
class _ComparisonAccumulator:
    sql_fact_count: int = 0
    schedule_fact_count: int = 0
    sql_programs: set[str] = field(default_factory=set)
    schedule_processes: set[str] = field(default_factory=set)


def _comparison_key(
    environment: str,
    source_table: str,
    target_table: str,
) -> tuple[str, str, str] | None:
    source = normalize_lineage_comparison_table_key(source_table)
    target = normalize_lineage_comparison_table_key(target_table)
    # Business lineage never traverses/reintroduces DLO, DWO, or TMP endpoints.
    # Schedule facts are filtered at this comparison boundary only; ingestion
    # facts and their raw/comparison values remain unchanged in DWS.
    if not is_business_asset(source) or not is_business_asset(target):
        return None
    return (environment, target, source)


def _build_target_summaries(
    rows: Iterable[LineageReconciliationRow],
    *,
    environment: str,
    sql_source_profile: str,
    schedule_source_profile: str,
    target_table: str | None,
) -> tuple[LineageReconciliationTargetSummary, ...]:
    values = tuple(rows)
    target_names = (
        (target_table,)
        if target_table is not None
        else tuple(sorted({row.target_table for row in values}))
    )
    summaries: list[LineageReconciliationTargetSummary] = []
    for target in target_names:
        target_rows = tuple(row for row in values if row.target_table == target)
        sql_sources = {row.source_table for row in target_rows if row.sql_present}
        schedule_sources = {
            row.source_table for row in target_rows if row.schedule_present
        }
        match_count = sum(
            row.status is ReconciliationStatus.MATCH for row in target_rows
        )
        sql_only_count = sum(
            row.status is ReconciliationStatus.SQL_ONLY for row in target_rows
        )
        schedule_only_count = sum(
            row.status is ReconciliationStatus.SCHEDULE_ONLY for row in target_rows
        )
        status = (
            TargetSummaryStatus.CONSISTENT
            if sql_only_count == 0 and schedule_only_count == 0
            else TargetSummaryStatus.DIFFERENT
        )
        summaries.append(
            LineageReconciliationTargetSummary(
                environment=environment,
                sql_source_profile=sql_source_profile,
                schedule_source_profile=schedule_source_profile,
                target_table=target,
                sql_source_count=len(sql_sources),
                schedule_source_count=len(schedule_sources),
                match_count=match_count,
                sql_only_count=sql_only_count,
                schedule_only_count=schedule_only_count,
                status=status,
            )
        )
    return tuple(sorted(summaries, key=lambda item: item.target_table))


def _resolve_status(sql_present: bool, schedule_present: bool) -> ReconciliationStatus:
    if sql_present and schedule_present:
        return ReconciliationStatus.MATCH
    if sql_present:
        return ReconciliationStatus.SQL_ONLY
    return ReconciliationStatus.SCHEDULE_ONLY


def _row_sort_key(row: LineageReconciliationRow) -> tuple[str, ...]:
    return (
        row.environment,
        row.target_table,
        row.source_table,
        row.status.value,
    )


def _key_sort_key(key: tuple[str, str, str]) -> tuple[str, ...]:
    environment, target, source = key
    return (environment, target, source)


def _resolve_source_profiles(
    *,
    source_profile: str | None,
    sql_source_profile: str | None,
    schedule_source_profile: str | None,
) -> tuple[str, str]:
    """Resolve the split profile contract and reject ambiguous shorthand."""

    if source_profile is not None:
        legacy_profile = _required_text(source_profile, "source_profile")
        resolved_sql = legacy_profile
        resolved_schedule = legacy_profile
        if sql_source_profile is not None:
            resolved_sql = _required_text(sql_source_profile, "sql_source_profile")
            if resolved_sql != legacy_profile:
                raise ValueError("source_profile conflicts with sql_source_profile")
        if schedule_source_profile is not None:
            resolved_schedule = _required_text(
                schedule_source_profile, "schedule_source_profile"
            )
            if resolved_schedule != legacy_profile:
                raise ValueError(
                    "source_profile conflicts with schedule_source_profile"
                )
        return resolved_sql, resolved_schedule

    if sql_source_profile is None or schedule_source_profile is None:
        raise ValueError(
            "sql_source_profile and schedule_source_profile must both be provided"
        )
    return (
        _required_text(sql_source_profile, "sql_source_profile"),
        _required_text(schedule_source_profile, "schedule_source_profile"),
    )


def _validate_scope(environment: str, source_profile: str) -> tuple[str, str]:
    _required_text(environment, "environment")
    _required_text(source_profile, "source_profile")
    return (environment.strip(), source_profile.strip())


def _normalize_snapshot_scopes(value: Iterable[object]) -> tuple[tuple[str, str], ...]:
    scopes: set[tuple[str, str]] = set()
    for item in value:
        if isinstance(item, tuple) and len(item) == 2:
            environment, source_profile = item
        elif isinstance(item, list) and len(item) == 2:
            environment, source_profile = item
        else:
            raise TypeError("snapshot scopes must contain two-item values")
        if not isinstance(environment, str) or not isinstance(source_profile, str):
            raise TypeError("snapshot scope values must be strings")
        scopes.add(_validate_scope(environment, source_profile))
    return tuple(sorted(scopes))


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _non_negative_int(value: object, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _is_true(value: object) -> bool:
    return isinstance(value, bool) and value


def _timestamp_value(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


__all__ = [
    "ActiveSnapshotNotFoundError",
    "LineageReconciliationError",
    "LineageReconciliationResult",
    "LineageReconciliationRow",
    "LineageReconciliationTargetSummary",
    "ReconciliationStatus",
    "ReconciliationTargetStatus",
    "SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND",
    "SQL_ACTIVE_SNAPSHOT_NOT_FOUND",
    "SQLBusinessLineageReader",
    "SQLBusinessLineageSnapshot",
    "ScheduleLineageReader",
    "ScheduleLineageSnapshot",
    "TargetSummaryStatus",
    "normalize_lineage_comparison_table_key",
    "read_active_schedule_snapshot",
    "read_active_sql_business_snapshot",
    "reconcile_active_dws_lineage",
    "reconcile_lineage_snapshots",
]
