"""DWS active-lineage reader for the bounded Lineage Query Service.

This module is a read-only adapter.  It consumes materialized active facts from
``dwp.lineage_edge`` or ``dwp.lineage_business_edge`` and never reads source
SQL, calls a provider, or reconstructs lineage.  A request scope resolves one
published active batch and reuses one DWS connection for all BFS neighbor
queries.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from time import perf_counter
from typing import Any

from .domain import canonicalize_dataset_name
from .query import LineageQueryTiming, LineageReadEdge, LineageView

LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND = "LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND"
LINEAGE_ACTIVE_SNAPSHOT_INVALID = "LINEAGE_ACTIVE_SNAPSHOT_INVALID"


class DWSLineageReaderError(RuntimeError):
    """Safe, coded errors raised by the active DWS lineage reader."""

    def __init__(self, code: str, *, reason: str | None = None) -> None:
        self.code = code
        self.reason = reason
        message = code if not reason else f"{code}: {reason}"
        super().__init__(message)


class DWSActiveSnapshotNotFoundError(DWSLineageReaderError):
    """No single published active lineage batch is available."""

    def __init__(self) -> None:
        super().__init__(LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND)


ACTIVE_BATCH_SQL = """
    SELECT batch_id, publish_status
    FROM dwp.lineage_batch
    WHERE is_active = TRUE
"""

_PHYSICAL_EDGE_TABLE = "dwp.lineage_edge"
_BUSINESS_EDGE_TABLE = "dwp.lineage_business_edge"


def _connect_with_profile(profile: str) -> Any:
    from shared.db.gaussdb import connect_with_profile

    return connect_with_profile(profile)


def _neighbor_sql(table_name: str, *, outgoing: bool) -> str:
    table_column = "source_table" if outgoing else "target_table"
    return f"""
        SELECT e.environment, e.source_profile, e.source_table,
               e.target_table, e.batch_id
        FROM {table_name} AS e
        JOIN dwp.lineage_batch AS b
          ON b.batch_id = e.batch_id
         AND b.is_active = TRUE
         AND b.publish_status = 'PUBLISHED'
        WHERE e.is_active = TRUE
          AND e.batch_id = ?
          AND e.environment = ?
          AND e.{table_column} = ?
    """


def _contains_node_sql(table_name: str) -> str:
    return f"""
        SELECT 1
        FROM {table_name} AS e
        JOIN dwp.lineage_batch AS b
          ON b.batch_id = e.batch_id
         AND b.is_active = TRUE
         AND b.publish_status = 'PUBLISHED'
        WHERE e.is_active = TRUE
          AND e.batch_id = ?
          AND e.environment = ?
          AND (e.source_table = ? OR e.target_table = ?)
    """


class DWSLineageEdgeReader:
    """Read one environment's active physical or business lineage projection."""

    def __init__(
        self,
        profile: str | None = None,
        *,
        connection: Any | None = None,
        connection_factory: Callable[[str], Any] | None = None,
    ) -> None:
        if profile is not None and (
            not isinstance(profile, str) or not profile.strip()
        ):
            raise ValueError("profile must be a non-empty string or None")
        if connection_factory is not None and not callable(connection_factory):
            raise TypeError("connection_factory must be callable or None")
        if connection is None and profile is None:
            raise ValueError("DWS lineage reader needs a profile or injected connection")
        self.profile = profile.strip() if isinstance(profile, str) else None
        self._connection = connection
        self._connection_factory = connection_factory or _connect_with_profile
        self._request_connection: Any | None = None
        self._active_batch_id: str | None = None

    @property
    def active_batch_id(self) -> str | None:
        """Return the batch resolved for the currently open request scope."""

        return self._active_batch_id

    @contextmanager
    def request_scope(
        self,
        *,
        timing: LineageQueryTiming | None = None,
    ) -> Iterator[DWSLineageEdgeReader]:
        """Open one connection and resolve one published active batch."""

        if self._request_connection is not None:
            yield self
            return

        owns_connection = self._connection is None
        connection = self._connection
        if connection is None:
            if self.profile is None:
                raise RuntimeError("DWS lineage reader has no database profile")
            connect_started = perf_counter() if timing is not None else None
            try:
                connection = self._connection_factory(self.profile)
            finally:
                if timing is not None and connect_started is not None:
                    timing.connection_ms += int(
                        (perf_counter() - connect_started) * 1000
                    )
            if connection is None:
                raise RuntimeError("DWS connection factory returned no connection")

        self._request_connection = connection
        try:
            self._active_batch_id = self._resolve_active_batch(timing=timing)
            yield self
        finally:
            self._active_batch_id = None
            self._request_connection = None
            if owns_connection:
                _close_quietly(connection)

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> tuple[LineageReadEdge, ...]:
        return self._read_neighbors(
            environment=environment,
            table=source_table,
            source_profile=source_profile,
            view=view,
            outgoing=True,
            timing=timing,
        )

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> tuple[LineageReadEdge, ...]:
        return self._read_neighbors(
            environment=environment,
            table=target_table,
            source_profile=source_profile,
            view=view,
            outgoing=False,
            timing=timing,
        )

    def contains_node(
        self,
        *,
        environment: str,
        table: str,
        source_profile: str | None = None,
        view: LineageView | str = LineageView.BUSINESS,
        timing: LineageQueryTiming | None = None,
    ) -> bool:
        environment = _required_text(environment, "environment")
        table = _normalize_table(table)
        source_profile = _optional_text(source_profile, "source_profile")
        resolved_view = _resolve_view(view)
        with self._request_for_read(timing=timing):
            assert self._request_connection is not None
            batch_id = self._require_active_batch_id()
            table_name = _table_for_view(resolved_view)
            params: list[object] = [batch_id, environment, table, table]
            if source_profile is not None:
                sql = _contains_node_sql(table_name) + " AND e.source_profile = ?"
                params.append(source_profile)
            else:
                sql = _contains_node_sql(table_name)
            with _cursor_scope(self._request_connection) as cursor:
                _execute(cursor, sql, params)
                return cursor.fetchone() is not None

    def _read_neighbors(
        self,
        *,
        environment: str,
        table: str,
        source_profile: str | None,
        view: LineageView | str,
        outgoing: bool,
        timing: LineageQueryTiming | None,
    ) -> tuple[LineageReadEdge, ...]:
        environment = _required_text(environment, "environment")
        table = _normalize_table(table)
        source_profile = _optional_text(source_profile, "source_profile")
        resolved_view = _resolve_view(view)
        with self._request_for_read(timing=timing):
            assert self._request_connection is not None
            batch_id = self._require_active_batch_id()
            table_name = _table_for_view(resolved_view)
            sql = _neighbor_sql(table_name, outgoing=outgoing)
            params: list[object] = [batch_id, environment, table]
            if source_profile is not None:
                sql += " AND e.source_profile = ?"
                params.append(source_profile)
            sql += (
                " ORDER BY e.environment, e.source_profile, e.source_table, "
                "e.target_table, e.batch_id"
            )
            query_started = perf_counter() if timing is not None else None
            try:
                with _cursor_scope(self._request_connection) as cursor:
                    _execute(cursor, sql, params)
                    rows = tuple(cursor.fetchall())
            finally:
                if timing is not None and query_started is not None:
                    timing.edge_query_ms += int(
                        (perf_counter() - query_started) * 1000
                    )
            conversion_started = perf_counter() if timing is not None else None
            try:
                result = tuple(_edge_from_row(row) for row in rows)
            finally:
                if timing is not None and conversion_started is not None:
                    timing.projection_ms += int(
                        (perf_counter() - conversion_started) * 1000
                    )
            if timing is not None:
                timing.edge_rows += len(result)
            return result

    @contextmanager
    def _request_for_read(
        self,
        *,
        timing: LineageQueryTiming | None,
    ) -> Iterator[None]:
        if self._request_connection is not None:
            yield
            return
        with self.request_scope(timing=timing):
            yield

    def _require_active_batch_id(self) -> str:
        if self._active_batch_id is None:
            raise DWSActiveSnapshotNotFoundError()
        return self._active_batch_id

    def _resolve_active_batch(self, *, timing: LineageQueryTiming | None) -> str:
        if self._request_connection is None:
            raise RuntimeError("DWS request connection is not open")
        started = perf_counter() if timing is not None else None
        try:
            with _cursor_scope(self._request_connection) as cursor:
                _execute(cursor, ACTIVE_BATCH_SQL)
                rows = tuple(cursor.fetchall())
            if not rows:
                raise DWSActiveSnapshotNotFoundError()
            if len(rows) != 1:
                raise DWSLineageReaderError(
                    LINEAGE_ACTIVE_SNAPSHOT_INVALID,
                    reason="DWS contains more than one active lineage batch",
                )
            try:
                batch_id = _required_text(rows[0][0], "active batch_id")
                status = _required_text(rows[0][1], "active publish_status").upper()
            except (IndexError, TypeError, ValueError) as exc:
                raise DWSLineageReaderError(
                    LINEAGE_ACTIVE_SNAPSHOT_INVALID,
                    reason="active lineage batch row is invalid",
                ) from exc
            if status != "PUBLISHED":
                raise DWSLineageReaderError(
                    LINEAGE_ACTIVE_SNAPSHOT_INVALID,
                    reason="active lineage batch is not PUBLISHED",
                )
            return batch_id
        finally:
            if timing is not None and started is not None:
                timing.active_batch_resolve_ms += int(
                    (perf_counter() - started) * 1000
                )


def _edge_from_row(row: Any) -> LineageReadEdge:
    try:
        values = tuple(row)
        if len(values) != 5:
            raise ValueError("DWS lineage neighbor row has invalid shape")
        return LineageReadEdge(
            environment=_required_text(values[0], "environment"),
            source_profile=_required_text(values[1], "source_profile"),
            source_table=_required_text(values[2], "source_table"),
            target_table=_required_text(values[3], "target_table"),
            batch_id=_required_text(values[4], "batch_id"),
        )
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("DWS lineage neighbor row is invalid") from exc


def _table_for_view(view: LineageView) -> str:
    return {
        LineageView.BUSINESS: _BUSINESS_EDGE_TABLE,
        LineageView.PHYSICAL: _PHYSICAL_EDGE_TABLE,
    }[view]


def _resolve_view(value: LineageView | str) -> LineageView:
    try:
        return LineageView(value)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(view.value for view in LineageView)
        raise ValueError(f"view must be one of: {valid}") from exc


def _normalize_table(value: object) -> str:
    normalized = canonicalize_dataset_name(value)
    if normalized is None:
        raise ValueError("table must be a qualified schema.table dataset reference")
    return normalized


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


@contextmanager
def _cursor_scope(connection: Any) -> Iterator[Any]:
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        _close_quietly(cursor)


def _execute(cursor: Any, sql: str, params: list[object] | tuple[object, ...] = ()) -> Any:
    # SQL is selected from fixed table/view branches; runtime values are bound.
    # pi-lens-ignore: python-sql-injection
    return cursor.execute(sql, tuple(params))


def _close_quietly(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


__all__ = [
    "DWSActiveSnapshotNotFoundError",
    "DWSLineageEdgeReader",
    "DWSLineageReaderError",
    "LINEAGE_ACTIVE_SNAPSHOT_INVALID",
    "LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND",
]
