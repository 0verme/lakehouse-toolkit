"""Formal PyWebIO UI for SQL/schedule lineage reconciliation.

This module is only a presentation adapter.  It resolves the user-selected
environment through :mod:`shared.lineage.environment_scope` and calls the
existing reconciliation function; it never reads source metadata, parses SQL,
or calculates schedule lineage itself.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from html import escape
from io import BytesIO
from time import perf_counter
from typing import Any, cast

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.worksheet.worksheet import Worksheet

from shared.lineage.environment_scope import (
    DISABLED_LINEAGE_ENVIRONMENT,
    LINEAGE_SCOPE_CONFIG_INVALID,
    LINEAGE_SCOPE_CONFIG_NOT_FOUND,
    UNKNOWN_LINEAGE_ENVIRONMENT,
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
    LineageEnvironmentScopeResolver,
    load_lineage_environment_scope_resolver,
)
from shared.lineage.reconciliation import (
    SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
    SQL_ACTIVE_SNAPSHOT_NOT_FOUND,
    LineageReconciliationResult,
    LineageReconciliationRow,
    ReconciliationStatus,
    ReconciliationTiming,
    TargetSummaryStatus,
    normalize_lineage_comparison_table_key,
)
from shared.lineage.reconciliation_suppression import (
    DWSReconciliationSuppressionStore,
)
from tools.lineage.reconcile_sql_schedule import run as run_reconciliation

_STATUS_LABELS = {
    ReconciliationStatus.MATCH: "两边一致",
    ReconciliationStatus.SQL_ONLY: "SQL实际调用但调度未配置",
    ReconciliationStatus.SCHEDULE_ONLY: "调度已配置但SQL未调用",
    ReconciliationStatus.SUPPRESSED: "手工码值/静态来源（不参与对账）",
}
_STATUS_PRIORITY = {
    ReconciliationStatus.SQL_ONLY: 0,
    ReconciliationStatus.SCHEDULE_ONLY: 1,
    ReconciliationStatus.MATCH: 2,
    ReconciliationStatus.SUPPRESSED: 3,
}
_NOT_APPLICABLE_LABEL = "—"
SHOW_SUPPRESSED_OPTION = "show_suppressed"
SHOW_SELF_REFERENCE_OPTION = "show_self_reference"


class ReconciliationRowKind(str, Enum):
    """Presentation-only classification of one raw reconciliation row.

    The core final status (including ``SUPPRESSED``) is authoritative; this enum
    only decides whether a row participates in the formal summary and whether
    the UI expands it as evidence.
    """

    NORMAL = "NORMAL"
    SUPPRESSED = "SUPPRESSED"
    SELF_REFERENCE = "SELF_REFERENCE"


_PRESENTATION_LABELS = {
    ReconciliationRowKind.SUPPRESSED: "手工码值/静态来源（不参与对账）",
    ReconciliationRowKind.SELF_REFERENCE: "自关联（不参与调度对账）",
}
_PRESENTATION_PRIORITY = {
    ReconciliationRowKind.NORMAL: 0,
    ReconciliationRowKind.SUPPRESSED: 1,
    ReconciliationRowKind.SELF_REFERENCE: 2,
}
_ERROR_MESSAGES = {
    SQL_ACTIVE_SNAPSHOT_NOT_FOUND: "SQL active snapshot 不存在，无法验证该目标。",
    SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND: "Schedule active snapshot 不存在，无法验证该目标。",
    UNKNOWN_LINEAGE_ENVIRONMENT: "未找到所选环境的 lineage scope 配置。",
    DISABLED_LINEAGE_ENVIRONMENT: "所选环境已停用，不能执行查询。",
    LINEAGE_SCOPE_CONFIG_NOT_FOUND: "未找到 lineage scope 配置文件。",
    LINEAGE_SCOPE_CONFIG_INVALID: "lineage scope 配置无效。",
}
EXPORT_SHEET_TITLE = "血缘对账"
EXPORT_HEADERS = ("目标表", "上游表", "SQL实际调用", "调度已配置", "对账结果")
_EXPORT_FILENAME_PREFIX = "lineage_reconciliation"

ReconciliationRunner = Callable[..., LineageReconciliationResult]


@dataclass(frozen=True, slots=True)
class EnvironmentOption:
    """An environment-only option exposed by the public UI."""

    label: str
    value: str

    def as_pywebio_option(self) -> dict[str, str]:
        return {"label": self.label, "value": self.value}


def _display_status_label(
    kind: ReconciliationRowKind,
    status: ReconciliationStatus,
) -> str:
    """Return the business label; evidence rows never reuse a diff label."""

    evidence_label = _PRESENTATION_LABELS.get(kind)
    return evidence_label if evidence_label is not None else status_to_label(status)


def _display_schedule_value(
    kind: ReconciliationRowKind,
    schedule_configured: bool,
) -> str:
    """The schedule side is deliberately not evaluated for evidence rows."""

    if kind is not ReconciliationRowKind.NORMAL:
        return _NOT_APPLICABLE_LABEL
    return "是" if schedule_configured else "否"


@dataclass(frozen=True, slots=True)
class ReconciliationRowView:
    """Safe row projection used by the HTML table.

    ``kind`` is a presentation classification layered on top of the raw
    domain status: one row is classified exactly once, and only ``NORMAL``
    rows participate in the formal summary and target status.
    """

    source_table: str
    sql_actual: bool
    schedule_configured: bool
    status: ReconciliationStatus
    kind: ReconciliationRowKind = ReconciliationRowKind.NORMAL

    @property
    def is_evidence(self) -> bool:
        """Return whether this row is expanded audit evidence only."""

        return self.kind is not ReconciliationRowKind.NORMAL

    @property
    def status_label(self) -> str:
        return _display_status_label(self.kind, self.status)

    @property
    def sql_display(self) -> str:
        return "是" if self.sql_actual else "否"

    @property
    def schedule_display(self) -> str:
        return _display_schedule_value(self.kind, self.schedule_configured)


@dataclass(frozen=True, slots=True)
class ReconciliationExportRow:
    """Business-facing row projection used by the XLSX export."""

    target_table: str
    source_table: str
    sql_actual: bool
    schedule_configured: bool
    status: ReconciliationStatus
    kind: ReconciliationRowKind = ReconciliationRowKind.NORMAL

    @property
    def status_label(self) -> str:
        return _display_status_label(self.kind, self.status)

    def as_excel_row(self) -> tuple[str, str, str, str, str]:
        return (
            self.target_table,
            self.source_table,
            "是" if self.sql_actual else "否",
            _display_schedule_value(self.kind, self.schedule_configured),
            self.status_label,
        )


@dataclass(frozen=True, slots=True)
class ReconciliationSummary:
    """Compatibility projection retained for callers of the presentation adapter."""

    sql_actual_count: int
    schedule_configured_count: int
    match_count: int
    sql_only_count: int
    schedule_only_count: int


@dataclass(frozen=True, slots=True)
class ReconciliationViewModel:
    """Presentation model for exactly one target table.

    ``rows`` keeps every classified row (NORMAL / SUPPRESSED / SELF_REFERENCE)
    exactly once.  ``summary`` is always the NORMAL-only formal result, while
    :meth:`visible_rows` only decides which evidence the user expanded.
    """

    target_table: str
    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    sql_batch_id: str
    schedule_batch_id: str
    status: TargetSummaryStatus
    status_label: str
    rows: tuple[ReconciliationRowView, ...]
    summary: ReconciliationSummary

    def visible_rows(
        self,
        *,
        show_suppressed: bool = False,
        show_self_reference: bool = False,
    ) -> tuple[ReconciliationRowView, ...]:
        """Return NORMAL rows plus only the evidence the user asked to expand."""

        return tuple(
            row
            for row in self.rows
            if row.kind is ReconciliationRowKind.NORMAL
            or (row.kind is ReconciliationRowKind.SUPPRESSED and show_suppressed)
            or (
                row.kind is ReconciliationRowKind.SELF_REFERENCE and show_self_reference
            )
        )

    @property
    def suppressed_count(self) -> int:
        """Return the number of suppressed static-source evidence rows."""

        return sum(row.kind is ReconciliationRowKind.SUPPRESSED for row in self.rows)

    @property
    def self_reference_count(self) -> int:
        """Return the number of self-reference evidence rows."""

        return sum(
            row.kind is ReconciliationRowKind.SELF_REFERENCE for row in self.rows
        )


@dataclass(frozen=True, slots=True)
class ReconciliationErrorView:
    """Coded, user-readable error projection for one target."""

    error_code: str
    message: str


@dataclass(frozen=True, slots=True)
class TargetReconciliationOutcome:
    """One independent target execution, successful or failed."""

    target_table: str
    view_model: ReconciliationViewModel | None = None
    error: ReconciliationErrorView | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target_table, str) or not self.target_table.strip():
            raise ValueError("target_table must be a non-empty string")
        if (self.view_model is None) == (self.error is None):
            raise ValueError("outcome must contain exactly one view model or error")

    @property
    def succeeded(self) -> bool:
        return self.view_model is not None


def build_environment_options(
    scopes: Iterable[LineageEnvironmentScope],
) -> tuple[EnvironmentOption, ...]:
    """Build sorted environment-only options from enabled scopes."""

    options = [
        EnvironmentOption(label=scope.label, value=scope.environment)
        for scope in scopes
        if scope.enabled
    ]
    return tuple(sorted(options, key=lambda item: (item.label, item.value)))


def status_to_label(status: ReconciliationStatus | str) -> str:
    """Map a domain status to its Chinese UI label without changing the status."""

    resolved = _coerce_status(status)
    return _STATUS_LABELS[resolved]


def sort_rows(
    rows: Iterable[LineageReconciliationRow],
) -> tuple[LineageReconciliationRow, ...]:
    """Put differences first, then MATCH rows, with deterministic ordering."""

    values = tuple(rows)
    return tuple(
        sorted(
            values,
            key=lambda row: (
                _STATUS_PRIORITY[_coerce_status(row.status)],
                row.target_table,
                row.source_table,
            ),
        )
    )


def is_self_reference_row(row: LineageReconciliationRow) -> bool:
    """Return True when both sides share one comparison-normalized identity."""

    return normalize_lineage_comparison_table_key(
        row.source_table
    ) == normalize_lineage_comparison_table_key(row.target_table)


def classify_reconciliation_row(
    row: LineageReconciliationRow,
) -> ReconciliationRowKind:
    """Classify one already-finalized domain row for optional evidence display.

    Suppression has already won in the shared reconciliation contract; this
    function only maps that one row to its display kind. Self-reference remains
    an independent presentation-only evidence classification.
    """

    if is_self_reference_row(row):
        return ReconciliationRowKind.SELF_REFERENCE
    if _coerce_status(row.status) is ReconciliationStatus.SUPPRESSED:
        return ReconciliationRowKind.SUPPRESSED
    return ReconciliationRowKind.NORMAL


def resolve_display_options(values: object) -> tuple[bool, bool]:
    """Resolve checkbox values into ``(show_suppressed, show_self_reference)``.

    Missing or unknown values keep the evidence hidden (fail closed), so the
    display options can never widen the formal reconciliation result.
    """

    if values is None:
        selected: set[object] = set()
    elif isinstance(values, str):
        selected = {values}
    elif isinstance(values, (list, tuple, set, frozenset)):
        selected = set(cast(Iterable[object], values))
    else:
        selected = set()
    return (
        SHOW_SUPPRESSED_OPTION in selected,
        SHOW_SELF_REFERENCE_OPTION in selected,
    )


def _export_row_sort_key(row: ReconciliationExportRow) -> tuple[int, int, str, str]:
    return (
        _PRESENTATION_PRIORITY[row.kind],
        _STATUS_PRIORITY[_coerce_status(row.status)],
        row.target_table,
        row.source_table,
    )


def build_export_rows(
    outcomes: Iterable[TargetReconciliationOutcome],
    *,
    show_suppressed: bool = False,
    show_self_reference: bool = False,
) -> tuple[ReconciliationExportRow, ...]:
    """Flatten the rows visible in the UI into the business export projection.

    Evidence rows are exported only when the matching display option is on,
    exactly like the HTML table.  The formal reconciliation result is not
    consulted here, so the export cannot disagree with the page.
    """

    export_rows: list[ReconciliationExportRow] = []
    for outcome in outcomes:
        if not isinstance(outcome, TargetReconciliationOutcome):
            raise TypeError("outcomes must contain TargetReconciliationOutcome values")
        if outcome.view_model is None:
            continue
        view_model = outcome.view_model
        export_rows.extend(
            ReconciliationExportRow(
                target_table=view_model.target_table,
                source_table=row.source_table,
                sql_actual=row.sql_actual,
                schedule_configured=row.schedule_configured,
                status=row.status,
                kind=row.kind,
            )
            for row in view_model.visible_rows(
                show_suppressed=show_suppressed,
                show_self_reference=show_self_reference,
            )
        )
    return tuple(sorted(export_rows, key=_export_row_sort_key))


def build_excel_bytes(
    rows: Iterable[ReconciliationExportRow],
) -> bytes:
    """Build one in-memory XLSX workbook from already-reconciled rows."""

    export_rows = tuple(rows)
    if any(not isinstance(row, ReconciliationExportRow) for row in export_rows):
        raise TypeError("rows must contain ReconciliationExportRow values")

    workbook = Workbook()
    sheet = cast(Worksheet, workbook.active)
    sheet.title = EXPORT_SHEET_TITLE
    sheet.append(list(EXPORT_HEADERS))
    for row in sorted(export_rows, key=_export_row_sort_key):
        sheet.append(list(row.as_excel_row()))

    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for column, width in {
        "A": 30,
        "B": 42,
        "C": 14,
        "D": 14,
        "E": 34,
    }.items():
        sheet.column_dimensions[column].width = width

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def sanitize_filename_fragment(value: object) -> str:
    """Return a readable filename fragment without path or Windows separators."""

    text = str(value).strip().replace(".", "_")
    safe_text = "".join(
        character if character.isalnum() or character in ("_", "-") else "_"
        for character in text
    )
    return safe_text.strip("_") or "result"


def build_export_filename(
    environment: str,
    target_tables: Iterable[str] | str,
    *,
    generated_at: datetime | str | None = None,
) -> str:
    """Build a safe single- or multi-target XLSX filename without credentials."""

    if isinstance(target_tables, str):
        targets = (target_tables.strip(),)
    else:
        targets = tuple(
            str(target).strip() for target in target_tables if str(target).strip()
        )
    if isinstance(generated_at, datetime):
        timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    elif generated_at is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    else:
        timestamp = str(generated_at).strip()

    parts = [_EXPORT_FILENAME_PREFIX, sanitize_filename_fragment(environment)]
    if len(targets) == 1:
        parts.append(sanitize_filename_fragment(targets[0]))
    parts.append(sanitize_filename_fragment(timestamp))
    return "_".join(parts) + ".xlsx"


def build_summary(rows: Iterable[LineageReconciliationRow]) -> ReconciliationSummary:
    """Build distinct upstream and three-state counts for one target."""

    values = tuple(rows)
    sql_sources = {row.source_table for row in values if row.sql_present}
    schedule_sources = {row.source_table for row in values if row.schedule_present}
    statuses = [_coerce_status(row.status) for row in values]
    return ReconciliationSummary(
        sql_actual_count=len(sql_sources),
        schedule_configured_count=len(schedule_sources),
        match_count=statuses.count(ReconciliationStatus.MATCH),
        sql_only_count=statuses.count(ReconciliationStatus.SQL_ONLY),
        schedule_only_count=statuses.count(ReconciliationStatus.SCHEDULE_ONLY),
    )


def build_reconciliation_view_model(
    result: LineageReconciliationResult,
    *,
    target_table: str | None = None,
) -> ReconciliationViewModel:
    """Convert one formal result into the target-centric UI model.

    Every raw row is classified exactly once.  The formal summary and target
    status consume only ``NORMAL`` rows; suppressed static sources and
    self-reference edges stay available as expandable audit evidence.
    """

    if not isinstance(result, LineageReconciliationResult):
        raise TypeError("result must be a LineageReconciliationResult")

    target_names = {row.target_table for row in result.rows}
    summary_targets = {summary.target_table for summary in result.target_summaries}
    requested_target = (
        normalize_lineage_comparison_table_key(target_table)
        if target_table is not None
        else None
    )
    if requested_target is not None:
        resolved_target = requested_target
    elif len(summary_targets) == 1:
        resolved_target = next(iter(summary_targets))
    elif len(target_names) == 1:
        resolved_target = next(iter(target_names))
    else:
        raise ValueError("a single target_table is required for the UI model")

    target_rows = tuple(
        row for row in result.rows if row.target_table == resolved_target
    )
    ordered_rows = sort_rows(target_rows)
    row_views = tuple(
        ReconciliationRowView(
            source_table=row.source_table,
            sql_actual=row.sql_present,
            schedule_configured=row.schedule_present,
            status=_coerce_status(row.status),
            kind=classify_reconciliation_row(row),
        )
        for row in ordered_rows
    )
    summary = build_summary(
        row
        for row in ordered_rows
        if classify_reconciliation_row(row) is ReconciliationRowKind.NORMAL
    )
    target_status = _summary_status(summary)
    visible_row_views = tuple(
        sorted(row_views, key=lambda row: _PRESENTATION_PRIORITY[row.kind])
    )
    return ReconciliationViewModel(
        target_table=resolved_target,
        environment=result.environment,
        sql_source_profile=result.sql_source_profile,
        schedule_source_profile=result.schedule_source_profile,
        sql_batch_id=result.sql_batch_id,
        schedule_batch_id=result.schedule_batch_id,
        status=target_status,
        status_label=(
            "一致" if target_status is TargetSummaryStatus.CONSISTENT else "有差异"
        ),
        rows=visible_row_views,
        summary=summary,
    )


def map_reconciliation_error(error: Exception) -> ReconciliationErrorView:
    """Keep formal error codes while adding a concise user-facing explanation."""

    code_value = getattr(error, "code", None)
    code = code_value.strip() if isinstance(code_value, str) else type(error).__name__
    if not code:
        code = type(error).__name__
    explanation = _ERROR_MESSAGES.get(code)
    if explanation is None:
        detail = str(error).strip()
        explanation = "查询失败。" if not detail else f"查询失败：{detail}"
    return ReconciliationErrorView(
        error_code=code,
        message=f"{explanation} 错误码：{code}",
    )


def reconcile_target(
    scope: LineageEnvironmentScope,
    target_table: str,
    *,
    runner: ReconciliationRunner | None = None,
    connection: Any | None = None,
    timing: ReconciliationTiming | None = None,
    suppression_store: Any | None = None,
) -> LineageReconciliationResult:
    """Call the formal target-scoped reconciliation function."""

    if not isinstance(scope, LineageEnvironmentScope):
        raise TypeError("scope must be a LineageEnvironmentScope")
    if not isinstance(target_table, str) or not target_table.strip():
        raise ValueError("target_table must be a non-empty string")
    target = normalize_lineage_comparison_table_key(target_table)
    reconcile = runner or run_reconciliation
    kwargs: dict[str, object] = {
        "dws_profile": scope.dws_profile,
        "environment": scope.environment,
        "sql_source_profile": scope.sql_source_profile,
        "schedule_source_profile": scope.schedule_source_profile,
        "target_table": target,
    }
    if connection is not None and runner is None:
        kwargs["connection"] = connection
    if timing is not None and runner is None:
        kwargs["timing"] = timing
    if runner is None:
        kwargs["suppression_store"] = suppression_store
    return reconcile(**kwargs)  # type: ignore[arg-type]


def reconcile_targets(
    scope: LineageEnvironmentScope,
    target_tables: Iterable[str] | str,
    *,
    runner: ReconciliationRunner | None = None,
    suppression_store: Any | None = None,
    connection: Any | None = None,
    timing: ReconciliationTiming | None = None,
) -> tuple[TargetReconciliationOutcome, ...]:
    """Run one batched DWS query and split its result back by target.

    A caller may provide one request-scoped connection for all DWS adapters.
    """

    if not isinstance(scope, LineageEnvironmentScope):
        raise TypeError("scope must be a LineageEnvironmentScope")
    batch_timing = timing or ReconciliationTiming()
    started = perf_counter()
    entries: list[tuple[str, str | None, ReconciliationErrorView | None]] = []
    normalized_targets: list[str] = []
    seen_targets: set[str] = set()
    raw_targets = (target_tables,) if isinstance(target_tables, str) else target_tables
    for raw_target in raw_targets:
        target = raw_target.strip() if isinstance(raw_target, str) else str(raw_target)
        if not target:
            continue
        try:
            normalized = normalize_lineage_comparison_table_key(target)
        except Exception as error:  # noqa: BLE001 - report invalid input per row
            entries.append((target, None, map_reconciliation_error(error)))
            continue
        if normalized in seen_targets:
            continue
        seen_targets.add(normalized)
        normalized_targets.append(normalized)
        entries.append((normalized, normalized, None))

    outcomes_by_target: dict[str, TargetReconciliationOutcome] = {}
    if normalized_targets:
        try:
            if len(normalized_targets) == 1:
                result = reconcile_target(
                    scope,
                    normalized_targets[0],
                    runner=runner,
                    connection=connection,
                    timing=batch_timing if runner is None else None,
                    suppression_store=suppression_store,
                )
            else:
                reconcile = runner or run_reconciliation
                kwargs: dict[str, object] = {
                    "dws_profile": scope.dws_profile,
                    "environment": scope.environment,
                    "sql_source_profile": scope.sql_source_profile,
                    "schedule_source_profile": scope.schedule_source_profile,
                    "target_tables": tuple(normalized_targets),
                }
                if runner is None:
                    if connection is not None:
                        kwargs["connection"] = connection
                    kwargs["timing"] = batch_timing
                    kwargs["suppression_store"] = suppression_store
                result = reconcile(**kwargs)  # type: ignore[arg-type]
            batch_timing.reconciliation_rows = len(result.rows)

            for target in normalized_targets:
                view_model = build_reconciliation_view_model(
                    result,
                    target_table=target,
                )
                outcomes_by_target[target] = TargetReconciliationOutcome(
                    target_table=target,
                    view_model=view_model,
                )
        except Exception as error:  # noqa: BLE001 - one batch failure is shared
            failure = map_reconciliation_error(error)
            for target in normalized_targets:
                outcomes_by_target[target] = TargetReconciliationOutcome(
                    target_table=target,
                    error=failure,
                )

    batch_timing.total_ms = int((perf_counter() - started) * 1000)
    outcomes: list[TargetReconciliationOutcome] = []
    for display_target, normalized_target, error in entries:
        if normalized_target is None:
            outcomes.append(
                TargetReconciliationOutcome(
                    target_table=display_target,
                    error=error
                    or ReconciliationErrorView(
                        error_code="INVALID_TARGET", message="目标表无效。"
                    ),
                )
            )
        else:
            outcomes.append(outcomes_by_target[normalized_target])
    return tuple(outcomes)


def parse_target_tables(value: str) -> tuple[str, ...]:
    """Parse the historical one-qualified-table-per-line textarea format."""

    if not isinstance(value, str):
        return ()
    return tuple(
        normalized
        for normalized in (
            line.strip().replace("\t", "") for line in value.splitlines()
        )
        if normalized
    )


def _coerce_status(status: object) -> ReconciliationStatus:
    if isinstance(status, ReconciliationStatus):
        return status
    try:
        return ReconciliationStatus(status)
    except (TypeError, ValueError) as error:
        raise ValueError("status is not a valid reconciliation status") from error


def _summary_status(summary: ReconciliationSummary) -> TargetSummaryStatus:
    if summary.sql_only_count == 0 and summary.schedule_only_count == 0:
        return TargetSummaryStatus.CONSISTENT
    return TargetSummaryStatus.DIFFERENT


def _render_hidden_evidence_note(
    view_model: ReconciliationViewModel,
    *,
    show_suppressed: bool,
    show_self_reference: bool,
) -> str:
    """Render the optional 'hidden evidence' hint for one target result."""

    hidden_parts: list[str] = []
    if not show_suppressed and view_model.suppressed_count:
        hidden_parts.append(f"静态来源 {view_model.suppressed_count} 条")
    if not show_self_reference and view_model.self_reference_count:
        hidden_parts.append(f"自关联 {view_model.self_reference_count} 条")
    if not hidden_parts:
        return ""
    return (
        '<p class="lineage-reconciliation-hidden">已隐藏（不参与对账）：'
        + "，".join(hidden_parts)
        + "。</p>"
    )


def _render_rows_html(
    view_model: ReconciliationViewModel,
    *,
    show_suppressed: bool = False,
    show_self_reference: bool = False,
) -> str:
    rows = []
    for row in view_model.visible_rows(
        show_suppressed=show_suppressed,
        show_self_reference=show_self_reference,
    ):
        if row.kind is ReconciliationRowKind.NORMAL:
            row_class = (
                ' class="lineage-reconciliation-diff"'
                if row.status is not ReconciliationStatus.MATCH
                else ""
            )
        else:
            # Evidence rows must never reuse the actionable-difference styling.
            row_class = ' class="lineage-reconciliation-evidence"'
        rows.append(
            "<tr"
            + row_class
            + ">"
            + f"<td>{escape(row.source_table)}</td>"
            + f"<td>{escape(row.sql_display)}</td>"
            + f"<td>{escape(row.schedule_display)}</td>"
            + f"<td>{escape(row.status_label)}</td>"
            + "</tr>"
        )
    if not rows:
        rows.append('<tr><td colspan="4">没有已物化的业务上游边</td></tr>')
    return (
        """<style>
.lineage-reconciliation-table {
  width: 100%;
  border-collapse: collapse;
  font-family: Arial, sans-serif;
  font-size: 14px;
  margin: 10px 0;
}
.lineage-reconciliation-table th,
.lineage-reconciliation-table td {
  border: 1px solid #ccc;
  padding: 9px 10px;
  text-align: left;
  vertical-align: top;
  word-break: break-all;
}
.lineage-reconciliation-table th {
  background: #f0f0f0;
  font-weight: 700;
}
.lineage-reconciliation-diff {
  color: #b42318;
  background: #fff1f0;
  font-weight: 600;
}
.lineage-reconciliation-evidence {
  color: #5b6472;
  background: #f5f6f8;
}
.lineage-reconciliation-hidden {
  color: #5b6472;
  font-size: 13px;
  margin: 4px 0 12px;
}
</style>
<table class="lineage-reconciliation-table">
<thead><tr><th>表名</th><th>SQL实际调用</th><th>调度已配置</th><th>差异类型</th></tr></thead>
<tbody>"""
        + "".join(rows)
        + "</tbody></table>"
        + _render_hidden_evidence_note(
            view_model,
            show_suppressed=show_suppressed,
            show_self_reference=show_self_reference,
        )
    )


def _render_view_model(
    view_model: ReconciliationViewModel,
    *,
    show_suppressed: bool = False,
    show_self_reference: bool = False,
) -> None:
    from pywebio.output import put_html  # pyright: ignore[reportMissingImports]

    from shared.ui.pywebio_helper import put_separator

    put_html(f"<h3>目标表：{escape(view_model.target_table)}</h3>")
    put_html(
        _render_rows_html(
            view_model,
            show_suppressed=show_suppressed,
            show_self_reference=show_self_reference,
        )
    )
    put_separator("-")


def _render_outcome(
    outcome: TargetReconciliationOutcome,
    *,
    show_suppressed: bool = False,
    show_self_reference: bool = False,
) -> None:
    from shared.ui.pywebio_helper import put_red_text

    if outcome.error is not None:
        put_red_text(escape(f"目标表：{outcome.target_table}；{outcome.error.message}"))
        return
    if outcome.view_model is not None:
        _render_view_model(
            outcome.view_model,
            show_suppressed=show_suppressed,
            show_self_reference=show_self_reference,
        )


def main(
    *,
    resolver: LineageEnvironmentScopeResolver | None = None,
    runner: ReconciliationRunner | None = None,
    suppression_store: Any | None = None,
) -> None:
    """Collect simple UI input and render independent target results."""

    from pywebio.input import (  # pyright: ignore[reportMissingImports]
        actions,
        checkbox,
        input_group,
        select,
        textarea,
    )
    from pywebio.output import (  # pyright: ignore[reportMissingImports]
        put_file,
        put_markdown,
    )

    from shared.ui.pywebio_helper import put_red_text

    try:
        resolved_resolver = resolver or load_lineage_environment_scope_resolver()
    except Exception as error:  # configuration errors should be visible, not hidden
        failure = map_reconciliation_error(error)
        put_red_text(escape(f"环境配置加载失败；{failure.message}"))
        return

    options = build_environment_options(resolved_resolver.enabled_scopes())
    if not options:
        put_red_text("没有启用的 lineage environment scope，请检查本地配置。")
        return

    while True:
        form = input_group(
            "查询条件",
            [
                select(
                    "环境",
                    name="environment",
                    options=[option.as_pywebio_option() for option in options],
                ),
                textarea(
                    "目标表",
                    name="target_tables",
                    rows=6,
                    placeholder="每行输入一个目标表，例如：DWM.RESULT",
                ),
                checkbox(
                    "展示选项",
                    name="display_options",
                    options=[
                        {
                            "label": "显示手工码值 / 静态来源",
                            "value": SHOW_SUPPRESSED_OPTION,
                            "selected": True,
                        },
                        {
                            "label": "显示自关联",
                            "value": SHOW_SELF_REFERENCE_OPTION,
                        },
                    ],
                ),
                actions(
                    "操作",
                    name="action",
                    buttons=[
                        {"label": "提交", "value": "submit"},
                        {"label": "重置", "value": "reset"},
                    ],
                ),
            ],
        )
        if form.get("action") == "reset":
            continue
        targets = parse_target_tables(str(form.get("target_tables", "")))
        if targets:
            break
        put_red_text("请至少输入一个目标表，每行一个 qualified schema.table。")

    try:
        scope = resolved_resolver.resolve(str(form.get("environment", "")))
    except LineageEnvironmentScopeError as error:
        put_red_text(escape(str(error)))
        return

    # The checkboxes only expand already-finalized SUPPRESSED/self-reference
    # evidence; they never deduplicate or alter reconciliation state.
    show_suppressed, show_self_reference = resolve_display_options(
        form.get("display_options")
    )

    request_connection: Any | None = None
    connection_error: Exception | None = None
    timing = ReconciliationTiming()
    request_started = perf_counter()
    if runner is None:
        try:
            from shared.db.gaussdb import connect_with_profile

            connect_started = perf_counter()
            request_connection = connect_with_profile(scope.dws_profile)
            if request_connection is None:
                raise RuntimeError("DWS connection factory returned no connection")
            timing.connect_ms = int((perf_counter() - connect_started) * 1000)
        except Exception as error:  # connection errors become per-target UI errors
            connection_error = error

    try:
        if suppression_store is None and connection_error is None:
            try:
                suppression_store = DWSReconciliationSuppressionStore(
                    connection=request_connection,
                    profile=scope.dws_profile if request_connection is None else None,
                )
            except Exception:
                # The reconciliation core handles lookup failures fail-open; the
                # UI never overlays or duplicates suppression rows.
                suppression_store = None

        put_markdown("## SQL / 调度血缘对账结果")
        if connection_error is not None:
            failure = map_reconciliation_error(connection_error)
            outcomes = tuple(
                TargetReconciliationOutcome(target_table=target, error=failure)
                for target in targets
            )
        else:
            outcomes = reconcile_targets(
                scope,
                targets,
                runner=runner,
                suppression_store=suppression_store,
                connection=request_connection,
                timing=timing,
            )
    finally:
        if request_connection is not None:
            close = getattr(request_connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        if runner is None:
            timing.total_ms = int((perf_counter() - request_started) * 1000)
    for outcome in outcomes:
        _render_outcome(
            outcome,
            show_suppressed=show_suppressed,
            show_self_reference=show_self_reference,
        )

    export_rows = build_export_rows(
        outcomes,
        show_suppressed=show_suppressed,
        show_self_reference=show_self_reference,
    )
    if export_rows:
        put_file(
            build_export_filename(
                scope.environment,
                tuple(outcome.target_table for outcome in outcomes),
            ),
            build_excel_bytes(export_rows),
            "导出 Excel",
        )


if __name__ == "__main__":
    from shared.ui.pywebio_helper import start_pywebio_app

    start_pywebio_app("SQL / 调度血缘对账", main)
