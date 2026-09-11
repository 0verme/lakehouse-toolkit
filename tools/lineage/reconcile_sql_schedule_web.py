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
    load_usable_suppressed_edge_keys,
)
from tools.lineage.reconcile_sql_schedule import run as run_reconciliation

_STATUS_LABELS = {
    ReconciliationStatus.MATCH: "两边一致",
    ReconciliationStatus.SQL_ONLY: "SQL实际调用但调度未配置",
    ReconciliationStatus.SCHEDULE_ONLY: "调度已配置但SQL未调用",
}
_STATUS_PRIORITY = {
    ReconciliationStatus.SQL_ONLY: 0,
    ReconciliationStatus.SCHEDULE_ONLY: 1,
    ReconciliationStatus.MATCH: 2,
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


@dataclass(frozen=True, slots=True)
class ReconciliationRowView:
    """Safe row projection used by the HTML table."""

    source_table: str
    sql_actual: bool
    schedule_configured: bool
    status: ReconciliationStatus
    status_label: str


@dataclass(frozen=True, slots=True)
class ReconciliationExportRow:
    """Business-facing row projection used by the XLSX export."""

    target_table: str
    source_table: str
    sql_actual: bool
    schedule_configured: bool
    status: ReconciliationStatus

    @property
    def status_label(self) -> str:
        return status_to_label(self.status)

    def as_excel_row(self) -> tuple[str, str, str, str, str]:
        return (
            self.target_table,
            self.source_table,
            "是" if self.sql_actual else "否",
            "是" if self.schedule_configured else "否",
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
    """Presentation model for exactly one target table."""

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


def filter_suppressed_rows(
    rows: Iterable[LineageReconciliationRow],
    suppressed_edge_keys: Iterable[tuple[str, str]],
) -> tuple[LineageReconciliationRow, ...]:
    """Hide only SQL_ONLY rows whose exact edge identity is usable."""

    keys = frozenset(suppressed_edge_keys)
    return tuple(
        row
        for row in rows
        if not (
            _coerce_status(row.status) is ReconciliationStatus.SQL_ONLY
            and (row.source_table, row.target_table) in keys
        )
    )


def _export_row_sort_key(row: ReconciliationExportRow) -> tuple[int, str, str]:
    return (
        _STATUS_PRIORITY[_coerce_status(row.status)],
        row.target_table,
        row.source_table,
    )


def build_export_rows(
    outcomes: Iterable[TargetReconciliationOutcome],
) -> tuple[ReconciliationExportRow, ...]:
    """Flatten successful target view models into the business export projection."""

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
            )
            for row in view_model.rows
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
    suppressed_edge_keys: Iterable[tuple[str, str]] | None = None,
) -> ReconciliationViewModel:
    """Convert one formal result into the target-centric UI model."""

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

    rows = filter_suppressed_rows(
        (row for row in result.rows if row.target_table == resolved_target),
        suppressed_edge_keys or (),
    )
    summary = build_summary(sort_rows(rows))
    target_status = _summary_status(summary)
    row_views = tuple(
        ReconciliationRowView(
            source_table=row.source_table,
            sql_actual=row.sql_present,
            schedule_configured=row.schedule_present,
            status=_coerce_status(row.status),
            status_label=status_to_label(row.status),
        )
        for row in sort_rows(rows)
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
        rows=row_views,
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
    timing: ReconciliationTiming | None = None,
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
    if timing is not None and runner is None:
        kwargs["timing"] = timing
    return reconcile(**kwargs)  # type: ignore[arg-type]


def reconcile_targets(
    scope: LineageEnvironmentScope,
    target_tables: Iterable[str] | str,
    *,
    runner: ReconciliationRunner | None = None,
    suppression_store: Any | None = None,
    timing: ReconciliationTiming | None = None,
) -> tuple[TargetReconciliationOutcome, ...]:
    """Run one batched DWS query and split its result back by target."""

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
                    timing=batch_timing if runner is None else None,
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
                    kwargs["timing"] = batch_timing
                result = reconcile(**kwargs)  # type: ignore[arg-type]
            batch_timing.reconciliation_rows = len(result.rows)

            suppression_started = perf_counter()
            suppressed_edge_keys = (
                load_usable_suppressed_edge_keys(
                    result,
                    suppression_store,
                    target_tables=tuple(normalized_targets),
                )
                if suppression_store is not None
                else frozenset()
            )
            batch_timing.suppression_lookup_ms = int(
                (perf_counter() - suppression_started) * 1000
            )
            for target in normalized_targets:
                view_model = build_reconciliation_view_model(
                    result,
                    target_table=target,
                    suppressed_edge_keys=suppressed_edge_keys,
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


def _render_rows_html(view_model: ReconciliationViewModel) -> str:
    rows = []
    for row in view_model.rows:
        row_class = (
            ' class="lineage-reconciliation-diff"'
            if row.status is not ReconciliationStatus.MATCH
            else ""
        )
        rows.append(
            "<tr"
            + row_class
            + ">"
            + f"<td>{escape(row.source_table)}</td>"
            + f"<td>{'是' if row.sql_actual else '否'}</td>"
            + f"<td>{'是' if row.schedule_configured else '否'}</td>"
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
</style>
<table class="lineage-reconciliation-table">
<thead><tr><th>表名</th><th>SQL实际调用</th><th>调度已配置</th><th>差异类型</th></tr></thead>
<tbody>"""
        + "".join(rows)
        + "</tbody></table>"
    )


def _render_view_model(view_model: ReconciliationViewModel) -> None:
    from pywebio.output import put_html  # pyright: ignore[reportMissingImports]

    from shared.ui.pywebio_helper import put_separator

    put_html(f"<h3>目标表：{escape(view_model.target_table)}</h3>")
    put_html(_render_rows_html(view_model))
    put_separator("-")


def _render_outcome(outcome: TargetReconciliationOutcome) -> None:
    from shared.ui.pywebio_helper import put_red_text

    if outcome.error is not None:
        put_red_text(escape(f"目标表：{outcome.target_table}；{outcome.error.message}"))
        return
    if outcome.view_model is not None:
        _render_view_model(outcome.view_model)


def main(
    *,
    resolver: LineageEnvironmentScopeResolver | None = None,
    runner: ReconciliationRunner | None = None,
    suppression_store: Any | None = None,
) -> None:
    """Collect simple UI input and render independent target results."""

    from pywebio.input import (  # pyright: ignore[reportMissingImports]
        actions,
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

    if suppression_store is None:
        try:
            suppression_store = DWSReconciliationSuppressionStore(
                profile=scope.dws_profile
            )
        except Exception:
            # Suppression is presentation-only; an unavailable store leaves
            # the raw reconciliation rows visible.
            suppression_store = None

    put_markdown("## SQL / 调度血缘对账结果")
    timing = ReconciliationTiming()
    outcomes = reconcile_targets(
        scope,
        targets,
        runner=runner,
        suppression_store=suppression_store,
        timing=timing,
    )
    put_markdown(
        "查询 timing："
        f"SQL target-scoped DWS read `{timing.sql_target_scoped_read_ms} ms / "
        f"{timing.sql_rows_read} rows`；"
        f"Schedule target-scoped DWS read `{timing.schedule_target_scoped_read_ms} "
        f"ms / {timing.schedule_rows_read} rows`；"
        f"reconciliation CPU `{timing.reconciliation_cpu_ms} ms / "
        f"{timing.reconciliation_rows} rows`；"
        f"suppression lookup `{timing.suppression_lookup_ms} ms`；"
        f"TOTAL `{timing.total_ms} ms`。"
    )
    for outcome in outcomes:
        _render_outcome(outcome)

    export_rows = build_export_rows(outcomes)
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
