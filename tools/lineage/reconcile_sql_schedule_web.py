"""Formal PyWebIO UI for SQL/schedule lineage reconciliation.

This module is only a presentation adapter.  It resolves the user-selected
environment through :mod:`shared.lineage.environment_scope` and calls the
existing reconciliation function; it never reads source metadata, parses SQL,
or calculates schedule lineage itself.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from html import escape

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
    LineageReconciliationResult,
    LineageReconciliationRow,
    ReconciliationStatus,
    SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND,
    SQL_ACTIVE_SNAPSHOT_NOT_FOUND,
    TargetSummaryStatus,
    normalize_lineage_comparison_table_key,
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
class ReconciliationSummary:
    """Small target-level summary shown above the detail table."""

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

    rows = tuple(row for row in result.rows if row.target_table == resolved_target)
    summary = build_summary(sort_rows(rows))
    target_summary = next(
        (
            item
            for item in result.target_summaries
            if item.target_table == resolved_target
        ),
        None,
    )
    target_status = (
        target_summary.status
        if target_summary is not None
        else _summary_status(summary)
    )
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
) -> LineageReconciliationResult:
    """Call the formal Python reconciliation function for one target."""

    if not isinstance(scope, LineageEnvironmentScope):
        raise TypeError("scope must be a LineageEnvironmentScope")
    if not isinstance(target_table, str) or not target_table.strip():
        raise ValueError("target_table must be a non-empty string")
    reconcile = runner or run_reconciliation
    return reconcile(
        dws_profile=scope.dws_profile,
        environment=scope.environment,
        sql_source_profile=scope.sql_source_profile,
        schedule_source_profile=scope.schedule_source_profile,
        target_table=target_table.strip(),
    )


def reconcile_targets(
    scope: LineageEnvironmentScope,
    target_tables: Iterable[str],
    *,
    runner: ReconciliationRunner | None = None,
) -> tuple[TargetReconciliationOutcome, ...]:
    """Run each target independently so one failure cannot abort the batch."""

    outcomes: list[TargetReconciliationOutcome] = []
    for raw_target in target_tables:
        target = raw_target.strip() if isinstance(raw_target, str) else str(raw_target)
        if not target:
            continue
        try:
            result = reconcile_target(scope, target, runner=runner)
            view_model = build_reconciliation_view_model(result, target_table=target)
        except Exception as error:  # noqa: BLE001 - isolate one user target
            outcomes.append(
                TargetReconciliationOutcome(
                    target_table=target,
                    error=map_reconciliation_error(error),
                )
            )
        else:
            outcomes.append(
                TargetReconciliationOutcome(
                    target_table=target,
                    view_model=view_model,
                )
            )
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
    from pywebio.output import put_html, put_text  # pyright: ignore[reportMissingImports]

    from shared.ui.pywebio_helper import put_separator, put_table_plus

    put_html(f"<h3>目标表：{escape(view_model.target_table)}</h3>")
    put_text(f"环境：{view_model.environment}")
    put_text(f"SQL 血缘来源：{view_model.sql_source_profile}")
    put_text(f"调度血缘来源：{view_model.schedule_source_profile}")
    put_text(f"SQL batch：{view_model.sql_batch_id}")
    put_text(f"Schedule batch：{view_model.schedule_batch_id}")
    put_text(f"汇总状态：{view_model.status_label}")
    summary = view_model.summary
    put_table_plus(
        [
            ["汇总项", "数量"],
            ["SQL 实际上游数", summary.sql_actual_count],
            ["调度配置上游数", summary.schedule_configured_count],
            ["MATCH 数", summary.match_count],
            ["SQL_ONLY 数", summary.sql_only_count],
            ["SCHEDULE_ONLY 数", summary.schedule_only_count],
        ]
    )
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
) -> None:
    """Collect simple UI input and render independent target results."""

    from pywebio.input import (  # pyright: ignore[reportMissingImports]
        actions,
        input_group,
        select,
        textarea,
    )
    from pywebio.output import put_markdown  # pyright: ignore[reportMissingImports]

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

    put_markdown("## SQL / 调度血缘对账结果")
    for outcome in reconcile_targets(scope, targets, runner=runner):
        _render_outcome(outcome)


if __name__ == "__main__":
    from shared.ui.pywebio_helper import start_pywebio_app

    start_pywebio_app("SQL / 调度血缘对账", main)
