"""Report SQL actual business lineage versus configured schedule lineage."""

from __future__ import annotations

import argparse
import csv
from collections.abc import Iterable
import io
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, cast

from shared.lineage.materialization_dws import DWSMaterializationStore
from shared.lineage.reconciliation import (
    ActiveSnapshotNotFoundError,
    LineageReconciliationResult,
    _resolve_source_profiles,
    normalize_lineage_comparison_table_key,
    reconcile_active_dws_lineage,
)
from shared.lineage.schedule_materialization import DWSScheduleLineageStore

_SAFE_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class _ReconciliationArgumentParser(argparse.ArgumentParser):
    """Argument parser that validates the split/legacy profile contract."""

    def parse_args(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        args: Iterable[str] | None = None,
        namespace: object = None,
    ) -> argparse.Namespace:
        parsed = super().parse_args(
            args=args,
            namespace=cast(argparse.Namespace | None, namespace),
        )
        if parsed is None:
            self.error("argument parser returned no namespace")
        parsed_namespace = cast(argparse.Namespace, parsed)
        try:
            sql_profile, schedule_profile = _resolve_source_profiles(
                source_profile=parsed_namespace.source_profile,
                sql_source_profile=parsed_namespace.sql_source_profile,
                schedule_source_profile=parsed_namespace.schedule_source_profile,
            )
        except ValueError as error:
            self.error(str(error))
        parsed_namespace.sql_source_profile = sql_profile
        parsed_namespace.schedule_source_profile = schedule_profile
        return cast(argparse.Namespace, parsed)


def build_parser() -> argparse.ArgumentParser:
    """Build the safe, read-only reconciliation CLI parser."""

    parser = _ReconciliationArgumentParser(
        description="Reconcile DWS SQL business lineage with configured schedule lineage."
    )
    parser.add_argument(
        "--dws-profile",
        default=os.getenv("PYTOOLS_LINEAGE_DWS_PROFILE") or None,
        metavar="DWS_PROFILE",
        help="database profile used by the existing DWS connection boundary",
    )
    parser.add_argument(
        "--environment",
        required=True,
        metavar="ENVIRONMENT",
        help="strict lineage environment scope, for example DEV214",
    )
    parser.add_argument(
        "--profile",
        default=None,
        dest="source_profile",
        metavar="PROFILE",
        help=(
            "legacy shorthand for using one profile on both sides; conflicts "
            "with different explicit profiles"
        ),
    )
    parser.add_argument(
        "--sql-profile",
        default=None,
        dest="sql_source_profile",
        metavar="SQL_SOURCE_PROFILE",
        help="SQL lineage fact read scope/profile",
    )
    parser.add_argument(
        "--schedule-profile",
        default=None,
        dest="schedule_source_profile",
        metavar="SCHEDULE_SOURCE_PROFILE",
        help="configured schedule fact read scope/profile",
    )
    parser.add_argument(
        "--target",
        default=None,
        metavar="SCHEMA.TABLE",
        help="only report one comparison-normalized target table",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json", "csv"),
        default="table",
        dest="output_format",
        help="report format; full-scope table output is aggregate-only",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="write the report to a local artifact instead of stdout",
    )
    return parser


def run(
    *,
    dws_profile: str | None,
    environment: str,
    sql_source_profile: str | None = None,
    schedule_source_profile: str | None = None,
    source_profile: str | None = None,
    target_table: str | None = None,
    sql_store: Any | None = None,
    schedule_store: Any | None = None,
) -> LineageReconciliationResult:
    """Read two DWS active snapshots and return their reconciliation report."""

    if sql_store is None or schedule_store is None:
        if not isinstance(dws_profile, str) or not dws_profile.strip():
            raise ValueError(
                "--dws-profile is required unless both DWS stores are injected"
            )
    resolved_sql_store = sql_store or DWSMaterializationStore(
        profile=dws_profile.strip() if isinstance(dws_profile, str) else None
    )
    resolved_schedule_store = schedule_store or DWSScheduleLineageStore(
        profile=dws_profile.strip() if isinstance(dws_profile, str) else None
    )
    return reconcile_active_dws_lineage(
        resolved_sql_store,
        resolved_schedule_store,
        environment=environment,
        sql_source_profile=sql_source_profile,
        schedule_source_profile=schedule_source_profile,
        source_profile=source_profile,
        target_table=target_table,
    )


def render_table(
    result: LineageReconciliationResult,
    *,
    target_table: str | None = None,
    elapsed_ms: int | None = None,
) -> str:
    """Render target rows or an aggregate-only full-scope report."""

    if target_table is None:
        values = result.aggregate_dict()
        if elapsed_ms is not None:
            values["elapsed_ms"] = elapsed_ms
        lines = [
            f"environment={values['environment']}",
            f"sql_profile={values['sql_source_profile']}",
            f"schedule_profile={values['schedule_source_profile']}",
            f"sql_edges={values['sql_edges']}",
            f"schedule_edges={values['schedule_edges']}",
            f"reconciliation_rows={values['reconciliation_rows']}",
            f"match={values['match']}",
            f"sql_only={values['sql_only']}",
            f"schedule_only={values['schedule_only']}",
            f"targets={values['targets']}",
            f"consistent_targets={values['consistent_targets']}",
            f"different_targets={values['different_targets']}",
            f"sql_batch_id={_safe_batch_id(result.sql_batch_id)}",
            f"schedule_batch_id={_safe_batch_id(result.schedule_batch_id)}",
            f"sql_observed_at={values['sql_observed_at'] or '-'}",
            f"schedule_observed_at={values['schedule_observed_at'] or '-'}",
        ]
        if elapsed_ms is not None:
            lines.append(f"elapsed_ms={elapsed_ms}")
        return "\n".join(lines)

    normalized_target = normalize_lineage_comparison_table_key(target_table)
    summary = next(
        (
            item
            for item in result.target_summaries
            if item.target_table == normalized_target
        ),
        None,
    )
    status = "CONSISTENT" if summary is None else summary.status.value
    lines = [
        f"Target: {normalized_target}",
        f"Status: {status}",
        f"SQL profile: {result.sql_source_profile}",
        f"Schedule profile: {result.schedule_source_profile}",
        f"SQL batch: {_safe_batch_id(result.sql_batch_id)}",
        f"Schedule batch: {_safe_batch_id(result.schedule_batch_id)}",
        "",
        "SOURCE_TABLE       SQL_ACTUAL   SCHEDULED   STATUS",
    ]
    for row in result.rows:
        lines.append(
            f"{row.source_table:<18} "
            f"{'YES' if row.sql_present else 'NO':<12} "
            f"{'YES' if row.schedule_present else 'NO':<11} "
            f"{row.status.value}"
        )
    return "\n".join(lines)


def render_json(
    result: LineageReconciliationResult,
    *,
    elapsed_ms: int | None = None,
) -> str:
    """Render the complete machine-readable report."""

    payload = result.to_dict()
    if elapsed_ms is not None:
        payload["elapsed_ms"] = elapsed_ms
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def render_csv(result: LineageReconciliationResult) -> str:
    """Render row-level results as a deterministic CSV artifact."""

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        (
            "ENVIRONMENT",
            "SQL_SOURCE_PROFILE",
            "SCHEDULE_SOURCE_PROFILE",
            "TARGET_TABLE",
            "SOURCE_TABLE",
            "SQL_ACTUAL",
            "SCHEDULED",
            "STATUS",
            "SQL_FACT_COUNT",
            "SCHEDULE_FACT_COUNT",
            "SQL_PROGRAM_COUNT",
            "SCHEDULE_PROCESS_COUNT",
        )
    )
    for row in result.rows:
        writer.writerow(
            (
                row.environment,
                row.sql_source_profile,
                row.schedule_source_profile,
                row.target_table,
                row.source_table,
                "YES" if row.sql_present else "NO",
                "YES" if row.schedule_present else "NO",
                row.status.value,
                row.sql_fact_count,
                row.schedule_fact_count,
                row.sql_program_count,
                row.schedule_process_count,
            )
        )
    return output.getvalue()


def _render(
    result: LineageReconciliationResult,
    *,
    output_format: str,
    target_table: str | None,
    elapsed_ms: int,
) -> str:
    if output_format == "table":
        return (
            render_table(
                result,
                target_table=target_table,
                elapsed_ms=elapsed_ms,
            )
            + "\n"
        )
    if output_format == "json":
        return render_json(result, elapsed_ms=elapsed_ms)
    if output_format == "csv":
        return render_csv(result)
    raise ValueError("unsupported output format")


def _safe_batch_id(value: str) -> str:
    return value if _SAFE_BATCH_ID.fullmatch(value) else "<redacted>"


def _safe_output_path(output: Path | None) -> Path | None:
    if output is None:
        return None
    if "\x00" in str(output):
        raise ValueError("--output must not contain a NUL character")
    root = Path.cwd().resolve()
    candidate = output.expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "--output must stay inside the current working directory"
        ) from error
    return candidate


def _write_or_print(content: str, output: Path | None) -> None:
    if output is None:
        sys.stdout.write(content)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    # pi-lens-ignore: python-path-traversal
    output.write_text(content, encoding="utf-8")


def cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    started = time.perf_counter()
    try:
        output = _safe_output_path(args.output)
        result = run(
            dws_profile=args.dws_profile,
            environment=args.environment,
            sql_source_profile=args.sql_source_profile,
            schedule_source_profile=args.schedule_source_profile,
            source_profile=args.source_profile,
            target_table=args.target,
        )
        content = _render(
            result,
            output_format=args.output_format,
            target_table=args.target,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )
        _write_or_print(content, output)
        return 0
    except ActiveSnapshotNotFoundError as error:
        print(f"ERROR {error.code}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"ERROR {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
