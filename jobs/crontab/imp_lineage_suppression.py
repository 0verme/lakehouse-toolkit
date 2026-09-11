"""Materialize conservative reconciliation suppression audit rows.

The command is intentionally separate from the PyWebIO page.  It reads the
same configured environment scopes and the same active DWS snapshots, then
writes only the explicit suppression audit projection.  A snapshot or
classifier error never retires an existing scope's active suppression rows.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from jobs.crontab._bootstrap import ensure_project_root_on_path

# Direct script execution needs the bootstrap before project-local imports.
# ruff: noqa: E402, I001
ensure_project_root_on_path()

from shared.lineage.domain import ProgramState  # noqa: E402
from shared.lineage.environment_scope import (  # noqa: E402
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
    load_lineage_environment_scopes,
)
from shared.lineage.materialization_dws import DWSMaterializationStore
from shared.lineage.reconciliation import (
    LineageReconciliationResult,
    read_active_schedule_snapshot,
    read_active_sql_business_snapshot,
    reconcile_lineage_snapshots,
)
from shared.lineage.reconciliation_suppression import (
    DWSReconciliationSuppressionStore,
    ReconciliationSuppression,
    ReconciliationSuppressionError,
    classify_reconciliation_suppressions,
)
from shared.lineage.schedule_materialization import DWSScheduleLineageStore


@dataclass(frozen=True, slots=True)
class SuppressionMaterializationSummary:
    """Bounded per-scope result suitable for CLI output."""

    environment: str
    sql_batch_id: str | None
    schedule_batch_id: str | None
    raw_sql_only_count: int
    suppressed_count: int
    actionable_sql_only_count: int
    dry_run: bool
    error: str | None = None


ScopeStoreFactory = Callable[[LineageEnvironmentScope], Any]

# Diagnostics must stay bounded and must never print a credential even when a
# driver error embeds a connection string.  Only secrets are masked; host/port
# survive so an operator can still see which endpoint failed.
_ERROR_MESSAGE_LIMIT = 400
_REDACTED = "<redacted>"
_URL_CREDENTIAL_RE = re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*://)[^/@\s]*@")
_SECRET_PAIR_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key"
    r"|private[_-]?key|credential)\b\s*[=:]\s*[^\s,;]+"
)


def _sanitize_log_message(message: object) -> str:
    """Collapse a message into one bounded, credential-free log line."""

    text = re.sub(r"\s+", " ", str(message or "")).strip()
    text = _URL_CREDENTIAL_RE.sub(rf"\1{_REDACTED}@", text)
    text = _SECRET_PAIR_RE.sub(rf"\1={_REDACTED}", text)
    if len(text) > _ERROR_MESSAGE_LIMIT:
        text = text[: _ERROR_MESSAGE_LIMIT - 3].rstrip() + "..."
    return text


def _describe_failure(error: Exception) -> str:
    """Return ``ExceptionType: root cause`` for fail-open diagnostics.

    The exception type alone is not diagnosable: an environment-wide fail-open
    used to report only ``RuntimeError`` and lost the real cause.
    """

    detail = _sanitize_log_message(str(error))
    return f"{type(error).__name__}: {detail}" if detail else type(error).__name__


def _default_sql_store(scope: LineageEnvironmentScope) -> DWSMaterializationStore:
    return DWSMaterializationStore(profile=scope.dws_profile)


def _default_schedule_store(scope: LineageEnvironmentScope) -> DWSScheduleLineageStore:
    return DWSScheduleLineageStore(profile=scope.dws_profile)


def _default_suppression_store(
    scope: LineageEnvironmentScope,
) -> DWSReconciliationSuppressionStore:
    return DWSReconciliationSuppressionStore(profile=scope.dws_profile)


def _select_scopes(
    scopes: Iterable[LineageEnvironmentScope],
    environment: str | None,
) -> tuple[LineageEnvironmentScope, ...]:
    values = tuple(scopes)
    if any(not isinstance(scope, LineageEnvironmentScope) for scope in values):
        raise TypeError("scopes must contain LineageEnvironmentScope values")
    enabled = tuple(scope for scope in values if scope.enabled)
    if environment is None:
        return enabled
    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("environment must be a non-empty string")
    selected = tuple(
        scope for scope in values if scope.environment == environment.strip()
    )
    if not selected:
        raise ValueError("requested environment is not configured")
    if not selected[0].enabled:
        raise ValueError("requested environment is disabled")
    return selected


def _reconcile_scope(
    scope: LineageEnvironmentScope,
    *,
    observed_at: datetime,
    sql_store_factory: ScopeStoreFactory,
    schedule_store_factory: ScopeStoreFactory,
) -> tuple[LineageReconciliationResult, tuple[ReconciliationSuppression, ...]]:
    sql_store = sql_store_factory(scope)
    schedule_store = schedule_store_factory(scope)
    sql_snapshot = read_active_sql_business_snapshot(
        sql_store,
        environment=scope.environment,
        source_profile=scope.sql_source_profile,
    )
    read_program_states = getattr(sql_store, "read_program_states", None)
    if not callable(read_program_states):
        raise ReconciliationSuppressionError(
            "DWS SQL store does not expose active program inventory"
        )
    program_states = cast(
        Iterable[ProgramState],
        read_program_states(
            active_only=True,
            environment=scope.environment,
        ),
    )
    schedule_snapshot = read_active_schedule_snapshot(
        schedule_store,
        environment=scope.environment,
        source_profile=scope.schedule_source_profile,
    )
    result = reconcile_lineage_snapshots(
        sql_snapshot,
        schedule_snapshot,
        environment=scope.environment,
        sql_source_profile=scope.sql_source_profile,
        schedule_source_profile=scope.schedule_source_profile,
    )
    suppressions = classify_reconciliation_suppressions(
        result,
        sql_snapshot,
        schedule_snapshot,
        program_states=program_states,
        observed_at=observed_at,
    )
    return result, suppressions


def run(
    scopes: Iterable[LineageEnvironmentScope],
    *,
    environment: str | None = None,
    dry_run: bool = False,
    observed_at: datetime | None = None,
    sql_store_factory: ScopeStoreFactory = _default_sql_store,
    schedule_store_factory: ScopeStoreFactory = _default_schedule_store,
    suppression_store_factory: ScopeStoreFactory = _default_suppression_store,
) -> tuple[SuppressionMaterializationSummary, ...]:
    """Materialize each enabled scope; failed scopes remain fail-open."""

    resolved_scopes = _select_scopes(scopes, environment)
    if not resolved_scopes:
        raise ValueError("no enabled lineage environment scopes are configured")
    effective_observed_at = observed_at or datetime.now(timezone.utc)
    if (
        effective_observed_at.tzinfo is None
        or effective_observed_at.utcoffset() is None
    ):
        raise ValueError("observed_at must include a timezone offset")

    summaries: list[SuppressionMaterializationSummary] = []
    failures: list[str] = []
    first_failure: Exception | None = None
    for scope in resolved_scopes:
        try:
            result, suppressions = _reconcile_scope(
                scope,
                observed_at=effective_observed_at,
                sql_store_factory=sql_store_factory,
                schedule_store_factory=schedule_store_factory,
            )
            if not dry_run:
                suppression_store_factory(scope).publish(
                    suppressions,
                    environment=scope.environment,
                    sql_source_profile=scope.sql_source_profile,
                    schedule_source_profile=scope.schedule_source_profile,
                    observed_at=effective_observed_at,
                )
        except Exception as error:  # noqa: BLE001 - preserve fail-open per scope
            failure = _describe_failure(error)
            failures.append(f"{scope.environment}:{failure}")
            if first_failure is None:
                first_failure = error
            summaries.append(
                SuppressionMaterializationSummary(
                    environment=scope.environment,
                    sql_batch_id=None,
                    schedule_batch_id=None,
                    raw_sql_only_count=0,
                    suppressed_count=0,
                    actionable_sql_only_count=0,
                    dry_run=dry_run,
                    error=failure,
                )
            )
            continue

        summaries.append(
            SuppressionMaterializationSummary(
                environment=scope.environment,
                sql_batch_id=result.sql_batch_id,
                schedule_batch_id=result.schedule_batch_id,
                raw_sql_only_count=result.sql_only_count,
                suppressed_count=len(suppressions),
                actionable_sql_only_count=result.sql_only_count - len(suppressions),
                dry_run=dry_run,
            )
        )

    if failures:
        raise RuntimeError(
            "suppression materialization failed for scope(s): " + ", ".join(failures)
        ) from first_failure
    return tuple(summaries)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize SQL_ONLY reconciliation suppression audit rows."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="lineage provider YAML; defaults to local/example scope loader rules",
    )
    parser.add_argument(
        "--environment",
        default=None,
        metavar="ENVIRONMENT",
        help="only materialize one enabled environment scope",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read and classify snapshots without writing the DWS audit table",
    )
    return parser


def _emit_summary(summary: SuppressionMaterializationSummary) -> None:
    values = [
        f"environment={summary.environment}",
        f"sql_batch={summary.sql_batch_id or '-'}",
        f"schedule_batch={summary.schedule_batch_id or '-'}",
        f"raw_sql_only_count={summary.raw_sql_only_count}",
        f"suppressed_count={summary.suppressed_count}",
        f"actionable_sql_only_count={summary.actionable_sql_only_count}",
        f"dry_run={summary.dry_run}",
    ]
    if summary.error is not None:
        values.append(f"error={summary.error}")
    print(" ".join(values), flush=True)


def main(
    *,
    config_path: str | Path | None = None,
    environment: str | None = None,
    dry_run: bool = False,
    scopes: Iterable[LineageEnvironmentScope] | None = None,
    observed_at: datetime | None = None,
    sql_store_factory: ScopeStoreFactory = _default_sql_store,
    schedule_store_factory: ScopeStoreFactory = _default_schedule_store,
    suppression_store_factory: ScopeStoreFactory = _default_suppression_store,
) -> int:
    configured_scopes = tuple(
        scopes if scopes is not None else load_lineage_environment_scopes(config_path)
    )
    summaries = run(
        configured_scopes,
        environment=environment,
        dry_run=dry_run,
        observed_at=observed_at,
        sql_store_factory=sql_store_factory,
        schedule_store_factory=schedule_store_factory,
        suppression_store_factory=suppression_store_factory,
    )
    for summary in summaries:
        _emit_summary(summary)
    return 0


def cli(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return main(
            config_path=args.config,
            environment=args.environment,
            dry_run=args.dry_run,
        )
    except (LineageEnvironmentScopeError, ValueError, RuntimeError) as error:
        print(f"ERROR {_describe_failure(error)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
