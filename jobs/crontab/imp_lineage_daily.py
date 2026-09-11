"""统一执行 SQL、Schedule 和 reconciliation suppression lineage 日批。"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from jobs.crontab._bootstrap import ensure_project_root_on_path

# Direct script execution needs the bootstrap before project-local imports.
# ruff: noqa: E402, I001
ensure_project_root_on_path()

from jobs.crontab import imp_lineage_edge  # noqa: E402
from jobs.crontab import imp_lineage_suppression  # noqa: E402
from jobs.crontab import imp_schedule_lineage  # noqa: E402
from shared.lineage.environment_scope import (  # noqa: E402
    LineageEnvironmentScope,
    LineageEnvironmentScopeResolver,
    load_lineage_environment_scopes,
)


class StepStatus(str, Enum):
    """Status of one lineage step for one environment."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True, slots=True)
class StepResult:
    """Small structured result used by the daily orchestration boundary."""

    step: str
    environment: str
    status: StepStatus
    batch_id: str | None = None
    elapsed: float = 0.0
    error_code: str | None = None
    message: str | None = None
    rows: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.step, str) or not self.step.strip():
            raise ValueError("step must be a non-empty string")
        if not isinstance(self.environment, str) or not self.environment.strip():
            raise ValueError("environment must be a non-empty string")
        if not isinstance(self.status, StepStatus):
            try:
                object.__setattr__(self, "status", StepStatus(self.status))
            except (TypeError, ValueError) as exc:
                raise ValueError("status is not a valid step status") from exc
        if (
            isinstance(self.elapsed, bool)
            or not isinstance(self.elapsed, (int, float))
            or self.elapsed < 0
        ):
            raise ValueError("elapsed must be a non-negative number")
        if self.rows is not None and (
            isinstance(self.rows, bool)
            or not isinstance(self.rows, int)
            or self.rows < 0
        ):
            raise ValueError("rows must be a non-negative integer or None")


@dataclass(frozen=True, slots=True)
class EnvironmentRunResult:
    """Results of the three steps for one configured environment scope."""

    environment: str
    sql: StepResult
    schedule: StepResult
    suppression: StepResult

    @property
    def succeeded(self) -> bool:
        return all(
            result.status is StepStatus.SUCCESS
            for result in (self.sql, self.schedule, self.suppression)
        )

    @property
    def failed(self) -> bool:
        return any(
            result.status is StepStatus.FAILED
            for result in (self.sql, self.schedule, self.suppression)
        )


@dataclass(frozen=True, slots=True)
class LineageDailyResult:
    """Aggregate result returned by :func:`run`."""

    environments: tuple[EnvironmentRunResult, ...]
    elapsed: float

    @property
    def success_count(self) -> int:
        return sum(item.succeeded for item in self.environments)

    @property
    def failed_count(self) -> int:
        return len(self.environments) - self.success_count

    @property
    def exit_code(self) -> int:
        return 0 if self.failed_count == 0 else 1


StepRunner = Callable[[LineageEnvironmentScope], Any]
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_SAFE_BATCH_ID = re.compile(r"batch-[A-Za-z0-9][A-Za-z0-9._-]{0,121}")
_SAFE_ERROR_CODE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")


def _safe_identifier(value: object) -> str:
    candidate = value.strip() if isinstance(value, str) else ""
    return candidate if _SAFE_IDENTIFIER.fullmatch(candidate) else "<redacted>"


def _safe_batch_id(value: object) -> str:
    candidate = value.strip() if isinstance(value, str) else ""
    return candidate if _SAFE_BATCH_ID.fullmatch(candidate) else "<redacted>"


def _safe_error_code(error: Exception) -> str:
    candidate = type(error).__name__
    return candidate if _SAFE_ERROR_CODE.fullmatch(candidate) else "STEP_FAILED"


def _select_scopes(
    scopes: Iterable[LineageEnvironmentScope],
    environment: str | None,
) -> tuple[LineageEnvironmentScope, ...]:
    resolver = LineageEnvironmentScopeResolver(tuple(scopes))
    if environment is not None:
        return (resolver.resolve(environment),)
    selected = resolver.enabled_scopes()
    if not selected:
        raise ValueError("no enabled lineage environment scopes are configured")
    return selected


def _result_batch_id(value: object) -> str | None:
    batch_id = getattr(value, "batch_id", None)
    return batch_id if isinstance(batch_id, str) and batch_id.strip() else None


def _result_rows(value: object) -> int | None:
    candidates = (value,)
    if isinstance(value, (tuple, list)) and len(value) == 1:
        candidates = (value[0],)
    for candidate in candidates:
        for field_name in ("suppression_count", "suppressed_count", "row_count"):
            count = getattr(candidate, field_name, None)
            if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                return count
    return None


def _emit_step(result: StepResult) -> None:
    values = [
        "[lineage-daily]",
        _safe_identifier(result.environment),
        f"{result.step:<12}",
        f"{result.status.value:<7}",
    ]
    if result.batch_id is not None:
        values.append(f"batch={_safe_batch_id(result.batch_id)}")
    if result.rows is not None:
        values.append(f"rows={result.rows}")
    if result.status is StepStatus.SKIPPED:
        values.append(f"reason={result.message or 'unspecified'}")
    elif result.status is StepStatus.FAILED:
        values.append(f"error_code={result.error_code or 'STEP_FAILED'}")
    values.append(f"elapsed={result.elapsed:.1f}s")
    print(" ".join(values), flush=True)


def _execute_step(
    scope: LineageEnvironmentScope,
    step: str,
    runner: StepRunner,
) -> StepResult:
    started_at = time.perf_counter()
    try:
        value = runner(scope)
    except Exception as error:  # noqa: BLE001 - isolate one environment step
        result = StepResult(
            step=step,
            environment=scope.environment,
            status=StepStatus.FAILED,
            elapsed=time.perf_counter() - started_at,
            error_code=_safe_error_code(error),
            message="runner_failed",
        )
    else:
        result = StepResult(
            step=step,
            environment=scope.environment,
            status=StepStatus.SUCCESS,
            batch_id=_result_batch_id(value),
            elapsed=time.perf_counter() - started_at,
            rows=_result_rows(value),
        )
    _emit_step(result)
    return result


def _skipped_step(scope: LineageEnvironmentScope) -> StepResult:
    result = StepResult(
        step="SUPPRESSION",
        environment=scope.environment,
        status=StepStatus.SKIPPED,
        message="upstream_failed",
    )
    _emit_step(result)
    return result


def _build_sql_runner(
    config_path: str | Path | None,
    observed_at: datetime,
) -> StepRunner:
    providers = tuple(imp_lineage_edge.load_default_providers(config_path))

    def execute(scope: LineageEnvironmentScope) -> object:
        return imp_lineage_edge.run(
            providers,
            config_path=config_path,
            dws_profile=scope.dws_profile,
            store_backend="dws",
            selected_profiles=(scope.sql_source_profile,),
            observed_at=observed_at,
            complete_snapshot=True,
        )

    return execute


def _build_schedule_runner(
    config_path: str | Path | None,
    observed_at: datetime,
) -> StepRunner:
    profiles = tuple(imp_schedule_lineage.load_mysql_process_profiles(config_path))

    def execute(scope: LineageEnvironmentScope) -> object:
        return imp_schedule_lineage.run(
            profiles,
            dws_profile=scope.dws_profile,
            selected_profiles=(scope.schedule_source_profile,),
            observed_at=observed_at,
        )

    return execute


def _build_suppression_runner(observed_at: datetime) -> StepRunner:
    def execute(scope: LineageEnvironmentScope) -> object:
        return imp_lineage_suppression.run(
            (scope,),
            environment=scope.environment,
            observed_at=observed_at,
        )

    return execute


def run(
    scopes: Iterable[LineageEnvironmentScope],
    *,
    environment: str | None = None,
    config_path: str | Path | None = None,
    observed_at: datetime | None = None,
    sql_runner: StepRunner | None = None,
    schedule_runner: StepRunner | None = None,
    suppression_runner: StepRunner | None = None,
) -> LineageDailyResult:
    """Run each enabled scope while isolating failures between environments."""

    started_at = time.perf_counter()
    selected_scopes = _select_scopes(scopes, environment)
    effective_observed_at = observed_at or datetime.now(timezone.utc)
    if (
        effective_observed_at.tzinfo is None
        or effective_observed_at.utcoffset() is None
    ):
        raise ValueError("observed_at must include a timezone offset")

    effective_sql_runner = sql_runner or _build_sql_runner(
        config_path, effective_observed_at
    )
    effective_schedule_runner = schedule_runner or _build_schedule_runner(
        config_path, effective_observed_at
    )
    effective_suppression_runner = suppression_runner or _build_suppression_runner(
        effective_observed_at
    )

    environment_results: list[EnvironmentRunResult] = []
    for scope in selected_scopes:
        sql_result = _execute_step(scope, "SQL", effective_sql_runner)
        schedule_result = _execute_step(scope, "SCHEDULE", effective_schedule_runner)
        suppression_result = (
            _execute_step(scope, "SUPPRESSION", effective_suppression_runner)
            if sql_result.status is StepStatus.SUCCESS
            and schedule_result.status is StepStatus.SUCCESS
            else _skipped_step(scope)
        )
        environment_results.append(
            EnvironmentRunResult(
                environment=scope.environment,
                sql=sql_result,
                schedule=schedule_result,
                suppression=suppression_result,
            )
        )

    result = LineageDailyResult(
        environments=tuple(environment_results),
        elapsed=time.perf_counter() - started_at,
    )
    print(
        "[lineage-daily] summary "
        f"environments={len(result.environments)} "
        f"success={result.success_count} "
        f"failed={result.failed_count} "
        f"elapsed={result.elapsed:.1f}s",
        flush=True,
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run SQL lineage, schedule lineage, and suppression materialization "
            "for configured lineage scopes."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="lineage provider YAML; defaults to local/example loader rules",
    )
    parser.add_argument(
        "--environment",
        default=None,
        metavar="ENVIRONMENT",
        help="only run one enabled environment scope",
    )
    return parser


def _cli_error_code(error: Exception) -> str:
    configured_code = getattr(error, "code", None)
    if isinstance(configured_code, str) and _SAFE_ERROR_CODE.fullmatch(configured_code):
        return configured_code
    return _safe_error_code(error)


def main(
    *,
    config_path: str | Path | None = None,
    environment: str | None = None,
    scopes: Iterable[LineageEnvironmentScope] | None = None,
    observed_at: datetime | None = None,
    sql_runner: StepRunner | None = None,
    schedule_runner: StepRunner | None = None,
    suppression_runner: StepRunner | None = None,
) -> int:
    """Daily CLI boundary; return non-zero when any environment failed."""

    configured_scopes = tuple(
        scopes if scopes is not None else load_lineage_environment_scopes(config_path)
    )
    result = run(
        configured_scopes,
        environment=environment,
        config_path=config_path,
        observed_at=observed_at,
        sql_runner=sql_runner,
        schedule_runner=schedule_runner,
        suppression_runner=suppression_runner,
    )
    return result.exit_code


def cli(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return main(
            config_path=args.config,
            environment=args.environment,
        )
    except Exception as error:  # noqa: BLE001 - emit only a safe error code
        print(f"[lineage-daily] ERROR {_cli_error_code(error)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
