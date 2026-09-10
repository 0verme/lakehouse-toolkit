"""将开发环境 MySQL 调度配置事实发布到 DWS。"""

from __future__ import annotations

import argparse
import os
import re
import time
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from jobs.crontab._bootstrap import ensure_project_root_on_path

# Direct script execution needs the bootstrap before project-local imports.
# ruff: noqa: E402, I001
ensure_project_root_on_path()

from shared.lineage.materialization import new_batch_id  # noqa: E402
from shared.lineage.providers import (  # noqa: E402
    MySQLProcessProfile,
    load_mysql_process_profiles,
)
from shared.lineage.schedule import (  # noqa: E402
    MySQLScheduleLineageProvider,
    ScheduleLineageEdge,
    ScheduleLineageLoadResult,
    deduplicate_schedule_edges,
)
from shared.lineage.schedule_materialization import (  # noqa: E402
    DWSScheduleLineageStore,
    DWSSchedulePublishMetrics,
    DWSSchedulePublishResult,
)

_PROVIDER_CONFIG_OVERRIDE = os.getenv("PYTOOLS_LINEAGE_PROVIDER_CONFIG", "").strip()
PROVIDER_CONFIG_PATH = Path(
    _PROVIDER_CONFIG_OVERRIDE or "configs/lineage_providers.local.yaml"
).expanduser()
_SAFE_BATCH_ID = re.compile(r"batch-[A-Za-z0-9][A-Za-z0-9._-]{0,121}")
_SAFE_PROFILE_VALUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


def _emit_log(stage: str, status: str, **fields: object) -> None:
    values = [f"stage={stage}", f"status={status}"]
    values.extend(f"{key}={value}" for key, value in fields.items())
    print(" ".join(values), flush=True)


def _elapsed_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _exception_name(error: Exception) -> str:
    return type(error).__name__


def _safe_profile(value: str) -> str:
    candidate = value.strip() if isinstance(value, str) else ""
    return candidate if _SAFE_PROFILE_VALUE.fullmatch(candidate) else "<redacted>"


def _safe_batch_id(value: str | None) -> str:
    candidate = value.strip() if isinstance(value, str) else ""
    return candidate if _SAFE_BATCH_ID.fullmatch(candidate) else "<redacted>"


def _normalize_profiles(
    profiles: Iterable[str] | str | None,
) -> tuple[str, ...]:
    if profiles is None:
        return ()
    values = (profiles,) if isinstance(profiles, str) else tuple(profiles)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("profile selection must contain non-empty strings")
    return tuple(sorted({value.strip() for value in values}))


def _validate_limit(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("limit must be a non-negative integer")
    return value


def _select_profiles(
    profiles: Iterable[MySQLProcessProfile],
    selected_names: tuple[str, ...],
) -> tuple[MySQLProcessProfile, ...]:
    configured = tuple(profiles)
    if any(not isinstance(profile, MySQLProcessProfile) for profile in configured):
        raise TypeError("profiles must contain MySQLProcessProfile values")
    by_name = {profile.name: profile for profile in configured}
    selected = (
        configured
        if not selected_names
        else tuple(by_name[name] for name in selected_names if name in by_name)
    )
    missing = tuple(name for name in selected_names if name not in by_name)
    if missing:
        raise ValueError("selected schedule profile was not found")
    enabled = tuple(
        profile
        for profile in selected
        if profile.schedule_lineage is not None and profile.schedule_lineage.enabled
    )
    if selected_names and len(enabled) != len(selected_names):
        raise ValueError("selected profile has schedule_lineage disabled")
    if not enabled:
        raise ValueError("no enabled DEV schedule profiles are configured")
    return enabled


def _emit_source_result(
    profile: MySQLProcessProfile,
    result: ScheduleLineageLoadResult,
) -> None:
    stats = result.stats
    _emit_log(
        "source_load",
        "SUCCESS",
        profile=_safe_profile(profile.name),
        rows=stats.source_rows,
        accepted=stats.accepted_rows,
        rejected=stats.rejected_rows,
        normalized_edges=stats.normalized_edges,
        deduplicated_edges=stats.deduplicated_edges,
        selected_edges=stats.selected_edges,
    )


def _publish_metric_fields(metrics: DWSSchedulePublishMetrics) -> dict[str, int]:
    return {
        "prepare_ms": metrics.prepare_ms,
        "insert_ms": metrics.insert_ms,
        "validate_ms": metrics.validate_ms,
        "active_switch_ms": metrics.active_switch_ms,
        "commit_ms": metrics.commit_ms,
        "prepared_edges": metrics.prepared_edge_rows,
        "validated_edges": metrics.validated_edge_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish DEV MySQL configured schedule lineage to DWS."
    )
    parser.add_argument(
        "--profile",
        action="append",
        metavar="SOURCE_PROFILE",
        help="only replay this mysql_process_profile; repeat for multiple profiles",
    )
    parser.add_argument(
        "--dws-profile",
        default=os.getenv("PYTOOLS_LINEAGE_DWS_PROFILE") or None,
        metavar="DWS_PROFILE",
        help="database profile used by the existing DWS connection boundary",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="lineage provider YAML; defaults to local/example loader rules",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="per-profile partial replay limit; never grants deletion authority",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        metavar="BATCH_ID",
        help="optional deterministic test/replay batch id",
    )
    return parser


def run(
    profiles: Iterable[MySQLProcessProfile],
    *,
    dws_profile: str | None,
    selected_profiles: Iterable[str] | str | None = None,
    limit: int | None = None,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    provider_factory: Any = MySQLScheduleLineageProvider,
    store: DWSScheduleLineageStore | None = None,
) -> DWSSchedulePublishResult:
    selected_names = _normalize_profiles(selected_profiles)
    limit = _validate_limit(limit)
    active_profiles = _select_profiles(profiles, selected_names)
    source_edges: list[ScheduleLineageEdge] = []
    scopes: list[tuple[str, str]] = []
    aggregate = {
        "source_rows": 0,
        "accepted": 0,
        "rejected": 0,
        "normalized_edges": 0,
    }

    for profile in active_profiles:
        provider = provider_factory(profile)
        result = provider.load(limit=limit)
        _emit_source_result(profile, result)
        stats = result.stats
        stats_fields = {
            "source_rows": "source_rows",
            "accepted": "accepted_rows",
            "rejected": "rejected_rows",
            "normalized_edges": "normalized_edges",
        }
        for field_name, stats_field in stats_fields.items():
            aggregate[field_name] += getattr(stats, stats_field)
        if not stats.source_complete:
            _emit_log(
                "filter",
                "FAILED",
                profile=_safe_profile(profile.name),
                invalid_rows=stats.invalid_rows,
                reason="SOURCE_ROWS_INVALID",
            )
            raise RuntimeError("schedule source contains invalid rows")
        source_edges.extend(result.edges)
        scopes.append((profile.environment, profile.name))

    edges = deduplicate_schedule_edges(source_edges)
    _emit_log(
        "filter",
        "SUCCESS",
        profiles=len(active_profiles),
        accepted=aggregate["accepted"],
        rejected=aggregate["rejected"],
        normalized_edges=aggregate["normalized_edges"],
        deduplicated_edges=len(edges),
        partial_replay=limit is not None,
    )

    resolved_batch_id = batch_id or new_batch_id()
    resolved_observed_at = observed_at or datetime.now(timezone.utc)
    writer = store or DWSScheduleLineageStore(profile=dws_profile)
    metrics = DWSSchedulePublishMetrics()
    publish_started = time.perf_counter()
    _emit_log("publish", "STARTED", edges=len(edges))
    try:
        published = writer.publish(
            edges,
            batch_id=resolved_batch_id,
            observed_at=resolved_observed_at,
            complete_snapshot=limit is None,
            snapshot_scopes=scopes,
            instrumentation=metrics,
        )
    except Exception as error:
        _emit_log(
            "publish",
            "FAILED",
            exception=_exception_name(error),
            **_publish_metric_fields(metrics),
            elapsed_ms=_elapsed_ms(publish_started),
        )
        raise
    _emit_log(
        "publish",
        "SUCCESS",
        edges=published.edge_count,
        batch_id=_safe_batch_id(published.batch_id),
        previous=(
            _safe_batch_id(published.previous_batch_id)
            if published.previous_batch_id
            else "-"
        ),
        **_publish_metric_fields(metrics),
        elapsed_ms=_elapsed_ms(publish_started),
    )
    return published


def main(
    *,
    config_path: str | Path | None = None,
    dws_profile: str | None = None,
    selected_profiles: Iterable[str] | str | None = None,
    limit: int | None = None,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    profiles: Iterable[MySQLProcessProfile] | None = None,
    store: DWSScheduleLineageStore | None = None,
) -> int:
    started = time.perf_counter()
    try:
        configured_profiles = tuple(
            profiles
            if profiles is not None
            else load_mysql_process_profiles(config_path)
        )
        names = _normalize_profiles(selected_profiles)
        _emit_log(
            "job",
            "STARTED",
            profiles=len(configured_profiles),
            selected_profiles=",".join(_safe_profile(name) for name in names) or "-",
            partial_replay=limit is not None,
        )
        result = run(
            configured_profiles,
            dws_profile=dws_profile,
            selected_profiles=names,
            limit=limit,
            batch_id=batch_id,
            observed_at=observed_at,
            store=store,
        )
    except Exception as error:
        _emit_log(
            "job",
            "FAILED",
            exception=_exception_name(error),
            elapsed_ms=_elapsed_ms(started),
        )
        raise
    _emit_log(
        "job",
        "SUCCESS",
        edges=result.edge_count,
        batch_id=_safe_batch_id(result.batch_id),
        elapsed_ms=_elapsed_ms(started),
    )
    return 0


def cli(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return main(
        config_path=args.config,
        dws_profile=args.dws_profile,
        selected_profiles=args.profile,
        limit=args.limit,
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(cli())
    except Exception:
        raise SystemExit(1) from None
