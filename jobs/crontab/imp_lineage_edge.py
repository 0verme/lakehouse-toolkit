"""构建并发布正式业务血缘的 Phase 5 定时任务入口。

默认 provider 只从公开的 lineage provider 配置读取；测试和 demo 可以直接注入
``ProgramSource`` provider 与 SQLite 路径，不会连接真实环境，也不会改写旧入口。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import time
from collections.abc import Iterable, Mapping, Sequence, Sized
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

try:
    from _bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from jobs.crontab._bootstrap import ensure_project_root_on_path

# Direct script execution needs the bootstrap before project-local imports.
# ruff: noqa: E402, I001
ensure_project_root_on_path()

from shared.lineage.audit import LineageAuditResult, audit_program_physical_dag  # noqa: E402
from shared.lineage.coverage import (  # noqa: E402
    DEFAULT_COVERAGE_REPORT_PATH,
    LineageCoverageAccumulator,
    write_json_report as write_coverage_json_report,
)
from shared.lineage.domain import (  # noqa: E402
    LineageEdge,
    LineageIssue,
    ProgramIdentity,
    ProgramSource,
)
from shared.lineage.evolution import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    IncrementalPlan,
    IncrementalStatus,
    SnapshotScope,
    build_program_states,
    detect_broken_lineage_branches,
    issue_identity_key,
    plan_incremental,
)
import shared.lineage.materialization as materialization_module  # noqa: E402
from shared.lineage.materialization import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    MaterializationBatch,
    build_materialization_batch,
    new_batch_id,
)
from shared.lineage.materialization_sqlite import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    DEFAULT_MATERIALIZATION_DB_PATH,
    PublishResult,
    SQLiteMaterializationStore,
    SQLitePublishMetrics,
)
from shared.lineage.physical_dag import build_program_physical_dag  # noqa: E402
from shared.lineage.providers import (  # noqa: E402
    MySQLProcessProvider,
    ProgramSourceProvider,
    iter_program_sources,
    load_mysql_process_profiles,
)

_PROVIDER_CONFIG_OVERRIDE = os.getenv("PYTOOLS_LINEAGE_PROVIDER_CONFIG", "").strip()
PROVIDER_CONFIG_PATH = Path(
    _PROVIDER_CONFIG_OVERRIDE or "configs/lineage_providers.local.yaml"
).expanduser()
MATERIALIZATION_DB_PATH = Path(
    os.getenv(
        "PYTOOLS_LINEAGE_MATERIALIZATION_DB",
        str(DEFAULT_MATERIALIZATION_DB_PATH),
    )
).expanduser()
COVERAGE_REPORT_PATH = Path(
    os.getenv(
        "PYTOOLS_LINEAGE_COVERAGE_REPORT",
        str(DEFAULT_COVERAGE_REPORT_PATH),
    )
).expanduser()
DEFAULT_PROGRESS_EVERY = 500
DEFAULT_SLOW_THRESHOLD_MS = 5_000
_SAFE_BATCH_ID = re.compile(r"batch-[A-Za-z0-9][A-Za-z0-9._-]{0,121}")
_SAFE_PROFILE_VALUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


@dataclass(slots=True)
class _ProgramTimingStats:
    slow_threshold_ms: int
    processed: int = 0
    slow_programs: int = 0
    total_program_elapsed_ms: int = 0
    max_program_elapsed_ms: int = 0
    total_program_computation_ms: int = 0
    total_program_materialization_ms: int = 0
    batch_finalize_ms: int = 0
    canonicalization_calls: int = 0
    canonical_json_calls: int = 0
    canonical_json_safe_calls: int = 0
    json_safe_calls: int = 0
    json_dumps_calls: int = 0

    @property
    def avg_program_elapsed_ms(self) -> int:
        if self.processed == 0:
            return 0
        return self.total_program_elapsed_ms // self.processed

    def observe(
        self,
        elapsed_ms: int,
        *,
        dag_ms: int = 0,
        audit_ms: int = 0,
        materialization_ms: int = 0,
    ) -> bool:
        self.processed += 1
        self.total_program_elapsed_ms += elapsed_ms
        self.total_program_computation_ms += dag_ms + audit_ms
        self.total_program_materialization_ms += materialization_ms
        self.max_program_elapsed_ms = max(self.max_program_elapsed_ms, elapsed_ms)
        is_slow = elapsed_ms > self.slow_threshold_ms
        if is_slow:
            self.slow_programs += 1
        return is_slow


@dataclass(frozen=True, slots=True)
class _ProgramStageTiming:
    dag_ms: int
    audit_ms: int


def _emit_log(stage: str, status: str, **fields: object) -> None:
    """输出低基数且已脱敏的阶段日志。"""

    values = [f"stage={stage}", f"status={status}"]
    values.extend(f"{key}={value}" for key, value in fields.items())
    print(" ".join(values), flush=True)


def _elapsed_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _exception_name(error: Exception) -> str:
    """只返回异常 class，避免把 provider 错误文本写入日志。"""

    return type(error).__name__


def _publish_metric_fields(metrics: SQLitePublishMetrics) -> dict[str, int]:
    return {
        "prepare_ms": metrics.prepare_ms,
        "insert_ms": metrics.insert_ms,
        "validate_ms": metrics.validate_ms,
        "active_switch_ms": metrics.active_switch_ms,
        "commit_ms": metrics.commit_ms,
        "prepared_edges": metrics.prepared_edge_rows,
        "prepared_issues": metrics.prepared_issue_rows,
        "prepared_programs": metrics.prepared_program_rows,
        "validated_edges": metrics.validated_edge_rows,
        "validated_issues": metrics.validated_issue_rows,
        "validated_programs": metrics.validated_program_rows,
        "serialization_calls": metrics.evidence_serialization_calls,
    }


def _safe_batch_id(value: str | None) -> str:
    """限制日志中的 batch ID，拒绝任意外部文本。"""

    if not isinstance(value, str):
        return "<redacted>"
    candidate = value.strip()
    return candidate if _SAFE_BATCH_ID.fullmatch(candidate) else "<redacted>"


def _validate_progress_every(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("progress_every must be a positive integer")
    return value


def _validate_limit(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("limit must be a non-negative integer")
    return value


def _validate_slow_threshold_ms(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("slow_threshold_ms must be a non-negative integer")
    return value


def _normalize_selected_profiles(
    profiles: Iterable[str] | str | None,
) -> tuple[str, ...]:
    if profiles is None:
        return ()
    values = (profiles,) if isinstance(profiles, str) else profiles
    try:
        values = tuple(values)
    except TypeError as exc:
        raise ValueError("selected_profiles must be an iterable of strings") from exc
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("selected_profiles must contain non-empty strings")
    return tuple(sorted({value.strip() for value in values}))


def _safe_profile_name(value: str) -> str:
    if not isinstance(value, str):
        return "<redacted>"
    candidate = value.strip()
    return candidate if _SAFE_PROFILE_VALUE.fullmatch(candidate) else "<redacted>"


def _format_selected_profiles(profiles: tuple[str, ...]) -> str:
    if not profiles:
        return "-"
    return ",".join(_safe_profile_name(profile) for profile in profiles)


def _replay_mode(profiles: tuple[str, ...], limit: int | None) -> str:
    if profiles and limit is not None:
        return "controlled_profile_limit"
    if profiles:
        return "controlled_profile"
    if limit is not None:
        return "controlled_limit"
    return "normal"


def _provider_source_profile(provider: object) -> str | None:
    direct_profile = getattr(provider, "source_profile", None)
    profile = getattr(provider, "profile", None)
    candidates = (direct_profile, getattr(profile, "name", None))
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _iter_selected_program_sources(
    providers: Iterable[ProgramSourceProvider],
    selected_profiles: tuple[str, ...],
) -> Iterable[ProgramSource]:
    selected = set(selected_profiles)
    for provider in providers:
        provider_profile = _provider_source_profile(provider)
        if (
            selected
            and provider_profile is not None
            and provider_profile not in selected
        ):
            continue
        for source in provider.iter_program_sources():
            if (
                selected
                and isinstance(source, ProgramSource)
                and source.identity.source_profile not in selected
            ):
                continue
            yield source


def _select_replay_sources(
    program_sources: Iterable[ProgramSource],
    limit: int | None,
) -> tuple[ProgramSource, ...]:
    sources = tuple(program_sources)
    if limit is None:
        return sources
    if any(not isinstance(source, ProgramSource) for source in sources):
        return sources
    return tuple(sorted(sources, key=lambda source: source.identity.key)[:limit])


def _anonymous_program_id(program_source: ProgramSource) -> str:
    identity = "\x1f".join(program_source.identity.key)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]


def _build_program_audit(
    program_source: ProgramSource,
    *,
    observed_at: datetime,
    batch_id: str,
    ordinal: int,
    diagnostic: bool,
    stage_timings: dict[tuple[str, ...], _ProgramStageTiming],
) -> LineageAuditResult:
    program_id = _anonymous_program_id(program_source)
    safe_profile = _safe_profile_name(program_source.source_profile)
    if diagnostic:
        _emit_log(
            "build_program",
            "STARTED",
            program_id=program_id,
            source_profile=safe_profile,
            ordinal=ordinal,
        )
    program_started_at = time.perf_counter()
    try:
        dag_started_at = time.perf_counter()
        dag = build_program_physical_dag(program_source)
        dag_elapsed_ms = _elapsed_ms(dag_started_at)
        audit_started_at = time.perf_counter()
        audit = audit_program_physical_dag(
            dag,
            observed_at=observed_at,
            batch_id=batch_id,
        )
        audit_elapsed_ms = _elapsed_ms(audit_started_at)
    except Exception as error:
        _emit_log(
            "build_program",
            "PATHOLOGICAL",
            program_id=program_id,
            source_profile=safe_profile,
            ordinal=ordinal,
            elapsed_ms=_elapsed_ms(program_started_at),
            exception=_exception_name(error),
        )
        raise
    stage_timings[program_source.identity.key] = _ProgramStageTiming(
        dag_ms=dag_elapsed_ms,
        audit_ms=audit_elapsed_ms,
    )
    return audit


def build_audits(
    program_sources: Iterable[ProgramSource],
    *,
    batch_id: str,
    observed_at: datetime,
    coverage: LineageCoverageAccumulator | None = None,
    count_program_totals: bool = True,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    progress_started_at: float | None = None,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    timing: _ProgramTimingStats | None = None,
    diagnostic: bool = False,
    stage_timings: dict[tuple[str, ...], _ProgramStageTiming] | None = None,
) -> Iterable[LineageAuditResult]:
    """逐个复用 Builder/Auditor，并顺手累加可选 coverage。"""

    progress_every = _validate_progress_every(progress_every)
    _validate_slow_threshold_ms(slow_threshold_ms)
    # ``timing`` remains a compatibility parameter; complete timing is observed
    # after materialization so it includes the stage that previously hid the hot spot.
    del timing
    stage_timings = stage_timings if stage_timings is not None else {}
    total = len(program_sources) if isinstance(program_sources, Sized) else None
    for processed, program_source in enumerate(program_sources, start=1):
        audit = _build_program_audit(
            program_source,
            observed_at=observed_at,
            batch_id=batch_id,
            ordinal=processed,
            diagnostic=diagnostic,
            stage_timings=stage_timings,
        )
        dag = audit.dag
        if coverage is not None:
            coverage.observe_dag(dag, count_program=count_program_totals)
        # consumer 在请求下一个 audit 前会先 materialize 当前结果。
        yield audit
        if (
            progress_started_at is not None
            and total is not None
            and processed < total
            and processed % progress_every == 0
        ):
            _emit_log(
                "build",
                "RUNNING",
                processed=processed,
                total=total,
                percent=processed * 100 // total,
                elapsed_ms=_elapsed_ms(progress_started_at),
            )


def build_candidate_batch(
    program_sources: Iterable[ProgramSource],
    *,
    batch_id: str,
    observed_at: datetime,
    job_keys: Mapping[str, str] | None = None,
    coverage: LineageCoverageAccumulator | None = None,
    count_program_totals: bool = True,
    observe_materialized_edges: bool = True,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    progress_started_at: float | None = None,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    timing: _ProgramTimingStats | None = None,
    diagnostic: bool = False,
) -> MaterializationBatch:
    """完成计算但不写库，返回可校验的 candidate batch。"""

    timing = timing or _ProgramTimingStats(slow_threshold_ms)
    stage_timings: dict[tuple[str, ...], _ProgramStageTiming] = {}

    def observe_program(
        audit: LineageAuditResult,
        result,
        ordinal: int,
        materialization_ms: int,
        error: Exception | None,
    ) -> None:
        program_source = audit.dag.program_source
        program_id = _anonymous_program_id(program_source)
        safe_profile = _safe_profile_name(program_source.source_profile)
        stage_timing = stage_timings.get(program_source.identity.key)
        dag_ms = stage_timing.dag_ms if stage_timing is not None else 0
        audit_ms = stage_timing.audit_ms if stage_timing is not None else 0
        elapsed_ms = dag_ms + audit_ms + materialization_ms
        common_fields = {
            "program_id": program_id,
            "source_profile": safe_profile,
            "ordinal": ordinal,
            "elapsed_ms": elapsed_ms,
            "dag_ms": dag_ms,
            "audit_ms": audit_ms,
            "materialization_ms": materialization_ms,
            # Keep the Phase 7 field names as aliases for existing log consumers.
            "build_program_physical_dag_ms": dag_ms,
            "audit_program_physical_dag_ms": audit_ms,
            "single_program_total_ms": elapsed_ms,
        }
        if error is not None:
            _emit_log(
                "build_program",
                "PATHOLOGICAL",
                **common_fields,
                exception=_exception_name(error),
            )
            return

        is_slow = timing.observe(
            elapsed_ms,
            dag_ms=dag_ms,
            audit_ms=audit_ms,
            materialization_ms=materialization_ms,
        )
        if result is None:
            raise RuntimeError("program observer received no materialization result")
        common_fields.update(
            {
                "physical_nodes": len(result.dag.nodes),
                "physical_edges": len(result.dag.edges),
                "lineage_edges": len(result.edges),
                "issues": len(result.issues),
            }
        )
        if diagnostic:
            _emit_log("build_program", "SUCCESS", **common_fields)
        if is_slow:
            _emit_log("build_program", "SLOW", **common_fields)

    materialization_metrics = materialization_module._MaterializationMetrics()
    with materialization_module._capture_metrics(materialization_metrics):
        candidate = build_materialization_batch(
            build_audits(
                program_sources,
                batch_id=batch_id,
                observed_at=observed_at,
                coverage=coverage,
                count_program_totals=count_program_totals,
                progress_every=progress_every,
                progress_started_at=progress_started_at,
                slow_threshold_ms=slow_threshold_ms,
                diagnostic=diagnostic,
                stage_timings=stage_timings,
            ),
            batch_id=batch_id,
            observed_at=observed_at,
            job_keys=job_keys,
            program_observer=observe_program,
        )
    timing.batch_finalize_ms = materialization_metrics.batch_finalize_ms
    timing.canonical_json_calls = materialization_metrics.canonical_json_calls
    timing.canonical_json_safe_calls = materialization_metrics.canonical_json_safe_calls
    timing.canonicalization_calls = (
        timing.canonical_json_calls + timing.canonical_json_safe_calls
    )
    timing.json_safe_calls = materialization_metrics.json_safe_calls
    timing.json_dumps_calls = materialization_metrics.json_dumps_calls
    if coverage is not None and observe_materialized_edges:
        coverage.observe_materialized_edges(candidate.edges)
    return candidate


def _fact_identity(value: LineageEdge | LineageIssue) -> ProgramIdentity | None:
    if value.program_name is None:
        return None
    return ProgramIdentity(
        value.environment,
        value.source_profile,
        value.program_name,
    )


def _retain_previous_fact(
    value: LineageEdge | LineageIssue,
    plan: IncrementalPlan,
) -> bool:
    identity = _fact_identity(value)
    if identity is None:
        return not (
            plan.complete_snapshot
            and (value.environment, value.source_profile)
            in {scope.key for scope in plan.snapshot_scopes}
        )
    status = plan.status_for(identity)
    if status in (IncrementalStatus.NEW, IncrementalStatus.CHANGED):
        return False
    if status is IncrementalStatus.UNCHANGED:
        return True
    return not (
        plan.complete_snapshot
        and identity.scope in {scope.key for scope in plan.snapshot_scopes}
    )


def _rebase_edge(
    edge: LineageEdge,
    *,
    batch_id: str,
    observed_at: datetime,
) -> LineageEdge:
    return replace(
        edge,
        batch_id=batch_id,
        observed_at=observed_at,
        updated_at=observed_at,
        is_active=True,
    )


def _rebase_issue(
    issue: LineageIssue,
    *,
    batch_id: str,
    observed_at: datetime,
    observed_now: bool,
) -> LineageIssue:
    return replace(
        issue,
        batch_id=batch_id,
        last_seen_at=observed_at if observed_now else issue.last_seen_at,
        is_active=True,
    )


def build_incremental_candidate_batch(
    program_sources: Iterable[ProgramSource],
    *,
    store: SQLiteMaterializationStore,
    batch_id: str,
    observed_at: datetime,
    job_keys: Mapping[str, str] | None = None,
    complete_snapshot: bool = False,
    snapshot_scopes: Iterable[
        SnapshotScope | ProgramIdentity | ProgramSource | tuple[str, str]
    ]
    | None = None,
    coverage: LineageCoverageAccumulator | None = None,
    force_rebuild: bool = False,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    diagnostic: bool = False,
) -> MaterializationBatch:
    """只重建 NEW/CHANGED，并把 candidate 合并成完整 snapshot。"""

    progress_every = _validate_progress_every(progress_every)
    slow_threshold_ms = _validate_slow_threshold_ms(slow_threshold_ms)
    sources = tuple(program_sources)
    if coverage is not None:
        coverage.observe_sources(sources)

    plan_started_at = time.perf_counter()
    _emit_log("incremental_plan", "STARTED")
    try:
        previous_states = store.read_program_states(active_only=True)
        base_plan = plan_incremental(
            sources,
            previous_states,
            complete_snapshot=complete_snapshot,
            snapshot_scopes=snapshot_scopes,
        )
        if force_rebuild:
            plan = IncrementalPlan(
                new=base_plan.new,
                changed=(*base_plan.changed, *base_plan.unchanged),
                deleted=base_plan.deleted,
                complete_snapshot=base_plan.complete_snapshot,
                snapshot_scopes=base_plan.snapshot_scopes,
                pipeline_version=base_plan.pipeline_version,
            )
        else:
            plan = base_plan
    except Exception as error:
        _emit_log(
            "incremental_plan",
            "FAILED",
            exception=_exception_name(error),
            elapsed_ms=_elapsed_ms(plan_started_at),
        )
        raise

    rebuild_sources = plan.rebuild
    _emit_log(
        "incremental_plan",
        "SUCCESS",
        total=len(sources),
        new=len(plan.new),
        changed=len(plan.changed),
        unchanged=len(plan.unchanged),
        deleted=len(plan.deleted),
        rebuild=len(rebuild_sources),
        elapsed_ms=_elapsed_ms(plan_started_at),
    )

    build_started_at = time.perf_counter()
    timing = _ProgramTimingStats(slow_threshold_ms)
    _emit_log(
        "build",
        "STARTED",
        total=len(rebuild_sources),
        slow_threshold_ms=slow_threshold_ms,
    )
    try:
        previous_edges = store.read_edges(active_only=True)
        previous_issues = store.read_issues(active_only=True)
        rebuilt = build_candidate_batch(
            rebuild_sources,
            batch_id=batch_id,
            observed_at=observed_at,
            job_keys=job_keys,
            coverage=coverage,
            count_program_totals=False,
            observe_materialized_edges=False,
            progress_every=progress_every,
            progress_started_at=build_started_at,
            slow_threshold_ms=slow_threshold_ms,
            timing=timing,
            diagnostic=diagnostic,
        )
        candidate_finalize_started_at = time.perf_counter()

        retained_edges = [
            _rebase_edge(edge, batch_id=batch_id, observed_at=observed_at)
            for edge in previous_edges
            if _retain_previous_fact(edge, plan)
        ]
        fresh_edges = list(rebuilt.edges)
        edge_by_identity: dict[tuple[object, ...], LineageEdge] = {}
        for edge in (*retained_edges, *fresh_edges):
            identity = (
                edge.environment,
                edge.source_profile,
                edge.source_table,
                edge.target_table,
                edge.program_name,
                edge.job_key,
            )
            edge_by_identity[identity] = edge

        retained_issues: list[LineageIssue] = []
        for issue in previous_issues:
            if not _retain_previous_fact(issue, plan):
                continue
            identity = _fact_identity(issue)
            observed_now = (
                identity is not None
                and plan.status_for(identity) is IncrementalStatus.UNCHANGED
            )
            retained_issues.append(
                _rebase_issue(
                    issue,
                    batch_id=batch_id,
                    observed_at=observed_at,
                    observed_now=observed_now,
                )
            )
        broken_issues = detect_broken_lineage_branches(
            previous_edges,
            fresh_edges,
            previous_issues,
            rebuilt.issues,
            observed_at=observed_at,
            batch_id=batch_id,
        )
        issue_by_identity = {
            issue_identity_key(issue): issue
            for issue in (*retained_issues, *rebuilt.issues, *broken_issues)
        }
        candidate = MaterializationBatch(
            batch_id=batch_id,
            observed_at=observed_at,
            edges=tuple(
                sorted(
                    edge_by_identity.values(),
                    key=lambda edge: (
                        edge.environment,
                        edge.source_profile,
                        edge.source_table,
                        edge.target_table,
                        edge.program_name or "",
                        edge.job_key or "",
                    ),
                )
            ),
            issues=tuple(sorted(issue_by_identity.values(), key=issue_identity_key)),
            program_states=build_program_states(
                plan,
                previous_states,
                observed_at=observed_at,
                batch_id=batch_id,
            ),
        )
        candidate_finalize_ms = _elapsed_ms(candidate_finalize_started_at)
    except Exception as error:
        _emit_log(
            "build",
            "FAILED",
            exception=_exception_name(error),
            elapsed_ms=_elapsed_ms(build_started_at),
        )
        raise

    _emit_log(
        "build",
        "SUCCESS",
        processed=len(rebuild_sources),
        slow_programs=timing.slow_programs,
        max_program_elapsed_ms=timing.max_program_elapsed_ms,
        avg_program_elapsed_ms=timing.avg_program_elapsed_ms,
        program_computation_ms=timing.total_program_computation_ms,
        program_materialization_ms=timing.total_program_materialization_ms,
        batch_finalize_ms=timing.batch_finalize_ms,
        candidate_finalize_ms=candidate_finalize_ms,
        canonicalization_calls=timing.canonicalization_calls,
        canonical_json_calls=timing.canonical_json_calls,
        canonical_json_safe_calls=timing.canonical_json_safe_calls,
        json_safe_calls=timing.json_safe_calls,
        serialization_calls=timing.json_dumps_calls,
        edges=len(candidate.edges),
        issues=len(candidate.issues),
        elapsed_ms=_elapsed_ms(build_started_at),
    )
    if coverage is not None:
        coverage.observe_materialized_edges(candidate.edges)
    return candidate


def materialize_sources(
    program_sources: Iterable[ProgramSource],
    *,
    db_path: str | Path = MATERIALIZATION_DB_PATH,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_keys: Mapping[str, str] | None = None,
    store: SQLiteMaterializationStore | None = None,
    complete_snapshot: bool = False,
    snapshot_scopes: Iterable[
        SnapshotScope | ProgramIdentity | ProgramSource | tuple[str, str]
    ]
    | None = None,
    coverage: LineageCoverageAccumulator | None = None,
    force_rebuild: bool = False,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    selected_profiles: Iterable[str] | str | None = None,
    limit: int | None = None,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    diagnostic: bool = False,
) -> PublishResult:
    """增量计算完整 candidate，再交给 SQLite adapter 做 atomic publish。

    ``complete_snapshot`` 默认为 False，防止部分 Provider 扫描误报 DELETED；
    定时任务 ``main`` 在所有 provider 成功迭代后显式启用完整 snapshot。
    """

    progress_every = _validate_progress_every(progress_every)
    selected_profiles = _normalize_selected_profiles(selected_profiles)
    limit = _validate_limit(limit)
    slow_threshold_ms = _validate_slow_threshold_ms(slow_threshold_ms)
    controlled_replay = bool(selected_profiles or limit is not None)
    effective_complete_snapshot = complete_snapshot and not controlled_replay
    resolved_batch_id = batch_id if batch_id is not None else new_batch_id()
    resolved_observed_at = (
        observed_at if observed_at is not None else datetime.now(timezone.utc)
    )
    materialization_store = store or SQLiteMaterializationStore(db_path)

    source_started_at = time.perf_counter()
    _emit_log("source_load", "STARTED")
    try:
        loaded_sources = tuple(program_sources)
        if selected_profiles:
            selected = set(selected_profiles)
            loaded_sources = tuple(
                source
                for source in loaded_sources
                if not isinstance(source, ProgramSource)
                or source.identity.source_profile in selected
            )
        sources = _select_replay_sources(loaded_sources, limit)
    except Exception as error:
        _emit_log(
            "source_load",
            "FAILED",
            exception=_exception_name(error),
            elapsed_ms=_elapsed_ms(source_started_at),
        )
        raise
    _emit_log(
        "source_load",
        "SUCCESS",
        sources=len(loaded_sources),
        elapsed_ms=_elapsed_ms(source_started_at),
    )
    _emit_log(
        "replay",
        "SELECTED",
        replay_mode=_replay_mode(selected_profiles, limit),
        selected_profiles=_format_selected_profiles(selected_profiles),
        source_total=len(loaded_sources),
        replay_total=len(sources),
        limit="-" if limit is None else limit,
        force_rebuild=bool(force_rebuild),
        partial_snapshot=not effective_complete_snapshot,
    )

    candidate = build_incremental_candidate_batch(
        sources,
        store=materialization_store,
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
        job_keys=job_keys,
        complete_snapshot=effective_complete_snapshot,
        snapshot_scopes=snapshot_scopes,
        coverage=coverage,
        force_rebuild=force_rebuild,
        progress_every=progress_every,
        slow_threshold_ms=slow_threshold_ms,
        diagnostic=diagnostic,
    )

    publish_started_at = time.perf_counter()
    publish_metrics = SQLitePublishMetrics()
    _emit_log("publish", "STARTED")
    try:
        result = materialization_store.publish(
            candidate,
            instrumentation=publish_metrics,
        )
    except Exception as error:
        _emit_log(
            "publish",
            "FAILED",
            exception=_exception_name(error),
            **_publish_metric_fields(publish_metrics),
            elapsed_ms=_elapsed_ms(publish_started_at),
        )
        raise
    _emit_log(
        "publish",
        "SUCCESS",
        batch_id=_safe_batch_id(result.batch_id),
        edges=result.edge_count,
        issues=result.issue_count,
        previous=_safe_batch_id(result.previous_batch_id)
        if result.previous_batch_id
        else "-",
        **_publish_metric_fields(publish_metrics),
        elapsed_ms=_elapsed_ms(publish_started_at),
    )
    return result


def _provider_snapshot_scopes(
    providers: Iterable[ProgramSourceProvider],
) -> tuple[SnapshotScope, ...]:
    scopes: set[SnapshotScope] = set()
    for provider in providers:
        environment = getattr(provider, "environment", None)
        source_profile = getattr(provider, "source_profile", None)
        profile = getattr(provider, "profile", None)
        if profile is not None:
            environment = environment or getattr(profile, "environment", None)
            source_profile = source_profile or getattr(profile, "name", None)
        if (
            isinstance(environment, str)
            and isinstance(source_profile, str)
            and environment.strip()
            and source_profile.strip()
        ):
            scopes.add(SnapshotScope(environment, source_profile))
    return tuple(sorted(scopes, key=lambda scope: scope.key))


def load_default_providers(
    config_path: str | Path | None = None,
) -> tuple[ProgramSourceProvider, ...]:
    """从 local/example 配置创建 DEV MySQL provider；不在 import 时连接数据库。"""

    selected_path = (
        config_path
        if config_path is not None
        else (PROVIDER_CONFIG_PATH if _PROVIDER_CONFIG_OVERRIDE else None)
    )
    profiles = load_mysql_process_profiles(selected_path)
    return tuple(MySQLProcessProvider(profile) for profile in profiles)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Build lineage facts and emit a sanitized parser coverage report.")
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=MATERIALIZATION_DB_PATH,
        help="local SQLite materialization path",
    )
    parser.add_argument(
        "--coverage-report",
        type=Path,
        default=COVERAGE_REPORT_PATH,
        help="sanitized JSON report under artifacts/lineage_coverage/",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help=(
            "reparse every current ProgramSource instead of relying on source hashes; "
            "use for a controlled coverage replay"
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="emit build progress every N rebuilt programs",
    )
    parser.add_argument(
        "--profile",
        action="append",
        metavar="SOURCE_PROFILE",
        help="replay only this source_profile; repeat for multiple profiles",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="replay at most N programs after deterministic identity sorting",
    )
    parser.add_argument(
        "--slow-threshold-ms",
        type=int,
        default=DEFAULT_SLOW_THRESHOLD_MS,
        metavar="MS",
        help="log per-program SLOW timing only when total elapsed time exceeds MS",
    )
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="emit per-program STARTED/SUCCESS diagnostics in addition to slow logs",
    )
    return parser


def cli(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return main(
        db_path=args.db_path,
        coverage_report_path=args.coverage_report,
        force_rebuild=args.force_rebuild,
        progress_every=args.progress_every,
        selected_profiles=args.profile,
        limit=args.limit,
        slow_threshold_ms=args.slow_threshold_ms,
        diagnostic=args.diagnostic,
    )


def main(
    providers: Iterable[ProgramSourceProvider] | None = None,
    *,
    db_path: str | Path = MATERIALIZATION_DB_PATH,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_keys: Mapping[str, str] | None = None,
    complete_snapshot: bool = True,
    snapshot_scopes: Iterable[
        SnapshotScope | ProgramIdentity | ProgramSource | tuple[str, str]
    ]
    | None = None,
    coverage_report_path: str | Path | None = COVERAGE_REPORT_PATH,
    force_rebuild: bool = False,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    selected_profiles: Iterable[str] | str | None = None,
    limit: int | None = None,
    slow_threshold_ms: int = DEFAULT_SLOW_THRESHOLD_MS,
    diagnostic: bool = False,
) -> int:
    """定时任务边界；异常向外传播并由进程返回 non-zero。"""

    progress_every = _validate_progress_every(progress_every)
    selected_profiles = _normalize_selected_profiles(selected_profiles)
    limit = _validate_limit(limit)
    slow_threshold_ms = _validate_slow_threshold_ms(slow_threshold_ms)
    controlled_replay = bool(selected_profiles or limit is not None)
    effective_complete_snapshot = complete_snapshot and not controlled_replay
    job_started_at = time.perf_counter()
    coverage = LineageCoverageAccumulator()
    try:
        active_providers = (
            tuple(providers) if providers is not None else load_default_providers()
        )
        _emit_log(
            "job",
            "STARTED",
            providers=len(active_providers),
            replay_mode=_replay_mode(selected_profiles, limit),
            selected_profiles=_format_selected_profiles(selected_profiles),
            force_rebuild=bool(force_rebuild),
            partial_snapshot=not effective_complete_snapshot,
            diagnostic=bool(diagnostic),
        )
        resolved_scopes = (
            tuple(snapshot_scopes)
            if snapshot_scopes is not None
            else _provider_snapshot_scopes(active_providers)
        )
        source_iterator = (
            _iter_selected_program_sources(active_providers, selected_profiles)
            if selected_profiles
            else iter_program_sources(active_providers)
        )
        result = materialize_sources(
            source_iterator,
            db_path=db_path,
            batch_id=batch_id,
            observed_at=observed_at,
            job_keys=job_keys,
            complete_snapshot=effective_complete_snapshot,
            snapshot_scopes=resolved_scopes or None,
            coverage=coverage,
            force_rebuild=force_rebuild,
            progress_every=progress_every,
            selected_profiles=selected_profiles,
            limit=limit,
            slow_threshold_ms=slow_threshold_ms,
            diagnostic=diagnostic,
        )
    except Exception as error:
        _emit_log(
            "job",
            "FAILED",
            exception=_exception_name(error),
            elapsed_ms=_elapsed_ms(job_started_at),
        )
        raise

    coverage_report = coverage.report()
    if coverage_report_path is not None:
        try:
            write_coverage_json_report(coverage_report, coverage_report_path)
        except (OSError, ValueError):
            _emit_log("coverage", "FAILED", reason="REPORT_WRITE_FAILED")
    for coverage_line in coverage_report.log_lines():
        print(coverage_line, flush=True)
    _emit_log(
        "job",
        "SUCCESS",
        batch_id=_safe_batch_id(result.batch_id),
        edges=result.edge_count,
        issues=result.issue_count,
        elapsed_ms=_elapsed_ms(job_started_at),
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(cli())
    except Exception:
        raise SystemExit(1) from None
