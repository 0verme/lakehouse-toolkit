"""构建并发布正式业务血缘的 Phase 5 定时任务入口。

默认 provider 只从公开的 lineage provider 配置读取；测试和 demo 可以直接注入
``ProgramSource`` provider 与 SQLite 路径，不会连接真实环境，也不会改写旧入口。
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import replace
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
from shared.lineage.materialization import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    MaterializationBatch,
    build_materialization_batch,
    new_batch_id,
)
from shared.lineage.materialization_sqlite import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    DEFAULT_MATERIALIZATION_DB_PATH,
    PublishResult,
    SQLiteMaterializationStore,
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


def build_audits(
    program_sources: Iterable[ProgramSource],
    *,
    batch_id: str,
    observed_at: datetime,
) -> Iterable[LineageAuditResult]:
    """只编排既有 Builder 与 Auditor，逐个产生 audited DAG。"""

    for program_source in program_sources:
        dag = build_program_physical_dag(program_source)
        yield audit_program_physical_dag(
            dag,
            observed_at=observed_at,
            batch_id=batch_id,
        )


def build_candidate_batch(
    program_sources: Iterable[ProgramSource],
    *,
    batch_id: str,
    observed_at: datetime,
    job_keys: Mapping[str, str] | None = None,
) -> MaterializationBatch:
    """完成计算但不写库，返回可校验的 candidate batch。"""

    return build_materialization_batch(
        build_audits(
            program_sources,
            batch_id=batch_id,
            observed_at=observed_at,
        ),
        batch_id=batch_id,
        observed_at=observed_at,
        job_keys=job_keys,
    )


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
) -> MaterializationBatch:
    """只重建 NEW/CHANGED，并把 candidate 合并成完整 snapshot。"""

    sources = tuple(program_sources)
    previous_states = store.read_program_states(active_only=True)
    plan = plan_incremental(
        sources,
        previous_states,
        complete_snapshot=complete_snapshot,
        snapshot_scopes=snapshot_scopes,
    )
    previous_edges = store.read_edges(active_only=True)
    previous_issues = store.read_issues(active_only=True)
    rebuilt = build_candidate_batch(
        plan.rebuild,
        batch_id=batch_id,
        observed_at=observed_at,
        job_keys=job_keys,
    )

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
    return MaterializationBatch(
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
) -> PublishResult:
    """增量计算完整 candidate，再交给 SQLite adapter 做 atomic publish。

    ``complete_snapshot`` 默认为 False，防止部分 Provider 扫描误报 DELETED；
    定时任务 ``main`` 在所有 provider 成功迭代后显式启用完整 snapshot。
    """

    resolved_batch_id = batch_id if batch_id is not None else new_batch_id()
    resolved_observed_at = (
        observed_at if observed_at is not None else datetime.now(timezone.utc)
    )
    materialization_store = store or SQLiteMaterializationStore(db_path)
    candidate = build_incremental_candidate_batch(
        tuple(program_sources),
        store=materialization_store,
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
        job_keys=job_keys,
        complete_snapshot=complete_snapshot,
        snapshot_scopes=snapshot_scopes,
    )
    return materialization_store.publish(candidate)


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
) -> int:
    """定时任务边界；异常向外传播并由进程返回 non-zero。"""

    active_providers = (
        tuple(providers) if providers is not None else load_default_providers()
    )
    resolved_scopes = (
        tuple(snapshot_scopes)
        if snapshot_scopes is not None
        else _provider_snapshot_scopes(active_providers)
    )
    result = materialize_sources(
        iter_program_sources(active_providers),
        db_path=db_path,
        batch_id=batch_id,
        observed_at=observed_at,
        job_keys=job_keys,
        complete_snapshot=complete_snapshot,
        snapshot_scopes=resolved_scopes or None,
    )
    print(
        "lineage materialization published "
        f"batch_id={result.batch_id} edges={result.edge_count} "
        f"issues={result.issue_count} previous={result.previous_batch_id or '-'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
