"""构建并发布正式业务血缘的 Phase 5 定时任务入口。

默认 provider 只从公开的 lineage provider 配置读取；测试和 demo 可以直接注入
``ProgramSource`` provider 与 SQLite 路径，不会连接真实环境，也不会改写旧入口。
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
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
from shared.lineage.domain import ProgramSource  # noqa: E402
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


def materialize_sources(
    program_sources: Iterable[ProgramSource],
    *,
    db_path: str | Path = MATERIALIZATION_DB_PATH,
    batch_id: str | None = None,
    observed_at: datetime | None = None,
    job_keys: Mapping[str, str] | None = None,
    store: SQLiteMaterializationStore | None = None,
) -> PublishResult:
    """计算完整 candidate 后交给 SQLite adapter 做 atomic publish。"""

    resolved_batch_id = batch_id if batch_id is not None else new_batch_id()
    resolved_observed_at = (
        observed_at if observed_at is not None else datetime.now(timezone.utc)
    )
    candidate = build_candidate_batch(
        program_sources,
        batch_id=resolved_batch_id,
        observed_at=resolved_observed_at,
        job_keys=job_keys,
    )
    materialization_store = store or SQLiteMaterializationStore(db_path)
    return materialization_store.publish(candidate)


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
) -> int:
    """定时任务边界；异常向外传播并由进程返回 non-zero。"""

    active_providers = (
        tuple(providers) if providers is not None else load_default_providers()
    )
    result = materialize_sources(
        iter_program_sources(active_providers),
        db_path=db_path,
        batch_id=batch_id,
        observed_at=observed_at,
        job_keys=job_keys,
    )
    print(
        "lineage materialization published "
        f"batch_id={result.batch_id} edges={result.edge_count} "
        f"issues={result.issue_count} previous={result.previous_batch_id or '-'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
