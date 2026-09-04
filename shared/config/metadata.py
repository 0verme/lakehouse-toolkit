from __future__ import annotations

from .env import metadata_table

# 这是公开 demo metadata model 的逻辑名称到表名映射。
# 部署到其他环境时，可通过 PYTOOLS_METADATA_*_TABLE 环境变量覆盖。
TABLES = {
    "term_roots": "term_roots",
    "app_users": "app_users",
    "reference_tables": "reference_tables",
    "receive_plans": "receive_plans",
    "job_outputs": "job_outputs",
    "result_receipts": "result_receipts",
    "jobs": "jobs",
    "programs": "programs",
    "roles": "roles",
    "reports": "reports",
    "partitions": "partitions",
    "processes": "processes",
    "relations": "relations",
    "runtimes": "runtimes",
    "send_jobs": "send_jobs",
    "migration_queue": "migration_queue",
    "schema_config": "schema_config",
}


def table(key: str, default_table: str | None = None) -> str:
    if default_table is None:
        try:
            default_table = TABLES[key]
        except KeyError as exc:
            raise KeyError(f"Unknown metadata table key: {key}") from exc
    return metadata_table(key, default_table)
