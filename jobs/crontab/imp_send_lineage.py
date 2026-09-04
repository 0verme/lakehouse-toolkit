"""从 demo 作业依赖生成发送链路索引。"""

from __future__ import annotations

import os

from shared.config.env import safe_identifier
from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import run_sql_with_profile, select_sql_with_profile

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
JOBS_TABLE = metadata_table("jobs", "jobs")
OUTPUT_TABLE = safe_identifier(
    os.getenv(
        "PYTOOLS_SEND_LINEAGE_TABLE", metadata_table("send_jobs", "send_lineage")
    ),
    "PYTOOLS_SEND_LINEAGE_TABLE",
)


def get_jobs(dependencies) -> list[str]:
    return [
        item[3:].strip()
        for item in str(dependencies or "").split("|")
        if item.startswith("33:") and item[3:].strip()
    ]


def rebuild_send_lineage() -> int:
    rows = (
        select_sql_with_profile(
            PROFILE,
            "select job_name, dependency_text, plan_name from __JOBS_TABLE__".replace(
                "__JOBS_TABLE__", JOBS_TABLE
            ),
        )
        or []
    )
    job_info = {
        str(row[0]): (str(row[1] or ""), str(row[2] or "")) for row in rows if row
    }
    insert_rows = []
    for send_job, (dependencies, send_plan) in job_info.items():
        if "SEND" not in send_job.upper():
            continue
        unload_jobs = get_jobs(dependencies) or [""]
        for unload_job in unload_jobs:
            process_jobs = get_jobs(job_info.get(unload_job, ("", ""))[0]) or [""]
            for process_job in process_jobs:
                insert_rows.append(
                    (
                        send_job,
                        unload_job,
                        process_job,
                        send_plan,
                        job_info.get(unload_job, ("", ""))[1],
                        job_info.get(process_job, ("", ""))[1],
                    )
                )

    run_sql_with_profile(
        PROFILE,
        "truncate table __OUTPUT_TABLE__;".replace("__OUTPUT_TABLE__", OUTPUT_TABLE),
    )
    insert_sql = (
        "insert into __OUTPUT_TABLE__ "
        "(send_job_name, unload_job_name, process_job_name, send_plan_name, unload_plan_name, process_plan_name) "
        "values (?, ?, ?, ?, ?, ?)"
    ).replace("__OUTPUT_TABLE__", OUTPUT_TABLE)
    for row in insert_rows:
        run_sql_with_profile(PROFILE, insert_sql, row)
    return len(insert_rows)


if __name__ == "__main__":
    print(f"发送链路索引数: {rebuild_send_lineage()}")
