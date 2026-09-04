"""根据计划搜索虚构数据层的下游作业。"""

from __future__ import annotations

import os

from pywebio.output import put_text

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.graph.dependency import (
    build_reverse_dependency_graph,
    find_all_dependent_jobs,
)
from shared.ui.pywebio_helper import (
    multiline_entries,
    put_red_text,
    safe_put_error,
    start_pywebio_app,
)

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
JOBS_TABLE = metadata_table("jobs", "jobs")
PROGRAMS_TABLE = metadata_table("programs", "programs")


def yilai():
    rows = (
        select_sql_with_profile(
            PROFILE,
            "select job_name, dependency_text, plan_name from __JOBS_TABLE__".replace(
                "__JOBS_TABLE__", JOBS_TABLE
            ),
        )
        or []
    )
    jobs = [(str(row[0]), str(row[1] or "")) for row in rows]
    job_plan = {str(row[0]): str(row[2] or "") for row in rows}
    table_rows = (
        select_sql_with_profile(
            PROFILE,
            "select target_table, program_name from __PROGRAMS_TABLE__".replace(
                "__PROGRAMS_TABLE__", PROGRAMS_TABLE
            ),
        )
        or []
    )
    job_table = {str(row[1]): str(row[0]) for row in table_rows if len(row) > 1}
    graph = build_reverse_dependency_graph(jobs)

    for plan_name in multiline_entries("请输入计划名，每行一个"):
        try:
            start_jobs = [
                job
                for job, plan in job_plan.items()
                if plan.upper() == plan_name.upper()
            ]
            put_red_text("=" * 66)
            put_text(f"{plan_name} 对应的下游表如下")
            found_tables = []
            for start_job in start_jobs:
                for job, _level in find_all_dependent_jobs(start_job, graph):
                    if job_plan.get(job, "").upper().endswith(
                        "EXPORT"
                    ) and job_table.get(job):
                        found_tables.append(job_table[job])
            for table_name in sorted(set(found_tables)):
                put_text(table_name)
            put_red_text(
                "========================== 结果结束 =============================="
            )
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("下游依赖搜索", yilai)
