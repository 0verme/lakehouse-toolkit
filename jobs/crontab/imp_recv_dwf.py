"""根据 demo 作业依赖重建结果表接入映射。"""

from __future__ import annotations

import os
import re

from shared.config.env import safe_identifier
from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import run_sql_with_profile, select_sql_with_profile
from shared.graph.dependency import (
    build_reverse_dependency_graph,
    find_all_dependent_jobs,
)

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
PLANS_TABLE = metadata_table("receive_plans", "receive_plans")
JOBS_TABLE = metadata_table("jobs", "jobs")
PROGRAMS_TABLE = metadata_table("programs", "programs")
RECEIPTS_TABLE = safe_identifier(
    metadata_table("result_receipts", "result_receipts"), "result table"
)
SCHKEY_PATTERN = re.compile(r"(?:^|\|)\s*-schkey:2:schkey=([^:|]*)(?::0)?(?=\||$)")


def extract_schkey(param_text) -> str:
    match = SCHKEY_PATTERN.search(str(param_text or ""))
    return match.group(1).strip() if match else ""


def rebuild_result_receipts() -> int:
    plans = (
        select_sql_with_profile(
            PROFILE,
            "select plan_name, source_system from __PLANS_TABLE__".replace(
                "__PLANS_TABLE__", PLANS_TABLE
            ),
        )
        or []
    )
    job_rows = (
        select_sql_with_profile(
            PROFILE,
            "select job_name, dependency_text, plan_name, event_text from __JOBS_TABLE__".replace(
                "__JOBS_TABLE__", JOBS_TABLE
            ),
        )
        or []
    )
    program_rows = (
        select_sql_with_profile(
            PROFILE,
            "select program_name, target_table from __PROGRAMS_TABLE__".replace(
                "__PROGRAMS_TABLE__", PROGRAMS_TABLE
            ),
        )
        or []
    )
    job_to_table = {
        str(program): str(table) for program, table in program_rows if program and table
    }
    dependency_pairs = [(str(row[0]), str(row[1] or "")) for row in job_rows if row]
    graph = build_reverse_dependency_graph(dependency_pairs)
    insert_rows = []
    for receive_plan, source_system in plans:
        for job_name, _dependency_text, plan_name, event_text in job_rows:
            if str(plan_name or "") != str(receive_plan):
                continue
            source_job = str(job_name)
            for dependent_job, _level in find_all_dependent_jobs(source_job, graph):
                target_table = job_to_table.get(dependent_job, "")
                if not target_table or not target_table.upper().startswith("DWF."):
                    continue
                insert_rows.append(
                    (
                        receive_plan,
                        target_table,
                        extract_schkey(event_text),
                        source_job,
                        dependent_job,
                        source_system,
                    )
                )

    run_sql_with_profile(
        PROFILE,
        "truncate table __RECEIPTS_TABLE__;".replace(
            "__RECEIPTS_TABLE__", RECEIPTS_TABLE
        ),
    )
    insert_sql = (
        "insert into __RECEIPTS_TABLE__ "
        "(receive_plan, table_name, data_source, receive_job_name, source_job_name, source_system) "
        "values (?, ?, ?, ?, ?, ?)"
    ).replace("__RECEIPTS_TABLE__", RECEIPTS_TABLE)
    for row in insert_rows:
        run_sql_with_profile(PROFILE, insert_sql, row)
    return len(insert_rows)


if __name__ == "__main__":
    print(f"重建结果映射数: {rebuild_result_receipts()}")
