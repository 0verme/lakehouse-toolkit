"""查询单个作业的 SQL 依赖与配置依赖。"""

from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.text.regex import (
    extract_tables,
    find_dot_strings,
    get_yilai,
    read_data_from_file,
)
from shared.ui.pywebio_helper import (
    put_black_text,
    run_for_multiline_input,
    start_pywebio_app,
)

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
WORKSPACE_ROOT = Path(
    os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
).expanduser()
JOBS_TABLE = metadata_table("jobs", "jobs")
PROGRAMS_TABLE = metadata_table("programs", "programs")


def main_job(job_name: str):
    pywebio_output = import_module("pywebio.output")
    put_table = pywebio_output.put_table
    put_text = pywebio_output.put_text
    sql = """SELECT
        j.job_name,
        p.file_path,
        p.target_table,
        COALESCE(j.dependency_text, ''),
        j.plan_name,
        j.description
    FROM __JOBS_TABLE__ j
    LEFT JOIN __PROGRAMS_TABLE__ p ON j.program_name = p.program_name
    WHERE j.job_name = ?""".replace("__JOBS_TABLE__", JOBS_TABLE).replace(
        "__PROGRAMS_TABLE__", PROGRAMS_TABLE
    )
    result = select_sql_with_profile(PROFILE, sql, (job_name,)) or []
    if not result:
        put_text(job_name + " 未找到对应依赖")
        return

    row = result[0]
    file_path = Path(str(row[1] or "").replace("$", str(WORKSPACE_ROOT)))
    configured_jobs = sorted(get_yilai(row[3]))

    program_sql = (
        (
            "SELECT target_table, job_name FROM __JOBS_TABLE__ j "
            "LEFT JOIN __PROGRAMS_TABLE__ p ON j.program_name = p.program_name"
        )
        .replace("__JOBS_TABLE__", JOBS_TABLE)
        .replace("__PROGRAMS_TABLE__", PROGRAMS_TABLE)
    )
    program_job = select_sql_with_profile(PROFILE, program_sql) or []
    program_job_map = {}
    for table_name, mapped_job in program_job:
        if table_name:
            program_job_map.setdefault(str(table_name), []).append(str(mapped_job))

    plan_sql = "select plan_name, job_name from __JOBS_TABLE__".replace(
        "__JOBS_TABLE__", JOBS_TABLE
    )
    plan_job = select_sql_with_profile(PROFILE, plan_sql) or []
    plan_map = {str(job): str(plan) for plan, job in plan_job}

    content = read_data_from_file(file_path)[1000:].replace("\n", "")
    sql_tables = sorted(set(extract_tables(content) + find_dot_strings(content)))
    actual_jobs = sorted(
        {
            mapped_job
            for table_name in sql_tables
            for mapped_job in program_job_map.get(table_name, [])
            if mapped_job != job_name
        }
    )

    put_black_text("依赖分析结果")
    put_black_text("作业名: " + job_name)
    put_black_text("目标表: " + str(row[2] or ""))
    put_black_text("备注: " + str(row[5] or ""))
    put_table(
        [
            ["SQL 实际调用", "配置依赖"],
            ["\n".join(actual_jobs), "\n".join(configured_jobs)],
        ]
    )
    put_table(
        [
            ["SQL 实际调用对应计划", "配置依赖对应计划"],
            [
                "\n".join(plan_map.get(job, "") for job in actual_jobs),
                "\n".join(plan_map.get(job, "") for job in configured_jobs),
            ],
        ]
    )


def main():
    run_for_multiline_input(
        "请输入作业名称，每行一个", lambda item: main_job(item.strip().upper())
    )


if __name__ == "__main__":
    start_pywebio_app("作业依赖分析", main)
