"""查询 demo metadata 中的任务与报表依赖。"""

from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.lineage.asset_tables import PLAN_LABELS, load_asset_plan_map
from shared.text.regex import extract_tables, find_dot_strings, read_data_from_file
from shared.ui.pywebio_helper import (
    put_black_text,
    put_red_text,
    run_for_multiline_input,
    start_pywebio_app,
)

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
WORKSPACE_ROOT = Path(
    os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
).expanduser()
JOBS_TABLE = metadata_table("jobs", "jobs")
PROGRAMS_TABLE = metadata_table("programs", "programs")
REPORTS_TABLE = metadata_table("reports", "reports")


def build_program_job_map():
    result = {}
    program_sql = (
        (
            "select target_table, job_name from __JOBS_TABLE__ j "
            "left join __PROGRAMS_TABLE__ p on j.program_name = p.program_name"
        )
        .replace("__JOBS_TABLE__", JOBS_TABLE)
        .replace("__PROGRAMS_TABLE__", PROGRAMS_TABLE)
    )
    rows = select_sql_with_profile(PROFILE, program_sql) or []
    for table_name, job_name in rows:
        result.setdefault(str(table_name), []).append(str(job_name))
    return result


def build_plan_map():
    return load_asset_plan_map(PROFILE)


def load_content(input_name: str):
    job_sql = (
        (
            "select j.job_name, p.file_path, p.target_table, j.description "
            "from __JOBS_TABLE__ j left join __PROGRAMS_TABLE__ p "
            "on j.program_name = p.program_name where j.job_name = ?"
        )
        .replace("__JOBS_TABLE__", JOBS_TABLE)
        .replace("__PROGRAMS_TABLE__", PROGRAMS_TABLE)
    )
    job_rows = select_sql_with_profile(PROFILE, job_sql, (input_name,)) or []
    report_sql = (
        "select report_name, report_path from __REPORTS_TABLE__ "
        "where upper(report_name) like upper(?)"
    ).replace("__REPORTS_TABLE__", REPORTS_TABLE)
    report_rows = (
        select_sql_with_profile(PROFILE, report_sql, (f"%{input_name}%",)) or []
    )
    if report_rows:
        name, path = report_rows[0]
        file_path = Path(str(path or ""))
        return {
            "job_name": str(name),
            "folder": str(name),
            "remark": "demo report",
            "content": read_data_from_file(file_path),
        }
    if job_rows:
        job_name, path, target_table, description = job_rows[0]
        file_path = Path(str(path or "").replace("$", str(WORKSPACE_ROOT)))
        return {
            "job_name": str(job_name),
            "folder": str(target_table or ""),
            "remark": str(description or ""),
            "content": read_data_from_file(file_path),
        }
    return None


def analyze_dependencies(job_name: str):
    put_table = import_module("pywebio.output").put_table
    loaded = load_content(job_name)
    if not loaded:
        put_red_text(f"未找到 {job_name} 对应的任务或报表")
        return
    program_job_map = build_program_job_map()
    plan_map = build_plan_map()
    table_names = sorted(
        set(extract_tables(loaded["content"]) + find_dot_strings(loaded["content"]))
    )
    related_tables = sorted(
        {table for table in table_names if program_job_map.get(table)}
    )
    put_black_text(f"输入名: {loaded['job_name']}")
    put_black_text(f"目标表: {loaded['folder']}")
    put_black_text(f"备注: {loaded['remark']}")
    rows = [["表名", "接入计划名", "资产分类"]]
    rows.extend(
        [table, plan_map.get(table, ""), PLAN_LABELS.get(plan_map.get(table, ""), "")]
        for table in related_tables
    )
    put_table(rows)


def main():
    run_for_multiline_input(
        "请输入作业名称或报表名称，每行一个",
        lambda item: analyze_dependencies(item.strip().upper()),
    )


if __name__ == "__main__":
    start_pywebio_app("元数据依赖分析", main)
