"""解析 SQL 中的表并查询对应的 demo 作业。"""

from __future__ import annotations

import os
from importlib import import_module

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.text.regex import extract_tables, find_dot_strings
from shared.ui.pywebio_helper import start_pywebio_app

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
JOBS_TABLE = metadata_table("jobs", "jobs")
PROGRAMS_TABLE = metadata_table("programs", "programs")
IGNORED_TABLES = {
    "DATETIME",
    "DUAL",
    "DEMO_META.DATE_FUNCTIONS",
    "DEMO_META.STANDARD_FUNCTIONS",
}


def main_job(content: str):
    put_table = import_module("pywebio.output").put_table
    tables = sorted(
        set(
            extract_tables((content or "").replace("\n", " "))
            + find_dot_strings(content or "")
        )
    )
    filtered_tables = [table for table in tables if table not in IGNORED_TABLES]
    sql = """
    SELECT p.target_table, j.plan_name
    FROM __JOBS_TABLE__ j
    LEFT JOIN __PROGRAMS_TABLE__ p ON j.program_name = p.program_name
    WHERE p.target_table IS NOT NULL
    """.replace("__JOBS_TABLE__", JOBS_TABLE).replace(
        "__PROGRAMS_TABLE__", PROGRAMS_TABLE
    )
    result = select_sql_with_profile(PROFILE, sql) or []
    plan_by_table = {str(row[0]).upper(): str(row[1] or "") for row in result}
    put_table(
        [
            ["SQL 依赖表", "对应计划"],
            [
                "\n".join(filtered_tables),
                "\n".join(
                    plan_by_table.get(table.upper(), "") for table in filtered_tables
                ),
            ],
        ]
    )


def main():
    pywebio_input = import_module("pywebio.input")
    main_job(pywebio_input.textarea("请输入 SQL", type=pywebio_input.TEXT))


if __name__ == "__main__":
    start_pywebio_app("SQL 依赖分析", main)
