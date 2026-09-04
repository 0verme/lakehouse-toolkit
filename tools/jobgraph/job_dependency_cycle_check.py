# !/bin/python
from __future__ import annotations

import os

from pywebio.output import put_markdown, put_table, put_text

from shared.config.env import required_env
from shared.config.metadata import table as metadata_table
from shared.graph.dependency import find_cycles
from shared.ui.pywebio_helper import put_black_text, put_red_text, start_pywebio_app

JOB_DEPENDENCY_SQL = """
select job_name, dependency_name
from __JOB_DEPENDENCY_TABLE__
where job_name is not null
""".replace("__JOB_DEPENDENCY_TABLE__", metadata_table("relations", "job_dependencies"))


def get_db():
    import pymysql

    return pymysql.connect(
        host=os.getenv("PYTOOLS_MYSQL_HOST", "localhost"),
        user=required_env("PYTOOLS_MYSQL_USER"),
        password=required_env("PYTOOLS_MYSQL_PASSWORD"),
        database=os.getenv("PYTOOLS_MYSQL_DATABASE", "pytools_demo"),
        charset="utf8mb4",
        autocommit=True,
    )


def select_mysql_sql(sql: str, params: tuple = ()):
    db = get_db()
    try:
        db.ping(reconnect=True)
        cursor = db.cursor()
        try:
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        db.close()


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def is_event_dependency(value: str) -> bool:
    return bool(value) and value.startswith("EVT")


def build_preview_and_graph(rows) -> tuple[list[list[str]], dict[str, list[str]]]:
    preview_rows = [["当前作业名", "前置作业名"]]
    graph = {}
    event_rows = []

    for row in rows:
        current_job_name = normalize_text(row[0] if len(row) > 0 else "")
        dependency_job_name = normalize_text(row[1] if len(row) > 1 else "")
        if not current_job_name:
            continue

        preview_rows.append([current_job_name, dependency_job_name])
        if is_event_dependency(dependency_job_name):
            event_rows.append([current_job_name, dependency_job_name])
        graph.setdefault(current_job_name, [])
        if dependency_job_name and dependency_job_name not in graph[current_job_name]:
            graph[current_job_name].append(dependency_job_name)
            graph.setdefault(dependency_job_name, [])
    return preview_rows, graph, event_rows


def render_event_warning(event_rows: list[list[str]]):
    put_black_text(f"事件依赖数量: {len(event_rows)}")
    if not event_rows:
        return

    put_red_text("预警: 发现前置依赖以 EVT 开头，请确认这是事件依赖。")
    rows = [["序号", "当前作业名", "事件前置"]]
    for index, row in enumerate(event_rows, start=1):
        rows.append([index, row[0], row[1]])
    put_table(rows[:201])
    if len(rows) > 201:
        put_text(f"仅展示前 200 条事件依赖记录，实际共 {len(event_rows)} 条。")


def render_cycle_result(cycles: list[list[str]]):
    put_black_text(f"依赖环数量: {len(cycles)}")
    if not cycles:
        put_markdown("**检查结果: 未发现依赖成环**")
        return

    rows = [["序号", "依赖环"]]
    for index, cycle in enumerate(cycles, start=1):
        rows.append([index, " -> ".join(cycle)])
    put_table(rows)
    put_red_text("检查结果: 存在依赖成环，请处理后再上线")


def main():
    put_black_text("作业依赖成环检查")

    rows = select_mysql_sql(JOB_DEPENDENCY_SQL)
    if not rows:
        put_red_text("SQL 未返回数据，请先补充查询语句或确认数据源。")
        return

    preview_rows, graph, event_rows = build_preview_and_graph(rows)
    put_black_text(f"依赖记录数: {len(preview_rows) - 1}")

    render_event_warning(event_rows)
    cycles = find_cycles(graph, max_cycles=50)
    render_cycle_result(cycles)


if __name__ == "__main__":
    start_pywebio_app("作业依赖成环检查", main)
