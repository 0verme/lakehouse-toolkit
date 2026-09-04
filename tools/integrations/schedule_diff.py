# !/bin/python
from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from html import escape

from pywebio.output import put_html, put_table, put_text

from shared.config.env import required_env
from shared.config.metadata import table as metadata_table
from shared.ui.pywebio_helper import (
    put_black_text,
    put_red_text,
    run_for_multiline_input,
    start_pywebio_app,
)


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


PROCESS_SQL = """
select 'process_registry' as source_table, process_name, script_code
from __PROCESS_TABLE__
where script_code is not null
""".replace("__PROCESS_TABLE__", metadata_table("processes", "processes"))

REL_SQL = """
select target_table, source_table
from __RELATIONS_TABLE__
where upper(target_table) = upper(%s)
""".replace("__RELATIONS_TABLE__", metadata_table("relations", "relations"))

BASE_SCHEMAS = {"DM", "DWA", "DWD", "DWF", "DWM", "DWO", "DWP", "DWE"}
SCHEMA_PREFIXES = BASE_SCHEMAS | {f"DWS_{schema}" for schema in BASE_SCHEMAS}
TARGET_SCHEMA_PRIORITY = (
    "DWS_DWM",
    "DWS_DWF",
    "DWS_DWD",
    "DWS_DWA",
    "DWS_DWP",
    "DWS_DM",
    "DWS_DWO",
    "DWS_DWE",
)
TABLE_TOKEN_RE = re.compile(
    r'\b(?:FROM|JOIN|USING)\s+([`"\[]?[\w$]+[`"\]]?\s*\.\s*[`"\[]?[\w$]+[`"\]]?)',
    re.IGNORECASE,
)


@dataclass
class ProcessInfo:
    source_table: str
    process_name: str
    script_code: str


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


def decode_code(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def normalize_table_name(value: str) -> str:
    text = str(value or "").strip().upper()
    text = text.replace("`", "").replace('"', "").replace("[", "").replace("]", "")
    text = re.sub(r"\s+", "", text)
    if text.startswith("DWS_"):
        return text
    if "." in text:
        schema, table = text.split(".", 1)
        if schema in BASE_SCHEMAS:
            return f"DWS_{schema}.{table}"
    return text


def is_valid_table_name(value: str) -> bool:
    if "." not in value:
        return False
    schema, table = value.split(".", 1)
    return schema in SCHEMA_PREFIXES and bool(table)


def extract_tables_from_code(script_code: str) -> set[str]:
    result = set()
    for match in TABLE_TOKEN_RE.finditer(script_code or ""):
        raw_name = match.group(1)
        next_text = script_code[match.end() :].lstrip()
        if next_text.startswith("("):
            continue
        table_name = normalize_table_name(raw_name)
        if is_valid_table_name(table_name):
            result.add(table_name)
    return result


def load_process_infos() -> list[ProcessInfo]:
    rows = select_mysql_sql(PROCESS_SQL)
    return [
        ProcessInfo(
            source_table=str(source_table),
            process_name=str(process_name),
            script_code=decode_code(script_code),
        )
        for source_table, process_name, script_code in rows
    ]


def normalize_input_name(value: str) -> str:
    return str(value or "").strip().upper().replace("，", ",")


def process_task_name(process_name: str) -> str:
    parts = str(process_name or "").split(":")
    if len(parts) > 1:
        name = parts[1].upper()
    else:
        name = str(process_name or "").upper()
    if "." in name:
        name = name.split(".", 1)[1]
    return name[4:] if name.startswith("DWS_") else name


def process_target_name(process_name: str) -> str:
    parts = str(process_name or "").split(":")
    if len(parts) > 1:
        return normalize_table_name(parts[1])
    return normalize_table_name(process_name)


def build_process_target_set(process_infos: Iterable[ProcessInfo]) -> set[str]:
    return {
        target_name
        for target_name in (
            process_target_name(item.process_name) for item in process_infos
        )
        if is_valid_table_name(target_name)
    }


def derive_target_candidates(
    input_name: str, process_info: ProcessInfo | None = None
) -> list[str]:
    candidates = []
    raw_names = [input_name]
    if process_info:
        raw_names.extend(
            [
                process_target_name(process_info.process_name),
                process_task_name(process_info.process_name),
            ]
        )

    for raw_name in raw_names:
        name = normalize_input_name(raw_name)
        if not name:
            continue
        if "." in name:
            candidates.append(normalize_table_name(name))
            continue
        for prefix in TARGET_SCHEMA_PRIORITY:
            candidates.append(f"{prefix}.{name}")

    return list(dict.fromkeys(candidates))


def find_process(
    input_name: str, process_infos: Iterable[ProcessInfo]
) -> tuple[ProcessInfo | None, list[ProcessInfo]]:
    keyword = normalize_input_name(input_name)
    exact_matches = [
        item for item in process_infos if item.process_name.upper() == keyword
    ]
    if len(exact_matches) == 1:
        return exact_matches[0], []
    if len(exact_matches) > 1:
        return None, exact_matches

    fuzzy_matches = []
    for item in process_infos:
        process_name = item.process_name.upper()
        task_name = process_task_name(item.process_name)
        if (
            process_name.endswith(keyword)
            or task_name == keyword
            or keyword in process_name
        ):
            fuzzy_matches.append(item)

    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0], []
    return None, fuzzy_matches


def load_schedule_tables(target_candidates: list[str]) -> tuple[str, set[str]]:
    for target_name in target_candidates:
        rows = select_mysql_sql(REL_SQL, (target_name,))
        if rows:
            return normalize_table_name(rows[0][0]), {
                normalize_table_name(row[1]) for row in rows if row[1]
            }
    return target_candidates[0] if target_candidates else "", set()


def build_diff_rows(
    actual_tables: set[str], configured_tables: set[str]
) -> list[list[str]]:
    rows = [["表名", "SQL实际调用", "调度已配置", "差异类型"]]
    for table_name in sorted(actual_tables | configured_tables):
        in_actual = table_name in actual_tables
        in_config = table_name in configured_tables
        if in_actual and in_config:
            diff_type = "两边一致"
        elif in_actual:
            diff_type = "SQL实际调用但调度未配置"
        else:
            diff_type = "调度已配置但SQL未调用"
        rows.append(
            [
                table_name,
                "是" if in_actual else "否",
                "是" if in_config else "否",
                diff_type,
            ]
        )
    return rows


def put_diff_table(actual_tables: set[str], configured_tables: set[str]):
    rows = build_diff_rows(actual_tables, configured_tables)
    headers = rows[0]
    body_rows = rows[1:]
    thead = (
        "<tr>"
        + "".join(f"<th>{escape(str(header))}</th>" for header in headers)
        + "</tr>"
    )
    tbody = ""
    for row in body_rows:
        diff_type = str(row[-1])
        row_class = ' class="diff-row"' if diff_type != "两边一致" else ""
        tbody += "<tr{}>{}</tr>".format(
            row_class,
            "".join(f"<td>{escape(str(cell))}</td>" for cell in row),
        )

    put_html(f"""
<style>
.schedule-diff-table {{
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-family: Arial, sans-serif;
    font-size: 14px;
}}
.schedule-diff-table th,
.schedule-diff-table td {{
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
    vertical-align: top;
    word-break: break-all;
}}
.schedule-diff-table th {{
    background-color: #f5f5f5;
    font-weight: bold;
}}
.schedule-diff-table .diff-row td {{
    color: #d93025;
    font-weight: 600;
}}
</style>
<table class="schedule-diff-table">
    <thead>{thead}</thead>
    <tbody>{tbody}</tbody>
</table>
""")


def put_candidates(input_name: str, candidates: list[ProcessInfo]):
    put_red_text(f"{input_name} 匹配到多个任务，请输入更精确的 PROCESS_NAME")
    rows = [["来源表", "PROCESS_NAME", "任务名"]]
    for item in candidates[:50]:
        rows.append(
            [item.source_table, item.process_name, process_task_name(item.process_name)]
        )
    put_table(rows)
    if len(candidates) > 50:
        put_text(f"仅展示前 50 条，共 {len(candidates)} 条")


def analyze_one(
    input_name: str, process_infos: list[ProcessInfo], process_target_set: set[str]
):
    keyword = normalize_input_name(input_name)
    if not keyword:
        return

    process_info, candidates = find_process(keyword, process_infos)
    if not process_info and candidates:
        put_candidates(keyword, candidates)
        put_red_text("=" * 66)
        return

    if not process_info:
        target_candidates = derive_target_candidates(keyword)
        target_name, configured_tables = load_schedule_tables(target_candidates)
        if not configured_tables:
            put_red_text(f"{keyword} 未找到任务，也未找到调度配置")
            put_red_text("=" * 66)
            return
        actual_tables = set()
        source_table = ""
        process_name = "未找到任务，仅检查调度配置"
    else:
        target_candidates = derive_target_candidates(keyword, process_info)
        target_name, configured_tables = load_schedule_tables(target_candidates)
        actual_tables = extract_tables_from_code(process_info.script_code)
        result_table_names = set(derive_target_candidates("", process_info))
        if target_name:
            result_table_names.add(target_name)
        actual_tables -= {
            normalize_table_name(table_name) for table_name in result_table_names
        }
        actual_tables &= process_target_set
        source_table = process_info.source_table
        process_name = process_info.process_name

    missing_config = actual_tables - configured_tables
    unused_config = configured_tables - actual_tables
    if not configured_tables:
        status = "未找到调度配置"
    elif missing_config or unused_config:
        status = "有差异"
    else:
        status = "一致"

    put_black_text("任务调度差异检查")
    put_black_text("输入: " + keyword)
    put_black_text("任务名: " + process_name)
    put_black_text("目标表: " + target_name)
    put_black_text("代码来源表: " + source_table)
    put_black_text("汇总状态: " + status)

    if not actual_tables and process_info:
        put_red_text("未从 SCRIPT_CODE 中抽取到实际调用表")
    if not configured_tables:
        put_red_text("未从 relations 中找到调度来源表配置")

    put_diff_table(actual_tables, configured_tables)
    put_red_text("=" * 66)


def main():
    process_infos = load_process_infos()
    process_target_set = build_process_target_set(process_infos)
    run_for_multiline_input(
        "请输入任务名或目标表名，每行一个",
        lambda item: analyze_one(item, process_infos, process_target_set),
    )


if __name__ == "__main__":
    start_pywebio_app("任务调度差异检查", main)
