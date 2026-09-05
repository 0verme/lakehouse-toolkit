# !/bin/python
"""Deprecated compatibility tool for legacy SQL/schedule-to-DWF tracing.

The formal table lineage path is now ``jobs.crontab.imp_lineage_edge`` plus
``LineageQueryService``. This tool remains available because its DWF cutoff and
interactive report are not API-compatible replacements; remove it only after
external caller confirmation and a compatibility adapter.
"""

# ruff: noqa: I001
from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass

from pywebio.input import (  # pyright: ignore[reportMissingImports]
    TEXT,
    input_group,
    radio,
    textarea,
)
from pywebio.output import put_table  # pyright: ignore[reportMissingImports]

from shared.config.env import required_env
from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.ui.export_helper import put_table_exports
from shared.ui.pywebio_helper import (
    put_black_text,
    put_red_text,
    safe_put_error,
    start_pywebio_app,
)


def get_mysql_config() -> dict:
    return {
        "host": os.getenv("PYTOOLS_MYSQL_HOST", "localhost"),
        "user": required_env("PYTOOLS_MYSQL_USER"),
        "password": required_env("PYTOOLS_MYSQL_PASSWORD"),
        "database": os.getenv("PYTOOLS_MYSQL_DATABASE", "pytools_demo"),
        "charset": "utf8mb4",
        "autocommit": True,
    }


PROCESS_SQL = """
select 'process_registry' as source_table, process_name, script_code
from __PROCESS_TABLE__
where script_code is not null
""".replace("__PROCESS_TABLE__", metadata_table("processes", "processes"))

REL_ALL_SQL = """
select target_table, source_table
from __RELATIONS_TABLE__
where target_table is not null
  and source_table is not null
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
MAX_DEPTH = 30
GAUSS_PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
MODE_SQL = "sql"
MODE_SCHEDULE = "schedule"


@dataclass
class ProcessInfo:
    source_table: str
    process_name: str
    script_code: str


@dataclass
class TraceResult:
    dwf_tables: set[str]


def get_db():
    import pymysql  # pyright: ignore[reportMissingModuleSource]

    return pymysql.connect(**get_mysql_config())


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


def normalize_input_name(value: str) -> str:
    return str(value or "").strip().upper().replace("，", ",")


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


def is_dwf_table(value: str) -> bool:
    return normalize_table_name(value).startswith("DWS_DWF.")


def to_plain_dwf_name(table_name: str) -> str:
    return normalize_table_name(table_name).replace("DWS_DWF.", "DWF.")


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


def extract_upstream_tables(process_info: ProcessInfo) -> list[str]:
    actual_tables = extract_tables_from_code(process_info.script_code)
    actual_tables.discard(process_target_name(process_info.process_name))
    return sorted(actual_tables)


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


def build_target_map(
    process_infos: Iterable[ProcessInfo],
) -> dict[str, list[ProcessInfo]]:
    target_map: dict[str, list[ProcessInfo]] = {}
    for item in process_infos:
        target_name = process_target_name(item.process_name)
        if is_valid_table_name(target_name):
            target_map.setdefault(target_name, []).append(item)
    return target_map


def build_schedule_map() -> dict[str, set[str]]:
    rows = select_mysql_sql(REL_ALL_SQL)
    result: dict[str, set[str]] = {}
    for target_name, source_name in rows:
        target = normalize_table_name(target_name)
        source = normalize_table_name(source_name)
        if not (is_valid_table_name(target) and is_valid_table_name(source)):
            continue
        result.setdefault(target, set()).add(source)
    return result


def dedupe_processes(items: Iterable[ProcessInfo]) -> list[ProcessInfo]:
    result: list[ProcessInfo] = []
    seen: set[str] = set()
    for item in items:
        key = f"{item.source_table}|{item.process_name}"
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def find_process(
    input_name: str,
    process_infos: list[ProcessInfo],
    target_map: dict[str, list[ProcessInfo]],
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
    if len(fuzzy_matches) > 1:
        return None, fuzzy_matches

    target_matches = []
    for target_name in derive_target_candidates(keyword):
        target_matches.extend(target_map.get(target_name, []))
    target_matches = dedupe_processes(target_matches)
    if len(target_matches) == 1:
        return target_matches[0], []
    return None, target_matches


def resolve_root_target(
    input_name: str,
    process_info: ProcessInfo | None,
    target_map: dict[str, list[ProcessInfo]],
    schedule_map: dict[str, set[str]],
) -> str:
    if process_info:
        return process_target_name(process_info.process_name)

    for target_name in derive_target_candidates(input_name):
        if (
            target_name in target_map
            or target_name in schedule_map
            or is_dwf_table(target_name)
        ):
            return target_name
    return ""


def put_candidates(input_name: str, candidates: list[ProcessInfo]):
    put_red_text(f"{input_name} 匹配到多个任务，请输入更精确的 PROCESS_NAME")
    rows = [["来源表", "PROCESS_NAME", "任务名", "目标表"]]
    for item in candidates[:50]:
        rows.append(
            [
                item.source_table,
                item.process_name,
                process_task_name(item.process_name),
                process_target_name(item.process_name),
            ]
        )
    put_table(rows)


def trace_to_dwf_by_sql(
    start_process: ProcessInfo, target_map: dict[str, list[ProcessInfo]]
) -> TraceResult:
    dwf_tables: set[str] = set()
    root_target = process_target_name(start_process.process_name)

    def walk(current_process: ProcessInfo, path: list[str], depth: int):
        if depth > MAX_DEPTH:
            return

        upstream_tables = extract_upstream_tables(current_process)
        if not upstream_tables:
            return

        for upstream_table in upstream_tables:
            if upstream_table in path:
                continue
            if is_dwf_table(upstream_table):
                dwf_tables.add(upstream_table)
                continue

            next_processes = dedupe_processes(target_map.get(upstream_table, []))
            for next_process in next_processes:
                walk(next_process, path + [upstream_table], depth + 1)

    walk(start_process, [root_target], 1)
    return TraceResult(dwf_tables=dwf_tables)


def trace_to_dwf_by_schedule(
    root_target: str, schedule_map: dict[str, set[str]]
) -> TraceResult:
    dwf_tables: set[str] = set()

    def walk(current_target: str, path: list[str], depth: int):
        if depth > MAX_DEPTH:
            return
        for upstream_table in sorted(schedule_map.get(current_target, set())):
            if upstream_table in path:
                continue
            if is_dwf_table(upstream_table):
                dwf_tables.add(upstream_table)
                continue
            walk(upstream_table, path + [upstream_table], depth + 1)

    walk(root_target, [root_target], 1)
    return TraceResult(dwf_tables=dwf_tables)


def load_dwf_endtime_map(dwf_tables: Iterable[str]) -> dict[str, str]:
    plain_dwf_tables = sorted(
        {
            to_plain_dwf_name(table_name)
            for table_name in dwf_tables
            if is_dwf_table(table_name)
        }
    )
    if not plain_dwf_tables:
        return {}

    placeholders = ",".join("?" for _ in plain_dwf_tables)
    sql = (
        """
select
    r.table_name,
    max(t.end_time) as endtime
from __RESULT_RECEIPTS_TABLE__ r
left join __RUNTIMES_TABLE__ t
    on r.source_job_name = t.job_name
where r.table_name in (__PLACEHOLDERS__)
group by r.table_name
""".replace(
            "__RESULT_RECEIPTS_TABLE__",
            metadata_table("result_receipts", "result_receipts"),
        )
        .replace("__RUNTIMES_TABLE__", metadata_table("runtimes", "runtimes"))
        .replace("__PLACEHOLDERS__", placeholders)
    )
    rows = select_sql_with_profile(GAUSS_PROFILE, sql, tuple(plain_dwf_tables)) or []
    return {
        str(table_name).upper(): "" if endtime is None else str(endtime)
        for table_name, endtime in rows
    }


def calc_latest_endtime(endtime_values: Iterable[str]) -> str:
    values = [
        str(value).strip() for value in endtime_values if str(value or "").strip()
    ]
    return max(values) if values else ""


def mode_label(mode: str) -> str:
    return "实际SQL" if mode == MODE_SQL else "调度依赖"


def analyze_one(
    input_name: str,
    mode: str,
    process_infos: list[ProcessInfo],
    target_map: dict[str, list[ProcessInfo]],
    schedule_map: dict[str, set[str]],
) -> dict | None:
    keyword = normalize_input_name(input_name)
    if not keyword:
        return None

    process_info, candidates = find_process(keyword, process_infos, target_map)
    if not process_info and candidates:
        put_candidates(keyword, candidates)
        put_red_text("=" * 66)
        return None

    root_target = resolve_root_target(keyword, process_info, target_map, schedule_map)
    if not root_target:
        put_red_text(f"{keyword} 未找到对应任务或目标表")
        put_red_text("=" * 66)
        return None

    if is_dwf_table(root_target):
        put_black_text("递归上游追踪")
        put_black_text("输入: " + keyword)
        put_black_text("检查方式: " + mode_label(mode))
        put_black_text("目标表: " + root_target)
        put_red_text("当前目标表已经是 DWF 层，无需继续向上追踪")
        put_red_text("=" * 66)
        return None

    if mode == MODE_SQL:
        if not process_info:
            put_red_text(f"{keyword} 未找到唯一加工任务，无法按实际 SQL 检查")
            put_red_text("=" * 66)
            return None
        result = trace_to_dwf_by_sql(process_info, target_map)
    else:
        result = trace_to_dwf_by_schedule(root_target, schedule_map)

    dwf_endtime_map = load_dwf_endtime_map(result.dwf_tables)
    detail_rows = [["前置的DWF表", "endtime"]]

    put_black_text("输入: " + keyword)
    put_black_text("检查方式: " + mode_label(mode))
    if result.dwf_tables:
        detail_rows.extend(
            [
                [
                    to_plain_dwf_name(table_name),
                    dwf_endtime_map.get(to_plain_dwf_name(table_name), ""),
                ]
                for table_name in sorted(result.dwf_tables)
            ]
        )
        latest_endtime = calc_latest_endtime(row[1] for row in detail_rows[1:])
        put_black_text("最晚出数时间: " + latest_endtime)
        put_table(detail_rows)
    else:
        latest_endtime = ""
        put_black_text("最晚出数时间: ")
        put_red_text("未追踪到前置 DWF 表")
    put_red_text("=" * 66)

    if len(detail_rows) <= 1:
        return None

    export_rows = [
        ["输入任务", keyword],
        ["检查方式", mode_label(mode)],
        ["最晚出数时间", latest_endtime],
        ["", ""],
    ] + detail_rows
    return {
        "title": keyword,
        "rows": export_rows,
        "index_latest_endtime": latest_endtime,
    }


def main():
    process_infos = load_process_infos()
    target_map = build_target_map(process_infos)
    schedule_map = build_schedule_map()

    info = input_group(
        "递归上游追踪",
        [
            textarea("请输入任务名或目标表名，每行一个", name="targets", type=TEXT),
            radio(
                "检查方式",
                name="mode",
                options=[
                    {"label": "使用实际 SQL 检查", "value": MODE_SQL},
                    {"label": "使用调度依赖检查", "value": MODE_SCHEDULE},
                ],
                value=MODE_SQL,
            ),
        ],
    )

    export_sections: list[dict] = []
    for item in [
        line.strip() for line in str(info["targets"] or "").splitlines() if line.strip()
    ]:
        try:
            section = analyze_one(
                item, info["mode"], process_infos, target_map, schedule_map
            )
            if section:
                export_sections.append(section)
        except Exception as exc:
            safe_put_error(exc)

    if export_sections:
        put_table_exports(
            prefix="sql_upstream_to_layer",
            name_parts=[info["mode"], "batch"],
            page_title="递归上游追踪",
            sheet_sections=export_sections,
            add_index_sheet=True,
            index_sheet_title="目录",
        )


if __name__ == "__main__":
    start_pywebio_app("PyTool", main)
