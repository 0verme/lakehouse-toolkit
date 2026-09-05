from __future__ import annotations

import os
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import lru_cache

from shared.config.env import required_env
from shared.config.metadata import table as metadata_table
from shared.lineage.domain import decode_code

DB_CONFIG = {
    "host": os.getenv("PYTOOLS_LINEAGE_MYSQL_HOST", "localhost"),
    "user": os.getenv("PYTOOLS_LINEAGE_MYSQL_USER", ""),
    "password": os.getenv("PYTOOLS_LINEAGE_MYSQL_PASSWORD"),
    "database": os.getenv("PYTOOLS_LINEAGE_MYSQL_DATABASE", "pytools_demo"),
    "charset": "utf8mb4",
    "autocommit": True,
}
DEBUG_ENABLED = str(os.getenv("PYTOOLS_LINEAGE_DEBUG", "")).strip() == "1"

# metadata_table validates every identifier before replacing the SQL template tokens.
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
TIME_TOKEN_RE = re.compile(r"(\d{1,2}):(\d{2})(?::(\d{2}))?")
TYPE_ORDER = {
    "root_table": 0,
    "process": 1,
    "table": 2,
    "source_table": 3,
}
PROGRAM_TARGET_SQL = "upper(trim(p.target_table))"
SCHEDULE_TIME_SQL = (
    """
select
    upper(trim(r.table_name)) as target_table,
    upper(r.source_job_name) as job_name,
    max(t.end_time) as endtime
from __RESULT_RECEIPTS_TABLE__ r
left join __RUNTIMES_TABLE__ t
    on r.source_job_name = t.job_name
where r.table_name is not null
group by upper(trim(r.table_name)), upper(r.source_job_name)
union all
select
    __PROGRAM_TARGET_SQL__ as target_table,
    upper(j.job_name) as job_name,
    max(t.end_time) as endtime
from __JOBS_TABLE__ j
inner join __PROGRAMS_TABLE__ p
    on j.program_name = p.program_name
left join __RUNTIMES_TABLE__ t
    on j.job_name = t.job_name
where p.target_table is not null
group by __PROGRAM_TARGET_SQL__, upper(j.job_name)
""".replace(
        "__RESULT_RECEIPTS_TABLE__",
        metadata_table("result_receipts", "result_receipts"),
    )
    .replace("__RUNTIMES_TABLE__", metadata_table("runtimes", "runtimes"))
    .replace("__PROGRAM_TARGET_SQL__", PROGRAM_TARGET_SQL)
    .replace("__JOBS_TABLE__", metadata_table("jobs", "jobs"))
    .replace("__PROGRAMS_TABLE__", metadata_table("programs", "programs"))
)


@dataclass(frozen=True)
class ProcessInfo:
    source_table: str
    process_name: str
    script_code: str


@dataclass
class GraphNode:
    id: str
    label: str
    type: str
    col: int
    detail: dict[str, str] = field(default_factory=dict)
    schedule_time: str = ""
    sort_has_time: bool = False
    sort_time_seconds: int | None = None
    type_order: int = 99
    sort_key: str = ""
    alert_rank: int | None = None
    alert_level: str = ""


@dataclass(frozen=True)
class ScheduleTimeInfo:
    table_name: str
    job_name: str
    schedule_time: str
    sort_time_seconds: int | None


@dataclass
class LineageGraph:
    root_input: str
    root_target: str
    root_process: str
    nodes: list[GraphNode]
    edges: list[tuple[str, str]]
    max_depth: int
    truncated: bool = False
    cycles: list[list[str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    debug_logs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "root_input": self.root_input,
            "root_target": self.root_target,
            "root_process": self.root_process,
            "max_depth": self.max_depth,
            "truncated": self.truncated,
            "cycles": self.cycles,
            "warnings": self.warnings,
            "debug_logs": self.debug_logs,
            "nodes": [
                {
                    "id": node.id,
                    "label": node.label,
                    "type": node.type,
                    "col": node.col,
                    "detail": node.detail,
                    "schedule_time": node.schedule_time,
                    "sort_has_time": node.sort_has_time,
                    "sort_time_seconds": node.sort_time_seconds,
                    "type_order": node.type_order,
                    "sort_key": node.sort_key,
                    "alert_rank": node.alert_rank,
                    "alert_level": node.alert_level,
                }
                for node in self.nodes
            ],
            "edges": [list(edge) for edge in self.edges],
        }


def get_db():
    import pymysql

    DB_CONFIG["user"] = required_env("PYTOOLS_LINEAGE_MYSQL_USER")
    DB_CONFIG["password"] = required_env("PYTOOLS_LINEAGE_MYSQL_PASSWORD")
    return pymysql.connect(**DB_CONFIG)


def select_mysql_sql(sql: str, params: tuple = ()):
    db = get_db()
    try:
        db.ping(reconnect=True)
        cursor = db.cursor()
        try:
            # SQL comes from internal templates; runtime values are passed separately.
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql, params)  # noqa: S608
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        db.close()


def normalize_input_name(value: str) -> str:
    return str(value or "").strip().upper().replace("\t", "").replace("锛?, ", "")


@lru_cache(maxsize=32768)
def normalize_table_name(value: str) -> str:
    text = str(value or "").strip().upper()
    text = (
        text.replace("`", "")
        .replace('"', "")
        .replace("'", "")
        .replace("[", "")
        .replace("]", "")
    )
    text = re.sub(r"\s+", "", text)
    if text.startswith("DWS_"):
        return text
    if "." in text:
        schema, table = text.split(".", 1)
        if schema in BASE_SCHEMAS:
            return f"DWS_{schema}.{table}"
    return text


@lru_cache(maxsize=32768)
def table_name_parts(value: str) -> tuple[str, str]:
    table_name = normalize_table_name(value)
    if "." in table_name:
        schema, short_name = table_name.split(".", 1)
        return table_name, short_name
    return table_name, table_name


@lru_cache(maxsize=32768)
def table_name_aliases(value: str) -> tuple[str, ...]:
    full_name, short_name = table_name_parts(value)
    aliases = {name for name in {full_name, short_name} if name}
    if full_name.startswith("DWS_") and "." in full_name:
        schema, table = full_name.split(".", 1)
        plain_schema = schema[4:]
        if plain_schema:
            aliases.add(f"{plain_schema}.{table}")
    elif "." in full_name:
        schema, table = full_name.split(".", 1)
        if schema in BASE_SCHEMAS:
            aliases.add(f"DWS_{schema}.{table}")
    if full_name.startswith("DWS_DWE."):
        short = full_name.split(".", 1)[1]
        aliases.add(f"DWP.DWE_{short}")
        aliases.add(f"DWE.{short}")
    if full_name.startswith("DWS_DWP.DWE_"):
        short = full_name.split(".", 1)[1][len("DWE_") :]
        aliases.add(f"DWS_DWE.{short}")
        aliases.add(f"DWE.{short}")
        aliases.add(f"DWE_{short}")
    if full_name.startswith("DWE."):
        short = full_name.split(".", 1)[1]
        aliases.add(f"DWS_DWE.{short}")
        aliases.add(f"DWP.DWE_{short}")
        aliases.add(f"DWE_{short}")
    if short_name.startswith("DWE_"):
        short = short_name[len("DWE_") :]
        aliases.add(f"DWS_DWE.{short}")
        aliases.add(f"DWE.{short}")
    return tuple(sorted(aliases))


def normalize_result_table_names(result_table_names: Iterable[str] | None) -> set[str]:
    result: set[str] = set()
    for item in result_table_names or []:
        result.update(table_name_aliases(item))
    return result


@lru_cache(maxsize=1)
def load_result_table_names() -> set[str]:
    from shared.lineage.mapping_sqlite import load_registered_result_tables

    return normalize_result_table_names(load_registered_result_tables())


def is_known_result_table(
    table_name: str, result_table_names: Iterable[str] | None
) -> bool:
    if result_table_names is None:
        return True
    known_names = normalize_result_table_names(result_table_names)
    return any(alias in known_names for alias in table_name_aliases(table_name))


def is_known_result_table_fast(
    table_name: str, normalized_result_table_names: set[str]
) -> bool:
    return any(
        alias in normalized_result_table_names
        for alias in table_name_aliases(table_name)
    )


def same_table(a: str, b: str) -> bool:
    a_full, a_short = table_name_parts(a)
    b_full, b_short = table_name_parts(b)
    if a_full and b_full and "." in a_full and "." in b_full:
        return a_full == b_full
    return bool(a_short and b_short and a_short == b_short)


def is_self_lineage(source_table: str, target_table: str) -> bool:
    return same_table(source_table, target_table)


def is_terminal_upstream_table(table_name: str) -> bool:
    full_name, short_name = table_name_parts(table_name)
    parts = [part for part in re.split(r"[._]", full_name) if part]
    if full_name.startswith(("DWF.", "DWS_DWF.")):
        return True
    if short_name.startswith("DWF_"):
        return True
    return "DWF" in parts


def is_f_layer_table(table_name: str) -> bool:
    full_name, short_name = table_name_parts(table_name)
    return full_name.startswith(("DWS_DWF.", "DWF.")) or short_name.startswith("DWF_")


def is_valid_table_name(value: str) -> bool:
    if "." not in value:
        return False
    schema, table = value.split(".", 1)
    return schema in SCHEMA_PREFIXES and bool(table)


@lru_cache(maxsize=32768)
def process_task_name(process_name: str) -> str:
    parts = str(process_name or "").split(":")
    if len(parts) > 1:
        name = parts[1].upper()
    else:
        name = str(process_name or "").upper()
    if "." in name:
        name = name.split(".", 1)[1]
    return name[4:] if name.startswith("DWS_") else name


@lru_cache(maxsize=32768)
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


def strip_sql_comments(script_code: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", script_code or "", flags=re.S)
    text = re.sub(r"--.*?$", " ", text, flags=re.M)
    return text


def extract_tables_from_code(script_code: str) -> set[str]:
    result = set()
    text = strip_sql_comments(script_code)
    for match in TABLE_TOKEN_RE.finditer(text):
        raw_name = match.group(1)
        next_text = text[match.end() :].lstrip()
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


@lru_cache(maxsize=1)
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


@lru_cache(maxsize=1)
def load_schedule_map() -> dict[str, set[str]]:
    rows = select_mysql_sql(REL_ALL_SQL)
    result: dict[str, set[str]] = {}
    for target_name, source_name in rows:
        target = normalize_table_name(target_name)
        source = normalize_table_name(source_name)
        if is_valid_table_name(target) and is_valid_table_name(source):
            result.setdefault(target, set()).add(source)
    return result


def normalize_schedule_time(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d %H:%M:%S")
        except (AttributeError, TypeError, ValueError):
            return str(value).strip()
    return str(value).strip()


def parse_schedule_time_seconds(value) -> int | None:
    text = normalize_schedule_time(value)
    matches = TIME_TOKEN_RE.findall(text)
    if not matches:
        return None
    hour_text, minute_text, second_text = matches[-1]
    try:
        hour = int(hour_text)
        minute = int(minute_text)
        second = int(second_text or 0)
    except (TypeError, ValueError):
        return None
    if hour > 23 or minute > 59 or second > 59:
        return None
    return hour * 3600 + minute * 60 + second


def schedule_time_sort_tuple(info: ScheduleTimeInfo | None) -> tuple:
    if info is None:
        return (1, 10**9, "", "")
    has_time = 0 if info.sort_time_seconds is not None else 1
    time_value = info.sort_time_seconds if info.sort_time_seconds is not None else 10**9
    return (has_time, time_value, info.schedule_time, info.job_name)


def choose_earliest_schedule_time(
    current: ScheduleTimeInfo | None, candidate: ScheduleTimeInfo | None
) -> ScheduleTimeInfo | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    if schedule_time_sort_tuple(candidate) < schedule_time_sort_tuple(current):
        return candidate
    return current


def resolve_graph_node_schedule(
    node: GraphNode,
    graph: LineageGraph,
    schedule_time_map: dict[str, ScheduleTimeInfo],
    reverse_adj: dict[str, list[str]],
    forward_adj: dict[str, list[str]],
) -> ScheduleTimeInfo | None:
    if node.type == "process":
        return schedule_time_map.get(node.detail.get("target_table", ""))

    primary = schedule_time_map.get(node.label)
    if primary is not None or node.type != "source_table":
        return primary

    fallback: ScheduleTimeInfo | None = None
    for downstream_id in forward_adj.get(node.id, []):
        downstream_node = next(
            (item for item in graph.nodes if item.id == downstream_id), None
        )
        if downstream_node is None or downstream_node.type != "process":
            continue
        fallback = choose_earliest_schedule_time(
            fallback,
            schedule_time_map.get(downstream_node.detail.get("target_table", "")),
        )
    return fallback


def apply_schedule_times_to_graph(
    graph: LineageGraph, schedule_time_map: dict[str, ScheduleTimeInfo] | None
):
    schedule_time_map = schedule_time_map or {}
    reverse_adj: dict[str, list[str]] = {}
    forward_adj: dict[str, list[str]] = {}
    for from_id, to_id in graph.edges:
        forward_adj.setdefault(from_id, []).append(to_id)
        reverse_adj.setdefault(to_id, []).append(from_id)

    resolved_samples: list[str] = []
    miss_samples: list[str] = []
    for node in graph.nodes:
        schedule_info = resolve_graph_node_schedule(
            node, graph, schedule_time_map, reverse_adj, forward_adj
        )
        node.schedule_time = (
            "" if schedule_info is None else schedule_info.schedule_time
        )
        node.sort_has_time = (
            schedule_info is not None and schedule_info.sort_time_seconds is not None
        )
        node.sort_time_seconds = (
            None if schedule_info is None else schedule_info.sort_time_seconds
        )
        node.type_order = TYPE_ORDER.get(node.type, 99)
        time_value = (
            node.sort_time_seconds if node.sort_time_seconds is not None else 10**9
        )
        node.sort_key = f"{0 if node.sort_has_time else 1}|{time_value:06d}|{node.type_order:02d}|{node.label}|{node.id}"
        node.alert_rank = None
        node.alert_level = ""
        node.detail["调度时间"] = node.schedule_time or "-"
        if schedule_info is not None and len(resolved_samples) < 5:
            resolved_samples.append(
                f"node.label={node.label} node.type={node.type} schedule_info={schedule_info.schedule_time}"
            )
        elif schedule_info is None and len(miss_samples) < 8:
            if not schedule_time_map:
                reason = "SQL 无返回"
            elif node.type == "process" and not node.detail.get("target_table"):
                reason = "detail key 不一致: process 缺少 target_table"
            elif node.type == "source_table" and not forward_adj.get(node.id):
                reason = "节点类型 fallback 没走到: source_table 无直接下游"
            elif node.type == "source_table":
                reason = "节点类型 fallback 已执行但下游任务也无时间"
            else:
                lookup_name = (
                    node.detail.get("target_table", "")
                    if node.type == "process"
                    else node.label
                )
                reason = f"alias 不匹配: lookup={lookup_name} aliases={table_name_aliases(lookup_name)}"
            miss_samples.append(
                f"node.label={node.label} node.type={node.type} miss={reason}"
            )

    graph.debug_logs.append(
        f"schedule_apply map_keys_sample={sorted(schedule_time_map)[:12]}"
    )
    graph.debug_logs.extend("schedule_resolve " + item for item in resolved_samples)
    graph.debug_logs.extend("schedule_miss " + item for item in miss_samples)

    f_layer_nodes = sorted(
        (
            node
            for node in graph.nodes
            if node.type != "process"
            and node.sort_time_seconds is not None
            and is_f_layer_table(node.label)
        ),
        key=lambda item: (
            -(item.sort_time_seconds if item.sort_time_seconds is not None else -1),
            item.label,
            item.id,
        ),
    )
    for index, node in enumerate(f_layer_nodes[:3], start=1):
        node.alert_rank = index
        node.alert_level = {1: "red", 2: "orange", 3: "amber"}.get(index, "")
        node.detail["F_LAYER_ALERT"] = f"TOP_{index}"

    graph.nodes = sorted(
        graph.nodes,
        key=lambda node: (
            node.col,
            0 if node.sort_has_time else 1,
            node.sort_time_seconds if node.sort_time_seconds is not None else 10**9,
            node.type_order,
            node.label,
            node.id,
        ),
    )


def sql_string_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def format_elapsed_ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}ms"


def emit_perf_log(logs: list[str], message: str):
    text = str(message)
    logs.append(text)
    print(f"[lineage_perf] {text}")


def schedule_time_query_names(table_names: Iterable[str]) -> tuple[str, ...]:
    result: set[str] = set()
    for item in table_names:
        normalized = normalize_table_name(item)
        if not normalized:
            continue
        result.update(alias for alias in table_name_aliases(normalized) if alias)
    return tuple(sorted(result))


def build_schedule_time_sql_for_tables(table_names: Iterable[str]) -> str:
    query_names = schedule_time_query_names(table_names)
    if not query_names:
        return ""
    targets = ", ".join(sql_string_literal(item) for item in query_names)
    recv_filter = f"and upper(trim(r.table_name)) in ({targets})"
    program_filter = f"and {PROGRAM_TARGET_SQL} in ({targets})"
    return SCHEDULE_TIME_SQL.replace(
        "where r.table_name is not null",
        "where r.table_name is not null\n" + recv_filter,
    ).replace(
        "where p.target_table is not null",
        "where p.target_table is not null\n" + program_filter,
    )


def merge_schedule_time_row(
    result: dict[str, ScheduleTimeInfo],
    table_name: str,
    job_name: str,
    endtime,
    requested_table_names: Iterable[str] | None = None,
):
    normalized_table_name = normalize_table_name(table_name)
    if not normalized_table_name:
        return
    schedule_time = normalize_schedule_time(endtime)
    schedule_info = ScheduleTimeInfo(
        table_name=normalized_table_name,
        job_name=str(job_name or "").strip().upper(),
        schedule_time=schedule_time,
        sort_time_seconds=parse_schedule_time_seconds(schedule_time),
    )
    aliases = set(table_name_aliases(normalized_table_name))
    for requested_name in requested_table_names or ():
        requested_aliases = set(table_name_aliases(requested_name))
        if normalized_table_name in requested_aliases or aliases.intersection(
            requested_aliases
        ):
            aliases.update(requested_aliases)
    if not aliases:
        return
    for alias in aliases:
        earliest = choose_earliest_schedule_time(result.get(alias), schedule_info)
        if earliest is not None:
            result[alias] = earliest


@lru_cache(maxsize=4)
def load_schedule_time_map(profile: str = "demo") -> dict[str, ScheduleTimeInfo]:
    from shared.db.gaussdb import select_sql_with_profile

    rows = select_sql_with_profile(profile, SCHEDULE_TIME_SQL) or []
    result: dict[str, ScheduleTimeInfo] = {}
    for table_name, job_name, endtime in rows:
        merge_schedule_time_row(result, table_name, job_name, endtime)
    return result


@lru_cache(maxsize=128)
def _load_schedule_time_map_for_tables_cached(
    profile: str, table_names_key: tuple[str, ...]
) -> dict[str, ScheduleTimeInfo]:
    from shared.db.gaussdb import select_sql_with_profile

    sql = build_schedule_time_sql_for_tables(table_names_key)
    if not sql:
        return {}
    query_names = schedule_time_query_names(table_names_key)
    print(
        "[lineage_schedule] targeted_where "
        "recv=upper(trim(r.table_name)) IN aliases; "
        "program=normalized(p.target_table) IN aliases; "
        f"aliases={list(query_names)[:30]}"
    )
    rows = select_sql_with_profile(profile, sql) or []
    print(f"[lineage_schedule] query_rows={len(rows)} sample={rows[:5]}")
    result: dict[str, ScheduleTimeInfo] = {}
    for table_name, job_name, endtime in rows:
        merge_schedule_time_row(
            result, table_name, job_name, endtime, requested_table_names=table_names_key
        )
    print(f"[lineage_schedule] map_key_sample={sorted(result)[:20]}")
    return result


def load_schedule_time_map_for_tables(
    table_names: Iterable[str], profile: str = "demo"
) -> dict[str, ScheduleTimeInfo]:
    table_names_key = tuple(
        sorted(
            {
                normalize_table_name(item)
                for item in table_names
                if normalize_table_name(item)
            }
        )
    )
    if not table_names_key:
        return {}
    return dict(_load_schedule_time_map_for_tables_cached(profile, table_names_key))


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


def build_target_map(
    process_infos: Iterable[ProcessInfo],
) -> dict[str, list[ProcessInfo]]:
    target_map: dict[str, list[ProcessInfo]] = {}
    for item in process_infos:
        target_name = process_target_name(item.process_name)
        if is_valid_table_name(target_name):
            target_map.setdefault(target_name, []).append(item)
    return target_map


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

    target_matches = []
    for target_name in derive_target_candidates(keyword):
        target_matches.extend(target_map.get(target_name, []))
    target_matches = dedupe_processes(target_matches)
    if len(target_matches) == 1:
        return target_matches[0], []
    if len(target_matches) > 1:
        return None, target_matches

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
    return None, dedupe_processes(fuzzy_matches)


class PipelineLineageBuilder:
    def __init__(
        self,
        process_infos: Iterable[ProcessInfo],
        schedule_map: dict[str, set[str]] | None = None,
        schedule_time_map: dict[str, ScheduleTimeInfo] | None = None,
        max_depth: int = 8,
        result_table_names: Iterable[str] | None = None,
    ):
        self.process_infos = list(process_infos)
        self.target_map = build_target_map(self.process_infos)
        self.schedule_map = schedule_map or {}
        self.schedule_time_map = schedule_time_map or {}
        self.max_depth = max_depth
        if result_table_names is None:
            result_table_names = load_result_table_names()
        self.result_table_names = normalize_result_table_names(result_table_names)

        self._nodes: dict[str, GraphNode] = {}
        self._logical_cols: dict[str, int] = {}
        self._edges: set[tuple[str, str]] = set()
        self._visited_processes: set[str] = set()
        self._truncated = False
        self._cycles: list[list[str]] = []
        self._warnings: list[str] = []
        self._debug_logs: list[str] = []
        self._root_target = ""
        self._root_process = ""
        self._root_schedule_info: ScheduleTimeInfo | None = None
        self._filtered_non_result_tables: set[str] = set()
        self._filtered_self_lineage_count = 0
        self._terminal_dwf_count = 0
        self._node_primary_schedule: dict[str, ScheduleTimeInfo] = {}
        self._node_fallback_schedule: dict[str, ScheduleTimeInfo] = {}

    def build(self, input_name: str) -> tuple[LineageGraph | None, list[ProcessInfo]]:
        keyword = normalize_input_name(input_name)
        process_info, candidates = find_process(
            keyword, self.process_infos, self.target_map
        )
        if not process_info and candidates:
            self._debug(
                f"keyword={keyword} matched_multiple_candidates={len(candidates)}"
            )
            return None, candidates

        root_target = self._resolve_root_target(keyword, process_info)
        if not root_target:
            self._debug(
                f"keyword={keyword} root_target=EMPTY process_found={bool(process_info)}"
            )
            return None, []

        self._root_target = root_target
        self._root_process = process_info.process_name if process_info else ""
        self._root_schedule_info = self.schedule_time_map.get(root_target)
        root_known = self._is_known_table(root_target)
        self._debug(
            "root_target={root} root_process={process} root_known={known} aliases={aliases}".format(
                root=root_target,
                process=self._root_process or "-",
                known=root_known,
                aliases=sorted(table_name_aliases(root_target)),
            )
        )
        if not root_known:
            self._debug(f"root_target_not_in_result_registry_but_kept={root_target}")
            self._warnings.append(
                "根目标表未命中结果表白名单，但因本次输入显式命中任务，已继续展开上游。"
            )

        self._add_table_node(root_target, 0, "root_table")

        if process_info:
            self._walk_process(process_info, target_col=0, depth=0, path=[root_target])
        else:
            self._warnings.append(
                "未定位到生产任务，仅展示根目标表；主链路未使用调度依赖展开。"
            )

        self._append_summary_warnings()
        self._normalize_cols()
        temp_graph = LineageGraph(
            root_input=keyword,
            root_target=root_target,
            root_process=self._root_process,
            nodes=list(self._nodes.values()),
            edges=list(self._edges),
            max_depth=self.max_depth,
            truncated=self._truncated,
            cycles=self._cycles,
            warnings=self._warnings,
            debug_logs=self._debug_logs,
        )
        apply_schedule_times_to_graph(temp_graph, self.schedule_time_map)
        nodes = temp_graph.nodes
        valid_node_ids = {node.id for node in nodes}
        edges = sorted(
            (from_id, to_id)
            for from_id, to_id in self._edges
            if from_id != to_id
            and from_id in valid_node_ids
            and to_id in valid_node_ids
        )
        return (
            LineageGraph(
                root_input=keyword,
                root_target=root_target,
                root_process=self._root_process,
                nodes=nodes,
                edges=edges,
                max_depth=self.max_depth,
                truncated=self._truncated,
                cycles=self._cycles,
                warnings=self._warnings,
                debug_logs=self._debug_logs,
            ),
            [],
        )

    def _resolve_root_target(
        self, input_name: str, process_info: ProcessInfo | None
    ) -> str:
        if process_info:
            return process_target_name(process_info.process_name)
        for target_name in derive_target_candidates(input_name):
            if (
                target_name in self.target_map
                or target_name in self.schedule_map
                or is_valid_table_name(target_name)
            ):
                return target_name
        return ""

    def _walk_process(
        self, process_info: ProcessInfo, target_col: int, depth: int, path: list[str]
    ):
        target_name = process_target_name(process_info.process_name)
        if target_name != self._root_target and not self._is_known_table(target_name):
            self._filtered_non_result_tables.add(target_name)
            self._debug(f"skip_target_non_result={target_name}")
            return

        visit_key = f"{process_info.source_table}|{process_info.process_name}"
        if visit_key in self._visited_processes:
            return
        self._visited_processes.add(visit_key)

        upstream_tables = sorted(extract_tables_from_code(process_info.script_code))
        filtered_upstream_tables: list[str] = []
        for upstream_table in upstream_tables:
            if is_self_lineage(upstream_table, target_name):
                self._filtered_self_lineage_count += 1
                self._debug(
                    f"filtered_self_lineage target={target_name} upstream={upstream_table}"
                )
                continue
            if not self._is_known_table(upstream_table):
                self._filtered_non_result_tables.add(upstream_table)
                self._debug(
                    f"filtered_non_result_upstream target={target_name} "
                    f"upstream={upstream_table} "
                    f"aliases={sorted(table_name_aliases(upstream_table))}"
                )
                continue
            filtered_upstream_tables.append(upstream_table)

        if not upstream_tables and target_name in self.schedule_map:
            self._warnings.append(
                f"{process_info.process_name} 未从 SCRIPT_CODE 抽到上游表，relations 仅作为提示存在。"
            )
        if upstream_tables and not filtered_upstream_tables:
            return

        process_id = self._process_id(process_info)
        process_col = target_col - 1
        self._add_process_node(process_info, process_col)
        self._add_edge(process_id, self._table_id(target_name))
        process_schedule_info = self.schedule_time_map.get(target_name)
        self._debug(
            "walk_process process={process} target={target} upstream_total={total} upstream_kept={kept} schedule_time={schedule}".format(
                process=process_info.process_name,
                target=target_name,
                total=len(upstream_tables),
                kept=len(filtered_upstream_tables),
                schedule=process_schedule_info.schedule_time
                if process_schedule_info
                else "-",
            )
        )

        for upstream_table in filtered_upstream_tables:
            table_col = process_col - 1
            terminal_upstream = is_terminal_upstream_table(upstream_table)
            producers = (
                []
                if terminal_upstream
                else dedupe_processes(self.target_map.get(upstream_table, []))
            )
            table_type = (
                "table" if producers and depth < self.max_depth else "source_table"
            )
            self._add_table_node(upstream_table, table_col, table_type)
            if table_type == "source_table":
                self._register_node_schedule(
                    self._table_id(upstream_table), process_schedule_info, fallback=True
                )
            self._add_edge(self._table_id(upstream_table), process_id)

            if terminal_upstream:
                self._terminal_dwf_count += 1
                continue
            if upstream_table in path:
                self._cycles.append(path + [upstream_table])
                continue
            if depth >= self.max_depth:
                self._truncated = True
                continue
            for producer in producers:
                self._walk_process(
                    producer,
                    target_col=table_col,
                    depth=depth + 1,
                    path=path + [upstream_table],
                )

    def _add_table_node(self, table_name: str, logical_col: int, node_type: str):
        normalized_table_name = normalize_table_name(table_name)
        node_id = self._table_id(normalized_table_name)
        old = self._nodes.get(node_id)
        if old:
            if old.type == "root_table":
                return
            if node_type == "root_table" or (
                old.type == "source_table" and node_type == "table"
            ):
                old.type = node_type
        else:
            self._nodes[node_id] = GraphNode(
                id=node_id,
                label=normalized_table_name,
                type=node_type,
                col=logical_col,
                detail={"table": normalized_table_name},
                type_order=TYPE_ORDER.get(node_type, 99),
            )
        self._nodes[node_id].type_order = TYPE_ORDER.get(self._nodes[node_id].type, 99)
        primary_schedule = (
            self._root_schedule_info
            if node_type == "root_table"
            else self.schedule_time_map.get(normalized_table_name)
        )
        self._register_node_schedule(node_id, primary_schedule)
        self._set_logical_col(node_id, logical_col)

    def _add_process_node(self, process_info: ProcessInfo, logical_col: int):
        node_id = self._process_id(process_info)
        target_name = process_target_name(process_info.process_name)
        if node_id not in self._nodes:
            self._nodes[node_id] = GraphNode(
                id=node_id,
                label=process_task_name(process_info.process_name),
                type="process",
                col=logical_col,
                detail={
                    "process_name": process_info.process_name,
                    "source_table": process_info.source_table,
                    "target_table": target_name,
                },
                type_order=TYPE_ORDER["process"],
            )
        self._register_node_schedule(node_id, self.schedule_time_map.get(target_name))
        self._set_logical_col(node_id, logical_col)

    def _set_logical_col(self, node_id: str, logical_col: int):
        if node_id not in self._logical_cols:
            self._logical_cols[node_id] = logical_col
            return
        self._logical_cols[node_id] = min(self._logical_cols[node_id], logical_col)

    def _normalize_cols(self):
        min_col = min(self._logical_cols.values()) if self._logical_cols else 0
        for node_id, node in self._nodes.items():
            node.col = self._logical_cols.get(node_id, 0) - min_col

    def _add_edge(self, from_id: str, to_id: str):
        if from_id != to_id:
            self._edges.add((from_id, to_id))

    def _is_known_table(self, table_name: str) -> bool:
        return is_known_result_table_fast(table_name, self.result_table_names)

    def _append_summary_warnings(self):
        if self._filtered_non_result_tables:
            self._warnings.append(
                f"已过滤非结果表节点 {len(self._filtered_non_result_tables)} 个，包括临时表、码值表、过程表或辅助表。"
            )
        if self._filtered_self_lineage_count:
            self._warnings.append(
                f"已过滤自调用链路 {self._filtered_self_lineage_count} 条。"
            )
        if self._terminal_dwf_count:
            self._warnings.append("上游追溯已在 DWF 层截止，未继续展开 DWO。")

    def _debug(self, message: str):
        if not DEBUG_ENABLED:
            return
        text = str(message)
        self._debug_logs.append(text)
        print(f"[lineage] {text}")

    def _register_node_schedule(
        self,
        node_id: str,
        schedule_info: ScheduleTimeInfo | None,
        fallback: bool = False,
    ):
        if schedule_info is None:
            return
        target = (
            self._node_fallback_schedule if fallback else self._node_primary_schedule
        )
        earliest = choose_earliest_schedule_time(target.get(node_id), schedule_info)
        if earliest is not None:
            target[node_id] = earliest

    def _resolve_node_schedule(self, node_id: str) -> ScheduleTimeInfo | None:
        primary = self._node_primary_schedule.get(node_id)
        if primary is not None:
            return primary
        return self._node_fallback_schedule.get(node_id)

    def _finalize_node_sorting(self):
        for node_id, node in self._nodes.items():
            schedule_info = self._resolve_node_schedule(node_id)
            node.schedule_time = (
                "" if schedule_info is None else schedule_info.schedule_time
            )
            node.sort_has_time = (
                schedule_info is not None
                and schedule_info.sort_time_seconds is not None
            )
            node.sort_time_seconds = (
                None if schedule_info is None else schedule_info.sort_time_seconds
            )
            node.type_order = TYPE_ORDER.get(node.type, 99)
            time_value = (
                node.sort_time_seconds if node.sort_time_seconds is not None else 10**9
            )
            node.sort_key = f"{0 if node.sort_has_time else 1}|{time_value:06d}|{node.type_order:02d}|{node.label}|{node.id}"
            node.detail["调度时间"] = node.schedule_time or "-"
            node.alert_rank = None
            node.alert_level = ""

        f_layer_nodes = sorted(
            (
                node
                for node in self._nodes.values()
                if node.type != "process"
                and node.sort_time_seconds is not None
                and is_f_layer_table(node.label)
            ),
            key=lambda item: (
                -(item.sort_time_seconds if item.sort_time_seconds is not None else -1),
                item.label,
                item.id,
            ),
        )
        for index, node in enumerate(f_layer_nodes[:3], start=1):
            node.alert_rank = index
            node.alert_level = {1: "red", 2: "orange", 3: "amber"}.get(index, "")
            node.detail["F层慢点预警"] = f"第 {index} 慢"

    @staticmethod
    def _node_sort_tuple(node: GraphNode) -> tuple:
        return (
            node.col,
            0 if node.sort_has_time else 1,
            node.sort_time_seconds if node.sort_time_seconds is not None else 10**9,
            node.type_order,
            node.label,
            node.id,
        )

    @staticmethod
    def _table_id(table_name: str) -> str:
        return "table:" + normalize_table_name(table_name)

    @staticmethod
    def _process_id(process_info: ProcessInfo) -> str:
        return "process:" + process_info.source_table + ":" + process_info.process_name


def build_lineage_graph(
    input_name: str,
    process_infos: Iterable[ProcessInfo],
    schedule_map: dict[str, set[str]] | None = None,
    schedule_time_map: dict[str, ScheduleTimeInfo] | None = None,
    max_depth: int = 8,
    result_table_names: Iterable[str] | None = None,
) -> tuple[LineageGraph | None, list[ProcessInfo]]:
    return PipelineLineageBuilder(
        process_infos,
        schedule_map=schedule_map,
        schedule_time_map=schedule_time_map,
        max_depth=max_depth,
        result_table_names=result_table_names,
    ).build(input_name)


def collect_lineage_table_names(graph: LineageGraph | None) -> set[str]:
    if graph is None:
        return set()
    return {
        normalize_table_name(node.label)
        for node in graph.nodes
        if node.type != "process"
        and is_valid_table_name(normalize_table_name(node.label))
    }


def build_lineage_graph_with_targeted_schedule_times(
    input_name: str,
    process_infos: Iterable[ProcessInfo],
    schedule_map: dict[str, set[str]] | None = None,
    max_depth: int = 8,
    result_table_names: Iterable[str] | None = None,
    schedule_profile: str = "demo",
    schedule_time_loader=load_schedule_time_map_for_tables,
) -> tuple[LineageGraph | None, list[ProcessInfo]]:
    perf_logs: list[str] = []
    total_start = time.perf_counter()

    stage_start = time.perf_counter()
    graph, candidates = build_lineage_graph(
        input_name,
        process_infos,
        schedule_map=schedule_map,
        schedule_time_map=None,
        max_depth=max_depth,
        result_table_names=result_table_names,
    )
    emit_perf_log(
        perf_logs,
        f"stage=build_without_schedule input={normalize_input_name(input_name)} "
        f"elapsed={format_elapsed_ms(time.perf_counter() - stage_start)} "
        f"candidates={len(candidates)} "
        f"nodes={0 if graph is None else len(graph.nodes)} "
        f"edges={0 if graph is None else len(graph.edges)}",
    )
    if candidates or graph is None:
        if graph is not None:
            graph.debug_logs = perf_logs + list(graph.debug_logs)
        return graph, candidates

    stage_start = time.perf_counter()
    table_names = collect_lineage_table_names(graph)
    emit_perf_log(
        perf_logs,
        f"stage=collect_tables elapsed={format_elapsed_ms(time.perf_counter() - stage_start)} "
        f"table_count={len(table_names)} tables={sorted(table_names)}",
    )
    if not table_names:
        graph.debug_logs = perf_logs + list(graph.debug_logs)
        return graph, []

    emit_perf_log(
        perf_logs,
        "targeted_schedule_where recv=upper(trim(r.table_name)) IN aliases; "
        f"program=normalized(p.target_table) IN aliases; "
        f"aliases={list(schedule_time_query_names(table_names))[:30]}",
    )

    stage_start = time.perf_counter()
    schedule_time_map = schedule_time_loader(table_names, profile=schedule_profile)
    emit_perf_log(
        perf_logs,
        f"stage=load_targeted_schedule "
        f"elapsed={format_elapsed_ms(time.perf_counter() - stage_start)} "
        f"schedule_rows={len(schedule_time_map)} profile={schedule_profile}",
    )
    emit_perf_log(
        perf_logs,
        f"targeted_schedule_map key_sample={sorted(schedule_time_map)[:20]}",
    )

    stage_start = time.perf_counter()
    apply_schedule_times_to_graph(graph, schedule_time_map)
    emit_perf_log(
        perf_logs,
        f"stage=apply_schedule_to_graph "
        f"elapsed={format_elapsed_ms(time.perf_counter() - stage_start)} "
        f"nodes={len(graph.nodes)} edges={len(graph.edges)}",
    )
    emit_perf_log(
        perf_logs,
        f"stage=total_targeted_schedule_build "
        f"elapsed={format_elapsed_ms(time.perf_counter() - total_start)}",
    )
    graph.debug_logs = perf_logs + list(graph.debug_logs)
    return graph, []
