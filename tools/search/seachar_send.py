# !/bin/python
import json
import os

from pywebio.output import put_table, put_text

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.ui.pywebio_helper import (
    put_red_text,
    run_for_multiline_input,
    start_pywebio_app,
)

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip()


def normalize_table_name(table_name: str):
    normalized = normalize_text(table_name).upper()
    if normalized.startswith("DWS_"):
        return normalized[4:]
    return normalized


def parse_field_names(parse_json_text: str):
    parse_json_text = normalize_text(parse_json_text)
    if not parse_json_text:
        return []

    try:
        data = json.loads(parse_json_text)
    except Exception:
        data = None

    fields = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                field_name = normalize_text(
                    item.get("tar_column_name")
                    or item.get("target_column_name")
                    or item.get("column_name")
                    or item.get("name")
                )
            else:
                field_name = normalize_text(item)
            if field_name:
                fields.append(field_name)
    elif isinstance(data, dict):
        for key in ("fields", "columns", "data"):
            value = data.get(key)
            if not isinstance(value, list):
                continue
            for item in value:
                field_name = normalize_text(item)
                if field_name:
                    fields.append(field_name)

    if fields:
        return fields

    return [
        item.strip() for item in parse_json_text.split(",") if item and item.strip()
    ]


def query_send_jobs(table_name: str):
    sql = """
    SELECT send_name, job_name, target_table, field_list
    FROM __SEND_JOBS_TABLE__
    """.replace("__SEND_JOBS_TABLE__", metadata_table("send_jobs", "send_jobs"))
    rows = select_sql_with_profile(PROFILE, sql) or []

    matched_rows = []
    for send, job_name, target_name, parse_json_text in rows:
        target_table = normalize_table_name(target_name)
        if target_table != table_name:
            continue
        matched_rows.append(
            [
                normalize_text(send),
                normalize_text(job_name),
                target_table,
                len(parse_field_names(parse_json_text)),
            ]
        )

    matched_rows.sort(key=lambda item: (item[0], item[1]))
    return matched_rows


def analyze_table(table_name: str):
    table_name = normalize_table_name(table_name)
    if not table_name:
        return

    rows = query_send_jobs(table_name)
    put_red_text("================================================================")
    put_text(f"{table_name} 对应的 SEND 任务")
    if rows:
        put_table([["SEND 系统", "JOB 名称", "卸数表名", "字段数"], *rows])
    else:
        put_text("未在 demo send_jobs 中找到对应任务。")
    put_red_text("========================== 分隔线 ==============================")


def yilai():
    run_for_multiline_input("请输入表名，每行一个", analyze_table)


if __name__ == "__main__":
    start_pywebio_app("PyTool", yilai)
