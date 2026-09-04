"""搜索并展示本地 JSON schema 配置。"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pywebio.output import put_table

from shared.ui.pywebio_helper import (
    put_black_text,
    put_separator,
    put_table_plus,
    run_for_multiline_input,
    start_pywebio_app,
)

CONFIG_ROOT = Path(
    os.getenv("PYTOOLS_SCHEMA_CONFIG_ROOT", "examples/schema_config")
).expanduser()
DISPLAY_ROOT = os.getenv("PYTOOLS_SCHEMA_CONFIG_DISPLAY_ROOT", "examples/schema_config")


def build_json_table_rows(data):
    if isinstance(data, dict):
        return [["字段", "值"]] + [
            [key, "" if value is None else str(value)] for key, value in data.items()
        ]
    if isinstance(data, list):
        if not data:
            return [["结果"], ["[]"]]
        if all(isinstance(item, dict) for item in data):
            headers = sorted({key for item in data for key in item})
            return [headers] + [
                [
                    "" if item.get(header) is None else str(item.get(header, ""))
                    for header in headers
                ]
                for item in data
            ]
        return [["序号", "值"]] + [
            [index, "" if item is None else str(item)]
            for index, item in enumerate(data, start=1)
        ]
    return [["结果"], ["" if data is None else str(data)]]


def search_schema(keyword: str):
    keyword = keyword.strip().upper()
    if not keyword:
        return
    put_black_text(f"检索内容: {keyword}")
    put_separator()
    matched = False
    for file_path in sorted(CONFIG_ROOT.glob("*.json")) if CONFIG_ROOT.exists() else []:
        try:
            content = file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if keyword not in content.upper():
            continue
        matched = True
        put_table_plus([["文件"], [str(Path(DISPLAY_ROOT) / file_path.name)]])
        try:
            put_table_plus(build_json_table_rows(json.loads(content)))
        except json.JSONDecodeError:
            put_table([["结果"], ["文件不是有效 JSON"]])
        put_separator()
    if not matched:
        put_table([["结果"], ["未找到匹配结果"]])
        put_separator()


def main():
    run_for_multiline_input("请输入关键字，每行一个", search_schema)


if __name__ == "__main__":
    start_pywebio_app("JSON Schema 搜索", main)
