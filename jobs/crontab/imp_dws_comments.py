"""从本地示例 SQL 生成字段注释映射。

本脚本只处理运行时传入的 workspace，不包含任何固定服务器路径或生产元数据名称。
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from _bootstrap import ensure_project_root_on_path

# Direct script execution needs the bootstrap before project-local imports.
# ruff: noqa: E402, I001
ensure_project_root_on_path()

from shared.config.env import safe_identifier  # noqa: E402
from shared.config.metadata import table as metadata_table  # noqa: E402
from shared.db.gaussdb import run_sql_with_profile, select_sql_with_profile  # noqa: E402


WORKSPACE_ROOT = Path(
    os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
).expanduser()
PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
MAPPING_TABLE = os.getenv(
    "PYTOOLS_MAPPING_TABLE",
    metadata_table("relations", "asset_mappings"),
)


def extract_table_mapping(sql: str) -> dict[str, str]:
    compact = re.sub(r"/\*.*?\*/|--.*?$", "", sql or "", flags=re.DOTALL | re.MULTILINE)
    compact = re.sub(r"\s+", " ", compact).strip()
    insert_match = re.search(r'INSERT\s+INTO\s+([\w.$"]+)', compact, re.IGNORECASE)
    from_match = re.search(r'\bFROM\s+([\w.$"]+)', compact, re.IGNORECASE)
    if not insert_match or not from_match:
        raise ValueError("无法从 SQL 中找到 INSERT INTO 和 FROM 表")
    return {
        "source": from_match.group(1).strip('"; ').upper(),
        "target": insert_match.group(1).strip('"; ').upper(),
    }


def extract_insert_select_mapping(sql: str) -> dict[str, str]:
    compact = re.sub(r"/\*.*?\*/|--.*?$", "", sql or "", flags=re.DOTALL | re.MULTILINE)
    compact = re.sub(r"\s+", " ", compact).strip()
    match = re.search(
        r"INSERT\s+INTO\s+[\w.]+\s*\((.*?)\)\s*SELECT\s+(.*?)\s+FROM",
        compact,
        re.IGNORECASE,
    )
    if not match:
        return {}
    targets = [
        item.strip().upper() for item in match.group(1).split(",") if item.strip()
    ]
    sources = [
        item.strip().split()[-1].upper()
        for item in match.group(2).split(",")
        if item.strip()
    ]
    return {
        source: target
        for source, target in zip(sources, targets, strict=False)
        if re.fullmatch(r"[A-Z_][A-Z0-9_.]*", source)
    }


def find_first_insert_sql(file_path: str | Path) -> str:
    content = Path(file_path).read_text(encoding="utf-8")
    match = re.search(r"INSERT\s+INTO.*?(?:;|\Z)", content, re.IGNORECASE | re.DOTALL)
    return match.group(0) if match else ""


def find_py_files(directory: str | Path) -> list[Path]:
    root = Path(directory)
    return (
        sorted(path for path in root.rglob("*.py") if path.is_file())
        if root.exists()
        else []
    )


def extract_function_description(file_path: str | Path) -> str:
    content = Path(file_path).read_text(encoding="utf-8")
    match = re.search(r"功能描述:\s*(.+)", content)
    return match.group(1).strip() if match else ""


def build_mapping_insert_sql(
    rows: list[dict[str, str]],
) -> tuple[str, list[tuple[str, ...]]]:
    mapping_table = safe_identifier(MAPPING_TABLE, "mapping table")
    insert_sql = (
        "INSERT INTO __MAPPING_TABLE__ "
        "(source_table, target_table, source_column, target_column, description) "
        "VALUES (?, ?, ?, ?, ?)"
    ).replace("__MAPPING_TABLE__", mapping_table)
    params = [
        tuple(
            str(row.get(key, ""))
            for key in (
                "source_table",
                "target_table",
                "source_column",
                "target_column",
                "description",
            )
        )
        for row in rows
    ]
    return insert_sql, params


def main(directory: str | Path = WORKSPACE_ROOT):
    result_receipts = metadata_table("result_receipts", "result_receipts")
    plan_sql = "select receive_plan, table_name from __RESULT_RECEIPTS__".replace(
        "__RESULT_RECEIPTS__", result_receipts
    )
    plan_rows = select_sql_with_profile(PROFILE, plan_sql) or []
    plan_by_table = {
        str(row[1]).upper(): str(row[0]) for row in plan_rows if len(row) >= 2
    }

    mapping_rows = []
    for file_path in find_py_files(directory):
        sql = find_first_insert_sql(file_path)
        if not sql:
            continue
        try:
            tables = extract_table_mapping(sql)
        except ValueError:
            continue
        description = extract_function_description(file_path)
        for source_column, target_column in extract_insert_select_mapping(sql).items():
            mapping_rows.append(
                {
                    "source_table": tables["source"],
                    "target_table": tables["target"],
                    "source_column": source_column,
                    "target_column": target_column,
                    "description": description
                    or plan_by_table.get(tables["target"], ""),
                }
            )

    if mapping_rows:
        mapping_table = safe_identifier(MAPPING_TABLE, "mapping table")
        run_sql_with_profile(
            PROFILE,
            "truncate table __MAPPING_TABLE__;".replace(
                "__MAPPING_TABLE__", mapping_table
            ),
        )
        insert_sql, insert_params = build_mapping_insert_sql(mapping_rows)
        for params in insert_params:
            run_sql_with_profile(PROFILE, insert_sql, params)
    return mapping_rows


if __name__ == "__main__":
    main()
