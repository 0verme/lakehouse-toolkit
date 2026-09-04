"""把本地 JSON 配置导入可选的 demo metadata table。"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

from shared.config.env import safe_identifier
from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import run_sql_with_profile

BASE_DIR = Path(
    os.getenv("PYTOOLS_SCHEMA_CONFIG_ROOT", "examples/schema_config")
).expanduser()
TABLE_NAME = safe_identifier(
    os.getenv(
        "PYTOOLS_SCHEMA_CONFIG_TABLE", metadata_table("schema_config", "schema_config")
    ),
    "PYTOOLS_SCHEMA_CONFIG_TABLE",
)
PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")


def scan_json_files(base_dir: str | Path = BASE_DIR) -> list[Path]:
    root = Path(base_dir)
    return (
        sorted(path for path in root.rglob("*.json") if path.is_file())
        if root.exists()
        else []
    )


def analyze_fields(json_files: Sequence[str | Path]):
    field_counter = defaultdict(int)
    records = []
    for path in json_files:
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            print(f"[ERROR] {path}: {exc}")
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            records.append((str(path), item))
            for key in item:
                field_counter[str(key)] += 1
    return field_counter, records


def get_fields(field_counter) -> list[str]:
    return [
        key
        for key, _ in sorted(
            field_counter.items(), key=lambda item: (-item[1], item[0])
        )
    ]


def build_create_table_sql(table_name: str, fields: list[str]) -> str:
    table_name = safe_identifier(table_name, "table")
    columns = ['"source_file" TEXT'] + [
        f'"{safe_identifier(field, "column")}" TEXT' for field in fields
    ]
    return f"CREATE TABLE {table_name} (\n    {',\n    '.join(columns)}\n);"


def truncate_table(table_name: str = TABLE_NAME):
    safe_table_name = safe_identifier(table_name, "table")
    run_sql_with_profile(
        PROFILE,
        "TRUNCATE TABLE __TABLE__".replace("__TABLE__", safe_table_name),
    )


def insert_records(table_name: str, fields: list[str], records):
    table_name = safe_identifier(table_name, "table")
    column_names = ["source_file"] + fields
    columns = ",".join(
        f'"{safe_identifier(column, "column")}"' for column in column_names
    )
    for source_file, item in records:
        values = [source_file] + [
            json.dumps(item.get(field), ensure_ascii=False)
            if isinstance(item.get(field), (dict, list))
            else item.get(field)
            for field in fields
        ]
        insert_sql = (
            ("INSERT INTO __TABLE__ (__COLUMNS__) VALUES (__PLACEHOLDERS__)")
            .replace("__TABLE__", table_name)
            .replace("__COLUMNS__", columns)
            .replace("__PLACEHOLDERS__", ",".join("?" for _ in values))
        )
        run_sql_with_profile(PROFILE, insert_sql, tuple(values))


def main(base_dir: str | Path = BASE_DIR):
    json_files = scan_json_files(base_dir)
    field_counter, records = analyze_fields(json_files)
    fields = get_fields(field_counter)
    print(build_create_table_sql(TABLE_NAME, fields))
    truncate_table(TABLE_NAME)
    insert_records(TABLE_NAME, fields, records)
    return len(records)


if __name__ == "__main__":
    print(f"导入记录数: {main()}")
