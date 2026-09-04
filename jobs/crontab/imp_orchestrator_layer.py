"""从调度 Excel 中导入公开 demo 的输出路径索引。"""

from __future__ import annotations

import os
import re
from pathlib import Path

import xlrd

from shared.config.env import safe_identifier
from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import run_sql_with_profile

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
INPUT_ROOT = Path(
    os.getenv("PYTOOLS_SCHEDULE_INPUT_ROOT", "runtime/input/schedule")
).expanduser()
OUTPUT_TABLE = safe_identifier(
    os.getenv("PYTOOLS_JOB_OUTPUT_TABLE", metadata_table("job_outputs", "job_outputs")),
    "PYTOOLS_JOB_OUTPUT_TABLE",
)


def extract_values(input_string) -> str:
    match = re.search(r"outfile=([^:]+)", str(input_string or ""))
    return match.group(1) if match else ""


def read_columns_from_recv_files(
    directory: str | Path = INPUT_ROOT,
) -> list[tuple[str, str]]:
    rows = []
    for file_path in sorted(Path(directory).glob("*.xls")):
        if "RECV" not in file_path.name.upper():
            continue
        workbook = xlrd.open_workbook(str(file_path))
        sheet = workbook.sheet_by_index(0)
        headers = [str(value).strip() for value in sheet.row_values(0)]
        job_index = headers.index("JOB_NAME") if "JOB_NAME" in headers else 2
        output_index = headers.index("OUTPUT_PATH") if "OUTPUT_PATH" in headers else 25
        for row_index in range(1, sheet.nrows):
            values = sheet.row_values(row_index)
            if len(values) <= max(job_index, output_index):
                continue
            output_path = extract_values(values[output_index])
            if output_path:
                rows.append((str(values[job_index]).strip(), output_path))
    return rows


def build_insert_sql(rows: list[tuple[str, str]]) -> tuple[str, list[tuple[str, str]]]:
    insert_sql = (
        "INSERT INTO __OUTPUT_TABLE__ (job_name, output_path) VALUES (?, ?)".replace(
            "__OUTPUT_TABLE__", OUTPUT_TABLE
        )
    )
    return insert_sql, [
        (str(job_name), str(output_path)) for job_name, output_path in rows
    ]


def main(directory: str | Path = INPUT_ROOT) -> int:
    rows = read_columns_from_recv_files(directory)
    run_sql_with_profile(
        PROFILE,
        "TRUNCATE TABLE __OUTPUT_TABLE__;".replace("__OUTPUT_TABLE__", OUTPUT_TABLE),
    )
    if rows:
        insert_sql, insert_params = build_insert_sql(rows)
        for params in insert_params:
            run_sql_with_profile(PROFILE, insert_sql, params)
    return len(rows)


if __name__ == "__main__":
    print(f"导入输出路径数: {main()}")
