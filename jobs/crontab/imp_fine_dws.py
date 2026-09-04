"""导入公开 demo 的角色和报表目录 Excel。"""

from __future__ import annotations

import os
from pathlib import Path

import xlrd

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import run_sql_with_profile

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
INPUT_FILE = Path(
    os.getenv("PYTOOLS_REPORT_CATALOG_FILE", "runtime/input/report_catalog.xls")
).expanduser()
ROLE_TABLE = metadata_table("roles", "roles")
REPORT_TABLE = metadata_table("reports", "reports")


def import_catalog(path: str | Path = INPUT_FILE) -> None:
    workbook = xlrd.open_workbook(str(path))
    role_sheet = workbook.sheet_by_index(2)
    report_sheet = workbook.sheet_by_index(1)

    role_insert_sql = "INSERT INTO __ROLE_TABLE__(role_name) VALUES (?)".replace(
        "__ROLE_TABLE__", ROLE_TABLE
    )
    report_insert_sql = (
        "INSERT INTO __REPORT_TABLE__(report_name, report_path) VALUES (?, ?)".replace(
            "__REPORT_TABLE__", REPORT_TABLE
        )
    )
    run_sql_with_profile(
        PROFILE, "TRUNCATE TABLE __ROLE_TABLE__;".replace("__ROLE_TABLE__", ROLE_TABLE)
    )
    run_sql_with_profile(
        PROFILE,
        "TRUNCATE TABLE __REPORT_TABLE__;".replace("__REPORT_TABLE__", REPORT_TABLE),
    )
    for row_index in range(1, role_sheet.nrows):
        run_sql_with_profile(
            PROFILE,
            role_insert_sql,
            (str(role_sheet.cell(row_index, 0).value or ""),),
        )
    for row_index in range(1, report_sheet.nrows):
        run_sql_with_profile(
            PROFILE,
            report_insert_sql,
            (
                str(report_sheet.cell(row_index, 2).value or ""),
                str(report_sheet.cell(row_index, 3).value or ""),
            ),
        )


if __name__ == "__main__":
    import_catalog()
