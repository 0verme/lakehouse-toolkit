from importlib import import_module

from shared.config.env import safe_identifier
from shared.db.gaussdb import select_sql_with_profile
from shared.ui.pywebio_helper import (
    multiline_entries,
    safe_put_error,
    start_pywebio_app,
)


def dws_create():
    put_text = import_module("pywebio.output").put_text
    for raw_table_name in multiline_entries("请输入表名，每行一个"):
        try:
            table_name = safe_identifier(raw_table_name.lower(), "table_name")
            table_bak = safe_identifier(f"{table_name}_bak", "backup table")
            raw_name = table_name.split(".")[-1]
            result = (
                select_sql_with_profile(
                    "demo_sc",
                    "select PG_GET_TABLEDEF(?)",
                    (table_name,),
                )
                or []
            )
            create_sql = result[0][0] if result and result[0] else None
            if not create_sql:
                safe_put_error(ValueError(f"对象 {table_name} 不存在"))
                continue

            create_sql = create_sql.replace(raw_name, f"{raw_name}_bak").replace(
                "TO GROUP group_version1", ""
            )
            put_text(f"drop table if exists {table_bak};\n{create_sql}")
            put_text(
                """insert into __TABLE_BAK__ select * from __TABLE__;
                alter table __TABLE__ rename to __RAW_NAME___bak1;
                alter table __TABLE_BAK__ rename to __RAW_NAME__;
                drop table if exists __TABLE___bak1;""".replace(
                    "__TABLE_BAK__", table_bak
                )
                .replace("__TABLE__", table_name)
                .replace("__RAW_NAME__", raw_name)
            )
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", dws_create)
