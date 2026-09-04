from importlib import import_module

from shared.config.env import safe_identifier
from shared.db.gaussdb import select_sql_with_profile
from shared.text.regex import fbj_rule
from shared.ui.pywebio_helper import (
    multiline_entries,
    safe_put_error,
    start_pywebio_app,
)


def dws_create():
    put_text = import_module("pywebio.output").put_text
    for table_name in multiline_entries("请输入表名，每行一个"):
        try:
            safe_table_name = safe_identifier(table_name.lower(), "table_name")
            result = select_sql_with_profile(
                "demo_sc",
                "select PG_GET_TABLEDEF(?)",
                (safe_table_name,),
            )
            create_sql = result[0][0] if result and result[0] else None
            if not create_sql:
                safe_put_error(ValueError(f"对象 {safe_table_name} 不存在"))
                continue
            put_text(f"{safe_table_name.split('.')[-1]}\t{fbj_rule(create_sql)}")
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", dws_create)
