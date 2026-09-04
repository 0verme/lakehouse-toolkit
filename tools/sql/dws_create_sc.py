from importlib import import_module

from shared.config.env import safe_identifier
from shared.db.gaussdb import select_sql_with_profile
from shared.ui.pywebio_helper import (
    multiline_entries,
    safe_put_error,
    start_pywebio_app,
)


def _get_view_def(profile, table_name):
    safe_table_name = safe_identifier(table_name, "table_name")
    parts = safe_table_name.split(".")
    if len(parts) == 2:
        sql = "select definition from pg_views where schemaname = ? and viewname = ?"
        params = (parts[0], parts[1])
    else:
        sql = "select definition from pg_views where viewname = ?"
        params = (safe_table_name,)
    result = select_sql_with_profile(profile, sql, params) or []
    return result[0][0] if result and result[0][0] else None


def _get_foreign_table_def(profile, table_name):
    safe_table_name = safe_identifier(table_name, "table_name")
    parts = safe_table_name.split(".")
    if len(parts) == 2:
        where = "n.nspname = ? AND c.relname = ?"
        params = (parts[0], parts[1])
    else:
        where = "c.relname = ?"
        params = (safe_table_name,)
    sql = """
SELECT 'CREATE FOREIGN TABLE ' || n.nspname || '.' || c.relname || E'\\n(\\n' ||
    string_agg('    ' || a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod),
               E',\\n' ORDER BY a.attnum) ||
    E'\\n)\\nSERVER ' || fs.srvname ||
    CASE WHEN ft.ftoptions IS NOT NULL
         THEN E'\\nOPTIONS (' || array_to_string(ft.ftoptions, ', ') || ')'
         ELSE '' END || ';'
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_foreign_table ft ON c.oid = ft.ftrelid
JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum > 0 AND NOT a.attisdropped
WHERE __WHERE__
GROUP BY n.nspname, c.relname, fs.srvname, ft.ftoptions
""".replace("__WHERE__", where)
    result = select_sql_with_profile(profile, sql, params) or []
    return result[0][0] if result and result[0][0] else None


def dws_create():
    put_text = import_module("pywebio.output").put_text
    for raw_table_name in multiline_entries("请输入表名，每行一个"):
        try:
            table_name = safe_identifier(raw_table_name.lower(), "table_name")
            result = (
                select_sql_with_profile(
                    "demo_sc",
                    "select PG_GET_TABLEDEF(?)",
                    (table_name,),
                )
                or []
            )
            create_sql = result[0][0] if result and result[0] else None

            if create_sql:
                put_text(
                    f"drop table if exists {table_name};\n"
                    + create_sql.replace("TO GROUP group_version1", "")
                )
                continue

            view_def = _get_view_def("demo_sc", table_name)
            if view_def:
                put_text(
                    f"drop view if exists {table_name};\n"
                    f"create or replace view {table_name} as\n{view_def};"
                )
                continue

            ft_def = _get_foreign_table_def("demo_sc", table_name)
            if ft_def:
                put_text(f"drop foreign table if exists {table_name};\n{ft_def}")
                continue

            safe_put_error(
                ValueError(f"对象 {table_name} 不存在（非表、非视图、非外表）")
            )
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("PyTool", dws_create)
