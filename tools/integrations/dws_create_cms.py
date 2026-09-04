from importlib import import_module

from shared.config.env import safe_identifier
from shared.db.gaussdb import connect_with_profile, load_db_profiles
from shared.ui.pywebio_helper import (
    iter_nonempty_lines,
    safe_put_error,
    start_pywebio_app,
)


def get_cms_profiles():
    return [
        name for name in sorted(load_db_profiles()) if name.lower().startswith("cms")
    ]


def dws_create():
    pywebio_input = import_module("pywebio.input")
    pywebio_output = import_module("pywebio.output")
    text_type = pywebio_input.TEXT
    input_group = pywebio_input.input_group
    radio = pywebio_input.radio
    textarea = pywebio_input.textarea
    put_error = pywebio_output.put_error
    put_text = pywebio_output.put_text

    cms_profiles = get_cms_profiles()
    if not cms_profiles:
        put_error("未在 configs/database.local.yaml 或环境变量中找到 demo 数据库配置")
        return

    info = input_group(
        "生成 CMS 建表 SQL",
        [
            textarea("请输入表名，每行一个", name="table_list", type=text_type),
            radio(
                "选择目标库",
                name="profile",
                options=[name.upper() for name in cms_profiles],
                value=cms_profiles[0].upper(),
            ),
        ],
    )

    selected_profile = info["profile"].lower()
    conn = connect_with_profile(selected_profile)
    curs = conn.cursor()
    try:
        for table_name in iter_nonempty_lines(info["table_list"]):
            try:
                safe_table_name = safe_identifier(table_name, "table_name")
                curs.execute("select PG_GET_TABLEDEF(?)", (safe_table_name,))
                result = curs.fetchall()
                create_sql = result[0][0]
                put_text(
                    f"drop table if exists {safe_table_name};\n"
                    + create_sql.replace("TO GROUP group_version1", "")
                )
            except Exception as exc:
                safe_put_error(exc)
    finally:
        curs.close()
        conn.close()


if __name__ == "__main__":
    start_pywebio_app("PyTool", dws_create)
