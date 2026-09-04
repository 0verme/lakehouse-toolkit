"""从可选 MySQL metadata source 搜索任务脚本依赖。"""

from __future__ import annotations

import os

from pywebio.input import TEXT, checkbox, input_group, textarea
from pywebio.output import (
    put_file,
    put_progressbar,
    put_table,
    put_text,
    set_progressbar,
)

from shared.config.env import required_env
from shared.config.metadata import table as metadata_table
from shared.ui.pywebio_helper import put_red_text, start_pywebio_app

PROCESS_SQL = """
select process_name, script_code
from __PROCESS_TABLE__
where script_code is not null
""".replace("__PROCESS_TABLE__", metadata_table("processes", "processes"))


def get_db():
    import pymysql

    return pymysql.connect(
        host=os.getenv("PYTOOLS_MYSQL_HOST", "localhost"),
        user=required_env("PYTOOLS_MYSQL_USER"),
        password=required_env("PYTOOLS_MYSQL_PASSWORD"),
        database=os.getenv("PYTOOLS_MYSQL_DATABASE", "pytools_demo"),
        charset="utf8mb4",
        autocommit=True,
    )


def select_mysql_sql(sql):
    db = get_db()
    try:
        db.ping(reconnect=True)
        cursor = db.cursor()
        try:
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        db.close()


def decode_code(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def yilai():
    info = input_group(
        "请输入需要检索的任务关键字",
        [
            textarea("支持多关键字，逗号分隔", name="table_list", type=TEXT),
            checkbox(
                "如果关键字本身包含逗号，请改用 @#@ 作为分隔符",
                options=["是"],
                name="split_marker",
                value=[],
            ),
        ],
    )
    keyword_lines = str(info.get("table_list") or "").splitlines()
    split_marker = info.get("split_marker") == ["是"]

    put_progressbar("progress", 0)
    process_rows = select_mysql_sql(PROCESS_SQL)
    set_progressbar("progress", 0.1)
    for raw_value in keyword_lines:
        keyword_text = raw_value.strip().replace("，", ",")
        if not keyword_text:
            continue
        put_text("关键字: " + keyword_text.upper())
        keywords = keyword_text.split("@#@" if split_marker else ",")
        keywords = sorted(set(filter(None, (item.strip() for item in keywords))))
        result_map = {}
        for process_name, script_code in process_rows:
            code_text = decode_code(script_code)
            if all(word.upper() in code_text.upper() for word in keywords):
                result_map[str(process_name)] = code_text.encode("utf-8")

        rows = [["匹配任务", "代码下载"]]
        for process_name in sorted(result_map):
            rows.append(
                [
                    process_name,
                    put_file(process_name + ".txt", result_map[process_name]),
                ]
            )
        put_table(rows)
        put_red_text("=" * 61)
    set_progressbar("progress", 1)
    put_red_text(
        "========================== 全部检查完成 =============================="
    )


if __name__ == "__main__":
    start_pywebio_app("任务脚本依赖搜索", yilai)
