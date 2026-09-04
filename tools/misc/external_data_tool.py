import os
import time
from importlib import import_module

from shared.db.gaussdb import fetch_all
from shared.log import get_logger
from shared.ui.pywebio_helper import put_black_text, safe_put_error, start_pywebio_app

logger = get_logger(__name__, log_file="external_data_tool.log")
PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
READ_ONLY_SQL_KEYWORDS = {"select", "show", "describe", "desc", "explain"}


def validate_read_only_sql(sql_text: str) -> str:
    statement = str(sql_text or "").strip()
    if not statement:
        raise ValueError("SQL 不能为空")
    statement = statement.rstrip(";").strip()
    if not statement or ";" in statement:
        raise ValueError("仅允许执行单条只读 SQL")
    keyword = statement.split(None, 1)[0].lower()
    if keyword not in READ_ONLY_SQL_KEYWORDS:
        raise ValueError("该工具仅允许 SELECT/SHOW/DESCRIBE/EXPLAIN 查询")
    return statement


def app():
    pywebio_input = import_module("pywebio.input")
    pywebio_output = import_module("pywebio.output")
    textarea = pywebio_input.textarea
    text_type = pywebio_input.TEXT
    put_text = pywebio_output.put_text
    session_info = import_module("pywebio.session").info

    try:
        sql_text = validate_read_only_sql(
            textarea("请输入要执行的 SQL", type=text_type)
        )
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        ip = session_info.user_ip
        logger.info("来源 IP: %s", ip)
        logger.info("收到 SQL 请求，长度=%s", len(sql_text))
        put_black_text(f"[{timestamp}] 来源 IP: {ip}")
        put_text("SQL: " + sql_text)
        rows = fetch_all(PROFILE, sql_text) or []
        result = f"返回 {len(rows)} 行"
        put_text("执行结果: " + result)
        logger.info("执行结果: %s", result)
    except Exception as exc:
        safe_put_error(exc)
        logger.exception("执行只读 SQL 失败")


if __name__ == "__main__":
    start_pywebio_app("PyTool", app)
