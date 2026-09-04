"""外部来源脚本的通用规则适配层。"""

from pathlib import Path

from services.re_service import match_path

from core.lakehouse.python_rule import rule_dws_py as _rule_dws_py
from core.lakehouse.sql_rule import rule_dws as _rule_dws

UPSTREAM_SQL_NAMES = {
    "schema.sql",
    "catalog.sql",
    "source.sql",
    "upstream.sql",
}


def is_upstream_py(file_path):
    return match_path(
        file_path, "**/UPSTREAM_DATA/**/*.py"
    ) or file_path.lower().endswith(".py")


def get_upstream_type(file_paths):
    sql_files = []
    python_files = []
    for file_path in file_paths:
        if Path(file_path).name.lower() in UPSTREAM_SQL_NAMES:
            sql_files.append(file_path)
        elif is_upstream_py(file_path):
            python_files.append(file_path)
    return sorted(sql_files), sorted(python_files)


def rule_dws(dws_url):
    result_text, warning_text, count = _rule_dws(dws_url)
    return result_text + warning_text, count


def rule_dws_py(dws_url):
    return _rule_dws_py(dws_url)
