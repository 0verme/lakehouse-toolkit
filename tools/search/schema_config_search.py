"""在本地 JSON 配置目录中搜索关键字。"""

from __future__ import annotations

import os
from pathlib import Path

from pywebio.output import put_text

from shared.ui.pywebio_helper import (
    put_black_text,
    put_separator,
    run_for_multiline_input,
    start_pywebio_app,
)

CONFIG_ROOT = Path(
    os.getenv("PYTOOLS_SCHEMA_CONFIG_ROOT", "examples/schema_config")
).expanduser()
DISPLAY_ROOT = os.getenv("PYTOOLS_SCHEMA_CONFIG_DISPLAY_ROOT", "examples/schema_config")


def search_keyword(keyword: str):
    keyword = keyword.strip().upper()
    if not keyword:
        return

    put_black_text(f"检索内容: {keyword}")
    put_separator()
    for file_path in sorted(CONFIG_ROOT.rglob("*")) if CONFIG_ROOT.exists() else []:
        if not file_path.is_file():
            continue
        try:
            content = file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if keyword in content.upper():
            put_text(f"命中: {Path(DISPLAY_ROOT) / file_path.relative_to(CONFIG_ROOT)}")
    put_separator()


def main():
    run_for_multiline_input("请输入关键字，每行一个", search_keyword)


if __name__ == "__main__":
    start_pywebio_app("JSON 配置搜索", main)
