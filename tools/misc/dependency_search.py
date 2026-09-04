"""在本地 workspace 与报表目录中搜索表名依赖。"""

from __future__ import annotations

import os
from pathlib import Path

from pywebio.output import put_text

from shared.fs.discovery import find_all_directories
from shared.ui.pywebio_helper import (
    multiline_entries,
    put_red_text,
    put_separator,
    safe_put_error,
    start_pywebio_app,
)

WORKSPACE_ROOT = Path(
    os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
).expanduser()
REPORT_ROOT = Path(os.getenv("PYTOOLS_REPORT_ROOT", "examples/reports")).expanduser()


def _search_directories(root: Path, suffixes: tuple[str, ...]):
    return [path for path in find_all_directories(str(root)) if path]


def yilai():
    workspace_dirs = _search_directories(WORKSPACE_ROOT, (".py",))
    report_dirs = _search_directories(REPORT_ROOT, (".cpt", ".frm"))
    for table_name in multiline_entries("请输入需要检索的表名，每行一张表"):
        keyword = table_name.upper()
        put_text("表名: " + keyword)
        put_separator(width=72)
        try:
            for directory in workspace_dirs:
                for filename in os.listdir(directory):
                    if not filename.endswith(".py"):
                        continue
                    file_path = Path(directory) / filename
                    content = file_path.read_text(
                        encoding="utf-8", errors="ignore"
                    ).upper()
                    if keyword in content:
                        put_text(
                            f"Workspace 路径: {file_path.relative_to(WORKSPACE_ROOT)}"
                        )
            put_separator(width=72)
            for directory in report_dirs:
                for filename in os.listdir(directory):
                    if not filename.endswith((".cpt", ".frm")):
                        continue
                    file_path = Path(directory) / filename
                    content = file_path.read_text(
                        encoding="utf-8", errors="ignore"
                    ).upper()
                    if keyword in content:
                        put_text(f"Report 路径: {file_path.relative_to(REPORT_ROOT)}")
            put_red_text(
                "==========================检查完成=============================="
            )
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("Workspace 依赖搜索", yilai)
