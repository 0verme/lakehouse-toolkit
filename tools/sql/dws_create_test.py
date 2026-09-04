"""兼容入口：复用安全的 demo 对象定义生成工具。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.ui.pywebio_helper import start_pywebio_app  # noqa: E402
from tools.sql.dws_create_sc import dws_create  # noqa: E402

if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", dws_create)
