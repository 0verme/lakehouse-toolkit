"""按文件列表执行本地 demo SQL。"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from shared.db.gaussdb import run_sql_with_profile

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
SQL_ROOT = Path(os.getenv("PYTOOLS_SQL_INPUT_ROOT", "runtime/input/sql")).expanduser()


def read_files_in_directory(directory: str | Path, filenames: list[str]) -> int:
    executed = 0
    for filename in filenames:
        path = Path(directory) / filename
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            print(f"File not found: {path}")
            continue
        run_sql_with_profile(PROFILE, content)
        executed += 1
    return executed


def main() -> int:
    filenames = sorted(path.name for path in SQL_ROOT.glob("*.sql"))
    print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
    count = read_files_in_directory(SQL_ROOT, filenames)
    print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
    return count


if __name__ == "__main__":
    print(f"执行 SQL 文件数: {main()}")
