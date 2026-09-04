"""生成本地 workspace 的目录索引。"""

from __future__ import annotations

import os
from pathlib import Path

WORKSPACE_ROOT = Path(
    os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
).expanduser()
DIRECTORY_INDEX_PATH = Path(
    os.getenv("PYTOOLS_DIRECTORY_INDEX_PATH", "runtime/cache/directories.txt")
).expanduser()


def find_and_write_directories(
    target_dirs: list[str | Path], output_file: str | Path
) -> int:
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as file:
        for target_dir in target_dirs:
            target_path = Path(target_dir).expanduser()
            if not target_path.is_dir():
                continue
            for path in sorted(target_path.rglob("*")):
                if path.is_dir() and ".svn" not in path.parts:
                    file.write(f"{path.resolve()}\n")
                    count += 1
    return count


def main() -> int:
    return find_and_write_directories([WORKSPACE_ROOT], DIRECTORY_INDEX_PATH)


if __name__ == "__main__":
    print(f"indexed directories: {main()}")
