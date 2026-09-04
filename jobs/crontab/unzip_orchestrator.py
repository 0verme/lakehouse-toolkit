"""安全解压运行时任务包到本地目录。"""

from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

DEFAULT_ZIP_PATH = Path(
    os.getenv("PYTOOLS_JOB_BUNDLE", "runtime/input/job_bundle.zip")
).expanduser()
DEFAULT_OUTPUT_FOLDER = Path(
    os.getenv("PYTOOLS_JOB_OUTPUT_ROOT", "runtime/workspaces/jobs")
).expanduser()


def clear_folder(folder_path: str | Path):
    folder = Path(folder_path)
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True, exist_ok=True)


def extract_zip_with_custom_folder(
    zip_path: str | Path, output_folder: str | Path = DEFAULT_OUTPUT_FOLDER
) -> int:
    output = Path(output_folder).expanduser().resolve()
    clear_folder(output)
    extracted = 0
    with zipfile.ZipFile(zip_path, "r") as archive:
        for info in archive.infolist():
            target = (output / info.filename).resolve()
            if target != output and output not in target.parents:
                raise ValueError(f"不安全的 ZIP 路径: {info.filename}")
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            extracted += 1
    return extracted


if __name__ == "__main__":
    print(f"解压文件数: {extract_zip_with_custom_folder(DEFAULT_ZIP_PATH)}")
