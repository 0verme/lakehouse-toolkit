# !/bin/python
from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from pathlib import Path


def find_all_directories(path, excluded_fragment: str = ".svn/text-base") -> list[str]:
    directories = []
    for root, dirs, _files in os.walk(path):
        for directory in dirs:
            full_directory = str(Path(root) / directory)
            if excluded_fragment and excluded_fragment in full_directory:
                continue
            directories.append(full_directory)
    return directories


def find_all_directories_in_paths(
    paths: Iterable[str],
    excluded_fragment: str = ".svn",
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[str]:
    normalized_paths = [os.path.normpath(os.path.abspath(path)) for path in paths]
    total_count = 0
    for target_dir in normalized_paths:
        if os.path.exists(target_dir) and os.path.isdir(target_dir):
            for _dirpath, dirnames, _ in os.walk(target_dir):
                total_count += len([d for d in dirnames if excluded_fragment not in d])

    all_subdirs = []
    index = 0
    for target_dir in normalized_paths:
        if not (os.path.exists(target_dir) and os.path.isdir(target_dir)):
            continue
        for dirpath, dirnames, _ in os.walk(target_dir):
            dirnames[:] = [d for d in dirnames if excluded_fragment not in d]
            for dirname in dirnames:
                subdir_path = os.path.join(dirpath, dirname)
                all_subdirs.append(subdir_path)
                index += 1
                if progress_callback is not None:
                    progress_callback(index, total_count)
    return all_subdirs
