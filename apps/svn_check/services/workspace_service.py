from __future__ import annotations

import os
from pathlib import Path
from typing import TypedDict

IGNORED_WORKSPACE_DIRS = {
    ".git",
    ".svn",
    "__pycache__",
    ".idea",
    ".vscode",
}


class WorkspaceInfo(TypedDict):
    source_type: str
    source_label: str
    workspace_root: str
    exported_paths: list[str]
    branch_changed_files: list[str]
    trunk_changed_files: list[str]
    trunk_conflict_files: list[str]


def _build_workspace_info(
    *,
    source_type: str,
    source_label: str,
    workspace_root: str,
    exported_paths: list[str],
    branch_changed_files: list[str],
    trunk_changed_files: list[str],
    trunk_conflict_files: list[str],
) -> WorkspaceInfo:
    return {
        "source_type": source_type,
        "source_label": source_label,
        "workspace_root": workspace_root,
        "exported_paths": exported_paths,
        "branch_changed_files": branch_changed_files,
        "trunk_changed_files": trunk_changed_files,
        "trunk_conflict_files": trunk_conflict_files,
    }


def load_local_workspace(local_dir: str) -> WorkspaceInfo:
    workspace_root = Path(local_dir).expanduser()
    if not workspace_root.exists():
        raise FileNotFoundError(f"Local workspace does not exist: {local_dir}")
    if not workspace_root.is_dir():
        raise NotADirectoryError(f"Local workspace is not a directory: {local_dir}")

    workspace_root = workspace_root.resolve()
    relative_file_paths: list[str] = []

    for current_root, dir_names, file_names in os.walk(workspace_root, topdown=True):
        dir_names[:] = sorted(
            name for name in dir_names if name not in IGNORED_WORKSPACE_DIRS
        )
        for file_name in sorted(file_names):
            file_path = Path(current_root) / file_name
            relative_file_paths.append(
                file_path.resolve().relative_to(workspace_root).as_posix()
            )

    relative_file_paths.sort()
    if not relative_file_paths:
        raise ValueError(f"Local workspace has no files to audit: {workspace_root}")

    branch_changed_files = relative_file_paths
    exported_paths = [
        str((workspace_root / relative_path).resolve())
        for relative_path in relative_file_paths
    ]

    return _build_workspace_info(
        source_type="local",
        source_label=str(workspace_root),
        workspace_root=str(workspace_root),
        exported_paths=exported_paths,
        branch_changed_files=branch_changed_files,
        trunk_changed_files=[],
        trunk_conflict_files=[],
    )


def load_svn_workspace(project: str, branch_url: str) -> WorkspaceInfo:
    from services.svn_service import svn_main

    svn_result = svn_main(project, branch_url)
    exported_paths = svn_result["exported_paths"]
    resolved_exported_paths = [
        str(Path(path_str).resolve()) for path_str in exported_paths
    ]
    workspace_root = ""
    if resolved_exported_paths:
        workspace_root = os.path.commonpath(resolved_exported_paths)

    return _build_workspace_info(
        source_type="svn",
        source_label=branch_url,
        workspace_root=workspace_root,
        exported_paths=resolved_exported_paths,
        branch_changed_files=svn_result["branch_changed_files"],
        trunk_changed_files=svn_result["trunk_changed_files"],
        trunk_conflict_files=svn_result["trunk_conflict_files"],
    )
