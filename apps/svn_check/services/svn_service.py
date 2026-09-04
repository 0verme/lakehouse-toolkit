from __future__ import annotations

import os
import subprocess
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote, unquote

import yaml

from services.diag_service import log_exception_event, log_task_event, log_warning_event
from services.re_service import get_export_base
from shared.config.env import required_env

ROOT_DIR = Path(__file__).resolve().parents[3]
CONFIG_PATH = ROOT_DIR / "configs" / "svn.local.yaml"
EXAMPLE_CONFIG_PATH = ROOT_DIR / "configs" / "svn.example.yaml"
DEFAULT_SVN_BIN = "svn"


def _parse_svn_xml(xml_text: str | bytes) -> ET.Element:
    """解析 SVN 返回的 XML，并拒绝 DTD/entity 声明。"""
    payload = xml_text if isinstance(xml_text, bytes) else xml_text.encode("utf-8")
    upper_payload = payload.upper()
    if b"<!DOCTYPE" in upper_payload or b"<!ENTITY" in upper_payload:
        raise ValueError("SVN XML DTD/entity declarations are not allowed")
    return ET.fromstring(payload)  # noqa: S314 - DTD/entity input is rejected above.


def build_compare_url(base_url: str, revision: str | None = None) -> str:
    plain_url = strip_peg_revision(base_url)
    if revision:
        return f"{plain_url}@{revision}"
    return plain_url


def expand_env_value(value):
    if not isinstance(value, str):
        return value
    if value.startswith("${") and value.endswith("}"):
        return os.getenv(value[2:-1], "")
    return value


def expand_env_config(config):
    return {key: expand_env_value(value) for key, value in config.items()}


def load_svn_config() -> dict:
    config_path = CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH
    try:
        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"无法读取 SVN 配置: {config_path}") from exc
    defaults = expand_env_config(data.get("defaults", {}))
    projects = data.get("projects", {})
    merged_projects = {}
    for name, project in projects.items():
        config = dict(defaults)
        config.update(expand_env_config(project or {}))
        merged_projects[name] = config
    return {"defaults": defaults, "projects": merged_projects}


def get_project_config(project: str) -> dict:
    config = load_svn_config()
    projects = config.get("projects", {})
    if project not in projects:
        normalized_project = str(project).lower()
        for project_name, project_config in projects.items():
            if str(project_name).lower() == normalized_project:
                return project_config
        raise KeyError(
            f"svn project not found: {project}, config: {CONFIG_PATH}, projects: {list(projects)}"
        )
    return projects[project]


def decode_svn_output(data: bytes) -> str:
    for encoding in ("gbk", "utf-8"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def build_svn_command(project_config: dict, *args: str) -> list[str]:
    command = [project_config.get("svn_bin", DEFAULT_SVN_BIN), *args]
    if project_config.get("non_interactive", True):
        command.append("--non-interactive")
    if project_config.get("trust_server_cert", False):
        command.append("--trust-server-cert")

    username_env = str(project_config.get("username_env", "") or "").strip()
    password_env = str(project_config.get("password_env", "") or "").strip()
    username = required_env(username_env) if username_env else ""
    password = required_env(password_env) if password_env else ""
    if username:
        command.extend(["--username", username])
    if password:
        command.extend(["--password", password])
    return command


def redact_command(command: list[str]) -> list[str]:
    redacted = list(command)
    for index, value in enumerate(redacted[:-1]):
        if value in {"--password", "--username"}:
            redacted[index + 1] = "<redacted>"
    return redacted


def run_svn_text(project_config: dict, *args: str) -> tuple[int, str, str]:
    command = build_svn_command(project_config, *args)
    safe_command = redact_command(command)
    start_ts = time.time()
    log_task_event(
        "svn_command",
        "start",
        command=safe_command,
    )
    try:
        timeout = int(project_config.get("timeout", 120))
    except (TypeError, ValueError) as exc:
        raise ValueError("SVN timeout must be an integer") from exc
    if timeout <= 0:
        raise ValueError("SVN timeout must be greater than 0")
    try:
        # shell=False is intentional: SVN arguments are passed as an argv list.
        result = subprocess.run(command, capture_output=True, timeout=timeout)  # noqa: S603
    except Exception as exc:
        log_exception_event("SVN_COMMAND_EXCEPTION", exc, command=safe_command)
        raise

    elapsed_seconds = round(time.time() - start_ts, 3)
    stdout = decode_svn_output(result.stdout)
    stderr = decode_svn_output(result.stderr)
    log_task_event(
        "svn_command",
        "end",
        command=safe_command,
        returncode=result.returncode,
        elapsed_seconds=elapsed_seconds,
        stdout_length=len(stdout),
        stderr_length=len(stderr),
    )
    if elapsed_seconds >= 10:
        log_warning_event(
            "SVN_COMMAND_SLOW",
            command=safe_command,
            returncode=result.returncode,
            elapsed_seconds=elapsed_seconds,
        )
    return result.returncode, stdout, stderr


def strip_peg_revision(url: str) -> str:
    return url.split("@", 1)[0]


def get_repo_root(project_config: dict, url: str) -> str:
    code, stdout, stderr = run_svn_text(
        project_config, "info", "--xml", strip_peg_revision(url)
    )
    if code != 0:
        raise RuntimeError(f"svn info 执行失败:\n{stderr}")
    root = _parse_svn_xml(stdout).findtext(".//repository/root")
    if not root:
        raise RuntimeError("未找到 SVN repository root")
    return root.rstrip("/")


def to_repo_path(url: str, repo_root: str) -> str:
    plain_url = strip_peg_revision(url).rstrip("/")
    if not plain_url.startswith(repo_root):
        raise RuntimeError(f"URL 不在仓库根路径下: {plain_url}")
    return "/" + plain_url[len(repo_root) :].lstrip("/")


def get_branch_origin(project_config: dict, branch_url: str) -> tuple[str, str]:
    repo_root = get_repo_root(project_config, branch_url)

    def resolve_origin(url: str, peg_revision: str | None = None) -> tuple[str, str]:
        target_url = strip_peg_revision(url)
        current_repo_path = to_repo_path(target_url, repo_root)
        log_target = f"{target_url}@{peg_revision}" if peg_revision else target_url

        code, stdout, stderr = run_svn_text(
            project_config, "log", "--xml", "--verbose", "--stop-on-copy", log_target
        )
        if code != 0:
            raise RuntimeError(f"svn log 执行失败:\n{stderr}")

        root = _parse_svn_xml(stdout)
        entries = root.findall("./logentry")
        if not entries:
            raise RuntimeError(f"未找到分支日志: {log_target}")

        oldest_entry = entries[-1]
        copy_path = None
        copy_rev = None
        for path_node in oldest_entry.findall("./paths/path"):
            if (path_node.text or "").strip() != current_repo_path:
                continue
            if path_node.attrib.get("action") != "A":
                continue
            copy_path = path_node.attrib.get("copyfrom-path")
            copy_rev = path_node.attrib.get("copyfrom-rev")
            if copy_path and copy_rev:
                break

        if not copy_path or not copy_rev:
            revision = oldest_entry.attrib.get("revision")
            if not revision:
                raise RuntimeError(f"未找到分支创建 revision: {log_target}")
            return target_url, revision

        source_url = f"{repo_root}{copy_path}"
        if "/branches/" in copy_path:
            return resolve_origin(source_url, copy_rev)
        return source_url, copy_rev

    origin_url, origin_revision = resolve_origin(branch_url)
    print("branch create revision:", origin_revision)
    print("branch origin:", origin_url)
    return origin_url, origin_revision


def diff_between_urls(project_config: dict, left_url: str, right_url: str) -> str:
    code, stdout, stderr = run_svn_text(
        project_config, "diff", "--summarize", left_url, right_url
    )
    if code != 0:
        raise RuntimeError(f"svn diff 执行失败:\n{stderr}")
    print("diff left:", left_url)
    print("diff right:", right_url)
    return stdout


def summarize_diff(diff_text: str):
    counts = {"A": 0, "M": 0, "D": 0, "OTHER": 0}
    for line in diff_text.splitlines():
        line = line.strip()
        if not line:
            continue
        status = line[0]
        counts[status if status in counts else "OTHER"] += 1
    print("diff summary:", counts)


def diff_url_to_repo_rel_path(marker: str, path: str) -> str:
    idx = path.find(marker)
    if idx == -1:
        raise ValueError(f"无法定位仓库相对路径: {path}")
    rel = path[idx + len(marker) :]
    parts = rel.split("/")
    if parts[0] == "trunk":
        rel = "/".join(parts[1:])
    elif parts[0] == "branches":
        if len(parts) >= 3 and parts[1] == "history":
            rel = "/".join(parts[3:])
        else:
            rel = "/".join(parts[2:])
    return unquote(rel)


def extract_active_files(marker: str, diff_text: str) -> list[str]:
    result = []
    for line in diff_text.splitlines():
        line = line.strip()
        if not line:
            continue
        status = line[0]
        path = line[1:].strip()
        if status not in ("A", "M"):
            continue
        if not path.lower().endswith(
            (".cpt", ".frm", ".txt", ".xls", ".sql", ".sh", ".py", ".json")
        ):
            continue
        result.append(diff_url_to_repo_rel_path(marker, path))
    return sorted(set(result))


def build_branch_file_url(branch_url: str, repo_rel_path: str) -> str:
    repo_rel_path = repo_rel_path.strip("/")
    encoded_rel = "/".join(quote(part) for part in repo_rel_path.split("/"))
    return f"{branch_url.rstrip('/')}/{encoded_rel}"


def export_svn_file(
    project_config: dict, repo_rel_path: str, branch_url: str, local_root: Path
) -> str:
    file_url = build_branch_file_url(branch_url, repo_rel_path)
    export_root = Path(local_root).expanduser().resolve()
    local_path = (export_root / Path(repo_rel_path)).resolve()
    if local_path == export_root or export_root not in local_path.parents:
        raise ValueError(f"不安全的 SVN 相对路径: {repo_rel_path}")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    code, _, stderr = run_svn_text(
        project_config, "export", "--force", file_url, str(local_path)
    )
    if code != 0:
        raise RuntimeError(f"export 失败:\n{stderr}\nURL: {file_url}")
    return str(local_path)


def svn_main(project: str, branch_url: str):
    log_task_event("svn_main", "start", project=project, branch_url=branch_url)
    project_config = get_project_config(project)
    marker = project_config["marker"]
    branch_name = branch_url.rstrip("/").rsplit("/", 1)[-1]
    local_export_root = get_export_base() / branch_name

    try:
        log_task_event(
            "svn_main.resolve_origin", "start", project=project, branch_url=branch_url
        )
        base_source_url, create_revision = get_branch_origin(project_config, branch_url)
        log_task_event(
            "svn_main.resolve_origin",
            "end",
            base_source_url=base_source_url,
            create_revision=create_revision,
        )

        base_compare_url = build_compare_url(base_source_url, create_revision)
        latest_trunk_url = build_compare_url(base_source_url)

        log_task_event(
            "svn_main.diff",
            "start",
            branch_url=branch_url,
            base_compare_url=base_compare_url,
        )
        branch_diff_text = diff_between_urls(
            project_config, base_compare_url, branch_url
        )
        trunk_diff_text = diff_between_urls(
            project_config, base_compare_url, latest_trunk_url
        )
        log_task_event(
            "svn_main.diff",
            "end",
            branch_diff_length=len(branch_diff_text),
            trunk_diff_length=len(trunk_diff_text),
        )
        summarize_diff(branch_diff_text)
        summarize_diff(trunk_diff_text)

        branch_changed_files = extract_active_files(marker, branch_diff_text)
        trunk_changed_files = extract_active_files(marker, trunk_diff_text)
        trunk_conflict_files = sorted(
            set(branch_changed_files) & set(trunk_changed_files)
        )

        print("export file count:", len(branch_changed_files))
        log_task_event(
            "svn_main.export",
            "start",
            file_count=len(branch_changed_files),
            local_export_root=str(local_export_root),
        )
        result_paths = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    export_svn_file,
                    project_config,
                    file_path,
                    branch_url,
                    local_export_root,
                ): file_path
                for file_path in branch_changed_files
            }
            for future in as_completed(futures):
                repo_rel_path = futures[future]
                try:
                    result_paths.append(future.result())
                except Exception as exc:
                    print(f"export failed: {repo_rel_path} -> {exc}")
                    log_exception_event(
                        "SVN_EXPORT_EXCEPTION",
                        exc,
                        repo_rel_path=repo_rel_path,
                        branch_url=branch_url,
                    )
        result_paths.sort()
        log_task_event(
            "svn_main.export",
            "end",
            exported_count=len(result_paths),
            requested_count=len(branch_changed_files),
        )
        log_task_event(
            "svn_main",
            "end",
            project=project,
            branch_url=branch_url,
            exported_count=len(result_paths),
            branch_changed_count=len(branch_changed_files),
            trunk_changed_count=len(trunk_changed_files),
            trunk_conflict_count=len(trunk_conflict_files),
        )
        return {
            "exported_paths": result_paths,
            "branch_changed_files": branch_changed_files,
            "trunk_changed_files": trunk_changed_files,
            "trunk_conflict_files": trunk_conflict_files,
            "base_source_url": base_source_url,
            "create_revision": create_revision,
            "latest_trunk_url": latest_trunk_url,
        }
    except Exception as exc:
        log_exception_event(
            "SVN_MAIN_EXCEPTION", exc, project=project, branch_url=branch_url
        )
        raise


if __name__ == "__main__":
    branch_url = os.getenv(
        "PYTOOLS_SVN_SAMPLE_URL", "svn://svn.example.invalid/lakehouse/trunk"
    )
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    print(svn_main("lakehouse", branch_url))
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
