from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import xlrd

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.fs.discovery import find_all_directories, find_all_directories_in_paths
from shared.text.encoding import detect_encoding

SOURCE_LAKE = "lake"
SOURCE_FINE = "fine"
SOURCE_UPSTREAM = "upstream"
SOURCE_SEND_XDATA = "send_xdata"


@dataclass(frozen=True)
class LakehouseSearchSettings:
    fgf: str
    directories_url: str
    fine_url: str
    fine_catalog_excel: str
    target_dirs: list[str]
    lakehouse_http_root: str
    fine_http_root: str
    upstream_url: str
    upstream_http_root: str


def get_default_settings() -> LakehouseSearchSettings:
    workspace_root = Path(
        os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
    ).expanduser()
    report_root = Path(
        os.getenv("PYTOOLS_REPORT_ROOT", "examples/reports")
    ).expanduser()
    upstream_root = Path(
        os.getenv("PYTOOLS_UPSTREAM_ROOT", "examples/upstream")
    ).expanduser()
    return LakehouseSearchSettings(
        fgf=os.sep,
        directories_url=os.getenv(
            "PYTOOLS_DIRECTORY_INDEX_PATH", "runtime/cache/directories.txt"
        ),
        fine_url=str(report_root),
        fine_catalog_excel=os.getenv(
            "PYTOOLS_REPORT_CATALOG_FILE", str(report_root / "catalog.xls")
        ),
        target_dirs=[str(workspace_root)],
        lakehouse_http_root=os.getenv(
            "PYTOOLS_WORKSPACE_HTTP_ROOT", "http://localhost:8500/workspace"
        ),
        fine_http_root=os.getenv(
            "PYTOOLS_REPORT_HTTP_ROOT", "http://localhost:8500/reports"
        ),
        upstream_url=str(upstream_root),
        upstream_http_root=os.getenv(
            "PYTOOLS_UPSTREAM_HTTP_ROOT", "http://localhost:8500/upstream"
        ),
    )


def normalize_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip().replace("\n", "").replace("\r", "")


def parse_send_xdata(xdata: str) -> tuple[str, int, list[str]]:
    if not xdata:
        return "", 0, []

    parts = [item.strip() for item in str(xdata).split(",") if item and item.strip()]
    if not parts:
        return "", 0, []

    target_table = parts[0]
    fields = parts[1:]
    return target_table, len(fields), fields


def _safe_child_path(directory: str | Path, filename: str) -> Path:
    root = Path(directory).expanduser().resolve()
    candidate = (root / filename).resolve()
    if candidate.parent != root:
        raise ValueError(f"不安全的文件名: {filename}")
    return candidate


def read_file_to_list(filename: str) -> list[str]:
    try:
        with open(filename, encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    except (OSError, UnicodeError):
        return []


def clean_table_list(table_list: list[str]) -> list[str]:
    return [item for item in table_list if item.strip()]


def load_program_status() -> dict[str, str]:
    sql = """SELECT p.target_table, min(j.status) AS status
FROM __JOBS_TABLE__ j
INNER JOIN __PROGRAMS_TABLE__ p ON j.program_name = p.program_name
GROUP BY p.target_table""".replace(
        "__JOBS_TABLE__", metadata_table("jobs", "jobs")
    ).replace("__PROGRAMS_TABLE__", metadata_table("programs", "programs"))
    return dict(select_sql_with_profile("demo", sql) or [])


def load_fine_catalog_names(settings: LakehouseSearchSettings) -> set[str]:
    try:
        workbook = xlrd.open_workbook(settings.fine_catalog_excel)
        sheet = workbook.sheet_by_index(1)
    except Exception:
        return set()
    names = set()
    for i in range(1, sheet.nrows):
        cpt_fullname = normalize_value(sheet.cell(i, 3).value)
        if cpt_fullname:
            names.add(cpt_fullname.split("/")[-1].split(".")[0])
    return names


def load_send_xdata_rows() -> list[dict[str, str | int]]:
    sql = """
    SELECT send_name, job_name, target_table || ',' || field_list AS xdata
    FROM __SEND_JOBS_TABLE__
    """.replace("__SEND_JOBS_TABLE__", metadata_table("send_jobs", "send_jobs"))
    rows = []
    for send, job_name, xdata in select_sql_with_profile("demo", sql) or []:
        target_table, field_count, _ = parse_send_xdata(xdata)
        rows.append(
            {
                "send": normalize_value(send),
                "job_name": normalize_value(job_name),
                "xdata": normalize_value(xdata),
                "target_table": normalize_value(target_table),
                "field_count": field_count,
            }
        )
    return rows


def collect_hc_search_directories(settings: LakehouseSearchSettings) -> list[str]:
    directories = read_file_to_list(settings.directories_url)
    if directories:
        return directories
    return find_all_directories_in_paths(settings.target_dirs, excluded_fragment=".svn")


def collect_search_context(
    settings: LakehouseSearchSettings | None = None,
    selected_sources: set[str] | None = None,
) -> dict:
    settings = settings or get_default_settings()
    selected_sources = selected_sources or {SOURCE_LAKE, SOURCE_FINE}
    return {
        "settings": settings,
        "program_status": load_program_status()
        if SOURCE_LAKE in selected_sources
        else {},
        "fine_catalog_names": load_fine_catalog_names(settings)
        if SOURCE_FINE in selected_sources
        else set(),
        "workspace_directories": collect_hc_search_directories(settings)
        if SOURCE_LAKE in selected_sources
        else [],
        "fine_directories": find_all_directories(settings.fine_url)
        if SOURCE_FINE in selected_sources
        else [],
        "upstream_directories": find_all_directories(settings.upstream_url)
        if SOURCE_UPSTREAM in selected_sources
        else [],
        "send_xdata_rows": load_send_xdata_rows()
        if SOURCE_SEND_XDATA in selected_sources
        else [],
    }


def format_task_name(
    py_url: str,
    settings: LakehouseSearchSettings,
    status_mapping: dict[str, str],
    exclude_disabled: bool,
) -> str:
    task_name = (
        py_url.split(settings.fgf)[-2]
        .replace("DWS_DWE.", "DWE.")
        .replace("DWS_DWM.", "DWM.")
        .replace("DWS_DWP.", "DWP.")
        .replace("DWS_DWD.", "DWD.")
        .replace("DWS_DWA.", "DWA.")
        .replace("DWS_DM.", "DM.")
        .replace("DWS_DWF.", "DWF.")
        .replace("DWS_DWO.", "DWO.")
    )
    status = status_mapping.get(task_name)
    if status is None:
        return "" if exclude_disabled else task_name + " 【已下线】"
    if status == "禁用":
        return "" if exclude_disabled else task_name + " 【禁用】"
    return task_name


def normalize_search_tokens(keyword_tokens: list[str]) -> list[str]:
    return [
        normalize_value(token) for token in keyword_tokens if normalize_value(token)
    ]


def search_lake_dependencies(
    search_tokens: list[str],
    context: dict,
    exclude_disabled: bool,
    remove_whitespace: bool,
) -> list[list[str]]:
    settings: LakehouseSearchSettings = context["settings"]
    raw_joined_token = "".join(search_tokens).upper()
    rows = []
    seen = set()

    for url in context["workspace_directories"]:
        try:
            file_names = os.listdir(url)
        except OSError:
            continue
        for filename in file_names:
            if not filename.endswith(".py"):
                continue
            py_path = _safe_child_path(url, filename)
            py_url = str(py_path)
            try:
                with open(py_path, encoding="utf-8") as f:
                    line = f.read()
            except (OSError, UnicodeError):
                continue
            if remove_whitespace:
                line = line.replace(" ", "").replace("\n", "").replace("\r", "")
            line_upper = line.upper()
            if not all(token.upper() in line_upper for token in search_tokens):
                continue
            display_name = format_task_name(
                py_url, settings, context["program_status"], exclude_disabled
            )
            if (
                not display_name
                or display_name.upper() == raw_joined_token
                or display_name in seen
            ):
                continue
            seen.add(display_name)
            rows.append(["湖仓脚本", display_name, py_url])

    rows.sort(key=lambda item: item[1])
    return rows


def search_fine_dependencies(
    search_tokens: list[str], context: dict, exclude_disabled: bool
) -> list[list[str]]:
    settings: LakehouseSearchSettings = context["settings"]
    rows = []
    seen = set()

    for url in context["fine_directories"]:
        try:
            file_names = os.listdir(url)
        except OSError:
            continue
        for filename in file_names:
            if not filename.endswith((".cpt", ".frm")):
                continue
            py_path = _safe_child_path(url, filename)
            py_url = str(py_path)
            try:
                with open(py_path, encoding="utf-8") as f:
                    line = f.read()
            except (OSError, UnicodeError):
                continue
            line_upper = line.upper()
            if not all(token.upper() in line_upper for token in search_tokens):
                continue
            fine_name = (
                py_url.replace(settings.fine_url, "")[1:]
                .split(settings.fgf)[-1]
                .replace(".cpt", "")
            )
            display_name = py_url.replace(settings.fine_url, "")[1:].replace(".cpt", "")
            if (
                fine_name not in context["fine_catalog_names"]
                and ".frm" not in display_name
            ):
                display_name = (
                    "" if exclude_disabled else display_name + " 【不在目录展示】"
                )
            if not display_name or display_name in seen:
                continue
            seen.add(display_name)
            rows.append(["Reporting报表", display_name, py_url])

    rows.sort(key=lambda item: item[1])
    return rows


def search_upstream_dependencies(
    search_tokens: list[str], context: dict
) -> list[list[str]]:
    settings: LakehouseSearchSettings = context["settings"]
    rows = []
    seen = set()

    for url in context["upstream_directories"]:
        try:
            file_names = os.listdir(url)
        except OSError:
            continue
        for filename in file_names:
            if not filename.endswith(".py"):
                continue
            py_path = _safe_child_path(url, filename)
            py_url = str(py_path)
            try:
                with open(py_path, encoding=detect_encoding(py_path)) as f:
                    line = f.read()
            except (OSError, UnicodeError):
                continue
            line_upper = line.upper()
            if not all(token.upper() in line_upper for token in search_tokens):
                continue
            display_name = py_url.replace(settings.upstream_url, "").lstrip(
                settings.fgf
            )
            if display_name in seen:
                continue
            seen.add(display_name)
            rows.append(["UPSTREAM脚本", display_name, py_url])

    rows.sort(key=lambda item: item[1])
    return rows


def search_send_xdata_dependencies(
    search_tokens: list[str], context: dict
) -> list[list[str]]:
    rows = []
    for item in context["send_xdata_rows"]:
        xdata_upper = item["xdata"].upper()
        if not all(token.upper() in xdata_upper for token in search_tokens):
            continue
        display_name = f"{item['send']} | {item['job_name']} | {item['target_table']} | {item['field_count']}字段"
        rows.append(["SEND卸数字段", display_name, ""])

    rows.sort(key=lambda row: row[1])
    return rows


def search_dependencies_for_keywords(
    keyword_tokens: list[str],
    context: dict,
    exclude_disabled: bool,
    remove_whitespace: bool,
    selected_sources: set[str] | None = None,
) -> list[list[str]]:
    search_tokens = normalize_search_tokens(keyword_tokens)
    if not search_tokens:
        return []

    selected_sources = selected_sources or {SOURCE_LAKE, SOURCE_FINE}
    rows = []

    if SOURCE_LAKE in selected_sources:
        rows.extend(
            search_lake_dependencies(
                search_tokens, context, exclude_disabled, remove_whitespace
            )
        )
    if SOURCE_FINE in selected_sources:
        rows.extend(search_fine_dependencies(search_tokens, context, exclude_disabled))
    if SOURCE_UPSTREAM in selected_sources:
        rows.extend(search_upstream_dependencies(search_tokens, context))
    if SOURCE_SEND_XDATA in selected_sources:
        rows.extend(search_send_xdata_dependencies(search_tokens, context))
    return rows


def build_download_link(
    category: str, file_path: str, settings: LakehouseSearchSettings | None = None
) -> str:
    settings = settings or get_default_settings()
    path = Path(file_path).expanduser()
    normalized_path = str(path).replace("\\", "/")
    category_key = {
        SOURCE_LAKE: SOURCE_LAKE,
        "湖仓脚本": SOURCE_LAKE,
        "湖仓": SOURCE_LAKE,
        SOURCE_UPSTREAM: SOURCE_UPSTREAM,
        "UPSTREAM脚本": SOURCE_UPSTREAM,
        SOURCE_FINE: SOURCE_FINE,
        "Reporting报表": SOURCE_FINE,
    }.get(category, SOURCE_LAKE)
    category_root = {
        SOURCE_LAKE: settings.lakehouse_http_root,
        SOURCE_UPSTREAM: settings.upstream_http_root,
        SOURCE_FINE: settings.fine_http_root,
    }[category_key]
    root_path = {
        SOURCE_LAKE: Path(
            os.getenv("PYTOOLS_WORKSPACE_ROOT", "examples/workspace")
        ).expanduser(),
        SOURCE_UPSTREAM: Path(settings.upstream_url).expanduser(),
        SOURCE_FINE: Path(settings.fine_url).expanduser(),
    }[category_key]
    if root_path:
        try:
            relative = path.relative_to(root_path)
            link = f"{category_root.rstrip('/')}/{('/'.join(relative.parts))}"
        except ValueError:
            link = f"{category_root.rstrip('/')}/{path.name}"
    else:
        link = normalized_path
    return f'<a href="{link}" target="_blank">下载</a>'
