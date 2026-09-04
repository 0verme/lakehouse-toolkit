from __future__ import annotations

# The root-path bootstrap intentionally precedes imports used by direct script execution.
# ruff: noqa: E402, I001

import os
import sys
import zipfile
from contextlib import suppress
from datetime import datetime
from importlib import import_module
from io import BytesIO
from pathlib import Path

from shared.config.metadata import table as metadata_table

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.db.gaussdb import connect_with_profile  # noqa: E402
from shared.graph.dependency import (
    build_reverse_dependency_graph,
    find_all_dependent_jobs,
    parse_job_dependencies,
)  # noqa: E402

xlwt = import_module("xlwt")

PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
JOB_SQL = """
SELECT *
FROM __JOBS_TABLE__
WHERE upper(coalesce(status, '')) IN ('ENABLED', '启用')
""".replace("__JOBS_TABLE__", metadata_table("jobs", "jobs"))

MAX_ROWS_PER_FILE = 500


def create_run_suffix(dt: datetime | None = None) -> str:
    current = dt or datetime.now()
    return current.strftime("%Y%m%d%H%M%S")


def append_suffix(name: str, suffix: str) -> str:
    value = "" if name is None else str(name).strip()
    if not value:
        return ""
    if value.endswith(suffix):
        return value
    return f"{value}{suffix}"


def value_to_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sanitize_filename_fragment(value: str) -> str:
    text = value_to_text(value).replace(".", "_").replace(" ", "_")
    return "".join(ch for ch in text if ch.isalnum() or ch in ("_", "-")) or "result"


def create_export_filename(name_parts: list[str]) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"PLAN_DWS_ZS_{timestamp}.xls"


def find_column_name(columns: list[str], target: str) -> str:
    mapping = {str(column).strip().lower(): column for column in columns}
    if target not in mapping:
        raise KeyError(f"未找到列: {target}")
    return mapping[target]


def build_job_index(
    columns: list[str], rows: list[dict]
) -> tuple[dict[str, dict], str, str | None, str, str]:
    plan_col = find_column_name(columns, "a")
    seq_col = next(
        (column for column in columns if str(column).strip().lower() == "b"), None
    )
    job_col = find_column_name(columns, "c")
    deps_col = find_column_name(columns, "ab")

    job_index = {}
    for row in rows:
        job_name = value_to_text(row.get(job_col))
        if not job_name:
            continue
        job_index[job_name] = {
            "job_name": job_name,
            "plan_name": value_to_text(row.get(plan_col)),
            "dependencies": parse_job_dependencies(row.get(deps_col)),
            "source_row": row,
        }
    return job_index, plan_col, seq_col, job_col, deps_col


def should_exclude_send_day(plan_name: str) -> bool:
    upper_name = value_to_text(plan_name).upper()
    return upper_name.endswith(("SEND_DAY", "SEND1_DAY", "SEND2_DAY"))


def collect_downstream_jobs_multi(
    start_jobs: list[str],
    job_index: dict[str, dict],
    include_start_jobs: bool = True,
) -> tuple[list[tuple[str, int]], list[str]]:
    dep_pairs = [
        (name, "|".join(f"33:{dep}" for dep in info["dependencies"]))
        for name, info in job_index.items()
    ]
    reverse_graph = build_reverse_dependency_graph(dep_pairs)

    seen = set()
    result = []
    not_found = []

    for raw in start_jobs:
        job = value_to_text(raw)
        if not job:
            continue
        if job not in job_index:
            not_found.append(job)
            continue
        if include_start_jobs and job not in seen:
            seen.add(job)
            result.append((job, 0))
        for downstream_job, level in find_all_dependent_jobs(job, reverse_graph):
            if downstream_job in seen:
                continue
            seen.add(downstream_job)
            result.append((downstream_job, level))

    return result, not_found


def transform_rows(
    start_jobs: list[str],
    columns: list[str],
    rows: list[dict],
    include_start_jobs: bool = True,
    suffix: str | None = None,
    exclude_send_day_plan: bool = False,
) -> tuple[list[dict], list[str]]:
    run_suffix = suffix or create_run_suffix()
    job_index, plan_col, seq_col, job_col, deps_col = build_job_index(columns, rows)
    downstream_jobs, not_found = collect_downstream_jobs_multi(
        start_jobs,
        job_index,
        include_start_jobs=include_start_jobs,
    )
    if not downstream_jobs:
        raise ValueError("未找到任何有效作业，请检查输入")

    selected_jobs = []
    for job_name, level in downstream_jobs:
        plan_name = job_index[job_name]["plan_name"]
        if exclude_send_day_plan and should_exclude_send_day(plan_name):
            continue
        selected_jobs.append((job_name, level))

    if not selected_jobs:
        raise ValueError("过滤后无可导出的作业，请检查输入或过滤条件")

    included_jobs = {job_name for job_name, _ in selected_jobs}
    result = []
    for job_name, _level in selected_jobs:
        info = job_index[job_name]
        source_row = dict(info["source_row"])
        source_row[plan_col] = f"PLAN_DWS_ZS_{run_suffix}"
        if seq_col is not None:
            source_row[seq_col] = f"SEQ_DWS_ZS_{run_suffix}"
        source_row[job_col] = append_suffix(job_name, f"_{run_suffix}")
        kept_dependencies = [
            dep for dep in info["dependencies"] if dep in included_jobs
        ]
        source_row[deps_col] = "|".join(
            f"33:{append_suffix(dep, f'_{run_suffix}')}" for dep in kept_dependencies
        )
        result.append(source_row)
    return result, not_found


def fetch_job_table() -> tuple[list[str], list[dict]]:
    conn = None
    curs = None
    try:
        conn = connect_with_profile(PROFILE)
        curs = conn.cursor()
        # pi-lens-ignore: python-sql-injection
        curs.execute(JOB_SQL)
        columns = [item[0] for item in (curs.description or [])]
        rows = [dict(zip(columns, row, strict=False)) for row in curs.fetchall()]
        return columns, rows
    except Exception as exc:
        raise RuntimeError(f"查询 demo jobs 失败，请检查本地数据库配置: {exc}") from exc
    finally:
        with suppress(Exception):
            if curs is not None:
                curs.close()
        with suppress(Exception):
            if conn is not None:
                conn.close()


def build_export_text(columns: list[str], result_rows: list[dict]) -> str:
    lines = ["\t".join(columns)]
    for row in result_rows:
        lines.append("\t".join(value_to_text(row.get(column)) for column in columns))
    return "\n".join(lines)


def build_sheet_rows(columns: list[str], result_rows: list[dict]) -> list[list[str]]:
    return [columns] + [
        [value_to_text(row.get(column)) for column in columns] for row in result_rows
    ]


def split_rows_preserving_dependencies(
    columns: list[str],
    result_rows: list[dict],
    max_rows_per_file: int = MAX_ROWS_PER_FILE,
) -> list[list[dict]]:
    if max_rows_per_file <= 0:
        raise ValueError("max_rows_per_file 必须大于 0")
    if len(result_rows) <= max_rows_per_file:
        return [result_rows]

    job_col = find_column_name(columns, "c")
    deps_col = find_column_name(columns, "ab")
    row_by_job = {}
    job_order = {}
    dep_map = {}

    for index, row in enumerate(result_rows):
        job_name = value_to_text(row.get(job_col))
        if not job_name:
            continue
        row_by_job[job_name] = row
        job_order[job_name] = index
        dep_map[job_name] = parse_job_dependencies(row.get(deps_col))

    closure_cache: dict[str, list[str]] = {}

    def resolve_closure(job_name: str) -> list[str]:
        if job_name in closure_cache:
            return closure_cache[job_name]

        seen = set()
        ordered_jobs = []

        def visit(current_job: str):
            for dep in dep_map.get(current_job, []):
                if dep not in row_by_job or dep in seen:
                    continue
                seen.add(dep)
                visit(dep)
                ordered_jobs.append(dep)

        visit(job_name)
        closure_cache[job_name] = ordered_jobs
        return ordered_jobs

    chunks: list[list[dict]] = []
    current_rows: list[dict] = []
    current_jobs: set[str] = set()

    for row in result_rows:
        job_name = value_to_text(row.get(job_col))
        if not job_name:
            continue

        required_jobs = resolve_closure(job_name) + [job_name]
        missing_jobs = [job for job in required_jobs if job not in current_jobs]
        if len(missing_jobs) > max_rows_per_file:
            raise ValueError(
                f"单个作业链路超过 {max_rows_per_file} 行，无法自动拆分: {job_name}"
            )

        if current_rows and len(current_rows) + len(missing_jobs) > max_rows_per_file:
            chunks.append(current_rows)
            current_rows = []
            current_jobs = set()
            missing_jobs = required_jobs
            if len(missing_jobs) > max_rows_per_file:
                raise ValueError(
                    f"单个作业链路超过 {max_rows_per_file} 行，无法自动拆分: {job_name}"
                )

        for missing_job in missing_jobs:
            current_rows.append(dict(row_by_job[missing_job]))
            current_jobs.add(missing_job)

    if current_rows:
        chunks.append(current_rows)

    return chunks


def build_xls_bytes(sheet_name: str, rows: list[list[str]]) -> bytes:
    workbook = xlwt.Workbook(encoding="utf-8")
    sheet = workbook.add_sheet((sheet_name or "job_downstream_zs")[:31])

    for row_index, row in enumerate(rows):
        for col_index, cell in enumerate(row):
            sheet.write(row_index, col_index, value_to_text(cell))

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def build_split_zip_bytes(
    columns: list[str],
    row_chunks: list[list[dict]],
    base_filename: str,
) -> bytes:
    base_stem = sanitize_filename_fragment(Path(base_filename).stem)[:40] or "export"
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for index, chunk_rows in enumerate(row_chunks, start=1):
            file_name = f"{base_stem}_part{index:03d}.xls"
            sheet_rows = build_sheet_rows(columns, chunk_rows)
            zf.writestr(file_name, build_xls_bytes(f"job_zs_{index:03d}", sheet_rows))
    return buffer.getvalue()


def render_app():
    pywebio_input = import_module("pywebio.input")
    pywebio_output = import_module("pywebio.output")
    text_type = pywebio_input.TEXT
    checkbox = pywebio_input.checkbox
    input_group = pywebio_input.input_group
    textarea = pywebio_input.textarea
    put_file = pywebio_output.put_file
    put_markdown = pywebio_output.put_markdown
    put_text = pywebio_output.put_text

    ui_helper = import_module("shared.ui.pywebio_helper")
    iter_nonempty_lines = ui_helper.iter_nonempty_lines
    put_black_text = ui_helper.put_black_text
    put_red_text = ui_helper.put_red_text

    form = input_group(
        "追数下游生成",
        [
            textarea(
                "请输入起始作业名，支持多行，每行一个",
                name="start_jobs_text",
                type=text_type,
            ),
            checkbox(
                "是否把起始 job 自身列入追数结果",
                options=["包含起始 job 自身"],
                name="include_start_jobs",
                value=["包含起始 job 自身"],
            ),
            checkbox(
                "是否剔除 a 列以 SEND_DAY / SEND1_DAY / SEND2_DAY 结尾的计划",
                options=["剔除 SEND 类计划"],
                name="exclude_send_day_plan",
                value=["剔除 SEND 类计划"],
            ),
        ],
    )
    start_jobs = iter_nonempty_lines(form["start_jobs_text"])
    if not start_jobs:
        put_red_text("未输入任何作业名")
        return

    include_start_jobs = form["include_start_jobs"] == ["包含起始 job 自身"]
    exclude_send_day_plan = form["exclude_send_day_plan"] == ["剔除 SEND 类计划"]
    run_suffix = create_run_suffix()
    columns, rows = fetch_job_table()
    result_rows, not_found = transform_rows(
        start_jobs,
        columns,
        rows,
        include_start_jobs=include_start_jobs,
        suffix=run_suffix,
        exclude_send_day_plan=exclude_send_day_plan,
    )

    if not_found:
        put_red_text(f"以下作业名未找到，已跳过：{', '.join(not_found)}")

    export_filename = create_export_filename(start_jobs)
    row_chunks = split_rows_preserving_dependencies(
        columns, result_rows, max_rows_per_file=MAX_ROWS_PER_FILE
    )

    put_black_text(f"起始作业数: {len(start_jobs)}")
    put_text("\n".join(start_jobs))
    put_text(f"共找到 {len(result_rows)} 条作业记录（已合并去重）。")
    put_markdown(
        f"说明：结果直接导出为 `.xls`。`a` 固定为 `PLAN_DWS_ZS_{run_suffix}`，"
        f"`b` 固定为 `SEQ_DWS_ZS_{run_suffix}`，`c` 追加 `_{run_suffix}`；"
        "`ab` 中不在本次链路内的依赖已删除；"
        f"当前{'包含' if include_start_jobs else '不包含'}起始 job 自身；"
        f"{'已' if exclude_send_day_plan else '未'}剔除 SEND 类计划；"
        f"超过 {MAX_ROWS_PER_FILE} 行时会按依赖关系自动拆分，当前共 {len(row_chunks)} 份。"
    )

    if len(row_chunks) == 1:
        sheet_rows = build_sheet_rows(columns, row_chunks[0])
        put_file(
            export_filename,
            build_xls_bytes("job_downstream_zs", sheet_rows),
            "点击下载 XLS",
        )
    else:
        put_file(
            f"{Path(export_filename).stem}.zip",
            build_split_zip_bytes(columns, row_chunks, export_filename),
            "点击下载 ZIP（按依赖自动拆分）",
        )


def main():
    from shared.ui.pywebio_helper import safe_put_error

    try:
        render_app()
    except Exception as exc:
        safe_put_error(exc)


if __name__ == "__main__":
    from shared.ui.pywebio_helper import start_pywebio_app

    start_pywebio_app("追数下游生成工具", main)
