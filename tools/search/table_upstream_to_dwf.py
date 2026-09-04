# !/bin/python
from __future__ import annotations

from shared.lineage.schedule_table_lineage import (
    DOWNSTREAM,
    UPSTREAM,
    build_table_job_map,
    find_table_candidates,
    is_dwf_table,
    load_job_index,
    normalize_table_name,
    trace_table_lineage,
)


def put_table(*args, **kwargs):
    from pywebio.output import put_table as render_table

    return render_table(*args, **kwargs)


def put_black_text(*args, **kwargs):
    from shared.ui.pywebio_helper import put_black_text as render_text

    return render_text(*args, **kwargs)


def put_red_text(*args, **kwargs):
    from shared.ui.pywebio_helper import put_red_text as render_text

    return render_text(*args, **kwargs)


def safe_put_error(*args, **kwargs):
    from shared.ui.pywebio_helper import safe_put_error as render_error

    return render_error(*args, **kwargs)


def put_table_exports(*args, **kwargs):
    from shared.ui.export_helper import put_table_exports as export_tables

    return export_tables(*args, **kwargs)


def start_pywebio_app(*args, **kwargs):
    from shared.ui.pywebio_helper import start_pywebio_app as start_app

    return start_app(*args, **kwargs)


DEFAULT_MAX_DEPTH = 30
DIRECTION_LABELS = {UPSTREAM: "上游检查", DOWNSTREAM: "下游检查"}


def analyze_one(
    input_name: str,
    job_index: dict,
    table_job_map: dict,
    max_depth: int,
    direction: str = UPSTREAM,
) -> dict | None:
    candidates = find_table_candidates(input_name, table_job_map)
    if not candidates:
        put_red_text(f"{normalize_table_name(input_name)} 未找到对应结果表")
        return None
    if len(candidates) > 1:
        put_red_text(
            f"{normalize_table_name(input_name)} 匹配到多个表，请输入包含 schema 的完整表名"
        )
        put_table([["候选表名"]] + [[item] for item in candidates[:100]])
        return None

    trace = trace_table_lineage(
        candidates[0],
        job_index,
        table_job_map,
        max_depth=max_depth,
        direction=direction,
    )
    direction_label = DIRECTION_LABELS[direction]
    if direction == UPSTREAM:
        rows = [["层级", "当前表", "前置表", "表依赖路径", "DWF 截止信息"]]
        for edge in sorted(
            trace.edges,
            key=lambda item: (item.depth, item.target_table, item.source_table),
        ):
            rows.append(
                [
                    edge.depth,
                    edge.target_table,
                    edge.source_table,
                    " -> ".join(edge.path),
                    "DWF 截止表" if is_dwf_table(edge.source_table) else "-",
                ]
            )
    else:
        rows = [["层级", "当前表", "下游表", "表依赖路径", "是否末端表"]]
        for edge in sorted(
            trace.edges,
            key=lambda item: (item.depth, item.source_table, item.target_table),
        ):
            rows.append(
                [
                    edge.depth,
                    edge.source_table,
                    edge.target_table,
                    " -> ".join(edge.path),
                    "是" if edge.target_table in trace.terminal_tables else "否",
                ]
            )

    put_black_text(f"检查方向: {direction_label}；根表: {trace.root_table}")
    if direction == UPSTREAM:
        put_black_text(
            f"上游表数: {max(0, len(trace.nodes) - 1)}，DWF 截止表数: {len(trace.dwf_tables)}"
        )
        if trace.dwf_tables:
            put_black_text("命中的 DWF 表: " + "、".join(sorted(trace.dwf_tables)))
        else:
            put_red_text("未追踪到 DWF 表")
    else:
        put_black_text(
            f"下游表数: {max(0, len(trace.nodes) - 1)}，末端表数: {len(trace.terminal_tables)}"
        )
        if trace.terminal_tables:
            put_black_text("末端表: " + "、".join(sorted(trace.terminal_tables)))

    if trace.cycles:
        put_red_text(f"检测到 {len(trace.cycles)} 条作业依赖环，对应分支已停止")
    if trace.missing_jobs:
        put_red_text(f"有 {len(trace.missing_jobs)} 个关联作业不存在，对应分支无法继续")
    if trace.unmapped_jobs:
        put_black_text(
            f"已透明穿透 {len(trace.unmapped_jobs)} 个没有关联表名的中间作业"
        )
    if trace.truncated:
        put_red_text(f"已达到最大递归深度 {trace.max_depth}，结果可能未完全展开")
    if len(rows) == 1:
        if direction == UPSTREAM and is_dwf_table(trace.root_table):
            put_red_text("输入表已经是 DWF 表，无需继续向上追踪")
        else:
            put_red_text(
                f"没有找到可展示的{'上游' if direction == UPSTREAM else '下游'}表关系"
            )
    else:
        put_table(rows)

    return {
        "title": trace.root_table,
        "rows": [
            ["检查方向", direction_label],
            ["根表", trace.root_table],
            [
                f"{'上游' if direction == UPSTREAM else '下游'}表数",
                len(trace.nodes) - 1,
            ],
            [
                "DWF 截止表" if direction == UPSTREAM else "末端表",
                "、".join(
                    sorted(
                        trace.dwf_tables
                        if direction == UPSTREAM
                        else trace.terminal_tables
                    )
                ),
            ],
            ["", ""],
        ]
        + rows,
    }


def main():
    from pywebio.input import NUMBER, TEXT, input, input_group, radio, textarea

    info = input_group(
        "表血缘双向追踪",
        [
            radio(
                "检查方向",
                name="direction",
                options=[("上游检查", UPSTREAM), ("下游检查", DOWNSTREAM)],
                value=UPSTREAM,
                required=True,
            ),
            textarea(
                "请输入表名，每行一个，例如 DWM.TABLE_NAME", name="targets", type=TEXT
            ),
            input(
                "最大作业递归深度，默认 30",
                name="max_depth",
                type=NUMBER,
                value=DEFAULT_MAX_DEPTH,
            ),
        ],
    )
    direction = (
        info.get("direction") if info.get("direction") in DIRECTION_LABELS else UPSTREAM
    )
    try:
        max_depth = max(1, min(int(info.get("max_depth") or DEFAULT_MAX_DEPTH), 100))
    except (TypeError, ValueError):
        max_depth = DEFAULT_MAX_DEPTH

    job_index = load_job_index()
    table_job_map = build_table_job_map(job_index)
    targets = [
        line.strip()
        for line in str(info.get("targets") or "").splitlines()
        if line.strip()
    ]
    if not targets:
        put_red_text("请输入至少一个表名")
        return

    export_sections = []
    for target in targets:
        try:
            section = analyze_one(
                target, job_index, table_job_map, max_depth, direction
            )
            if section:
                export_sections.append(section)
        except Exception as exc:
            safe_put_error(exc)
    put_table_exports(
        prefix="table_lineage_trace",
        name_parts=[direction, "batch"],
        page_title=f"表血缘双向追踪 - {DIRECTION_LABELS[direction]}",
        sheet_sections=export_sections,
        add_index_sheet=True,
        index_sheet_title="目录",
    )


if __name__ == "__main__":
    start_pywebio_app("表血缘双向追踪", main)
