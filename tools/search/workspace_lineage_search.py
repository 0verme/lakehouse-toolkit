from __future__ import annotations

from importlib import import_module

from shared.lineage.mapping_sqlite import (
    MAPPING_DB_PATH,
    MAPPING_XLSX_PATH,
    compact_identifier,
    filter_registered_result_nodes,
    find_start_nodes_in_sqlite,
    get_mapping_db_status,
    load_registered_result_tables,
    normalize_value,
    walk_downstream_in_sqlite,
)
from shared.search.lakehouse_search import (
    build_download_link,
    collect_search_context,
    get_default_settings,
    search_dependencies_for_keywords,
)
from shared.ui.export_helper import put_table_exports
from shared.ui.pywebio_helper import put_red_text, put_table_plus, start_pywebio_app

SETTINGS = get_default_settings()

OPTION_EXCLUDE_DISABLED = "排除已停用结果"
OPTION_REMOVE_WHITESPACE = "搜索时忽略空白字符"
OPTION_SHOW_DEBUG = "显示调试信息"
DEFAULT_MAX_DEPTH = 4


def format_keyword_variants(keyword_tokens: list[str]) -> str:
    return ",".join(
        [
            token.strip().replace(" ", "").upper()
            for token in keyword_tokens
            if token and token.strip()
        ]
    )


def parse_batch_targets(raw_text: str) -> tuple[list[tuple[str, str]], list[str]]:
    targets: list[tuple[str, str]] = []
    errors: list[str] = []
    seen_targets: set[tuple[str, str]] = set()

    for line_no, raw_line in enumerate((raw_text or "").splitlines(), start=1):
        line = normalize_value(raw_line)
        if "#" in line:
            line = normalize_value(line.split("#", 1)[0])
        if "//" in line:
            line = normalize_value(line.split("//", 1)[0])
        if not line:
            continue

        parts = [segment.strip() for segment in line.split(".") if segment.strip()]
        if len(parts) < 2:
            errors.append(f"第 {line_no} 行格式不正确: {raw_line}")
            continue

        column_name = parts[-1]
        table_name = ".".join(parts[:-1])
        if not table_name or not column_name:
            errors.append(f"第 {line_no} 行格式不正确: {raw_line}")
            continue

        target = (table_name.strip(), column_name.strip())
        if target in seen_targets:
            continue

        seen_targets.add(target)
        targets.append(target)

    return targets, errors


def app():
    pywebio_input = import_module("pywebio.input")
    pywebio_output = import_module("pywebio.output")
    number_type = pywebio_input.NUMBER
    checkbox = pywebio_input.checkbox
    input_group = pywebio_input.input_group
    textarea = pywebio_input.textarea
    put_progressbar = pywebio_output.put_progressbar
    put_text = pywebio_output.put_text
    set_progressbar = pywebio_output.set_progressbar

    info = input_group(
        "血缘搜索与导出",
        [
            textarea(
                "批量输入（每行一个，格式：表名.字段名）",
                name="targets_text",
                rows=12,
                placeholder="DWF.F_DEMO_ACTIVITY.ACTIVITY_LABEL\nF_DEMO_ACTIVITY.ACTIVITY_CODE",
            ),
            pywebio_input.input(
                "最大层级，默认 4 层",
                name="max_depth",
                type=number_type,
                value=DEFAULT_MAX_DEPTH,
            ),
            checkbox(
                "选项",
                options=[
                    OPTION_EXCLUDE_DISABLED,
                    OPTION_REMOVE_WHITESPACE,
                    OPTION_SHOW_DEBUG,
                ],
                name="options",
                value=[OPTION_EXCLUDE_DISABLED, OPTION_REMOVE_WHITESPACE],
            ),
        ],
    )

    targets, parse_errors = parse_batch_targets(info.get("targets_text", ""))
    if parse_errors:
        for message in parse_errors:
            put_red_text(message)
    if not targets:
        put_red_text("请至少输入一行，格式为 表名.字段名")
        return

    raw_max_depth = info.get("max_depth")
    try:
        max_depth = (
            DEFAULT_MAX_DEPTH if raw_max_depth in (None, "") else int(raw_max_depth)
        )
    except (TypeError, ValueError):
        put_red_text("最大层级必须是整数")
        return
    if max_depth <= 0:
        put_red_text("最大层级必须大于 0")
        return

    db_status = get_mapping_db_status(MAPPING_DB_PATH, MAPPING_XLSX_PATH)
    if not db_status["db_exists"]:
        put_red_text(f"SQLite 不存在: {db_status['db_path']}")
        put_red_text("请先执行: python tools/search/import_mapping_to_sqlite.py")
        return
    if not db_status["is_fresh"]:
        put_red_text("SQLite 比 mapping.xlsx 旧，请先刷新映射数据库")
        put_red_text("请先执行: python tools/search/import_mapping_to_sqlite.py")
        return

    exclude_disabled = OPTION_EXCLUDE_DISABLED in info["options"]
    remove_whitespace = OPTION_REMOVE_WHITESPACE in info["options"]
    show_debug_logs = OPTION_SHOW_DEBUG in info["options"]
    export_sections: list[dict] = []
    merged_search_result_rows: list[list[str]] = []
    summary_rows: list[list[object]] = [
        ["序号", "输入表字段", "起始节点数", "下游节点数", "搜索结果数"]
    ]

    put_progressbar("progress", 0)
    put_text("开始批量查询血缘关系和依赖...")
    try:
        result_tables = load_registered_result_tables()
        context = collect_search_context(SETTINGS)
        if not context["workspace_directories"]:
            put_red_text(
                f"未找到湖仓脚本目录，请检查 {SETTINGS.directories_url} 或 {SETTINGS.target_dirs[0]}"
            )
        if not context["fine_directories"]:
            put_red_text(f"未找到 Reporting 目录，请检查 {SETTINGS.fine_url}")

        total_targets = len(targets)

        for target_index, (table_name, column_name) in enumerate(targets, start=1):
            target_label = f"{table_name}.{column_name}"
            put_text(f"正在处理 {target_index}/{total_targets}: {target_label}")

            start_nodes = find_start_nodes_in_sqlite(
                table_name, column_name, MAPPING_DB_PATH
            )
            relation_rows: list[list[str]] = []
            downstream_nodes: list[tuple[str, str, str]] = []

            if not start_nodes:
                put_red_text(
                    f"{target_label} 没有找到起始血缘节点，将仅搜索当前表字段依赖"
                )
            else:
                relation_rows, downstream_nodes = walk_downstream_in_sqlite(
                    start_nodes,
                    MAPPING_DB_PATH,
                    max_depth=max_depth,
                )

            if start_nodes:
                start_node_rows = [
                    [compact_identifier(node)] for node in sorted(start_nodes)
                ]
                put_table_plus([["起始节点"]] + start_node_rows)
                export_sections.append(
                    {
                        "title": f"{target_index}_起始节点",
                        "rows": [["输入表字段", target_label], ["起始节点"]]
                        + start_node_rows,
                    }
                )

            if relation_rows:
                numbered_relation_rows = [
                    [seq] + row for seq, row in enumerate(relation_rows, start=1)
                ]
                put_table_plus(
                    [["序号", "关系层级", "来源节点", "目标节点"]]
                    + numbered_relation_rows
                )
                export_sections.append(
                    {
                        "title": f"{target_index}_血缘关系",
                        "rows": [
                            ["输入表字段", target_label],
                            ["序号", "关系层级", "来源节点", "目标节点"],
                        ]
                        + numbered_relation_rows,
                    }
                )
            elif start_nodes:
                put_red_text(
                    f"{target_label} 没有找到下游血缘关系，将仅搜索当前表字段依赖"
                )

            filtered_downstream_nodes = filter_registered_result_nodes(
                downstream_nodes, result_tables
            )
            numbered_downstream_rows = [
                [seq, compact_identifier(node)]
                for seq, node in enumerate(filtered_downstream_nodes, start=1)
            ]
            if numbered_downstream_rows:
                put_table_plus([["序号", "最终下游字段"]] + numbered_downstream_rows)
                export_sections.append(
                    {
                        "title": f"{target_index}_最终下游字段",
                        "rows": [["输入表字段", target_label], ["序号", "最终下游字段"]]
                        + numbered_downstream_rows,
                    }
                )
            else:
                put_red_text(f"{target_label} 没有筛选到已登记的结果节点")

            all_keyword_sets = [[table_name, column_name]]
            all_keyword_sets.extend(
                [
                    [f"{schema}.{table}" if schema else table, column]
                    for schema, table, column in filtered_downstream_nodes
                ]
            )

            result_rows_for_target: list[list[str]] = []
            for keyword_set in all_keyword_sets:
                matches = search_dependencies_for_keywords(
                    keyword_set,
                    context,
                    exclude_disabled,
                    remove_whitespace,
                )
                keyword_label = ",".join([item for item in keyword_set if item])
                variant_label = format_keyword_variants(keyword_set)

                if show_debug_logs:
                    put_table_plus(
                        [["调试信息"]]
                        + [
                            [
                                f"搜索关键字: {keyword_label} | 实际匹配词: {variant_label}"
                            ]
                        ]
                    )

                if matches:
                    for category, display_name, file_path in matches:
                        result_rows_for_target.append(
                            [
                                keyword_label,
                                variant_label,
                                category,
                                display_name,
                                build_download_link(category, file_path, SETTINGS),
                            ]
                        )
                else:
                    result_rows_for_target.append(
                        [
                            keyword_label,
                            variant_label,
                            "未命中",
                            "未找到依赖",
                            "",
                        ]
                    )

            put_table_plus(
                [["搜索关键字", "实际匹配词", "结果类型", "依赖对象", "代码下载"]]
                + result_rows_for_target
            )
            export_sections.append(
                {
                    "title": f"{target_index}_搜索结果",
                    "rows": [
                        ["输入表字段", target_label],
                        [
                            "搜索关键字",
                            "实际匹配词",
                            "结果类型",
                            "依赖对象",
                            "代码下载",
                        ],
                    ]
                    + result_rows_for_target,
                }
            )

            for row in result_rows_for_target:
                merged_search_result_rows.append([target_index, target_label] + row)

            summary_rows.append(
                [
                    target_index,
                    target_label,
                    len(start_nodes),
                    len(filtered_downstream_nodes),
                    len(result_rows_for_target),
                ]
            )
            set_progressbar("progress", target_index / total_targets)

        export_sections.insert(
            0,
            {
                "title": "批量汇总",
                "rows": summary_rows,
            },
        )
        if merged_search_result_rows:
            export_sections.append(
                {
                    "title": "搜索结果汇总",
                    "rows": [
                        [
                            "输入序号",
                            "输入表字段",
                            "搜索关键字",
                            "实际匹配词",
                            "结果类型",
                            "依赖对象",
                            "代码下载",
                        ]
                    ]
                    + merged_search_result_rows,
                }
            )

        put_table_exports(
            prefix="lineage_total",
            name_parts=["batch", str(total_targets)],
            page_title=f"lineage_total batch {total_targets}",
            sheet_sections=export_sections,
        )
        put_red_text(
            "========================== 批量查询完成 =========================="
        )
    except Exception as exc:
        put_red_text(f"执行失败: {exc}")


if __name__ == "__main__":
    start_pywebio_app("Workspace 字段血缘搜索", app)
