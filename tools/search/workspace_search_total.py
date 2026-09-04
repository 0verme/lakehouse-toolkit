from __future__ import annotations

from importlib import import_module

from shared.search.lakehouse_search import (
    SOURCE_FINE,
    SOURCE_LAKE,
    SOURCE_SEND_XDATA,
    SOURCE_UPSTREAM,
    build_download_link,
    clean_table_list,
    collect_search_context,
    get_default_settings,
    search_dependencies_for_keywords,
)
from shared.ui.export_helper import put_table_exports
from shared.ui.pywebio_helper import (
    put_red_text,
    put_table_plus,
    safe_put_error,
    start_pywebio_app,
)

SETTINGS = get_default_settings()

OPTION_EXCLUDE_DISABLED = "排除已下线 / 禁用 / 未展示结果"
OPTION_ONLY_DOWNSTREAM = "只判断是否存在下游依赖"
OPTION_SPLIT_BY_MARKER = "关键字中包含逗号时，使用 @#@ 分隔"
OPTION_REMOVE_WHITESPACE = "搜索时忽略源码中的空格和换行"

SOURCE_OPTION_LAKE = "湖仓脚本"
SOURCE_OPTION_FINE = "Reporting报表"
SOURCE_OPTION_UPSTREAM = "UPSTREAM脚本"

SOURCE_OPTION_SEND_XDATA = "SEND卸数字段"

SOURCE_OPTION_MAPPING = {
    SOURCE_OPTION_LAKE: SOURCE_LAKE,
    SOURCE_OPTION_FINE: SOURCE_FINE,
    SOURCE_OPTION_UPSTREAM: SOURCE_UPSTREAM,
    SOURCE_OPTION_SEND_XDATA: SOURCE_SEND_XDATA,
}


def parse_table_groups(raw_value: str, split_by_marker: bool) -> list[list[str]]:
    groups = []
    for raw_line in clean_table_list(raw_value.splitlines()):
        normalized_line = raw_line.strip().replace("，", ",")
        if not normalized_line:
            continue
        tokens = normalized_line.split("@#@" if split_by_marker else ",")
        cleaned_tokens = sorted(
            {token.strip() for token in tokens if token and token.strip()}
        )
        if cleaned_tokens:
            groups.append(cleaned_tokens)
    return groups


def build_result_table(
    keyword_tokens: list[str],
    matches: list[list[str]],
    downstream_only: bool,
    context: dict,
) -> list[list[str]]:
    keyword_label = ",".join(keyword_tokens)

    if downstream_only:
        return [
            ["关键字", "下游依赖"],
            [keyword_label, "有下游" if matches else "无下游"],
        ]

    result_rows = [
        [
            category,
            display_name,
            build_download_link(category, file_path, context["settings"])
            if file_path
            else "",
        ]
        for category, display_name, file_path in matches
    ]
    if not result_rows:
        result_rows.append(["-", "未找到依赖对象", ""])
    return [["结果类型", "搜索结果", "下载链接"]] + result_rows


def render_matches(result_table: list[list[str]], downstream_only: bool):
    if downstream_only:
        return
    put_table_plus(result_table)
    put_red_text("=============================================================")


def app():
    pywebio_input = import_module("pywebio.input")
    pywebio_output = import_module("pywebio.output")
    input_group = pywebio_input.input_group
    textarea = pywebio_input.textarea
    checkbox = pywebio_input.checkbox
    text_type = pywebio_input.TEXT
    put_progressbar = pywebio_output.put_progressbar
    put_text = pywebio_output.put_text
    set_progressbar = pywebio_output.set_progressbar

    try:
        info = input_group(
            "湖仓全链路搜索",
            [
                textarea(
                    "请输入需要解析的表或关键字，每行一组；默认用英文逗号分隔，例如 DWF.F_DEMO_TABLE,DEMO_ID",
                    name="table_list",
                    type=text_type,
                ),
                checkbox(
                    "搜索范围",
                    options=[
                        SOURCE_OPTION_LAKE,
                        SOURCE_OPTION_FINE,
                        SOURCE_OPTION_UPSTREAM,
                        SOURCE_OPTION_SEND_XDATA,
                    ],
                    name="sources",
                    value=[
                        SOURCE_OPTION_LAKE,
                        SOURCE_OPTION_FINE,
                        SOURCE_OPTION_SEND_XDATA,
                    ],
                ),
                checkbox(
                    "搜索选项",
                    options=[
                        OPTION_EXCLUDE_DISABLED,
                        OPTION_ONLY_DOWNSTREAM,
                        OPTION_SPLIT_BY_MARKER,
                        OPTION_REMOVE_WHITESPACE,
                    ],
                    name="options",
                    value=[OPTION_EXCLUDE_DISABLED, OPTION_REMOVE_WHITESPACE],
                ),
            ],
        )

        selected_options = set(info["options"])
        selected_sources = {
            SOURCE_OPTION_MAPPING[item]
            for item in info["sources"]
            if item in SOURCE_OPTION_MAPPING
        }
        if not selected_sources:
            put_red_text("请至少选择一个搜索范围")
            return

        table_groups = parse_table_groups(
            info["table_list"],
            split_by_marker=OPTION_SPLIT_BY_MARKER in selected_options,
        )
        if not table_groups:
            put_red_text("请至少输入一组有效关键字")
            return

        exclude_disabled = OPTION_EXCLUDE_DISABLED in selected_options
        downstream_only = OPTION_ONLY_DOWNSTREAM in selected_options
        remove_whitespace = OPTION_REMOVE_WHITESPACE in selected_options

        put_progressbar("progress", 0)
        put_text("开始加载搜索上下文...")
        context = collect_search_context(SETTINGS, selected_sources=selected_sources)
        if SOURCE_LAKE in selected_sources and not context["workspace_directories"]:
            put_red_text(
                f"未找到湖仓脚本目录，请检查 {SETTINGS.directories_url} 或 target_dirs 配置"
            )
        if SOURCE_FINE in selected_sources and not context["fine_directories"]:
            put_red_text(f"未找到 Reporting 目录，请检查 {SETTINGS.fine_url}")
        if SOURCE_UPSTREAM in selected_sources and not context["upstream_directories"]:
            put_red_text(f"未找到 UPSTREAM 目录，请检查 {SETTINGS.upstream_url}")
        set_progressbar("progress", 0.2)

        downstream_rows = []
        export_sections: list[dict] = []
        total = len(table_groups)
        for index, keyword_tokens in enumerate(table_groups, start=1):
            keyword_label = ",".join(keyword_tokens)
            put_text(f"关键字: {keyword_label.upper()}")
            matches = search_dependencies_for_keywords(
                keyword_tokens,
                context,
                exclude_disabled=exclude_disabled,
                remove_whitespace=remove_whitespace,
                selected_sources=selected_sources,
            )
            result_table = build_result_table(
                keyword_tokens, matches, downstream_only, context
            )
            render_matches(result_table, downstream_only)
            if downstream_only:
                downstream_rows.append(result_table[1])
            export_sections.append(
                {
                    "title": f"{index}_搜索结果",
                    "rows": [["搜索关键字", keyword_label]] + result_table,
                }
            )
            set_progressbar("progress", 0.2 + index * 0.8 / total)

        if downstream_only:
            put_table_plus([["关键字", "下游依赖"]] + downstream_rows)

        put_table_exports(
            prefix="workspace_search_total",
            name_parts=[
                "downstream_only" if downstream_only else "detail",
                "batch",
                str(total),
            ],
            page_title=f"湖仓全链路搜索（{total} 组关键字）",
            sheet_sections=export_sections,
        )
        set_progressbar("progress", 1)
        put_red_text(
            "========================== 检查完成 =============================="
        )
    except Exception as exc:
        safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("Workspace 依赖搜索", app, port=8025)
