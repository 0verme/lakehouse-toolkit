from datetime import datetime
from importlib import import_module

from core.asset_issue import dedupe_issues as dedupe_asset_issues
from core.public_data import (
    all_disabled_result_tables,
    all_result_table_recv_details,
    all_result_table_sys_names,
)

st = import_module("streamlit")
pd = import_module("pandas")
Styler = import_module("pandas.io.formats.style").Styler

HIGHLIGHT_RESULT_SOURCE_SYSTEMS = [
    "DEMO_CATALOG",
    "DEMO_REPORTING",
    "DEMO_OPERATIONS",
]


MAX_RENDERED_ASSET_ISSUES = 20
OPTIONAL_METADATA_WARNING_TEXT = "部分元数据不可用，相关展示已降级"


def now():
    return datetime.now().strftime("%H:%M:%S")


def append_log(logs, msg):
    logs.append(f"[{now()}] {msg}")


def render_svn_file_section(
    title, files, empty_text=None, color=None, collapsed=False, expanded=False
):
    if not files:
        st.markdown(f"#### {title}")
        st.caption(empty_text or "无")
        return

    content = "<br>".join(files)
    if collapsed:
        with st.expander(title, expanded=expanded):
            if color:
                st.markdown(
                    f"<span style='color:{color}'>{content}</span>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(content, unsafe_allow_html=True)
        return

    st.markdown(f"#### {title}")
    if color:
        st.markdown(
            f"<span style='color:{color}'>{content}</span>", unsafe_allow_html=True
        )
        return
    st.markdown(content, unsafe_allow_html=True)


def inject_global_styles():
    st.markdown(
        """
        <style>
        .svn-check-table th {
            text-align: center !important;
            vertical-align: middle !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_dataframe(df, hide_index=True):
    if isinstance(df, Styler):
        table = df.hide(axis="index") if hide_index else df
        st.markdown(
            table.to_html(table_attributes='class="svn-check-table"'),
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        df.to_html(index=not hide_index, classes="svn-check-table"),
        unsafe_allow_html=True,
    )


def format_elapsed_seconds(elapsed_seconds):
    try:
        total_seconds = max(0, int(elapsed_seconds or 0))
    except (OverflowError, TypeError, ValueError):
        total_seconds = 0
    if total_seconds < 60:
        return f"{total_seconds}秒"
    if total_seconds < 3600:
        minutes, seconds = divmod(total_seconds, 60)
        return f"{minutes}分{seconds:02d}秒"

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}时{minutes:02d}分{seconds:02d}秒"


def render_status(status_box, task_status, sql_files, warnings, elapsed_seconds=0):
    with status_box.container():
        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        c1.metric("任务状态", task_status)
        c2.metric("运行时间", format_elapsed_seconds(elapsed_seconds))
        c3.metric("检查文件数", sql_files)
        c4.metric("告警数", warnings)


def compare_clean(arr1, arr2):
    set1 = set(arr1)
    set2 = set(arr2)
    common = sorted(set1 & set2)
    only_a = sorted(set1 - set2)
    only_b = sorted(set2 - set1)
    data = []
    # ✅ 相同（绿色）
    for item in common:
        data.append({"SQL引用": item, "调度依赖": item, "状态": "相同"})
    # ✅ A独有（红色）
    for item in only_a:
        data.append({"SQL引用": item, "调度依赖": None, "状态": "仅SQL"})
    # ✅ B独有（红色）
    for item in only_b:
        data.append({"SQL引用": None, "调度依赖": item, "状态": "仅调度"})
    df = pd.DataFrame(data, columns=["SQL引用", "调度依赖", "状态"])
    if not df.empty:
        df["_sort_order"] = df.iloc[:, :2].isna().any(axis=1).map({True: 0, False: 1})
        df = (
            df.sort_values("_sort_order", kind="stable")
            .drop(columns="_sort_order")
            .reset_index(drop=True)
        )
    return df


def highlight_row(row):
    if row["状态"] == "相同":
        return ["background-color: #e8f5e9"] * len(row)
    else:
        return ["background-color: #ffebee"] * len(row)


def get_styled_df(arr1, arr2):
    df = compare_clean(arr1, arr2)
    styled_df = df.style.apply(highlight_row, axis=1)
    return styled_df


def normalize_table_name(table_name):
    if table_name is None:
        return ""
    return str(table_name).strip().upper()


def dedupe_table_names(table_names):
    seen = set()
    result = []
    for table_name in table_names:
        normalized = normalize_table_name(table_name)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def build_single_column_df(column_name, table_names):
    return pd.DataFrame({column_name: sorted(dedupe_table_names(table_names))})


def render_rule_messages(result_text, warn_result_text):
    if result_text:
        formatted_text = result_text.replace("\n", "<br>")
        st.markdown(
            f"<span style='color:red'>{formatted_text}</span>", unsafe_allow_html=True
        )
    if warn_result_text:
        formatted_text = warn_result_text.replace("\n", "<br>")
        st.markdown(
            f"<span style='color:#1565c0'>{formatted_text}</span>",
            unsafe_allow_html=True,
        )


def sort_asset_issues(issues):
    return sorted(
        issues or [],
        key=lambda issue: (
            issue.issue_type,
            issue.schema_name,
            issue.table_name,
            issue.root_word,
            issue.source_file,
            issue.issue_key,
        ),
    )


def render_asset_issue_section(issues, section_title="资产待维护"):
    asset_issues = sort_asset_issues(dedupe_asset_issues(issues))
    if not asset_issues:
        return

    st.markdown(f"##### {section_title}")
    hidden_issue_count = max(0, len(asset_issues) - MAX_RENDERED_ASSET_ISSUES)
    for issue in asset_issues[:MAX_RENDERED_ASSET_ISSUES]:
        with st.container(border=True):
            st.write(f"问题类型：{issue.issue_title}")
            if issue.root_word:
                st.write(f"词根值：{issue.root_word}")
            if issue.table_name:
                qualified_table_name = ".".join(
                    [value for value in (issue.schema_name, issue.table_name) if value]
                )
                st.write(f"表名：{qualified_table_name}")
            st.write(f"建议动作：{issue.suggestion}")
            if issue.source_file:
                st.caption(f"来源文件：{issue.source_file}")
            action_label = issue.action_label or "打开资产门户"
            if issue.portal_url:
                if getattr(st, "link_button", None):
                    st.link_button(
                        action_label,
                        issue.portal_url,
                        key=f"asset_issue_{issue.issue_hash_key}",
                    )
                else:
                    st.markdown(f"[{action_label}]({issue.portal_url})")
    if hidden_issue_count:
        st.caption(
            f"还有 {hidden_issue_count} 条待维护问题未展示，请结合审计明细筛选处理"
        )


def normalize_source_system(source_system):
    if source_system is None or pd.isna(source_system):
        return ""
    return str(source_system).strip().upper()


def warn_optional_metadata_unavailable(metadata_name, exc):
    st.warning(f"{OPTIONAL_METADATA_WARNING_TEXT}：{metadata_name}，{exc}")


def load_disabled_registered_result_tables():
    try:
        rows = all_disabled_result_tables() or []
    except Exception as exc:
        warn_optional_metadata_unavailable("已禁用结果表", exc)
        return set()

    disabled_tables = set()
    for row in rows:
        table_name = normalize_table_name(row[0]) if row else ""
        if table_name:
            disabled_tables.add(table_name)
    return disabled_tables


def load_result_table_sys_name_map():
    try:
        rows = all_result_table_sys_names() or []
    except Exception as exc:
        warn_optional_metadata_unavailable("结果表来源系统映射", exc)
        return {}

    table_sys_name_map = {}
    for row in rows:
        if not row or len(row) < 2:
            continue
        table_name = normalize_table_name(row[0])
        sys_name = "" if row[1] is None or pd.isna(row[1]) else str(row[1]).strip()
        if not table_name or not sys_name:
            continue
        if table_name not in table_sys_name_map:
            table_sys_name_map[table_name] = []
        if sys_name not in table_sys_name_map[table_name]:
            table_sys_name_map[table_name].append(sys_name)
    return table_sys_name_map


def load_result_table_recv_detail_map():
    try:
        rows = all_result_table_recv_details() or []
    except Exception as exc:
        warn_optional_metadata_unavailable("结果表接收计划映射", exc)
        return {}

    detail_map = {}
    for row in rows:
        if not row or len(row) < 3:
            continue
        table_name = normalize_table_name(row[0])
        recv_plan = (
            "" if row[1] is None or pd.isna(row[1]) else str(row[1]).strip().upper()
        )
        sys_name = "" if row[2] is None or pd.isna(row[2]) else str(row[2]).strip()
        if not table_name:
            continue
        if table_name not in detail_map:
            detail_map[table_name] = []
        detail = {
            "recv_plan": recv_plan,
            "source_system": sys_name,
        }
        if detail not in detail_map[table_name]:
            detail_map[table_name].append(detail)
    return detail_map


def format_result_table_name(table_name, disabled_tables, table_sys_name_map):
    normalized = normalize_table_name(table_name)
    if not normalized:
        return normalized

    tags = []
    if normalized in disabled_tables:
        tags.append("禁用")

    sys_names = table_sys_name_map.get(normalized, [])
    if sys_names:
        tags.append("/".join(sys_names))

    if tags:
        return f"{normalized}（{'，'.join(tags)}）"
    return normalized


def build_result_table_display_df(
    table_names,
    column_name,
    disabled_tables,
    table_sys_name_map,
    highlight_source_systems=None,
):
    normalized_highlight_source_systems = {
        normalize_source_system(source_system)
        for source_system in (highlight_source_systems or [])
        if normalize_source_system(source_system)
    }
    rows = []
    highlight_flags = []
    for table_name in table_names:
        normalized_table_name = normalize_table_name(table_name)
        rows.append(
            format_result_table_name(table_name, disabled_tables, table_sys_name_map)
        )
        highlight_flags.append(
            normalized_table_name in disabled_tables
            or any(
                normalize_source_system(sys_name) in normalized_highlight_source_systems
                for sys_name in table_sys_name_map.get(normalized_table_name, [])
            )
        )
    display_df = pd.DataFrame(rows, columns=[column_name])
    if not any(highlight_flags):
        return display_df

    def highlight_disabled_or_source_system(row):
        if highlight_flags[row.name]:
            return ["color: #d32f2f; font-weight: 700"] * len(row)
        return [""] * len(row)

    return display_df.style.apply(highlight_disabled_or_source_system, axis=1)


def build_result_compare_display_df(
    result_tables,
    dependency_tables,
    disabled_tables,
    table_sys_name_map,
    highlight_source_systems=None,
):
    result_table_count = len(set(result_tables))
    dependency_table_count = len(set(dependency_tables))
    compare_df = compare_clean(result_tables, dependency_tables)
    if not compare_df.empty:
        sql_ref_col = compare_df.columns[0]
        normalized_highlight_source_systems = {
            normalize_source_system(source_system)
            for source_system in (highlight_source_systems or [])
            if normalize_source_system(source_system)
        }
        highlight_col = "_result_table_highlight"
        compare_df[highlight_col] = compare_df[sql_ref_col].apply(
            lambda value: (
                (
                    normalize_table_name(value) in disabled_tables
                    or any(
                        normalize_source_system(sys_name)
                        in normalized_highlight_source_systems
                        for sys_name in table_sys_name_map.get(
                            normalize_table_name(value), []
                        )
                    )
                )
                if value is not None and not pd.isna(value) and str(value).strip()
                else False
            )
        )
        compare_df[sql_ref_col] = compare_df[sql_ref_col].apply(
            lambda value: (
                format_result_table_name(value, disabled_tables, table_sys_name_map)
                if value is not None and not pd.isna(value) and str(value).strip()
                else value
            )
        )

        highlight_flags = compare_df[highlight_col].copy()
        display_df = compare_df.drop(columns=[highlight_col])
        display_df = display_df.rename(
            columns={
                "SQL引用": f"SQL引用（{result_table_count}）",
                "调度依赖": f"调度依赖（{dependency_table_count}）",
            }
        )

        def highlight_disabled_or_source_system(row):
            styles = [""] * len(row)
            if highlight_flags.loc[row.name]:
                styles[0] = "color: #d32f2f; font-weight: 700"
            return styles

        return display_df.style.apply(highlight_row, axis=1).apply(
            highlight_disabled_or_source_system, axis=1
        )
    return compare_df.rename(
        columns={
            "SQL引用": f"SQL引用（{result_table_count}）",
            "调度依赖": f"调度依赖（{dependency_table_count}）",
        }
    ).style.apply(highlight_row, axis=1)
