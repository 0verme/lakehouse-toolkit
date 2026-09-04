import os
import time
from importlib import import_module
from urllib.parse import quote

from core.fine_rule import rule_authority, rule_fine, rule_menu
from core.public_data import all_para_table_lists
from services.ai_service import call_sql_llm
from services.re_service import (
    build_export_download_url,
    load_txt_to_df,
    load_txt_to_df2,
)
from services.svn_service import svn_main

from shared.lineage.mapping_sqlite import load_registered_result_tables
from ui.public_stream import (
    HIGHLIGHT_RESULT_SOURCE_SYSTEMS,
    append_log,
    build_result_table_display_df,
    build_single_column_df,
    dedupe_table_names,
    inject_global_styles,
    load_disabled_registered_result_tables,
    load_result_table_sys_name_map,
    normalize_table_name,
    render_dataframe,
    render_rule_messages,
    render_status,
    render_svn_file_section,
)

st = import_module("streamlit")


def fine_stream(
    strict_mode, status_box, log_box, progress_bar, step_text, project, svn_path
):
    inject_global_styles()
    log_box.code("等待开始...")
    progress_bar.progress(5)
    task_status = "运行中"
    warnings = 0
    logs = []
    start_ts = time.time()
    sql_files = 0

    def refresh_status():
        try:
            render_status(
                status_box,
                task_status,
                sql_files,
                warnings,
                int(time.time() - start_ts),
            )
        except (AttributeError, TypeError, ValueError) as exc:
            status_box.warning(f"状态展示失败: {type(exc).__name__}")

    refresh_status()

    step_text.info("当前步骤：读取数据库参数")
    step_text.info("当前步骤：读取数据库")
    log_box.code("\n".join(logs))

    append_log(logs, f"开始处理：{svn_path}")
    progress_bar.progress(20)
    step_text.info("当前步骤：拉取 SVN")
    log_box.code("\n".join(logs))

    svn_result = svn_main(project, svn_path)
    r_lists = svn_result["exported_paths"]
    branch_changed_files = svn_result["branch_changed_files"]
    trunk_conflict_files = svn_result["trunk_conflict_files"]
    sql_files = len(r_lists)
    append_log(logs, "SVN 拉取完成")
    log_box.code("\n".join(logs))
    refresh_status()

    with st.container(border=True):
        render_svn_file_section(
            f"SVN 变更文件（{len(branch_changed_files)}）",
            branch_changed_files,
            empty_text="未识别到本次分支变更文件",
            collapsed=True,
        )
        render_svn_file_section(
            f"最新 trunk 重叠文件（{len(trunk_conflict_files)}）",
            trunk_conflict_files,
            empty_text="未发现 trunk 后续重叠修改",
            color="red",
        )

    cpt_lists = []
    menu_path = ""
    permission_file = ""
    for i in r_lists:
        if i.endswith((".frm", ".cpt")):
            cpt_lists.append(i)
        elif "menu.txt" in i:
            menu_path = i
        elif "authority.txt" in i:
            permission_file = i

    progress_bar.progress(25)
    append_log(logs, "打印目录、权限信息")
    log_box.code("\n".join(logs))

    memu_lists = []
    if menu_path:
        st.markdown("#### 目录检查")
        with st.container(border=True):
            render_dataframe(
                load_txt_to_df(menu_path, ["后台目录", "前台目录", "预览方式"]),
                hide_index=True,
            )
            memu_lists, memu_reslut_text, memu_warncnt = rule_menu(menu_path)
            render_rule_messages(memu_reslut_text, "")
        warnings += memu_warncnt
        refresh_status()

    if permission_file:
        st.markdown("#### 权限检查")
        with st.container(border=True):
            render_dataframe(
                load_txt_to_df2(permission_file, ["前台目录", "赋予权限"]),
                hide_index=True,
            )
            authority_reslut_text, authority_warncnt = rule_authority(
                permission_file, memu_lists
            )
            render_rule_messages(authority_reslut_text, "")
        warnings += authority_warncnt
        refresh_status()

    progress_bar.progress(30)
    step_text.info("当前步骤：分析文件")
    append_log(logs, "cpt、frm 分析")
    log_box.code("\n".join(logs))

    ai_boxes = {}
    registered_result_tables = load_registered_result_tables()
    para_tables = {
        normalize_table_name(row[0]) for row in all_para_table_lists() if row and row[0]
    }
    disabled_registered_result_tables = load_disabled_registered_result_tables()
    result_table_sys_name_map = load_result_table_sys_name_map()

    st.markdown("#### 帆软检查")
    for idx, i in enumerate(cpt_lists):
        relsut_test, cpt_warncnt, rrtotal = rule_fine(i)
        card_id = f"card_{idx}"
        warnings += cpt_warncnt
        normalized_sql_tables = dedupe_table_names(rrtotal[4])
        result_sql_tables = [
            t for t in normalized_sql_tables if t in registered_result_tables
        ]
        code_sql_tables = [t for t in normalized_sql_tables if t in para_tables]
        middle_sql_tables = [
            t
            for t in normalized_sql_tables
            if t not in registered_result_tables and t not in para_tables
        ]
        with st.container(border=True):
            preview_root = os.getenv(
                "REPORT_PREVIEW_BASE_URL", "http://localhost:8500/reports"
            )
            preview_url = f"{preview_root.rstrip('/')}/preview?viewlet={quote(str(rrtotal[0]), safe='')}"
            download_url = build_export_download_url(i)
            st.markdown(
                f'<a href="{preview_url}" target="_blank">预览报表</a>',
                unsafe_allow_html=True,
            )
            object_line = f"检查对象：{rrtotal[0]}"
            if download_url:
                object_line = f'{object_line} <a href="{download_url}" target="_blank">下载代码</a>'
            st.markdown(object_line, unsafe_allow_html=True)
            st.write(f"数据源：{rrtotal[1]}")
            st.write(f"是否使用引擎：{rrtotal[2]}")
            st.write(f"sheet页数量：{len(rrtotal[3])}个：{rrtotal[3]}")
            render_rule_messages(relsut_test, "")

            st.markdown(f"##### 结果表（{len(result_sql_tables)}）")
            render_dataframe(
                build_result_table_display_df(
                    result_sql_tables,
                    "结果表",
                    disabled_registered_result_tables,
                    result_table_sys_name_map,
                    HIGHLIGHT_RESULT_SOURCE_SYSTEMS,
                ),
                hide_index=True,
            )
            st.markdown(f"##### 码值表（{len(code_sql_tables)}）")
            render_dataframe(
                build_single_column_df("码值表", code_sql_tables), hide_index=True
            )
            st.markdown(f"##### 中间表（{len(middle_sql_tables)}）")
            render_dataframe(
                build_single_column_df("中间表", middle_sql_tables), hide_index=True
            )

            if strict_mode:
                st.markdown("#### 🤖 AI分析")
                ai_box = st.empty()
                ai_box.info("AI分析中...")
                ai_boxes[card_id] = ai_box

        refresh_status()

    progress_bar.progress(80)
    step_text.info("当前步骤：大模型检查")
    if strict_mode:
        append_log(logs, "接入行内大模型")
        for idx, i in enumerate(cpt_lists):
            card_id = f"card_{idx}"
            ai_result = call_sql_llm(i)
            ai_boxes[card_id].info(ai_result)
            refresh_status()

    progress_bar.progress(100)
    step_text.success("当前步骤：完成")
    task_status = "完成"
    append_log(logs, "任务完成")
    log_box.code("\n".join(logs))
    refresh_status()
