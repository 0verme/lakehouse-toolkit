import time
from importlib import import_module

from core.upstream_rule import get_upstream_type, rule_dws, rule_dws_py
from services.ai_service import call_sql_llm
from services.re_service import (
    build_export_download_url,
    get_filename,
    safe_remove_prefix,
)
from services.svn_service import svn_main

from ui.public_stream import (
    append_log,
    build_single_column_df,
    dedupe_table_names,
    inject_global_styles,
    render_dataframe,
    render_rule_messages,
    render_status,
    render_svn_file_section,
)

st = import_module("streamlit")


def upstream_stream(
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

    dws_url, py_lists = get_upstream_type(r_lists)

    progress_bar.progress(35)
    step_text.info("当前步骤：分析文件")
    append_log(logs, "分析 UPSTREAM SQL 和 UPSTREAM 加工脚本")
    log_box.code("\n".join(logs))

    if dws_url:
        st.markdown("#### UPSTREAM SQL检查")
        for sql_path in dws_url:
            with st.container(border=True):
                st.write(f"检查脚本：{get_filename(sql_path)}")
                dws_result_text, dws_warncnt = rule_dws(sql_path)
                render_rule_messages(dws_result_text, "")
            warnings += dws_warncnt
            refresh_status()

    ai_boxes = {}
    if py_lists:
        st.markdown("#### UPSTREAM加工程序检查")
        for idx, py_path in enumerate(py_lists):
            py_result_text, py_warning_text, py_warncnt, sql_tables = rule_dws_py(
                py_path
            )
            py_result_text += py_warning_text
            warnings += py_warncnt
            card_id = f"card_{idx}"
            download_url = build_export_download_url(py_path)

            with st.container(border=True):
                import html as _html

                script_line = f"检查脚本：{_html.escape(get_filename(py_path))}"
                if download_url:
                    script_line = f'{script_line} <a href="{download_url}" target="_blank">下载代码</a>'
                st.markdown(script_line, unsafe_allow_html=True)
                st.caption(safe_remove_prefix(py_path))
                render_rule_messages(py_result_text, "")
                st.markdown(f"##### SQL引用表（{len(dedupe_table_names(sql_tables))}）")
                render_dataframe(
                    build_single_column_df("SQL引用表", sql_tables), hide_index=True
                )

                if strict_mode:
                    st.markdown("#### AI分析")
                    ai_box = st.empty()
                    ai_box.info("AI分析中...")
                    ai_boxes[card_id] = ai_box
            refresh_status()

    progress_bar.progress(80)
    step_text.info("当前步骤：大模型检查")
    if strict_mode:
        append_log(logs, "接入行内大模型")
        log_box.code("\n".join(logs))
        for idx, py_path in enumerate(py_lists):
            card_id = f"card_{idx}"
            ai_result = call_sql_llm(py_path)
            ai_boxes[card_id].info(ai_result)
            refresh_status()

    progress_bar.progress(100)
    step_text.success("当前步骤：完成")
    task_status = "完成"
    append_log(logs, "任务完成")
    log_box.code("\n".join(logs))
    refresh_status()
