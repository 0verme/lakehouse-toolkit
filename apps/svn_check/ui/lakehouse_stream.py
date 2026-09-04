import html
import json
import re
import time
import traceback
from importlib import import_module
from pathlib import Path

from core.lakehouse.ddl_rule import collect_root_missing_issues
from core.lakehouse.python_rule import build_asset_table_review_issues
from core.lakehouse.sql_rule import collect_created_table_review_issues
from core.lakehouse_rule import (
    all_program_df,
    get_lakehouse_type,
    get_program_table_name,
    rule_config,
    rule_dwf,
    rule_dwo,
    rule_dws,
    rule_dws_py,
    rule_excle_job,
    rule_excle_plan,
    rule_excle_seq,
    rule_hive,
    rule_recv_json,
    rule_sbin,
)
from core.public_data import all_job, all_job_outfile, all_para_table_lists
from services.ai_service import call_sql_llm
from services.diag_service import log_session_state_snapshot, log_task_event
from services.re_service import (
    build_dependency_table_lookup,
    build_export_download_url,
    build_job_outfile_lookup,
    build_program_lookup,
    build_wide_table_lineage_summary,
    get_filename,
    get_program_lookup_result,
    get_yilai_table_from_lookup,
    load_xls_to_df,
    merge_job_program,
    read_data_from_file,
    safe_remove_prefix,
)
from services.workspace_service import load_svn_workspace

from shared.lineage.mapping_sqlite import load_registered_result_tables
from ui.public_stream import (
    HIGHLIGHT_RESULT_SOURCE_SYSTEMS,
    append_log,
    build_result_compare_display_df,
    build_single_column_df,
    dedupe_table_names,
    inject_global_styles,
    load_disabled_registered_result_tables,
    load_result_table_recv_detail_map,
    load_result_table_sys_name_map,
    normalize_table_name,
    render_asset_issue_section,
    render_dataframe,
    render_rule_messages,
    render_status,
    render_svn_file_section,
)

pd = import_module("pandas")
st = import_module("streamlit")

cale_map = {
    "SYS_MONTH_END_CALENDAR": "每月末",
    "SYS_EVERYDAY_CALENDAR": "每日",
}

TIMING_LOG_THRESHOLD_SECONDS = 1.0


def sort_df_by_first_column(df):
    if df is None or df.empty:
        return df
    first_col = df.columns[0]
    return df.sort_values(
        by=first_col,
        key=lambda col: col.astype(str).str.strip().str.upper(),
        kind="stable",
    ).reset_index(drop=True)


def normalize_job_name(job_name):
    if job_name is None or pd.isna(job_name):
        return ""
    return str(job_name).strip().upper()


def decode_job_status(status):
    status_text = "" if status is None or pd.isna(status) else str(status).strip()
    if status_text in ("1", "1.0"):
        return "启用"
    if status_text in ("9", "9.0"):
        return "禁用"
    return status_text


def load_disabled_job_names(job_rows=None):
    if job_rows is None:
        job_rows = all_job() or []
    return {
        normalize_job_name(row[2])
        for row in job_rows
        if len(row) > 23
        and normalize_job_name(row[2])
        and str(row[23]).strip() in ("9", "9.0")
    }


def build_job_df_from_rows(job_df, job_rows):
    db_df = pd.DataFrame(job_rows, columns=job_df.columns)
    return pd.concat([job_df, db_df], ignore_index=True)


def build_job_display_df(job_df, job_rows=None):
    display_df = job_df.copy()
    if job_rows is None:
        job_rows = all_job() or []
    prod_job_rows = {
        normalize_job_name(row[2]): row
        for row in job_rows
        if len(row) > 23 and normalize_job_name(row[2])
    }
    job_col = display_df.columns[2]
    status_col = display_df.columns[23] if len(display_df.columns) > 23 else None

    def row_state(row):
        prod_row = prod_job_rows.get(normalize_job_name(row[job_col]))
        if prod_row is None:
            return "new"
        if str(prod_row[23]).strip() in ("9", "9.0"):
            return "disabled"
        return ""

    row_states = display_df.apply(row_state, axis=1)
    if status_col is not None:
        display_df[status_col] = display_df[status_col].apply(decode_job_status)

    def highlight_job_row(row):
        state = row_states.loc[row.name]
        if state == "new":
            return ["background-color: #e8f5e9"] * len(row)
        if state == "disabled":
            return ["background-color: #ffebee"] * len(row)
        return [""] * len(row)

    return display_df.style.apply(highlight_job_row, axis=1)


def table_name_from_program_path(path_value):
    if path_value is None or pd.isna(path_value):
        return ""
    folder = Path(str(path_value)).parent.name
    if "." not in folder:
        return ""
    schema, table_name = folder.split(".", 1)
    schema = schema.replace("DWS_", "")
    return normalize_table_name(f"{schema}.{table_name}")


def collect_program_result_tables(merge_df, prog_path_col):
    if prog_path_col not in merge_df.columns:
        return set()
    return {
        table_name
        for table_name in merge_df[prog_path_col].apply(table_name_from_program_path)
        if table_name
    }


def build_json_display_df(data):
    if isinstance(data, dict):
        return pd.DataFrame(
            [
                {"字段": key, "值": "" if value is None else str(value)}
                for key, value in data.items()
            ]
        )

    if isinstance(data, list):
        if not data:
            return pd.DataFrame([{"结果": "[]"}])
        if all(isinstance(item, dict) for item in data):
            headers = sorted({key for item in data for key in item})
            return pd.DataFrame(
                [
                    {
                        h: "" if item.get(h) is None else str(item.get(h, ""))
                        for h in headers
                    }
                    for item in data
                ]
            )
        return pd.DataFrame(
            [
                {"序号": index, "值": "" if item is None else str(item)}
                for index, item in enumerate(data, start=1)
            ]
        )

    return pd.DataFrame([{"结果": "" if data is None else str(data)}])


def render_schema_config_tables(config_paths):
    for config_path in config_paths:
        file_name = Path(config_path).name
        st.markdown(f"##### {file_name}")
        try:
            content = Path(config_path).read_text(encoding="utf-8")
            json_data = json.loads(content)
            render_dataframe(build_json_display_df(json_data), hide_index=True)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            st.warning(f"{file_name} 无法按 JSON 表格展示: {exc}")


def render_wide_table_lineage_summary(summary):
    st.markdown("##### 宽表依赖链路摘要")
    summary_rows = [
        {"字段": "计划", "值": summary.get("plan_name") or "未匹配"},
        {"字段": "作业", "值": summary.get("job_name") or "未匹配"},
        {"字段": "程序", "值": summary.get("program_name") or "未匹配"},
        {"字段": "程序路径", "值": summary.get("program_path") or "未匹配"},
        {"字段": "推导结果表", "值": summary.get("result_table") or "未匹配"},
        {
            "字段": "依赖作业",
            "值": " | ".join(summary.get("dependency_jobs") or []) or "未匹配",
        },
        {
            "字段": "依赖结果表",
            "值": " | ".join(summary.get("dependency_result_tables") or []) or "未匹配",
        },
        {"字段": "recv plan", "值": summary.get("recv_plan") or "未匹配"},
        {"字段": "来源系统", "值": summary.get("source_system") or "未匹配"},
        {"字段": "outfile", "值": summary.get("outfile") or "未匹配"},
    ]
    render_dataframe(pd.DataFrame(summary_rows), hide_index=True)

    missing_steps = summary.get("missing_steps") or []
    if missing_steps:
        st.caption("断链提示：" + " / ".join(missing_steps))

    with st.expander("查看链路明细", expanded=False):
        render_dataframe(
            pd.DataFrame(summary.get("source_fields") or []), hide_index=True
        )


# =========================
# 各检查区段渲染函数
# =========================


def _render_sql_checks(dws_url, hive_url, sbin_lists, schame_config_lists, recv_lists):
    warnings = 0

    if dws_url:
        st.markdown("#### dws.sql检查")
        with st.container(border=True):
            download_url = build_export_download_url(dws_url)
            script_line = f"检查脚本：{get_filename(dws_url)}"
            if download_url:
                script_line = f'{script_line} <a href="{download_url}" target="_blank">下载代码</a>'
            st.markdown(script_line, unsafe_allow_html=True)
            dws_reslut_text, dws_warn_result_text, dws_warncnt = rule_dws(dws_url)
            render_rule_messages(dws_reslut_text, dws_warn_result_text)
            asset_issues = collect_root_missing_issues(
                read_data_from_file(dws_url),
                source_module="lakehouse",
                source_file=safe_remove_prefix(dws_url),
            ) + collect_created_table_review_issues(
                dws_url,
                source_module="lakehouse",
                source_file=safe_remove_prefix(dws_url),
            )
            render_asset_issue_section(asset_issues)
        warnings += dws_warncnt

    if hive_url:
        st.markdown("#### hive.sql检查")
        with st.container(border=True):
            download_url = build_export_download_url(hive_url)
            script_line = f"检查脚本：{get_filename(hive_url)}"
            if download_url:
                script_line = f'{script_line} <a href="{download_url}" target="_blank">下载代码</a>'
            st.markdown(script_line, unsafe_allow_html=True)
            hive_reslut_text, hive_warn_result_text, hive_warncnt = rule_hive(hive_url)
            render_rule_messages(hive_reslut_text, hive_warn_result_text)
        warnings += hive_warncnt

    if sbin_lists:
        st.markdown("#### sbin后置脚本检查")
        with st.container(border=True):
            sbin_reslut_text, sbin_warn_result_text, sbin_warncnt = rule_sbin(
                sbin_lists
            )
            render_rule_messages(sbin_reslut_text, sbin_warn_result_text)
        warnings += sbin_warncnt

    if schame_config_lists:
        st.markdown("#### schame_config连接信息检查")
        with st.container(border=True):
            config_reslut_text, config_warn_result_text, config_warncnt = rule_config(
                schame_config_lists
            )
            render_rule_messages(config_reslut_text, config_warn_result_text)
            render_schema_config_tables(schame_config_lists)
        warnings += config_warncnt

    if recv_lists:
        st.markdown("#### recv卸数检查")
        with st.container(border=True):
            recv_reslut_text, recv_warn_result_text, recv_warncnt = rule_recv_json(
                recv_lists
            )
            render_rule_messages(recv_reslut_text, recv_warn_result_text)
        warnings += recv_warncnt

    return warnings


def _render_schedule_section(
    plan_xls, seq_xls, cale_xls, job_xls, program_xls, append_timing_log
):
    """Returns (warnings, job_df, r_plan, db_job_rows, program_df)."""
    warnings = 0
    job_df = None
    program_df = None
    db_job_rows = None
    r_plan = None

    if program_xls:
        stage_start = time.time()
        program_df = load_xls_to_df(program_xls)
        append_timing_log(
            f"PROGRAM Excel 读取完成：{len(program_df)} 行，{time.time() - stage_start:.2f}s"
        )

    if plan_xls:
        stage_start = time.time()
        st.markdown("#### PLAN计划清单")
        with st.container(border=True):
            sub_stage_start = time.time()
            plan_source_df = load_xls_to_df(plan_xls)
            if plan_source_df is None:
                raise ValueError(f"无法读取 PLAN Excel: {plan_xls}")
            append_timing_log(
                f"PLAN Excel 读取完成：{len(plan_source_df)} 行，{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            plan_df = sort_df_by_first_column(plan_source_df.iloc[:, [0, 4]])
            if plan_df is None:
                raise ValueError(f"无法整理 PLAN Excel: {plan_xls}")
            plan_df = plan_df.fillna("")
            plan_df.columns = ["计划名", "前置依赖"]
            append_timing_log(
                f"PLAN 表格整理完成：{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            render_dataframe(plan_df, hide_index=True)
            append_timing_log(
                f"PLAN 表格渲染完成：{time.time() - sub_stage_start:.2f}s"
            )
            try:
                sub_stage_start = time.time()
                plan_reslut_text, plan_warn_result_text, plan_warncnt, r_plan = (
                    rule_excle_plan(plan_df)
                )
                append_timing_log(
                    f"PLAN 规则检查完成：{time.time() - sub_stage_start:.2f}s"
                )
            except Exception:
                print(traceback.format_exc())
                raise
            render_rule_messages(plan_reslut_text, plan_warn_result_text)
        warnings += plan_warncnt
        append_timing_log(f"PLAN 调度分析完成：{time.time() - stage_start:.2f}s")

    if seq_xls:
        stage_start = time.time()
        st.markdown("#### SEQ作业流清单")
        with st.container(border=True):
            sub_stage_start = time.time()
            seq_source_df = load_xls_to_df(seq_xls)
            if seq_source_df is None:
                raise ValueError(f"无法读取 SEQ Excel: {seq_xls}")
            append_timing_log(
                f"SEQ Excel 读取完成：{len(seq_source_df)} 行，{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            seq_df = sort_df_by_first_column(seq_source_df.iloc[:, [0, 1, 2]])
            if seq_df is None:
                raise ValueError(f"无法整理 SEQ Excel: {seq_xls}")
            seq_df = seq_df.fillna("")
            seq_df.columns = ["计划名", "作业流名", "作业流描述"]
            append_timing_log(f"SEQ 表格整理完成：{time.time() - sub_stage_start:.2f}s")
            sub_stage_start = time.time()
            render_dataframe(seq_df, hide_index=True)
            append_timing_log(f"SEQ 表格渲染完成：{time.time() - sub_stage_start:.2f}s")
            try:
                sub_stage_start = time.time()
                seq_reslut_text, seq_warn_result_text, seq_warncnt = rule_excle_seq(
                    seq_df
                )
                append_timing_log(
                    f"SEQ 规则检查完成：{time.time() - sub_stage_start:.2f}s"
                )
            except Exception:
                print(traceback.format_exc())
                raise
            render_rule_messages(seq_reslut_text, seq_warn_result_text)
        warnings += seq_warncnt
        append_timing_log(f"SEQ 调度分析完成：{time.time() - stage_start:.2f}s")

    if cale_xls:
        stage_start = time.time()
        st.markdown("#### CALE日历清单")
        with st.container(border=True):
            sub_stage_start = time.time()
            cale_df = load_xls_to_df(cale_xls)
            append_timing_log(
                f"CALE Excel 读取完成：{len(cale_df)} 行，{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            render_dataframe(cale_df, hide_index=True)
            append_timing_log(
                f"CALE 表格渲染完成：{time.time() - sub_stage_start:.2f}s"
            )
        append_timing_log(f"CALE 调度分析完成：{time.time() - stage_start:.2f}s")

    if job_xls:
        stage_start = time.time()
        st.markdown("#### JOB作业清单(绿色新增，红色禁用再上线)")
        with st.container(border=True):
            sub_stage_start = time.time()
            job_source_df = load_xls_to_df(job_xls)
            append_timing_log(
                f"JOB Excel 读取完成：{len(job_source_df)} 行，{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            job_df = sort_df_by_first_column(job_source_df)
            if job_df is None:
                raise ValueError(f"无法整理 JOB Excel: {job_xls}")
            job_df_tb = job_df.iloc[:, :4].fillna("")
            job_df_tb.columns = ["计划名", "作业流名", "作业名", "作业描述"]
            append_timing_log(f"JOB 表格整理完成：{time.time() - sub_stage_start:.2f}s")
            sub_stage_start = time.time()
            db_job_rows = all_job() or []
            append_timing_log(
                f"JOB 线上作业查询完成：{len(db_job_rows)} 行，{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            job_display_df = build_job_display_df(job_df_tb, db_job_rows)
            append_timing_log(
                f"JOB 展示状态合并完成：{time.time() - sub_stage_start:.2f}s"
            )
            sub_stage_start = time.time()
            render_dataframe(job_display_df, hide_index=True)
            append_timing_log(f"JOB 表格渲染完成：{time.time() - sub_stage_start:.2f}s")
            try:
                sub_stage_start = time.time()
                job_reslut_text, job_warn_result_text, job_warncnt = rule_excle_job(
                    job_df,
                    r_plan=r_plan,
                    timing_log=append_timing_log,
                    job_rows=db_job_rows,
                )
                append_timing_log(
                    f"JOB 规则检查完成：{time.time() - sub_stage_start:.2f}s"
                )
            except Exception:
                print(traceback.format_exc())
                raise
            render_rule_messages(job_reslut_text, job_warn_result_text)
        warnings += job_warncnt
        append_timing_log(f"JOB 调度分析完成：{time.time() - stage_start:.2f}s")

    return warnings, job_df, r_plan, db_job_rows, program_df


def _render_dwo_dwf_checks(dwo_lists, dwf_lists):
    warnings = 0

    if dwo_lists:
        st.markdown("#### dwo检查")
        for i in dwo_lists:
            dwo_reslut_text, dwo_warn_result_text, dwo_warncnt = rule_dwo(i)
            if dwo_warncnt > 0 or dwo_warn_result_text:
                with st.container(border=True):
                    st.write(f"检查脚本：{safe_remove_prefix(i)}")
                    render_rule_messages(dwo_reslut_text, dwo_warn_result_text)
            warnings += dwo_warncnt

    if dwf_lists:
        st.markdown("#### dwf检查")
        for i in dwf_lists:
            dwf_reslut_text, dwf_warn_result_text, dwf_warncnt = rule_dwf(i)
            if dwf_warncnt > 0 or dwf_warn_result_text:
                with st.container(border=True):
                    st.write(f"检查脚本：{safe_remove_prefix(i)}")
                    render_rule_messages(dwf_reslut_text, dwf_warn_result_text)
            warnings += dwf_warncnt

    return warnings


def _render_program_checks(
    py_lists, job_df, program_df, db_job_rows, append_timing_log, strict_mode
):
    """Returns (warnings, ai_boxes)."""
    warnings = 0
    ai_boxes = {}

    process_check_start = time.time()
    stage_start = time.time()

    sub_stage_start = time.time()
    if db_job_rows is None:
        db_job_rows = all_job() or []
        append_timing_log(
            f"all_job 查询完成：{len(db_job_rows)} 行，{time.time() - sub_stage_start:.2f}s"
        )
    else:
        append_timing_log(
            f"all_job 复用调度阶段查询结果：{len(db_job_rows)} 行，{time.time() - sub_stage_start:.2f}s"
        )

    sub_stage_start = time.time()
    mergejob_df = build_job_df_from_rows(job_df, db_job_rows)
    append_timing_log(f"all_job_df 合并完成：{time.time() - sub_stage_start:.2f}s")

    sub_stage_start = time.time()
    mergeprogram_df = all_program_df(program_df)
    append_timing_log(f"all_program_df 完成：{time.time() - sub_stage_start:.2f}s")

    program_path_col = mergeprogram_df.columns[4]
    sub_stage_start = time.time()
    mergetotal_df = merge_job_program(mergejob_df, mergeprogram_df)
    append_timing_log(f"merge_job_program 完成：{time.time() - sub_stage_start:.2f}s")
    append_timing_log(f"JOB/PROGRAM 合并完成：{time.time() - stage_start:.2f}s")

    stage_start = time.time()
    sub_stage_start = time.time()
    registered_result_tables = load_registered_result_tables()
    append_timing_log(
        f"load_registered_result_tables 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    disabled_registered_result_tables = load_disabled_registered_result_tables()
    append_timing_log(
        f"load_disabled_registered_result_tables 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    result_table_sys_name_map = load_result_table_sys_name_map()
    append_timing_log(
        f"load_result_table_sys_name_map 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    result_table_recv_detail_map = load_result_table_recv_detail_map()
    append_timing_log(
        f"load_result_table_recv_detail_map 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    disabled_job_names = load_disabled_job_names(db_job_rows)
    append_timing_log(
        f"load_disabled_job_names 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    job_outfile_lookup = build_job_outfile_lookup(all_job_outfile())
    append_timing_log(
        f"build_job_outfile_lookup 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    program_result_tables = collect_program_result_tables(
        mergetotal_df, program_path_col
    )
    append_timing_log(
        f"collect_program_result_tables 完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    all_result_tables = registered_result_tables | program_result_tables
    append_timing_log(
        f"all_result_tables 合并完成：{time.time() - sub_stage_start:.2f}s"
    )

    sub_stage_start = time.time()
    para_tables = {
        normalize_table_name(row[0]) for row in all_para_table_lists() if row and row[0]
    }
    append_timing_log(
        f"all_para_table_lists 完成：{time.time() - sub_stage_start:.2f}s"
    )
    append_timing_log(f"结果表集合加载完成：{time.time() - stage_start:.2f}s")

    stage_start = time.time()
    program_lookup = build_program_lookup(
        mergetotal_df, program_path_col, tail_levels=4
    )
    dependency_table_lookup = build_dependency_table_lookup(mergetotal_df)
    append_timing_log(f"加工程序索引构建完成：{time.time() - stage_start:.2f}s")

    st.markdown("#### 加工程序检查")
    for idx, i in enumerate(py_lists):
        stage_start = time.time()
        pydws_reslut_text, pydws_warn_result_text, pydws_warncnt, sql_table = (
            rule_dws_py(i)
        )
        append_timing_log(
            f"加工程序规则检查完成：{get_filename(i)}，{time.time() - stage_start:.2f}s"
        )
        card_id = f"card_{idx}"
        warnings += pydws_warncnt
        result = get_program_lookup_result(
            program_lookup, safe_remove_prefix(i), tail_levels=4
        )
        yilai_table = get_yilai_table_from_lookup(result[2], dependency_table_lookup)
        table_name = get_program_table_name(i)
        normalized_sql_tables = dedupe_table_names(sql_table)
        normalized_yilai_tables = dedupe_table_names(yilai_table)
        current_result_tables = all_result_tables | {normalize_table_name(table_name)}
        result_sql_tables = [
            t for t in normalized_sql_tables if t in current_result_tables
        ]
        code_sql_tables = [t for t in normalized_sql_tables if t in para_tables]
        middle_sql_tables = [
            t
            for t in normalized_sql_tables
            if t not in current_result_tables and t not in para_tables
        ]
        review_table_candidates = list(middle_sql_tables)
        normalized_result_table_name = normalize_table_name(table_name)
        if (
            normalized_result_table_name
            and normalized_result_table_name not in registered_result_tables
        ):
            review_table_candidates.append(normalized_result_table_name)
        download_url = build_export_download_url(i)
        with st.container(border=True):
            script_line = f"检查脚本：{get_filename(i)}"
            if download_url:
                script_line = f'{script_line} <a href="{download_url}" target="_blank">下载代码</a>'
            st.markdown(script_line, unsafe_allow_html=True)
            st.write(f"表名：{table_name}")
            if normalize_job_name(result[0]) in disabled_job_names:
                st.markdown(
                    f"<span style='color:#d32f2f;font-weight:700'>作业名：{html.escape(str(result[0]))}（禁用）</span>",
                    unsafe_allow_html=True,
                )
            else:
                st.write(f"作业名：{result[0]}")
            st.write(f"跑批频率：{cale_map.get(result[1], result[1])}")
            render_wide_table_lineage_summary(
                build_wide_table_lineage_summary(
                    mergetotal_df,
                    safe_remove_prefix(i),
                    job_outfile_lookup=job_outfile_lookup,
                    result_table_recv_detail_map=result_table_recv_detail_map,
                    tail_levels=4,
                )
            )
            render_rule_messages(pydws_reslut_text, pydws_warn_result_text)
            asset_issues = collect_root_missing_issues(
                read_data_from_file(i),
                source_module="lakehouse",
                source_file=safe_remove_prefix(i),
            ) + build_asset_table_review_issues(
                review_table_candidates,
                source_module="lakehouse",
                source_file=safe_remove_prefix(i),
            )
            render_asset_issue_section(asset_issues)
            st.markdown(f"##### 结果表（{len(result_sql_tables)}）")
            render_dataframe(
                build_result_compare_display_df(
                    result_sql_tables,
                    normalized_yilai_tables,
                    disabled_registered_result_tables,
                    result_table_sys_name_map,
                    HIGHLIGHT_RESULT_SOURCE_SYSTEMS,
                ),
                hide_index=True,
            )
            st.markdown(f"##### 码值参数表（{len(code_sql_tables)}）")
            render_dataframe(
                build_single_column_df("码值表", code_sql_tables), hide_index=True
            )
            st.markdown(f"##### 中间临时表（{len(middle_sql_tables)}）")
            render_dataframe(
                build_single_column_df("中间表", middle_sql_tables), hide_index=True
            )

            if strict_mode:
                st.markdown("#### 🤖 AI分析")
                ai_box = st.empty()
                ai_box.info("AI分析中...")
                ai_boxes[card_id] = ai_box

    append_timing_log(f"加工程序检查完成：{time.time() - process_check_start:.2f}s")
    return warnings, ai_boxes


# =========================
# 主入口
# =========================


def lakehouse_stream(
    strict_mode,
    status_box,
    log_box,
    progress_bar,
    step_text,
    project,
    svn_path,
    debug_mode=False,
    workspace_info=None,
):
    log_task_event(
        "lakehouse_stream",
        "start",
        project=project,
        svn_path=svn_path,
        strict_mode=bool(strict_mode),
        debug_mode=bool(debug_mode),
    )
    log_session_state_snapshot("TASK_START_LAKEHOUSE")
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

    def append_timing_log(message):
        elapsed_match = re.search(r"([0-9]+(?:\.[0-9]+)?)s\s*$", message)
        try:
            elapsed_seconds = float(elapsed_match.group(1)) if elapsed_match else None
        except (AttributeError, TypeError, ValueError):
            elapsed_seconds = None
        if (
            not debug_mode
            and elapsed_seconds is not None
            and elapsed_seconds < TIMING_LOG_THRESHOLD_SECONDS
        ):
            return
        print(message, flush=True)
        append_log(logs, message)
        log_box.code("\n".join(logs))

    refresh_status()
    step_text.info("当前步骤：读取数据库参数")
    step_text.info("当前步骤：读取数据库")
    log_box.code("\n".join(logs))

    source_mode = workspace_info["source_type"] if workspace_info else "svn"
    source_label = workspace_info["source_label"] if workspace_info else svn_path

    # Step 1: workspace 加载
    append_log(logs, f"开始处理：{source_label}")
    progress_bar.progress(10)
    step_text.info(
        "当前步骤：拉取 SVN" if source_mode == "svn" else "当前步骤：读取本地目录"
    )
    log_box.code("\n".join(logs))

    log_task_event(
        "lakehouse_stream.workspace_load",
        "start",
        project=project,
        svn_path=svn_path,
        source_mode=source_mode,
    )
    if workspace_info is None:
        workspace_info = load_svn_workspace(project, svn_path)
        append_log(logs, "SVN 拉取完成")
    else:
        append_log(logs, "本地目录加载完成")

    r_lists = workspace_info["exported_paths"]
    branch_changed_files = workspace_info["branch_changed_files"]
    trunk_conflict_files = workspace_info["trunk_conflict_files"]
    sql_files = len(r_lists)
    log_task_event(
        "lakehouse_stream.workspace_load",
        "end",
        sql_files=sql_files,
        source_mode=source_mode,
    )
    log_box.code("\n".join(logs))

    with st.container(border=True):
        if source_mode == "svn":
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
        else:
            render_svn_file_section(
                f"待审计文件列表（{len(branch_changed_files)}）",
                branch_changed_files,
                empty_text="未发现可审计文件",
                collapsed=True,
            )

    refresh_status()

    (
        dws_url,
        hive_url,
        schame_config_lists,
        sbin_lists,
        recv_lists,
        dwo_lists,
        dwf_lists,
        _dlo_meta,
        _dlo,
        py_lists,
        plan_xls,
        seq_xls,
        job_xls,
        program_xls,
        cale_xls,
    ) = get_lakehouse_type(r_lists)

    # Step 2: 脚本分析
    progress_bar.progress(30)
    step_text.info("当前步骤：分析文件")
    append_log(logs, "分析sql执行语句hive/dws")
    log_box.code("\n".join(logs))
    warnings += _render_sql_checks(
        dws_url, hive_url, sbin_lists, schame_config_lists, recv_lists
    )
    refresh_status()

    # Step 3: 调度分析
    append_log(logs, "打印待上线调度信息")
    log_box.code("\n".join(logs))
    append_log(logs, "分析调度规范")
    log_box.code("\n".join(logs))

    schedule_stage_start = time.time()
    schedule_warnings, job_df, r_plan, db_job_rows, program_df = (
        _render_schedule_section(
            plan_xls,
            seq_xls,
            cale_xls,
            job_xls,
            program_xls,
            append_timing_log,
        )
    )
    warnings += schedule_warnings
    refresh_status()
    append_timing_log(f"分析调度规范完成：{time.time() - schedule_stage_start:.2f}s")

    progress_bar.progress(50)
    append_log(logs, "分析加工脚本代码规范")
    log_box.code("\n".join(logs))

    warnings += _render_dwo_dwf_checks(dwo_lists, dwf_lists)
    refresh_status()
    progress_bar.progress(60)

    ai_boxes = {}
    if py_lists:
        if job_df is None:
            st.error("未找到 JOB Excel，无法关联加工程序与作业信息")
            return
        if program_df is None:
            st.error("未找到 PROGRAM Excel，无法关联加工程序路径")
            return
        py_warnings, ai_boxes = _render_program_checks(
            py_lists,
            job_df,
            program_df,
            db_job_rows,
            append_timing_log,
            strict_mode,
        )
        warnings += py_warnings
        refresh_status()

    progress_bar.progress(75)

    # Step 4: 大模型检查
    progress_bar.progress(80)
    step_text.info("当前步骤：大模型检查")
    if strict_mode:
        append_log(logs, "接入行内大模型")
        for idx, i in enumerate(py_lists):
            card_id = f"card_{idx}"
            ai_result = call_sql_llm(i)
            ai_boxes[card_id].info(ai_result)
            refresh_status()

    # 完成
    progress_bar.progress(100)
    step_text.success("当前步骤：完成")
    task_status = "完成"
    append_log(logs, "任务完成")
    log_box.code("\n".join(logs))
    refresh_status()
    log_task_event(
        "lakehouse_stream",
        "end",
        project=project,
        svn_path=svn_path,
        sql_files=sql_files,
        warnings=warnings,
    )
    log_session_state_snapshot("TASK_END_LAKEHOUSE")
