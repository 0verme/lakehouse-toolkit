import io

import pandas as pd  # pyright: ignore[reportMissingImports]
import streamlit as st  # pyright: ignore[reportMissingImports]
from db import execute_sql, init_db, query_df

from shared.config.env import safe_identifier

st.set_page_config(page_title="下游接口资产管理", layout="wide")
init_db()

st.title("下游接口资产管理平台")


PAGE_SYSTEM_LIST = "system_list"
PAGE_JOB_LIST = "job_list"
PAGE_SPEC_DETAIL = "spec_detail"


SYSTEM_COLUMNS = [
    "系统名称",
    "湖仓数据源名称",
    "IP",
    "用户名",
    "加密密码",
    "Ftp方式",
    "端口",
    "备注",
]

JOB_COLUMNS = [
    "系统名称",
    "作业名称",
    "文件描述",
    "湖仓路径",
    "下游路径",
    "推送频率",
    "是否启用",
    "备注",
]

SPEC_HEADER_COLUMNS = [
    "系统名称",
    "作业名称",
    "文件名",
    "中文注释",
    "业务逻辑说明",
    "文件备注",
    "分隔符",
    "推送频率",
]

SPEC_FIELD_COLUMNS = [
    "字段序号",
    "字段名称",
    "中文名称",
    "字段含义",
    "数据产生源系统",
    "字段备注",
]


def init_session_state():
    defaults = {
        "page": PAGE_SYSTEM_LIST,
        "selected_system_code": None,
        "selected_system_name": None,
        "selected_job_name": None,
        "selected_file_desc": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def make_excel_download(df: pd.DataFrame, sheet_name: str = "data") -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output


def make_multi_sheet_excel(sheet_map: dict[str, pd.DataFrame]) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output


def filter_df(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    if not keyword:
        return df
    return df[
        df.astype(str).apply(
            lambda row: row.str.contains(keyword, case=False, na=False).any(),
            axis=1,
        )
    ]


def set_page(page: str):
    st.session_state["page"] = page
    st.rerun()


def enter_job_list(system_code: str, system_name: str):
    st.session_state["selected_system_code"] = system_code
    st.session_state["selected_system_name"] = system_name
    st.session_state["selected_job_name"] = None
    st.session_state["selected_file_desc"] = None
    set_page(PAGE_JOB_LIST)


def enter_spec_detail(job_name: str, file_desc: str):
    st.session_state["selected_job_name"] = job_name
    st.session_state["selected_file_desc"] = file_desc
    set_page(PAGE_SPEC_DETAIL)


def back_to_system_list():
    st.session_state["selected_system_code"] = None
    st.session_state["selected_system_name"] = None
    st.session_state["selected_job_name"] = None
    st.session_state["selected_file_desc"] = None
    set_page(PAGE_SYSTEM_LIST)


def back_to_job_list():
    st.session_state["selected_job_name"] = None
    st.session_state["selected_file_desc"] = None
    set_page(PAGE_JOB_LIST)


def run_import_deletes(delete_actions):
    for sql, params in delete_actions:
        execute_sql(sql, params)


def insert_rows(table_name: str, import_df: pd.DataFrame):
    safe_table_name = safe_identifier(table_name, "table")
    db_cols = [safe_identifier(str(column), "column") for column in import_df.columns]
    if not db_cols:
        return 0
    placeholders = ",".join(["?"] * len(db_cols))
    col_sql = ",".join(db_cols)
    insert_sql = (
        """
        INSERT OR REPLACE INTO __TABLE__
        (__COLUMNS__)
        VALUES (__PLACEHOLDERS__)
    """.replace("__TABLE__", safe_table_name)
        .replace("__COLUMNS__", col_sql)
        .replace("__PLACEHOLDERS__", placeholders)
    )

    success_count = 0
    for _, row in import_df.iterrows():
        execute_sql(insert_sql, row.tolist())
        success_count += 1
    return success_count


def import_excel_to_table(
    uploaded_file,
    table_name,
    column_map,
    mode="append",
    delete_actions=None,
    validators=None,
):
    df = pd.read_excel(uploaded_file)
    missing_cols = [c for c in column_map if c not in df.columns]

    if missing_cols:
        st.error(f"模板字段缺失: {', '.join(missing_cols)}")
        return

    import_df = df[list(column_map)].copy()
    import_df = import_df.rename(columns=column_map).fillna("")

    for validator in validators or []:
        error_message = validator(import_df)
        if error_message:
            st.error(error_message)
            return

    if mode == "overwrite":
        safe_table_name = safe_identifier(table_name, "table")
        run_import_deletes(
            delete_actions
            or [("DELETE FROM __TABLE__".replace("__TABLE__", safe_table_name), [])]
        )

    try:
        success_count = insert_rows(table_name, import_df)
    except Exception as exc:
        st.error(f"导入失败: {exc}")
        return

    st.success(f"导入完成，成功导入 {success_count} 行。")


def render_import_area(
    *,
    title,
    template_df,
    export_df,
    table_name,
    column_map,
    mode_key,
    delete_actions=None,
    validators=None,
):
    with st.expander(f"{title} 导入导出", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.download_button(
                "下载模板",
                data=make_excel_download(template_df),
                file_name=f"{title}_模板.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col2:
            st.download_button(
                "导出当前数据",
                data=make_excel_download(export_df),
                file_name=f"{title}_当前数据.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col3:
            mode = st.selectbox(
                "导入模式",
                ["追加导入", "覆盖导入"],
                key=f"mode_{mode_key}",
            )

        uploaded = st.file_uploader(
            "上传 Excel",
            type=["xlsx"],
            key=f"upload_{mode_key}",
        )

        if uploaded and st.button("确认导入", key=f"btn_{mode_key}"):
            import_excel_to_table(
                uploaded,
                table_name,
                column_map,
                mode="overwrite" if mode == "覆盖导入" else "append",
                delete_actions=delete_actions,
                validators=validators,
            )
            st.rerun()


def render_spec_import_area(
    *,
    title,
    header_template_df,
    field_template_df,
    header_export_df,
    field_export_df,
    mode_key,
    delete_actions=None,
    selected_system_code=None,
    selected_job_name=None,
):
    with st.expander(f"{title} 导入导出", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.download_button(
                "下载双 Sheet 模板",
                data=make_multi_sheet_excel(
                    {
                        "文件信息": header_template_df,
                        "字段清单": field_template_df,
                    }
                ),
                file_name=f"{title}_模板.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col2:
            st.download_button(
                "导出当前数据",
                data=make_multi_sheet_excel(
                    {
                        "文件信息": header_export_df,
                        "字段清单": field_export_df,
                    }
                ),
                file_name=f"{title}_当前数据.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with col3:
            mode = st.selectbox(
                "导入模式",
                ["追加导入", "覆盖导入"],
                key=f"mode_{mode_key}",
            )

        st.caption("Sheet1: 文件信息，只保留 1 行。Sheet2: 字段清单，一行一个字段。")

        uploaded = st.file_uploader(
            "上传双 Sheet Excel",
            type=["xlsx"],
            key=f"upload_{mode_key}",
        )

        if uploaded and st.button("确认导入", key=f"btn_{mode_key}"):
            import_spec_workbook(
                uploaded_file=uploaded,
                mode="overwrite" if mode == "覆盖导入" else "append",
                delete_actions=delete_actions,
                selected_system_code=selected_system_code,
                selected_job_name=selected_job_name,
            )
            st.rerun()


def render_sidebar():
    with st.sidebar:
        st.markdown("### 当前导航")
        page_name = {
            PAGE_SYSTEM_LIST: "系统清单",
            PAGE_JOB_LIST: "推数清单",
            PAGE_SPEC_DETAIL: "明细说明",
        }[st.session_state["page"]]
        st.write(f"页面: {page_name}")
        st.write(f"系统: {st.session_state.get('selected_system_code') or '-'}")
        st.write(f"作业: {st.session_state.get('selected_job_name') or '-'}")

        if st.button("回到系统首页", use_container_width=True):
            back_to_system_list()


def validate_job_import(selected_system_code):
    def _validator(df):
        system_codes = {str(v).strip() for v in df["system_code"] if str(v).strip()}
        if not system_codes:
            return "推数清单导入失败: system_code 不能为空。"
        if system_codes != {selected_system_code}:
            return f"推数清单导入失败: 当前页面只允许导入系统 {selected_system_code} 的数据。"
        return None

    return _validator


def read_required_sheet(uploaded_file, sheet_name: str) -> pd.DataFrame | None:
    try:
        return pd.read_excel(uploaded_file, sheet_name=sheet_name)
    except ValueError:
        st.error(f"导入失败: 缺少 Sheet `{sheet_name}`。")
        return None


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def import_spec_workbook(
    *,
    uploaded_file,
    mode,
    delete_actions,
    selected_system_code,
    selected_job_name,
):
    header_df = read_required_sheet(uploaded_file, "文件信息")
    if header_df is None:
        return

    field_df = read_required_sheet(uploaded_file, "字段清单")
    if field_df is None:
        return

    missing_header_cols = [c for c in SPEC_HEADER_COLUMNS if c not in header_df.columns]
    missing_field_cols = [c for c in SPEC_FIELD_COLUMNS if c not in field_df.columns]

    if missing_header_cols:
        st.error(f"文件信息 Sheet 缺少字段: {', '.join(missing_header_cols)}")
        return

    if missing_field_cols:
        st.error(f"字段清单 Sheet 缺少字段: {', '.join(missing_field_cols)}")
        return

    header_df = header_df[SPEC_HEADER_COLUMNS].copy().fillna("")
    field_df = field_df[SPEC_FIELD_COLUMNS].copy().fillna("")

    if len(header_df) != 1:
        st.error("文件信息 Sheet 必须且只能有 1 行。")
        return

    if field_df.empty:
        st.error("字段清单 Sheet 不能为空。")
        return

    header_row = header_df.iloc[0]
    workbook_system_code = normalize_text(header_row["系统名称"])
    workbook_job_name = normalize_text(header_row["作业名称"])

    if not workbook_system_code or not workbook_job_name:
        st.error("文件信息 Sheet 中的系统名称、作业名称不能为空。")
        return

    if workbook_system_code != selected_system_code:
        st.error(f"导入失败: 当前页面只允许导入系统 {selected_system_code} 的数据。")
        return

    if workbook_job_name != selected_job_name:
        st.error(f"导入失败: 当前页面只允许导入作业 {selected_job_name} 的数据。")
        return

    field_df["字段序号"] = pd.to_numeric(field_df["字段序号"], errors="coerce")
    if field_df["字段序号"].isna().any():
        st.error("字段清单 Sheet 中存在非法字段序号，请填写数字。")
        return

    field_seq_values = field_df["字段序号"].astype(int)
    if field_seq_values.duplicated().any():
        st.error("字段清单 Sheet 中字段序号不能重复。")
        return

    header_map = {
        "系统名称": "system_code",
        "作业名称": "job_name",
        "文件名": "file_name",
        "中文注释": "file_comment",
        "业务逻辑说明": "biz_desc",
        "文件备注": "file_remark",
        "分隔符": "delimiter",
        "推送频率": "push_frequency",
    }
    field_map = {
        "字段序号": "field_seq",
        "字段名称": "field_name",
        "中文名称": "field_cn_name",
        "字段含义": "field_meaning",
        "数据产生源系统": "source_system",
        "字段备注": "field_remark",
    }

    header_payload = header_df.rename(columns=header_map).iloc[0].to_dict()
    field_payload = field_df.rename(columns=field_map).copy()

    # 将文件级信息广播到每一个字段行，保持库内结构不变。
    for key, value in header_payload.items():
        field_payload[key] = value

    insert_cols = [
        "system_code",
        "job_name",
        "file_name",
        "file_comment",
        "biz_desc",
        "file_remark",
        "delimiter",
        "push_frequency",
        "field_seq",
        "field_name",
        "field_cn_name",
        "field_meaning",
        "source_system",
        "field_remark",
    ]
    import_df = field_payload[insert_cols].copy()
    import_df["field_seq"] = import_df["field_seq"].astype(int)

    if mode == "overwrite":
        run_import_deletes(delete_actions or [])

    try:
        success_count = insert_rows("file_spec", import_df)
    except Exception as exc:
        st.error(f"导入失败: {exc}")
        return

    st.success(f"导入完成，成功导入 {success_count} 条字段记录。")


def render_row_action_table(
    df: pd.DataFrame, action_label: str, action_key_prefix: str, action_fn
):
    header_cols = st.columns([2, 3, 2, 2, 1])
    headers = ["代码/作业", "名称/描述", "路径/IP", "附加信息", "操作"]
    for col, text in zip(header_cols, headers, strict=False):
        col.markdown(f"**{text}**")

    for idx, row in df.reset_index(drop=True).iterrows():
        cols = st.columns([2, 3, 2, 2, 1])
        cols[0].write(row.iloc[0])
        cols[1].write(row.iloc[1])
        cols[2].write(row.iloc[2])
        cols[3].write(row.iloc[3])
        if cols[4].button(
            action_label, key=f"{action_key_prefix}_{idx}", use_container_width=True
        ):
            action_fn(row)


def render_system_list_page():
    st.subheader("下游系统清单")

    export_df = query_df(
        """
        SELECT
            system_code AS 系统名称,
            system_name AS 湖仓数据源名称,
            ip AS IP,
            username AS 用户名,
            password_enc AS 加密密码,
            ftp_type AS Ftp方式,
            port AS 端口,
            remark AS 备注
        FROM downstream_system
        ORDER BY system_code
        """
    )

    template_df = pd.DataFrame(columns=SYSTEM_COLUMNS)
    column_map = {
        "系统名称": "system_code",
        "湖仓数据源名称": "system_name",
        "IP": "ip",
        "用户名": "username",
        "加密密码": "password_enc",
        "Ftp方式": "ftp_type",
        "端口": "port",
        "备注": "remark",
    }

    render_import_area(
        title="下游系统清单",
        template_df=template_df,
        export_df=export_df,
        table_name="downstream_system",
        column_map=column_map,
        mode_key="system_import",
        delete_actions=[
            ("DELETE FROM file_spec", []),
            ("DELETE FROM push_job", []),
            ("DELETE FROM downstream_system", []),
        ],
    )

    keyword = st.text_input("搜索系统名称 / 湖仓数据源名称 / IP")
    df = filter_df(export_df, keyword)

    st.caption(f"共 {len(df)} 条系统记录")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        st.info("当前没有系统数据，请先导入系统清单。")
        return

    st.markdown("### 进入系统推数清单")
    action_df = df[["系统名称", "湖仓数据源名称", "IP", "Ftp方式"]].copy()

    def _enter(row):
        enter_job_list(row["系统名称"], row["湖仓数据源名称"])

    render_row_action_table(action_df, "查看推数", "system_enter", _enter)


def render_job_list_page():
    selected_system_code = st.session_state.get("selected_system_code")
    selected_system_name = st.session_state.get("selected_system_name")

    if not selected_system_code:
        st.warning("请先从系统清单进入具体系统。")
        return

    top_cols = st.columns([1, 6])
    if top_cols[0].button("返回系统清单", use_container_width=True):
        back_to_system_list()
    top_cols[1].markdown(
        f"**系统清单 > {selected_system_code}**"
        + (f"  ({selected_system_name})" if selected_system_name else "")
    )

    st.subheader(f"{selected_system_code} 推数清单")

    system_info_df = query_df(
        """
        SELECT
            system_code AS 系统名称,
            system_name AS 湖仓数据源名称,
            ip AS IP,
            username AS 用户名,
            ftp_type AS Ftp方式,
            port AS 端口,
            remark AS 备注
        FROM downstream_system
        WHERE system_code = ?
        """,
        [selected_system_code],
    )

    if not system_info_df.empty:
        st.dataframe(system_info_df, use_container_width=True, hide_index=True)

    export_df = query_df(
        """
        SELECT
            system_code AS 系统名称,
            job_name AS 作业名称,
            file_desc AS 文件描述,
            lake_path AS 湖仓路径,
            target_path AS 下游路径,
            push_frequency AS 推送频率,
            enabled_flag AS 是否启用,
            remark AS 备注
        FROM push_job
        WHERE system_code = ?
        ORDER BY job_name
        """,
        [selected_system_code],
    )

    template_df = pd.DataFrame(columns=JOB_COLUMNS)
    column_map = {
        "系统名称": "system_code",
        "作业名称": "job_name",
        "文件描述": "file_desc",
        "湖仓路径": "lake_path",
        "下游路径": "target_path",
        "推送频率": "push_frequency",
        "是否启用": "enabled_flag",
        "备注": "remark",
    }

    render_import_area(
        title=f"{selected_system_code}_推数清单",
        template_df=template_df,
        export_df=export_df,
        table_name="push_job",
        column_map=column_map,
        mode_key="job_import",
        delete_actions=[
            ("DELETE FROM push_job WHERE system_code = ?", [selected_system_code])
        ],
        validators=[validate_job_import(selected_system_code)],
    )

    keyword = st.text_input("搜索作业名称 / 文件描述 / 湖仓路径")
    df = filter_df(export_df, keyword)

    st.caption(f"当前系统共 {len(df)} 条推数记录")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        st.info("当前系统还没有推数记录。")
        return

    st.markdown("### 进入明细说明")
    action_df = df[["作业名称", "文件描述", "湖仓路径", "推送频率"]].copy()

    def _enter(row):
        enter_spec_detail(row["作业名称"], row["文件描述"])

    render_row_action_table(action_df, "查看明细", "job_enter", _enter)


def build_spec_export_frames(export_df: pd.DataFrame):
    header_export_df = pd.DataFrame(columns=SPEC_HEADER_COLUMNS)
    field_export_df = pd.DataFrame(columns=SPEC_FIELD_COLUMNS)

    if export_df.empty:
        return header_export_df, field_export_df

    header_row = export_df.iloc[0]
    header_export_df = pd.DataFrame(
        [
            {
                "系统名称": header_row["系统名称"],
                "作业名称": header_row["作业名称"],
                "文件名": header_row["文件名"],
                "中文注释": header_row["中文注释"],
                "业务逻辑说明": header_row["业务逻辑说明"],
                "文件备注": header_row["文件备注"],
                "分隔符": header_row["分隔符"],
                "推送频率": header_row["推送频率"],
            }
        ]
    )

    field_export_df = export_df[SPEC_FIELD_COLUMNS].copy()
    return header_export_df, field_export_df


def render_spec_detail_page():
    selected_system_code = st.session_state.get("selected_system_code")
    selected_job_name = st.session_state.get("selected_job_name")
    selected_file_desc = st.session_state.get("selected_file_desc")

    if not selected_system_code or not selected_job_name:
        st.warning("请先从推数清单进入具体作业。")
        return

    top_cols = st.columns([1, 6])
    if top_cols[0].button("返回推数清单", use_container_width=True):
        back_to_job_list()
    top_cols[1].markdown(f"**系统清单 > {selected_system_code} > {selected_job_name}**")

    st.subheader("推数明细")
    st.info(
        f"当前记录: {selected_system_code} | {selected_job_name}"
        + (f" | {selected_file_desc}" if selected_file_desc else "")
    )

    export_df = query_df(
        """
        SELECT
            system_code AS 系统名称,
            job_name AS 作业名称,
            file_name AS 文件名,
            file_comment AS 中文注释,
            biz_desc AS 业务逻辑说明,
            file_remark AS 文件备注,
            delimiter AS 分隔符,
            push_frequency AS 推送频率,
            field_seq AS 字段序号,
            field_name AS 字段名称,
            field_cn_name AS 中文名称,
            field_meaning AS 字段含义,
            source_system AS 数据产生源系统,
            field_remark AS 字段备注
        FROM file_spec
        WHERE system_code = ?
          AND job_name = ?
        ORDER BY field_seq
        """,
        [selected_system_code, selected_job_name],
    )

    header_export_df, field_export_df = build_spec_export_frames(export_df)

    render_spec_import_area(
        title=f"{selected_system_code}_{selected_job_name}_字段明细",
        header_template_df=pd.DataFrame(columns=SPEC_HEADER_COLUMNS),
        field_template_df=pd.DataFrame(columns=SPEC_FIELD_COLUMNS),
        header_export_df=header_export_df,
        field_export_df=field_export_df,
        mode_key="spec_import",
        delete_actions=[
            (
                "DELETE FROM file_spec WHERE system_code = ? AND job_name = ?",
                [selected_system_code, selected_job_name],
            )
        ],
        selected_system_code=selected_system_code,
        selected_job_name=selected_job_name,
    )

    job_info_df = query_df(
        """
        SELECT
            system_code AS 系统名称,
            job_name AS 作业名称,
            file_desc AS 文件描述,
            lake_path AS 湖仓路径,
            target_path AS 下游路径,
            push_frequency AS 推送频率,
            enabled_flag AS 是否启用,
            remark AS 备注
        FROM push_job
        WHERE system_code = ?
          AND job_name = ?
        """,
        [selected_system_code, selected_job_name],
    )

    if not job_info_df.empty:
        st.markdown("### 推数任务摘要")
        st.dataframe(job_info_df, use_container_width=True, hide_index=True)

    st.markdown("### 文件头信息")

    if export_df.empty:
        st.info("当前作业还没有字段明细，请先导入双 Sheet 明细 Excel。")
        return

    header_row = export_df.iloc[0]
    header_df = pd.DataFrame(
        [
            {"项目": "中文注释", "内容": header_row["中文注释"]},
            {"项目": "文件名", "内容": header_row["文件名"]},
            {"项目": "业务逻辑说明", "内容": header_row["业务逻辑说明"]},
            {"项目": "文件备注", "内容": header_row["文件备注"]},
            {"项目": "分隔符", "内容": header_row["分隔符"]},
            {"项目": "推送频率", "内容": header_row["推送频率"]},
        ]
    )
    st.table(header_df)

    st.markdown("### 字段清单")
    st.dataframe(field_export_df, use_container_width=True, hide_index=True)


init_session_state()
render_sidebar()

page = st.session_state["page"]
if page == PAGE_SYSTEM_LIST:
    render_system_list_page()
elif page == PAGE_JOB_LIST:
    render_job_list_page()
else:
    render_spec_detail_page()
