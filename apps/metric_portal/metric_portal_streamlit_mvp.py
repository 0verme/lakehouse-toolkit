import hashlib
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pandas as pd  # pyright: ignore[reportMissingImports]
import streamlit as st  # pyright: ignore[reportMissingImports]

# =========================
# 基础配置
# =========================
st.set_page_config(
    page_title="指标搜索门户 MVP",
    page_icon="📊",
    layout="wide",
)

DB_PATH = Path(
    os.getenv(
        "PYTOOLS_METRIC_DB_PATH",
        str(
            Path(__file__).resolve().parents[2]
            / "runtime"
            / "metric_portal"
            / "metric_portal.db"
        ),
    )
).expanduser()


# =========================
# 数据库层
# =========================
@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> bool:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metric_definition (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_alias TEXT,
                metric_desc TEXT,
                metric_caliber TEXT,
                source_table TEXT,
                source_column TEXT,
                layer TEXT,
                owner TEXT,
                tags TEXT,
                status TEXT DEFAULT '启用',
                update_freq TEXT,
                biz_domain TEXT,
                metric_sql TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metric_name
            ON metric_definition(metric_name)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_layer
            ON metric_definition(layer)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_owner
            ON metric_definition(owner)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_user (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '启用',
                create_time TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        row = conn.execute("SELECT COUNT(1) AS cnt FROM app_user").fetchone()
        if row["cnt"] == 0:
            username = os.getenv("ADMIN_USERNAME", "").strip()
            password = os.getenv("ADMIN_PASSWORD", "")
            if not username or not password:
                return False
            conn.execute(
                """
                INSERT INTO app_user (username, password_hash, role, status)
                VALUES (?, ?, ?, ?)
                """,
                (username, hash_password(password), "admin", "启用"),
            )
    return True


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_user(username: str, password: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT username, role, status FROM app_user WHERE username = ? AND password_hash = ?",
            (username.strip(), hash_password(password)),
        ).fetchone()
        if row and row["status"] == "启用":
            return {"username": row["username"], "role": row["role"]}
    return None


def is_logged_in() -> bool:
    return bool(st.session_state.get("logged_in"))


def list_distinct_values(column: str) -> list[str]:
    with get_conn() as conn:
        if column == "layer":
            rows = conn.execute(
                "SELECT DISTINCT layer AS val FROM metric_definition "
                "WHERE layer IS NOT NULL AND TRIM(layer) <> '' ORDER BY layer"
            ).fetchall()
        elif column == "biz_domain":
            rows = conn.execute(
                "SELECT DISTINCT biz_domain AS val FROM metric_definition "
                "WHERE biz_domain IS NOT NULL AND TRIM(biz_domain) <> '' ORDER BY biz_domain"
            ).fetchall()
        elif column == "owner":
            rows = conn.execute(
                "SELECT DISTINCT owner AS val FROM metric_definition "
                "WHERE owner IS NOT NULL AND TRIM(owner) <> '' ORDER BY owner"
            ).fetchall()
        elif column == "status":
            rows = conn.execute(
                "SELECT DISTINCT status AS val FROM metric_definition "
                "WHERE status IS NOT NULL AND TRIM(status) <> '' ORDER BY status"
            ).fetchall()
        else:
            raise ValueError(f"不支持的指标筛选列: {column}")
    return [r["val"] for r in rows]


def search_metrics(
    keyword: str = "",
    layer: str = "全部",
    biz_domain: str = "全部",
    owner: str = "全部",
    status: str = "全部",
) -> pd.DataFrame:
    keyword_pattern = f"%{keyword.strip()}%" if keyword.strip() else ""
    sql = """
        SELECT
            id,
            metric_name,
            metric_alias,
            biz_domain,
            layer,
            source_table,
            owner,
            status,
            update_freq,
            tags,
            updated_at
        FROM metric_definition
        WHERE
            (
                ? = '' OR
                metric_name LIKE ? OR
                COALESCE(metric_alias, '') LIKE ? OR
                COALESCE(metric_desc, '') LIKE ? OR
                COALESCE(metric_caliber, '') LIKE ? OR
                COALESCE(source_table, '') LIKE ? OR
                COALESCE(tags, '') LIKE ?
            )
            AND (? = '全部' OR layer = ?)
            AND (? = '全部' OR biz_domain = ?)
            AND (? = '全部' OR owner = ?)
            AND (? = '全部' OR status = ?)
        ORDER BY updated_at DESC, id DESC
    """
    params = [
        keyword_pattern,
        keyword_pattern,
        keyword_pattern,
        keyword_pattern,
        keyword_pattern,
        keyword_pattern,
        keyword_pattern,
        layer,
        layer,
        biz_domain,
        biz_domain,
        owner,
        owner,
        status,
        status,
    ]

    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_metric_detail(metric_id: int) -> sqlite3.Row | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM metric_definition WHERE id = ?",
            (metric_id,),
        ).fetchone()
    return row


def upsert_metric(data: dict, metric_id: int | None = None) -> None:
    with get_conn() as conn:
        if metric_id is None:
            conn.execute(
                """
                INSERT INTO metric_definition (
                    metric_name, metric_alias, metric_desc, metric_caliber,
                    source_table, source_column, layer, owner, tags, status,
                    update_freq, biz_domain, metric_sql, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    data["metric_name"],
                    data.get("metric_alias", ""),
                    data.get("metric_desc", ""),
                    data.get("metric_caliber", ""),
                    data.get("source_table", ""),
                    data.get("source_column", ""),
                    data.get("layer", ""),
                    data.get("owner", ""),
                    data.get("tags", ""),
                    data.get("status", "启用"),
                    data.get("update_freq", ""),
                    data.get("biz_domain", ""),
                    data.get("metric_sql", ""),
                ),
            )
        else:
            conn.execute(
                """
                UPDATE metric_definition
                SET metric_name = ?,
                    metric_alias = ?,
                    metric_desc = ?,
                    metric_caliber = ?,
                    source_table = ?,
                    source_column = ?,
                    layer = ?,
                    owner = ?,
                    tags = ?,
                    status = ?,
                    update_freq = ?,
                    biz_domain = ?,
                    metric_sql = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    data["metric_name"],
                    data.get("metric_alias", ""),
                    data.get("metric_desc", ""),
                    data.get("metric_caliber", ""),
                    data.get("source_table", ""),
                    data.get("source_column", ""),
                    data.get("layer", ""),
                    data.get("owner", ""),
                    data.get("tags", ""),
                    data.get("status", "启用"),
                    data.get("update_freq", ""),
                    data.get("biz_domain", ""),
                    data.get("metric_sql", ""),
                    metric_id,
                ),
            )


def delete_metric(metric_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM metric_definition WHERE id = ?", (metric_id,))


# =========================
# 页面组件
# =========================
def render_header() -> None:
    st.title("📊 指标搜索门户 MVP")
    st.caption(
        "轻量版：先解决指标录入、搜索、查看。后续可接 embedding / AI 推荐 / 血缘。"
    )


def render_login_box() -> None:
    with st.sidebar:
        st.markdown("### 用户状态")
        if is_logged_in():
            st.success(
                f"已登录：{st.session_state.get('username')}（{st.session_state.get('role')}）"
            )
            if st.button("退出登录", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["username"] = None
                st.session_state["role"] = None
                st.session_state["show_new_form"] = False
                st.rerun()
        else:
            with st.form("login_form"):
                username = st.text_input("用户名")
                password = st.text_input("密码", type="password")
                submitted = st.form_submit_button("登录")
                if submitted:
                    user = verify_user(username, password)
                    if user:
                        st.session_state["logged_in"] = True
                        st.session_state["username"] = user["username"]
                        st.session_state["role"] = user["role"]
                        st.success("登录成功")
                        st.rerun()
                    else:
                        st.error("用户名或密码错误")
            st.caption(
                "首次运行请先设置 ADMIN_USERNAME 和 ADMIN_PASSWORD；系统不会创建默认管理员。"
            )


def render_readonly_notice() -> None:
    if not is_logged_in():
        st.info("当前为只读模式。登录后才可以新增、编辑、删除、初始化指标数据。")


def render_sidebar_filters() -> tuple[str, str, str, str, str]:
    st.sidebar.header("筛选条件")
    keyword = st.sidebar.text_input(
        "关键词", placeholder="指标名 / 别名 / 口径 / 来源表 / 标签"
    )

    layer_options = ["全部"] + list_distinct_values("layer")
    biz_options = ["全部"] + list_distinct_values("biz_domain")
    owner_options = ["全部"] + list_distinct_values("owner")
    status_options = ["全部"] + list_distinct_values("status")

    layer = st.sidebar.selectbox("数仓层级", layer_options)
    biz_domain = st.sidebar.selectbox("业务域", biz_options)
    owner = st.sidebar.selectbox("负责人", owner_options)
    status = st.sidebar.selectbox("状态", status_options)

    return keyword, layer, biz_domain, owner, status


def render_metric_form(metric_id: int | None = None) -> None:
    if not is_logged_in():
        return

    detail = get_metric_detail(metric_id) if metric_id else None
    title = "新增指标" if detail is None else f"编辑指标 #{metric_id}"

    # Streamlit requires the form to remain inside the expander to preserve layout semantics.
    with (
        st.expander(
            title,
            expanded=(detail is None and st.session_state.get("show_new_form", False))
            or detail is not None,
        ),
        st.form(key=f"metric_form_{metric_id or 'new'}", clear_on_submit=False),
    ):
        col1, col2 = st.columns(2)
        with col1:
            metric_name = st.text_input(
                "指标名称 *", value=detail["metric_name"] if detail else ""
            )
            metric_alias = st.text_input(
                "指标别名", value=detail["metric_alias"] if detail else ""
            )
            biz_domain = st.text_input(
                "业务域", value=detail["biz_domain"] if detail else ""
            )
            layer = st.text_input("数仓层级", value=detail["layer"] if detail else "")
            owner = st.text_input("负责人", value=detail["owner"] if detail else "")
            status = st.selectbox(
                "状态",
                ["启用", "停用", "建设中"],
                index=["启用", "停用", "建设中"].index(detail["status"])
                if detail and detail["status"] in ["启用", "停用", "建设中"]
                else 0,
            )

        with col2:
            update_freq = st.text_input(
                "更新频率",
                value=detail["update_freq"] if detail else "",
                placeholder="日 / 月 / 实时",
            )
            source_table = st.text_input(
                "来源表", value=detail["source_table"] if detail else ""
            )
            source_column = st.text_input(
                "来源字段", value=detail["source_column"] if detail else ""
            )
            tags = st.text_input(
                "标签",
                value=detail["tags"] if detail else "",
                placeholder="逗号分隔",
            )

        metric_desc = st.text_area(
            "指标说明", value=detail["metric_desc"] if detail else "", height=80
        )
        metric_caliber = st.text_area(
            "统计口径", value=detail["metric_caliber"] if detail else "", height=120
        )
        metric_sql = st.text_area(
            "指标SQL", value=detail["metric_sql"] if detail else "", height=180
        )

        submitted = st.form_submit_button("保存")
        if submitted:
            if not metric_name.strip():
                st.error("指标名称不能为空")
            else:
                upsert_metric(
                    {
                        "metric_name": metric_name.strip(),
                        "metric_alias": metric_alias.strip(),
                        "metric_desc": metric_desc.strip(),
                        "metric_caliber": metric_caliber.strip(),
                        "source_table": source_table.strip(),
                        "source_column": source_column.strip(),
                        "layer": layer.strip(),
                        "owner": owner.strip(),
                        "tags": tags.strip(),
                        "status": status,
                        "update_freq": update_freq.strip(),
                        "biz_domain": biz_domain.strip(),
                        "metric_sql": metric_sql.strip(),
                    },
                    metric_id=metric_id,
                )
                st.success("保存成功，刷新后可见最新结果")


def render_metric_detail(metric_id: int) -> None:
    row = get_metric_detail(metric_id)
    if not row:
        st.warning("未找到该指标")
        return

    st.subheader(f"📌 {row['metric_name']}")
    meta1, meta2, meta3, meta4 = st.columns(4)
    meta1.metric("业务域", row["biz_domain"] or "-")
    meta2.metric("数仓层级", row["layer"] or "-")
    meta3.metric("负责人", row["owner"] or "-")
    meta4.metric("状态", row["status"] or "-")

    st.markdown("### 基础信息")
    st.write(f"**别名：** {row['metric_alias'] or '-'}")
    st.write(f"**来源表：** {row['source_table'] or '-'}")
    st.write(f"**来源字段：** {row['source_column'] or '-'}")
    st.write(f"**更新频率：** {row['update_freq'] or '-'}")
    st.write(f"**标签：** {row['tags'] or '-'}")

    st.markdown("### 指标说明")
    st.write(row["metric_desc"] or "-")

    st.markdown("### 统计口径")
    st.write(row["metric_caliber"] or "-")

    st.markdown("### 指标SQL")
    st.code(row["metric_sql"] or "-- 暂无SQL", language="sql")


def render_metric_tree(df: pd.DataFrame) -> None:
    st.subheader("🌲 指标目录树")
    if df.empty:
        st.info("当前条件下没有可展示的指标目录")
        return

    tree_df = df[["id", "metric_name", "biz_domain", "layer", "owner"]].copy()
    tree_df["biz_domain"] = tree_df["biz_domain"].fillna("").replace("", "未归类业务域")
    tree_df["layer"] = tree_df["layer"].fillna("").replace("", "未归类层级")

    biz_list = sorted(tree_df["biz_domain"].unique().tolist())
    for biz in biz_list:
        biz_df = tree_df[tree_df["biz_domain"] == biz]
        with st.expander(f"📁 {biz}（{len(biz_df)}）", expanded=True):
            layer_list = sorted(biz_df["layer"].unique().tolist())
            for layer in layer_list:
                layer_df = biz_df[biz_df["layer"] == layer]
                st.markdown(f"**├─ {layer}（{len(layer_df)}）**")
                for _, row in layer_df.sort_values(["metric_name", "id"]).iterrows():
                    col1, col2 = st.columns([5, 1])
                    with col1:
                        owner_text = (
                            f" · {row['owner']}" if str(row["owner"]).strip() else ""
                        )
                        st.write(f"└─ {row['metric_name']}{owner_text}")
                    with col2:
                        if st.button("查看", key=f"tree_view_{row['id']}"):
                            try:
                                selected_metric_id = int(row["id"])
                            except (TypeError, ValueError):
                                st.error("指标 ID 格式无效，无法打开详情")
                            else:
                                st.session_state["selected_metric_id"] = (
                                    selected_metric_id
                                )


def render_main_table(df: pd.DataFrame) -> None:
    st.subheader("🔎 搜索结果")
    st.caption(f"共 {len(df)} 条")
    if df.empty:
        st.info("没有查到符合条件的指标")
        return

    tab1, tab2 = st.tabs(["表格视图", "目录树视图"])

    with tab1:
        show_df = df.copy()
        st.dataframe(
            show_df,
            use_container_width=True,
            hide_index=True,
        )

        selected_id = st.selectbox(
            "选择一个指标查看详情",
            options=show_df["id"].tolist(),
            format_func=lambda x: (
                f"{x} - {show_df.loc[show_df['id'] == x, 'metric_name'].iloc[0]}"
            ),
        )

        if is_logged_in():
            col1, col2, col3 = st.columns([1, 1, 5])
            with col1:
                if st.button("查看详情", use_container_width=True):
                    st.session_state["selected_metric_id"] = selected_id
            with col2:
                if st.button("删除指标", use_container_width=True):
                    delete_metric(selected_id)
                    st.success("删除成功，请手动刷新或重新搜索")
        else:
            col1, col2 = st.columns([1, 6])
            with col1:
                if st.button("查看详情", use_container_width=True):
                    st.session_state["selected_metric_id"] = selected_id

    with tab2:
        render_metric_tree(df)


def render_toolbar() -> None:
    if not is_logged_in():
        return

    col1, col2 = st.columns([1, 7])
    with col1:
        if st.button("新增指标", use_container_width=True):
            st.session_state["show_new_form"] = True


# =========================
# 主流程
# =========================
def main() -> None:
    if not init_db():
        st.error(
            "尚未初始化管理员。请在启动环境中配置 ADMIN_USERNAME 和 ADMIN_PASSWORD 后重试。"
        )
        st.stop()
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
        st.session_state["username"] = None
        st.session_state["role"] = None
    if "show_new_form" not in st.session_state:
        st.session_state["show_new_form"] = False
    if "selected_metric_id" not in st.session_state:
        st.session_state["selected_metric_id"] = None

    render_header()
    render_login_box()
    render_readonly_notice()
    render_toolbar()

    keyword, layer, biz_domain, owner, status = render_sidebar_filters()

    left, right = st.columns([1.4, 1])

    with left:
        df = search_metrics(
            keyword=keyword,
            layer=layer,
            biz_domain=biz_domain,
            owner=owner,
            status=status,
        )
        render_metric_form(metric_id=None)
        render_main_table(df)

    with right:
        selected_metric_id = st.session_state.get("selected_metric_id")
        if selected_metric_id:
            render_metric_detail(selected_metric_id)
            if is_logged_in():
                st.markdown("---")
                render_metric_form(metric_id=selected_metric_id)


if __name__ == "__main__":
    main()
