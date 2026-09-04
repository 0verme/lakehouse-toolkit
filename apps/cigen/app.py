import hashlib
import os
from contextlib import closing
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import connect_with_profile

pd = import_module("pandas")
st = import_module("streamlit")

DB_PROFILE = os.getenv("PYTOOLS_DB_PROFILE", "demo")
USER_TABLE = metadata_table("app_users", "app_users")
ROOT_TABLE = metadata_table("term_roots", "term_roots")
BANNED_WORDS = {"tmp", "test", "aaa", "bbb", "field1", "zzz", "demo"}
TYPE_TO_EXPECTED_SUFFIX = {
    "date": ["dt"],
    "timestamp": ["ts", "tm"],
    "datetime": ["ts", "tm", "dt"],
    "time": ["tm", "ts"],
    "decimal": ["amt", "bal", "rate", "pct"],
    "numeric": ["amt", "bal", "rate", "pct"],
    "number": ["amt", "bal", "rate", "pct", "cnt"],
    "int": ["cnt", "id", "flag", "amt"],
    "bigint": ["cnt", "id", "flag", "amt"],
    "smallint": ["cnt", "id", "flag"],
    "boolean": ["flag"],
}


@dataclass
class ColumnDef:
    name: str
    data_type: str


@dataclass
class Issue:
    object_type: str
    object_name: str
    issue_level: str
    issue_type: str
    issue_desc: str
    suggest_name: str = ""


def get_conn():
    return connect_with_profile(DB_PROFILE)


def _render_table_sql(template: str, table_name: str) -> str:
    return template.replace("__TABLE__", table_name)


def query_df(sql: str, params: tuple | None = None) -> Any:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        try:
            if params is None:
                # pi-lens-ignore: python-sql-injection
                cur.execute(sql)
            else:
                # pi-lens-ignore: python-sql-injection
                cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=columns)
        finally:
            cur.close()


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_user(username: str, password: str) -> dict | None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        try:
            # pi-lens-ignore: python-sql-injection
            cur.execute(
                _render_table_sql(
                    "SELECT username, role, status FROM __TABLE__ "
                    "WHERE username = ? AND password_hash = ?",
                    USER_TABLE,
                ),
                (username.strip(), hash_password(password)),
            )
            row = cur.fetchone()
            if row and row[2] == "启用":
                return {"username": row[0], "role": row[1]}
        finally:
            cur.close()
    return None


def is_admin() -> bool:
    return st.session_state.get("role") == "admin"


def render_login_box() -> None:
    with st.sidebar:
        st.markdown("### 用户状态")
        if st.session_state.get("logged_in"):
            st.success(
                f"已登录：{st.session_state.get('username')}（{st.session_state.get('role')}）"
            )
            if st.button("退出登录", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["username"] = None
                st.session_state["role"] = None
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


def render_readonly_notice() -> None:
    if not is_admin():
        st.info("当前为只读模式。登录后才可以新增、修改、导入词根。")


def load_roots() -> Any:
    return query_df(
        _render_table_sql(
            "SELECT id, root_code, root_cn, category, status, remark, create_time "
            "FROM __TABLE__ ORDER BY id",
            ROOT_TABLE,
        )
    )


def upsert_root(
    root_code: str, root_cn: str, category: str, status: str, remark: str
) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        try:
            params = (
                root_code.strip(),
                root_cn.strip(),
                category.strip(),
                status.strip(),
                remark.strip(),
            )
            # pi-lens-ignore: python-sql-injection
            cur.execute(
                _render_table_sql(
                    "SELECT id FROM __TABLE__ WHERE root_code = ?",
                    ROOT_TABLE,
                ),
                (params[0],),
            )
            exists = cur.fetchone()
            if exists:
                # pi-lens-ignore: python-sql-injection
                cur.execute(
                    _render_table_sql(
                        """
                        UPDATE __TABLE__
                        SET root_cn = ?, category = ?, status = ?, remark = ?
                        WHERE root_code = ?
                        """,
                        ROOT_TABLE,
                    ),
                    (params[1], params[2], params[3], params[4], params[0]),
                )
            else:
                # pi-lens-ignore: python-sql-injection
                cur.execute(
                    _render_table_sql(
                        """
                        INSERT INTO __TABLE__ (root_code, root_cn, category, status, remark)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        ROOT_TABLE,
                    ),
                    params,
                )
            conn.commit()
        finally:
            cur.close()


def import_roots_from_excel(df: Any) -> tuple[int, list[str]]:
    required_cols = ["root_code", "root_cn"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return 0, missing

    inserted = 0
    for _, row in df.fillna("").iterrows():
        root_code = str(row.get("root_code", "")).strip().lower()
        if not root_code:
            continue
        upsert_root(
            root_code=root_code,
            root_cn=str(row.get("root_cn", "")).strip(),
            category=str(row.get("category", "field")).strip(),
            status=str(row.get("status", "启用")).strip(),
            remark=str(row.get("remark", "")).strip(),
        )
        inserted += 1
    return inserted, []


def roots_template_df() -> Any:
    return pd.DataFrame(
        [
            {
                "root_code": "cust",
                "root_cn": "客户",
                "category": "field,table",
                "status": "启用",
                "remark": "标准词根",
            },
            {
                "root_code": "amt",
                "root_cn": "金额",
                "category": "field",
                "status": "启用",
                "remark": "金额类后缀",
            },
        ]
    )


def render_root_management() -> None:
    st.subheader("词根管理")

    admin_mode = is_admin()
    if admin_mode:
        st.success("你当前已登录管理员账号，可以维护词根。")

    if admin_mode:
        with st.expander("新增 / 修改词根", expanded=False), st.form("root_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                root_code = (
                    st.text_input("词根编码", placeholder="如 cust").strip().lower()
                )
                root_cn = st.text_input("中文含义", placeholder="如 客户").strip()
            with c2:
                category = st.text_input("适用范围", value="field,table").strip()
                remark = st.text_input("备注", placeholder="说明").strip()
            with c3:
                status = st.selectbox("状态", ["启用", "谨慎", "废弃"], index=0)

            submitted = st.form_submit_button("保存词根")
            if submitted:
                if not root_code or not root_cn:
                    st.error("词根编码和中文含义不能为空")
                else:
                    upsert_root(root_code, root_cn, category, status, remark)
                    st.success(f"词根 {root_code} 已保存")
                    st.rerun()

        with st.expander("Excel 导入", expanded=False):
            st.caption("模板列建议：root_code, root_cn, category, status, remark")
            template = roots_template_df().to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "下载CSV模板",
                template,
                file_name="term_roots_template.csv",
                mime="text/csv",
            )

            upload = st.file_uploader(
                "上传 Excel/CSV", type=["xlsx", "xls", "csv"], key="root_upload"
            )
            if upload is not None:
                try:
                    if upload.name.lower().endswith(".csv"):
                        df = pd.read_csv(upload)
                    else:
                        df = pd.read_excel(upload)

                    st.markdown("#### 导入预览")
                    st.dataframe(df.head(20), use_container_width=True, hide_index=True)

                    if st.button("执行导入", use_container_width=True):
                        count, missing = import_roots_from_excel(df)
                        if missing:
                            st.error(f"缺少必需列: {', '.join(missing)}")
                        else:
                            st.success(f"成功导入/更新 {count} 条词根")
                            st.rerun()
                except Exception as exc:
                    st.exception(exc)

    roots = load_roots()
    st.markdown("#### 当前词根库")
    st.dataframe(roots, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="数据命名治理助手", page_icon="🧭", layout="wide")
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
        st.session_state["username"] = None
        st.session_state["role"] = None

    st.title("🧭 数据命名治理助手")
    st.caption("词根管理")

    render_login_box()
    render_readonly_notice()

    st.sidebar.radio(
        "功能菜单",
        ["词根管理"],
    )

    render_root_management()


if __name__ == "__main__":
    main()
