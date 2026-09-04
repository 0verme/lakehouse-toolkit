from __future__ import annotations

from services.db_profile import get_active_audit_profile, is_postgres_profile


def select_sql(
    sql: str, profile: str = "demo_local", params: tuple | list | None = None
):
    """按公开 demo 配置查询；不会在未配置凭据时回退到内部数据库。"""
    if profile in ("demo_local", "local_pg") and is_postgres_profile():
        from shared.db.postgres import fetch_all

        return fetch_all(sql, params=params, profile=get_active_audit_profile())

    from shared.db.gaussdb import select_sql_with_profile

    if params:
        raise ValueError("JDBC 查询参数请在本地适配器中显式实现")
    return select_sql_with_profile(profile, sql)
