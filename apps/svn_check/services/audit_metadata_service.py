from __future__ import annotations

from services.db_profile import (
    get_active_audit_profile,
    is_gauss_jdbc_profile,
    is_postgres_profile,
)
from shared.config.metadata import table as metadata_table

VIEW_NAME_SQL = """
SELECT upper(table_schema) || '.' || upper(table_name)
FROM information_schema.views
WHERE upper(table_schema) NOT IN ('PG_CATALOG', 'INFORMATION_SCHEMA')
"""

FUNCTION_NAME_SQL = """
SELECT upper(n.nspname) || '.' || upper(p.proname)
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE upper(n.nspname) NOT IN ('PG_CATALOG', 'INFORMATION_SCHEMA')
"""

LOCAL_PG_VIEW_SQL = """
select table_name
from information_schema.views
where table_schema not in ('pg_catalog', 'information_schema')
"""

LOCAL_PG_FUNCTION_SQL = """
select proname
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname not in ('pg_catalog', 'information_schema')
"""


def _metadata_sql(template: str, key: str) -> str:
    return template.format(table=metadata_table(key))


def _get_gauss_profile_name(profile: dict) -> str:
    db_profile = str(profile.get("db_profile", "") or "").strip()
    if not db_profile:
        raise KeyError(
            f"gauss_jdbc profile missing db_profile: {profile.get('name', '')}"
        )
    return db_profile


def _normalize_single_column_rows(rows) -> list[tuple[str]]:
    result = []
    for row in rows or []:
        value = "" if not row else str(row[0]).strip().upper()
        if value:
            result.append((value,))
    return result


def _fetch(sql: str, profile: dict):
    if is_postgres_profile():
        from shared.db.postgres import fetch_all

        return fetch_all(sql, profile=profile)
    if is_gauss_jdbc_profile():
        from shared.db.gaussdb import select_sql_with_profile

        return select_sql_with_profile(_get_gauss_profile_name(profile), sql) or []
    raise ValueError(f"unsupported audit metadata backend: {profile.get('backend')}")


def list_term_roots() -> list[tuple[str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        "select root_code from {table} where root_code is not null",
        "term_roots",
    )
    return _normalize_single_column_rows(_fetch(sql, profile))


def list_view_names() -> list[tuple[str]]:
    profile = get_active_audit_profile()
    return _normalize_single_column_rows(
        _fetch(LOCAL_PG_VIEW_SQL if is_postgres_profile() else VIEW_NAME_SQL, profile)
    )


def list_function_names() -> list[tuple[str]]:
    profile = get_active_audit_profile()
    return _normalize_single_column_rows(
        _fetch(
            LOCAL_PG_FUNCTION_SQL if is_postgres_profile() else FUNCTION_NAME_SQL,
            profile,
        )
    )


def list_para_table_names() -> list[tuple[str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        "select table_name from {table}",
        "reference_tables",
    )
    return _normalize_single_column_rows(_fetch(sql, profile))


def list_recv_mapping_plans() -> list[tuple[str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        "select distinct plan_name from {table} where plan_name is not null",
        "receive_plans",
    )
    return _normalize_single_column_rows(_fetch(sql, profile))


def list_job_outfiles() -> list[tuple[str, str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        "select job_name, output_path from {table}",
        "job_outputs",
    )
    return [tuple(row[:2]) for row in (_fetch(sql, profile) or [])]


def list_result_table_sys_names() -> list[tuple[str, str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        """select table_name, source_system
           from {table}
           where table_name is not null""",
        "result_receipts",
    )
    return [tuple(row[:2]) for row in (_fetch(sql, profile) or [])]


def list_result_table_recv_details() -> list[tuple[str, str, str]]:
    profile = get_active_audit_profile()
    sql = _metadata_sql(
        """select table_name, receive_plan, source_system
           from {table}
           where table_name is not null""",
        "result_receipts",
    )
    return [tuple(row[:3]) for row in (_fetch(sql, profile) or [])]
