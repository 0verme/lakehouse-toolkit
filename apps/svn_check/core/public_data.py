from services.audit_metadata_service import (
    list_function_names,
    list_job_outfiles,
    list_para_table_names,
    list_recv_mapping_plans,
    list_result_table_recv_details,
    list_result_table_sys_names,
    list_term_roots,
    list_view_names,
)
from services.db_profile import is_postgres_profile
from services.db_service import select_sql

from shared.config.metadata import table as metadata_table


def _table(key: str) -> str:
    return metadata_table(
        key,
        {
            "plans": "plans",
            "jobs": "jobs",
            "programs": "programs",
            "roles": "roles",
            "reports": "reports",
            "partitions": "partitions",
            "job_outputs": "job_outputs",
        }[key],
    )


def _table_sql(template: str, key: str) -> str:
    return template.replace("__TABLE__", _table(key))


def all_real_seq():
    return select_sql(
        _table_sql(
            "select distinct sequence_name from __TABLE__ "
            "where upper(coalesce(realtime_flag, 'N')) in ('Y', 'TRUE', '1')",
            "jobs",
        )
    )


def all_plan():
    return select_sql(
        _table_sql(
            "select plan_name, dependency_text, description, owner, status, calendar "
            "from __TABLE__",
            "plans",
        )
    )


def get_job2():
    return select_sql(
        _table_sql(
            """
        select job_name,
               case
                   when cast(status as text) = '1' then '启用'
                   when cast(status as text) = '9' then '禁用'
                   else cast(status as text)
               end as status
        from __TABLE__
        """,
            "jobs",
        )
    )


def all_job_dependencies():
    return select_sql(
        _table_sql(
            "select job_name, dependency_text from __TABLE__",
            "jobs",
        )
    )


def all_job():
    # 保持审查页面原有的宽表接口，同时使用独立 demo model 的语义列。
    return select_sql(
        _table_sql(
            """select plan_name, sequence_name, job_name, description, program_name,
        domain, priority, option_01, option_02, calendar, option_03,
        option_04, option_05, option_06, option_07, option_08,
        option_09, option_10, option_11, option_12, option_13,
        option_14, option_15, status, option_16, event_text,
        option_17, dependency_text
        from __TABLE__""",
            "jobs",
        )
    )


def all_program():
    return select_sql(
        _table_sql(
            "select program_name, program_key, description, language, file_path, "
            "target_table, option_01, option_02, option_03, option_04 "
            "from __TABLE__",
            "programs",
        )
    )


def all_seq():
    return select_sql(
        _table_sql("select distinct sequence_name from __TABLE__", "jobs")
    )


def all_role():
    return [
        row[0]
        for row in select_sql(_table_sql("select role_name from __TABLE__", "roles"))
        or []
    ]


def all_fine():
    return [
        row[0]
        for row in select_sql(
            _table_sql("select report_name from __TABLE__", "reports")
        )
        or []
    ]


def all_seqjob():
    return select_sql(
        _table_sql(
            "select distinct sequence_name, job_name from __TABLE__",
            "jobs",
        )
    )


def all_planjob():
    return select_sql(
        _table_sql(
            "select distinct plan_name, job_name from __TABLE__",
            "jobs",
        )
    )


def all_planseq():
    return select_sql(
        _table_sql(
            "select distinct plan_name, sequence_name from __TABLE__",
            "jobs",
        )
    )


def all_job_outfile():
    return list_job_outfiles()


def all_sstb():
    return select_sql(_table_sql("select output_path from __TABLE__", "job_outputs"))


def all_para_table_lists():
    return list_para_table_names()


def all_disabled_result_tables():
    return select_sql(
        _table_sql(
            "select target_table from __TABLE__ "
            "where target_table is not null and upper(coalesce(status, '')) = 'DISABLED'",
            "programs",
        )
    )


def all_result_table_sys_names():
    return list_result_table_sys_names()


def all_result_table_recv_details():
    return list_result_table_recv_details()


def all_recv_mapping_plans():
    return list_recv_mapping_plans()


def all_view_names():
    return list_view_names()


def all_function_names():
    return list_function_names()


def all_term_roots():
    return list_term_roots()


def all_tab_partitions(tb_name):
    schema_name, table_name = tb_name.upper().split(".", 1)
    if is_postgres_profile():
        sql = """
        SELECT count(*)
        FROM pg_inherits child
        JOIN pg_class parent ON parent.oid = child.inhparent
        JOIN pg_namespace schema_info ON schema_info.oid = parent.relnamespace
        WHERE upper(schema_info.nspname) = %s
          AND upper(parent.relname) = %s
        """
        return select_sql(sql, params=(schema_name, table_name))

    sql = """
    SELECT count(*)
    FROM information_schema.tables
    WHERE upper(table_schema) = upper(?)
      AND upper(table_name) = upper(?)
    """
    return select_sql(sql, params=(schema_name, table_name))
