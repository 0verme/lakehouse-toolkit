"""SQL 文件规则与资产登记提示。"""

from dataclasses import replace

from services.portal_link_builder import build_portal_link
from services.re_service import find_dot_strings, read_data_from_file

from core.asset_issue import create_audit_asset_issue
from core.lakehouse._sql_parser import (
    detect_created_functions,
    detect_created_views,
    detect_used_functions,
    split_schema_table,
)
from core.lakehouse.ddl_rule import (
    extract_create_table_objects,
    load_metadata_name_set,
    run_dws_ddl_rules,
)
from core.public_data import all_function_names, all_view_names


def _build_asset_table_review_issue(
    full_table_name, source_module, source_file, issue_desc
):
    schema_name, table_name = split_schema_table(full_table_name)
    issue = create_audit_asset_issue(
        issue_type="ASSET_TABLE_REVIEW",
        issue_title="资产表待核对",
        issue_desc=issue_desc,
        asset_type="table",
        source_module=source_module,
        source_file=source_file,
        severity="warning",
        suggestion="请核对该表是否已登记，必要时补充资产表和字段信息",
        portal_module="data-warehouse",
        action_label="去核对资产表",
        schema_name=schema_name,
        table_name=table_name or full_table_name,
    )
    return replace(issue, portal_url=build_portal_link(issue))


def collect_created_table_review_issues(
    dws_url, source_module="lakehouse", source_file=None
):
    data = read_data_from_file(dws_url)
    source_file = source_file or dws_url
    return [
        _build_asset_table_review_issue(
            item["table_name"],
            source_module,
            source_file,
            f"建表语句涉及资产表待核对：{item['table_name']}",
        )
        for item in extract_create_table_objects(data)
    ]


def _append_message(result: list[str], message: str):
    if message and message not in result:
        result.append(message)


def rule_dws(dws_url):
    data = read_data_from_file(dws_url)
    result = []
    warnings = ["存在 SQL 文件，请结合变更范围审核"]
    view_names = load_metadata_name_set(all_view_names())
    function_names = load_metadata_name_set(all_function_names())
    created_views = detect_created_views(data)
    created_functions = detect_created_functions(data)
    used_views = sorted(
        {item for item in find_dot_strings(data.upper()) if item.upper() in view_names}
    )
    used_functions = detect_used_functions(data, function_names)
    if created_views:
        _append_message(
            result, f"检测到创建视图，请重点审核: {','.join(created_views)}"
        )
    if created_functions:
        _append_message(
            result, f"检测到创建函数，请重点审核: {','.join(created_functions)}"
        )
    if used_views:
        _append_message(result, f"检测到使用视图，请重点审核: {','.join(used_views)}")
    if used_functions:
        _append_message(
            result, f"检测到使用函数，请重点审核: {','.join(used_functions)}"
        )
    if len(data.splitlines()) > 20000:
        _append_message(result, "SQL 行数过多，请拆分后审核")
    if "ALTER" in data.upper():
        _append_message(result, "存在 ALTER 命令，请重点检查")
    if "TO GROUP GROUP_VERSION1" in data.upper():
        _append_message(result, "建表脚本不应包含固定的 group 目标")
    if "DISTINCT" in data.upper():
        warnings.append("请确认 DISTINCT 是否确有必要")
    ddl_messages = run_dws_ddl_rules(data)
    for message in ddl_messages:
        _append_message(result, message)
    return (
        "\n".join(result) + ("\n" if result else ""),
        "\n".join(warnings) + "\n",
        len(result),
    )


def rule_hive(hive_url):
    data = read_data_from_file(hive_url)
    result = []
    warnings = ["存在 Hive SQL 文件，请结合变更范围审核"]
    if len(data.splitlines()) > 20000:
        result.append("SQL 行数过多，请拆分后审核")
    if "VARCHAR2" in data.upper():
        result.append("脚本不应使用 VARCHAR2 类型")
    if "ALTER" in data.upper():
        result.append("存在 ALTER 命令，请重点检查")
    return (
        "\n".join(result) + ("\n" if result else ""),
        "\n".join(warnings) + "\n",
        len(result),
    )
