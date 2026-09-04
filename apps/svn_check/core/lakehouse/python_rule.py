"""Python 数据处理脚本规则。"""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

from services.portal_link_builder import build_portal_link
from services.re_service import (
    detect_file_format,
    extract_tables,
    find_dot_strings,
    find_hardcoded_dates,
    read_data_from_file,
    safe_remove_prefix,
)

from core.asset_issue import create_audit_asset_issue
from core.lakehouse._sql_parser import (
    detect_created_functions,
    detect_created_views,
    detect_used_functions,
)
from core.lakehouse.ddl_rule import check_table_name_rule, run_dws_ddl_rules
from core.public_data import (
    all_function_names,
    all_sstb,
    all_tab_partitions,
    all_view_names,
)

STANDARD_SQL_OBJECTS = {
    "DATETIME",
    "DUAL",
    "AGE",
    "LAST_DAY",
    "DEMO_META.DATE_FUNCTIONS",
    "DEMO_META.STANDARD_FUNCTIONS",
    "DEMO_META.TABLE_FUNCTIONS",
}


def build_asset_table_review_issues(
    table_names, source_module, source_file, issue_desc_prefix="SQL中识别到资产表待核对"
):
    issues = []
    seen = set()
    for table_name in table_names or []:
        normalized = str(table_name).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        schema_name, simple_name = (
            normalized.split(".", 1) if "." in normalized else ("", normalized)
        )
        issue = create_audit_asset_issue(
            issue_type="ASSET_TABLE_REVIEW",
            issue_title="资产表待核对",
            issue_desc=f"{issue_desc_prefix}：{normalized}",
            asset_type="table",
            source_module=source_module,
            source_file=source_file,
            severity="warning",
            suggestion="请核对该表是否已登记，必要时补充资产表和字段信息",
            portal_module="data-warehouse",
            action_label="去核对资产表",
            schema_name=schema_name,
            table_name=simple_name,
        )
        issues.append(replace(issue, portal_url=build_portal_link(issue)))
    return issues


def get_program_table_name(py_url):
    folder = Path(py_url).parent.name
    if "." not in folder:
        return ""
    schema, table_name = folder.split(".", 1)
    return f"{schema.replace('DWS_', '')}.{table_name}"


def has_partition_rollback_step(text):
    upper_text = text.upper()
    return re.search(r"\bDEF\s+ROLLBACK_DEAL\s*\(", upper_text) is not None and any(
        keyword in upper_text
        for keyword in (
            "TRUNCATE PARTITION",
            "ADD PARTITION",
            "DROP PARTITION",
            "SPLIT PARTITION",
        )
    )


def rule_sbin(sbin_url):
    result = []
    for path in sbin_url:
        data = read_data_from_file(path)
        if path.endswith(".py") and "\t" in data:
            result.append(f"{path} 脚本含有 TAB 键，请替换成空格")
        if detect_file_format(path) == "DOS":
            result.append(f"{path} 编码是 DOS，不是 UNIX，请修改")
    return "\n".join(result) + ("\n" if result else ""), "", len(result)


def rule_config(config_names):
    result = [
        f"新增或修改配置文件，请确认部署环境参数：{Path(path).name}"
        for path in config_names
    ]
    return "\n".join(result) + ("\n" if result else ""), "", len(result)


def rule_recv_json(recv_lists):
    result = []
    for path in recv_lists:
        data = read_data_from_file(path)
        display_path = safe_remove_prefix(path)
        if "LOCAL_" in display_path.upper():
            continue
        if '"SQL" : ""' not in data.upper() and '"SQL":""' not in data.upper():
            result.append(f"{display_path} 是自定义接入配置，请重点检查环境参数")
    return "\n".join(result) + ("\n" if result else ""), "", len(result)


def rule_dwf(dwf_url):
    data = read_data_from_file(dwf_url).upper()
    result = []
    if "SOURCE_TABLE = ''" in data or "SOURCE_COLUMN = ''" in data:
        result.append(f"{dwf_url} 的来源映射为空")
    return "\n".join(result) + ("\n" if result else ""), "", len(result)


def rule_dwo(dwo_url):
    data = read_data_from_file(dwo_url).upper()
    result = []
    if "<VERSION>1.0</VERSION>" in data and "GENERIC DATA PROCESSING" in data:
        result.append(f"{dwo_url} 不应使用过期的处理模板版本")
    return "\n".join(result) + ("\n" if result else ""), "", len(result)


def rule_dws_py(dws_url):
    result = []
    warnings = []
    data = read_data_from_file(dws_url)
    view_names = {
        str(row[0]).strip().upper() for row in all_view_names() or [] if row and row[0]
    }
    function_names = {
        str(row[0]).strip().upper()
        for row in all_function_names() or []
        if row and row[0]
    }
    result_table_name = get_program_table_name(dws_url)
    table_name_message = check_table_name_rule(result_table_name)
    if table_name_message:
        result.append(table_name_message)

    created_views = detect_created_views(data)
    created_functions = detect_created_functions(data)
    used_functions = detect_used_functions(data, function_names)
    if created_views:
        result.append(f"检测到创建视图，请重点审核: {','.join(created_views)}")
    if created_functions:
        result.append(f"检测到创建函数，请重点审核: {','.join(created_functions)}")
    if "RECURSIVE" in data.upper():
        result.append("检测到 recursive 语法，请确认是否必须使用递归")
    if "FOR I IN" in data.upper():
        result.append("检测到脚本循环，请确认是否可以改为集合操作")
    if "CHARACTER VARYING(" in data.upper() or "VARCHAR2(" in data.upper():
        result.append("脚本中写死了字段长度，请结合目标模型审核")
    if "NVL(NVL(" in data.upper() or "COALESCE(COALESCE(" in data.upper():
        result.append("检测到嵌套空值函数，请考虑使用单层 COALESCE")
    if "DISTINCT" in data.upper():
        warnings.append("请审核 DISTINCT 是否确有必要")
    if "(+)" in data.upper():
        result.append("脚本使用了旧式连接语法 (+)，请修改")
    if "影响条数" not in data:
        result.append("模板缺少影响条数日志，例如 LOG.info('影响条数:' + str(rownum))")

    for message in run_dws_ddl_rules(data):
        if message not in result:
            result.append(message)

    content = data[1000:]
    hardcoded_dates = [f"'{item}'" for item in set(find_hardcoded_dates(content))]
    if hardcoded_dates:
        result.append(
            f"检测到写死日期，请甄别是否为必要条件: {' '.join(hardcoded_dates)}"
        )

    tables = extract_tables(content) + find_dot_strings(content)
    sql_tables = [
        item for item in set(tables) if item.upper() not in STANDARD_SQL_OBJECTS
    ]
    used_views = sorted(item for item in sql_tables if item.upper() in view_names)
    if used_views:
        result.append(f"检测到使用视图，请重点审核: {','.join(used_views)}")
    if used_functions:
        result.append(f"检测到使用函数，请重点审核: {','.join(used_functions)}")
    sql_tables = [
        item
        for item in sql_tables
        if item.upper() != result_table_name.upper()
        and item.upper() not in function_names
    ]

    schema_name, simple_name = (
        result_table_name.split(".", 1)
        if "." in result_table_name
        else ("", result_table_name)
    )
    partitions = all_tab_partitions(result_table_name) if result_table_name else []
    has_partition = bool(partitions and partitions[0] and partitions[0][0] > 0)
    if has_partition != has_partition_rollback_step(data):
        result.append(f"{result_table_name} 的分区操作与元数据状态不一致，请确认")
    for table_name in sql_tables:
        if "." not in table_name and table_name.upper() not in STANDARD_SQL_OBJECTS:
            result.append(f"表名 {table_name} 未带 schema，请补充")
    for row in all_sstb() or []:
        value = str(row[0] or "").strip()
        if value and value.upper() in data.upper():
            result.append(f"检测到可能误用的输出表 {value}")

    normalized = data.replace(" ", "").replace("\t", "").upper()
    if "=(SELECT" in normalized:
        result.append("存在等号子查询，请关注跑批效率与多行结果")
    if "IN(SELECT" in normalized:
        warnings.append("存在 IN 子查询，请关注跑批效率")
    return (
        "\n".join(result) + ("\n" if result else ""),
        "\n".join(warnings) + ("\n" if warnings else ""),
        len(result),
        sql_tables,
    )
