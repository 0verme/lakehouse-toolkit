import re
from dataclasses import replace

from services.portal_link_builder import build_portal_link

from core.asset_issue import create_audit_asset_issue
from core.lakehouse._sql_parser import (
    is_temp_table_statement,
    normalize_sql_identifier,
    normalize_sql_table_name,
    split_schema_table,
    split_top_level_commas,
)
from core.public_data import all_term_roots

DWS_TABLE_PREFIX_RULES = {
    "DWF": ("F_",),
    "DWM": ("M_",),
    "DWA": ("A_",),
    "DWP": ("P_",),
    "DWD": ("RMD_",),
}

DWS_TEMP_TABLE_PREFIXES = ("TMP_",)

COLUMN_COMMENT_REQUIRED_SCHEMAS = {"DWF", "DWM", "DWA", "DWP", "DWD"}
ROOT_CHECK_REQUIRED_SCHEMAS = {"DWM", "DWA", "DM"}


def load_metadata_name_set(rows):
    result = set()
    for row in rows:
        if not row or not row[0]:
            continue
        result.add(str(row[0]).strip().upper())
    return result


def extract_create_table_objects(sql_text):
    pattern = re.compile(
        r"CREATE\s+(?:GLOBAL\s+|LOCAL\s+|UNLOGGED\s+|TEMPORARY\s+|TEMP\s+)*TABLE\s+"
        r'(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_".]+)\s*\(',
        re.IGNORECASE | re.DOTALL,
    )
    objects = []
    for match in pattern.finditer(sql_text):
        table_name = normalize_sql_table_name(match.group(1))
        statement_start = sql_text.rfind(";", 0, match.start()) + 1
        statement_end = sql_text.find(";", match.start())
        if statement_end == -1:
            statement_end = len(sql_text)
        statement = sql_text[statement_start:statement_end]
        objects.append(
            {
                "table_name": table_name,
                "is_temp": is_temp_table_statement(statement.upper()),
            }
        )
    return objects


def extract_alter_table_targets(sql_text):
    pattern = re.compile(r'ALTER\s+TABLE\s+([A-Z0-9_".]+)', re.IGNORECASE)
    return sorted(
        {
            normalize_sql_table_name(match.group(1))
            for match in pattern.finditer(sql_text)
            if normalize_sql_table_name(match.group(1))
        }
    )


def extract_comment_on_column_map(sql_text):
    pattern = re.compile(
        r'COMMENT\s+ON\s+COLUMN\s+([A-Z0-9_".]+)\s+IS\s+\'((?:\'\'|[^\'])*)\'',
        re.IGNORECASE | re.DOTALL,
    )
    comment_map = {}
    for match in pattern.finditer(sql_text):
        full_name = normalize_sql_table_name(match.group(1))
        comment_text = match.group(2).replace("''", "'").strip()
        comment_map[full_name] = comment_text
    return comment_map


def extract_create_table_column_defs(sql_text):
    pattern = re.compile(
        r"CREATE\s+(?:GLOBAL\s+|LOCAL\s+|UNLOGGED\s+|TEMPORARY\s+|TEMP\s+)*TABLE\s+"
        r'(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_".]+)\s*\((.*?)\)\s*(?:WITH|DISTRIBUTE|PARTITION|TO\s+GROUP|;|$)',
        re.IGNORECASE | re.DOTALL,
    )
    table_columns = []
    for match in pattern.finditer(sql_text):
        table_name = normalize_sql_table_name(match.group(1))
        body = match.group(2)
        for column_def in split_top_level_commas(body):
            clean_def = column_def.strip()
            clean_upper = clean_def.upper()
            if not clean_def or clean_upper.startswith(
                (
                    "PRIMARY KEY",
                    "UNIQUE",
                    "KEY ",
                    "INDEX ",
                    "CONSTRAINT ",
                    "PARTITION ",
                    "DISTRIBUTE ",
                    "LIKE ",
                    "CHECK ",
                    "FOREIGN KEY",
                )
            ):
                continue
            column_match = re.match(r'"?([A-Z0-9_]+)"?\s+', clean_def, re.IGNORECASE)
            if not column_match:
                continue
            column_name = normalize_sql_identifier(column_match.group(1))
            inline_comment = ""
            inline_match = re.search(
                r"\bCOMMENT\s+\'((?:\'\'|[^\'])*)\'",
                clean_def,
                re.IGNORECASE | re.DOTALL,
            )
            if inline_match:
                inline_comment = inline_match.group(1).replace("''", "'").strip()
            table_columns.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "has_inline_comment": bool(inline_comment),
                    "inline_comment": inline_comment,
                    "source": "create_table",
                }
            )
    return table_columns


def extract_alter_table_add_columns(sql_text):
    from core.lakehouse._sql_parser import split_sql_statements

    statements = split_sql_statements(sql_text)
    result = []
    for statement in statements:
        statement_upper = statement.upper()
        if "ALTER TABLE" not in statement_upper or "ADD COLUMN" not in statement_upper:
            continue
        table_match = re.search(
            r'ALTER\s+TABLE\s+([A-Z0-9_".]+)', statement, re.IGNORECASE
        )
        if not table_match:
            continue
        table_name = normalize_sql_table_name(table_match.group(1))
        add_matches = re.finditer(
            r'ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?("?([A-Z0-9_]+)"?)\s+(.*?)(?=(?:,\s*ADD\s+COLUMN\b)|$)',
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        for add_match in add_matches:
            column_name = normalize_sql_identifier(add_match.group(2))
            column_tail = add_match.group(3).strip()
            inline_comment = ""
            inline_match = re.search(
                r"\bCOMMENT\s+\'((?:\'\'|[^\'])*)\'",
                column_tail,
                re.IGNORECASE | re.DOTALL,
            )
            if inline_match:
                inline_comment = inline_match.group(1).replace("''", "'").strip()
            result.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "has_inline_comment": bool(inline_comment),
                    "inline_comment": inline_comment,
                    "source": "alter_table_add_column",
                }
            )
    return result


def check_table_name_rule(full_table_name, is_temp=False):
    schema_name, table_name = split_schema_table(full_table_name)
    if is_temp or schema_name == "TMP":
        if not any(table_name.startswith(prefix) for prefix in DWS_TEMP_TABLE_PREFIXES):
            return f"临时表 {full_table_name} 命名不符合规范，应以 {'/'.join(DWS_TEMP_TABLE_PREFIXES)} 开头"
        return ""

    allowed_prefixes = DWS_TABLE_PREFIX_RULES.get(schema_name)
    if allowed_prefixes and not any(
        table_name.startswith(prefix) for prefix in allowed_prefixes
    ):
        return f"表 {full_table_name} 命名不符合规范，{schema_name} 层表名应以 {'/'.join(allowed_prefixes)} 开头"
    return ""


def check_column_comment_rule(column_item, comment_map):
    table_name = column_item["table_name"]
    schema_name, _ = split_schema_table(table_name)
    if schema_name not in COLUMN_COMMENT_REQUIRED_SCHEMAS:
        return ""

    full_column_name = f"{table_name}.{column_item['column_name']}"
    comment_text = column_item["inline_comment"] or comment_map.get(
        full_column_name, ""
    )
    if not comment_text:
        return f"字段 {full_column_name} 缺少注释，请在建表字段后补 COMMENT 或增加 COMMENT ON COLUMN"
    return ""


def strip_table_prefix(schema_name, table_name, is_temp=False):
    if is_temp or schema_name == "TMP":
        prefixes = DWS_TEMP_TABLE_PREFIXES
    else:
        prefixes = DWS_TABLE_PREFIX_RULES.get(schema_name, ())
    for prefix in sorted(prefixes, key=len, reverse=True):
        if table_name.startswith(prefix):
            return table_name[len(prefix) :]
    return table_name


def extract_root_tokens(name):
    return [
        token
        for token in normalize_sql_identifier(name).split("_")
        if token and not token.isdigit()
    ]


def check_table_root_rule(full_table_name, term_roots, is_temp=False):
    schema_name, table_name = split_schema_table(full_table_name)
    if schema_name not in ROOT_CHECK_REQUIRED_SCHEMAS:
        return ""
    pure_table_name = strip_table_prefix(schema_name, table_name, is_temp=is_temp)
    missing_roots = [
        token
        for token in extract_root_tokens(pure_table_name)
        if token not in term_roots
    ]
    if missing_roots:
        return f"建表表名 {full_table_name} 存在未维护词根: {','.join(missing_roots)}，规范命名或联系一审在词根平台加上"
    return ""


def check_column_root_rule(column_item, term_roots):
    schema_name, _ = split_schema_table(column_item["table_name"])
    if schema_name not in ROOT_CHECK_REQUIRED_SCHEMAS:
        return ""
    full_column_name = f"{column_item['table_name']}.{column_item['column_name']}"
    missing_roots = [
        token
        for token in extract_root_tokens(column_item["column_name"])
        if token not in term_roots
    ]
    if missing_roots:
        return f"字段 {full_column_name} 存在未维护词根: {','.join(missing_roots)}，规范命名或联系一审在词根平台加上"
    return ""


def _build_root_missing_issue(
    *,
    root_word,
    issue_desc,
    source_module,
    source_file,
    schema_name="",
    table_name="",
    field_name="",
):
    issue = create_audit_asset_issue(
        issue_type="ROOT_MISSING",
        issue_title="词根待维护",
        issue_desc=issue_desc,
        asset_type="root",
        source_module=source_module,
        source_file=source_file,
        severity="warning",
        suggestion="建议前往资产门户词根管理页核对并维护词根",
        portal_module="root-management",
        action_label="去维护词根",
        schema_name=schema_name,
        table_name=table_name,
        field_name=field_name,
        root_word=root_word,
    )
    return replace(issue, portal_url=build_portal_link(issue))


def collect_root_missing_issues(sql_text, source_module, source_file):
    issues = []
    created_tables = extract_create_table_objects(sql_text)
    column_items = extract_create_table_column_defs(
        sql_text
    ) + extract_alter_table_add_columns(sql_text)
    term_roots = load_metadata_name_set(all_term_roots())

    for item in created_tables:
        schema_name, table_name = split_schema_table(item["table_name"])
        if schema_name not in ROOT_CHECK_REQUIRED_SCHEMAS:
            continue
        pure_table_name = strip_table_prefix(
            schema_name, table_name, is_temp=item["is_temp"]
        )
        missing_roots = [
            token
            for token in extract_root_tokens(pure_table_name)
            if token not in term_roots
        ]
        for root_word in missing_roots:
            issues.append(
                _build_root_missing_issue(
                    root_word=root_word,
                    issue_desc=f"表名 {item['table_name']} 存在未维护词根：{root_word}",
                    source_module=source_module,
                    source_file=source_file,
                    schema_name=schema_name,
                    table_name=table_name,
                )
            )

    for column_item in column_items:
        schema_name, table_name = split_schema_table(column_item["table_name"])
        if schema_name not in ROOT_CHECK_REQUIRED_SCHEMAS:
            continue
        missing_roots = [
            token
            for token in extract_root_tokens(column_item["column_name"])
            if token not in term_roots
        ]
        full_column_name = f"{column_item['table_name']}.{column_item['column_name']}"
        for root_word in missing_roots:
            issues.append(
                _build_root_missing_issue(
                    root_word=root_word,
                    issue_desc=f"字段 {full_column_name} 存在未维护词根：{root_word}",
                    source_module=source_module,
                    source_file=source_file,
                    schema_name=schema_name,
                    table_name=table_name,
                    field_name=column_item["column_name"],
                )
            )

    return issues


def run_dws_ddl_rules(sql_text):
    warnings = []
    created_tables = extract_create_table_objects(sql_text)
    altered_tables = extract_alter_table_targets(sql_text)
    comment_map = extract_comment_on_column_map(sql_text)
    column_items = extract_create_table_column_defs(
        sql_text
    ) + extract_alter_table_add_columns(sql_text)
    term_roots = load_metadata_name_set(all_term_roots())

    for item in created_tables:
        message = check_table_name_rule(item["table_name"], is_temp=item["is_temp"])
        if message:
            warnings.append(message)
        root_message = check_table_root_rule(
            item["table_name"], term_roots, is_temp=item["is_temp"]
        )
        if root_message:
            warnings.append(root_message)

    for table_name in altered_tables:
        message = check_table_name_rule(table_name)
        if message:
            warnings.append(f"{message}（ALTER TABLE 对象）")

    for column_item in column_items:
        comment_message = check_column_comment_rule(column_item, comment_map)
        if comment_message:
            warnings.append(comment_message)
        root_message = check_column_root_rule(column_item, term_roots)
        if root_message:
            warnings.append(root_message)

    return warnings
