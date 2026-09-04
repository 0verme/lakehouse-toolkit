import re


def split_sql_statements(sql_text):
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'" and not in_double_quote:
            if in_single_quote and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                current.append(ch)
                current.append(sql_text[i + 1])
                i += 2
                continue
            in_single_quote = not in_single_quote
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def split_top_level_commas(text):
    parts = []
    current = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_double_quote:
            if in_single_quote and i + 1 < len(text) and text[i + 1] == "'":
                current.append(ch)
                current.append(text[i + 1])
                i += 2
                continue
            in_single_quote = not in_single_quote
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif not in_single_quote and not in_double_quote:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
                i += 1
                continue
        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def normalize_sql_identifier(name):
    if not name:
        return ""
    return str(name).strip().strip('"').upper()


def normalize_sql_table_name(name):
    table_name = normalize_sql_identifier(name)
    table_name = re.sub(r"\s+", "", table_name)
    return table_name.rstrip("(")


def split_schema_table(full_name):
    normalized = normalize_sql_table_name(full_name)
    if "." not in normalized:
        return "", normalized
    return normalized.split(".", 1)


def is_temp_table_statement(statement_upper):
    return (
        "CREATE TEMP TABLE" in statement_upper
        or "CREATE TEMPORARY TABLE" in statement_upper
        or "CREATE GLOBAL TEMP TABLE" in statement_upper
        or "CREATE LOCAL TEMP TABLE" in statement_upper
    )


def detect_created_views(text):
    return sorted(
        set(
            re.findall(
                r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([A-Z0-9_]+\.[A-Z0-9_]+|[A-Z0-9_]+)",
                text.upper(),
            )
        )
    )


def detect_created_functions(text):
    return sorted(
        set(
            re.findall(
                r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([A-Z0-9_]+\.[A-Z0-9_]+|[A-Z0-9_]+)",
                text.upper(),
            )
        )
    )


def detect_used_functions(text, function_names):
    upper_text = text.upper()
    hits = []
    for function_name in function_names:
        if re.search(rf"(?<![A-Z0-9_]){re.escape(function_name)}\s*\(", upper_text):
            hits.append(function_name)
    return sorted(set(hits))
