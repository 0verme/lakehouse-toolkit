import re
from pathlib import Path

VALID_SCHEMA_PREFIXES = {"DM", "DWA", "DWD", "DWE", "DWM", "DWP", "DWO", "DWF"}
TABLE_PATTERN = re.compile(r"\b(FROM|JOIN|USING)\s+([\w.]+)", re.IGNORECASE)
DOT_STRING_PATTERN = re.compile(r"\b\w+\.\w+\b")


def fbj_rule(text: str) -> str:
    match = re.search(r"DISTRIBUTE BY HASH\((.+)\)", text)
    if not match:
        return ""
    return match.group(1).replace(" ", "").upper()


def get_yilai(input_string: str):
    return [part[3:] for part in input_string.split("|") if part.startswith("33:")]


def is_chinese(char: str) -> bool:
    return "\u4e00" <= char <= "\u9fff"


def extract_tables(sql_query: str):
    matches = TABLE_PATTERN.findall(sql_query)
    return sorted(
        {
            match[1].strip().upper()
            for match in matches
            if match[1].strip() and not any(is_chinese(c) for c in match[1])
        }
    )


def find_dot_strings(sql: str):
    matches = DOT_STRING_PATTERN.findall(sql)
    result = []
    for item in matches:
        if any(is_chinese(c) for c in item):
            continue
        item = item.upper()
        if item.split(".")[0] in VALID_SCHEMA_PREFIXES:
            result.append(item)
    return result


def read_data_from_file(file_path):
    return Path(file_path).read_text(encoding="utf-8", errors="ignore")
