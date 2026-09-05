"""共享的、无外部依赖的 SQL 语句切分 helper。"""

from __future__ import annotations


def split_sql_statements(sql_text: str | None) -> list[str]:
    """按 SQL 顶层分号切分语句，同时保留引号中的分号。"""

    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    text = sql_text or ""
    index = 0
    while index < len(text):
        char = text[index]
        if char == "'" and not in_double_quote:
            if in_single_quote and index + 1 < len(text) and text[index + 1] == "'":
                current.extend((char, text[index + 1]))
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        index += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


__all__ = ["split_sql_statements"]
