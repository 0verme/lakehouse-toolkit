"""从 ``ProgramSource`` 构建保留程序内部步骤的 Physical 图。

本模块只记录程序实际可静态确认的 SQL 写入关系。它不做 target 判断、
Issue 检测、TMP collapse 或递归 lineage materialization。
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from string import Formatter
from textwrap import dedent

from shared.lineage.domain import (
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramSource,
)
from shared.lineage.lineage_builder import normalize_table_name, strip_sql_comments

from .parser_backend import ParserBackend, analyze_sql
from .sql_parser import (  # pyright: ignore[reportMissingImports]
    split_sql_statements,
)

_IDENTIFIER_PART = r'(?:`[^`]+`|"[^"]+"|\[[^\]]+\]|[A-Za-z_$#][\w$#]*)'
_QUALIFIED_IDENTIFIER = rf"{_IDENTIFIER_PART}(?:\s*\.\s*{_IDENTIFIER_PART})*"
_ASSET_NAME_RE = re.compile(r"[A-Z0-9_$#]+(?:\.[A-Z0-9_$#]+)*\Z")
_SQL_LEADING_RE = re.compile(
    r"^(?:WITH|SELECT|INSERT|CREATE|MERGE|UPDATE|DELETE|TRUNCATE|ALTER|DROP|"
    r"EXPLAIN|SET|BEGIN|DECLARE)\b",
    re.IGNORECASE,
)
_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)
_SOURCE_PATTERN = re.compile(
    rf"\b(?:FROM|JOIN|USING)\s+(?:ONLY\s+)?(?P<table>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_INSERT_TARGET_PATTERN = re.compile(
    rf"\bINSERT\s+(?P<mode>OVERWRITE|INTO)\s+"
    rf"(?:INTO\s+)?(?:LOCAL\s+)?(?:TABLE\s+)?"
    rf"(?:IF\s+NOT\s+EXISTS\s+)?(?P<target>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_CREATE_TABLE_PATTERN = re.compile(
    rf"\bCREATE\s+(?P<modifiers>"
    rf"(?:(?:OR\s+REPLACE|GLOBAL|LOCAL|UNLOGGED|TEMPORARY|TEMP)\s+)*"
    rf")TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?P<target>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_CREATE_VIEW_PATTERN = re.compile(
    rf"\bCREATE\s+(?P<modifiers>"
    rf"(?:(?:OR\s+REPLACE|GLOBAL|LOCAL|MATERIALIZED|TEMPORARY|TEMP)\s+)*"
    rf")VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?P<target>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_MERGE_TARGET_PATTERN = re.compile(
    rf"\bMERGE\s+INTO\s+(?P<target>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_UPDATE_TARGET_PATTERN = re.compile(
    rf"\bUPDATE\s+(?:ONLY\s+)?(?P<target>{_QUALIFIED_IDENTIFIER})",
    re.IGNORECASE,
)
_CTE_PATTERN = re.compile(
    rf"(?:\bWITH\b|,)\s*(?:RECURSIVE\s+)?(?P<name>{_IDENTIFIER_PART})"
    rf"(?:\s*\([^)]*\))?\s+AS\s+"
    rf"(?:MATERIALIZED\s+|NOT\s+MATERIALIZED\s+)?\(",
    re.IGNORECASE,
)
_IGNORED_RELATION_NAMES = {
    "AS",
    "BY",
    "CASE",
    "DATABASE",
    "DIRECTORY",
    "EXISTS",
    "IF",
    "JOIN",
    "LATERAL",
    "LOCAL",
    "NOT",
    "ON",
    "ONLY",
    "OVERWRITE",
    "PARTITION",
    "RECURSIVE",
    "SELECT",
    "SET",
    "TABLE",
    "TEMP",
    "TEMPORARY",
    "UNNEST",
    "USING",
    "VALUES",
    "VIEW",
    "WHERE",
    "WITH",
}
_SQL_CALL_NAMES = {
    "execute",
    "executemany",
    "executescript",
    "exec_sql",
    "execute_query",
    "execute_sql",
    "fetch_all",
    "query",
    "read_sql",
    "read_sql_query",
    "do",
    "run",
    "run_query",
    "run_sql",
    "run_sql_with_profile",
    "select_mysql_sql",
    "select_sql",
    "select_sql_with_profile",
    "sql",
}
_SQL_ARGUMENT_KEYWORDS = {"command", "query", "sql", "sql_str", "statement"}
_LEGACY_SQL_WRAPPER_NAMES = {"do", "run"}
_LEGACY_ASSIGNMENT_OPERATORS = {
    "=",
    "+=",
    "-=",
    "*=",
    "/=",
    "%=",
    "//=",
    "**=",
    "&=",
    "|=",
    "^=",
    "<<=",
    ">>=",
    "@=",
}


class SQLExtractionReason(str, Enum):
    """Safe parser-stage classification used by the coverage funnel."""

    CANDIDATE_FOUND = "CANDIDATE_FOUND"
    RAW_SQL = "RAW_SQL"
    EMPTY_SCRIPT = "EMPTY_SCRIPT"
    PYTHON_PARSE_FAILED = "PYTHON_PARSE_FAILED"
    PYTHON_PARSE_RECOVERED = "PYTHON_PARSE_RECOVERED"
    NO_SQL_CANDIDATE = "NO_SQL_CANDIDATE"
    SQL_CALL_NOT_RECOGNIZED = "SQL_CALL_NOT_RECOGNIZED"
    SQL_ARGUMENT_DYNAMIC = "SQL_ARGUMENT_DYNAMIC"
    SQL_ARGUMENT_MISSING = "SQL_ARGUMENT_MISSING"
    SQL_ARGUMENT_NOT_SQL = "SQL_ARGUMENT_NOT_SQL"
    SQL_RETURN_DYNAMIC = "SQL_RETURN_DYNAMIC"
    SQL_RETURN_NOT_SQL = "SQL_RETURN_NOT_SQL"


@dataclass(frozen=True, slots=True)
class SQLStep:
    """一个可静态确认的程序 SQL statement。

    ``target`` 与 ``sources`` 都是已复用 legacy normalizer 的名称；``raw_*``
    只保留 statement 中的标识符 token，方便解释边的来源，不保存整段代码。
    ``statement_index`` 从零开始，按程序中实际提取到的 SQL statement 排序。
    """

    statement_index: int
    statement_type: str
    target: str | None
    sources: tuple[str, ...]
    raw_target: str | None = None
    raw_sources: tuple[str, ...] = ()
    line_number: int | None = None
    column_number: int | None = None
    is_temporary: bool = False
    insert_mode: str | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)

    @property
    def statement_kind(self) -> str:
        """兼容以 ``kind`` 称呼 statement 类型的调用方。"""

        return self.statement_type

    @property
    def normalized_target(self) -> str | None:
        return self.target

    @property
    def normalized_sources(self) -> tuple[str, ...]:
        return self.sources


ProgramSQLStep = SQLStep


@dataclass(frozen=True, slots=True)
class ProgramPhysicalDAG:
    """一个程序的 Physical 图及后续审计所需的事实。

    ``sql_candidate_count`` 与 ``sql_extraction_reason`` 来自同一次 parser
    extraction，供 coverage funnel 使用，不保存候选 SQL 文本。
    """

    program_source: ProgramSource
    nodes: tuple[PhysicalNode, ...]
    edges: tuple[PhysicalEdge, ...]
    steps: tuple[SQLStep, ...]
    sinks: tuple[str, ...]
    expected_target: str | None
    sql_candidate_count: int = 0
    sql_extraction_reason: str = SQLExtractionReason.NO_SQL_CANDIDATE.value

    @property
    def node_map(self) -> dict[str, PhysicalNode]:
        """按标准化资产名返回节点索引；不会改变图中的原始顺序。"""

        return {node.node_key: node for node in self.nodes}

    @property
    def edge_pairs(self) -> frozenset[tuple[str, str]]:
        return frozenset((edge.source, edge.target) for edge in self.edges)


@dataclass(frozen=True, slots=True)
class _SQLCandidate:
    text: str
    line_number: int | None
    column_number: int | None


@dataclass(frozen=True, slots=True)
class _PythonCandidateExtraction:
    candidates: tuple[_SQLCandidate, ...]
    reason: SQLExtractionReason


@dataclass(frozen=True, slots=True)
class _PythonBinding:
    name: str
    expression: ast.AST | None
    line_number: int
    column_number: int


@dataclass(frozen=True, slots=True)
class _LegacyBinding:
    token_index: int
    text: str | None


@dataclass(frozen=True, slots=True)
class _RawAsset:
    raw_name: str
    normalized_name: str


@dataclass(frozen=True, slots=True)
class _StatementTarget:
    statement_type: str
    target: str | None
    raw_target: str | None
    is_temporary: bool = False
    insert_mode: str | None = None


def _node_position(node: ast.AST) -> tuple[int, int]:
    line_number = getattr(node, "lineno", 0)
    column_number = getattr(node, "col_offset", 0)
    return (
        line_number if isinstance(line_number, int) else 0,
        column_number if isinstance(column_number, int) else 0,
    )


def _call_name(call: ast.Call) -> str:
    function = call.func
    if isinstance(function, ast.Name):
        return function.id.lower()
    if isinstance(function, ast.Attribute):
        return function.attr.lower()
    return ""


def _is_sql_call(node: ast.Call) -> bool:
    return _call_name(node) in _SQL_CALL_NAMES


def _sql_argument_candidates(call: ast.Call) -> tuple[ast.AST, ...]:
    for keyword in call.keywords:
        if keyword.arg and keyword.arg.lower() in _SQL_ARGUMENT_KEYWORDS:
            return (keyword.value,)

    name = _call_name(call)
    if name in {"run_sql_with_profile", "select_sql_with_profile"}:
        positions = (1,)
    elif name in {"execute_sql", "fetch_all"}:
        # The repository's shared helper takes (profile, sql), while the
        # historical generic name also appears with SQL as its first argument.
        positions = (0, 1)
    else:
        positions = (0,)
    return tuple(call.args[index] for index in positions if index < len(call.args))


def _diagnostic_argument_candidates(call: ast.Call) -> tuple[ast.AST, ...]:
    for keyword in call.keywords:
        if keyword.arg and keyword.arg.lower() in _SQL_ARGUMENT_KEYWORDS:
            return (keyword.value,)
    return (call.args[0],) if call.args else ()


def _record_bindings(tree: ast.AST) -> dict[str, list[_PythonBinding]]:
    bindings: dict[str, list[_PythonBinding]] = {}
    for node in ast.walk(tree):
        expression: ast.AST | None | object = None
        names: list[str] = []
        if isinstance(node, ast.Assign):
            expression = node.value
            names = [
                target.id for target in node.targets if isinstance(target, ast.Name)
            ]
        elif (
            isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
        ) or (isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name)):
            expression = node.value
            names = [node.target.id]
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            # +=/overwrite 的静态结果不可靠，明确使之前的 binding 失效。
            expression = None
            names = [node.target.id]
        if not names:
            continue
        line_number, column_number = _node_position(node)
        for name in names:
            bindings.setdefault(name, []).append(
                _PythonBinding(
                    name=name,
                    expression=expression if isinstance(expression, ast.AST) else None,
                    line_number=line_number,
                    column_number=column_number,
                )
            )
    for items in bindings.values():
        items.sort(key=lambda item: (item.line_number, item.column_number))
    return bindings


def _latest_binding(
    name: str,
    position: tuple[int, int],
    bindings: Mapping[str, list[_PythonBinding]],
) -> _PythonBinding | None:
    candidates = [
        item
        for item in bindings.get(name, [])
        if (item.line_number, item.column_number) < position
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item.line_number, item.column_number))


_UNRESOLVED = object()
_FORMATTER = Formatter()
_FORMAT_FIELD_ROOT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_FORMAT_TYPES = (str, int, float, bool, type(None))


def _static_value(
    expression: ast.AST,
    position: tuple[int, int],
    bindings: Mapping[str, list[_PythonBinding]],
    resolving: set[tuple[str, int, int]],
) -> object:
    if isinstance(expression, ast.Constant):
        if expression.value is None or type(expression.value) in _SAFE_FORMAT_TYPES:
            return expression.value
        return _UNRESOLVED

    if isinstance(expression, ast.JoinedStr):
        parts: list[str] = []
        for value in expression.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if not isinstance(value, ast.FormattedValue) or value.conversion != -1:
                return _UNRESOLVED
            formatted = _static_value(value.value, position, bindings, resolving)
            if formatted is _UNRESOLVED:
                return _UNRESOLVED
            parts.append(str(formatted))
        return "".join(parts)

    if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
        left = _static_value(expression.left, position, bindings, resolving)
        right = _static_value(expression.right, position, bindings, resolving)
        if isinstance(left, str) and isinstance(right, str):
            return left + right
        return _UNRESOLVED

    if isinstance(expression, ast.Name):
        binding = _latest_binding(expression.id, position, bindings)
        if binding is None or binding.expression is None:
            return _UNRESOLVED
        binding_key = (binding.name, binding.line_number, binding.column_number)
        if binding_key in resolving:
            return _UNRESOLVED
        resolving.add(binding_key)
        try:
            return _static_value(
                binding.expression,
                (binding.line_number, binding.column_number),
                bindings,
                resolving,
            )
        finally:
            resolving.discard(binding_key)

    if isinstance(expression, ast.Call):
        function = expression.func
        function_name = ""
        if isinstance(function, ast.Name):
            function_name = function.id
        elif isinstance(function, ast.Attribute):
            function_name = function.attr
            if function_name.lower() == "format":
                template = _static_value(
                    function.value,
                    position,
                    bindings,
                    resolving,
                )
                if isinstance(template, str):
                    return _format_static_string(
                        template,
                        expression,
                        position,
                        bindings,
                        resolving,
                    )
                return _UNRESOLVED
        if function_name.lower() in {"sql", "text"} and expression.args:
            return _static_value(expression.args[0], position, bindings, resolving)

    return _UNRESOLVED


def _format_placeholder(field_name: str) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9_$#]+", "_", field_name).strip("_")
    return f"<SQL_DYNAMIC_{safe_name or 'VALUE'}>"


def _safe_format_value(
    value: object,
    conversion: str | None,
    format_spec: str | None,
) -> str | None:
    """只对内建 scalar 做 format，绝不触发用户对象的 magic method。"""

    if type(value) not in _SAFE_FORMAT_TYPES:
        return None
    format_spec = format_spec or ""
    if "{" in format_spec or "}" in format_spec:
        return None
    try:
        if conversion == "r":
            value = repr(value)
        elif conversion == "s":
            value = str(value)
        elif conversion == "a":
            value = ascii(value)
        elif conversion is not None:
            return None
        return format(value, format_spec)
    except (TypeError, ValueError):
        return None


def _format_static_string(
    template: str,
    call: ast.Call,
    position: tuple[int, int],
    bindings: Mapping[str, list[_PythonBinding]],
    resolving: set[tuple[str, int, int]],
) -> str | object:
    """恢复 ``str.format`` 的 SQL 文本，不执行被分析代码。"""

    positional_values: list[object] = []
    for argument in call.args:
        if isinstance(argument, ast.Starred):
            positional_values.append(_UNRESOLVED)
        else:
            positional_values.append(
                _static_value(argument, position, bindings, resolving)
            )

    keyword_values: dict[str, object] = {}
    for keyword in call.keywords:
        if keyword.arg is None:
            continue
        keyword_values[keyword.arg] = _static_value(
            keyword.value,
            position,
            bindings,
            resolving,
        )

    try:
        parsed_fields = _FORMATTER.parse(template)
    except (TypeError, ValueError):
        return _UNRESOLVED

    parts: list[str] = []
    automatic_index = 0
    for literal, field_name, format_spec, conversion in parsed_fields:
        parts.append(literal)
        if field_name is None:
            continue

        value: object = _UNRESOLVED
        if field_name == "":
            if automatic_index < len(positional_values):
                value = positional_values[automatic_index]
            automatic_index += 1
        elif field_name.isdecimal():
            try:
                index = int(field_name)
            except ValueError:
                index = -1
            if index >= 0 and index < len(positional_values):
                value = positional_values[index]
        elif _FORMAT_FIELD_ROOT_RE.fullmatch(field_name):
            value = keyword_values.get(field_name, _UNRESOLVED)

        rendered = _safe_format_value(value, conversion, format_spec)
        parts.append(
            rendered if rendered is not None else _format_placeholder(field_name)
        )
    return "".join(parts)


def _resolve_static_text(
    expression: ast.AST,
    position: tuple[int, int],
    bindings: Mapping[str, list[_PythonBinding]],
) -> str | None:
    value = _static_value(expression, position, bindings, set())
    return value if isinstance(value, str) and value.strip() else None


def _mask_sql_string_literals(sql_text: str) -> str:
    """遮盖单引号字符串，避免字符串文本中的 FROM/JOIN 被当成 SQL。"""

    chars = list(sql_text)
    index = 0
    while index < len(sql_text):
        if sql_text[index] != "'":
            index += 1
            continue
        chars[index] = " "
        index += 1
        while index < len(sql_text):
            char = sql_text[index]
            if char in "\r\n":
                index += 1
                continue
            chars[index] = " "
            if char == "'":
                if index + 1 < len(sql_text) and sql_text[index + 1] == "'":
                    chars[index + 1] = " "
                    index += 2
                    continue
                index += 1
                break
            if char == "\\" and index + 1 < len(sql_text):
                chars[index + 1] = " "
                index += 2
                continue
            index += 1
    return "".join(chars)


def _looks_like_sql(text: str) -> bool:
    without_comments = strip_sql_comments(text).lstrip()
    return bool(_SQL_LEADING_RE.match(without_comments))


def _legacy_triple_quoted_text(token: tokenize.TokenInfo) -> str | None:
    if token.type != tokenize.STRING:
        return None

    token_text = token.string
    prefix = ""
    quote = ""
    if token_text.startswith(('"""', "'''")):
        quote = token_text[:3]
    elif len(token_text) >= 4 and token_text[0].lower() in {"r", "u"}:
        prefix = token_text[0]
        if token_text[1:].startswith(('"""', "'''")):
            quote = token_text[1:4]
    if not quote or prefix.lower() not in {"", "r", "u"}:
        return None
    if not token_text.endswith(quote):
        return None
    return token_text[len(prefix) + len(quote) : -len(quote)]


def _legacy_statement_boundary(
    tokens: tuple[tokenize.TokenInfo, ...],
    index: int,
) -> bool:
    while index < len(tokens) and tokens[index].type == tokenize.COMMENT:
        index += 1
    if index >= len(tokens):
        return True
    token = tokens[index]
    return (
        token.type
        in {
            tokenize.DEDENT,
            tokenize.ENDMARKER,
            tokenize.NEWLINE,
        }
        or token.string == ";"
    )


def _recover_legacy_sql_candidates(script_code: str) -> tuple[_SQLCandidate, ...]:
    """从失败的 Python source 中恢复极小范围的静态 ``do/run`` literal。

    ``tokenize`` 只读取 token，不解码 Python string，也不执行 source，因此可以
    观察包含 malformed escape 的完整 triple-quoted token。恢复边界刻意限制为
    单名直接赋值、独立 triple-quoted literal，以及紧随其后的 ``do(name)`` /
    ``run(name)`` 调用；任何重复赋值、动态表达式或不完整 token stream 都拒绝。
    """

    try:
        tokens = tuple(tokenize.generate_tokens(io.StringIO(script_code).readline))
    except (IndentationError, SyntaxError, tokenize.TokenError):
        return ()
    if any(token.type == tokenize.ERRORTOKEN for token in tokens):
        return ()

    bindings: dict[str, list[_LegacyBinding]] = {}
    for index, token in enumerate(tokens[:-2]):
        if token.type != tokenize.NAME:
            continue
        operator = tokens[index + 1]
        if (
            operator.type != tokenize.OP
            or operator.string not in _LEGACY_ASSIGNMENT_OPERATORS
        ):
            continue
        literal = None
        literal_token = tokens[index + 2]
        if literal_token.type == tokenize.STRING and _legacy_statement_boundary(
            tokens, index + 3
        ):
            literal = _legacy_triple_quoted_text(literal_token)
        bindings.setdefault(token.string, []).append(_LegacyBinding(index, literal))

    if not bindings:
        return ()

    candidates: list[_SQLCandidate] = []
    for index, token in enumerate(tokens[:-4]):
        if token.type != tokenize.NAME:
            continue
        if token.string.lower() not in _LEGACY_SQL_WRAPPER_NAMES:
            continue
        if (
            tokens[index + 1].string != "("
            or tokens[index + 2].type != tokenize.NAME
            or tokens[index + 3].string != ")"
            or not _legacy_statement_boundary(tokens, index + 4)
        ):
            continue

        previous = tokens[index - 1] if index else None
        if previous is not None and previous.type == tokenize.NAME:
            if previous.string.lower() in {"class", "def"}:
                continue
        if previous is not None and previous.string == ".":
            if index < 2 or tokens[index - 2].type != tokenize.NAME:
                continue

        binding = bindings.get(tokens[index + 2].string, ())
        if len(binding) != 1:
            continue
        literal = binding[0]
        if literal.token_index >= index or literal.text is None:
            continue
        if not _looks_like_sql(literal.text):
            continue
        call_start = (
            tokens[index - 2]
            if previous is not None and previous.string == "."
            else token
        )
        candidates.append(
            _SQLCandidate(literal.text, call_start.start[0], call_start.start[1])
        )
    return tuple(candidates)


def _extract_python_candidates_with_reason(
    script_code: str,
) -> _PythonCandidateExtraction:
    if not script_code.strip():
        return _PythonCandidateExtraction((), SQLExtractionReason.EMPTY_SCRIPT)

    python_code = dedent(script_code)
    try:
        tree = ast.parse(python_code)
    except (SyntaxError, ValueError, TypeError):
        recovered_candidates = _recover_legacy_sql_candidates(python_code)
        if recovered_candidates:
            return _PythonCandidateExtraction(
                recovered_candidates,
                SQLExtractionReason.PYTHON_PARSE_RECOVERED,
            )
        if _looks_like_sql(script_code):
            return _PythonCandidateExtraction(
                (_SQLCandidate(script_code, 1, 0),),
                SQLExtractionReason.RAW_SQL,
            )
        return _PythonCandidateExtraction((), SQLExtractionReason.PYTHON_PARSE_FAILED)

    bindings = _record_bindings(tree)
    calls = sorted(
        (node for node in ast.walk(tree) if isinstance(node, ast.Call)),
        key=_node_position,
    )
    candidates: list[_SQLCandidate] = []
    dynamic_argument = False
    missing_argument = False
    non_sql_argument = False
    unrecognized_sql_call = False
    recognized_sql_call = False
    return_dynamic = False
    return_not_sql = False
    for call in calls:
        line_number, column_number = _node_position(call)
        if _is_sql_call(call):
            recognized_sql_call = True
            expressions = _sql_argument_candidates(call)
            if not expressions:
                missing_argument = True
                continue
            resolved_candidate = False
            unresolved = False
            for expression in expressions:
                value = _static_value(
                    expression,
                    (line_number, column_number),
                    bindings,
                    set(),
                )
                if value is _UNRESOLVED:
                    unresolved = True
                    continue
                if not isinstance(value, str) or not value.strip():
                    non_sql_argument = True
                    continue
                if _looks_like_sql(value):
                    candidates.append(_SQLCandidate(value, line_number, column_number))
                    resolved_candidate = True
                    break
                non_sql_argument = True
            if not resolved_candidate and unresolved:
                dynamic_argument = True
            continue

        for expression in _diagnostic_argument_candidates(call):
            text = _resolve_static_text(
                expression,
                (line_number, column_number),
                bindings,
            )
            if text is not None and _looks_like_sql(text):
                # This is diagnostic-only: unknown wrappers are never parsed.
                unrecognized_sql_call = True
                break

    if not recognized_sql_call and not unrecognized_sql_call:
        return_candidates: list[_SQLCandidate] = []
        return_nodes = sorted(
            (item for item in ast.walk(tree) if isinstance(item, ast.Return)),
            key=_node_position,
        )
        if len(return_nodes) == 1:
            node = return_nodes[0]
            if node.value is None:
                return_not_sql = True
            else:
                line_number, column_number = _node_position(node)
                value = _static_value(
                    node.value,
                    (line_number, column_number),
                    bindings,
                    set(),
                )
                if value is _UNRESOLVED:
                    return_dynamic = True
                elif isinstance(value, str) and value.strip():
                    if _looks_like_sql(value):
                        return_candidates.append(
                            _SQLCandidate(value, line_number, column_number)
                        )
                    else:
                        return_not_sql = True
                else:
                    return_not_sql = True
        elif return_nodes:
            # Mutually exclusive branches cannot be selected statically.
            return_dynamic = True
        if return_candidates:
            candidates.extend(return_candidates)

    if candidates:
        reason = SQLExtractionReason.CANDIDATE_FOUND
    elif unrecognized_sql_call:
        reason = SQLExtractionReason.SQL_CALL_NOT_RECOGNIZED
    elif dynamic_argument:
        reason = SQLExtractionReason.SQL_ARGUMENT_DYNAMIC
    elif missing_argument:
        reason = SQLExtractionReason.SQL_ARGUMENT_MISSING
    elif non_sql_argument:
        reason = SQLExtractionReason.SQL_ARGUMENT_NOT_SQL
    elif return_dynamic:
        reason = SQLExtractionReason.SQL_RETURN_DYNAMIC
    elif return_not_sql:
        reason = SQLExtractionReason.SQL_RETURN_NOT_SQL
    else:
        reason = SQLExtractionReason.NO_SQL_CANDIDATE
    return _PythonCandidateExtraction(tuple(candidates), reason)


def _extract_python_candidates(script_code: str) -> tuple[_SQLCandidate, ...]:
    """Compatibility helper returning candidates without parser diagnostics."""

    return _extract_python_candidates_with_reason(script_code).candidates


def _normalize_asset(raw_name: str | None) -> str | None:
    normalized = normalize_table_name(raw_name or "")
    if not normalized or not _ASSET_NAME_RE.fullmatch(normalized):
        return None
    if normalized in _IGNORED_RELATION_NAMES:
        return None
    return normalized


def _cte_names(sanitized_sql: str) -> set[str]:
    result: set[str] = set()
    for match in _CTE_PATTERN.finditer(sanitized_sql):
        name = _normalize_asset(match.group("name"))
        if name:
            result.add(name)
    return result


def _find_sources(
    sanitized_sql: str,
    cte_names: set[str],
) -> tuple[_RawAsset, ...]:
    sources: list[_RawAsset] = []
    seen: set[str] = set()
    for match in _SOURCE_PATTERN.finditer(sanitized_sql):
        following_text = sanitized_sql[match.end() :].lstrip()
        if following_text.startswith("("):
            continue
        normalized = _normalize_asset(match.group("table"))
        if not normalized or normalized in cte_names or normalized in seen:
            continue
        seen.add(normalized)
        sources.append(_RawAsset(match.group("table"), normalized))
    return tuple(sources)


def _matched_target(
    statement_type: str,
    match: re.Match[str] | None,
    *,
    is_temporary: bool = False,
    insert_mode: str | None = None,
) -> _StatementTarget:
    if match is None:
        return _StatementTarget(statement_type, None, None, is_temporary, insert_mode)
    raw_target = match.group("target")
    target = _normalize_asset(raw_target)
    return _StatementTarget(
        statement_type,
        target,
        raw_target if target else None,
        is_temporary,
        insert_mode,
    )


def _classify_statement(sanitized_sql: str) -> _StatementTarget:
    insert_match = _INSERT_TARGET_PATTERN.search(sanitized_sql)
    if insert_match:
        mode = insert_match.group("mode").upper()
        return _matched_target(
            "insert",
            insert_match,
            insert_mode=mode.lower(),
        )

    create_table_match = _CREATE_TABLE_PATTERN.search(sanitized_sql)
    if create_table_match:
        modifiers = create_table_match.group("modifiers").upper()
        return _matched_target(
            "create_table",
            create_table_match,
            is_temporary=("TEMP" in modifiers or "TEMPORARY" in modifiers),
        )

    create_view_match = _CREATE_VIEW_PATTERN.search(sanitized_sql)
    if create_view_match:
        modifiers = create_view_match.group("modifiers").upper()
        return _matched_target(
            "create_view",
            create_view_match,
            is_temporary=("TEMP" in modifiers or "TEMPORARY" in modifiers),
        )

    merge_match = _MERGE_TARGET_PATTERN.search(sanitized_sql)
    if merge_match:
        return _matched_target("merge", merge_match)

    update_match = _UPDATE_TARGET_PATTERN.search(sanitized_sql)
    if update_match:
        return _matched_target("update", update_match)

    if _SELECT_RE.search(sanitized_sql):
        return _StatementTarget("select", None, None)
    return _StatementTarget("unknown", None, None)


def _parse_statement_with_ctes(
    statement: str,
    statement_index: int,
    line_number: int | None,
    column_number: int | None,
) -> tuple[SQLStep, tuple[str, ...]]:
    comment_free = strip_sql_comments(statement)
    sanitized = _mask_sql_string_literals(comment_free)
    cte_names = _cte_names(sanitized)
    target_info = _classify_statement(sanitized)
    source_items = (
        _find_sources(sanitized, cte_names)
        if target_info.statement_type
        in {"insert", "merge", "create_table", "create_view", "update", "select"}
        else ()
    )
    sources = tuple(item.normalized_name for item in source_items)
    raw_sources = tuple(item.raw_name for item in source_items)
    evidence: dict[str, object] = {
        "statement_index": statement_index,
        "statement_type": target_info.statement_type,
        "raw_target": target_info.raw_target,
        "raw_sources": raw_sources,
    }
    if line_number is not None:
        evidence["line_number"] = line_number
    if column_number is not None:
        evidence["column_number"] = column_number
    return (
        SQLStep(
            statement_index=statement_index,
            statement_type=target_info.statement_type,
            target=target_info.target,
            sources=sources,
            raw_target=target_info.raw_target,
            raw_sources=raw_sources,
            line_number=line_number,
            column_number=column_number,
            is_temporary=target_info.is_temporary,
            insert_mode=target_info.insert_mode,
            evidence=evidence,
        ),
        tuple(sorted(cte_names)),
    )


def _parse_statement(
    statement: str,
    statement_index: int,
    line_number: int | None,
    column_number: int | None,
) -> SQLStep:
    """兼容内部调用方，只返回原有 SQLStep。"""

    step, _ = _parse_statement_with_ctes(
        statement,
        statement_index,
        line_number,
        column_number,
    )
    return step


def _parse_sql_candidates_with_ctes(
    candidates: Iterable[_SQLCandidate],
) -> tuple[tuple[SQLStep, ...], tuple[tuple[str, ...], ...]]:
    steps: list[SQLStep] = []
    ctes: list[tuple[str, ...]] = []
    statement_index = 0
    for candidate in candidates:
        comment_free = strip_sql_comments(candidate.text)
        for statement in split_sql_statements(comment_free):
            if not statement.strip():
                continue
            step, statement_ctes = _parse_statement_with_ctes(
                statement,
                statement_index,
                candidate.line_number,
                candidate.column_number,
            )
            steps.append(step)
            ctes.append(statement_ctes)
            statement_index += 1
    return tuple(steps), tuple(ctes)


def _parse_sql_candidates(
    candidates: Iterable[_SQLCandidate],
) -> tuple[SQLStep, ...]:
    steps, _ = _parse_sql_candidates_with_ctes(candidates)
    return steps


def extract_sql_steps(
    script_code: str,
    *,
    backend: ParserBackend | None = None,
) -> tuple[SQLStep, ...]:
    """从 raw SQL 或已知 Python SQL execution context 提取 SQL steps。"""

    if not isinstance(script_code, str) or not script_code.strip():
        return ()
    return analyze_sql(script_code, backend=backend).steps


def _merge_edge_evidence(
    existing: PhysicalEdge,
    occurrence: dict[str, object],
) -> PhysicalEdge:
    evidence = dict(existing.evidence) if isinstance(existing.evidence, Mapping) else {}
    raw_occurrences = evidence.get("occurrences", ())
    occurrences = (
        list(raw_occurrences) if isinstance(raw_occurrences, (list, tuple)) else []
    )
    occurrences.append(occurrence)
    evidence["occurrences"] = occurrences
    raw_statement_indices = evidence.get("statement_indices", ())
    statement_indices = (
        list(raw_statement_indices)
        if isinstance(raw_statement_indices, (list, tuple))
        else []
    )
    statement_indices.append(occurrence["statement_index"])
    evidence["statement_indices"] = statement_indices
    return PhysicalEdge(
        source=existing.source,
        target=existing.target,
        evidence_type=existing.evidence_type,
        evidence=evidence,
    )


def _edge_occurrence(
    step: SQLStep,
    source: str,
    raw_source: str | None,
) -> dict[str, object]:
    occurrence: dict[str, object] = {
        "statement_index": step.statement_index,
        "statement_type": step.statement_type,
        "raw_source": raw_source,
        "raw_target": step.raw_target,
        "normalized_source": source,
        "normalized_target": step.target,
    }
    if step.line_number is not None:
        occurrence["line_number"] = step.line_number
    if step.column_number is not None:
        occurrence["column_number"] = step.column_number
    if step.insert_mode is not None:
        occurrence["insert_mode"] = step.insert_mode
    return occurrence


def _normalized_expected_target(program_source: ProgramSource) -> str | None:
    """只消费 explicit/provider 或 canonical program-name target authority。"""

    resolved_target = program_source.resolved_target
    if resolved_target is None:
        return None
    normalized = normalize_table_name(resolved_target)
    return normalized or None


def build_program_physical_dag(
    program_source: ProgramSource,
    *,
    backend: ParserBackend | None = None,
) -> ProgramPhysicalDAG:
    """将一个 ``ProgramSource`` 转为保留 TMP/self/cycle 的 Physical 图。"""

    if not isinstance(program_source, ProgramSource):
        raise TypeError("program_source must be a ProgramSource")

    analysis = analyze_sql(program_source.script_code, backend=backend)
    steps = analysis.steps
    nodes: dict[str, PhysicalNode] = {}
    edges: dict[tuple[str, str], PhysicalEdge] = {}
    written_targets: list[str] = []

    def add_node(asset_name: str, *, temporary: bool = False) -> None:
        current = nodes.get(asset_name)
        if current is None:
            nodes[asset_name] = PhysicalNode(
                node_key=asset_name,
                asset_name=asset_name,
                kind=(PhysicalNodeKind.TEMPORARY_ASSET if temporary else None),
            )
            return
        if temporary and not current.is_temporary:
            nodes[asset_name] = PhysicalNode(
                node_key=current.node_key,
                asset_name=current.asset_name,
                kind=PhysicalNodeKind.TEMPORARY_ASSET,
            )

    for step in steps:
        for source in step.sources:
            add_node(source)
        if step.target is None:
            continue

        add_node(step.target, temporary=step.is_temporary)
        if step.target not in written_targets:
            written_targets.append(step.target)
        for index, source in enumerate(step.sources):
            raw_source = (
                step.raw_sources[index] if index < len(step.raw_sources) else None
            )
            key = (source, step.target)
            occurrence = _edge_occurrence(step, source, raw_source)
            existing = edges.get(key)
            if existing is None:
                edges[key] = PhysicalEdge(
                    source=source,
                    target=step.target,
                    evidence_type="program_sql_step",
                    evidence={
                        **occurrence,
                        "occurrences": [occurrence],
                        "statement_indices": [step.statement_index],
                    },
                )
            else:
                edges[key] = _merge_edge_evidence(existing, occurrence)

    outgoing_sources = {edge.source for edge in edges.values()}
    sinks = tuple(
        target for target in written_targets if target not in outgoing_sources
    )
    expected_target = _normalized_expected_target(program_source)
    return ProgramPhysicalDAG(
        program_source=program_source,
        nodes=tuple(nodes.values()),
        edges=tuple(edges.values()),
        steps=steps,
        sinks=sinks,
        expected_target=expected_target,
        sql_candidate_count=analysis.candidate_count,
        sql_extraction_reason=analysis.extraction_reason,
    )


class ProgramPhysicalDAGBuilder:
    """面向后续调用方的轻量 builder facade。"""

    def __init__(self, backend: ParserBackend | None = None) -> None:
        self.backend = backend

    def build(self, program_source: ProgramSource) -> ProgramPhysicalDAG:
        return build_program_physical_dag(program_source, backend=self.backend)

    def __call__(self, program_source: ProgramSource) -> ProgramPhysicalDAG:
        return self.build(program_source)


def build_physical_dag(
    program_source: ProgramSource,
    *,
    backend: ParserBackend | None = None,
) -> ProgramPhysicalDAG:
    """``build_program_physical_dag`` 的简短兼容入口。"""

    return build_program_physical_dag(program_source, backend=backend)


def extract_program_sql_steps(
    script_code: str,
    *,
    backend: ParserBackend | None = None,
) -> tuple[SQLStep, ...]:
    """``extract_sql_steps`` 的语义化兼容入口。"""

    return extract_sql_steps(script_code, backend=backend)


__all__ = [
    "ProgramPhysicalDAG",
    "SQLExtractionReason",
    "ProgramPhysicalDAGBuilder",
    "ProgramSQLStep",
    "SQLStep",
    "build_physical_dag",
    "build_program_physical_dag",
    "extract_program_sql_steps",
    "extract_sql_steps",
]
