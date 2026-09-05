"""从 ``ProgramSource`` 构建保留程序内部步骤的 Physical 图。

本模块只记录程序实际可静态确认的 SQL 写入关系。它不做 target 判断、
Issue 检测、TMP collapse 或递归 lineage materialization。
"""

from __future__ import annotations

import ast
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from textwrap import dedent

from shared.lineage.domain import (
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramSource,
)
from shared.lineage.lineage_builder import normalize_table_name, strip_sql_comments

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
    "query",
    "read_sql",
    "read_sql_query",
    "run_query",
    "run_sql",
    "sql",
}
_SQL_ARGUMENT_KEYWORDS = {"command", "query", "sql", "statement"}


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
    """一个程序的 Physical 图及后续审计所需的事实。"""

    program_source: ProgramSource
    nodes: tuple[PhysicalNode, ...]
    edges: tuple[PhysicalEdge, ...]
    steps: tuple[SQLStep, ...]
    sinks: tuple[str, ...]
    expected_target: str | None

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
class _PythonBinding:
    name: str
    expression: ast.AST | None
    line_number: int
    column_number: int


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


def _is_sql_call(node: ast.Call) -> bool:
    function = node.func
    name = ""
    if isinstance(function, ast.Name):
        name = function.id
    elif isinstance(function, ast.Attribute):
        name = function.attr
    return name.lower() in _SQL_CALL_NAMES


def _sql_argument(call: ast.Call) -> ast.AST | None:
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg and keyword.arg.lower() in _SQL_ARGUMENT_KEYWORDS:
            return keyword.value
    return None


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


def _static_value(
    expression: ast.AST,
    position: tuple[int, int],
    bindings: Mapping[str, list[_PythonBinding]],
    resolving: set[tuple[str, int, int]],
) -> object:
    if isinstance(expression, ast.Constant):
        if isinstance(expression.value, (str, int, float, bool)):
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
            resolving.remove(binding_key)

    if isinstance(expression, ast.Call):
        function = expression.func
        function_name = ""
        if isinstance(function, ast.Name):
            function_name = function.id
        elif isinstance(function, ast.Attribute):
            function_name = function.attr
        if function_name.lower() in {"sql", "text"} and expression.args:
            return _static_value(expression.args[0], position, bindings, resolving)

    return _UNRESOLVED


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


def _extract_python_candidates(script_code: str) -> tuple[_SQLCandidate, ...]:
    python_code = dedent(script_code)
    try:
        tree = ast.parse(python_code)
    except (SyntaxError, ValueError, TypeError):
        if _looks_like_sql(script_code):
            return (_SQLCandidate(script_code, 1, 0),)
        return ()

    bindings = _record_bindings(tree)
    calls = sorted(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and _is_sql_call(node)
        ),
        key=_node_position,
    )
    candidates: list[_SQLCandidate] = []
    for call in calls:
        expression = _sql_argument(call)
        if expression is None:
            continue
        line_number, column_number = _node_position(call)
        text = _resolve_static_text(
            expression,
            (line_number, column_number),
            bindings,
        )
        if text is not None and _looks_like_sql(text):
            candidates.append(_SQLCandidate(text, line_number, column_number))
    return tuple(candidates)


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


def _parse_statement(
    statement: str,
    statement_index: int,
    line_number: int | None,
    column_number: int | None,
) -> SQLStep:
    comment_free = strip_sql_comments(statement)
    sanitized = _mask_sql_string_literals(comment_free)
    target_info = _classify_statement(sanitized)
    source_items = (
        _find_sources(sanitized, _cte_names(sanitized))
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
    return SQLStep(
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
    )


def extract_sql_steps(script_code: str) -> tuple[SQLStep, ...]:
    """从 raw SQL 或已知 Python SQL execution context 提取 SQL steps。"""

    if not isinstance(script_code, str) or not script_code.strip():
        return ()

    candidates = _extract_python_candidates(script_code)
    steps: list[SQLStep] = []
    statement_index = 0
    for candidate in candidates:
        comment_free = strip_sql_comments(candidate.text)
        for statement in split_sql_statements(comment_free):
            if not statement.strip():
                continue
            steps.append(
                _parse_statement(
                    statement,
                    statement_index,
                    candidate.line_number,
                    candidate.column_number,
                )
            )
            statement_index += 1
    return tuple(steps)


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
    if program_source.expected_target is None:
        return None
    normalized = normalize_table_name(program_source.expected_target)
    return normalized or None


def build_program_physical_dag(program_source: ProgramSource) -> ProgramPhysicalDAG:
    """将一个 ``ProgramSource`` 转为保留 TMP/self/cycle 的 Physical 图。"""

    if not isinstance(program_source, ProgramSource):
        raise TypeError("program_source must be a ProgramSource")

    steps = extract_sql_steps(program_source.script_code)
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
    )


class ProgramPhysicalDAGBuilder:
    """面向后续调用方的轻量 builder facade。"""

    def build(self, program_source: ProgramSource) -> ProgramPhysicalDAG:
        return build_program_physical_dag(program_source)

    def __call__(self, program_source: ProgramSource) -> ProgramPhysicalDAG:
        return self.build(program_source)


def build_physical_dag(program_source: ProgramSource) -> ProgramPhysicalDAG:
    """``build_program_physical_dag`` 的简短兼容入口。"""

    return build_program_physical_dag(program_source)


def extract_program_sql_steps(script_code: str) -> tuple[SQLStep, ...]:
    """``extract_sql_steps`` 的语义化兼容入口。"""

    return extract_sql_steps(script_code)


__all__ = [
    "ProgramPhysicalDAG",
    "ProgramPhysicalDAGBuilder",
    "ProgramSQLStep",
    "SQLStep",
    "build_physical_dag",
    "build_program_physical_dag",
    "extract_program_sql_steps",
    "extract_sql_steps",
]
