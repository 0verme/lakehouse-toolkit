"""最小 SQL parser backend contract。

这个模块只定义 parser 与现有 lineage domain 之间的边界。production 的
legacy parser 仍由 ``physical_dag`` 实现；这里不引入第三方 SQL parser，也不
负责 Physical DAG、Audit 或 Materialization。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .physical_dag import SQLStep

LEGACY_PARSER_BACKEND = "legacy"
LEGACY_PARSER_BACKEND_VERSION = "legacy-parser-v1"


class SqlParseStatus(str, Enum):
    """Parser 对本次输入的最小结果分类。"""

    SUCCESS = "success"
    UNRESOLVED = "unresolved"
    FAILED = "failed"


class SqlParseConfidence(str, Enum):
    """不把保守恢复结果伪装成和静态成功相同的确定性。"""

    HIGH = "high"
    CONSERVATIVE = "conservative"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class SqlAnalysis:
    """一个 backend 对单个 script 的确定性分析结果。

    ``steps`` 继续使用现有 ``SQLStep``，因此 downstream 不需要认识具体
    backend。``ctes`` 与 ``steps`` 一一对应；CTE 只作为 parser evidence，
    不会自动变成 Physical 节点或边。
    """

    steps: tuple[SQLStep, ...]
    ctes: tuple[tuple[str, ...], ...]
    candidate_count: int
    parse_status: SqlParseStatus
    extraction_reason: str
    evidence: Mapping[str, object]
    confidence: SqlParseConfidence
    backend: str
    backend_version: str

    def __post_init__(self) -> None:
        steps = tuple(self.steps)
        ctes = tuple(tuple(group) for group in self.ctes)
        if len(ctes) != len(steps):
            raise ValueError("ctes must contain one group for every SQLStep")
        if any(
            not isinstance(name, str) or not name for group in ctes for name in group
        ):
            raise ValueError("cte names must be non-empty strings")
        if not isinstance(self.candidate_count, int) or isinstance(
            self.candidate_count, bool
        ):
            raise TypeError("candidate_count must be an integer")
        if self.candidate_count < 0:
            raise ValueError("candidate_count must not be negative")
        if not isinstance(self.evidence, Mapping):
            raise TypeError("evidence must be a mapping")
        if not isinstance(self.extraction_reason, str) or not self.extraction_reason:
            raise ValueError("extraction_reason must be a non-empty string")
        if not isinstance(self.backend, str) or not self.backend.strip():
            raise ValueError("backend must be a non-empty string")
        if (
            not isinstance(self.backend_version, str)
            or not self.backend_version.strip()
        ):
            raise ValueError("backend_version must be a non-empty string")

        for step in steps:
            if not all(
                hasattr(step, attribute)
                for attribute in (
                    "statement_index",
                    "statement_type",
                    "target",
                    "sources",
                )
            ):
                raise TypeError("steps must contain SQLStep-compatible values")

        object.__setattr__(self, "steps", steps)
        object.__setattr__(
            self,
            "ctes",
            tuple(tuple(sorted(dict.fromkeys(group))) for group in ctes),
        )
        object.__setattr__(self, "parse_status", SqlParseStatus(self.parse_status))
        object.__setattr__(self, "confidence", SqlParseConfidence(self.confidence))
        object.__setattr__(self, "evidence", dict(self.evidence))
        object.__setattr__(self, "backend", self.backend.strip())
        object.__setattr__(self, "backend_version", self.backend_version.strip())

    def to_compare_dict(self) -> dict[str, object]:
        """返回供 future shadow compare 使用的最小、无代码文本快照。

        这不是 compare engine，也不包含 edge diff 或性能统计；调用方可以在
        外层附加 ``elapsed_ms``、edge count 和 issue count。
        """

        steps: list[dict[str, object]] = []
        for step, ctes in zip(self.steps, self.ctes, strict=True):
            steps.append(
                {
                    "statement_index": step.statement_index,
                    "statement_type": step.statement_type,
                    "sources": tuple(step.sources),
                    "target": step.target,
                    "ctes": ctes,
                }
            )
        return {
            "backend": self.backend,
            "backend_version": self.backend_version,
            "parse_status": self.parse_status.value,
            "confidence": self.confidence.value,
            "extraction_reason": self.extraction_reason,
            "candidate_count": self.candidate_count,
            "steps": tuple(steps),
            "evidence": dict(self.evidence),
        }


@runtime_checkable
class ParserBackend(Protocol):
    """可替换 parser backend 的最小运行时 contract。"""

    backend: str
    backend_version: str

    def analyze(self, script_code: str) -> SqlAnalysis:
        """分析一个 script；不要猜测无法静态确认的 source/target。"""


# Issue #41 中的两个命名都保留：实现边界叫 ParserBackend，调用语义叫
# SqlAnalyzer。它们是同一个 structural Protocol，不创建第二套接口。
SqlAnalyzer = ParserBackend


_SUCCESS_REASONS = frozenset({"CANDIDATE_FOUND", "RAW_SQL", "PYTHON_PARSE_RECOVERED"})


class LegacyParserBackend:
    """把现有 ``physical_dag`` parser 原样包进 backend contract。"""

    backend = LEGACY_PARSER_BACKEND
    backend_version = LEGACY_PARSER_BACKEND_VERSION

    def analyze(self, script_code: str) -> SqlAnalysis:
        if not isinstance(script_code, str):
            raise TypeError("script_code must be a string")

        # 延迟导入避免 physical_dag -> parser_backend -> physical_dag 循环；
        # 实际执行的 extraction/parser helper 仍是原有 production 实现。
        from . import physical_dag

        extraction = physical_dag._extract_python_candidates_with_reason(script_code)
        steps, ctes = physical_dag._parse_sql_candidates_with_ctes(
            extraction.candidates
        )
        reason = extraction.reason.value
        if reason in _SUCCESS_REASONS:
            status = SqlParseStatus.SUCCESS
            confidence = (
                SqlParseConfidence.CONSERVATIVE
                if reason == "PYTHON_PARSE_RECOVERED"
                else SqlParseConfidence.HIGH
            )
        elif reason == "PYTHON_PARSE_FAILED":
            status = SqlParseStatus.FAILED
            confidence = SqlParseConfidence.NONE
        else:
            status = SqlParseStatus.UNRESOLVED
            confidence = SqlParseConfidence.NONE

        candidate_count = len(extraction.candidates)
        return SqlAnalysis(
            steps=steps,
            ctes=ctes,
            candidate_count=candidate_count,
            parse_status=status,
            extraction_reason=reason,
            evidence={
                "candidate_count": candidate_count,
                "sql_extraction_reason": reason,
            },
            confidence=confidence,
            backend=self.backend,
            backend_version=self.backend_version,
        )


DEFAULT_PARSER_BACKEND: ParserBackend = LegacyParserBackend()


def analyze_sql(
    script_code: str,
    *,
    backend: ParserBackend | None = None,
) -> SqlAnalysis:
    """使用指定 backend 分析 SQL；省略时始终选择 legacy production backend。"""

    selected_backend = DEFAULT_PARSER_BACKEND if backend is None else backend
    if not isinstance(selected_backend, ParserBackend):
        raise TypeError("backend must implement ParserBackend")
    result = selected_backend.analyze(script_code)
    if not isinstance(result, SqlAnalysis):
        raise TypeError("backend.analyze() must return SqlAnalysis")
    if result.backend != selected_backend.backend:
        raise ValueError("analysis backend metadata does not match selected backend")
    if result.backend_version != selected_backend.backend_version:
        raise ValueError("analysis backend version does not match selected backend")
    return result


__all__ = [
    "DEFAULT_PARSER_BACKEND",
    "LEGACY_PARSER_BACKEND",
    "LEGACY_PARSER_BACKEND_VERSION",
    "LegacyParserBackend",
    "ParserBackend",
    "SqlAnalysis",
    "SqlAnalyzer",
    "SqlParseConfidence",
    "SqlParseStatus",
    "analyze_sql",
]
