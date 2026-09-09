"""Issue #42 的 SQLGlot shadow/oracle 研究 runner。

本模块只位于 research 工具边界：

* legacy backend 复用当前 ``shared.lineage.physical_dag`` 的现有抽取函数；
* SQLGlot 只把 SQL AST 映射为脱敏的 statement/source/target 事实；
* 两个 backend 的结果不会写入 ProgramPhysicalDAG、Audit、Materialization 或 DWS；
* corpus 只接受合成/脱敏 SQL，报告不输出 SQL 原文、源码或资产名称。

SQLGlot 是 parser candidate，不是 ground truth。每个 corpus sample 都带有
synthetic expected result 或 sanitized human review basis；comparison classification
和 truth classification 分开记录，避免把 ``SQLGlot != legacy`` 误当作 legacy 错误。
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import platform
import re
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from shared.lineage.lineage_builder import strip_sql_comments
from shared.lineage.physical_dag import (
    _cte_names,
    _extract_python_candidates_with_reason,
    _parse_sql_candidates,
)
from shared.lineage.sql_parser import split_sql_statements

try:
    _sqlglot = importlib.import_module("sqlglot")
except ImportError:  # pragma: no cover - exercised by the CLI environment check
    _sqlglot = None

ErrorLevel = getattr(_sqlglot, "ErrorLevel", None)
exp = getattr(_sqlglot, "exp", None)
parse = getattr(_sqlglot, "parse", None)


SCHEMA_VERSION = 1
RUNNER_VERSION = "1"
STATUS_RESOLVED = "resolved"
STATUS_UNRESOLVED = "unresolved"
STATUS_FAILED = "failed"
VALID_STATUSES = {STATUS_RESOLVED, STATUS_UNRESOLVED, STATUS_FAILED}
TRUTH_SCOPE_SQL = "sql_semantics"
TRUTH_SCOPE_BOUNDARY = "extraction_boundary"
TRUTH_SCOPE_OUTSIDE = "outside_production_lineage"
VALID_TRUTH_SCOPES = {
    TRUTH_SCOPE_SQL,
    TRUTH_SCOPE_BOUNDARY,
    TRUTH_SCOPE_OUTSIDE,
}
TRUTH_BASES = {
    "manual_fixture_truth",
    "synthetic_expected_result",
    "sanitized_human_review",
}

# These are intentionally fixed.  A corpus that contains other identifiers must
# be reviewed before it can be committed to this public repository.
_SAFE_IDENTIFIER_PREFIXES = (
    "DEMO_",
    "TMP_",
    "SESSION_",
    "CATALOG_",
)
_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"(?:password|passwd|secret|token|credential|private[_ -]?key)", re.I),
    re.compile(r"(?:jdbc|mysql|postgres(?:ql)?|oracle|snowflake)://", re.I),
    re.compile(r"(?:svn|https?)://", re.I),
    re.compile(r"[A-Za-z]:[\\/]"),
)
_SAMPLE_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class StepFact:
    """一个 backend 的单 statement 事实，只保留 lineage 研究所需字段。"""

    statement_type: str
    target: str | None
    sources: tuple[str, ...]

    def signature(self) -> tuple[str, str | None, tuple[str, ...]]:
        return (
            self.statement_type,
            self.target,
            tuple(sorted(set(self.sources))),
        )


@dataclass(frozen=True, slots=True)
class ExpectedFact:
    """Corpus 中由 fixture truth 定义的期望结果。"""

    status: str
    steps: tuple[StepFact, ...]
    ctes: tuple[str, ...]
    truth_basis: str
    truth_scope: str
    truth_note: str

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"invalid expected status: {self.status}")
        if self.truth_basis not in TRUTH_BASES:
            raise ValueError(f"invalid truth basis: {self.truth_basis}")
        if self.truth_scope not in VALID_TRUTH_SCOPES:
            raise ValueError(f"invalid truth scope: {self.truth_scope}")
        if self.status != STATUS_RESOLVED and self.steps:
            raise ValueError("unresolved/failed truth cannot contain steps")

    def signature(self) -> tuple[tuple[str, str | None, tuple[str, ...]], ...]:
        return tuple(step.signature() for step in self.steps)


@dataclass(frozen=True, slots=True)
class CorpusSample:
    """单个公开、可重放、无真实资产的研究 sample。"""

    sample_id: str
    origin: str
    evidence_ref: str
    shape: tuple[str, ...]
    dialect: str
    legacy_input: str
    sqlglot_input: str | None
    expected: ExpectedFact
    production_scope: str

    def __post_init__(self) -> None:
        if not _SAMPLE_ID_RE.fullmatch(self.sample_id):
            raise ValueError(f"unsafe sample id: {self.sample_id}")
        if not self.shape:
            raise ValueError(f"sample has no shape: {self.sample_id}")
        if not self.legacy_input.strip():
            raise ValueError(f"sample has empty legacy input: {self.sample_id}")
        if not self.dialect.strip():
            raise ValueError(f"sample has empty dialect: {self.sample_id}")


@dataclass(frozen=True, slots=True)
class BackendResult:
    """一次 backend 执行的内部结果；不会直接作为公开 report 输出。"""

    backend: str
    status: str
    steps: tuple[StepFact, ...]
    ctes: tuple[str, ...]
    reason: str | None
    evidence: str
    confidence: str
    elapsed_ns: int = 0

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"invalid backend status: {self.status}")

    @property
    def signature(self) -> tuple[tuple[str, str | None, tuple[str, ...]], ...]:
        return tuple(step.signature() for step in self.steps)

    @property
    def source_count(self) -> int:
        return sum(len(set(step.sources)) for step in self.steps)

    @property
    def target_count(self) -> int:
        return sum(step.target is not None for step in self.steps)


@dataclass(frozen=True, slots=True)
class TruthMetrics:
    """一个 backend 相对 expected truth 的 bounded error 计数。"""

    exact: bool
    false_positive_events: int
    false_negative_events: int
    silent_wrong_target: int
    silent_wrong_source: int
    mismatches: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Comparison:
    sample: CorpusSample
    legacy: BackendResult
    sqlglot: BackendResult
    availability_class: str
    failure_classes: tuple[str, ...]
    truth_class: str
    legacy_truth: TruthMetrics
    sqlglot_truth: TruthMetrics


@dataclass(frozen=True, slots=True)
class BenchmarkSummary:
    """不设 wall-clock threshold 的相对性能 summary。"""

    backend: str
    sample_count: int
    operation_count: int
    total_runtime_ms: float
    mean_ms_per_sample: float
    p50_ms_per_sample: float
    p95_ms_per_sample: float
    max_ms_per_sample: float
    failure_count: int
    unresolved_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "sample_count": self.sample_count,
            "operation_count": self.operation_count,
            "total_runtime_ms": self.total_runtime_ms,
            "mean_ms_per_sample": self.mean_ms_per_sample,
            "p50_ms_per_sample": self.p50_ms_per_sample,
            "p95_ms_per_sample": self.p95_ms_per_sample,
            "max_ms_per_sample": self.max_ms_per_sample,
            "failure_count": self.failure_count,
            "unresolved_count": self.unresolved_count,
        }


@contextmanager
def _quiet_sqlglot() -> Iterator[None]:
    """屏蔽 SQLGlot 对 unsupported syntax 的 warning，避免污染脱敏 runner 输出。"""

    logger = logging.getLogger("sqlglot")
    previous_disabled = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = previous_disabled


def _require_sqlglot() -> None:
    if parse is None or exp is None or ErrorLevel is None:
        raise RuntimeError(
            "SQLGlot is required for Issue #42 research; install "
            "requirements-research.txt"
        )


def _normalize_name(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    text = text.replace("`", "").replace('"', "").replace("[", "").replace("]", "")
    text = re.sub(r"\s+", "", text)
    return text or None


def _table_name(table: Any) -> str | None:
    """从 SQLGlot Table AST 取得不含 alias 的 canonical 物理名称。"""

    parts = []
    for part in getattr(table, "parts", ()):
        name = getattr(part, "name", None)
        if name:
            parts.append(str(name))
    if not parts:
        return None
    return _normalize_name(".".join(parts))


def _first_table(expression: Any) -> Any | None:
    if exp is None:
        return None
    if isinstance(expression, exp.Table):
        return expression
    return next(expression.find_all(exp.Table), None)


def _sqlglot_ctes(expression: Any) -> tuple[str, ...]:
    if exp is None:
        return ()
    names = {
        name
        for cte in expression.find_all(exp.CTE)
        if (name := _normalize_name(getattr(cte, "alias", None)))
    }
    return tuple(sorted(names))


def _sqlglot_step(expression: Any, cte_names: set[str]) -> StepFact | None:
    """把一个 SQLGlot expression 映射为有限的 statement fact。"""

    if exp is None:
        return None

    statement_type: str | None = None
    if isinstance(expression, exp.Insert):
        statement_type = "insert"
    elif isinstance(expression, exp.Create):
        kind = str(expression.args.get("kind") or "").lower()
        if kind == "table":
            statement_type = "create_table"
        elif kind == "view":
            statement_type = "create_view"
    elif isinstance(expression, exp.Merge):
        statement_type = "merge"
    elif isinstance(expression, exp.Update):
        statement_type = "update"
    elif isinstance(expression, exp.Select):
        statement_type = "select"
    elif isinstance(expression, exp.Delete):
        statement_type = "delete"

    if statement_type is None:
        return None

    target_table = _first_table(expression.this) if statement_type != "select" else None
    target = _table_name(target_table)
    if (
        statement_type
        in {"insert", "create_table", "create_view", "merge", "update", "delete"}
        and target is None
    ):
        return None

    sources: list[str] = []
    seen: set[str] = set()
    for table in expression.find_all(exp.Table):
        if table is target_table:
            continue
        # SQLGlot also represents table-valued functions as ``exp.Table``
        # whose ``this`` is an Anonymous expression.  They have no physical
        # table identity and must remain unresolved rather than become a
        # fabricated source named TABLE.
        if not isinstance(getattr(table, "this", None), exp.Identifier):
            continue
        name = _table_name(table)
        if not name:
            continue
        if name in cte_names or name in seen:
            continue
        seen.add(name)
        sources.append(name)

    return StepFact(statement_type, target, tuple(sources))


def analyze_legacy(script_code: str) -> BackendResult:
    """调用当前 legacy extractor，不调用 DAG/materialization。"""

    started = time.perf_counter_ns()
    extraction = _extract_python_candidates_with_reason(script_code)
    steps = _parse_sql_candidates(extraction.candidates)

    ctes: set[str] = set()
    for candidate in extraction.candidates:
        comment_free = strip_sql_comments(candidate.text)
        for statement in split_sql_statements(comment_free):
            ctes.update(_cte_names(statement))

    reason = extraction.reason.value
    if extraction.reason.value == "PYTHON_PARSE_FAILED":
        status = STATUS_FAILED
    elif not steps:
        status = STATUS_UNRESOLVED
    elif any(step.statement_type == "unknown" for step in steps):
        status = STATUS_UNRESOLVED
        reason = "UNSUPPORTED_STATEMENT"
    else:
        status = STATUS_RESOLVED

    facts = tuple(
        StepFact(step.statement_type, step.target, tuple(step.sources))
        for step in steps
    )
    return BackendResult(
        backend="legacy",
        status=status,
        steps=facts,
        ctes=tuple(sorted(ctes)),
        reason=reason,
        evidence="legacy_extractor_and_regex_relation_scan",
        confidence="production_semantics",
        elapsed_ns=time.perf_counter_ns() - started,
    )


def analyze_sqlglot(sql_text: str | None, dialect: str) -> BackendResult:
    """用固定 read dialect 做 SQLGlot AST parse 和最小事实映射。"""

    _require_sqlglot()
    parse_function = parse
    error_level = ErrorLevel
    if parse_function is None or error_level is None:
        raise RuntimeError(
            "SQLGlot is required for Issue #42 research; install "
            "requirements-research.txt"
        )
    started = time.perf_counter_ns()
    if sql_text is None or not sql_text.strip():
        return BackendResult(
            backend="sqlglot",
            status=STATUS_UNRESOLVED,
            steps=(),
            ctes=(),
            reason="NO_STATIC_SQL_INPUT",
            evidence="no_sql_text_available_at_backend_boundary",
            confidence="not_comparable",
            elapsed_ns=time.perf_counter_ns() - started,
        )

    try:
        with _quiet_sqlglot():
            expressions = parse_function(
                sql_text,
                read=dialect,
                error_level=error_level.RAISE,
            )
    except Exception:
        return BackendResult(
            backend="sqlglot",
            status=STATUS_FAILED,
            steps=(),
            ctes=(),
            reason="PARSE_ERROR",
            evidence="sqlglot_parse_exception_class_suppressed_for_privacy",
            confidence="parser_result_only",
            elapsed_ns=time.perf_counter_ns() - started,
        )

    if not expressions:
        return BackendResult(
            backend="sqlglot",
            status=STATUS_UNRESOLVED,
            steps=(),
            ctes=(),
            reason="EMPTY_PARSE_RESULT",
            evidence="sqlglot_ast_parse",
            confidence="parser_result_only",
            elapsed_ns=time.perf_counter_ns() - started,
        )

    facts: list[StepFact] = []
    ctes: set[str] = set()
    for expression in expressions:
        ctes.update(_sqlglot_ctes(expression))
        fact = _sqlglot_step(expression, set(ctes))
        if fact is None:
            return BackendResult(
                backend="sqlglot",
                status=STATUS_FAILED,
                steps=tuple(facts),
                ctes=tuple(sorted(ctes)),
                reason="UNSUPPORTED_STATEMENT",
                evidence="sqlglot_ast_without_supported_lineage_statement_mapping",
                confidence="parser_result_only",
                elapsed_ns=time.perf_counter_ns() - started,
            )
        facts.append(fact)

    return BackendResult(
        backend="sqlglot",
        status=STATUS_RESOLVED,
        steps=tuple(facts),
        ctes=tuple(sorted(ctes)),
        reason=None,
        evidence="sqlglot_ast_table_relation_scan",
        confidence="parser_result_only",
        elapsed_ns=time.perf_counter_ns() - started,
    )


def _parse_step(value: Mapping[str, Any]) -> StepFact:
    statement_type = str(value["statement_type"])
    target = value.get("target")
    if target is not None:
        target = _normalize_name(str(target))
    sources = tuple(
        item
        for item in (
            _normalize_name(str(source)) for source in value.get("sources", ())
        )
        if item is not None
    )
    return StepFact(statement_type, target, sources)


def _load_jsonl(path: Path) -> tuple[CorpusSample, ...]:
    samples: list[CorpusSample] = []
    seen_ids: set[str] = set()
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid corpus JSON at line {line_number}") from exc
        if not isinstance(raw, dict):
            raise ValueError(f"corpus line {line_number} must be an object")
        sample_id = str(raw["id"])
        if sample_id in seen_ids:
            raise ValueError(f"duplicate corpus sample id: {sample_id}")
        seen_ids.add(sample_id)
        expected_raw = raw["expected"]
        if not isinstance(expected_raw, dict):
            raise ValueError(f"expected must be an object: {sample_id}")
        expected = ExpectedFact(
            status=str(expected_raw["status"]),
            steps=tuple(_parse_step(item) for item in expected_raw.get("steps", ())),
            ctes=tuple(
                sorted(
                    item
                    for item in (
                        _normalize_name(str(cte))
                        for cte in expected_raw.get("ctes", ())
                    )
                    if item is not None
                )
            ),
            truth_basis=str(expected_raw["truth_basis"]),
            truth_scope=str(expected_raw["truth_scope"]),
            truth_note=str(expected_raw["truth_note"]),
        )
        sample = CorpusSample(
            sample_id=sample_id,
            origin=str(raw["origin"]),
            evidence_ref=str(raw["evidence_ref"]),
            shape=tuple(str(item) for item in raw["shape"]),
            dialect=str(raw["dialect"]),
            legacy_input=str(raw["legacy_input"]),
            sqlglot_input=(
                None if raw.get("sqlglot_input") is None else str(raw["sqlglot_input"])
            ),
            expected=expected,
            production_scope=str(raw.get("production_scope", "production_lineage")),
        )
        samples.append(sample)
    if not samples:
        raise ValueError("corpus is empty")
    return tuple(samples)


def _raw_corpus_text(samples: Sequence[CorpusSample]) -> str:
    """为 hash 和 deterministic test 生成不包含路径/时间的 canonical corpus。"""

    rows = []
    for sample in samples:
        rows.append(
            {
                "id": sample.sample_id,
                "origin": sample.origin,
                "evidence_ref": sample.evidence_ref,
                "shape": sample.shape,
                "dialect": sample.dialect,
                "legacy_input": sample.legacy_input,
                "sqlglot_input": sample.sqlglot_input,
                "expected": {
                    "status": sample.expected.status,
                    "steps": [
                        {
                            "statement_type": step.statement_type,
                            "target": step.target,
                            "sources": step.sources,
                        }
                        for step in sample.expected.steps
                    ],
                    "ctes": sample.expected.ctes,
                    "truth_basis": sample.expected.truth_basis,
                    "truth_scope": sample.expected.truth_scope,
                    "truth_note": sample.expected.truth_note,
                },
                "production_scope": sample.production_scope,
            }
        )
    return json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def validate_privacy(samples: Sequence[CorpusSample]) -> tuple[str, ...]:
    """拒绝可能把真实样本带入公开 corpus 的明显信号。"""

    errors: list[str] = []
    for sample in samples:
        serialized = json.dumps(
            {
                "id": sample.sample_id,
                "origin": sample.origin,
                "evidence_ref": sample.evidence_ref,
                "shape": sample.shape,
                "dialect": sample.dialect,
                "legacy_input": sample.legacy_input,
                "sqlglot_input": sample.sqlglot_input,
                "truth_note": sample.expected.truth_note,
            },
            ensure_ascii=False,
        )
        for pattern in _SENSITIVE_TEXT_PATTERNS:
            if pattern.search(serialized):
                errors.append(f"{sample.sample_id}: sensitive text pattern")
                break

        for input_text in (sample.legacy_input, sample.sqlglot_input or ""):
            # Qualified identifiers in this corpus must be visibly synthetic.  The
            # check intentionally ignores SQL keywords, aliases and CTE names.
            for match in re.finditer(
                r"(?<![A-Za-z0-9_])([A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)+)"
                r"(?![A-Za-z0-9_])",
                input_text,
            ):
                parts = match.group(1).upper().split(".")
                if any(part.startswith(_SAFE_IDENTIFIER_PREFIXES) for part in parts):
                    continue
                if all(
                    part in {"BASE", "JOINED", "TREE", "SRC", "RESULT", "SOURCE"}
                    for part in parts
                ):
                    continue
                # Python dotted module names are not SQL assets and do not expose
                # data identity.  Keep the allowlist narrow and explicit.
                if match.group(1) in {
                    "executor.do",
                    "executor.run",
                    "runtime_sql",
                    "runtime_target",
                    "shared.db",
                }:
                    continue
                if parts[0].lower() in {
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                    "f",
                    "g",
                    "q",
                    "r",
                    "s",
                    "t",
                    "src",
                    "base",
                    "joined",
                    "tree",
                }:
                    # These are synthetic relation aliases qualifying columns,
                    # not physical asset identities.
                    continue
                # Single identifiers such as ``ROW_NUMBER`` are not matched here;
                # a qualified name outside the synthetic namespace is reviewable.
                errors.append(f"{sample.sample_id}: non-synthetic qualified identifier")
                break
    return tuple(sorted(set(errors)))


def _signature_mismatches(
    expected: ExpectedFact,
    actual: BackendResult,
) -> tuple[str, ...]:
    mismatches: list[str] = []
    if actual.status != expected.status:
        mismatches.append("parse_status")
    if expected.status != STATUS_RESOLVED or actual.status != STATUS_RESOLVED:
        return tuple(mismatches)
    if len(actual.steps) != len(expected.steps):
        mismatches.append("statement_type")
        return tuple(dict.fromkeys(mismatches))
    for expected_step, actual_step in zip(expected.steps, actual.steps):
        if expected_step.statement_type != actual_step.statement_type:
            mismatches.append("statement_type")
        if expected_step.target != actual_step.target:
            mismatches.append("target")
        if set(expected_step.sources) != set(actual_step.sources):
            mismatches.append("source")
    if tuple(expected.ctes) != tuple(actual.ctes):
        mismatches.append("cte")
    return tuple(dict.fromkeys(mismatches))


def _truth_metrics(expected: ExpectedFact, actual: BackendResult) -> TruthMetrics:
    mismatches = _signature_mismatches(expected, actual)
    if expected.status != STATUS_RESOLVED:
        if expected.status == actual.status and not actual.steps:
            return TruthMetrics(True, 0, 0, 0, 0, mismatches)
        if actual.status == STATUS_RESOLVED:
            return TruthMetrics(
                False,
                1,
                0,
                int(any(step.target for step in actual.steps)),
                int(any(step.sources for step in actual.steps)),
                mismatches,
            )
        return TruthMetrics(False, 0, 1, 0, 0, mismatches)

    if actual.status != STATUS_RESOLVED:
        return TruthMetrics(False, 0, 1, 0, 0, mismatches)

    false_positive = 0
    false_negative = 0
    wrong_target = 0
    wrong_source = 0
    for index, expected_step in enumerate(expected.steps):
        if index >= len(actual.steps):
            false_negative += 1
            continue
        actual_step = actual.steps[index]
        if expected_step.statement_type != actual_step.statement_type:
            false_negative += 1
        if expected_step.target != actual_step.target:
            wrong_target += 1
            if expected_step.target is None:
                false_positive += 1
            elif actual_step.target is None:
                false_negative += 1
            else:
                false_positive += 1
                false_negative += 1
        expected_sources = set(expected_step.sources)
        actual_sources = set(actual_step.sources)
        false_positive += len(actual_sources - expected_sources)
        false_negative += len(expected_sources - actual_sources)
        if expected_sources != actual_sources:
            wrong_source += 1
    false_positive += len(set(actual.ctes) - set(expected.ctes))
    false_negative += len(set(expected.ctes) - set(actual.ctes))
    exact = not mismatches
    return TruthMetrics(
        exact,
        false_positive,
        false_negative,
        wrong_target,
        wrong_source,
        mismatches,
    )


def _availability_class(legacy: BackendResult, sqlglot: BackendResult) -> str:
    if legacy.status == STATUS_RESOLVED and sqlglot.status == STATUS_RESOLVED:
        if legacy.signature == sqlglot.signature and legacy.ctes == sqlglot.ctes:
            return "MATCH"
        return "BOTH_RESOLVED_DISAGREEMENT"
    if legacy.status == STATUS_RESOLVED:
        return "LEGACY_ONLY"
    if sqlglot.status == STATUS_RESOLVED:
        return "SQLGLOT_ONLY"
    if legacy.status == STATUS_FAILED and sqlglot.status == STATUS_FAILED:
        return "BOTH_FAILED"
    if legacy.status == STATUS_FAILED or sqlglot.status == STATUS_FAILED:
        return "ONE_FAILED_ONE_UNRESOLVED"
    return "BOTH_UNRESOLVED"


def _failure_classes(legacy: BackendResult, sqlglot: BackendResult) -> tuple[str, ...]:
    failures = []
    if legacy.status == STATUS_FAILED:
        failures.append("LEGACY_FAILED")
    if sqlglot.status == STATUS_FAILED:
        failures.append("SQLGLOT_FAILED")
    return tuple(failures)


def _truth_class(
    sample: CorpusSample,
    legacy: BackendResult,
    sqlglot: BackendResult,
    legacy_truth: TruthMetrics,
    sqlglot_truth: TruthMetrics,
) -> str:
    expected = sample.expected
    if expected.truth_scope != TRUTH_SCOPE_SQL:
        return "BOUNDARY_NOT_COMPARABLE"
    if legacy_truth.exact and sqlglot_truth.exact:
        if expected.status == STATUS_RESOLVED:
            return "MATCH"
        if expected.status == STATUS_FAILED:
            return "BOTH_FAILED"
        return "BOTH_UNRESOLVED"
    if legacy_truth.exact and not sqlglot_truth.exact:
        if sqlglot_truth.false_positive_events > 0:
            return "SQLGLOT_MORE_AGGRESSIVE"
        return "SQLGLOT_MORE_CONSERVATIVE"
    if sqlglot_truth.exact and not legacy_truth.exact:
        if legacy_truth.false_positive_events > 0:
            return "LEGACY_MORE_AGGRESSIVE"
        return "LEGACY_MORE_CONSERVATIVE"

    if legacy_truth.false_positive_events > sqlglot_truth.false_positive_events:
        return "LEGACY_MORE_AGGRESSIVE"
    if sqlglot_truth.false_positive_events > legacy_truth.false_positive_events:
        return "SQLGLOT_MORE_AGGRESSIVE"
    if legacy_truth.false_negative_events > sqlglot_truth.false_negative_events:
        return "LEGACY_MORE_CONSERVATIVE"
    if sqlglot_truth.false_negative_events > legacy_truth.false_negative_events:
        return "SQLGLOT_MORE_CONSERVATIVE"
    if legacy.status == STATUS_FAILED and sqlglot.status == STATUS_FAILED:
        return "BOTH_FAILED"
    if legacy.status == STATUS_UNRESOLVED and sqlglot.status == STATUS_UNRESOLVED:
        return "BOTH_UNRESOLVED"
    return "REVIEW_REQUIRED"


def compare_sample(sample: CorpusSample) -> Comparison:
    legacy = analyze_legacy(sample.legacy_input)
    sqlglot = analyze_sqlglot(sample.sqlglot_input, sample.dialect)
    legacy_truth = _truth_metrics(sample.expected, legacy)
    sqlglot_truth = _truth_metrics(sample.expected, sqlglot)
    return Comparison(
        sample=sample,
        legacy=legacy,
        sqlglot=sqlglot,
        availability_class=_availability_class(legacy, sqlglot),
        failure_classes=_failure_classes(legacy, sqlglot),
        truth_class=_truth_class(
            sample,
            legacy,
            sqlglot,
            legacy_truth,
            sqlglot_truth,
        ),
        legacy_truth=legacy_truth,
        sqlglot_truth=sqlglot_truth,
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[int(percentile) - 1]


def _benchmark_backend(
    samples: Sequence[CorpusSample],
    backend: Literal["legacy", "legacy_sql_surface", "sqlglot"],
    repeats: int,
    warmup: int,
) -> BenchmarkSummary:
    if repeats < 1:
        raise ValueError("benchmark repeats must be positive")
    if warmup < 0:
        raise ValueError("benchmark warmup cannot be negative")

    def run_one(sample: CorpusSample) -> BackendResult:
        if backend == "legacy":
            return analyze_legacy(sample.legacy_input)
        if backend == "legacy_sql_surface":
            # Fair SQL parser comparison: both backends receive the same
            # reconstructed SQL text, without Python extraction work.
            return analyze_legacy(sample.sqlglot_input or "")
        return analyze_sqlglot(sample.sqlglot_input, sample.dialect)

    for _ in range(warmup):
        for sample in samples:
            run_one(sample)

    durations: list[float] = []
    failure_count = 0
    unresolved_count = 0
    total_ns = 0
    for _ in range(repeats):
        for sample in samples:
            started = time.perf_counter_ns()
            result = run_one(sample)
            elapsed_ns = time.perf_counter_ns() - started
            total_ns += elapsed_ns
            durations.append(elapsed_ns / 1_000_000)
            failure_count += result.status == STATUS_FAILED
            unresolved_count += result.status == STATUS_UNRESOLVED

    return BenchmarkSummary(
        backend=backend,
        sample_count=len(samples),
        operation_count=len(durations),
        total_runtime_ms=round(total_ns / 1_000_000, 6),
        mean_ms_per_sample=round(statistics.fmean(durations), 6),
        p50_ms_per_sample=round(_percentile(durations, 50), 6),
        p95_ms_per_sample=round(_percentile(durations, 95), 6),
        max_ms_per_sample=round(max(durations, default=0.0), 6),
        failure_count=failure_count,
        unresolved_count=unresolved_count,
    )


def _backend_public(result: BackendResult) -> dict[str, object]:
    return {
        "status": result.status,
        "reason": result.reason,
        "step_count": len(result.steps),
        "source_count": result.source_count,
        "target_count": result.target_count,
        "cte_count": len(result.ctes),
        "evidence": result.evidence,
        "confidence": result.confidence,
    }


def _comparison_public(comparison: Comparison) -> dict[str, object]:
    return {
        "id": comparison.sample.sample_id,
        "shape": list(comparison.sample.shape),
        "dialect": comparison.sample.dialect,
        "production_scope": comparison.sample.production_scope,
        "truth_basis": comparison.sample.expected.truth_basis,
        "truth_scope": comparison.sample.expected.truth_scope,
        "availability_class": comparison.availability_class,
        "failure_classes": list(comparison.failure_classes),
        "truth_class": comparison.truth_class,
        "mismatches": {
            "legacy": list(comparison.legacy_truth.mismatches),
            "sqlglot": list(comparison.sqlglot_truth.mismatches),
        },
        "legacy": _backend_public(comparison.legacy),
        "sqlglot": _backend_public(comparison.sqlglot),
    }


def _accuracy_summary(
    comparisons: Sequence[Comparison],
    backend: Literal["legacy", "sqlglot"],
) -> dict[str, object]:
    eligible = [
        item
        for item in comparisons
        if item.sample.expected.truth_scope == TRUTH_SCOPE_SQL
    ]
    metrics = [
        item.legacy_truth if backend == "legacy" else item.sqlglot_truth
        for item in eligible
    ]
    return {
        "eligible_samples": len(eligible),
        "truth_exact_samples": sum(metric.exact for metric in metrics),
        "truth_exact_rate_pct": round(
            100.0 * sum(metric.exact for metric in metrics) / len(metrics), 2
        )
        if metrics
        else 0.0,
        "false_positive_samples": sum(
            metric.false_positive_events > 0 for metric in metrics
        ),
        "false_negative_samples": sum(
            metric.false_negative_events > 0 for metric in metrics
        ),
        "false_positive_events": sum(
            metric.false_positive_events for metric in metrics
        ),
        "false_negative_events": sum(
            metric.false_negative_events for metric in metrics
        ),
        "silent_wrong_target": sum(metric.silent_wrong_target for metric in metrics),
        "silent_wrong_source": sum(metric.silent_wrong_source for metric in metrics),
        "parse_failure_count": sum(
            item.legacy.status == STATUS_FAILED
            if backend == "legacy"
            else item.sqlglot.status == STATUS_FAILED
            for item in eligible
        ),
        "unresolved_count": sum(
            item.legacy.status == STATUS_UNRESOLVED
            if backend == "legacy"
            else item.sqlglot.status == STATUS_UNRESOLVED
            for item in eligible
        ),
    }


def _git_revision() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def build_report(
    samples: Sequence[CorpusSample],
    *,
    run_benchmark: bool = True,
    benchmark_repeats: int = 25,
    benchmark_warmup: int = 2,
) -> dict[str, object]:
    """运行 compare 并返回无 SQL 原文的机器可读 research report。"""

    _require_sqlglot()
    privacy_errors = validate_privacy(samples)
    if privacy_errors:
        raise ValueError(
            "corpus privacy validation failed: " + "; ".join(privacy_errors)
        )

    comparisons = tuple(compare_sample(sample) for sample in samples)
    shape_counts: Counter[str] = Counter(
        shape for sample in samples for shape in sample.shape
    )
    dialect_counts: Counter[str] = Counter(sample.dialect for sample in samples)
    availability_counts = Counter(item.availability_class for item in comparisons)
    failure_counts = Counter(
        failure for item in comparisons for failure in item.failure_classes
    )
    truth_counts = Counter(item.truth_class for item in comparisons)
    mismatch_counts = Counter(
        mismatch
        for item in comparisons
        for mismatch in item.legacy_truth.mismatches + item.sqlglot_truth.mismatches
    )
    sql_semantics_disagreements = [
        item
        for item in comparisons
        if item.sample.expected.truth_scope == TRUTH_SCOPE_SQL
        and item.truth_class != "MATCH"
    ]
    boundary_findings = [
        item
        for item in comparisons
        if item.sample.expected.truth_scope != TRUTH_SCOPE_SQL
    ]

    benchmark: dict[str, object] | None = None
    if run_benchmark:
        benchmark_samples = tuple(
            sample for sample in samples if sample.sqlglot_input is not None
        )
        legacy_benchmark = _benchmark_backend(
            benchmark_samples,
            "legacy",
            benchmark_repeats,
            benchmark_warmup,
        )
        legacy_sql_benchmark = _benchmark_backend(
            benchmark_samples,
            "legacy_sql_surface",
            benchmark_repeats,
            benchmark_warmup,
        )
        sqlglot_benchmark = _benchmark_backend(
            benchmark_samples,
            "sqlglot",
            benchmark_repeats,
            benchmark_warmup,
        )
        production_relative_cost = (
            sqlglot_benchmark.total_runtime_ms / legacy_benchmark.total_runtime_ms
            if legacy_benchmark.total_runtime_ms
            else None
        )
        sql_surface_relative_cost = (
            sqlglot_benchmark.total_runtime_ms / legacy_sql_benchmark.total_runtime_ms
            if legacy_sql_benchmark.total_runtime_ms
            else None
        )
        benchmark = {
            "configuration": {
                "repeats": benchmark_repeats,
                "warmup": benchmark_warmup,
                "sample_selection": "sqlglot_input_is_not_null",
                "wall_clock_threshold": None,
            },
            "sample_count": len(benchmark_samples),
            "legacy": legacy_benchmark.to_dict(),
            "legacy_sql_surface": legacy_sql_benchmark.to_dict(),
            "sqlglot": sqlglot_benchmark.to_dict(),
            "relative_cost": {
                "total_runtime_multiple_vs_legacy_production": (
                    round(production_relative_cost, 4)
                    if production_relative_cost is not None
                    else None
                ),
                "total_runtime_multiple_vs_legacy_sql_surface": (
                    round(sql_surface_relative_cost, 4)
                    if sql_surface_relative_cost is not None
                    else None
                ),
                "mean_ms_multiple_vs_legacy_production": round(
                    sqlglot_benchmark.mean_ms_per_sample
                    / legacy_benchmark.mean_ms_per_sample,
                    4,
                )
                if legacy_benchmark.mean_ms_per_sample
                else None,
                "mean_ms_multiple_vs_legacy_sql_surface": round(
                    sqlglot_benchmark.mean_ms_per_sample
                    / legacy_sql_benchmark.mean_ms_per_sample,
                    4,
                )
                if legacy_sql_benchmark.mean_ms_per_sample
                else None,
                "p95_ms_multiple_vs_legacy_production": round(
                    sqlglot_benchmark.p95_ms_per_sample
                    / legacy_benchmark.p95_ms_per_sample,
                    4,
                )
                if legacy_benchmark.p95_ms_per_sample
                else None,
                "p95_ms_multiple_vs_legacy_sql_surface": round(
                    sqlglot_benchmark.p95_ms_per_sample
                    / legacy_sql_benchmark.p95_ms_per_sample,
                    4,
                )
                if legacy_sql_benchmark.p95_ms_per_sample
                else None,
            },
        }

    sqlglot_version = importlib.metadata.version("sqlglot")
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_version": RUNNER_VERSION,
        "issue": 42,
        "git_revision": _git_revision(),
        "runtime": {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "platform": platform.system(),
        },
        "corpus": {
            "sample_count": len(samples),
            "canonical_sha256": hashlib.sha256(
                _raw_corpus_text(samples).encode("utf-8")
            ).hexdigest(),
            "origin_counts": dict(Counter(sample.origin for sample in samples)),
            "shape_counts": dict(sorted(shape_counts.items())),
            "dialect_counts": dict(sorted(dialect_counts.items())),
            "sanitization": {
                "privacy_validation": "PASS",
                "real_sql": False,
                "real_python_source": False,
                "real_identity": False,
                "runtime_credentials": False,
                "report_contains_sql_text": False,
            },
        },
        "configuration": {
            "legacy": {
                "entrypoint": "shared.lineage.physical_dag._extract_python_candidates_with_reason + _parse_sql_candidates",
                "normalizer": "shared.lineage.lineage_builder.normalize_table_name via production parser",
                "production_parser_modified": False,
            },
            "sqlglot": {
                "version": sqlglot_version,
                "dialect": "per_sample_corpus_dialect",
                "read_error_level": "RAISE",
                "unsupported_statement_policy": "failed_not_guessed",
                "relation_policy": "AST Table nodes excluding target and CTE names",
                "production_parser_modified": False,
            },
        },
        "classification_contract": {
            "availability": [
                "MATCH",
                "LEGACY_ONLY",
                "SQLGLOT_ONLY",
                "BOTH_UNRESOLVED",
                "BOTH_FAILED",
                "ONE_FAILED_ONE_UNRESOLVED",
                "BOTH_RESOLVED_DISAGREEMENT",
            ],
            "failure": ["LEGACY_FAILED", "SQLGLOT_FAILED"],
            "truth": [
                "MATCH",
                "LEGACY_MORE_CONSERVATIVE",
                "SQLGLOT_MORE_CONSERVATIVE",
                "LEGACY_MORE_AGGRESSIVE",
                "SQLGLOT_MORE_AGGRESSIVE",
                "BOTH_UNRESOLVED",
                "LEGACY_FAILED",
                "SQLGLOT_FAILED",
                "BOTH_FAILED",
                "BOUNDARY_NOT_COMPARABLE",
                "REVIEW_REQUIRED",
            ],
            "mismatch_dimensions": [
                "source",
                "target",
                "cte",
                "statement_type",
                "parse_status",
            ],
        },
        "summary": {
            "availability_class_counts": dict(sorted(availability_counts.items())),
            "failure_class_counts": dict(sorted(failure_counts.items())),
            "truth_class_counts": dict(sorted(truth_counts.items())),
            "mismatch_dimension_counts": dict(sorted(mismatch_counts.items())),
            "sql_semantics_disagreement_count": len(sql_semantics_disagreements),
            "boundary_or_outside_scope_count": len(boundary_findings),
            "accuracy": {
                "legacy": _accuracy_summary(comparisons, "legacy"),
                "sqlglot": _accuracy_summary(comparisons, "sqlglot"),
            },
            "blind_spot_samples": [
                {
                    "id": item.sample.sample_id,
                    "shape": list(item.sample.shape),
                    "truth_class": item.truth_class,
                    "mismatches": list(item.legacy_truth.mismatches),
                    "evidence_confidence": item.sample.expected.truth_basis,
                }
                for item in sql_semantics_disagreements
                if item.truth_class
                in {
                    "LEGACY_MORE_CONSERVATIVE",
                    "LEGACY_MORE_AGGRESSIVE",
                    "SQLGLOT_MORE_CONSERVATIVE",
                    "SQLGLOT_MORE_AGGRESSIVE",
                }
            ],
        },
        "comparisons": [_comparison_public(item) for item in comparisons],
        "truth_review": {
            "all_samples_have_bounded_truth": all(
                bool(item.sample.expected.truth_note.strip()) for item in comparisons
            ),
            "basis_counts": dict(
                Counter(item.sample.expected.truth_basis for item in comparisons)
            ),
            "sql_semantics_reviewed_disagreements": [
                {
                    "id": item.sample.sample_id,
                    "truth_basis": item.sample.expected.truth_basis,
                    "truth_note": item.sample.expected.truth_note,
                    "truth_class": item.truth_class,
                    "mismatches": list(item.legacy_truth.mismatches),
                }
                for item in sql_semantics_disagreements
            ],
            "boundary_findings": [
                {
                    "id": item.sample.sample_id,
                    "truth_basis": item.sample.expected.truth_basis,
                    "truth_scope": item.sample.expected.truth_scope,
                    "truth_note": item.sample.expected.truth_note,
                    "availability_class": item.availability_class,
                    "failure_classes": list(item.failure_classes),
                }
                for item in boundary_findings
            ],
        },
        "benchmark": benchmark,
    }


def run_cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--benchmark-repeats", type=int, default=25)
    parser.add_argument("--benchmark-warmup", type=int, default=2)
    parser.add_argument("--no-benchmark", action="store_true")
    args = parser.parse_args(argv)

    try:
        samples = _load_jsonl(args.corpus)
        report = build_report(
            samples,
            run_benchmark=not args.no_benchmark,
            benchmark_repeats=args.benchmark_repeats,
            benchmark_warmup=args.benchmark_warmup,
        )
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        print(f"sqlglot research runner failed: {exc}", file=sys.stderr)
        return 2

    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via CLI
    raise SystemExit(run_cli())


__all__ = [
    "BackendResult",
    "Comparison",
    "CorpusSample",
    "ExpectedFact",
    "StepFact",
    "analyze_legacy",
    "analyze_sqlglot",
    "build_report",
    "compare_sample",
    "validate_privacy",
]
