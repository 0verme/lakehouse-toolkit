"""Audit Golden Corpus 的脱敏格式、抽样、标注校验与指标计算。

本模块只消费已经生成的 ``AuditFact``、兼容 ``LineageIssue`` 或脱敏 candidate
manifest，不修改 Audit detector，也不引入 runtime disposition policy。公开 manifest
只允许保存不可逆 fingerprint、IssueType、结构化 evidence summary 和人工标注。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from shared.lineage.audit import AuditFact
from shared.lineage.domain import IssueType, LineageIssue

CORPUS_FORMAT = "lineage-audit-golden"
CORPUS_VERSION = "audit-golden-v1"
_FINGERPRINT_NAMESPACE = "lakehouse-toolkit:lineage-audit-golden:v1"
_SAMPLE_ID_PREFIX = "gc-"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_REASON_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")

_SAFE_EVIDENCE_SUMMARY_KEYS = frozenset(
    {
        "evidence_key_count",
        "statement_index_count",
        "edge_count",
        "node_count",
        "sink_count",
        "formal_sink_count",
        "temporary_sink_count",
        "written_target_count",
        "entry_source_count",
        "has_expected_target",
        "expected_target_written",
        "expected_target_is_sink",
        "has_previous_valid_target",
        "has_previous_broken_evidence",
    }
)


class FactLabel(str, Enum):
    """事实检测正确性标签，不表达业务是否接受该 issue。"""

    TRUE_POSITIVE = "TRUE_POSITIVE"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    AMBIGUOUS = "AMBIGUOUS"
    NO_ISSUE = "NO_ISSUE"


class BusinessDispositionLabel(str, Enum):
    """业务接受语义标签，独立于事实检测正确性。"""

    ACCEPTED = "ACCEPTED"


class CorpusFormatError(ValueError):
    """Corpus 或 candidate manifest 不符合公开格式。"""


class UnknownIssueTypeError(CorpusFormatError):
    """manifest 使用了当前代码不支持的 IssueType。"""


class DuplicateSampleError(CorpusFormatError):
    """Corpus 中出现重复 sample identity 或 fingerprint。"""


class UnlabeledCorpusError(CorpusFormatError):
    """指标计算要求人工标注，但 corpus 仍有未标注样本。"""


def _coerce_issue_type(value: IssueType | str | None, *, field_name: str) -> IssueType | None:
    if value is None:
        return None
    try:
        return IssueType(value)
    except (TypeError, ValueError):
        raise UnknownIssueTypeError(f"unknown IssueType in {field_name}") from None


def _normalized_issue_type(value: IssueType | str | None) -> IssueType | None:
    return _coerce_issue_type(value, field_name="issue_type")


def _normalized_fact_label(value: FactLabel | str | None) -> FactLabel | None:
    return _coerce_fact_label(value)


def _normalized_business_label(
    value: BusinessDispositionLabel | str | None,
) -> BusinessDispositionLabel | None:
    return _coerce_business_label(value)


def _require_token(value: str, *, field_name: str, pattern: re.Pattern[str] = _TOKEN_RE) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise CorpusFormatError(f"{field_name} must be a safe token")
    return value


def _require_fingerprint(value: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise CorpusFormatError("fingerprint must be a lowercase SHA-256 hex string")
    return value


def _normalize_summary(value: Mapping[str, object]) -> dict[str, int | bool]:
    if not isinstance(value, Mapping):
        raise CorpusFormatError("evidence_summary must be an object")
    normalized: dict[str, int | bool] = {}
    for key, item in value.items():
        if not isinstance(key, str) or key not in _SAFE_EVIDENCE_SUMMARY_KEYS:
            raise CorpusFormatError("evidence_summary contains an unsupported field")
        if isinstance(item, bool):
            normalized[key] = item
        elif isinstance(item, int) and not isinstance(item, bool) and item >= 0:
            normalized[key] = item
        else:
            raise CorpusFormatError("evidence_summary values must be boolean or non-negative integer")
    return dict(sorted(normalized.items()))


def _safe_collection_length(value: object) -> int:
    if isinstance(value, Mapping):
        return len(value)
    if isinstance(value, (list, tuple, set, frozenset)):
        return len(value)
    return 0


def _first_collection_length(evidence: Mapping[str, object], *keys: str) -> int:
    for key in keys:
        if key in evidence:
            return _safe_collection_length(evidence[key])
    return 0


def summarize_issue_evidence(evidence: Mapping[str, object] | str | None) -> dict[str, int | bool]:
    """把 issue evidence 压缩为只含计数/布尔值的公开摘要。

    不复制任何 evidence value，因此不会把 program/table/path/SQL 名称带入
    candidate manifest。
    """

    if not isinstance(evidence, Mapping):
        return {"evidence_key_count": 0}

    statement_index_count = _first_collection_length(evidence, "statement_indices")
    if statement_index_count == 0 and "statement_index" in evidence:
        statement_index_count = 1
    edge_count = _first_collection_length(
        evidence,
        "branch_edges",
        "cycle_edges",
        "branch_edge_pairs",
        "cycle_edge_pairs",
    )
    if edge_count == 0 and isinstance(evidence.get("edge"), Mapping):
        edge_count = 1
    node_count = _first_collection_length(evidence, "branch_nodes", "cycle_nodes")

    return {
        "evidence_key_count": len(evidence),
        "statement_index_count": statement_index_count,
        "edge_count": edge_count,
        "node_count": node_count,
        "sink_count": _first_collection_length(evidence, "sinks", "all_sinks"),
        "formal_sink_count": _first_collection_length(evidence, "formal_sinks", "actual_formal_sinks"),
        "temporary_sink_count": _first_collection_length(evidence, "temporary_sinks"),
        "written_target_count": _first_collection_length(evidence, "written_targets"),
        "entry_source_count": _first_collection_length(evidence, "entry_sources", "branch_roots"),
        "has_expected_target": evidence.get("expected_target") is not None,
        "expected_target_written": evidence.get("expected_target_written") is True,
        "expected_target_is_sink": evidence.get("expected_target_is_sink") is True,
        "has_previous_valid_target": evidence.get("previous_valid_target") is True,
        "has_previous_broken_evidence": "previous_broken_evidence" in evidence,
    }


def compute_golden_fingerprint(value: str) -> str:
    """用固定 namespace 对已有稳定 identity 做不可逆 SHA-256 fingerprint。"""

    if not isinstance(value, str) or not value:
        raise CorpusFormatError("fingerprint source must be non-empty text")
    payload = f"{_FINGERPRINT_NAMESPACE}\0{value}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class AuditCandidate:
    """待抽样的、已脱敏的 issue candidate 或 negative control。"""

    fingerprint: str
    issue_type: IssueType | str | None
    evidence_summary: Mapping[str, object]
    source_kind: str
    corpus_version: str = CORPUS_VERSION
    negative_control: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "fingerprint", _require_fingerprint(self.fingerprint))
        object.__setattr__(
            self,
            "issue_type",
            _coerce_issue_type(self.issue_type, field_name="issue_type"),
        )
        object.__setattr__(self, "evidence_summary", _normalize_summary(self.evidence_summary))
        _require_token(self.source_kind, field_name="source_kind")
        _require_token(self.corpus_version, field_name="corpus_version")
        if not isinstance(self.negative_control, bool):
            raise CorpusFormatError("negative_control must be boolean")
        if self.issue_type is None and not self.negative_control:
            raise CorpusFormatError("a candidate without issue_type must be a negative control")

    def to_dict(self) -> dict[str, object]:
        normalized_issue_type = _normalized_issue_type(self.issue_type)
        return {
            "fingerprint": self.fingerprint,
            "issue_type": normalized_issue_type.value if normalized_issue_type is not None else None,
            "evidence_summary": dict(self.evidence_summary),
            "source_kind": self.source_kind,
            "corpus_version": self.corpus_version,
            "negative_control": self.negative_control,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "AuditCandidate":
        expected = {
            "fingerprint",
            "issue_type",
            "evidence_summary",
            "source_kind",
            "corpus_version",
            "negative_control",
        }
        _require_exact_fields(value, expected, "candidate")
        return cls(
            fingerprint=value["fingerprint"],  # type: ignore[arg-type]
            issue_type=value["issue_type"],  # type: ignore[arg-type]
            evidence_summary=value["evidence_summary"],  # type: ignore[arg-type]
            source_kind=value["source_kind"],  # type: ignore[arg-type]
            corpus_version=value["corpus_version"],  # type: ignore[arg-type]
            negative_control=value["negative_control"],  # type: ignore[arg-type]
        )


def candidate_from_fact(
    fact: AuditFact,
    *,
    source_kind: str = "lineage_audit",
    corpus_version: str = CORPUS_VERSION,
) -> AuditCandidate:
    """从 policy 无关的 fact 生成脱敏 candidate。"""

    if not isinstance(fact, AuditFact):
        raise TypeError("fact must be an AuditFact")
    return AuditCandidate(
        fingerprint=compute_golden_fingerprint(fact.stable_issue_identity),
        issue_type=fact.issue_type,
        evidence_summary=summarize_issue_evidence(fact.evidence),
        source_kind=source_kind,
        corpus_version=corpus_version,
    )


def candidate_from_issue(
    issue: LineageIssue,
    *,
    source_kind: str = "lineage_audit",
    corpus_version: str = CORPUS_VERSION,
) -> AuditCandidate:
    """兼容入口：只读取 ``LineageIssue`` 的 fact 部分，忽略 policy。"""

    if not isinstance(issue, LineageIssue):
        raise TypeError("issue must be a LineageIssue")
    if issue.fingerprint is None:
        raise CorpusFormatError("LineageIssue must have a stable fingerprint")
    return candidate_from_fact(
        AuditFact.from_issue(issue),
        source_kind=source_kind,
        corpus_version=corpus_version,
    )


def negative_control_candidate(
    identity: str,
    *,
    source_kind: str = "negative_control",
    corpus_version: str = CORPUS_VERSION,
) -> AuditCandidate:
    """为无告警程序创建 candidate；``identity`` 只在本地用于计算 fingerprint。"""

    return AuditCandidate(
        fingerprint=compute_golden_fingerprint(identity),
        issue_type=None,
        evidence_summary={},
        source_kind=source_kind,
        corpus_version=corpus_version,
        negative_control=True,
    )


@dataclass(frozen=True, slots=True)
class CorpusSample:
    """JSONL corpus 的一条样本记录。"""

    sample_id: str
    issue_type: IssueType | str | None
    fact_label: FactLabel | str | None
    business_disposition_label: BusinessDispositionLabel | str | None
    fingerprint: str
    evidence_summary: Mapping[str, object]
    annotation_reason: str | None
    source_kind: str
    corpus_version: str
    sample_seed: int | None = None
    sampling_group: str | None = None
    negative_control: bool = False

    def __post_init__(self) -> None:
        _require_token(self.sample_id, field_name="sample_id")
        object.__setattr__(
            self,
            "issue_type",
            _coerce_issue_type(self.issue_type, field_name="issue_type"),
        )
        object.__setattr__(self, "fact_label", _coerce_fact_label(self.fact_label))
        object.__setattr__(
            self,
            "business_disposition_label",
            _coerce_business_label(self.business_disposition_label),
        )
        object.__setattr__(self, "fingerprint", _require_fingerprint(self.fingerprint))
        object.__setattr__(self, "evidence_summary", _normalize_summary(self.evidence_summary))
        _require_token(self.source_kind, field_name="source_kind")
        _require_token(self.corpus_version, field_name="corpus_version")
        if self.sampling_group is not None:
            _require_token(self.sampling_group, field_name="sampling_group")
        if self.sample_seed is not None and (
            not isinstance(self.sample_seed, int) or isinstance(self.sample_seed, bool)
        ):
            raise CorpusFormatError("sample_seed must be an integer or null")
        if not isinstance(self.negative_control, bool):
            raise CorpusFormatError("negative_control must be boolean")
        if self.annotation_reason is not None and (
            not isinstance(self.annotation_reason, str)
            or _REASON_RE.fullmatch(self.annotation_reason) is None
        ):
            raise CorpusFormatError("annotation_reason must be a safe reason code")

        if self.issue_type is None:
            if not self.negative_control:
                raise CorpusFormatError("a sample without issue_type must be a negative control")
            if self.fact_label not in (None, FactLabel.NO_ISSUE):
                raise CorpusFormatError("negative controls may only use NO_ISSUE")
            if self.business_disposition_label is not None:
                raise CorpusFormatError("negative controls cannot have business disposition")
        elif self.fact_label is FactLabel.NO_ISSUE:
            raise CorpusFormatError("issue samples cannot use NO_ISSUE")

        if self.business_disposition_label is not None and self.fact_label is None:
            raise CorpusFormatError("business disposition requires a fact label")
        if self.fact_label is not None and self.annotation_reason is None:
            raise CorpusFormatError("labeled samples require annotation_reason")

    def to_dict(self) -> dict[str, object]:
        normalized_issue_type = _normalized_issue_type(self.issue_type)
        normalized_fact_label = _normalized_fact_label(self.fact_label)
        normalized_business_label = _normalized_business_label(
            self.business_disposition_label
        )
        return {
            "sample_id": self.sample_id,
            "issue_type": normalized_issue_type.value if normalized_issue_type is not None else None,
            "label": normalized_fact_label.value if normalized_fact_label is not None else None,
            "business_disposition_label": (
                normalized_business_label.value
                if normalized_business_label is not None
                else None
            ),
            "fingerprint": self.fingerprint,
            "evidence_summary": dict(self.evidence_summary),
            "annotation_reason": self.annotation_reason,
            "source_kind": self.source_kind,
            "corpus_version": self.corpus_version,
            "sample_seed": self.sample_seed,
            "sampling_group": self.sampling_group,
            "negative_control": self.negative_control,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "CorpusSample":
        expected = {
            "sample_id",
            "issue_type",
            "label",
            "business_disposition_label",
            "fingerprint",
            "evidence_summary",
            "annotation_reason",
            "source_kind",
            "corpus_version",
            "sample_seed",
            "sampling_group",
            "negative_control",
        }
        _require_exact_fields(value, expected, "corpus sample")
        return cls(
            sample_id=value["sample_id"],  # type: ignore[arg-type]
            issue_type=value["issue_type"],  # type: ignore[arg-type]
            fact_label=value["label"],  # type: ignore[arg-type]
            business_disposition_label=value["business_disposition_label"],  # type: ignore[arg-type]
            fingerprint=value["fingerprint"],  # type: ignore[arg-type]
            evidence_summary=value["evidence_summary"],  # type: ignore[arg-type]
            annotation_reason=value["annotation_reason"],  # type: ignore[arg-type]
            source_kind=value["source_kind"],  # type: ignore[arg-type]
            corpus_version=value["corpus_version"],  # type: ignore[arg-type]
            sample_seed=value["sample_seed"],  # type: ignore[arg-type]
            sampling_group=value["sampling_group"],  # type: ignore[arg-type]
            negative_control=value["negative_control"],  # type: ignore[arg-type]
        )


def _coerce_fact_label(value: FactLabel | str | None) -> FactLabel | None:
    if value is None:
        return None
    try:
        return FactLabel(value)
    except (TypeError, ValueError):
        raise CorpusFormatError("invalid fact label") from None


def _coerce_business_label(
    value: BusinessDispositionLabel | str | None,
) -> BusinessDispositionLabel | None:
    if value is None:
        return None
    try:
        return BusinessDispositionLabel(value)
    except (TypeError, ValueError):
        raise CorpusFormatError("invalid business disposition label") from None


def _require_exact_fields(value: Mapping[str, object], expected: set[str], name: str) -> None:
    if not isinstance(value, Mapping):
        raise CorpusFormatError(f"{name} must be an object")
    actual = set(value)
    if actual != expected:
        raise CorpusFormatError(f"{name} fields do not match the versioned schema")


def _validate_non_negative_int(value: int, *, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _candidate_rank(
    candidate: AuditCandidate,
    *,
    seed: int,
    sampling_group: str,
    corpus_version: str,
) -> str:
    normalized_issue_type = _normalized_issue_type(candidate.issue_type)
    issue_type = (
        normalized_issue_type.value
        if normalized_issue_type is not None
        else "__NEGATIVE_CONTROL__"
    )
    material = "\0".join(
        (
            CORPUS_FORMAT,
            corpus_version,
            sampling_group,
            str(seed),
            issue_type,
            candidate.fingerprint,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def compute_sample_id(
    candidate: AuditCandidate,
    *,
    seed: int,
    sampling_group: str,
    corpus_version: str,
) -> str:
    """计算与 candidate 内容、seed 和抽样分组绑定的稳定 sample identity。"""

    rank = _candidate_rank(
        candidate,
        seed=seed,
        sampling_group=sampling_group,
        corpus_version=corpus_version,
    )
    return f"{_SAMPLE_ID_PREFIX}{rank[:32]}"


def _sample_candidate(
    candidate: AuditCandidate,
    *,
    seed: int,
    sampling_group: str,
    corpus_version: str,
) -> CorpusSample:
    return CorpusSample(
        sample_id=compute_sample_id(
            candidate,
            seed=seed,
            sampling_group=sampling_group,
            corpus_version=corpus_version,
        ),
        issue_type=candidate.issue_type,
        fact_label=None,
        business_disposition_label=None,
        fingerprint=candidate.fingerprint,
        evidence_summary=candidate.evidence_summary,
        annotation_reason=None,
        source_kind=candidate.source_kind,
        corpus_version=corpus_version,
        sample_seed=seed,
        sampling_group=sampling_group,
        negative_control=candidate.negative_control,
    )


def sample_candidates(
    candidates: Iterable[AuditCandidate],
    *,
    seed: int,
    per_issue_type: int,
    negative_control_count: int = 0,
    sampling_group: str = "default",
    corpus_version: str = CORPUS_VERSION,
) -> tuple[CorpusSample, ...]:
    """按 IssueType 分层、固定 seed、无放回地生成可 replay 的样本。"""

    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("seed must be an integer")
    _validate_non_negative_int(per_issue_type, field_name="per_issue_type")
    _validate_non_negative_int(
        negative_control_count,
        field_name="negative_control_count",
    )
    _require_token(sampling_group, field_name="sampling_group")
    _require_token(corpus_version, field_name="corpus_version")

    normalized = tuple(
        item if isinstance(item, AuditCandidate) else AuditCandidate.from_dict(item)
        for item in candidates
    )
    _validate_candidate_uniqueness(normalized)

    by_type: dict[IssueType, list[AuditCandidate]] = {issue_type: [] for issue_type in IssueType}
    controls: list[AuditCandidate] = []
    for candidate in normalized:
        candidate_issue_type = _normalized_issue_type(candidate.issue_type)
        if candidate_issue_type is None:
            controls.append(candidate)
        else:
            by_type[candidate_issue_type].append(candidate)

    selected: list[AuditCandidate] = []
    for issue_type in sorted(IssueType, key=lambda item: item.value):
        selected.extend(
            sorted(
                by_type[issue_type],
                key=lambda item: _candidate_rank(
                    item,
                    seed=seed,
                    sampling_group=sampling_group,
                    corpus_version=corpus_version,
                ),
            )[:per_issue_type]
        )
    selected.extend(
        sorted(
            controls,
            key=lambda item: _candidate_rank(
                item,
                seed=seed,
                sampling_group=sampling_group,
                corpus_version=corpus_version,
            ),
        )[:negative_control_count]
    )

    samples = tuple(
        _sample_candidate(
            candidate,
            seed=seed,
            sampling_group=sampling_group,
            corpus_version=corpus_version,
        )
        for candidate in selected
    )
    validate_corpus(samples)
    return samples


def _validate_candidate_uniqueness(candidates: Iterable[AuditCandidate]) -> None:
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.fingerprint in seen:
            raise DuplicateSampleError("duplicate candidate fingerprint")
        seen.add(candidate.fingerprint)


def validate_corpus(
    samples: Iterable[CorpusSample],
    *,
    require_labels: bool = False,
) -> tuple[CorpusSample, ...]:
    """校验 schema、IssueType、label 语义、重复 identity 和 replay identity。"""

    normalized = tuple(
        item if isinstance(item, CorpusSample) else CorpusSample.from_dict(item)
        for item in samples
    )
    sample_ids: set[str] = set()
    fingerprints: set[str] = set()
    versions: set[str] = set()
    for sample in normalized:
        if sample.sample_id in sample_ids:
            raise DuplicateSampleError("duplicate sample_id")
        if sample.fingerprint in fingerprints:
            raise DuplicateSampleError("duplicate sample fingerprint")
        sample_ids.add(sample.sample_id)
        fingerprints.add(sample.fingerprint)
        versions.add(sample.corpus_version)

        if (sample.sample_seed is None) != (sample.sampling_group is None):
            raise CorpusFormatError("sample_seed and sampling_group must be set together")
        if sample.sample_seed is not None and sample.sampling_group is not None:
            candidate = AuditCandidate(
                fingerprint=sample.fingerprint,
                issue_type=sample.issue_type,
                evidence_summary=sample.evidence_summary,
                source_kind=sample.source_kind,
                corpus_version=sample.corpus_version,
                negative_control=sample.negative_control,
            )
            expected_id = compute_sample_id(
                candidate,
                seed=sample.sample_seed,
                sampling_group=sample.sampling_group,
                corpus_version=sample.corpus_version,
            )
            if sample.sample_id != expected_id:
                raise CorpusFormatError("sample_id does not match deterministic sampling identity")

        if require_labels and sample.fact_label is None:
            raise UnlabeledCorpusError("metrics require every sample to be labeled")
        if require_labels and sample.issue_type is None and sample.fact_label is not FactLabel.NO_ISSUE:
            raise UnlabeledCorpusError("negative controls must be labeled NO_ISSUE")

    if len(versions) > 1:
        raise CorpusFormatError("a corpus cannot mix corpus_version values")
    return normalized


@dataclass(frozen=True, slots=True)
class CorpusMetrics:
    """某个 IssueType 或 overall 的可审计指标。"""

    sample_count: int
    fact_sample_count: int
    true_positive: int
    false_positive: int
    ambiguous: int
    accepted: int
    precision: float | None
    false_positive_rate: float | None
    ambiguous_rate: float | None
    negative_control_count: int
    negative_control_no_issue: int
    negative_control_issue_sample_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "sample_count": self.sample_count,
            "fact_sample_count": self.fact_sample_count,
            "true_positive": self.true_positive,
            "false_positive": self.false_positive,
            "ambiguous": self.ambiguous,
            "accepted": self.accepted,
            "precision": self.precision,
            "false_positive_rate": self.false_positive_rate,
            "ambiguous_rate": self.ambiguous_rate,
            "negative_control_count": self.negative_control_count,
            "negative_control_no_issue": self.negative_control_no_issue,
            "negative_control_issue_sample_count": self.negative_control_issue_sample_count,
        }


@dataclass(frozen=True, slots=True)
class MetricsReport:
    """按 IssueType 和 overall 输出的 corpus metrics。"""

    corpus_version: str
    overall: CorpusMetrics
    by_issue_type: Mapping[IssueType, CorpusMetrics]

    def to_dict(self) -> dict[str, object]:
        return {
            "corpus_format": CORPUS_FORMAT,
            "corpus_version": self.corpus_version,
            "overall": self.overall.to_dict(),
            "by_issue_type": {
                issue_type.value: metrics.to_dict()
                for issue_type, metrics in self.by_issue_type.items()
            },
        }


def _calculate_metrics_for(
    samples: tuple[CorpusSample, ...],
    *,
    sample_count: int | None = None,
) -> CorpusMetrics:
    fact_samples = tuple(sample for sample in samples if sample.issue_type is not None)
    true_positive = sum(sample.fact_label is FactLabel.TRUE_POSITIVE for sample in fact_samples)
    false_positive = sum(sample.fact_label is FactLabel.FALSE_POSITIVE for sample in fact_samples)
    ambiguous = sum(sample.fact_label is FactLabel.AMBIGUOUS for sample in fact_samples)
    accepted = sum(
        sample.business_disposition_label is BusinessDispositionLabel.ACCEPTED
        for sample in samples
    )
    denominator = true_positive + false_positive
    fact_count = true_positive + false_positive + ambiguous
    precision = true_positive / denominator if denominator else None
    false_positive_rate = false_positive / denominator if denominator else None
    ambiguous_rate = ambiguous / fact_count if fact_count else None
    control_samples = tuple(sample for sample in samples if sample.negative_control)
    return CorpusMetrics(
        sample_count=len(samples) if sample_count is None else sample_count,
        fact_sample_count=fact_count,
        true_positive=true_positive,
        false_positive=false_positive,
        ambiguous=ambiguous,
        accepted=accepted,
        precision=precision,
        false_positive_rate=false_positive_rate,
        ambiguous_rate=ambiguous_rate,
        negative_control_count=len(control_samples),
        negative_control_no_issue=sum(
            sample.fact_label is FactLabel.NO_ISSUE for sample in control_samples
        ),
        negative_control_issue_sample_count=sum(
            sample.issue_type is not None for sample in control_samples
        ),
    )


def calculate_metrics(samples: Iterable[CorpusSample]) -> MetricsReport:
    """计算每个 IssueType 与 overall metrics；AMBIGUOUS 不进入 precision 分母。"""

    normalized = validate_corpus(samples, require_labels=True)
    version = next(iter(normalized), None)
    corpus_version = version.corpus_version if version is not None else CORPUS_VERSION
    by_issue_type = {
        issue_type: _calculate_metrics_for(
            tuple(sample for sample in normalized if sample.issue_type is issue_type)
        )
        for issue_type in sorted(IssueType, key=lambda item: item.value)
    }
    return MetricsReport(
        corpus_version=corpus_version,
        overall=_calculate_metrics_for(normalized),
        by_issue_type=by_issue_type,
    )


def _read_jsonl(path: str | Path) -> tuple[dict[str, object], ...]:
    source = Path(path)
    records: list[dict[str, object]] = []
    try:
        handle = source.open("r", encoding="utf-8")
    except OSError:
        raise
    with handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise CorpusFormatError(f"invalid JSONL at line {line_number}") from exc
            if not isinstance(value, dict):
                raise CorpusFormatError(f"JSONL record at line {line_number} must be an object")
            records.append(value)
    return tuple(records)


def read_candidate_manifest(path: str | Path) -> tuple[AuditCandidate, ...]:
    return tuple(AuditCandidate.from_dict(record) for record in _read_jsonl(path))


def read_corpus(path: str | Path, *, require_labels: bool = False) -> tuple[CorpusSample, ...]:
    return validate_corpus(
        (CorpusSample.from_dict(record) for record in _read_jsonl(path)),
        require_labels=require_labels,
    )


def _write_jsonl(path: str | Path, records: Iterable[Mapping[str, object]]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")


def write_candidate_manifest(path: str | Path, candidates: Iterable[AuditCandidate]) -> None:
    normalized = tuple(candidates)
    _validate_candidate_uniqueness(normalized)
    _write_jsonl(path, (candidate.to_dict() for candidate in normalized))


def write_corpus(
    path: str | Path,
    samples: Iterable[CorpusSample],
    *,
    require_labels: bool = False,
) -> None:
    normalized = validate_corpus(samples, require_labels=require_labels)
    _write_jsonl(path, (sample.to_dict() for sample in normalized))


__all__ = [
    "AuditCandidate",
    "BusinessDispositionLabel",
    "CORPUS_FORMAT",
    "CORPUS_VERSION",
    "CorpusFormatError",
    "CorpusMetrics",
    "CorpusSample",
    "DuplicateSampleError",
    "FactLabel",
    "MetricsReport",
    "UnknownIssueTypeError",
    "UnlabeledCorpusError",
    "calculate_metrics",
    "candidate_from_fact",
    "candidate_from_issue",
    "compute_golden_fingerprint",
    "compute_sample_id",
    "negative_control_candidate",
    "read_candidate_manifest",
    "read_corpus",
    "sample_candidates",
    "summarize_issue_evidence",
    "validate_corpus",
    "write_candidate_manifest",
    "write_corpus",
]
