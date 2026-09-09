"""Sanitized dialect evidence classification and report rendering for Issue #43.

The helper accepts fixed labels and aggregate counts only.  It deliberately does
not parse SQL, inspect source code, or infer a dialect gap from a generic parser
failure.  It is a research/reporting utility and is not imported by production
lineage code.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping


class FailureCategory(str, Enum):
    """The bounded failure categories required by the research issue."""

    PYTHON_WRAPPER = "PYTHON_WRAPPER"
    DYNAMIC_SQL = "DYNAMIC_SQL"
    TARGET_AUTHORITY = "TARGET_AUTHORITY"
    PROGRAM_NAME = "PROGRAM_NAME"
    PARSER_BUG = "PARSER_BUG"
    DIALECT_SYNTAX = "DIALECT_SYNTAX"
    UNKNOWN = "UNKNOWN"


class DialectFeature(str, Enum):
    """The feature rows in the cross-profile evidence matrix."""

    CTE = "CTE"
    MERGE = "MERGE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INSERT = "INSERT"
    IDENTIFIER_QUOTING = "IDENTIFIER_QUOTING"
    FUNCTION_SYNTAX = "FUNCTION_SYNTAX"
    DATE_FUNCTIONS = "DATE_FUNCTIONS"
    PARTITION_SYNTAX = "PARTITION_SYNTAX"
    WAREHOUSE_HINTS = "WAREHOUSE_HINTS"
    SUBQUERY = "SUBQUERY"
    ALIAS = "ALIAS"


class EvidenceState(str, Enum):
    """State of one feature in one sanitized profile."""

    OBSERVED = "OBSERVED"
    SUPPORTED = "SUPPORTED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class Decision(str, Enum):
    """Decision framework from Issue #43."""

    NOT_NEEDED = "NOT_NEEDED"
    MINIMAL_HOOK_ONLY = "MINIMAL_HOOK_ONLY"
    ABSTRACTION_REQUIRED = "ABSTRACTION_REQUIRED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class ReplayStatus(str, Enum):
    """Sanitized replay outcome; it says nothing about feature coverage."""

    PASS = "PASS"
    UNKNOWN = "UNKNOWN"


PROFILE_LABELS = ("profile_A", "profile_B", "profile_C", "profile_D")
AGGREGATE_SCOPE = "aggregate"

# These mappings are intentionally conservative.  Generic parse failures and
# UNSUPPORTED_STATEMENT do not become DIALECT_SYNTAX without an explicit,
# reviewer-confirmed dialect label.
_REASON_CATEGORY: dict[str, FailureCategory] = {
    "SQL_CALL_NOT_RECOGNIZED": FailureCategory.PYTHON_WRAPPER,
    "SQL_ARGUMENT_DYNAMIC": FailureCategory.DYNAMIC_SQL,
    "SQL_RETURN_DYNAMIC": FailureCategory.DYNAMIC_SQL,
    "SQL_ARGUMENT_MISSING": FailureCategory.PYTHON_WRAPPER,
    "SQL_ARGUMENT_NOT_SQL": FailureCategory.PYTHON_WRAPPER,
    "SQL_RETURN_NOT_SQL": FailureCategory.PYTHON_WRAPPER,
    "WRITE_TARGET_NOT_FOUND": FailureCategory.TARGET_AUTHORITY,
    "TARGET_WITHOUT_SOURCE": FailureCategory.TARGET_AUTHORITY,
    "PROGRAM_NAME_TARGET_UNRESOLVED": FailureCategory.PROGRAM_NAME,
    "PROGRAM_NAME_STEP_INVALID": FailureCategory.PROGRAM_NAME,
    "PARSER_BUG_CONFIRMED": FailureCategory.PARSER_BUG,
    "DIALECT_SYNTAX_GAP_CONFIRMED": FailureCategory.DIALECT_SYNTAX,
}


@dataclass(frozen=True, slots=True)
class DialectObservation:
    """One reviewer-confirmed, aggregate-only feature observation."""

    profile: str
    feature: DialectFeature
    state: EvidenceState
    count: int | None = None

    def __post_init__(self) -> None:
        _validate_profile(self.profile)
        if self.state is EvidenceState.UNKNOWN:
            if self.count is not None:
                raise ValueError("UNKNOWN observations cannot carry a count")
            return
        if self.count is None or self.count < 1:
            raise ValueError("known observations require a positive count")


@dataclass(frozen=True, slots=True)
class FailureObservation:
    """One sanitized reason/count pair, optionally scoped to a profile."""

    scope: str
    reason: str
    count: int | None

    def __post_init__(self) -> None:
        if self.scope != AGGREGATE_SCOPE:
            _validate_profile(self.scope)
        if self.count is not None and self.count < 0:
            raise ValueError("failure count cannot be negative")

    @property
    def category(self) -> FailureCategory:
        return classify_failure_reason(self.reason)


@dataclass(frozen=True, slots=True)
class ProfileDialectEvidence:
    """Sanitized matrix and replay summary for one public profile label."""

    profile: str
    program_count: int
    replay_status: ReplayStatus
    observations: tuple[DialectObservation, ...]

    def __post_init__(self) -> None:
        _validate_profile(self.profile)
        if self.program_count < 0:
            raise ValueError("program count cannot be negative")
        expected_features = tuple(DialectFeature)
        actual_features = tuple(item.feature for item in self.observations)
        if actual_features != expected_features:
            raise ValueError("profile observations must contain each feature once")
        if any(item.profile != self.profile for item in self.observations):
            raise ValueError("profile observation belongs to another profile")

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "program_count": self.program_count,
            "replay_status": self.replay_status.value,
            "features": [
                {
                    "feature": item.feature.value,
                    "state": item.state.value,
                    "count": item.count,
                }
                for item in self.observations
            ],
        }


@dataclass(frozen=True, slots=True)
class DialectEvidenceReport:
    """Machine-readable, privacy-safe research report."""

    decision: Decision
    profiles: tuple[ProfileDialectEvidence, ...]
    failure_counts: dict[str, dict[str, int | None]]

    def __post_init__(self) -> None:
        actual_profiles = tuple(item.profile for item in self.profiles)
        if actual_profiles != PROFILE_LABELS:
            raise ValueError("report must contain profile_A through profile_D in order")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "decision": self.decision.value,
            "profiles": [profile.to_dict() for profile in self.profiles],
            "failure_counts": self.failure_counts,
        }


def classify_failure_reason(reason: str) -> FailureCategory:
    """Classify a fixed sanitized reason without inspecting SQL or source text.

    ``PYTHON_PARSE_FAILED`` and ``UNSUPPORTED_STATEMENT`` intentionally return
    ``UNKNOWN``: either can have several causes, and neither proves a dialect
    gap.  A dialect label is only accepted when the replay reviewer explicitly
    records ``DIALECT_SYNTAX_GAP_CONFIRMED``.
    """

    return _REASON_CATEGORY.get(reason, FailureCategory.UNKNOWN)


def build_report(
    *,
    program_counts: Mapping[str, int],
    replay_statuses: Mapping[str, ReplayStatus],
    observations: Iterable[DialectObservation] = (),
    failures: Iterable[FailureObservation] = (),
    decision: Decision = Decision.INSUFFICIENT_EVIDENCE,
) -> DialectEvidenceReport:
    """Build a four-profile report from sanitized observations only."""

    if tuple(program_counts) != PROFILE_LABELS:
        raise ValueError("program_counts must contain profile_A through profile_D")
    if tuple(replay_statuses) != PROFILE_LABELS:
        raise ValueError("replay_statuses must contain profile_A through profile_D")

    by_profile_feature: dict[tuple[str, DialectFeature], DialectObservation] = {}
    for observation in observations:
        key = (observation.profile, observation.feature)
        if key in by_profile_feature:
            raise ValueError("duplicate profile/feature observation")
        by_profile_feature[key] = observation

    profiles: list[ProfileDialectEvidence] = []
    for profile in PROFILE_LABELS:
        profile_observations = tuple(
            by_profile_feature.get(
                (profile, feature),
                DialectObservation(
                    profile=profile,
                    feature=feature,
                    state=EvidenceState.UNKNOWN,
                ),
            )
            for feature in DialectFeature
        )
        profiles.append(
            ProfileDialectEvidence(
                profile=profile,
                program_count=program_counts[profile],
                replay_status=replay_statuses[profile],
                observations=profile_observations,
            )
        )

    failure_counts = _build_failure_counts(failures)
    return DialectEvidenceReport(
        decision=decision,
        profiles=tuple(profiles),
        failure_counts=failure_counts,
    )


def render_matrix(report: DialectEvidenceReport) -> str:
    """Render only the sanitized matrix and category/count table as Markdown."""

    feature_headers = [feature.value for feature in DialectFeature]
    lines = [
        "| Profile | Programs | Replay | " + " | ".join(feature_headers) + " |",
        "| --- | ---: | --- | " + " | ".join("---" for _ in feature_headers) + " |",
    ]
    for profile in report.profiles:
        states = [item.state.value for item in profile.observations]
        lines.append(
            f"| {profile.profile} | {profile.program_count} | "
            f"{profile.replay_status.value} | "
            + " | ".join(states)
            + " |"
        )

    lines.extend(
        [
            "",
            "| Failure scope | "
            + " | ".join(category.value for category in FailureCategory)
            + " |",
            "| --- | " + " | ".join("---" for _ in FailureCategory) + " |",
        ]
    )
    for scope, counts in report.failure_counts.items():
        lines.append(
            f"| {scope} | "
            + " | ".join(
                "UNKNOWN" if counts[category.value] is None else str(counts[category.value])
                for category in FailureCategory
            )
            + " |"
        )
    return "\n".join(lines)


def _build_failure_counts(
    failures: Iterable[FailureObservation],
) -> dict[str, dict[str, int | None]]:
    by_scope: dict[str, dict[str, int | None]] = {}
    for failure in failures:
        counts = by_scope.setdefault(
            failure.scope,
            {category.value: None for category in FailureCategory},
        )
        category = failure.category.value
        if failure.count is None:
            counts[category] = None
            continue
        previous_count = counts[category]
        if previous_count is None:
            counts[category] = failure.count
        else:
            counts[category] = previous_count + failure.count
    if not by_scope:
        by_scope[AGGREGATE_SCOPE] = {
            category.value: None for category in FailureCategory
        }
    return by_scope


def _validate_profile(profile: str) -> None:
    if profile not in PROFILE_LABELS:
        raise ValueError(f"unknown public profile label: {profile}")


__all__ = [
    "AGGREGATE_SCOPE",
    "Decision",
    "DialectEvidenceReport",
    "DialectFeature",
    "DialectObservation",
    "EvidenceState",
    "FailureCategory",
    "FailureObservation",
    "PROFILE_LABELS",
    "ProfileDialectEvidence",
    "ReplayStatus",
    "build_report",
    "classify_failure_reason",
    "render_matrix",
]
