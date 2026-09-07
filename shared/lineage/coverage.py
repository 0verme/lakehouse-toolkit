"""Lineage parser coverage funnel and sanitized aggregate reports.

This module observes the existing ProgramSource -> Physical DAG -> materialization
pipeline. It never stores or serializes program names, script text, SQL text, asset
names, or provider connection settings.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from .domain import LineageEdge, ProgramIdentity, ProgramSource
from .physical_dag import ProgramPhysicalDAG

DEFAULT_COVERAGE_REPORT_PATH = Path("artifacts/lineage_coverage/report.json")


class CoverageReason(str, Enum):
    """Primary safe reason for a program not producing a physical or lineage edge."""

    CANDIDATE_FOUND = "CANDIDATE_FOUND"
    RAW_SQL = "RAW_SQL"
    EMPTY_SCRIPT = "EMPTY_SCRIPT"
    PYTHON_PARSE_FAILED = "PYTHON_PARSE_FAILED"
    NO_SQL_CANDIDATE = "NO_SQL_CANDIDATE"
    SQL_CALL_NOT_RECOGNIZED = "SQL_CALL_NOT_RECOGNIZED"
    SQL_ARGUMENT_DYNAMIC = "SQL_ARGUMENT_DYNAMIC"
    SQL_ARGUMENT_MISSING = "SQL_ARGUMENT_MISSING"
    SQL_ARGUMENT_NOT_SQL = "SQL_ARGUMENT_NOT_SQL"
    SQL_RETURN_DYNAMIC = "SQL_RETURN_DYNAMIC"
    SQL_RETURN_NOT_SQL = "SQL_RETURN_NOT_SQL"
    NO_SQL_STEP = "NO_SQL_STEP"
    READ_ONLY_SQL = "READ_ONLY_SQL"
    WRITE_TARGET_NOT_FOUND = "WRITE_TARGET_NOT_FOUND"
    TARGET_WITHOUT_SOURCE = "TARGET_WITHOUT_SOURCE"
    UNSUPPORTED_STATEMENT = "UNSUPPORTED_STATEMENT"
    NO_PHYSICAL_EDGE = "NO_PHYSICAL_EDGE"
    NO_LINEAGE_EDGE = "NO_LINEAGE_EDGE"


_FAILURE_REASONS = tuple(
    reason
    for reason in CoverageReason
    if reason
    not in {
        CoverageReason.CANDIDATE_FOUND,
        CoverageReason.RAW_SQL,
        CoverageReason.NO_LINEAGE_EDGE,
    }
)


@dataclass
class _ProfileCoverageAccumulator:
    environment: str
    source_profile: str
    total_programs: int = 0
    parsed_programs: int = 0
    programs_with_sql_candidates: int = 0
    programs_with_sql_steps: int = 0
    programs_with_write_target: int = 0
    programs_with_physical_nodes: int = 0
    programs_with_physical_edges: int = 0
    sql_candidate_count: int = 0
    sql_step_count: int = 0
    write_target_count: int = 0
    physical_node_count: int = 0
    physical_edge_count: int = 0
    lineage_edge_count: int = 0
    lineage_programs: set[ProgramIdentity] = field(default_factory=set)
    physical_edge_programs: set[ProgramIdentity] = field(default_factory=set)
    failure_reasons: Counter[str] = field(default_factory=Counter)
    lineage_failure_reasons: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True, slots=True)
class ProfileCoverage:
    """Safe aggregate counters for one environment/source_profile pair."""

    environment: str
    source_profile: str
    total_programs: int
    parsed_programs: int
    programs_with_sql_candidates: int
    programs_with_sql_steps: int
    programs_with_write_target: int
    programs_with_physical_nodes: int
    programs_with_physical_edges: int
    programs_with_lineage_edges: int
    sql_candidate_count: int
    sql_step_count: int
    write_target_count: int
    physical_node_count: int
    physical_edge_count: int
    lineage_edge_count: int
    failure_reasons: dict[str, int]
    lineage_failure_reasons: dict[str, int]

    @property
    def program_count(self) -> int:
        """Compatibility alias used by the verification artifact style."""

        return self.total_programs

    def ratios(self) -> dict[str, float]:
        denominator = self.total_programs
        return {
            "sql_candidate_program_ratio": _ratio(
                self.programs_with_sql_candidates, denominator
            ),
            "sql_step_program_ratio": _ratio(
                self.programs_with_sql_steps, denominator
            ),
            "write_target_program_ratio": _ratio(
                self.programs_with_write_target, denominator
            ),
            "physical_node_program_ratio": _ratio(
                self.programs_with_physical_nodes, denominator
            ),
            "physical_edge_program_ratio": _ratio(
                self.programs_with_physical_edges, denominator
            ),
            "lineage_edge_program_ratio": _ratio(
                self.programs_with_lineage_edges, denominator
            ),
        }

    def to_dict(self) -> dict[str, object]:
        """Return an aggregate-only JSON-safe whitelist."""

        return {
            "environment": self.environment,
            "source_profile": self.source_profile,
            "program_count": self.program_count,
            "total_programs": self.total_programs,
            "parsed_programs": self.parsed_programs,
            "programs_with_sql_candidates": self.programs_with_sql_candidates,
            "programs_with_sql_steps": self.programs_with_sql_steps,
            "programs_with_write_target": self.programs_with_write_target,
            "programs_with_physical_nodes": self.programs_with_physical_nodes,
            "programs_with_physical_edges": self.programs_with_physical_edges,
            "programs_with_lineage_edges": self.programs_with_lineage_edges,
            "sql_candidate_count": self.sql_candidate_count,
            "sql_step_count": self.sql_step_count,
            "write_target_count": self.write_target_count,
            "physical_node_count": self.physical_node_count,
            "physical_edge_count": self.physical_edge_count,
            "lineage_edge_count": self.lineage_edge_count,
            "ratios": self.ratios(),
            "failure_reasons": dict(self.failure_reasons),
            "lineage_failure_reasons": dict(self.lineage_failure_reasons),
        }


@dataclass(frozen=True, slots=True)
class LineageCoverageReport:
    """Machine-readable coverage report containing counts and enum values only."""

    generated_at: str
    coverage_scope: str
    sample_only: bool
    sample_limit: int | None
    profiles: tuple[ProfileCoverage, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "generated_at": self.generated_at,
            "coverage_scope": self.coverage_scope,
            "sample_only": self.sample_only,
            "sample_limit": self.sample_limit,
            "profiles": [profile.to_dict() for profile in self.profiles],
        }

    def log_lines(self) -> tuple[str, ...]:
        """Render low-cardinality lines safe for normal job logs."""

        lines: list[str] = []
        for profile in self.profiles:
            reasons = _format_reason_counts(profile.failure_reasons)
            lineage_reasons = _format_reason_counts(profile.lineage_failure_reasons)
            lines.append(
                "stage=coverage "
                f"environment={profile.environment} "
                f"source_profile={profile.source_profile} "
                f"programs={profile.total_programs} "
                f"parsed_programs={profile.parsed_programs} "
                f"sql_candidates={profile.sql_candidate_count} "
                f"programs_with_sql_candidates={profile.programs_with_sql_candidates} "
                f"sql_steps={profile.sql_step_count} "
                f"programs_with_sql_steps={profile.programs_with_sql_steps} "
                f"write_targets={profile.write_target_count} "
                f"programs_with_write_target={profile.programs_with_write_target} "
                f"physical_nodes={profile.physical_node_count} "
                f"programs_with_physical_nodes={profile.programs_with_physical_nodes} "
                f"physical_edges={profile.physical_edge_count} "
                f"programs_with_physical_edges={profile.programs_with_physical_edges} "
                f"lineage_edges={profile.lineage_edge_count} "
                f"programs_with_lineage_edges={profile.programs_with_lineage_edges} "
                f"failure_reasons={reasons} "
                f"lineage_failure_reasons={lineage_reasons}"
            )
        return tuple(lines)


class LineageCoverageAccumulator:
    """Single-pass observer for the existing DAG and materialization pipeline."""

    def __init__(self) -> None:
        self._profiles: dict[tuple[str, str], _ProfileCoverageAccumulator] = {}
        self._finalized_lineage_failures = False

    def observe_sources(self, sources: Iterable[ProgramSource]) -> None:
        """Count the current source snapshot without retaining source contents."""

        self._finalized_lineage_failures = False
        for source in sources:
            self._profile(source.environment, source.source_profile).total_programs += 1

    def observe_dag(
        self,
        dag: ProgramPhysicalDAG,
        *,
        count_program: bool = True,
    ) -> None:
        """Accumulate parser and Physical DAG stages from one already-built DAG."""

        if not isinstance(dag, ProgramPhysicalDAG):
            raise TypeError("dag must be a ProgramPhysicalDAG")
        self._finalized_lineage_failures = False
        source = dag.program_source
        profile = self._profile(source.environment, source.source_profile)
        identity = source.identity
        if count_program:
            profile.total_programs += 1
        profile.parsed_programs += 1
        profile.sql_candidate_count += dag.sql_candidate_count
        profile.sql_step_count += len(dag.steps)
        if dag.sql_candidate_count:
            profile.programs_with_sql_candidates += 1
        if dag.steps:
            profile.programs_with_sql_steps += 1
        write_targets = sum(step.target is not None for step in dag.steps)
        if write_targets:
            profile.programs_with_write_target += 1
            profile.write_target_count += write_targets
        if dag.nodes:
            profile.programs_with_physical_nodes += 1
            profile.physical_node_count += len(dag.nodes)
        if dag.edges:
            profile.programs_with_physical_edges += 1
            profile.physical_edge_count += len(dag.edges)
            profile.physical_edge_programs.add(identity)
        else:
            reason = primary_failure_reason(dag)
            profile.failure_reasons[reason.value] += 1

    def observe_materialized_edges(self, edges: Iterable[LineageEdge]) -> None:
        """Count formal edges already produced by materialization."""

        self._finalized_lineage_failures = False
        for edge in edges:
            profile = self._profile(edge.environment, edge.source_profile)
            profile.lineage_edge_count += 1
            if edge.program_name is not None:
                profile.lineage_programs.add(
                    ProgramIdentity(
                        edge.environment,
                        edge.source_profile,
                        edge.program_name,
                    )
                )

    def report(
        self,
        *,
        sample_only: bool = False,
        sample_limit: int | None = None,
        generated_at: str | None = None,
    ) -> LineageCoverageReport:
        """Freeze the current aggregate counters into a safe report."""

        self._finalize_lineage_failures()
        profiles = tuple(
            _snapshot_profile(profile)
            for profile in sorted(
                self._profiles.values(),
                key=lambda item: (item.environment, item.source_profile),
            )
        )
        total_programs = sum(profile.total_programs for profile in profiles)
        parsed_programs = sum(profile.parsed_programs for profile in profiles)
        scope = "ALL_PROGRAMS" if total_programs == parsed_programs else "REBUILT_PROGRAMS"
        return LineageCoverageReport(
            generated_at=generated_at or datetime.now(timezone.utc).isoformat(),
            coverage_scope=scope,
            sample_only=sample_only,
            sample_limit=sample_limit,
            profiles=profiles,
        )

    def _finalize_lineage_failures(self) -> None:
        if self._finalized_lineage_failures:
            return
        for profile in self._profiles.values():
            profile.lineage_failure_reasons.clear()
            missing = profile.physical_edge_programs - profile.lineage_programs
            if missing:
                profile.lineage_failure_reasons[CoverageReason.NO_LINEAGE_EDGE.value] = len(
                    missing
                )
        self._finalized_lineage_failures = True

    def _profile(
        self,
        environment: str,
        source_profile: str,
    ) -> _ProfileCoverageAccumulator:
        key = (environment, source_profile)
        profile = self._profiles.get(key)
        if profile is None:
            profile = _ProfileCoverageAccumulator(environment, source_profile)
            self._profiles[key] = profile
        return profile


def primary_failure_reason(dag: ProgramPhysicalDAG) -> CoverageReason:
    """Classify one zero-PhysicalEdge program without inspecting its source text."""

    if dag.edges:
        raise ValueError("primary_failure_reason requires a DAG without physical edges")
    if dag.sql_candidate_count == 0:
        try:
            return CoverageReason(dag.sql_extraction_reason)
        except ValueError:
            return CoverageReason.NO_SQL_CANDIDATE
    if not dag.steps:
        return CoverageReason.NO_SQL_STEP

    statement_types = {step.statement_type for step in dag.steps}
    has_target = any(step.target is not None for step in dag.steps)
    has_source = any(step.sources for step in dag.steps)
    if not has_target:
        if statement_types == {"select"}:
            return CoverageReason.READ_ONLY_SQL
        if "unknown" in statement_types:
            return CoverageReason.UNSUPPORTED_STATEMENT
        return CoverageReason.WRITE_TARGET_NOT_FOUND
    if not has_source:
        return CoverageReason.TARGET_WITHOUT_SOURCE
    return CoverageReason.NO_PHYSICAL_EDGE


def write_json_report(
    report: LineageCoverageReport,
    output_path: str | Path = DEFAULT_COVERAGE_REPORT_PATH,
) -> Path:
    """Write only the aggregate report under the ignored coverage artifact root."""

    if not isinstance(report, LineageCoverageReport):
        raise TypeError("report must be a LineageCoverageReport")
    path = _safe_report_path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # pi-lens-ignore: python-path-traversal (path is resolved under the report root)
    path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _snapshot_profile(profile: _ProfileCoverageAccumulator) -> ProfileCoverage:
    return ProfileCoverage(
        environment=profile.environment,
        source_profile=profile.source_profile,
        total_programs=profile.total_programs,
        parsed_programs=profile.parsed_programs,
        programs_with_sql_candidates=profile.programs_with_sql_candidates,
        programs_with_sql_steps=profile.programs_with_sql_steps,
        programs_with_write_target=profile.programs_with_write_target,
        programs_with_physical_nodes=profile.programs_with_physical_nodes,
        programs_with_physical_edges=profile.programs_with_physical_edges,
        programs_with_lineage_edges=len(profile.lineage_programs),
        sql_candidate_count=profile.sql_candidate_count,
        sql_step_count=profile.sql_step_count,
        write_target_count=profile.write_target_count,
        physical_node_count=profile.physical_node_count,
        physical_edge_count=profile.physical_edge_count,
        lineage_edge_count=profile.lineage_edge_count,
        failure_reasons={
            reason.value: profile.failure_reasons.get(reason.value, 0)
            for reason in _FAILURE_REASONS
        },
        lineage_failure_reasons={
            CoverageReason.NO_LINEAGE_EDGE.value: profile.lineage_failure_reasons.get(
                CoverageReason.NO_LINEAGE_EDGE.value, 0
            )
        },
    )


def _format_reason_counts(values: dict[str, int]) -> str:
    non_zero = [f"{key}:{count}" for key, count in values.items() if count]
    return ",".join(non_zero) if non_zero else "-"


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 6)


def _safe_report_path(output_path: str | Path) -> Path:
    root = (Path.cwd() / "artifacts" / "lineage_coverage").resolve()
    candidate = Path(output_path)
    resolved = (
        candidate if candidate.is_absolute() else Path.cwd() / candidate
    ).resolve()
    if root not in resolved.parents:
        raise ValueError("report output must stay under artifacts/lineage_coverage")
    return resolved


__all__ = [
    "CoverageReason",
    "DEFAULT_COVERAGE_REPORT_PATH",
    "LineageCoverageAccumulator",
    "LineageCoverageReport",
    "ProfileCoverage",
    "primary_failure_reason",
    "write_json_report",
]
