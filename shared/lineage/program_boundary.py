"""Program Boundary Dependency projection for reconciliation.

The persisted business edge table remains a direct business-lineage fact.  This
module only projects the direct facts of one authoritative ``005`` logical
Program group into the dependency shape exposed by a Program Result:
``external input -> authoritative result``.

No table-name convention participates in this projection.  Program grouping is
based on the existing ``parse_program_name`` contract and edge provenance, not
on TMP/TEMP/STG/TEST names or graph-wide dataset joins.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from shared.lineage.domain import (
    is_business_asset,
    normalize_lineage_comparison_table_key,
    parse_program_name,
)


_PROGRAM_BOUNDARY_NO_AUTHORITY: Final[str] = "NO_AUTHORITATIVE_PROGRAM"
_PROGRAM_BOUNDARY_AMBIGUOUS_PROGRAM: Final[str] = "AMBIGUOUS_PROGRAM_IDENTITY"
_PROGRAM_BOUNDARY_DUPLICATE_STEP: Final[str] = "DUPLICATE_PROGRAM_STEP"
_PROGRAM_BOUNDARY_CYCLE: Final[str] = "PROGRAM_CYCLE"
_PROGRAM_BOUNDARY_RESULT_NOT_OBSERVED: Final[str] = "PROGRAM_RESULT_NOT_OBSERVED"
_PROGRAM_BOUNDARY_RESULT_USED_AS_SOURCE: Final[str] = "PROGRAM_RESULT_USED_AS_SOURCE"
_PROGRAM_BOUNDARY_EDGE_PROVENANCE_MISMATCH: Final[str] = (
    "PROGRAM_EDGE_PROVENANCE_MISMATCH"
)
_PROGRAM_BOUNDARY_EDGE_IDENTITY_INVALID: Final[str] = "EDGE_IDENTITY_INVALID"


@dataclass(frozen=True, slots=True)
class ProgramBoundaryProgram:
    """One active Program inventory row used for group resolution."""

    environment: str
    source_profile: str
    program_key: str
    program_name: str

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.environment, "environment"),
            (self.source_profile, "source_profile"),
            (self.program_key, "program_key"),
            (self.program_name, "program_name"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

    @property
    def semantics(self):
        """Return the existing canonical 005 parser result."""

        return parse_program_name(self.program_name)

    @property
    def logical_target(self) -> str | None:
        return self.semantics.logical_target

    @property
    def step_seq(self) -> int | None:
        return self.semantics.step_seq

    @property
    def is_authoritative(self) -> bool:
        return self.logical_target is not None and self.step_seq is not None


@dataclass(frozen=True, slots=True)
class ProgramBoundaryBusinessEdge:
    """Minimal direct business fact needed by the boundary projection."""

    environment: str
    source_profile: str
    program_key: str
    program_name: str
    source_table: str
    target_table: str

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.environment, "environment"),
            (self.source_profile, "source_profile"),
            (self.program_key, "program_key"),
            (self.program_name, "program_name"),
            (self.source_table, "source_table"),
            (self.target_table, "target_table"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ProgramBoundaryDependency:
    """One projected comparison fact from an external input to a result."""

    source_table: str
    target_table: str
    fact_count: int
    provenance_count: int

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.source_table, "source_table"),
            (self.target_table, "target_table"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        for value, field_name in (
            (self.fact_count, "fact_count"),
            (self.provenance_count, "provenance_count"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        if self.provenance_count > self.fact_count:
            raise ValueError("provenance_count cannot exceed fact_count")


@dataclass(frozen=True, slots=True)
class ProgramBoundaryProjection:
    """Projection result for one requested Program Result.

    ``used_direct_fallback`` is deliberately diagnostic metadata.  It does not
    introduce a reconciliation status; fallback facts remain ordinary SQL
    facts so uncertain input stays visible instead of being silently removed.
    """

    target_table: str
    dependencies: tuple[ProgramBoundaryDependency, ...]
    program_keys: tuple[str, ...] = ()
    used_direct_fallback: bool = False
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.target_table, str) or not self.target_table.strip():
            raise ValueError("target_table must be a non-empty string")
        dependencies = tuple(self.dependencies)
        if any(
            not isinstance(value, ProgramBoundaryDependency) for value in dependencies
        ):
            raise TypeError(
                "dependencies must contain ProgramBoundaryDependency values"
            )
        object.__setattr__(self, "dependencies", dependencies)
        keys = tuple(self.program_keys)
        if any(not isinstance(value, str) or not value.strip() for value in keys):
            raise ValueError("program_keys must contain non-empty strings")
        object.__setattr__(self, "program_keys", tuple(sorted(set(keys))))
        diagnostics = tuple(self.diagnostics)
        if any(
            not isinstance(value, str) or not value.strip() for value in diagnostics
        ):
            raise ValueError("diagnostics must contain non-empty strings")
        object.__setattr__(self, "diagnostics", tuple(sorted(set(diagnostics))))


@dataclass(frozen=True, slots=True)
class _NormalizedEdge:
    edge: ProgramBoundaryBusinessEdge
    source_table: str
    target_table: str


def build_program_boundary_projections(
    programs: Iterable[ProgramBoundaryProgram],
    edges: Iterable[ProgramBoundaryBusinessEdge],
    *,
    target_tables: Iterable[object] | str | None = None,
) -> tuple[ProgramBoundaryProjection, ...]:
    """Build bounded Program Boundary projections for requested targets.

    The caller is expected to provide only the active scope and the edge rows
    selected by the DWS adapter.  For a requested target, the adapter may send
    all rows for candidate Program keys plus direct rows targeting that target;
    this function keeps Program groups isolated by ``program_key``.

    Valid groups use ``sources - targets`` to derive external inputs.  Cycles,
    duplicate step numbers, malformed authority, missing authoritative result
    edges, and provenance inconsistencies use a conservative direct-target
    fallback.  This retains visible SQL evidence and never guesses a new
    Program identity.
    """

    program_values = tuple(programs)
    edge_values = tuple(edges)
    normalized_edges, edge_diagnostics = _normalize_edges(edge_values)
    requested_targets = _resolve_targets(
        target_tables,
        programs=program_values,
        edges=normalized_edges,
    )
    programs_by_target: dict[str, list[ProgramBoundaryProgram]] = defaultdict(list)
    programs_by_key: dict[str, ProgramBoundaryProgram] = {}
    ambiguous_keys: set[str] = set()
    for program in program_values:
        if not program.is_authoritative or program.logical_target is None:
            continue
        target = _normalize_table(program.logical_target)
        if target is None:
            continue
        previous = programs_by_key.get(program.program_key)
        if previous is not None and previous.program_name != program.program_name:
            ambiguous_keys.add(program.program_key)
            continue
        programs_by_key[program.program_key] = program
        programs_by_target[target].append(program)

    results: list[ProgramBoundaryProjection] = []
    for target in requested_targets:
        target_edges = tuple(
            item for item in normalized_edges if item.target_table == target
        )
        target_programs = tuple(programs_by_target.get(target, ()))
        diagnostics = set(edge_diagnostics)
        if not target_programs:
            results.append(
                _direct_fallback_projection(
                    target,
                    target_edges,
                    diagnostics={
                        *_diagnostics(diagnostics),
                        _PROGRAM_BOUNDARY_NO_AUTHORITY,
                    },
                )
            )
            continue

        program_keys = tuple(program.program_key for program in target_programs)
        group_edges = tuple(
            item for item in normalized_edges if item.edge.program_key in program_keys
        )
        diagnostics.update(
            _group_diagnostics(
                target,
                target_programs,
                group_edges,
                programs_by_key=programs_by_key,
                ambiguous_keys=ambiguous_keys,
            )
        )
        if diagnostics:
            results.append(
                _direct_fallback_projection(
                    target,
                    target_edges,
                    program_keys=program_keys,
                    diagnostics=diagnostics,
                )
            )
            continue

        projected = _project_valid_group(
            target,
            target_programs,
            group_edges,
            target_edges,
        )
        results.append(projected)

    return tuple(results)


# A descriptive alias for callers that use the noun-first domain vocabulary.
project_program_boundary_dependencies = build_program_boundary_projections


def _resolve_targets(
    target_tables: Iterable[object] | str | None,
    *,
    programs: tuple[ProgramBoundaryProgram, ...],
    edges: tuple[_NormalizedEdge, ...],
) -> tuple[str, ...]:
    if target_tables is not None:
        values = (
            (target_tables,) if isinstance(target_tables, str) else tuple(target_tables)
        )
        if not values:
            raise ValueError("target_tables must contain at least one table")
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            target = _normalize_table(value)
            if target is None:
                raise ValueError("target_tables must contain qualified tables")
            if target not in seen:
                seen.add(target)
                normalized.append(target)
        return tuple(normalized)

    values = {
        item.target_table for item in edges if is_business_asset(item.target_table)
    }
    values.update(
        target
        for program in programs
        if program.is_authoritative
        for target in (_normalize_table(program.logical_target),)
        if target is not None and is_business_asset(target)
    )
    return tuple(sorted(values))


def _normalize_edges(
    edges: tuple[ProgramBoundaryBusinessEdge, ...],
) -> tuple[tuple[_NormalizedEdge, ...], tuple[str, ...]]:
    normalized: list[_NormalizedEdge] = []
    diagnostics: set[str] = set()
    for edge in edges:
        if not isinstance(edge, ProgramBoundaryBusinessEdge):
            raise TypeError("edges must contain ProgramBoundaryBusinessEdge values")
        source = _normalize_table(edge.source_table)
        target = _normalize_table(edge.target_table)
        if source is None or target is None:
            diagnostics.add(_PROGRAM_BOUNDARY_EDGE_IDENTITY_INVALID)
            continue
        if not is_business_asset(source) or not is_business_asset(target):
            # Reuse the formal DLO/DWO boundary; never create a bypass edge.
            continue
        normalized.append(
            _NormalizedEdge(edge=edge, source_table=source, target_table=target)
        )
    return tuple(normalized), tuple(sorted(diagnostics))


def _group_diagnostics(
    target: str,
    programs: tuple[ProgramBoundaryProgram, ...],
    group_edges: tuple[_NormalizedEdge, ...],
    *,
    programs_by_key: dict[str, ProgramBoundaryProgram],
    ambiguous_keys: set[str],
) -> set[str]:
    diagnostics: set[str] = set()
    if len({program.program_key for program in programs}) != len(programs):
        diagnostics.add(_PROGRAM_BOUNDARY_AMBIGUOUS_PROGRAM)
    if any(program.program_key in ambiguous_keys for program in programs):
        diagnostics.add(_PROGRAM_BOUNDARY_AMBIGUOUS_PROGRAM)

    steps = [program.step_seq for program in programs]
    if any(step is None for step in steps) or len(set(steps)) != len(steps):
        diagnostics.add(_PROGRAM_BOUNDARY_DUPLICATE_STEP)

    expected_names = {program.program_key: program.program_name for program in programs}
    for item in group_edges:
        expected_name = expected_names.get(item.edge.program_key)
        if expected_name is None or expected_name != item.edge.program_name:
            diagnostics.add(_PROGRAM_BOUNDARY_EDGE_PROVENANCE_MISMATCH)
            continue
        semantics = parse_program_name(item.edge.program_name)
        if (
            semantics.logical_target is None
            or _normalize_table(semantics.logical_target) != target
        ):
            diagnostics.add(_PROGRAM_BOUNDARY_EDGE_PROVENANCE_MISMATCH)
        if item.edge.program_key not in programs_by_key:
            diagnostics.add(_PROGRAM_BOUNDARY_EDGE_PROVENANCE_MISMATCH)

    if not group_edges:
        diagnostics.add(_PROGRAM_BOUNDARY_RESULT_NOT_OBSERVED)
        return diagnostics
    if not any(item.target_table == target for item in group_edges):
        diagnostics.add(_PROGRAM_BOUNDARY_RESULT_NOT_OBSERVED)
    if any(
        item.source_table == target and item.target_table != target
        for item in group_edges
    ):
        # Do not turn a result read in a later step into a fabricated
        # ``RESULT -> RESULT`` configured dependency.  Fall back to the
        # persisted direct target evidence and keep the diagnostic visible.
        diagnostics.add(_PROGRAM_BOUNDARY_RESULT_USED_AS_SOURCE)
    if _contains_cycle(group_edges):
        diagnostics.add(_PROGRAM_BOUNDARY_CYCLE)
    return diagnostics


def _project_valid_group(
    target: str,
    programs: tuple[ProgramBoundaryProgram, ...],
    group_edges: tuple[_NormalizedEdge, ...],
    direct_target_edges: tuple[_NormalizedEdge, ...],
) -> ProgramBoundaryProjection:
    internal_targets = {item.target_table for item in group_edges}
    external_sources = {
        item.source_table
        for item in group_edges
        if item.source_table not in internal_targets and item.source_table != target
    }
    contributions: list[tuple[str, str, str]] = []
    for item in group_edges:
        if item.source_table in external_sources:
            contributions.append((item.source_table, target, item.edge.program_key))

    # Direct facts from another Program are not allowed to disappear merely
    # because one valid group exists for the target.
    group_keys = {program.program_key for program in programs}
    for item in direct_target_edges:
        if item.edge.program_key not in group_keys:
            contributions.append((item.source_table, target, item.edge.program_key))

    contributions.extend(
        (item.source_table, target, item.edge.program_key)
        for item in group_edges
        if item.source_table == target and item.target_table == target
    )
    return ProgramBoundaryProjection(
        target_table=target,
        dependencies=_dependencies_from_contributions(contributions),
        program_keys=tuple(program.program_key for program in programs),
        diagnostics=(),
    )


def _direct_fallback_projection(
    target: str,
    direct_target_edges: tuple[_NormalizedEdge, ...],
    *,
    program_keys: Iterable[str] = (),
    diagnostics: Iterable[str],
) -> ProgramBoundaryProjection:
    contributions = [
        (item.source_table, target, item.edge.program_key)
        for item in direct_target_edges
    ]
    return ProgramBoundaryProjection(
        target_table=target,
        dependencies=_dependencies_from_contributions(contributions),
        program_keys=tuple(program_keys),
        used_direct_fallback=True,
        diagnostics=tuple(diagnostics),
    )


def _dependencies_from_contributions(
    contributions: Iterable[tuple[str, str, str]],
) -> tuple[ProgramBoundaryDependency, ...]:
    values: dict[tuple[str, str], list[str]] = defaultdict(list)
    for source, target, program_key in contributions:
        values[(source, target)].append(program_key)
    return tuple(
        ProgramBoundaryDependency(
            source_table=source,
            target_table=target,
            fact_count=len(program_keys),
            provenance_count=len(set(program_keys)),
        )
        for (source, target), program_keys in sorted(values.items())
    )


def _contains_cycle(edges: Iterable[_NormalizedEdge]) -> bool:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for item in edges:
        if item.source_table != item.target_table:
            adjacency[item.source_table].add(item.target_table)
    colors: dict[str, int] = {}

    def visit(node: str) -> bool:
        colors[node] = 1
        for child in adjacency.get(node, ()):
            color = colors.get(child, 0)
            if color == 1 or (color == 0 and visit(child)):
                return True
        colors[node] = 2
        return False

    for node in tuple(adjacency):
        if colors.get(node, 0) == 0 and visit(node):
            return True
    return False


def _normalize_table(value: object) -> str | None:
    try:
        return normalize_lineage_comparison_table_key(value)
    except (TypeError, ValueError):
        return None


def _diagnostics(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


__all__ = [
    "ProgramBoundaryBusinessEdge",
    "ProgramBoundaryDependency",
    "ProgramBoundaryProgram",
    "ProgramBoundaryProjection",
    "build_program_boundary_projections",
    "project_program_boundary_dependencies",
]
