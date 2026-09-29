"""Dependency-aware row chunking for tabular job datasets.

This module deliberately contains no file-format, database, or UI concerns.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from shared.graph.dependency import find_job_dependency_cycles, parse_job_dependencies


@dataclass(frozen=True)
class SplitDiagnostics:
    """Input and dependency issues discovered while splitting rows."""

    original_data_rows: int
    valid_id_count: int
    empty_id_rows: tuple[int, ...]
    missing_dependencies: tuple[str, ...]
    missing_dependency_references: int
    cycles: tuple[tuple[str, ...], ...]

    @property
    def empty_id_count(self) -> int:
        return len(self.empty_id_rows)

    @property
    def missing_dependency_count(self) -> int:
        return len(self.missing_dependencies)


@dataclass(frozen=True)
class DependencySplitResult:
    """Rows grouped into dependency-complete chunks plus input diagnostics."""

    chunks: list[list[dict[str, Any]]]
    diagnostics: SplitDiagnostics


class DuplicateIdentifierError(ValueError):
    """Raised when multiple source rows claim the same non-empty identifier."""


class DependencyClosureTooLargeError(ValueError):
    """Raised when one row and its complete dependency closure cannot fit."""


def _identifier(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _validate_chunk_dependencies(
    chunks: list[list[dict[str, Any]]],
    row_by_id: dict[str, dict[str, Any]],
    id_column: str,
    dependency_column: str,
) -> None:
    for chunk in chunks:
        chunk_ids = {_identifier(row.get(id_column)) for row in chunk}
        for row in chunk:
            for dependency in parse_job_dependencies(row.get(dependency_column)):
                if dependency in row_by_id and dependency not in chunk_ids:
                    raise RuntimeError(
                        f"拆分结果校验失败：{_identifier(row.get(id_column))!r} "
                        f"依赖的 {dependency!r} 不在同一 chunk 中"
                    )


def _dependency_closure(job_name: str, dep_map: dict[str, list[str]]) -> list[str]:
    """Return known dependencies in dependency-first order without recursion.

    A dependency cycle is treated as a reachable set: each member is emitted
    once, and the requested job is excluded from the returned closure so it can
    be appended exactly once by the caller.
    """

    state = {job_name: 1}
    ordered: list[str] = []
    stack: list[tuple[str, Iterator[str]]] = [
        (job_name, iter(dep_map.get(job_name, [])))
    ]

    while stack:
        current, dependencies = stack[-1]
        try:
            dependency = next(dependencies)
        except StopIteration:
            stack.pop()
            state[current] = 2
            if current != job_name:
                ordered.append(current)
            continue

        if dependency not in dep_map or state.get(dependency, 0) != 0:
            continue

        state[dependency] = 1
        stack.append((dependency, iter(dep_map.get(dependency, []))))

    return ordered


def split_rows_preserving_dependencies(
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    id_column: str,
    dependency_column: str,
    max_rows_per_chunk: int = 500,
) -> DependencySplitResult:
    """Split rows while keeping each row's available dependency closure intact.

    Empty identifiers are excluded from the dependency graph and from all
    output chunks; their 1-based data-row positions are reported in diagnostics.
    Dependencies whose identifiers do not occur in the source rows are not
    fabricated and are reported as missing. Such references do not block a
    split. Chunks may repeat dependency rows to keep each chunk self-contained.
    """

    if max_rows_per_chunk <= 0:
        raise ValueError("max_rows_per_chunk 必须大于 0")
    if id_column not in columns:
        raise ValueError(f"未找到唯一标识列: {id_column}")
    if dependency_column not in columns:
        raise ValueError(f"未找到依赖关系列: {dependency_column}")
    if id_column == dependency_column:
        raise ValueError("唯一标识列和依赖关系列不能相同")

    row_by_id: dict[str, dict[str, Any]] = {}
    row_order: list[str] = []
    positions_by_id: dict[str, list[int]] = {}
    empty_id_rows: list[int] = []

    for row_number, row in enumerate(rows, start=1):
        job_name = _identifier(row.get(id_column))
        if not job_name:
            empty_id_rows.append(row_number)
            continue
        positions_by_id.setdefault(job_name, []).append(row_number)
        if job_name not in row_by_id:
            row_by_id[job_name] = row
            row_order.append(job_name)

    duplicates = [
        (job_name, positions)
        for job_name, positions in positions_by_id.items()
        if len(positions) > 1
    ]
    if duplicates:
        job_name, positions = duplicates[0]
        formatted_positions = "、".join(str(position) for position in positions)
        raise DuplicateIdentifierError(
            f"唯一标识列 {id_column!r} 存在重复 ID {job_name!r}："
            f"数据行 {formatted_positions}（共出现 {len(positions)} 次）"
        )

    dep_map = {
        job_name: parse_job_dependencies(row_by_id[job_name].get(dependency_column))
        for job_name in row_order
    }
    missing_names: dict[str, None] = {}
    missing_references = 0
    for dependencies in dep_map.values():
        for dependency in dependencies:
            if dependency not in row_by_id:
                missing_names.setdefault(dependency, None)
                missing_references += 1

    cycle_rows = [
        (job_name, row_by_id[job_name].get(dependency_column))
        for job_name in row_order
    ]
    cycles = tuple(
        tuple(cycle)
        for cycle in find_job_dependency_cycles(cycle_rows, max_cycles=20)
    )
    diagnostics = SplitDiagnostics(
        original_data_rows=len(rows),
        valid_id_count=len(row_by_id),
        empty_id_rows=tuple(empty_id_rows),
        missing_dependencies=tuple(missing_names),
        missing_dependency_references=missing_references,
        cycles=cycles,
    )

    if not row_order:
        return DependencySplitResult(chunks=[], diagnostics=diagnostics)

    # Preserve source order when the complete data set already fits.
    if len(row_order) <= max_rows_per_chunk:
        only_chunk = [dict(row_by_id[job_name]) for job_name in row_order]
        chunks = [only_chunk]
        _validate_chunk_dependencies(
            chunks, row_by_id, id_column, dependency_column
        )
        return DependencySplitResult(chunks=chunks, diagnostics=diagnostics)

    closure_cache: dict[str, list[str]] = {}

    def resolve_closure(job_name: str) -> list[str]:
        if job_name not in closure_cache:
            closure_cache[job_name] = _dependency_closure(job_name, dep_map)
        return closure_cache[job_name]

    chunks: list[list[dict[str, Any]]] = []
    current_rows: list[dict[str, Any]] = []
    current_ids: set[str] = set()

    for job_name in row_order:
        required_ids = resolve_closure(job_name) + [job_name]
        missing_ids = [required for required in required_ids if required not in current_ids]
        if len(missing_ids) > max_rows_per_chunk:
            raise DependencyClosureTooLargeError(
                f"唯一标识 {job_name!r} 的完整依赖闭包共 {len(missing_ids)} 行，"
                f"超过上限 {max_rows_per_chunk}，无法自动拆分"
            )

        if current_rows and len(current_rows) + len(missing_ids) > max_rows_per_chunk:
            chunks.append(current_rows)
            current_rows = []
            current_ids = set()
            missing_ids = required_ids
            if len(missing_ids) > max_rows_per_chunk:
                raise DependencyClosureTooLargeError(
                    f"唯一标识 {job_name!r} 的完整依赖闭包共 {len(missing_ids)} 行，"
                    f"超过上限 {max_rows_per_chunk}，无法自动拆分"
                )

        for required_id in missing_ids:
            current_rows.append(dict(row_by_id[required_id]))
            current_ids.add(required_id)

    if current_rows:
        chunks.append(current_rows)

    # Guard the public invariant here too, so a future graph change cannot
    # silently produce a chunk with an omitted in-dataset dependency.
    _validate_chunk_dependencies(chunks, row_by_id, id_column, dependency_column)
    return DependencySplitResult(chunks=chunks, diagnostics=diagnostics)
