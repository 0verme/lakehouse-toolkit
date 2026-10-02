from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError

from shared.lineage.physical_dag import _extract_python_candidates_with_reason

from .metadata_resolver import (
    MetadataResolver,
    canonical_dwf_target,
    normalize_logical_target,
)
from .models import AuditResult, MappingField, MappingItem, Resolution

DEFAULT_PROGRESS_EVERY = 100

IGNORED_DIRECTORIES = frozenset(
    {".git", ".svn", ".hg", "__pycache__", ".venv", "venv", "node_modules"}
)


@dataclass(frozen=True, slots=True)
class _InsertDefinition:
    target_key: str
    target_name: str
    target_fields: tuple[str, ...]
    query: Any
    program: str
    project: str


@dataclass(frozen=True, slots=True)
class _Relation:
    alias: str
    table_key: str | None = None
    table_name: str | None = None
    query: Any | None = None
    cte_columns: tuple[str, ...] = ()
    cte_scope: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class _LeafColumn:
    physical_table: str
    field: str
    program: str


@dataclass(frozen=True, slots=True)
class _Projection:
    sources: tuple[_LeafColumn, ...] = ()
    error: str | None = None


def _name(value: Any) -> str:
    return str(getattr(value, "name", "") or "").strip().upper()


def _parts(table: Any) -> tuple[str, ...]:
    return tuple(_name(part) for part in getattr(table, "parts", ()) if _name(part))


def _table_key(table: Any) -> str:
    return ".".join(_parts(table))


def _is_dwo_table(table: Any) -> bool:
    parts = _parts(table)
    return len(parts) >= 2 and parts[-2] == "DWO" and parts[-1].startswith("DWO_")


def _is_dwf_table(table: Any) -> bool:
    parts = _parts(table)
    if not parts:
        return False
    return "DWF" in parts[:-1] or parts[-1].startswith(("DWF_", "F_"))


def _insert_target_and_fields(insert: Any) -> tuple[Any | None, tuple[str, ...]]:
    target = insert.this
    fields: tuple[str, ...] = ()
    if isinstance(target, exp.Schema):
        fields = tuple(_name(item) for item in target.expressions)
        target = target.this
    if not isinstance(target, exp.Table) or any(not item for item in fields):
        return None, ()
    return target, fields


def _project_files(root: Path) -> tuple[list[Path], list[Path], int]:
    project_dirs = sorted(
        (
            path
            for path in root.iterdir()
            if path.is_dir()
            and not path.is_symlink()
            and path.name not in IGNORED_DIRECTORIES
            and not path.name.startswith(".")
        ),
        key=lambda path: path.name.casefold(),
    )
    files: list[Path] = [
        path
        for path in root.iterdir()
        if path.is_file()
        and not path.is_symlink()
        and path.suffix.casefold() in {".py", ".sql"}
    ]
    python_files: list[Path] = [
        path for path in files if path.suffix.casefold() == ".py"
    ]
    for project_root in project_dirs:
        errors: list[OSError] = []
        for current, directories, names in os.walk(project_root, onerror=errors.append):
            directories[:] = sorted(
                name
                for name in directories
                if name not in IGNORED_DIRECTORIES and not name.startswith(".")
            )
            for name in sorted(names):
                path = Path(current) / name
                if (
                    path.suffix.casefold() in {".py", ".sql"}
                    and path.is_file()
                    and not path.is_symlink()
                ):
                    files.append(path)
                    if path.suffix.casefold() == ".py":
                        python_files.append(path)
        if errors:
            raise OSError(
                "workspace scan was incomplete because a directory was unreadable"
            )
    unique_files = sorted(set(files), key=lambda item: item.as_posix().casefold())
    return unique_files, sorted(set(python_files)), len(project_dirs)


def _read_program(path: Path) -> tuple[list[str], str | None]:
    try:
        source = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError):
        return [], "program could not be read as UTF-8"
    if path.suffix.casefold() == ".sql":
        return [source], None
    extraction = _extract_python_candidates_with_reason(source)
    if extraction.candidates:
        return [candidate.text for candidate in extraction.candidates], None
    reason = getattr(extraction.reason, "value", str(extraction.reason))
    if reason in {
        "SQL_CALL_NOT_RECOGNIZED",
        "SQL_ARGUMENT_DYNAMIC",
        "SQL_ARGUMENT_MISSING",
        "PYTHON_PARSE_FAILED",
        "PYTHON_PARSE_RECOVERED",
    }:
        return [], f"Python SQL extraction unresolved: {reason}"
    return [], None


def _query_with_ctes(query: Any) -> tuple[Any, dict[str, Any]]:
    if isinstance(query, exp.Subquery):
        query = query.this
    if not isinstance(query, exp.Select):
        return query, {}
    with_clause = query.args.get("with_")
    ctes: dict[str, Any] = {}
    if with_clause is not None:
        for cte in with_clause.expressions:
            alias = cte.alias_or_name or cte.alias
            if alias:
                ctes[str(alias).casefold()] = cte
    return query, ctes


def _relations(query: Any, ctes: dict[str, Any]) -> list[_Relation]:
    result: list[_Relation] = []
    if not isinstance(query, exp.Select):
        return result
    sources: list[Any] = []
    from_clause = query.args.get("from_")
    if from_clause is not None:
        if from_clause.this is not None:
            sources.append(from_clause.this)
        sources.extend(from_clause.expressions or [])
    sources.extend(
        join.this for join in query.args.get("joins") or [] if join.this is not None
    )
    for source in sources:
        if isinstance(source, exp.Table):
            table_name = _table_key(source)
            bare_name = source.name.casefold()
            cte = ctes.get(bare_name)
            alias = str(source.alias_or_name or source.name or "").casefold()
            if cte is not None:
                columns = tuple(_name(item) for item in cte.alias_column_names)
                result.append(
                    _Relation(
                        alias=alias, query=cte.this, cte_columns=columns, cte_scope=ctes
                    )
                )
            else:
                result.append(
                    _Relation(
                        alias=alias,
                        table_key=table_name.casefold(),
                        table_name=table_name,
                    )
                )
        elif isinstance(source, exp.Subquery):
            alias = str(source.alias_or_name or "").casefold()
            result.append(_Relation(alias=alias, query=source.this, cte_scope=ctes))
        else:
            result.append(
                _Relation(
                    alias=str(getattr(source, "alias_or_name", "") or "").casefold()
                )
            )
    return result


class _LineageProjector:
    def __init__(self, definitions: dict[str, list[_InsertDefinition]]) -> None:
        self.definitions = definitions

    @staticmethod
    def _output_names(query: Any) -> list[str]:
        if not isinstance(query, exp.Select):
            return []
        return [
            str(expression.alias_or_name or "").strip().upper()
            for expression in query.expressions
        ]

    def _resolve_query_output(
        self,
        query: Any,
        output_name: str,
        *,
        stack: frozenset[tuple[str, str]],
        cte_columns: tuple[str, ...] = (),
        inherited_ctes: dict[str, Any] | None = None,
        program: str,
    ) -> _Projection:
        query, query_ctes = _query_with_ctes(query)
        ctes = {**(inherited_ctes or {}), **query_ctes}
        if not isinstance(query, exp.Select):
            return _Projection(error="nested query is not a supported SELECT")
        names = list(cte_columns) or self._output_names(query)
        indexes = [
            index
            for index, name in enumerate(names)
            if name.casefold() == output_name.casefold()
        ]
        if len(indexes) != 1 or indexes[0] >= len(query.expressions):
            return _Projection(
                error=f"query output {output_name} is not uniquely named"
            )
        return self._resolve_expression(
            query.expressions[indexes[0]], query, ctes, stack=stack, program=program
        )

    def _resolve_expression(
        self,
        expression: Any,
        query: Any,
        ctes: dict[str, Any],
        *,
        stack: frozenset[tuple[str, str]],
        program: str,
    ) -> _Projection:
        if expression.find(exp.Subquery) is not None:
            return _Projection(error="scalar subquery expressions are unsupported")
        columns = list(expression.find_all(exp.Column))
        if not columns:
            return _Projection(error="constant or star expression has no source field")
        relations = _relations(query, ctes)
        sources: dict[tuple[str, str, str], _LeafColumn] = {}
        for column in columns:
            field_name = _name(column.this)
            qualifier = str(getattr(column, "table", "") or "").strip('`"[]').casefold()
            candidates = (
                [relation for relation in relations if relation.alias == qualifier]
                if qualifier
                else list(relations)
            )
            if not qualifier and len(candidates) > 1:
                return _Projection(
                    error=f"unqualified source field {field_name} is ambiguous"
                )
            if len(candidates) != 1:
                return _Projection(
                    error=f"source field {field_name} has no unique relation"
                )
            relation = candidates[0]
            if relation.query is not None:
                query_marker = (f"query:{relation.alias}", field_name.casefold())
                if query_marker in stack:
                    return _Projection(
                        error="recursive CTE/subquery projection is unsupported"
                    )
                projection = self._resolve_query_output(
                    relation.query,
                    field_name,
                    stack=stack | {query_marker},
                    cte_columns=relation.cte_columns,
                    inherited_ctes=relation.cte_scope,
                    program=program,
                )
            elif relation.table_name and relation.table_key:
                parts = tuple(part.upper() for part in relation.table_name.split("."))
                if (
                    len(parts) >= 2
                    and parts[-2] == "DWO"
                    and parts[-1].startswith("DWO_")
                ):
                    leaf = _LeafColumn(relation.table_name, field_name, program)
                    projection = _Projection((leaf,))
                elif _is_dwf_relation(parts):
                    projection = self._resolve_internal_table(
                        relation.table_key, field_name, stack=stack, program=program
                    )
                else:
                    projection = _Projection(
                        error=f"non-DWO source relation {relation.table_name}"
                    )
            else:
                projection = _Projection(
                    error=f"unsupported relation for field {field_name}"
                )
            if projection.error:
                return projection
            for leaf in projection.sources:
                sources[
                    (
                        leaf.physical_table.casefold(),
                        leaf.field.casefold(),
                        leaf.program.casefold(),
                    )
                ] = leaf
        return _Projection(tuple(sources.values()))

    def _resolve_internal_table(
        self,
        table_key: str,
        field_name: str,
        *,
        stack: frozenset[tuple[str, str]],
        program: str,
    ) -> _Projection:
        marker = (table_key.casefold(), field_name.casefold())
        if marker in stack:
            return _Projection(error="cyclic internal DWF table projection")
        definitions = self.definitions.get(table_key.casefold(), [])
        matching = [
            definition
            for definition in definitions
            if any(
                column.casefold() == field_name.casefold()
                for column in definition.target_fields
            )
        ]
        if not matching:
            return _Projection(error=f"internal DWF field {field_name} has no producer")
        results: list[_LeafColumn] = []
        for definition in matching:
            index = next(
                index
                for index, column in enumerate(definition.target_fields)
                if column.casefold() == field_name.casefold()
            )
            if not isinstance(definition.query, exp.Select):
                return _Projection(error="internal DWF producer is not a plain SELECT")
            if index >= len(definition.query.expressions):
                return _Projection(error="internal DWF producer field order is invalid")
            query, ctes = _query_with_ctes(definition.query)
            result = self._resolve_expression(
                query.expressions[index],
                query,
                ctes,
                stack=stack | {marker},
                program=definition.program or program,
            )
            if result.error:
                return result
            results.extend(result.sources)
        unique = {
            (
                item.physical_table.casefold(),
                item.field.casefold(),
                item.program.casefold(),
            ): item
            for item in results
        }
        return _Projection(tuple(unique.values()))


def _is_dwf_relation(parts: tuple[str, ...]) -> bool:
    return (
        len(parts) >= 2
        and parts[-2] == "DWF"
        and (parts[-1].startswith(("DWF_", "F_")) or parts[-2] == "DWF")
    )


def _is_direct_expression(expression: Any) -> bool:
    value = expression.this if isinstance(expression, exp.Alias) else expression
    while isinstance(value, exp.Paren):
        value = value.this
    return isinstance(value, exp.Column)


def _add_issue(
    destination: list[dict[str, Any]],
    reason: str,
    *,
    project: str,
    program: str = "",
    target: str = "",
    physical_source_table: str = "",
    detail: str = "",
) -> None:
    destination.append(
        {
            "reason": reason,
            "project": project,
            "program": program,
            "targetTable": target,
            "physicalSourceTable": physical_source_table,
            "detail": detail,
        }
    )


def _resolved_audit_row(
    item: MappingItem,
    field: MappingField,
    *,
    recv_plan: str,
    data_source: str,
    project: str,
    evidence: Iterable[str],
) -> dict[str, Any]:
    return {
        "sourceSystemIdentity": recv_plan,
        "sourceSystemId": item.source_system_id,
        "dataSource": data_source,
        "sourceTable": item.source_table,
        "physicalSourceTable": field.physical_source_table,
        "sourceField": field.source_field,
        "targetTable": item.target_table,
        "targetField": field.target_field,
        "fieldOrder": field.field_order,
        "mappingRule": field.mapping_rule,
        "project": project,
        "program": field.program,
        "dbSchema": field.db_schema,
        "evidence": ";".join(evidence),
    }


def _resolve_candidate(
    resolver: MetadataResolver,
    *,
    target: str,
    programs: tuple[str, ...],
    source: str,
) -> Resolution:
    return resolver.resolve(
        target=target, program_names=programs, physical_source=source
    )


def collect_workspace(
    root: str | Path,
    resolver: MetadataResolver,
    *,
    dialect: str = "mysql",
    progress_every: int = DEFAULT_PROGRESS_EVERY,
) -> AuditResult:
    """Scan program projects, project field lineage, and resolve authoritative metadata."""

    if progress_every < 1:
        raise ValueError("progress_every must be a positive integer")
    collection_started_at = time.monotonic()
    workspace = Path(root).expanduser().resolve()
    if not workspace.is_dir():
        raise ValueError("workspace directory does not exist or is not a directory")
    files, python_files, project_count = _project_files(workspace)
    projects: dict[str, list[Path]] = defaultdict(list)
    for project_dir in workspace.iterdir():
        if (
            project_dir.is_dir()
            and not project_dir.is_symlink()
            and project_dir.name not in IGNORED_DIRECTORIES
            and not project_dir.name.startswith(".")
        ):
            projects[
                project_dir.name
            ]  # retain empty first-level projects for no_program audit
    for path in files:
        project = (
            "." if path.parent == workspace else path.relative_to(workspace).parts[0]
        )
        projects[project].append(path)

    unresolved: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    definitions_by_project: dict[str, dict[str, list[_InsertDefinition]]] = {}
    dwo_physical: set[str] = set()
    unsupported_count = 0
    projects_without_program = 0

    for project, project_files in projects.items():
        definitions: dict[str, list[_InsertDefinition]] = defaultdict(list)
        saw_candidate = False
        for path in project_files:
            candidates, extraction_issue = _read_program(path)
            if extraction_issue:
                unsupported_count += 1
                _add_issue(
                    unresolved,
                    "unsupported_sql",
                    project=project,
                    program=path.name,
                    detail=extraction_issue,
                )
                continue
            if not candidates:
                continue
            saw_candidate = True
            for sql_text in candidates:
                try:
                    statements = [
                        item
                        for item in parse(sql_text, read=dialect)
                        if item is not None
                    ]
                except (SqlglotError, TypeError, ValueError):
                    unsupported_count += 1
                    _add_issue(
                        unresolved,
                        "unsupported_sql",
                        project=project,
                        program=path.name,
                        detail="SQL parse failed for configured dialect",
                    )
                    continue
                for statement in statements:
                    for table in statement.find_all(exp.Table):
                        if _is_dwo_table(table):
                            dwo_physical.add(_table_key(table))
                    if isinstance(statement, exp.Merge):
                        merge_target = statement.this
                        if isinstance(merge_target, exp.Table) and _is_dwf_table(
                            merge_target
                        ):
                            unsupported_count += 1
                            _add_issue(
                                unresolved,
                                "unsupported_sql",
                                project=project,
                                program=path.name,
                                target=_table_key(merge_target),
                                detail="MERGE field projection is unsupported; no mapping was emitted",
                            )
                    if not isinstance(statement, exp.Insert):
                        continue
                    target, target_fields = _insert_target_and_fields(statement)
                    if target is None or not _is_dwf_table(target):
                        continue
                    query = statement.expression
                    target_name = _table_key(target)
                    target_key = target_name.casefold()
                    if not target_fields or not isinstance(query, exp.Select):
                        unsupported_count += 1
                        _add_issue(
                            unresolved,
                            "unsupported_sql",
                            project=project,
                            program=path.name,
                            target=target_name,
                            detail="DWF INSERT requires an explicit target list and SELECT projection",
                        )
                        continue
                    if len(target_fields) != len(query.expressions):
                        unsupported_count += 1
                        _add_issue(
                            unresolved,
                            "unsupported_sql",
                            project=project,
                            program=path.name,
                            target=target_name,
                            detail="INSERT target column count does not match SELECT projection",
                        )
                        continue
                    definitions[target_key].append(
                        _InsertDefinition(
                            target_key=target_key,
                            target_name=target_name,
                            target_fields=target_fields,
                            query=query,
                            program=path.name,
                            project=project,
                        )
                    )
        if not saw_candidate:
            projects_without_program += 1
            _add_issue(unresolved, "no_program", project=project)
        elif not definitions:
            _add_issue(
                unresolved,
                "no_final_target",
                project=project,
                detail="no supported DWF INSERT projection was found",
            )
        definitions_by_project[project] = definitions

    builders: dict[tuple[str, str, str], dict[str, Any]] = {}
    resolved_projects: set[str] = set()
    no_final_target_count = sum(
        issue["reason"] == "no_final_target" for issue in unresolved
    )
    no_recv_count = 0
    no_schema_count = 0
    no_dwo_count = 0
    unknown_upstream_count = 0
    conflict_counts: dict[str, int] = defaultdict(int)
    known_logicals = set(resolver.recv_by_logical_target)
    progress_total = len(definitions_by_project)

    def log_project_progress(processed: int) -> None:
        if processed % progress_every == 0 or processed == progress_total:
            elapsed = time.monotonic() - collection_started_at
            print(
                f"[collector] projects={processed}/{progress_total} "
                f"elapsed={elapsed:.1f}s",
                file=sys.stderr,
            )

    for processed_projects, (project, definitions) in enumerate(
        definitions_by_project.items(), start=1
    ):
        if processed_projects > 1:
            log_project_progress(processed_projects - 1)
        if not definitions:
            continue
        all_targets = set(definitions)
        consumed: set[str] = set()
        for target_definitions in definitions.values():
            for definition in target_definitions:
                query, _ = _query_with_ctes(definition.query)
                for table in query.find_all(exp.Table):
                    if _is_dwf_table(table):
                        consumed.add(_table_key(table).casefold())
        terminal_targets = sorted(all_targets - consumed)
        matching_terminals = [
            target
            for target in terminal_targets
            if normalize_logical_target(definitions[target][0].target_name)
            in known_logicals
        ]
        if not matching_terminals:
            reason = "no_recv_dwf" if terminal_targets else "no_final_target"
            if reason == "no_final_target":
                no_final_target_count += 1
            _add_issue(
                unresolved,
                reason,
                project=project,
                target=",".join(
                    definitions[target][0].target_name for target in terminal_targets
                ),
                detail="no terminal DWF target matches p_recv_dwf metadata",
            )
            if reason == "no_recv_dwf":
                no_recv_count += 1
            continue

        projector = _LineageProjector(definitions)
        for target_key in matching_terminals:
            target_definitions = definitions[target_key]
            target_name = target_definitions[0].target_name
            target_canonical = canonical_dwf_target(target_name)
            field_origins: list[tuple[Any, _InsertDefinition, int, str]] = []
            for definition in target_definitions:
                for index, target_field in enumerate(definition.target_fields):
                    field_origins.append(
                        (
                            definition.query.expressions[index],
                            definition,
                            index,
                            target_field,
                        )
                    )
            if not field_origins:
                no_final_target_count += 1
                _add_issue(
                    unresolved, "no_final_target", project=project, target=target_name
                )
                continue

            for expression, definition, field_order, target_field in sorted(
                field_origins,
                key=lambda row: (row[2], row[3].casefold(), row[1].program.casefold()),
            ):
                query, ctes = _query_with_ctes(definition.query)
                projection = projector._resolve_expression(
                    expression,
                    query,
                    ctes,
                    stack=frozenset(),
                    program=definition.program,
                )
                if projection.error:
                    reason = (
                        "no_dwo_source"
                        if projection.error.startswith("non-DWO source relation")
                        else "unsupported_sql"
                    )
                    if reason == "unsupported_sql":
                        unsupported_count += 1
                    if reason == "no_dwo_source":
                        no_dwo_count += 1
                    _add_issue(
                        unresolved,
                        reason,
                        project=project,
                        program=definition.program,
                        target=target_name,
                        detail=f"targetField={target_field}: {projection.error}",
                    )
                    continue
                if not projection.sources:
                    unsupported_count += 1
                    _add_issue(
                        unresolved,
                        "unsupported_sql",
                        project=project,
                        program=definition.program,
                        target=target_name,
                        detail=f"targetField={target_field}: no source field was resolved",
                    )
                    continue

                for leaf in projection.sources:
                    resolution = _resolve_candidate(
                        resolver,
                        target=target_name,
                        programs=(leaf.program, definition.program),
                        source=leaf.physical_table,
                    )
                    if resolution.status != "RESOLVED":
                        issue_destination = (
                            conflicts if resolution.status == "CONFLICT" else unresolved
                        )
                        reason = resolution.reason or "unsupported_sql"
                        _add_issue(
                            issue_destination,
                            reason,
                            project=project,
                            program=leaf.program or definition.program,
                            target=target_name,
                            physical_source_table=leaf.physical_table,
                            detail=";".join(resolution.evidence),
                        )
                        if resolution.status == "CONFLICT":
                            conflict_counts[reason] += 1
                        else:
                            if reason == "no_schema_config":
                                no_schema_count += 1
                            elif reason == "no_dwo_source":
                                no_dwo_count += 1
                            elif reason == "unknown_upstream_system":
                                unknown_upstream_count += 1
                        continue

                    assert resolution.record is not None
                    assert resolution.source is not None
                    expression_rule = (
                        "DIRECT"
                        if _is_direct_expression(expression)
                        and leaf.field.casefold() == target_field.casefold()
                        else "RENAME"
                        if _is_direct_expression(expression)
                        else "待补充"
                    )
                    item_key = (
                        resolution.record.recv_plan.casefold(),
                        resolution.source.source_table.casefold(),
                        target_canonical.casefold(),
                    )
                    builder = builders.setdefault(
                        item_key,
                        {
                            "recv_plan": resolution.record.recv_plan,
                            "data_source": resolution.record.data_source,
                            "system_id": resolution.upstream_system_id,
                            "source_table": resolution.source.source_table,
                            "target_table": target_canonical,
                            "project": project,
                            "projects": set(),
                            "fields": {},
                            "field_evidence": defaultdict(set),
                            "conflicting": False,
                        },
                    )
                    builder["projects"].add(project)
                    field = MappingField(
                        source_field=leaf.field,
                        target_field=target_field,
                        mapping_rule=expression_rule,
                        field_order=field_order + 1,
                        physical_source_table=resolution.source.physical_table,
                        db_schema=resolution.source.db_schema,
                        program=leaf.program or definition.program,
                        evidence=resolution.evidence,
                    )
                    field_key = (
                        field.source_field.casefold(),
                        field.target_field.casefold(),
                    )
                    previous = builder["fields"].get(field_key)
                    if builder["conflicting"]:
                        continue
                    if previous is not None and (
                        previous.mapping_rule != field.mapping_rule
                        or previous.field_order != field.field_order
                        or previous.physical_source_table.casefold()
                        != field.physical_source_table.casefold()
                        or previous.db_schema.casefold() != field.db_schema.casefold()
                    ):
                        conflict_counts["field_mapping_conflict"] += 1
                        builder["conflicting"] = True
                        _add_issue(
                            conflicts,
                            "field_mapping_conflict",
                            project=project,
                            program=field.program,
                            target=target_name,
                            physical_source_table=field.physical_source_table,
                            detail=(
                                f"sourceField={field.source_field} "
                                f"targetField={field.target_field}; "
                                f"previous={previous.physical_source_table}"
                                f"/{previous.db_schema}/{previous.field_order}/"
                                f"{previous.mapping_rule}; "
                                f"current={field.physical_source_table}"
                                f"/{field.db_schema}/{field.field_order}/"
                                f"{field.mapping_rule}"
                            ),
                        )
                        builder["fields"].pop(field_key, None)
                        builder["field_evidence"].pop(field_key, None)
                        continue
                    builder["field_evidence"][field_key].update(field.evidence)
                    builder["fields"][field_key] = field

    if progress_total:
        log_project_progress(progress_total)

    items: list[MappingItem] = []
    for key in sorted(builders):
        builder = builders[key]
        if builder["conflicting"] or not builder["fields"]:
            continue
        item = MappingItem(
            source_system_identity=builder["recv_plan"],
            source_system_id=builder["system_id"],
            source_table=builder["source_table"],
            target_table=builder["target_table"],
            fields=tuple(
                sorted(
                    builder["fields"].values(),
                    key=lambda item: (item.field_order, item.source_field.casefold()),
                )
            ),
        )
        items.append(item)
        resolved_projects.update(builder["projects"])
        for field in item.fields:
            resolved.append(
                _resolved_audit_row(
                    item,
                    field,
                    recv_plan=builder["recv_plan"],
                    data_source=builder["data_source"],
                    project=";".join(sorted(builder["projects"], key=str.casefold)),
                    evidence=tuple(
                        name
                        for name in (
                            "ods_job_name",
                            "table_name",
                            "db_schema",
                            "dap_upstream_system",
                        )
                        if name
                        in builder["field_evidence"][
                            (
                                field.source_field.casefold(),
                                field.target_field.casefold(),
                            )
                        ]
                    ),
                )
            )

    # API identity cannot represent two recv_plan values that collapse to the same DAP
    # system/table/target tuple; keep those mappings out instead of merging identities.
    by_dap_identity: dict[tuple[int, str, str], list[MappingItem]] = defaultdict(list)
    for item in items:
        by_dap_identity[item.dap_identity].append(item)
    collided_identities = {
        identity
        for identity, group in by_dap_identity.items()
        if len({item.source_system_identity.casefold() for item in group}) > 1
    }
    if collided_identities:
        kept: list[MappingItem] = []
        for item in items:
            if item.dap_identity in collided_identities:
                conflict_counts["multiple_recv_plan_conflict"] += 1
                conflicts.append(
                    {
                        "reason": "multiple_recv_plan_conflict",
                        "project": "",
                        "program": "",
                        "targetTable": item.target_table,
                        "physicalSourceTable": "",
                        "detail": f"DAP identity collision across recv_plan values for {item.source_table}",
                    }
                )
            else:
                kept.append(item)
        items = kept
        resolved = [
            row
            for row in resolved
            if (
                row.get("sourceSystemId"),
                str(row.get("sourceTable", "")).casefold(),
                str(row.get("targetTable", "")).casefold(),
            )
            not in collided_identities
        ]
        resolved_projects = {
            project for item in items for project in builders[item.identity]["projects"]
        }

    metadata_project_candidates = {
        issue.get("project") for issue in unresolved + conflicts if issue.get("project")
    }
    summary: dict[str, Any] = {
        "project_count": project_count,
        "python_file_count": len(python_files),
        "dwo_physical_table_count": len(dwo_physical),
        "metadata": {
            "recv_dwf_rows": len(resolver.recv_dwf),
            "schema_config_rows": len(resolver.schema_configs),
            "upstream_system_count": resolver.upstream_system_count or 0,
        },
        "resolution": {
            "resolved_projects": len(resolved_projects),
            "resolved_table_mappings": len({item.identity for item in items}),
            "resolved_field_mappings": sum(len(item.fields) for item in items),
        },
        "unresolved": {
            "no_program": projects_without_program,
            "no_final_target": no_final_target_count,
            "no_recv_dwf": no_recv_count,
            "no_schema_config": no_schema_count,
            "no_dwo_source": no_dwo_count,
            "unknown_upstream_system": unknown_upstream_count,
            "unsupported_sql": unsupported_count,
            "multi_source_field_unsupported": 0,
        },
        "conflict": {
            reason: conflict_counts.get(reason, 0)
            for reason in (
                "program_metadata_conflict",
                "multiple_recv_plan_conflict",
                "multiple_data_source_conflict",
                "schema_match_conflict",
                "upstream_system_conflict",
                "field_mapping_conflict",
            )
        },
        "failed": 0,
        "diagnostic_project_count": len(metadata_project_candidates),
    }
    return AuditResult(
        items=tuple(items),
        summary=summary,
        resolved=resolved,
        unresolved=unresolved,
        conflicts=conflicts,
    )


__all__ = ["DEFAULT_PROGRESS_EVERY", "collect_workspace"]
