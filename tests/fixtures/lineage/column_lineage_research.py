"""Research-only column lineage contract and synthetic prototype.

This module intentionally lives under ``tests/fixtures``.  It is not imported by
``shared.lineage`` and must not be used by the production table-lineage pipeline.
The parser is a deliberately small, conservative evaluator for public synthetic
SQL shapes used by Issue #45.  It emits no dependency when metadata or scope is
not trustworthy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Mapping, Protocol, cast

from shared.lineage.domain import DatasetIdentity


class MetadataStatus(str, Enum):
    """The contract-level result of one metadata lookup."""

    RESOLVED = "RESOLVED"
    NOT_AVAILABLE = "NOT_AVAILABLE"
    STALE = "STALE"
    AMBIGUOUS = "AMBIGUOUS"
    ERROR = "ERROR"


class ColumnAvailability(str, Enum):
    """Whether an individual column record is usable for resolution."""

    AVAILABLE = "AVAILABLE"
    NOT_AVAILABLE = "NOT_AVAILABLE"


@dataclass(frozen=True)
class ColumnMetadata:
    """The minimum immutable column record returned by a provider."""

    name: str
    ordinal: int
    data_type: str | None
    snapshot_version: str
    observed_at: datetime
    availability: ColumnAvailability | str
    source: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("column name must be non-empty")
        if self.ordinal < 1:
            raise ValueError("column ordinal must be positive")
        if not isinstance(self.snapshot_version, str) or not self.snapshot_version.strip():
            raise ValueError("column snapshot_version must be non-empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("column observed_at must be timezone-aware")
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("column source must be non-empty")
        object.__setattr__(
            self,
            "availability",
            ColumnAvailability(self.availability),
        )


@dataclass(frozen=True)
class MetadataSnapshot:
    """A provider response; callers must inspect ``status`` before using columns."""

    dataset: DatasetIdentity
    status: MetadataStatus | str
    columns: tuple[ColumnMetadata, ...] = ()
    snapshot_version: str | None = None
    observed_at: datetime | None = None
    availability: ColumnAvailability | str = ColumnAvailability.AVAILABLE
    source: str | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dataset, DatasetIdentity):
            raise TypeError("dataset must be a DatasetIdentity")
        object.__setattr__(self, "status", MetadataStatus(self.status))
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(
            self,
            "availability",
            ColumnAvailability(self.availability),
        )
        if not self.source or not self.source.strip():
            raise ValueError("metadata source must be non-empty")
        if self.observed_at is not None and (
            self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None
        ):
            raise ValueError("metadata observed_at must be timezone-aware")
        names: set[str] = set()
        ordinals: set[int] = set()
        for column in self.columns:
            if not isinstance(column, ColumnMetadata):
                raise TypeError("columns must contain ColumnMetadata values")
            normalized_name = column.name.strip().upper()
            if normalized_name in names:
                raise ValueError("metadata column names must be unique")
            if column.ordinal in ordinals:
                raise ValueError("metadata column ordinals must be unique")
            names.add(normalized_name)
            ordinals.add(column.ordinal)
        if self.status in {MetadataStatus.RESOLVED, MetadataStatus.STALE}:
            if not self.columns:
                raise ValueError("resolved or stale metadata requires columns")
            if not self.snapshot_version or self.observed_at is None or not self.source:
                raise ValueError(
                    "resolved or stale metadata requires version, observed_at and source"
                )
            if any(
                column.availability is not ColumnAvailability.AVAILABLE
                for column in self.columns
            ):
                raise ValueError("resolved or stale metadata cannot contain unavailable columns")
        if self.status is MetadataStatus.NOT_AVAILABLE:
            if self.columns:
                raise ValueError("not-available metadata cannot contain columns")
            if self.availability is not ColumnAvailability.NOT_AVAILABLE:
                raise ValueError("not-available metadata must expose unavailable status")
        if self.status is MetadataStatus.ERROR and not self.error_code:
            raise ValueError("error metadata requires a stable error_code")


class MetadataProvider(Protocol):
    """The smallest injectable boundary proposed by Issue #45."""

    def get_columns(self, dataset_identity: DatasetIdentity) -> MetadataSnapshot:
        """Return a versioned snapshot; never infer missing columns."""
        ...


class FakeMetadataProvider:
    """Deterministic provider for synthetic tests; no database or network access."""

    def __init__(self, snapshots: Mapping[DatasetIdentity, MetadataSnapshot]) -> None:
        self._snapshots = dict(snapshots)
        self.calls: list[DatasetIdentity] = []

    def get_columns(self, dataset_identity: DatasetIdentity) -> MetadataSnapshot:
        self.calls.append(dataset_identity)
        snapshot = self._snapshots.get(dataset_identity)
        if snapshot is not None:
            return snapshot
        return MetadataSnapshot(
            dataset=dataset_identity,
            status=MetadataStatus.NOT_AVAILABLE,
            availability=ColumnAvailability.NOT_AVAILABLE,
            source="fake-metadata-provider",
        )


class ColumnLineageStatus(str, Enum):
    """Fact confidence for one synthetic SQL statement."""

    RESOLVED = "RESOLVED"
    PARTIALLY_RESOLVED = "PARTIALLY_RESOLVED"
    AMBIGUOUS = "AMBIGUOUS"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True)
class ColumnDependency:
    """An in-memory dependency; it is not a production persistence model."""

    output_column: str
    source_dataset: DatasetIdentity
    source_column: str


@dataclass(frozen=True)
class ColumnLineageResult:
    """Conservative prototype output for one SQL statement."""

    statement_kind: str
    status: ColumnLineageStatus
    output_columns: tuple[str, ...]
    dependencies: tuple[ColumnDependency, ...]
    reason: str
    metadata_lookups: int

    def dependencies_for(self, output_column: str) -> tuple[ColumnDependency, ...]:
        normalized = output_column.strip().upper()
        return tuple(
            dependency
            for dependency in self.dependencies
            if dependency.output_column == normalized
        )


@dataclass(frozen=True)
class _Origin:
    dataset: DatasetIdentity
    column: str


@dataclass
class _Relation:
    alias: str
    columns: dict[str, tuple[_Origin, ...]]
    column_order: tuple[str, ...]
    metadata_status: MetadataStatus | None = None
    dataset: DatasetIdentity | None = None


@dataclass
class _Item:
    output_column: str
    dependencies: tuple[ColumnDependency, ...]
    state: str
    reason: str


@dataclass
class _Evaluation:
    items: list[_Item]
    forced_partial: bool = False
    reasons: list[str] = field(default_factory=list)

    @property
    def output_columns(self) -> list[str]:
        return [item.output_column for item in self.items]

    @property
    def dependencies(self) -> list[ColumnDependency]:
        values: list[ColumnDependency] = []
        seen: set[tuple[str, DatasetIdentity, str]] = set()
        for item in self.items:
            for dependency in item.dependencies:
                key = (
                    dependency.output_column,
                    dependency.source_dataset,
                    dependency.source_column,
                )
                if key not in seen:
                    seen.add(key)
                    values.append(dependency)
        return values


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_$]*"
_IDENTIFIER_RE = re.compile(_IDENTIFIER)
_QUALIFIED_RE = re.compile(
    rf"(?P<qual>{_IDENTIFIER})\s*\.\s*(?P<column>{_IDENTIFIER}|\*)"
)
_TABLE_RE = re.compile(
    rf"^\s*(?P<table>{_IDENTIFIER}\s*\.\s*{_IDENTIFIER})"
    rf"(?:\s+(?:AS\s+)?(?P<alias>{_IDENTIFIER}))?\s*$",
    re.IGNORECASE,
)
_CTE_RE = re.compile(rf"^\s*(?P<name>{_IDENTIFIER})\s+AS\s*\(", re.IGNORECASE)
_RESERVED = {
    "ALL",
    "AND",
    "AS",
    "ASC",
    "BY",
    "CASE",
    "CAST",
    "CREATE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "IS",
    "JOIN",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OVER",
    "PARTITION",
    "SELECT",
    "SET",
    "THEN",
    "UNION",
    "WHEN",
    "WHERE",
    "WITH",
}
_JOIN_MODIFIERS = re.compile(r"\b(?:LEFT|RIGHT|FULL|INNER|CROSS|OUTER)\s*$", re.IGNORECASE)
_CLAUSE_KEYWORDS = ("WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "QUALIFY", "WINDOW")


def synthetic_identity(name: str, environment: str = "SYNTHETIC") -> DatasetIdentity:
    """Build a DatasetIdentity for public synthetic SQL only."""

    identity = DatasetIdentity.from_name(environment, name)
    if identity is None:
        raise ValueError(f"synthetic dataset must be schema.table: {name!r}")
    return identity


def resolved_snapshot(
    dataset_name: str,
    columns: tuple[str, ...] | list[str],
    *,
    environment: str = "SYNTHETIC",
    snapshot_version: str = "snapshot-1",
    observed_at: datetime | None = None,
    source: str = "offline-synthetic-snapshot",
) -> MetadataSnapshot:
    """Create a fully resolved fake snapshot with sanitized column names."""

    observed = observed_at or datetime(2026, 1, 1, tzinfo=timezone.utc)
    identity = synthetic_identity(dataset_name, environment)
    metadata_columns = tuple(
        ColumnMetadata(
            name=name,
            ordinal=ordinal,
            data_type="VARCHAR",
            snapshot_version=snapshot_version,
            observed_at=observed,
            availability=ColumnAvailability.AVAILABLE,
            source=source,
        )
        for ordinal, name in enumerate(columns, start=1)
    )
    return MetadataSnapshot(
        dataset=identity,
        status=MetadataStatus.RESOLVED,
        columns=metadata_columns,
        snapshot_version=snapshot_version,
        observed_at=observed,
        availability=ColumnAvailability.AVAILABLE,
        source=source,
    )


def stale_snapshot(
    dataset_name: str,
    columns: tuple[str, ...] | list[str],
    *,
    environment: str = "SYNTHETIC",
) -> MetadataSnapshot:
    """Create columns that must not be promoted to resolved facts."""

    base = resolved_snapshot(
        dataset_name,
        columns,
        environment=environment,
        snapshot_version="snapshot-old",
    )
    return MetadataSnapshot(
        dataset=base.dataset,
        status=MetadataStatus.STALE,
        columns=base.columns,
        snapshot_version=base.snapshot_version,
        observed_at=base.observed_at,
        availability=base.availability,
        source=base.source,
    )


def infer_column_lineage(
    sql: str,
    provider: MetadataProvider,
    *,
    environment: str = "SYNTHETIC",
) -> ColumnLineageResult:
    """Evaluate supported synthetic SQL conservatively.

    This is intentionally not a replacement parser.  Unsupported or incomplete
    syntax produces ``UNRESOLVED``/``PARTIALLY_RESOLVED`` rather than guessed
    dependencies.
    """

    if not isinstance(sql, str) or not sql.strip():
        return ColumnLineageResult(
            statement_kind="UNKNOWN",
            status=ColumnLineageStatus.UNRESOLVED,
            output_columns=(),
            dependencies=(),
            reason="empty SQL",
            metadata_lookups=0,
        )
    metadata_cache: dict[DatasetIdentity, MetadataSnapshot] = {}
    statement = _strip_sql_comments(sql).strip().rstrip(";").strip()
    upper = statement.upper()
    if upper.startswith("MERGE"):
        evaluation = _evaluate_merge(
            statement,
            provider,
            environment,
            metadata_cache,
            {},
        )
        kind = "MERGE"
    elif upper.startswith("INSERT"):
        evaluation = _evaluate_insert(
            statement,
            provider,
            environment,
            metadata_cache,
            {},
        )
        kind = "INSERT_SELECT"
    elif upper.startswith("SELECT") or upper.startswith("WITH"):
        evaluation = _evaluate_query(
            statement,
            provider,
            environment,
            metadata_cache,
            {},
        )
        kind = "SELECT"
    else:
        evaluation = _Evaluation([], reasons=["unsupported statement kind"])
        kind = "UNKNOWN"
    return _finalize(evaluation, kind, metadata_cache)


def _finalize(
    evaluation: _Evaluation,
    statement_kind: str,
    metadata_cache: Mapping[DatasetIdentity, MetadataSnapshot],
) -> ColumnLineageResult:
    states = [item.state for item in evaluation.items]
    if any(state == "ambiguous" for state in states):
        status = ColumnLineageStatus.AMBIGUOUS
    elif evaluation.forced_partial:
        status = (
            ColumnLineageStatus.PARTIALLY_RESOLVED
            if any(state == "resolved" for state in states)
            else ColumnLineageStatus.UNRESOLVED
        )
    elif states and all(state == "resolved" for state in states):
        status = ColumnLineageStatus.RESOLVED
    elif any(state == "resolved" for state in states):
        status = ColumnLineageStatus.PARTIALLY_RESOLVED
    else:
        status = ColumnLineageStatus.UNRESOLVED

    reasons = list(evaluation.reasons or [])
    reasons.extend(item.reason for item in evaluation.items if item.reason)
    unique_reasons = tuple(dict.fromkeys(reasons))
    return ColumnLineageResult(
        statement_kind=statement_kind,
        status=status,
        output_columns=tuple(evaluation.output_columns),
        dependencies=tuple(evaluation.dependencies),
        reason="; ".join(unique_reasons) or "all output dependencies resolved",
        metadata_lookups=len(metadata_cache),
    )


def _evaluate_insert(
    statement: str,
    provider: MetadataProvider,
    environment: str,
    cache: dict[DatasetIdentity, MetadataSnapshot],
    ctes: Mapping[str, str],
) -> _Evaluation:
    select_position = _find_top_level_keyword(statement, "SELECT")
    if select_position is None:
        return _Evaluation([], reasons=["INSERT without SELECT is not column-resolvable"])
    selected = _evaluate_query(
        statement[select_position:], provider, environment, cache, ctes
    )
    target_columns = _insert_target_columns(statement)
    if target_columns:
        if len(target_columns) != len(selected.items):
            selected.forced_partial = True
            selected.reasons.append("target column list and SELECT arity differ")
            return selected
        _rename_outputs(selected, target_columns)
        return selected

    target_name = _insert_target_name(statement)
    if target_name is None:
        selected.forced_partial = True
        selected.reasons.append("INSERT target is not a qualified DatasetIdentity")
        return selected
    target = synthetic_identity(target_name, environment)
    target_snapshot = _metadata_for(target, provider, cache)
    if target_snapshot.status is not MetadataStatus.RESOLVED:
        selected.forced_partial = True
        for item in selected.items:
            # The source expansion is observable, but positional target mapping
            # is not a lineage fact without the target schema.
            item.dependencies = ()
        selected.reasons.append("target metadata is unavailable for positional INSERT mapping")
        return selected
    target_columns = tuple(column.name.strip().upper() for column in target_snapshot.columns)
    if len(target_columns) != len(selected.items):
        selected.forced_partial = True
        selected.reasons.append("target metadata and SELECT arity differ")
        return selected
    _rename_outputs(selected, target_columns)
    return selected


def _evaluate_merge(
    statement: str,
    provider: MetadataProvider,
    environment: str,
    cache: dict[DatasetIdentity, MetadataSnapshot],
    ctes: Mapping[str, str],
) -> _Evaluation:
    using_position = _find_top_level_keyword(statement, "USING")
    if using_position is None:
        return _Evaluation([], forced_partial=True, reasons=["MERGE has no USING relation"])
    on_position = _find_top_level_keyword(statement, "ON", using_position + 5)
    using_text = statement[using_position + 5 : on_position or len(statement)]
    relation = _parse_relation_spec(
        using_text.split(" WHEN ", 1)[0],
        provider,
        environment,
        cache,
        ctes,
    )
    if relation is None:
        return _Evaluation([], forced_partial=True, reasons=["MERGE USING relation is unresolved"])

    items: list[_Item] = []
    set_position = _find_top_level_keyword(statement, "SET")
    if set_position is not None:
        set_end = _find_top_level_keyword(statement, "WHEN", set_position + 3)
        assignments = statement[set_position + 3 : set_end or len(statement)]
        for assignment in _split_top_level(assignments, ","):
            if "=" not in assignment:
                continue
            output, expression = assignment.split("=", 1)
            output_name = _last_identifier(output)
            if output_name is None:
                continue
            items.append(
                _expression_item(
                    output_name,
                    expression,
                    (relation,),
                    reason_prefix="MERGE UPDATE",
                )
            )

    insert_match = re.search(
        rf"\bINSERT\s*\((?P<columns>[^)]*)\)\s*VALUES\s*\((?P<values>[^)]*)\)",
        statement,
        re.IGNORECASE | re.DOTALL,
    )
    if insert_match:
        output_columns = [
            _normalize_identifier(value)
            for value in _split_top_level(insert_match.group("columns"), ",")
        ]
        values = _split_top_level(insert_match.group("values"), ",")
        if len(output_columns) != len(values):
            return _Evaluation(
                items,
                forced_partial=True,
                reasons=["MERGE INSERT column/value arity differs"],
            )
        for output, expression in zip(output_columns, values):
            items.append(
                _expression_item(
                    output,
                    expression,
                    (relation,),
                    reason_prefix="MERGE INSERT",
                )
            )

    if not items:
        return _Evaluation(
            [],
            forced_partial=True,
            reasons=["MERGE action assignments are not supported by the prototype"],
        )
    return _Evaluation(
        items,
        forced_partial=True,
        reasons=[
            "MERGE action predicates and target-row semantics are intentionally not persisted"
        ],
    )


def _evaluate_query(
    query: str,
    provider: MetadataProvider,
    environment: str,
    cache: dict[DatasetIdentity, MetadataSnapshot],
    outer_ctes: Mapping[str, str],
    *,
    depth: int = 0,
) -> _Evaluation:
    if depth > 8:
        return _Evaluation([], reasons=["nested query depth exceeds research limit"])
    query = query.strip().rstrip(";").strip()
    local_ctes, main_query = _parse_ctes(query)
    all_ctes = dict(outer_ctes)
    all_ctes.update(local_ctes)

    union_branches = _split_top_level_keyword(main_query, "UNION")
    if len(union_branches) > 1:
        evaluations = [
            _evaluate_query(
                branch.replace("ALL ", "", 1).strip()
                if branch.upper().startswith("ALL ")
                else branch,
                provider,
                environment,
                cache,
                all_ctes,
                depth=depth + 1,
            )
            for branch in union_branches
        ]
        first = evaluations[0]
        if not all(len(value.items) == len(first.items) for value in evaluations[1:]):
            first.forced_partial = True
            first.reasons.append("UNION branches have different output arity")
            return first
        for index, item in enumerate(first.items):
            branch_items = [value.items[index] for value in evaluations]
            merged_dependencies: list[ColumnDependency] = []
            for branch_item in branch_items:
                merged_dependencies.extend(
                    ColumnDependency(
                        output_column=item.output_column,
                        source_dataset=dependency.source_dataset,
                        source_column=dependency.source_column,
                    )
                    for dependency in branch_item.dependencies
                )
            item.dependencies = tuple(_dedupe_dependencies(merged_dependencies))
            if any(branch_item.state == "ambiguous" for branch_item in branch_items):
                item.state = "ambiguous"
            elif all(branch_item.state == "resolved" for branch_item in branch_items):
                item.state = "resolved"
            else:
                item.state = "unresolved"
        for value in evaluations:
            first.reasons.extend(value.reasons or [])
        return first

    select_position = _find_top_level_keyword(main_query, "SELECT")
    if select_position is None:
        return _Evaluation([], reasons=["query has no SELECT projection"])
    from_position = _find_top_level_keyword(main_query, "FROM", select_position + 6)
    if from_position is None:
        select_list = main_query[select_position + 6 :]
        from_text = ""
    else:
        select_list = main_query[select_position + 6 : from_position]
        from_text = _from_clause(main_query, from_position + 4)
    relations = _parse_relations(
        from_text,
        provider,
        environment,
        cache,
        all_ctes,
        depth=depth + 1,
    )
    items: list[_Item] = []
    for index, raw_item in enumerate(_split_top_level(select_list, ","), start=1):
        expression, alias = _split_projection_alias(raw_item)
        stripped = expression.strip()
        if stripped == "*" or re.fullmatch(rf"{_IDENTIFIER}\s*\.\s*\*", stripped):
            items.extend(_expand_star(stripped, relations, index))
            continue
        output_name = alias or _simple_column_name(stripped)
        if output_name is None:
            output_name = f"EXPR_{index}"
            items.append(
                _Item(
                    output_name,
                    (),
                    "unresolved",
                    "expression output needs an explicit alias",
                )
            )
            continue
        items.append(_expression_item(output_name, stripped, relations))

    normalized_names = [item.output_column for item in items]
    if len(set(normalized_names)) != len(normalized_names):
        return _Evaluation(
            [
                _Item(item.output_column, item.dependencies, "ambiguous", "duplicate output column")
                for item in items
            ],
            reasons=["duplicate output names are ambiguous"],
        )
    return _Evaluation(items)


def _parse_relations(
    from_text: str,
    provider: MetadataProvider,
    environment: str,
    cache: dict[DatasetIdentity, MetadataSnapshot],
    ctes: Mapping[str, str],
    *,
    depth: int,
) -> tuple[_Relation, ...]:
    if not from_text.strip():
        return ()
    parts = _split_top_level_keyword(from_text, "JOIN")
    relation_specs: list[str] = []
    first = _JOIN_MODIFIERS.sub("", parts[0].strip())
    relation_specs.extend(_split_top_level(first, ","))
    for part in parts[1:]:
        without_on = _split_top_level_keyword(part, "ON")[0]
        without_using = _split_top_level_keyword(without_on, "USING")[0]
        relation_specs.append(without_using.strip())
    relations: list[_Relation] = []
    for spec in relation_specs:
        relation = _parse_relation_spec(
            spec,
            provider,
            environment,
            cache,
            ctes,
            depth=depth,
        )
        if relation is not None:
            relations.append(relation)
    return tuple(relations)


def _parse_relation_spec(
    spec: str,
    provider: MetadataProvider,
    environment: str,
    cache: dict[DatasetIdentity, MetadataSnapshot],
    ctes: Mapping[str, str],
    *,
    depth: int = 0,
) -> _Relation | None:
    spec = spec.strip()
    if not spec:
        return None
    if spec.startswith("("):
        close = _matching_close(spec, 0)
        if close is None:
            return None
        inner = spec[1:close]
        alias = _relation_alias(spec[close + 1 :]) or f"SUBQUERY_{depth}"
        evaluation = _evaluate_query(
            inner,
            provider,
            environment,
            cache,
            ctes,
            depth=depth + 1,
        )
        return _relation_from_evaluation(alias, evaluation)

    cte_match = re.match(
        rf"^\s*(?P<name>{_IDENTIFIER})(?:\s+(?:AS\s+)?(?P<alias>{_IDENTIFIER}))?\s*$",
        spec,
        re.IGNORECASE,
    )
    if cte_match and cte_match.group("name").upper() in ctes:
        name = cte_match.group("name").upper()
        alias = cte_match.group("alias") or name
        evaluation = _evaluate_query(
            ctes[name],
            provider,
            environment,
            cache,
            ctes,
            depth=depth + 1,
        )
        return _relation_from_evaluation(alias, evaluation)

    table_match = _TABLE_RE.match(spec)
    if not table_match:
        return None
    table_name = _normalize_identifier(table_match.group("table"))
    alias = table_match.group("alias") or table_name.split(".")[-1]
    if alias.upper() in _RESERVED:
        alias = table_name.split(".")[-1]
    identity = DatasetIdentity.from_name(environment, table_name)
    if identity is None:
        return _Relation(alias.upper(), {}, (), MetadataStatus.NOT_AVAILABLE)
    snapshot = _metadata_for(identity, provider, cache)
    if snapshot.status not in {MetadataStatus.RESOLVED, MetadataStatus.STALE}:
        return _Relation(
            alias.upper(),
            {},
            (),
            cast(MetadataStatus, snapshot.status),
            identity,
        )
    ordered = sorted(snapshot.columns, key=lambda column: column.ordinal)
    columns = {
        column.name.strip().upper(): (_Origin(identity, column.name.strip().upper()),)
        for column in ordered
    }
    return _Relation(
        alias.upper(),
        columns,
        tuple(columns),
        cast(MetadataStatus, snapshot.status),
        identity,
    )


def _relation_from_evaluation(alias: str, evaluation: _Evaluation) -> _Relation:
    columns: dict[str, tuple[_Origin, ...]] = {}
    for item in evaluation.items:
        columns[item.output_column] = tuple(
            _Origin(dependency.source_dataset, dependency.source_column)
            for dependency in item.dependencies
        )
    return _Relation(
        alias.upper(),
        columns,
        tuple(evaluation.output_columns),
        MetadataStatus.RESOLVED
        if evaluation.items and all(item.state == "resolved" for item in evaluation.items)
        else MetadataStatus.AMBIGUOUS
        if any(item.state == "ambiguous" for item in evaluation.items)
        else MetadataStatus.NOT_AVAILABLE,
    )


def _expand_star(expression: str, relations: tuple[_Relation, ...], index: int) -> list[_Item]:
    qualifier = None
    if expression.strip() != "*":
        qualifier = expression.split(".", 1)[0].strip().upper()
    selected = [relation for relation in relations if qualifier is None or relation.alias == qualifier]
    if not selected:
        return [_Item(f"STAR_{index}", (), "unresolved", "star qualifier is unresolved")]
    items: list[_Item] = []
    for relation in selected:
        if relation.metadata_status is MetadataStatus.AMBIGUOUS:
            return [_Item("*", (), "ambiguous", "star source metadata is ambiguous")]
        for column in relation.column_order:
            origins = relation.columns.get(column, ())
            if relation.metadata_status is not MetadataStatus.RESOLVED or not origins:
                items.append(_Item(column, (), "unresolved", "SELECT * needs fresh source columns"))
                continue
            items.append(
                _Item(
                    column,
                    tuple(
                        ColumnDependency(column, origin.dataset, origin.column)
                        for origin in origins
                    ),
                    "resolved",
                    "",
                )
            )
    return items or [_Item(f"STAR_{index}", (), "unresolved", "SELECT * has no source columns")]


def _expression_item(
    output_name: str,
    expression: str,
    relations: tuple[_Relation, ...],
    *,
    reason_prefix: str = "",
) -> _Item:
    output = _normalize_identifier(output_name)
    references = _expression_references(expression)
    if not references:
        return _Item(output, (), "unresolved", f"{reason_prefix} expression has no resolvable source columns".strip())
    dependencies: list[ColumnDependency] = []
    saw_ambiguous = False
    saw_unresolved = False
    for reference in references:
        origins, state = _resolve_reference(reference, relations)
        if state == "ambiguous":
            saw_ambiguous = True
        elif state != "resolved":
            saw_unresolved = True
        dependencies.extend(
            ColumnDependency(output, origin.dataset, origin.column)
            for origin in origins
        )
    if saw_ambiguous:
        return _Item(output, (), "ambiguous", f"{reason_prefix} column reference is ambiguous".strip())
    if saw_unresolved:
        return _Item(output, (), "unresolved", f"{reason_prefix} source metadata is not usable".strip())
    return _Item(output, tuple(_dedupe_dependencies(dependencies)), "resolved", "")


def _resolve_reference(
    reference: tuple[str | None, str],
    relations: tuple[_Relation, ...],
) -> tuple[tuple[_Origin, ...], str]:
    qualifier, column = reference
    normalized_column = column.strip().upper()
    candidates = [
        relation
        for relation in relations
        if (qualifier is None or relation.alias == qualifier.upper())
    ]
    if qualifier is not None and len(candidates) != 1:
        return (), "ambiguous" if len(candidates) > 1 else "unresolved"
    matches = [
        relation
        for relation in candidates
        if normalized_column in relation.columns
    ]
    if len(matches) > 1:
        return (), "ambiguous"
    if not matches:
        return (), "unresolved"
    relation = matches[0]
    if relation.metadata_status is MetadataStatus.AMBIGUOUS:
        return (), "ambiguous"
    if relation.metadata_status is not MetadataStatus.RESOLVED:
        return (), "unresolved"
    return relation.columns[normalized_column], "resolved"


def _expression_references(expression: str) -> tuple[tuple[str | None, str], ...]:
    masked = _mask_literals(expression)
    references: list[tuple[str | None, str]] = []
    consumed: list[tuple[int, int]] = []
    for match in _QUALIFIED_RE.finditer(masked):
        qualifier = match.group("qual").upper()
        column = match.group("column").upper()
        if column != "*":
            references.append((qualifier, column))
        consumed.append(match.span())
    remainder = list(masked)
    for start, end in consumed:
        remainder[start:end] = " " * (end - start)
    remainder_text = "".join(remainder)
    for match in _IDENTIFIER_RE.finditer(remainder_text):
        token = match.group(0).upper()
        if token in _RESERVED:
            continue
        next_text = remainder_text[match.end() :].lstrip()
        if next_text.startswith("("):
            continue
        references.append((None, token))
    return tuple(dict.fromkeys(references))


def _metadata_for(
    identity: DatasetIdentity,
    provider: MetadataProvider,
    cache: dict[DatasetIdentity, MetadataSnapshot],
) -> MetadataSnapshot:
    if identity not in cache:
        snapshot = provider.get_columns(identity)
        if snapshot.dataset != identity:
            raise ValueError("metadata provider returned a different DatasetIdentity")
        cache[identity] = snapshot
    return cache[identity]


def _rename_outputs(evaluation: _Evaluation, names: tuple[str, ...] | list[str]) -> None:
    normalized = tuple(_normalize_identifier(name) for name in names)
    old_names = tuple(evaluation.output_columns)
    mapping = dict(zip(old_names, normalized))
    for item in evaluation.items:
        new_name = mapping[item.output_column]
        item.output_column = new_name
        item.dependencies = tuple(
            ColumnDependency(new_name, dependency.source_dataset, dependency.source_column)
            for dependency in item.dependencies
        )


def _insert_target_name(statement: str) -> str | None:
    match = re.search(
        rf"\bINSERT\s+INTO\s+(?P<table>{_IDENTIFIER}\s*\.\s*{_IDENTIFIER})",
        statement,
        re.IGNORECASE,
    )
    return _normalize_identifier(match.group("table")) if match else None


def _insert_target_columns(statement: str) -> tuple[str, ...]:
    match = re.search(
        rf"\bINSERT\s+INTO\s+{_IDENTIFIER}\s*\.\s*{_IDENTIFIER}\s*\((?P<columns>[^)]*)\)",
        statement,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ()
    return tuple(
        _normalize_identifier(value)
        for value in _split_top_level(match.group("columns"), ",")
    )


def _parse_ctes(query: str) -> tuple[dict[str, str], str]:
    if not query.lstrip().upper().startswith("WITH"):
        return {}, query
    position = _find_top_level_keyword(query, "WITH")
    if position is None:
        return {}, query
    position += 4
    if query[position:].lstrip().upper().startswith("RECURSIVE"):
        position += len(query[position:]) - len(query[position:].lstrip()) + len("RECURSIVE")
    ctes: dict[str, str] = {}
    while position < len(query):
        match = _CTE_RE.match(query[position:])
        if match is None:
            break
        name = match.group("name").upper()
        open_position = position + match.end() - 1
        close_position = _matching_close(query, open_position)
        if close_position is None:
            return {}, query
        ctes[name] = query[open_position + 1 : close_position]
        position = close_position + 1
        while position < len(query) and query[position].isspace():
            position += 1
        if position < len(query) and query[position] == ",":
            position += 1
            while position < len(query) and query[position].isspace():
                position += 1
            continue
        break
    return ctes, query[position:]


def _from_clause(query: str, start: int) -> str:
    end_positions = [
        position
        for keyword in _CLAUSE_KEYWORDS
        for position in [_find_top_level_keyword(query, keyword, start)]
        if position is not None
    ]
    union_position = _find_top_level_keyword(query, "UNION", start)
    if union_position is not None:
        end_positions.append(union_position)
    end = min(end_positions) if end_positions else len(query)
    return query[start:end]


def _split_projection_alias(item: str) -> tuple[str, str | None]:
    positions = _keyword_positions(item, "AS")
    if positions:
        position = positions[-1]
        alias = _first_identifier(item[position + 2 :])
        if alias:
            return item[:position].strip(), _normalize_identifier(alias)
    return item.strip(), None


def _simple_column_name(expression: str) -> str | None:
    match = re.fullmatch(
        rf"(?:{_IDENTIFIER}\s*\.\s*)?(?P<column>{_IDENTIFIER})",
        expression.strip(),
    )
    return _normalize_identifier(match.group("column")) if match else None


def _normalize_identifier(value: str) -> str:
    return re.sub(r"\s+", "", value).strip("`\" ").upper()


def _last_identifier(value: str) -> str | None:
    values = _IDENTIFIER_RE.findall(value)
    return values[-1] if values else None


def _first_identifier(value: str) -> str | None:
    match = _IDENTIFIER_RE.search(value)
    return match.group(0) if match else None


def _relation_alias(value: str) -> str | None:
    match = re.match(rf"^\s*(?:AS\s+)?(?P<alias>{_IDENTIFIER})", value, re.IGNORECASE)
    return match.group("alias") if match else None


def _strip_sql_comments(value: str) -> str:
    value = re.sub(r"/\*.*?\*/", " ", value, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", value)


def _mask_literals(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        return " " * len(match.group(0))

    return re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", replace, value)


def _keyword_positions(value: str, keyword: str, start: int = 0) -> list[int]:
    masked = _mask_literals(value)
    pattern = re.compile(rf"\b{re.escape(keyword)}\b", re.IGNORECASE)
    positions: list[int] = []
    depth = 0
    index = start
    while index < len(masked):
        character = masked[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            match = pattern.match(masked, index)
            if match:
                positions.append(index)
                index = match.end()
                continue
        index += 1
    return positions


def _find_top_level_keyword(value: str, keyword: str, start: int = 0) -> int | None:
    positions = _keyword_positions(value, keyword, start)
    return positions[0] if positions else None


def _split_top_level_keyword(value: str, keyword: str) -> list[str]:
    positions = _keyword_positions(value, keyword)
    if not positions:
        return [value]
    parts: list[str] = []
    previous = 0
    for position in positions:
        parts.append(value[previous:position])
        previous = position + len(keyword)
    parts.append(value[previous:])
    return parts


def _split_top_level(value: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    previous = 0
    depth = 0
    masked = _mask_literals(value)
    for index, character in enumerate(masked):
        if character == "(":
            depth += 1
        elif character == ")":
            depth = max(0, depth - 1)
        elif character == delimiter and depth == 0:
            parts.append(value[previous:index].strip())
            previous = index + 1
    parts.append(value[previous:].strip())
    return [part for part in parts if part]


def _matching_close(value: str, open_position: int) -> int | None:
    depth = 0
    masked = _mask_literals(value)
    for index in range(open_position, len(masked)):
        if masked[index] == "(":
            depth += 1
        elif masked[index] == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _dedupe_dependencies(
    dependencies: list[ColumnDependency],
) -> list[ColumnDependency]:
    result: list[ColumnDependency] = []
    seen: set[tuple[str, DatasetIdentity, str]] = set()
    for dependency in dependencies:
        key = (
            dependency.output_column,
            dependency.source_dataset,
            dependency.source_column,
        )
        if key not in seen:
            seen.add(key)
            result.append(dependency)
    return result
