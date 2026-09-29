"""Collect explicit ODS/upstream -> DWF field mappings and upsert them through DAP."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

if __package__:
    from ._bootstrap import ensure_project_root_on_path
else:
    from _bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

try:
    import requests
    from sqlglot import exp, parse
    from sqlglot.errors import SqlglotError
except ImportError as error:  # pragma: no cover - exercised by deployment setup
    raise SystemExit(
        "Missing collector dependency; install project requirements (sqlglot and requests)."
    ) from error

from shared.lineage.physical_dag import (
    _extract_python_candidates_with_reason,
)

DAP_IMPORT_PATH = "/api/field-mappings/import"
DAP_MAX_ITEMS = 500
DAP_MAX_FIELDS_PER_ITEM = 1_000
DEFAULT_BATCH_SIZE = 100
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF = 0.5
SUPPORTED_SUFFIXES = frozenset({".py", ".sql"})
IGNORED_DIRECTORIES = frozenset(
    {".git", ".svn", ".hg", "__pycache__", ".venv", "venv", "node_modules"}
)
KNOWN_WAREHOUSE_LAYERS = ("DWF", "DWM", "DWD", "DWA", "DWS", "DM")
VALID_ACTIONS = frozenset({"created", "updated", "unchanged", "failed"})


@dataclass(frozen=True, slots=True)
class MappingField:
    source_field: str
    target_field: str
    mapping_rule: str
    field_order: int
    source_type: str | None = None
    source_comment: str | None = None

    def to_contract(self) -> dict[str, object]:
        result: dict[str, object] = {
            "sourceField": self.source_field,
            "targetField": self.target_field,
            "mappingRule": self.mapping_rule,
            "fieldOrder": self.field_order,
        }
        if self.source_type is not None:
            result["sourceType"] = self.source_type
        if self.source_comment is not None:
            result["sourceComment"] = self.source_comment
        return result


@dataclass(frozen=True, slots=True)
class MappingItem:
    """Local collector fact; serialized losslessly to the DAP import contract."""

    source_system_id: int
    source_table: str
    target_table: str
    fields: tuple[MappingField, ...]
    source_table_cn: str | None = None
    load_mode: str | None = None
    table_desc: str | None = None

    @property
    def identity(self) -> tuple[int, str]:
        return self.source_system_id, self.source_table.casefold()

    def to_contract(self) -> dict[str, object]:
        result: dict[str, object] = {
            "sourceSystemId": self.source_system_id,
            "sourceTable": self.source_table,
            "targetLayer": "DWF",
            "targetTable": self.target_table,
            "fields": [item.to_contract() for item in self.fields],
        }
        if self.source_table_cn is not None:
            result["sourceTableCn"] = self.source_table_cn
        if self.load_mode is not None:
            result["loadMode"] = self.load_mode
        if self.table_desc is not None:
            result["tableDesc"] = self.table_desc
        return result


@dataclass(slots=True)
class CollectorStats:
    scanned_files: int = 0
    candidate_programs: int = 0
    parsed_tables: int = 0
    parsed_fields: int = 0
    skipped_files: int = 0
    skipped_tables: int = 0
    batches: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0
    field_count: int = 0
    failed_tables: list[dict[str, str]] = field(default_factory=list)
    diagnostics: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Collection:
    items: tuple[MappingItem, ...]
    stats: CollectorStats


@dataclass(frozen=True, slots=True)
class _ResolvedColumn:
    source_table: str
    source_field: str


@dataclass(slots=True)
class _TableBuilder:
    source_table: str
    targets: set[str] = field(default_factory=set)
    fields: list[MappingField] = field(default_factory=list)
    field_identities: dict[tuple[str, str], MappingField] = field(default_factory=dict)
    conflicting_field: bool = False

    def add_field(self, target_table: str, mapping: MappingField) -> None:
        self.targets.add(target_table)
        identity = (mapping.source_field.casefold(), mapping.target_field.casefold())
        existing = self.field_identities.get(identity)
        if existing is None:
            self.field_identities[identity] = mapping
            self.fields.append(mapping)
        elif existing.mapping_rule != mapping.mapping_rule:
            self.conflicting_field = True


@dataclass(frozen=True, slots=True)
class _StatementResult:
    target_table: str
    source_tables: tuple[str, ...]
    field_mappings: tuple[tuple[str, MappingField], ...]
    unsupported: tuple[str, ...] = ()


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def resolve_source_system_id(
    explicit: int | None, environ: dict[str, str] | None = None
) -> int | None:
    """Only an explicit DAP primary key is authoritative; names/profiles are not."""

    if explicit is not None:
        if explicit <= 0:
            raise ValueError("source system ID must be a positive integer")
        return explicit
    values = os.environ if environ is None else environ
    raw = values.get("PYTOOLS_DAP_SOURCE_SYSTEM_ID", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(
            "PYTOOLS_DAP_SOURCE_SYSTEM_ID must be a positive integer"
        ) from error
    if value <= 0:
        raise ValueError("PYTOOLS_DAP_SOURCE_SYSTEM_ID must be a positive integer")
    return value


def _identifier_name(identifier: Any) -> str:
    name = getattr(identifier, "name", "")
    return str(name or "").strip().upper()


def _table_parts(table: Any) -> tuple[str, ...]:
    parts = getattr(table, "parts", ())
    normalized = tuple(_identifier_name(part) for part in parts)
    if not normalized or any(not part for part in normalized):
        return ()
    return normalized


def _canonical_table(table: Any) -> str | None:
    parts = _table_parts(table)
    return ".".join(parts) if parts else None


def _is_dwf_target(table: Any) -> bool:
    parts = _table_parts(table)
    if not parts:
        return False
    return any(
        part == "DWF" or part.startswith("DWF_") for part in parts[:-1]
    ) or parts[-1].startswith("DWF_")


def _is_known_non_ods_source(table: Any) -> bool:
    parts = _table_parts(table)
    for part in parts:
        if any(
            part == layer or part.startswith(f"{layer}_")
            for layer in KNOWN_WAREHOUSE_LAYERS
        ):
            return True
    return False


def _insert_target_and_columns(insert: Any) -> tuple[Any | None, list[str]]:
    target = insert.this
    if isinstance(target, exp.Schema):
        table = target.this
        columns = [_identifier_name(value) for value in target.expressions]
    else:
        table = target
        columns = []
    if not isinstance(table, exp.Table):
        return None, []
    if any(not column for column in columns):
        return None, []
    return table, columns


def _build_source_aliases(
    tables: Sequence[Any],
) -> tuple[dict[str, set[str]], dict[str, tuple[str, ...]]]:
    aliases: dict[str, set[str]] = defaultdict(set)
    parts_by_table: dict[str, tuple[str, ...]] = {}
    for table in tables:
        canonical = _canonical_table(table)
        parts = _table_parts(table)
        if not canonical or not parts:
            continue
        parts_by_table[canonical] = parts
        for label in (table.alias_or_name, table.name, canonical):
            normalized = str(label or "").strip().strip('`"[]').casefold()
            if normalized:
                aliases[normalized].add(canonical)
    return aliases, parts_by_table


def _resolve_column(
    column: Any,
    aliases: dict[str, set[str]],
    parts_by_table: dict[str, tuple[str, ...]],
) -> _ResolvedColumn | None:
    field_name = _identifier_name(column.this)
    if not field_name:
        return None
    qualifier = str(getattr(column, "table", "") or "").strip().strip('`"[]').casefold()
    source_candidates: set[str]
    if qualifier:
        source_candidates = set(aliases.get(qualifier, set()))
        qualifier_schema = (
            str(getattr(column, "db", "") or "").strip().strip('`"[]').upper()
        )
        qualifier_catalog = (
            str(getattr(column, "catalog", "") or "").strip().strip('`"[]').upper()
        )
        if len(source_candidates) > 1 and qualifier_schema:
            source_candidates = {
                source
                for source in source_candidates
                if len(parts_by_table[source]) >= 2
                and parts_by_table[source][-2] == qualifier_schema
            }
        if not source_candidates and qualifier_schema:
            wanted = tuple(
                part
                for part in (
                    qualifier_catalog,
                    qualifier_schema,
                    _identifier_name(column.args.get("table")),
                )
                if part
            )
            source_candidates = {
                source
                for source, parts in parts_by_table.items()
                if parts[-len(wanted) :] == wanted
            }
    else:
        source_candidates = set(parts_by_table)
    if len(source_candidates) != 1:
        return None
    source_table = next(iter(source_candidates))
    return _ResolvedColumn(source_table, field_name)


def _statement_to_mappings(insert: Any) -> _StatementResult | None:
    target, target_fields = _insert_target_and_columns(insert)
    if target is None or not _is_dwf_target(target):
        return None
    target_table = _canonical_table(target)
    if target_table is None:
        return _StatementResult("", (), (), ("unsupported DWF target identifier",))

    query = insert.expression
    if not isinstance(query, exp.Select):
        return _StatementResult(
            target_table, (), (), ("INSERT source is not a plain SELECT",)
        )
    if query.find(exp.With) is not None:
        return _StatementResult(target_table, (), (), ("CTE source is unsupported",))
    if (
        len(list(query.find_all(exp.Select))) != 1
        or query.find(exp.Subquery) is not None
    ):
        return _StatementResult(
            target_table, (), (), ("nested SELECT source is unsupported",)
        )

    source_expressions = list(query.find_all(exp.Table))
    tables: list[Any] = []
    seen_sources: set[str] = set()
    for table in source_expressions:
        canonical = _canonical_table(table)
        if (
            not canonical
            or not isinstance(table.this, exp.Identifier)
            or _is_known_non_ods_source(table)
        ):
            continue
        seen_sources.add(canonical)
        tables.append(table)
    source_tables = tuple(sorted(seen_sources))
    if not source_tables:
        return _StatementResult(
            target_table, (), (), ("no supported upstream source table",)
        )
    if not target_fields:
        return _StatementResult(
            target_table,
            source_tables,
            (),
            ("INSERT target columns are required to establish field order",),
        )
    select_fields = list(query.expressions)
    if len(select_fields) != len(target_fields):
        return _StatementResult(
            target_table,
            source_tables,
            (),
            ("INSERT target column count does not match SELECT field count",),
        )

    aliases, parts_by_table = _build_source_aliases(tables)
    mappings: list[tuple[str, MappingField]] = []
    unsupported: list[str] = []
    for field_order, (expression, target_field) in enumerate(
        zip(select_fields, target_fields, strict=True), start=1
    ):
        columns = list(expression.find_all(exp.Column))
        resolved = [
            _resolve_column(column, aliases, parts_by_table) for column in columns
        ]
        unique = {
            (item.source_table, item.source_field): item
            for item in resolved
            if item is not None
        }
        if len(unique) != 1 or any(item is None for item in resolved):
            unsupported.append(
                f"targetField={target_field}: source expression does not resolve to one unique source field"
            )
            continue
        source = next(iter(unique.values()))
        if _is_direct_column_expression(expression):
            rule = "DIRECT" if source.source_field == target_field else "RENAME"
        else:
            rule = "待补充"
        mappings.append(
            (
                source.source_table,
                MappingField(
                    source_field=source.source_field,
                    target_field=target_field,
                    mapping_rule=rule,
                    field_order=field_order,
                ),
            )
        )
    return _StatementResult(
        target_table, source_tables, tuple(mappings), tuple(unsupported)
    )


def _is_direct_column_expression(expression: Any) -> bool:
    value = expression.this if isinstance(expression, exp.Alias) else expression
    while isinstance(value, exp.Paren):
        value = value.this
    return isinstance(value, exp.Column)


def _iter_program_sql(path: Path) -> tuple[list[str], str | None]:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return [], "file could not be read as UTF-8"
    if path.suffix.lower() == ".sql":
        return [content], None
    extraction = _extract_python_candidates_with_reason(content)
    candidates = [candidate.text for candidate in extraction.candidates]
    if candidates:
        return candidates, None
    if extraction.reason.value in {
        "SQL_CALL_NOT_RECOGNIZED",
        "SQL_ARGUMENT_DYNAMIC",
        "SQL_ARGUMENT_MISSING",
        "PYTHON_PARSE_FAILED",
        "PYTHON_PARSE_RECOVERED",
    }:
        return [], f"Python SQL extraction unresolved: {extraction.reason.value}"
    return [], None


def discover_program_files(root: str | Path) -> list[Path]:
    workspace = Path(root).expanduser()
    if not workspace.is_dir():
        raise ValueError("workspace directory does not exist or is not a directory")
    files: list[Path] = []
    scan_errors: list[OSError] = []
    for current, directories, names in os.walk(workspace, onerror=scan_errors.append):
        directories[:] = sorted(
            name
            for name in directories
            if name not in IGNORED_DIRECTORIES and not name.startswith(".")
        )
        for name in sorted(names):
            path = Path(current) / name
            if path.suffix.lower() in SUPPORTED_SUFFIXES and path.is_file():
                files.append(path)
    if scan_errors:
        raise OSError(
            "workspace scan was incomplete because a directory was unreadable"
        )
    return sorted(files, key=lambda item: item.as_posix().casefold())


def collect_workspace(
    root: str | Path,
    *,
    source_system_id: int | None,
    dialect: str = "mysql",
) -> Collection:
    """Collect field facts only; this does not create or persist lineage records."""

    workspace = Path(root).expanduser().resolve()
    files = discover_program_files(workspace)
    stats = CollectorStats(scanned_files=len(files))
    builders: dict[str, _TableBuilder] = {}
    skipped_files: set[str] = set()
    unresolved_table_count = 0

    for path in files:
        relative = path.relative_to(workspace).as_posix()
        candidates, extraction_error = _iter_program_sql(path)
        if extraction_error:
            skipped_files.add(relative)
            stats.diagnostics.append(
                f"[skip-file] file={relative} reason={extraction_error}"
            )
            continue
        if not candidates:
            continue

        parsed_statements: list[Any] = []
        parse_failed = False
        for sql_text in candidates:
            try:
                parsed_statements.extend(
                    item for item in parse(sql_text, read=dialect) if item is not None
                )
            except (SqlglotError, TypeError, ValueError):
                parse_failed = True
                break
        if parse_failed:
            skipped_files.add(relative)
            stats.diagnostics.append(
                f"[skip-file] file={relative} reason=SQL parse failed for configured dialect"
            )
            continue

        found_insert = False
        found_dwf_insert = False
        for statement in parsed_statements:
            if not isinstance(statement, exp.Insert):
                continue
            found_insert = True
            target, _ = _insert_target_and_columns(statement)
            if target is None or not _is_dwf_target(target):
                continue
            found_dwf_insert = True
            result = _statement_to_mappings(statement)
            if result is None:
                continue
            if not result.source_tables:
                unresolved_table_count += 1
                for reason in result.unsupported:
                    stats.diagnostics.append(
                        f"[skip-table] file={relative} target={result.target_table} reason={reason}"
                    )
                continue
            for source_table in result.source_tables:
                builder = builders.setdefault(
                    source_table.casefold(), _TableBuilder(source_table=source_table)
                )
                builder.targets.add(result.target_table)
            for source_table, mapping in result.field_mappings:
                builder = builders[source_table.casefold()]
                builder.add_field(result.target_table, mapping)
            for reason in result.unsupported:
                stats.diagnostics.append(
                    f"[skip-field] file={relative} target={result.target_table} reason={reason}"
                )

        if found_dwf_insert:
            stats.candidate_programs += 1
        elif found_insert:
            skipped_files.add(relative)
            stats.diagnostics.append(
                f"[skip-file] file={relative} reason=no DWF target INSERT found"
            )

    if source_system_id is not None and source_system_id <= 0:
        raise ValueError("source system ID must be a positive integer")

    items: list[MappingItem] = []
    for key in sorted(builders):
        builder = builders[key]
        if len(builder.targets) != 1:
            stats.skipped_tables += 1
            stats.diagnostics.append(
                f"[skip-table] sourceTable={builder.source_table} reason=multiple DWF targets share DAP table identity"
            )
            continue
        if builder.conflicting_field:
            stats.skipped_tables += 1
            stats.diagnostics.append(
                f"[skip-table] sourceTable={builder.source_table} reason=conflicting duplicate field identity"
            )
            continue
        if not builder.fields:
            stats.skipped_tables += 1
            stats.diagnostics.append(
                f"[skip-table] sourceTable={builder.source_table} reason=no uniquely resolved fields"
            )
            continue
        if source_system_id is None:
            stats.skipped_tables += 1
            stats.diagnostics.append(
                f"[skip-table] sourceTable={builder.source_table} reason=missing explicit DAP sourceSystemId"
            )
            continue
        target_table = next(iter(builder.targets))
        item = MappingItem(
            source_system_id=source_system_id,
            source_table=builder.source_table,
            target_table=target_table,
            fields=tuple(builder.fields),
        )
        try:
            validate_mapping_item(item.to_contract())
        except ValueError as error:
            stats.skipped_tables += 1
            stats.diagnostics.append(
                f"[skip-table] sourceTable={builder.source_table} reason={error}"
            )
            continue
        items.append(item)

    stats.parsed_tables = len(items)
    stats.parsed_fields = sum(len(item.fields) for item in items)
    stats.skipped_files = len(skipped_files)
    stats.skipped_tables += unresolved_table_count
    return Collection(tuple(items), stats)


def filter_collection_by_source_tables(
    collection: Collection, source_tables: Sequence[str]
) -> Collection:
    """Restrict a rescan to exact source-table identities for item-level retry."""

    requested = {value.strip().casefold() for value in source_tables if value.strip()}
    if not requested:
        return collection
    items = tuple(
        item for item in collection.items if item.source_table.casefold() in requested
    )
    found = {item.source_table.casefold() for item in items}
    for missing in sorted(requested - found):
        collection.stats.diagnostics.append(
            f"[retry-filter] sourceTable={missing.upper()} was not found in current scan"
        )
    collection.stats.parsed_tables = len(items)
    collection.stats.parsed_fields = sum(len(item.fields) for item in items)
    return Collection(items, collection.stats)


def validate_mapping_item(item: dict[str, object]) -> None:
    source_system_id = item.get("sourceSystemId")
    if (
        not isinstance(source_system_id, int)
        or isinstance(source_system_id, bool)
        or source_system_id <= 0
    ):
        raise ValueError("sourceSystemId must be a positive integer")
    for key, maximum in (("sourceTable", 128), ("targetTable", 128)):
        value = item.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > maximum:
            raise ValueError(f"{key} must contain 1..{maximum} characters")
    optional_lengths = {
        "sourceTableCn": 256,
        "loadMode": 32,
        "tableDesc": 2_000,
    }
    for key, maximum in optional_lengths.items():
        value = item.get(key)
        if value is not None and (not isinstance(value, str) or len(value) > maximum):
            raise ValueError(f"{key} must contain at most {maximum} characters")
    if item.get("targetLayer") != "DWF":
        raise ValueError("targetLayer must be DWF")
    fields = item.get("fields")
    if not isinstance(fields, list) or not 1 <= len(fields) <= DAP_MAX_FIELDS_PER_ITEM:
        raise ValueError("fields must contain 1..1000 items")
    seen: set[tuple[str, str]] = set()
    for index, mapping in enumerate(fields, start=1):
        if not isinstance(mapping, dict):
            raise TypeError(f"fields[{index - 1}] must be an object")
        for key, maximum in (
            ("sourceField", 128),
            ("targetField", 128),
            ("mappingRule", 64),
        ):
            value = mapping.get(key)
            if not isinstance(value, str) or not value.strip() or len(value) > maximum:
                raise ValueError(
                    f"fields[{index - 1}].{key} must contain 1..{maximum} characters"
                )
        if (
            not isinstance(mapping.get("fieldOrder"), int)
            or isinstance(mapping["fieldOrder"], bool)
            or mapping["fieldOrder"] < 1
        ):
            raise ValueError(
                f"fields[{index - 1}].fieldOrder must be a positive integer"
            )
        for key, maximum in (("sourceType", 128), ("sourceComment", 1_000)):
            value = mapping.get(key)
            if value is not None and (
                not isinstance(value, str) or len(value) > maximum
            ):
                raise ValueError(f"fields[{index - 1}].{key} exceeds contract length")
        identity = (
            str(mapping["sourceField"]).casefold(),
            str(mapping["targetField"]).casefold(),
        )
        if identity in seen:
            raise ValueError(f"fields[{index - 1}] duplicates sourceField/targetField")
        seen.add(identity)


def build_import_payload(
    items: Sequence[MappingItem], *, server_dry_run: bool = False
) -> dict[str, object]:
    if not items:
        raise ValueError("at least one mapping item is required")
    if len(items) > DAP_MAX_ITEMS:
        raise ValueError("DAP accepts at most 500 mapping items per request")
    identities: set[tuple[int, str]] = set()
    serialized: list[dict[str, object]] = []
    for item in items:
        identity = item.identity
        if identity in identities:
            raise ValueError("duplicate sourceSystemId/sourceTable identity in request")
        identities.add(identity)
        contract_item = item.to_contract()
        validate_mapping_item(contract_item)
        serialized.append(contract_item)
    return {"mode": "upsert", "dryRun": server_dry_run, "items": serialized}


def split_batches(
    items: Sequence[MappingItem], batch_size: int
) -> list[list[MappingItem]]:
    if not 1 <= batch_size <= DAP_MAX_ITEMS:
        raise ValueError("batch size must be between 1 and 500")
    return [
        list(items[index : index + batch_size])
        for index in range(0, len(items), batch_size)
    ]


class FieldMappingApiError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


class FieldMappingApiClient:
    """DAP client using the deployment's authenticated signed session cookie."""

    def __init__(
        self,
        base_url: str,
        *,
        session_cookie: str,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        normalized_url = (base_url or "").strip().rstrip("/")
        parsed_url = urlsplit(normalized_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("DAP base URL must be an HTTP(S) URL")
        if parsed_url.username or parsed_url.password:
            raise ValueError("DAP base URL must not contain credentials")
        if parsed_url.query or parsed_url.fragment:
            raise ValueError("DAP base URL must not include a query or fragment")
        if not session_cookie.strip():
            raise ValueError("DAP_SESSION_COOKIE is required")
        if connect_timeout <= 0 or read_timeout <= 0:
            raise ValueError("HTTP timeouts must be greater than zero")
        if max_retries < 0 or retry_backoff < 0:
            raise ValueError("retry settings must not be negative")
        self._url = f"{normalized_url}{DAP_IMPORT_PATH}"
        self._cookie = session_cookie.strip()
        self._timeout = (connect_timeout, read_timeout)
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._sleep = sleep
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._headers = {"Accept": "application/json", "Cookie": self._cookie}

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.close()

    def import_mappings(self, payload: dict[str, object]) -> dict[str, Any]:
        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.post(
                    self._url,
                    json=payload,
                    headers=self._headers,
                    timeout=self._timeout,
                )
            except requests.exceptions.Timeout:
                failure = FieldMappingApiError(
                    "CONNECTION_TIMEOUT", "DAP request timed out", retryable=True
                )
            except requests.exceptions.SSLError as error:
                raise FieldMappingApiError(
                    "TLS_ERROR", "DAP TLS connection failed"
                ) from error
            except requests.exceptions.ConnectionError:
                failure = FieldMappingApiError(
                    "NETWORK_ERROR", "DAP connection failed", retryable=True
                )
            except requests.exceptions.RequestException as error:
                raise FieldMappingApiError(
                    "REQUEST_ERROR", "DAP request could not be completed"
                ) from error
            else:
                if response.status_code in {502, 503, 504}:
                    failure = FieldMappingApiError(
                        f"HTTP_{response.status_code}",
                        f"temporary DAP HTTP {response.status_code}",
                        retryable=True,
                    )
                elif response.status_code >= 400:
                    raise self._http_error(response)
                else:
                    return self._validate_response(response, payload)
            if attempt >= self._max_retries or not failure.retryable:
                raise failure
            self._sleep(self._retry_backoff * (2**attempt))
        raise FieldMappingApiError("NETWORK_ERROR", "DAP request failed")

    def _http_error(self, response: Any) -> FieldMappingApiError:
        code = f"HTTP_{response.status_code}"
        message = "DAP rejected the request"
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            error = body["error"]
            if isinstance(error.get("code"), str) and error["code"].strip():
                code = self._redact(error["code"].strip())
            if isinstance(error.get("message"), str) and error["message"].strip():
                message = self._redact(error["message"].strip())
        return FieldMappingApiError(code, message)

    def _validate_response(
        self, response: Any, payload: dict[str, object]
    ) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError as error:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_JSON", "DAP returned invalid JSON"
            ) from error
        if not isinstance(data, dict) or data.get("mode") != "upsert":
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response did not match import contract",
            )
        summary = data.get("summary")
        items = data.get("items")
        if not isinstance(summary, dict) or not isinstance(items, list):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response did not match import contract",
            )
        expected_dry_run = bool(payload.get("dryRun", False))
        if data.get("dryRun") is not expected_dry_run:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP dryRun response did not match request"
            )
        for key in (
            "received",
            "created",
            "updated",
            "unchanged",
            "failed",
            "fieldCount",
        ):
            if not isinstance(summary.get(key), int):
                raise FieldMappingApiError(
                    "INVALID_RESPONSE_CONTRACT", "DAP response summary is incomplete"
                )
        if summary["received"] != len(payload.get("items", [])):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response received count did not match request",
            )
        return data

    def redact_message(self, message: str) -> str:
        return message.replace(self._cookie, "[REDACTED]")

    def _redact(self, message: str) -> str:
        return self.redact_message(message)


def _response_items(
    response: dict[str, Any], expected: Sequence[MappingItem]
) -> list[dict[str, Any]]:
    expected_count = len(expected)
    items = response.get("items")
    if not isinstance(items, list) or len(items) != expected_count:
        raise FieldMappingApiError(
            "INVALID_RESPONSE_CONTRACT", "DAP response item count did not match request"
        )
    by_index: dict[int, dict[str, Any]] = {}
    for result in items:
        if not isinstance(result, dict):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item is invalid"
            )
        index = result.get("index")
        action = result.get("action")
        if not isinstance(index, int) or index < 0 or index >= expected_count:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item index is invalid"
            )
        if index in by_index or action not in VALID_ACTIONS:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item identity is invalid"
            )
        identity = result.get("identity")
        response_system_id = (
            identity.get("sourceSystemId") or identity.get("upstreamSystemId")
            if isinstance(identity, dict)
            else None
        )
        response_source_table = (
            identity.get("sourceTable") if isinstance(identity, dict) else None
        )
        requested = expected[index]
        if (
            response_system_id != requested.source_system_id
            or not isinstance(response_source_table, str)
            or response_source_table.casefold() != requested.source_table.casefold()
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response identity did not match request",
            )
        by_index[index] = result
    if len(by_index) != expected_count:
        raise FieldMappingApiError(
            "INVALID_RESPONSE_CONTRACT", "DAP response item indexes are incomplete"
        )
    return [by_index[index] for index in range(expected_count)]


def _record_item_result(
    item: MappingItem,
    result: dict[str, Any],
    stats: CollectorStats,
    *,
    redact: Callable[[str], str] = lambda value: value,
) -> None:
    action = result["action"]
    if action in {"created", "updated", "unchanged"}:
        setattr(stats, action, getattr(stats, action) + 1)
        return
    stats.failed += 1
    error = result.get("error")
    code = error.get("code") if isinstance(error, dict) else "ITEM_FAILED"
    message = (
        error.get("message") if isinstance(error, dict) else "DAP marked item failed"
    )
    code = redact(str(code or "ITEM_FAILED"))
    message = redact(str(message or "DAP marked item failed"))
    stats.failed_tables.append(
        {"sourceTable": item.source_table, "errorCode": code, "message": message}
    )
    print(
        f"[failed] sourceSystemId={item.source_system_id} sourceTable={item.source_table} "
        f"errorCode={code} message={message}",
        file=sys.stderr,
    )


def submit_batches(
    items: Sequence[MappingItem],
    client: FieldMappingApiClient,
    *,
    batch_size: int,
    server_dry_run: bool,
    stats: CollectorStats,
) -> None:
    batches = split_batches(items, batch_size)
    for batch_number, batch in enumerate(batches, start=1):
        payload = build_import_payload(batch, server_dry_run=server_dry_run)
        stats.batches += 1
        stats.field_count += sum(len(item.fields) for item in batch)
        try:
            response = client.import_mappings(payload)
            results = _response_items(response, batch)
        except FieldMappingApiError as error:
            for item in batch:
                stats.failed += 1
                stats.failed_tables.append(
                    {
                        "sourceTable": item.source_table,
                        "errorCode": error.code,
                        "message": error.message,
                    }
                )
                print(
                    f"[failed] sourceSystemId={item.source_system_id} sourceTable={item.source_table} "
                    f"errorCode={error.code} message={error.message}",
                    file=sys.stderr,
                )
            print(f"[batch] number={batch_number} failed={len(batch)}", file=sys.stderr)
            continue
        redactor = (
            client.redact_message
            if isinstance(client, FieldMappingApiClient)
            else (lambda value: value)
        )
        for item, result in zip(batch, results, strict=True):
            _record_item_result(item, result, stats, redact=redactor)
        print(
            f"[batch] number={batch_number} received={len(batch)} "
            f"created={sum(row['action'] == 'created' for row in results)} "
            f"updated={sum(row['action'] == 'updated' for row in results)} "
            f"unchanged={sum(row['action'] == 'unchanged' for row in results)} "
            f"failed={sum(row['action'] == 'failed' for row in results)}"
        )


def _configured_batch_size(cli_value: int | None) -> int:
    if cli_value is not None:
        value = cli_value
    else:
        raw = os.environ.get("PYTOOLS_DAP_MAPPING_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))
        try:
            value = int(raw)
        except ValueError as error:
            raise ValueError(
                "PYTOOLS_DAP_MAPPING_BATCH_SIZE must be an integer"
            ) from error
    if not 1 <= value <= DAP_MAX_ITEMS:
        raise ValueError("batch size must be between 1 and DAP's 500-item limit")
    return value


def _environment_timeout(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be a positive number") from error
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def _retry_count() -> int:
    raw = os.environ.get("PYTOOLS_DAP_MAPPING_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(
            "PYTOOLS_DAP_MAPPING_MAX_RETRIES must be an integer"
        ) from error
    if not 0 <= value <= 5:
        raise ValueError("PYTOOLS_DAP_MAPPING_MAX_RETRIES must be between 0 and 5")
    return value


def write_local_preview(
    output_path: str | Path,
    items: Sequence[MappingItem],
    *,
    batch_size: int,
) -> None:
    batches = [
        build_import_payload(batch, server_dry_run=True)
        for batch in split_batches(items, batch_size)
    ]
    preview = {"executionMode": "local-dry-run", "batches": batches}
    Path(output_path).expanduser().write_text(
        json.dumps(preview, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def print_stats(stats: CollectorStats, *, dry_run_mode: str) -> None:
    print(
        "Stats: "
        f"dry_run={str(dry_run_mode != 'none').lower()} "
        f"dry_run_mode={dry_run_mode} "
        f"scanned_files={stats.scanned_files} "
        f"candidate_programs={stats.candidate_programs} "
        f"parsed_tables={stats.parsed_tables} "
        f"parsed_fields={stats.parsed_fields} "
        f"skipped_files={stats.skipped_files} "
        f"skipped_tables={stats.skipped_tables} "
        f"batches={stats.batches} "
        f"created={stats.created} updated={stats.updated} unchanged={stats.unchanged} "
        f"failed={stats.failed} field_count={stats.field_count} "
        f"failed_tables={json.dumps(stats.failed_tables, ensure_ascii=False)}"
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect ODS/upstream -> DWF field mappings and upsert via DAP API."
    )
    parser.add_argument(
        "--directory",
        default=os.environ.get("PYTOOLS_DAP_MAPPING_WORKSPACE"),
        help="Workspace/program root (.py and .sql are scanned); may use PYTOOLS_DAP_MAPPING_WORKSPACE.",
    )
    parser.add_argument(
        "--source-system-id",
        type=positive_int,
        default=None,
        help="Explicit DAP upstream-system primary key; may use PYTOOLS_DAP_SOURCE_SYSTEM_ID.",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("DAP_API_BASE_URL", ""),
        help="DAP base URL; required for real sync and --server-dry-run.",
    )
    parser.add_argument(
        "--sql-dialect",
        default=os.environ.get("PYTOOLS_DAP_MAPPING_SQL_DIALECT", "mysql"),
        help="SQLGlot dialect (default: mysql); override per workspace if needed.",
    )
    parser.add_argument("--batch-size", type=positive_int, default=None)
    parser.add_argument(
        "--source-table",
        action="append",
        default=[],
        help="Retry only this exact canonical sourceTable; repeat for multiple tables.",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--dry-run",
        action="store_true",
        help="Local scan/parse/validate only; makes no HTTP request.",
    )
    modes.add_argument(
        "--server-dry-run",
        action="store_true",
        help="Call DAP with dryRun=true; DAP validates without persistence.",
    )
    parser.add_argument(
        "--payload-output",
        help="Write local dry-run preview JSON (an envelope of server dryRun=true batches).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if not args.directory:
        parser.error("--directory or PYTOOLS_DAP_MAPPING_WORKSPACE is required")
    if args.payload_output and not args.dry_run:
        parser.error("--payload-output is only available with local --dry-run")

    try:
        source_system_id = resolve_source_system_id(args.source_system_id)
        batch_size = _configured_batch_size(args.batch_size)
        collection = collect_workspace(
            args.directory,
            source_system_id=source_system_id,
            dialect=args.sql_dialect,
        )
        collection = filter_collection_by_source_tables(collection, args.source_table)
    except (ValueError, OSError) as error:
        print(f"[error] {error}", file=sys.stderr)
        return 2

    for diagnostic in collection.stats.diagnostics:
        print(diagnostic, file=sys.stderr)

    stats = collection.stats
    if not collection.items:
        print(
            "[no-mappings] no valid DAP mapping items were collected; "
            + (
                "provide an explicit PYTOOLS_DAP_SOURCE_SYSTEM_ID/--source-system-id and verify that the workspace is scoped to one upstream system"
                if source_system_id is None and stats.candidate_programs > 0
                else "all candidate tables were unsupported, ambiguous, or invalid"
                if stats.candidate_programs
                else "no DWF INSERT...SELECT candidate was found in the scanned .py/.sql files"
            ),
            file=sys.stderr,
        )
        print_stats(
            stats,
            dry_run_mode="local"
            if args.dry_run
            else "server"
            if args.server_dry_run
            else "none",
        )
        return 2

    if args.dry_run:
        if args.payload_output:
            try:
                write_local_preview(
                    args.payload_output, collection.items, batch_size=batch_size
                )
            except OSError as error:
                print(
                    f"[error] local preview could not be written: {error}",
                    file=sys.stderr,
                )
                return 2
            print(f"Local dry-run payload preview written: {args.payload_output}")
        else:
            print("Local dry-run: payloads validated; no HTTP request was made.")
        print_stats(stats, dry_run_mode="local")
        return 0

    base_url = (args.api_base_url or "").strip()
    session_cookie = os.environ.get("DAP_SESSION_COOKIE", "").strip()
    if not base_url:
        print("[error] --api-base-url or DAP_API_BASE_URL is required", file=sys.stderr)
        print_stats(stats, dry_run_mode="server" if args.server_dry_run else "none")
        return 2
    if not session_cookie:
        print(
            "[error] DAP_SESSION_COOKIE is required (signed DAP session cookie)",
            file=sys.stderr,
        )
        print_stats(stats, dry_run_mode="server" if args.server_dry_run else "none")
        return 2

    try:
        connect_timeout = _environment_timeout(
            "PYTOOLS_DAP_MAPPING_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT
        )
        read_timeout = _environment_timeout(
            "PYTOOLS_DAP_MAPPING_READ_TIMEOUT", DEFAULT_READ_TIMEOUT
        )
        retries = _retry_count()
        with FieldMappingApiClient(
            base_url,
            session_cookie=session_cookie,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_retries=retries,
        ) as client:
            submit_batches(
                collection.items,
                client,
                batch_size=batch_size,
                server_dry_run=args.server_dry_run,
                stats=stats,
            )
    except ValueError as error:
        print(f"[error] {error}", file=sys.stderr)
        return 2
    except FieldMappingApiError as error:
        print(f"[error] {error.code}: {error.message}", file=sys.stderr)
        return 1

    print_stats(stats, dry_run_mode="server" if args.server_dry_run else "none")
    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
