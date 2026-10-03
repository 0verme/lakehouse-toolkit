from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
from typing import Any

from .models import (
    DapUpstreamSystem,
    DwoSource,
    RecvDwfRecord,
    Resolution,
    SchemaConfigRecord,
)

RECV_PLAN_PREFIX = "PLAN_SA_RECV_"


class MetadataInputError(ValueError):
    pass


def _text(value: Any) -> str:
    return str(value or "").strip()


def _field(row: dict[str, Any], *names: str) -> str:
    lowered = {str(key).casefold(): value for key, value in row.items()}
    for name in names:
        value = lowered.get(name.casefold())
        if value is not None:
            return _text(value)
    return ""


def _rows(value: Any, name: str) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        for key in (name, f"{name}_rows", f"p_{name}"):
            candidate = value.get(key)
            if isinstance(candidate, list):
                value = candidate
                break
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise MetadataInputError(f"metadata {name} must be a list of objects")
    return value


def load_metadata_snapshot(
    path: str | Path,
) -> tuple[list[RecvDwfRecord], list[SchemaConfigRecord]]:
    """Load a whitelisted metadata export; connection strings are never consumed."""

    try:
        payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise MetadataInputError(
            "metadata snapshot could not be read as JSON"
        ) from error
    if not isinstance(payload, dict):
        raise MetadataInputError("metadata snapshot root must be an object")

    raw_recv = payload.get(
        "recv_dwf_rows", payload.get("recv_dwf", payload.get("p_recv_dwf"))
    )
    raw_schema = payload.get(
        "schema_config_rows",
        payload.get("schema_config", payload.get("p_schema_config")),
    )
    recv_rows = _rows(raw_recv, "recv_dwf")
    schema_rows = _rows(raw_schema, "schema_config")
    recv = [
        RecvDwfRecord(
            recv_plan=_field(row, "recv_plan"),
            table_name=_field(row, "table_name"),
            data_source=_field(row, "data_source"),
            recv_job_name=_field(row, "recv_job_name"),
            ods_job_name=_field(row, "ods_job_name"),
        )
        for row in recv_rows
    ]
    schemas = [
        SchemaConfigRecord(
            schema_key=_field(row, "schema_key"),
            db_schema=_field(row, "db_schema"),
        )
        for row in schema_rows
    ]
    if any(
        not item.recv_plan or not item.table_name or not item.data_source
        for item in recv
    ):
        raise MetadataInputError(
            "p_recv_dwf rows require recv_plan, table_name and data_source"
        )
    if any(not item.schema_key or not item.db_schema for item in schemas):
        raise MetadataInputError(
            "p_schema_config rows require schema_key and db_schema"
        )
    return recv, schemas


def normalize_logical_target(value: str) -> str:
    """Normalize history (DWF.F_*) and source-code (DWF_*) names for matching only."""

    text = _text(value).strip('`"[]').upper()
    parts = [part.strip('`"[]') for part in text.split(".") if part.strip()]
    if not parts:
        return ""
    name = parts[-1].upper()
    if name.startswith("DWF_"):
        name = name[4:]
    elif name.startswith("F_"):
        name = name[2:]
    return name


def canonical_dwf_target(value: str) -> str:
    logical = normalize_logical_target(value)
    return f"DWF_{logical}" if logical else ""


def normalize_program_name(value: str) -> str:
    """Normalize `005_*.py` and `JOB_*_DAY` without fuzzy substring matching."""

    name = Path(_text(value)).name
    if "." in name:
        name = name.rsplit(".", 1)[0]
    normalized = name.upper().strip()
    while True:
        stripped = re.sub(r"^(?:JOB_|\d+_)", "", normalized)
        if stripped == normalized:
            break
        normalized = stripped
    if normalized.startswith("DWS_DWS_"):
        normalized = "DWS_" + normalized[len("DWS_DWS_") :]
    normalized = re.sub(r"_(?:DAY|NIGHT)$", "", normalized)
    return normalized


def _identifier_parts(physical_table: str) -> tuple[str, ...]:
    return tuple(
        part.strip().strip('`"[]').upper()
        for part in _text(physical_table).split(".")
        if part.strip()
    )


def derive_recv_namespace(recv_plan: str, data_source: str) -> str | None:
    """Derive the DWO landing namespace from the full ``recv_plan`` + ``data_source``.

    The receiving plan is modeled as::

        recv_plan = PLAN_SA_RECV_<recv_namespace>_<data_source>[_<suffix>...]

    Both the complete ``recv_plan`` and the complete ``data_source`` are used as
    anchors: ``data_source`` must occur as a whole underscore-delimited token
    sequence inside ``recv_plan`` and the tokens before it form the namespace.
    The trailing suffix is deliberately not interpreted, so ``DAY``, ``NIGHT``,
    ``PRO`` and future suffixes are all allowed.

    The function fails closed (returns ``None``) when the plan does not follow
    the anchor shape, when the namespace would be empty, or when more than one
    legal split exists. Callers must never fall back to a guessed namespace.
    """

    plan = _text(recv_plan).upper()
    source = _text(data_source).upper()
    if not plan.startswith(RECV_PLAN_PREFIX) or not source:
        return None
    remainder = plan[len(RECV_PLAN_PREFIX) :]
    plan_tokens = tuple(remainder.split("_"))
    source_tokens = tuple(source.split("_"))
    if not remainder or any(not token for token in plan_tokens):
        return None
    if any(not token for token in source_tokens):
        return None
    width = len(source_tokens)
    namespaces: set[str] = set()
    for index in range(len(plan_tokens) - width + 1):
        if plan_tokens[index : index + width] != source_tokens:
            continue
        namespace = "_".join(plan_tokens[:index])
        if namespace:
            namespaces.add(namespace)
    if len(namespaces) != 1:
        return None
    return namespaces.pop()


def match_dwo_source(physical_table: str, recv_namespace: str) -> DwoSource | None:
    """Split ``DWO_<recv_namespace>_<source_table>`` with namespace boundary matching.

    Only ``encoded == namespace`` or ``encoded.startswith(namespace + "_")`` is
    accepted, so a namespace such as ``CBS`` never matches ``CBSX`` or ``CBS2``.
    Multi-token namespaces (for example ``NUPS_DATA``) are stripped as a whole.
    """

    parts = _identifier_parts(physical_table)
    if len(parts) < 2 or parts[-2] != "DWO" or not parts[-1].startswith("DWO_"):
        return None
    encoded = parts[-1][4:]
    namespace = _text(recv_namespace).upper()
    if not namespace:
        return None
    if encoded == namespace:
        return None
    if not encoded.startswith(namespace + "_"):
        return None
    source_table = encoded[len(namespace) + 1 :]
    if not source_table:
        return None
    return DwoSource(
        physical_table=".".join(parts),
        recv_namespace=namespace,
        source_table=source_table,
    )


def _upstream_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            payload = payload["items"]
        elif isinstance(payload.get("data"), dict) and isinstance(
            payload["data"].get("items"), list
        ):
            payload = payload["data"]["items"]
    if not isinstance(payload, list) or any(
        not isinstance(row, dict) for row in payload
    ):
        raise MetadataInputError(
            "DAP upstream systems response must contain an items array"
        )
    return payload


def load_upstream_snapshot(path: str | Path) -> Any:
    try:
        return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise MetadataInputError(
            "DAP upstream systems snapshot could not be read as JSON"
        ) from error


def load_upstream_systems(payload: Any) -> tuple[list[DapUpstreamSystem], int | None]:
    rows = _upstream_rows(payload)
    systems: list[DapUpstreamSystem] = []
    for row in rows:
        identity = _field(row, "system_id", "systemId", "id")
        raw_id = _field(
            row, "upstreamSystemId", "sourceSystemId", "system_pk", "systemPk"
        )
        if not identity:
            continue
        try:
            system_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if system_id > 0:
            systems.append(
                DapUpstreamSystem(identity=identity, upstream_system_id=system_id)
            )
    return systems, len(rows)


class MetadataResolver:
    """Resolve DWO physical fields to DWF targets using recv namespace identity.

    Candidate selection starts from the logical DWF target, derives each
    candidate's DWO landing namespace from the full ``recv_plan`` +
    ``data_source`` pair, and then boundary-matches the physical DWO relation.
    Program names and ``p_schema_config.db_schema`` are auxiliary evidence only;
    neither one can decide upstream identity.
    """

    def __init__(
        self,
        recv_dwf: Iterable[RecvDwfRecord],
        schema_configs: Iterable[SchemaConfigRecord],
        upstream_payload: Any,
    ) -> None:
        self.recv_dwf = tuple(recv_dwf)
        self.schema_configs = tuple(schema_configs)
        self.upstream_systems, self.upstream_system_count = load_upstream_systems(
            upstream_payload
        )
        self.schemas_by_key: dict[str, set[str]] = defaultdict(set)
        for config in self.schema_configs:
            self.schemas_by_key[config.schema_key.casefold()].add(
                config.db_schema.upper()
            )
        self.systems_by_identity: dict[str, set[int]] = defaultdict(set)
        for system in self.upstream_systems:
            self.systems_by_identity[system.identity.casefold()].add(
                system.upstream_system_id
            )

        recv_by_logical_target: dict[str, list[RecvDwfRecord]] = defaultdict(list)
        recv_namespace_by_record: dict[RecvDwfRecord, str | None] = {}
        normalized_program_name_by_record: dict[RecvDwfRecord, str] = {}
        for row in self.recv_dwf:
            logical_target = normalize_logical_target(row.table_name)
            recv_by_logical_target[logical_target].append(row)
            recv_namespace_by_record[row] = derive_recv_namespace(
                row.recv_plan, row.data_source
            )
            normalized_program_name_by_record[row] = normalize_program_name(
                row.ods_job_name
            )
        self.recv_by_logical_target = {
            key: tuple(rows) for key, rows in recv_by_logical_target.items()
        }
        self.recv_namespace_by_record = recv_namespace_by_record
        self._normalized_program_name_by_record = normalized_program_name_by_record
        self._resolution_cache: dict[
            tuple[str, tuple[str, ...], tuple[str, ...]], Resolution
        ] = {}

    def resolve(
        self,
        *,
        target: str,
        program_names: Iterable[str],
        physical_source: str,
    ) -> Resolution:
        logical_target = normalize_logical_target(target)
        target_rows = self.recv_by_logical_target.get(logical_target, ())
        if not target_rows:
            return Resolution(status="UNRESOLVED", reason="no_recv_dwf")

        normalized_program_names = tuple(
            sorted({normalize_program_name(name) for name in program_names})
        )
        normalized_source = _identifier_parts(physical_source)
        cache_key = (logical_target, normalized_program_names, normalized_source)
        cached = self._resolution_cache.get(cache_key)
        if cached is not None:
            return cached
        resolution = self._resolve_indexed(
            target_rows=target_rows,
            program_keys=frozenset(normalized_program_names),
            physical_source=physical_source,
        )
        self._resolution_cache[cache_key] = resolution
        return resolution

    def _schema_db_schema(self, data_source: str) -> str | None:
        """Return p_schema_config.db_schema for one data_source, never an arbitrary pick."""

        configured = self.schemas_by_key.get(data_source.casefold())
        if not configured or len(configured) != 1:
            return None
        return next(iter(configured))

    def _resolve_indexed(
        self,
        *,
        target_rows: tuple[RecvDwfRecord, ...],
        program_keys: frozenset[str],
        physical_source: str,
    ) -> Resolution:
        parts = _identifier_parts(physical_source)
        if len(parts) < 2 or parts[-2] != "DWO" or not parts[-1].startswith("DWO_"):
            return Resolution(status="UNRESOLVED", reason="no_dwo_source")

        matched: dict[tuple[str, str], list[RecvDwfRecord]] = defaultdict(list)
        matched_sources: dict[tuple[str, str], DwoSource] = {}
        namespace_unresolved_rows = 0
        for row in target_rows:
            namespace = self.recv_namespace_by_record[row]
            if not namespace:
                namespace_unresolved_rows += 1
                continue
            source = match_dwo_source(physical_source, namespace)
            if source is None:
                continue
            identity = (row.recv_plan.casefold(), row.data_source.casefold())
            matched[identity].append(row)
            matched_sources[identity] = source

        if not matched:
            if namespace_unresolved_rows == len(target_rows):
                return Resolution(
                    status="UNRESOLVED",
                    reason="recv_namespace_unresolved",
                    evidence=("table_name", "recv_namespace"),
                )
            return Resolution(
                status="UNRESOLVED",
                reason="no_recv_namespace_match",
                evidence=("table_name", "recv_namespace"),
            )
        if len(matched) > 1:
            return Resolution(
                status="CONFLICT",
                reason="multiple_recv_namespace_conflict",
                evidence=("table_name", "recv_namespace"),
            )

        identity, identity_rows = next(iter(matched.items()))
        # Multiple metadata rows with the same (recv_plan, data_source) are
        # equivalent evidence; choose a deterministic representative row only.
        record = min(
            identity_rows,
            key=lambda row: (
                row.recv_plan.casefold(),
                row.data_source.casefold(),
                self._normalized_program_name_by_record[row],
                row.ods_job_name.casefold(),
            ),
        )
        source = replace(
            matched_sources[identity],
            db_schema=self._schema_db_schema(record.data_source),
        )

        evidence = ["table_name", "recv_namespace"]
        if any(
            self._normalized_program_name_by_record[row] in program_keys
            for row in identity_rows
        ):
            evidence.append("ods_job_name")

        matching_systems = self.systems_by_identity.get(
            record.recv_plan.casefold(), set()
        )
        if not matching_systems:
            return Resolution(
                status="UNRESOLVED",
                reason="unknown_upstream_system",
                record=record,
                source=source,
                evidence=tuple(evidence),
            )
        if len(matching_systems) != 1:
            return Resolution(
                status="CONFLICT",
                reason="upstream_system_conflict",
                record=record,
                source=source,
                evidence=tuple(evidence),
            )
        return Resolution(
            status="RESOLVED",
            record=record,
            source=source,
            upstream_system_id=next(iter(matching_systems)),
            evidence=tuple(evidence + ["dap_upstream_system"]),
        )


__all__ = [
    "MetadataInputError",
    "MetadataResolver",
    "RECV_PLAN_PREFIX",
    "canonical_dwf_target",
    "derive_recv_namespace",
    "load_metadata_snapshot",
    "load_upstream_snapshot",
    "load_upstream_systems",
    "match_dwo_source",
    "normalize_logical_target",
    "normalize_program_name",
]
