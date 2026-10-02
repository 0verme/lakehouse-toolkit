from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .models import (
    DapUpstreamSystem,
    DwoSource,
    RecvDwfRecord,
    Resolution,
    SchemaConfigRecord,
)


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


def parse_dwo_physical_table(
    physical_table: str,
    schema_configs: Iterable[SchemaConfigRecord],
) -> tuple[DwoSource | None, str | None]:
    """Split DWO_<db_schema>_<source_table> using longest exact schema prefix."""

    parts = _identifier_parts(physical_table)
    if len(parts) < 2 or parts[-2] != "DWO" or not parts[-1].startswith("DWO_"):
        return None, "no_dwo_source"
    encoded = parts[-1][4:]
    candidates = {
        config.db_schema.strip().upper()
        for config in schema_configs
        if config.db_schema.strip()
        and encoded.casefold().startswith((config.db_schema.strip() + "_").casefold())
    }
    if not candidates:
        return None, "no_schema_config"
    longest = max(len(schema) for schema in candidates)
    best = sorted(schema for schema in candidates if len(schema) == longest)
    if len(best) != 1:
        return None, "schema_match_conflict"
    db_schema = best[0]
    source_table = encoded[len(db_schema) + 1 :]
    if not source_table:
        return None, "no_dwo_source"
    return DwoSource(
        physical_table=".".join(parts), db_schema=db_schema, source_table=source_table
    ), None


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

    def resolve(
        self,
        *,
        target: str,
        program_names: Iterable[str],
        physical_source: str,
    ) -> Resolution:
        logical_target = normalize_logical_target(target)
        target_rows = [
            row
            for row in self.recv_dwf
            if normalize_logical_target(row.table_name) == logical_target
        ]
        if not target_rows:
            return Resolution(status="UNRESOLVED", reason="no_recv_dwf")

        program_keys = {normalize_program_name(name) for name in program_names}
        program_rows = [
            row
            for row in self.recv_dwf
            if normalize_program_name(row.ods_job_name)
            and normalize_program_name(row.ods_job_name) in program_keys
        ]
        target_program_rows = [
            row
            for row in program_rows
            if normalize_logical_target(row.table_name) == logical_target
        ]
        if program_rows and not target_program_rows:
            return Resolution(status="CONFLICT", reason="program_metadata_conflict")
        if target_program_rows:
            target_identities = {
                (row.recv_plan.casefold(), row.data_source.casefold())
                for row in target_rows
            }
            matched_identities = {
                (row.recv_plan.casefold(), row.data_source.casefold())
                for row in target_program_rows
            }
            if not target_identities.intersection(matched_identities):
                return Resolution(status="CONFLICT", reason="program_metadata_conflict")
            candidates = target_program_rows
            evidence = ["ods_job_name", "table_name"]
        else:
            candidates = target_rows
            evidence = ["table_name"]

        source, source_error = parse_dwo_physical_table(
            physical_source, self.schema_configs
        )
        if source_error:
            return Resolution(
                status="UNRESOLVED", reason=source_error, evidence=tuple(evidence)
            )

        schema_compatible: list[RecvDwfRecord] = []
        missing_schema: list[RecvDwfRecord] = []
        for row in candidates:
            configured = self.schemas_by_key.get(row.data_source.casefold(), set())
            if not configured:
                missing_schema.append(row)
            elif source and source.db_schema.casefold() in {
                schema.casefold() for schema in configured
            }:
                schema_compatible.append(row)
        if not schema_compatible:
            if missing_schema:
                return Resolution(
                    status="UNRESOLVED",
                    reason="no_schema_config",
                    source=source,
                    evidence=tuple(evidence),
                )
            return Resolution(
                status="CONFLICT",
                reason="schema_match_conflict",
                source=source,
                evidence=tuple(evidence + ["db_schema"]),
            )
        candidates = schema_compatible
        evidence.append("db_schema")

        identities = {
            (row.recv_plan.casefold(), row.data_source.casefold()) for row in candidates
        }
        recv_plans = {identity[0] for identity in identities}
        data_sources = {identity[1] for identity in identities}
        if len(recv_plans) > 1:
            return Resolution(
                status="CONFLICT",
                reason="multiple_recv_plan_conflict",
                source=source,
                evidence=tuple(evidence),
            )
        if len(data_sources) > 1:
            return Resolution(
                status="CONFLICT",
                reason="multiple_data_source_conflict",
                source=source,
                evidence=tuple(evidence),
            )

        # Identical candidates are equivalent metadata evidence; no conflicting row is
        # selected by order because recv_plan/data_source/table identity is now unique.
        # Multiple metadata rows with the same recv_plan/data_source are acceptable
        # only after table, program (when matched), and db_schema evidence have been
        # evaluated. Their remaining row-level differences do not change the business
        # identity consumed by the collector.
        record = min(
            candidates,
            key=lambda row: (
                row.recv_plan.casefold(),
                row.data_source.casefold(),
                normalize_program_name(row.ods_job_name),
                row.ods_job_name.casefold(),
            ),
        )
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
    "canonical_dwf_target",
    "load_metadata_snapshot",
    "load_upstream_snapshot",
    "load_upstream_systems",
    "normalize_logical_target",
    "normalize_program_name",
    "parse_dwo_physical_table",
]
