"""Pure projection from toolkit active business lineage to DAP Contract V1.

This module does not connect to DWS, read secrets, import DAP code, or make
HTTP requests.  Its compatibility limits mirror DAP's public Contract V1
metadata_ingestion.py and are not toolkit business limits.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from .domain import DatasetIdentity, LineageEdge, ProgramIdentity
from .materialization_dws import (
    DWSActiveSnapshotMetadata,
    DWSBusinessEdgeRow,
    business_edge_key,
    program_key,
)

DAP_LINEAGE_CONTRACT_VERSION = "1.0"
DAP_LINEAGE_ADAPTER_VERSION = "dap-lineage-adapter-v1"
DAP_LINEAGE_SOURCE_TYPE = "lakehouse-toolkit"
DAP_LINEAGE_COLLECTOR_NAME = "lakehouse-toolkit-dap-lineage"

# Audited from DAP's public backend/app/contracts/metadata_ingestion.py on
# 2026-09-30 (DAP main 927c403511f4774e3260fa99b044358be5c1b299).
# These are compatibility preflight defaults, not toolkit business rules.
DAP_V1_MAX_LINEAGE_NODES = 10_000
DAP_V1_MAX_LINEAGE_EDGES = 20_000
DAP_V1_MAX_METADATA_BODY_BYTES = 8 * 1024 * 1024

DAP_V1_MAX_EXTERNAL_ID_LENGTH = 256
DAP_V1_MAX_QUALIFIED_NAME_LENGTH = 512
DAP_V1_MAX_NODE_TYPE_LENGTH = 64
DAP_V1_MAX_NODE_NAME_LENGTH = 256
DAP_V1_MAX_NAMESPACE_LENGTH = 128
DAP_V1_MAX_SOURCE_NAME_LENGTH = 128
DAP_V1_MAX_SOURCE_TYPE_LENGTH = 64
DAP_V1_MAX_COLLECTOR_NAME_LENGTH = 128
DAP_V1_MAX_COLLECTOR_VERSION_LENGTH = 64
DAP_V1_MAX_EVIDENCE_TYPE_LENGTH = 64
DAP_V1_MAX_EVIDENCE_RECORD_ID_LENGTH = 256
DAP_V1_MAX_EVIDENCE_DESCRIPTION_LENGTH = 1_000


@dataclass(frozen=True, slots=True)
class DAPLineageCapacityLimits:
    """DAP compatibility defaults, overridable by tests or future callers."""

    max_nodes: int = DAP_V1_MAX_LINEAGE_NODES
    max_edges: int = DAP_V1_MAX_LINEAGE_EDGES
    max_payload_bytes: int = DAP_V1_MAX_METADATA_BODY_BYTES

    def __post_init__(self) -> None:
        for name in ("max_nodes", "max_edges", "max_payload_bytes"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")


DEFAULT_DAP_V1_CAPACITY_LIMITS = DAPLineageCapacityLimits()


@dataclass(frozen=True, slots=True)
class DAPLineagePreflight:
    environment: str
    source_profile: str
    toolkit_batch_id: str
    table_nodes: int
    task_nodes: int
    total_nodes: int
    business_edges: int
    dap_edges: int
    diagnostic_count: int
    payload_bytes: int
    payload_megabytes: float
    contract_version: str
    limits: DAPLineageCapacityLimits
    errors: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, object]:
        return {
            "status": "READY" if self.ready else "NON-READY",
            "environment": self.environment,
            "source_profile": self.source_profile,
            "toolkit_batch_id": self.toolkit_batch_id,
            "table_nodes": self.table_nodes,
            "task_nodes": self.task_nodes,
            "total_nodes": self.total_nodes,
            "business_edges": self.business_edges,
            "dap_edges": self.dap_edges,
            "diagnostic_count": self.diagnostic_count,
            "payload_bytes": self.payload_bytes,
            "payload_megabytes": self.payload_megabytes,
            "contract_version": self.contract_version,
            "capacity": {
                "nodes": {"used": self.total_nodes, "limit": self.limits.max_nodes},
                "edges": {"used": self.dap_edges, "limit": self.limits.max_edges},
                "payload_bytes": {
                    "used": self.payload_bytes,
                    "limit": self.limits.max_payload_bytes,
                },
            },
            "errors": list(self.errors),
        }


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def serialize_dap_lineage_contract(contract: Mapping[str, object]) -> str:
    """Return deterministic UTF-8-ready JSON text with one final newline."""

    if not isinstance(contract, Mapping):
        raise TypeError("contract must be a mapping")
    return _canonical_json(contract) + "\n"


def _encoded_identity_part(value: str) -> str:
    # Escape separators to make the readable IDs unambiguous without inventing
    # a new table-name normalization rule.
    return quote(value, safe="-._~")


def _external_id(kind: str, *parts: str) -> str:
    readable = ":".join((kind, *(_encoded_identity_part(part) for part in parts)))
    if len(readable) <= DAP_V1_MAX_EXTERNAL_ID_LENGTH:
        return readable
    identity = _canonical_json([DAP_LINEAGE_ADAPTER_VERSION, kind, *parts])
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"{kind}:sha256:{digest}"


def _snapshot_external_id(
    *, environment: str, source_profile: str, batch_id: str
) -> str:
    identity = _canonical_json(
        {
            "adapterVersion": DAP_LINEAGE_ADAPTER_VERSION,
            "batchId": batch_id,
            "environment": environment,
            "sourceProfile": source_profile,
        }
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"lakehouse-toolkit:{DAP_LINEAGE_ADAPTER_VERSION}:{digest}"


def _validate_row(
    row: DWSBusinessEdgeRow,
    *,
    environment: str,
    source_profile: str,
    batch_id: str,
) -> tuple[str, str, str, str]:
    if not isinstance(row, DWSBusinessEdgeRow):
        raise TypeError("rows must contain DWSBusinessEdgeRow values")
    if row.environment != environment or row.source_profile != source_profile:
        raise ValueError("business edge is outside the requested environment/profile")
    if (
        row.batch_id != batch_id
        or not isinstance(row.is_active, bool)
        or not row.is_active
    ):
        raise ValueError("business edge is not from the requested active batch")

    source_identity = DatasetIdentity.from_name(environment, row.source_table)
    target_identity = DatasetIdentity.from_name(environment, row.target_table)
    if (
        source_identity is None
        or target_identity is None
        or source_identity.canonical_name != row.source_table
        or target_identity.canonical_name != row.target_table
    ):
        raise ValueError(
            "business edge endpoints are not canonical DatasetIdentity values"
        )

    program_name = _required_text(row.program_name, "program_name")
    stored_program_key = _required_text(row.program_key, "program_key")
    expected_program_key = program_key(
        ProgramIdentity(environment, source_profile, program_name)
    )
    if stored_program_key != expected_program_key:
        raise ValueError("business edge program_key does not match ProgramIdentity")

    edge = LineageEdge(
        environment=environment,
        source_profile=source_profile,
        source_table=source_identity.canonical_name,
        target_table=target_identity.canonical_name,
        program_name=program_name,
    )
    stored_business_key = _required_text(
        row.business_edge_key, "business_edge_key"
    )
    if stored_business_key != business_edge_key(edge):
        raise ValueError(
            "business_edge_key does not match the canonical business edge"
        )
    if (
        not isinstance(row.collapse_depth, int)
        or isinstance(row.collapse_depth, bool)
        or row.collapse_depth < 1
    ):
        raise ValueError("collapse_depth must be a positive integer")
    if row.pipeline_version is not None and not isinstance(row.pipeline_version, str):
        raise ValueError("pipeline_version must be a string or None")
    return (
        source_identity.canonical_name,
        target_identity.canonical_name,
        program_name,
        stored_program_key,
    )


def build_dap_lineage_contract(
    rows: Iterable[DWSBusinessEdgeRow],
    *,
    active_snapshot: DWSActiveSnapshotMetadata,
    environment: str,
    source_profile: str,
) -> dict[str, object]:
    """Project one exact active toolkit business scope to DAP Lineage V1."""

    environment = _required_text(environment, "environment")
    source_profile = _required_text(source_profile, "source_profile")
    if not isinstance(active_snapshot, DWSActiveSnapshotMetadata):
        raise TypeError("active_snapshot must be DWSActiveSnapshotMetadata")
    batch_id = _required_text(active_snapshot.batch_id, "toolkit_batch_id")
    if not active_snapshot.is_active:
        raise ValueError("active_snapshot must be active")
    if (
        not active_snapshot.complete_snapshot
        or active_snapshot.snapshot_mode != "FULL"
    ):
        raise ValueError(
            "DAP replace export requires a complete FULL toolkit snapshot"
        )
    if (environment, source_profile) not in active_snapshot.snapshot_scope:
        raise ValueError(
            "requested environment/profile is not declared by active batch"
        )
    observed_at = active_snapshot.observed_at
    if (
        not isinstance(observed_at, datetime)
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
    ):
        raise ValueError("active snapshot observed_at must include a timezone")

    unique_rows: dict[str, DWSBusinessEdgeRow] = {}
    for row in rows:
        if not isinstance(row, DWSBusinessEdgeRow):
            raise TypeError("rows must contain DWSBusinessEdgeRow values")
        row_key = _required_text(row.business_edge_key, "business_edge_key")
        previous = unique_rows.get(row_key)
        if previous is not None:
            if previous != row:
                raise ValueError("duplicate business_edge_key has conflicting row data")
            continue
        unique_rows[row_key] = row

    table_ids: dict[str, str] = {}
    task_names: dict[str, str] = {}
    task_program_keys: dict[str, str] = {}
    edge_records: list[tuple[str, str, str, DWSBusinessEdgeRow]] = []
    for row in sorted(unique_rows.values(), key=lambda item: item.business_edge_key):
        source_table, target_table, task_name, task_key = _validate_row(
            row,
            environment=environment,
            source_profile=source_profile,
            batch_id=batch_id,
        )
        source_id = table_ids.setdefault(
            source_table, _external_id("table", environment, source_table)
        )
        target_id = table_ids.setdefault(
            target_table, _external_id("table", environment, target_table)
        )
        task_id = _external_id("task", environment, task_key)
        previous_task_name = task_names.setdefault(task_id, task_name)
        if previous_task_name != task_name:
            raise ValueError("one task identity resolved to conflicting program names")
        previous_program_key = task_program_keys.setdefault(task_id, task_key)
        if previous_program_key != task_key:
            raise ValueError("one task identity resolved to conflicting program keys")
        edge_records.append((source_id, task_id, target_id, row))

    nodes: list[dict[str, object]] = []
    for table_name, external_id in table_ids.items():
        identity = DatasetIdentity.from_name(environment, table_name)
        if identity is None:
            raise ValueError("business table lost DatasetIdentity during projection")
        nodes.append(
            {
                "externalId": external_id,
                "qualifiedName": identity.canonical_name,
                "type": "table",
                "name": identity.canonical_table,
                "namespace": identity.canonical_schema,
                "attributes": {
                    "environment": environment,
                    "sourceProfile": source_profile,
                },
            }
        )
    for task_id, task_name in task_names.items():
        nodes.append(
            {
                "externalId": task_id,
                "type": "task",
                "name": task_name,
                "namespace": environment,
                "attributes": {
                    "environment": environment,
                    "sourceProfile": source_profile,
                    "programKey": task_program_keys[task_id],
                },
            }
        )
    nodes.sort(key=lambda item: str(item["externalId"]))

    edges: list[dict[str, object]] = []
    for source_id, task_id, target_id, row in edge_records:
        evidence = {
            "type": "toolkit_business_lineage",
            "sourceRecordId": row.business_edge_key,
            "description": (
                "Toolkit business projection; "
                f"collapseDepth={row.collapse_depth}."
            ),
        }
        diagnostic: dict[str, object] = {
            "code": "toolkit_business_projection",
            "collapseDepth": row.collapse_depth,
        }
        if row.pipeline_version:
            diagnostic["pipelineVersion"] = row.pipeline_version
        for direction, source, target, edge_type in (
            ("table-to-task", source_id, task_id, "table_to_task"),
            ("task-to-table", task_id, target_id, "task_to_table"),
        ):
            edges.append(
                {
                    "externalId": _external_id(
                        "edge", row.business_edge_key, direction
                    ),
                    "sourceId": source,
                    "targetId": target,
                    "type": edge_type,
                    "evidence": dict(evidence),
                    # Toolkit business rows do not carry a confidence score.
                    "confidence": "unknown",
                    "diagnostics": [dict(diagnostic)],
                }
            )
    edges.sort(key=lambda item: str(item["externalId"]))

    return {
        "contractVersion": DAP_LINEAGE_CONTRACT_VERSION,
        "source": {
            "type": DAP_LINEAGE_SOURCE_TYPE,
            "name": source_profile,
            "namespace": environment,
        },
        "collector": {
            "name": DAP_LINEAGE_COLLECTOR_NAME,
            "version": DAP_LINEAGE_ADAPTER_VERSION,
        },
        "snapshot": {
            "externalSnapshotId": _snapshot_external_id(
                environment=environment,
                source_profile=source_profile,
                batch_id=batch_id,
            ),
            "generatedAt": observed_at.isoformat(timespec="microseconds"),
            "mode": "replace",
        },
        "nodes": nodes,
        "edges": edges,
    }


def _text_error(
    value: object,
    *,
    field_name: str,
    maximum: int,
    required: bool = True,
) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value.strip()):
        return f"{field_name} must be a non-empty string"
    if len(value) > maximum:
        return f"{field_name} exceeds DAP V1 maximum length {maximum}"
    return None


def _validate_dap_shape(contract: Mapping[str, object]) -> list[str]:
    errors: list[str] = []
    if contract.get("contractVersion") != DAP_LINEAGE_CONTRACT_VERSION:
        errors.append("contractVersion must be 1.0")
    source = contract.get("source")
    if not isinstance(source, Mapping):
        errors.append("source must be an object")
    else:
        for key, maximum in (
            ("type", DAP_V1_MAX_SOURCE_TYPE_LENGTH),
            ("name", DAP_V1_MAX_SOURCE_NAME_LENGTH),
            ("namespace", DAP_V1_MAX_NAMESPACE_LENGTH),
        ):
            message = _text_error(
                source.get(key),
                field_name=f"source.{key}",
                maximum=maximum,
            )
            if message:
                errors.append(message)
    collector = contract.get("collector")
    if not isinstance(collector, Mapping):
        errors.append("collector must be an object")
    else:
        for key, maximum in (
            ("name", DAP_V1_MAX_COLLECTOR_NAME_LENGTH),
            ("version", DAP_V1_MAX_COLLECTOR_VERSION_LENGTH),
        ):
            message = _text_error(
                collector.get(key),
                field_name=f"collector.{key}",
                maximum=maximum,
            )
            if message:
                errors.append(message)

    snapshot = contract.get("snapshot")
    if not isinstance(snapshot, Mapping):
        errors.append("snapshot must be an object")
    else:
        message = _text_error(
            snapshot.get("externalSnapshotId"),
            field_name="snapshot.externalSnapshotId",
            maximum=DAP_V1_MAX_EXTERNAL_ID_LENGTH,
        )
        if message:
            errors.append(message)
        generated_at = snapshot.get("generatedAt")
        if not isinstance(generated_at, str):
            errors.append("snapshot.generatedAt must be a timezone-aware string")
        else:
            try:
                parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError
            except ValueError:
                errors.append("snapshot.generatedAt must include a timezone")
        if snapshot.get("mode") != "replace":
            errors.append("snapshot.mode must be replace for DAP Contract V1")

    nodes = contract.get("nodes")
    node_references: dict[str, int] = {}
    if not isinstance(nodes, list):
        errors.append("nodes must be an array")
        nodes = []
    for index, node in enumerate(nodes):
        prefix = f"nodes[{index}]"
        if not isinstance(node, Mapping):
            errors.append(f"{prefix} must be an object")
            continue
        for key, maximum in (
            ("externalId", DAP_V1_MAX_EXTERNAL_ID_LENGTH),
            ("qualifiedName", DAP_V1_MAX_QUALIFIED_NAME_LENGTH),
            ("type", DAP_V1_MAX_NODE_TYPE_LENGTH),
            ("name", DAP_V1_MAX_NODE_NAME_LENGTH),
            ("namespace", DAP_V1_MAX_NAMESPACE_LENGTH),
        ):
            required = key in {"type", "name"}
            message = _text_error(
                node.get(key),
                field_name=f"{prefix}.{key}",
                maximum=maximum,
                required=required,
            )
            if message:
                errors.append(message)
        attributes = node.get("attributes", {})
        if not isinstance(attributes, Mapping):
            errors.append(f"{prefix}.attributes must be an object")
        for identity_key in ("externalId", "qualifiedName"):
            identity_value = node.get(identity_key)
            if isinstance(identity_value, str) and identity_value:
                if (
                    identity_value in node_references
                    and node_references[identity_value] != index
                ):
                    errors.append(f"duplicate node identity: {identity_value}")
                else:
                    node_references[identity_value] = index
        if not (node.get("externalId") or node.get("qualifiedName")):
            errors.append(f"{prefix} needs externalId or qualifiedName")

    edges = contract.get("edges")
    edge_ids: set[str] = set()
    if not isinstance(edges, list):
        errors.append("edges must be an array")
        edges = []
    for index, edge in enumerate(edges):
        prefix = f"edges[{index}]"
        if not isinstance(edge, Mapping):
            errors.append(f"{prefix} must be an object")
            continue
        external_id = edge.get("externalId")
        if external_id is not None:
            message = _text_error(
                external_id,
                field_name=f"{prefix}.externalId",
                maximum=DAP_V1_MAX_EXTERNAL_ID_LENGTH,
            )
            if message:
                errors.append(message)
            elif external_id in edge_ids:
                errors.append(f"duplicate edge identity: {external_id}")
            else:
                edge_ids.add(external_id)
        for key in ("sourceId", "targetId"):
            value = edge.get(key)
            message = _text_error(
                value,
                field_name=f"{prefix}.{key}",
                maximum=512,
            )
            if message:
                errors.append(message)
            elif value not in node_references:
                errors.append(f"{prefix}.{key} does not resolve to a snapshot node")
        message = _text_error(
            edge.get("type"),
            field_name=f"{prefix}.type",
            maximum=DAP_V1_MAX_NODE_TYPE_LENGTH,
        )
        if message:
            errors.append(message)
        evidence = edge.get("evidence")
        if not isinstance(evidence, Mapping):
            errors.append(f"{prefix}.evidence must be an object")
        else:
            for key, maximum in (
                ("type", DAP_V1_MAX_EVIDENCE_TYPE_LENGTH),
                ("sourceRecordId", DAP_V1_MAX_EVIDENCE_RECORD_ID_LENGTH),
                ("description", DAP_V1_MAX_EVIDENCE_DESCRIPTION_LENGTH),
            ):
                required = key == "type"
                message = _text_error(
                    evidence.get(key),
                    field_name=f"{prefix}.evidence.{key}",
                    maximum=maximum,
                    required=required,
                )
                if message:
                    errors.append(message)
        confidence = edge.get("confidence")
        if isinstance(confidence, bool) or not isinstance(
            confidence, (str, int, float)
        ):
            errors.append(f"{prefix}.confidence must be a string or number")
        elif isinstance(confidence, float) and not math.isfinite(confidence):
            errors.append(f"{prefix}.confidence must be finite")
        diagnostics = edge.get("diagnostics", [])
        if not isinstance(diagnostics, list) or any(
            not isinstance(item, Mapping) for item in diagnostics
        ):
            errors.append(f"{prefix}.diagnostics must be an array of objects")

    return errors


def preflight_dap_lineage_contract(
    contract: Mapping[str, object],
    *,
    environment: str,
    source_profile: str,
    toolkit_batch_id: str,
    limits: DAPLineageCapacityLimits = DEFAULT_DAP_V1_CAPACITY_LIMITS,
) -> DAPLineagePreflight:
    """Validate the generated V1 DTO and report DAP compatibility capacity."""

    if not isinstance(limits, DAPLineageCapacityLimits):
        raise TypeError("limits must be DAPLineageCapacityLimits")
    serialized = serialize_dap_lineage_contract(contract)
    payload_bytes = len(serialized.encode("utf-8"))
    raw_nodes = contract.get("nodes", [])
    nodes = raw_nodes if isinstance(raw_nodes, list) else []
    raw_edges = contract.get("edges", [])
    edges = raw_edges if isinstance(raw_edges, list) else []
    table_nodes = sum(
        isinstance(node, Mapping) and node.get("type") == "table" for node in nodes
    )
    task_nodes = sum(
        isinstance(node, Mapping) and node.get("type") == "task" for node in nodes
    )
    diagnostic_count = sum(
        len(edge.get("diagnostics", []))
        for edge in edges
        if isinstance(edge, Mapping)
        and isinstance(edge.get("diagnostics", []), list)
    )
    errors = _validate_dap_shape(contract)
    if len(nodes) > limits.max_nodes:
        errors.append(
            f"DAP V1 node capacity exceeded: {len(nodes)} > {limits.max_nodes}"
        )
    if len(edges) > limits.max_edges:
        errors.append(
            f"DAP V1 edge capacity exceeded: {len(edges)} > {limits.max_edges}"
        )
    if payload_bytes > limits.max_payload_bytes:
        errors.append(
            "DAP V1 body capacity exceeded: "
            f"{payload_bytes} > {limits.max_payload_bytes} bytes"
        )
    if len(edges) % 2:
        errors.append(
            "DAP edge count is not a whole number of two-edge business projections"
        )

    return DAPLineagePreflight(
        environment=_required_text(environment, "environment"),
        source_profile=_required_text(source_profile, "source_profile"),
        toolkit_batch_id=_required_text(toolkit_batch_id, "toolkit_batch_id"),
        table_nodes=table_nodes,
        task_nodes=task_nodes,
        total_nodes=len(nodes),
        business_edges=len(edges) // 2,
        dap_edges=len(edges),
        diagnostic_count=diagnostic_count,
        payload_bytes=payload_bytes,
        payload_megabytes=payload_bytes / (1024 * 1024),
        contract_version=str(contract.get("contractVersion", "")),
        limits=limits,
        errors=tuple(errors),
    )


__all__ = [
    "DAP_LINEAGE_ADAPTER_VERSION",
    "DAP_LINEAGE_COLLECTOR_NAME",
    "DAP_LINEAGE_CONTRACT_VERSION",
    "DAPLineageCapacityLimits",
    "DAPLineagePreflight",
    "DEFAULT_DAP_V1_CAPACITY_LIMITS",
    "DAP_V1_MAX_LINEAGE_EDGES",
    "DAP_V1_MAX_LINEAGE_NODES",
    "DAP_V1_MAX_METADATA_BODY_BYTES",
    "build_dap_lineage_contract",
    "preflight_dap_lineage_contract",
    "serialize_dap_lineage_contract",
]
