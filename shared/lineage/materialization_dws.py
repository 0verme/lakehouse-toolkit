"""DWS production adapter for the existing lineage materialization pipeline.

The adapter deliberately keeps DWS SQL at the repository boundary.  It receives
an already-built :class:`MaterializationBatch` (the existing formal/business
projection) and the ``ProgramPhysicalDAG`` objects produced during that same
build.  It never parses SQL or performs TMP collapse itself.

The first DWS schema is an executable smoke schema without database-side
uniqueness/check/partition/foreign-key constraints.  Consequently this module
validates every candidate inside the publish transaction before switching the
logical active batch.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from time import perf_counter
from typing import Any

from shared.lineage.audit import AuditFact
from shared.lineage.domain import (
    AuditConfidence,
    DatasetIdentity,
    IssueDisposition,
    IssueType,
    LineageEdge,
    LineageIssue,
    PhysicalEdge,
    PhysicalNodeKind,
    ProgramIdentity,
    ProgramSource,
    ProgramState,
    is_temporary_asset,
)
from shared.lineage.evolution import (
    BatchMetadata,
    SnapshotScope,
    reconcile_issue_lifecycle,
)
from shared.lineage.materialization import MaterializationBatch, _canonical_json
from shared.lineage.physical_dag import ProgramPhysicalDAG
from shared.lineage.version import LINEAGE_PIPELINE_VERSION

DWS_SCHEMA = "dwp"
DWS_KEY_MAX_LENGTH = 128
DWS_TABLES = (
    "lineage_batch",
    "lineage_program_state",
    "lineage_edge",
    "lineage_business_edge",
    "lineage_issue",
)
KEY_SEPARATOR = "\x1f"


def _connect_with_profile(profile: str) -> Any:
    """Load the JDBC dependency only when the DWS backend is selected."""

    from shared.db.gaussdb import connect_with_profile

    return connect_with_profile(profile)


# Every statement below is source-controlled and schema-qualified.  Values from
# providers/configuration are always passed through DB-API parameters.  Keep
# timestamptz parameters as ISO-8601 text and apply the same explicit SQL cast
# at every DWS write boundary; this avoids relying on old JDBC datetime binding.
TIMESTAMPTZ_PARAM_SQL = "CAST(? AS TIMESTAMP WITH TIME ZONE)"

INSERT_BATCH_SQL = f"""
    INSERT INTO dwp.lineage_batch(
        batch_id, snapshot_mode, complete_snapshot, snapshot_scope,
        pipeline_version, observed_at, previous_batch_id, publish_status,
        published_at, program_count, edge_count, issue_count, is_active,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, {TIMESTAMPTZ_PARAM_SQL}, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, ?, ?, ?, ?, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL})
"""
INSERT_PROGRAM_STATE_SQL = f"""
    INSERT INTO dwp.lineage_program_state(
        row_key, program_key, environment, source_profile, program_name,
        source_hash, pipeline_version, batch_id, first_seen_at, last_seen_at,
        last_changed_at, is_active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL})
"""
INSERT_PHYSICAL_EDGE_SQL = f"""
    INSERT INTO dwp.lineage_edge(
        row_key, edge_key, environment, source_profile, program_key,
        program_name, source_table, target_table, source_node_kind,
        target_node_kind, source_dataset_key, target_dataset_key,
        evidence_type, evidence_json, source_hash, pipeline_version, batch_id,
        observed_at, first_seen_at, last_seen_at, last_changed_at, is_active,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL})
"""
INSERT_BUSINESS_EDGE_SQL = f"""
    INSERT INTO dwp.lineage_business_edge(
        row_key, business_edge_key, environment, source_profile, program_key,
        program_name, source_dataset_key, source_table, target_dataset_key,
        target_table, collapse_depth, physical_derivation_hash, source_hash,
        pipeline_version, batch_id, observed_at, first_seen_at, last_seen_at,
        last_changed_at, is_active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL})
"""
INSERT_ISSUE_SQL = f"""
    INSERT INTO dwp.lineage_issue(
        row_key, stable_issue_key, environment, source_profile, program_key,
        program_name, issue_type, confidence, rule_version, severity,
        disposition, policy_version, node_key, branch_sink, message,
        evidence_json, batch_id, first_seen_at, last_seen_at, last_changed_at,
        disposition_updated_at, disposition_updated_by, is_active, created_at,
        updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL},
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL}, ?, ?,
              {TIMESTAMPTZ_PARAM_SQL}, {TIMESTAMPTZ_PARAM_SQL})
"""

# Raw JDBC timestamp objects lose the DWS offset during JayDeBeApi conversion.
# Every timestamptz projection below is therefore text at the READ boundary;
# row converters then enforce the timezone-aware Python datetime contract.
BATCH_SELECT_SQL = """
    SELECT batch_id, snapshot_mode, complete_snapshot, snapshot_scope,
           pipeline_version,
           CAST(observed_at AS VARCHAR(128)) AS observed_at,
           previous_batch_id, publish_status,
           CAST(published_at AS VARCHAR(128)) AS published_at,
           program_count, edge_count, issue_count, is_active,
           CAST(created_at AS VARCHAR(128)) AS created_at,
           CAST(updated_at AS VARCHAR(128)) AS updated_at
    FROM dwp.lineage_batch
"""
PROGRAM_STATE_SELECT_SQL = """
    SELECT s.row_key, s.program_key, s.environment, s.source_profile,
           s.program_name, s.source_hash, s.pipeline_version, s.batch_id,
           CAST(s.first_seen_at AS VARCHAR(128)) AS first_seen_at,
           CAST(s.last_seen_at AS VARCHAR(128)) AS last_seen_at,
           CAST(s.last_changed_at AS VARCHAR(128)) AS last_changed_at,
           s.is_active,
           CAST(s.created_at AS VARCHAR(128)) AS created_at,
           CAST(s.updated_at AS VARCHAR(128)) AS updated_at
    FROM dwp.lineage_program_state AS s
"""
PHYSICAL_EDGE_SELECT_SQL = """
    SELECT e.row_key, e.edge_key, e.environment, e.source_profile,
           e.program_key, e.program_name, e.source_table, e.target_table,
           e.source_node_kind, e.target_node_kind, e.source_dataset_key,
           e.target_dataset_key, e.evidence_type, e.evidence_json,
           e.source_hash, e.pipeline_version, e.batch_id,
           CAST(e.observed_at AS VARCHAR(128)) AS observed_at,
           CAST(e.first_seen_at AS VARCHAR(128)) AS first_seen_at,
           CAST(e.last_seen_at AS VARCHAR(128)) AS last_seen_at,
           CAST(e.last_changed_at AS VARCHAR(128)) AS last_changed_at,
           e.is_active,
           CAST(e.created_at AS VARCHAR(128)) AS created_at,
           CAST(e.updated_at AS VARCHAR(128)) AS updated_at
    FROM dwp.lineage_edge AS e
"""
BUSINESS_EDGE_SELECT_SQL = """
    SELECT e.row_key, e.business_edge_key, e.environment, e.source_profile,
           e.program_key, e.program_name, e.source_dataset_key, e.source_table,
           e.target_dataset_key, e.target_table, e.collapse_depth,
           e.physical_derivation_hash, e.source_hash, e.pipeline_version,
           e.batch_id,
           CAST(e.observed_at AS VARCHAR(128)) AS observed_at,
           CAST(e.first_seen_at AS VARCHAR(128)) AS first_seen_at,
           CAST(e.last_seen_at AS VARCHAR(128)) AS last_seen_at,
           CAST(e.last_changed_at AS VARCHAR(128)) AS last_changed_at,
           e.is_active,
           CAST(e.created_at AS VARCHAR(128)) AS created_at,
           CAST(e.updated_at AS VARCHAR(128)) AS updated_at
    FROM dwp.lineage_business_edge AS e
"""
ISSUE_SELECT_SQL = """
    SELECT i.row_key, i.stable_issue_key, i.environment, i.source_profile,
           i.program_key, i.program_name, i.issue_type, i.confidence,
           i.rule_version, i.severity, i.disposition, i.policy_version,
           i.node_key, i.branch_sink, i.message, i.evidence_json, i.batch_id,
           CAST(i.first_seen_at AS VARCHAR(128)) AS first_seen_at,
           CAST(i.last_seen_at AS VARCHAR(128)) AS last_seen_at,
           CAST(i.last_changed_at AS VARCHAR(128)) AS last_changed_at,
           CAST(i.disposition_updated_at AS VARCHAR(128)) AS disposition_updated_at,
           i.disposition_updated_by, i.is_active,
           CAST(i.created_at AS VARCHAR(128)) AS created_at,
           CAST(i.updated_at AS VARCHAR(128)) AS updated_at
    FROM dwp.lineage_issue AS i
"""

ACTIVE_BATCH_JOIN = """
    JOIN dwp.lineage_batch AS b
      ON b.batch_id = e.batch_id
     AND b.is_active = TRUE
"""
ACTIVE_STATE_JOIN = """
    JOIN dwp.lineage_batch AS b
      ON b.batch_id = s.batch_id
     AND b.is_active = TRUE
"""
ACTIVE_ISSUE_JOIN = """
    JOIN dwp.lineage_batch AS b
      ON b.batch_id = i.batch_id
     AND b.is_active = TRUE
"""

DEACTIVATE_PHYSICAL_SQL = (
    "UPDATE dwp.lineage_edge SET is_active = FALSE WHERE is_active = TRUE"
)
DEACTIVATE_BUSINESS_SQL = (
    "UPDATE dwp.lineage_business_edge SET is_active = FALSE WHERE is_active = TRUE"
)
DEACTIVATE_ISSUE_SQL = (
    "UPDATE dwp.lineage_issue SET is_active = FALSE WHERE is_active = TRUE"
)
DEACTIVATE_STATE_SQL = (
    "UPDATE dwp.lineage_program_state SET is_active = FALSE WHERE is_active = TRUE"
)
RETIRE_BATCH_SQL = f"""
    UPDATE dwp.lineage_batch
    SET is_active = FALSE, publish_status = 'RETIRED',
        updated_at = {TIMESTAMPTZ_PARAM_SQL}
    WHERE is_active = TRUE
"""
ACTIVATE_BATCH_SQL = f"""
    UPDATE dwp.lineage_batch
    SET is_active = TRUE, publish_status = 'PUBLISHED',
        published_at = {TIMESTAMPTZ_PARAM_SQL},
        updated_at = {TIMESTAMPTZ_PARAM_SQL}
    WHERE batch_id = ? AND is_active = FALSE
"""
ACTIVATE_PHYSICAL_SQL = """
    UPDATE dwp.lineage_edge
    SET is_active = TRUE
    WHERE batch_id = ?
"""
ACTIVATE_BUSINESS_SQL = """
    UPDATE dwp.lineage_business_edge
    SET is_active = TRUE
    WHERE batch_id = ?
"""
ACTIVATE_ISSUE_SQL = """
    UPDATE dwp.lineage_issue
    SET is_active = CASE WHEN disposition = 'RESOLVED' THEN FALSE ELSE TRUE END
    WHERE batch_id = ?
"""
ACTIVATE_STATE_SQL = """
    UPDATE dwp.lineage_program_state
    SET is_active = TRUE
    WHERE batch_id = ?
"""


@dataclass(frozen=True, slots=True)
class DWSPhysicalEdgeRow:
    """One raw direct physical edge projection for ``dwp.lineage_edge``."""

    row_key: str
    edge_key: str
    environment: str
    source_profile: str
    program_key: str
    program_name: str
    source_table: str
    target_table: str
    source_node_kind: str
    target_node_kind: str
    source_dataset_key: str | None
    target_dataset_key: str | None
    evidence_type: str
    evidence_json: str
    source_hash: str | None
    pipeline_version: str | None
    batch_id: str
    observed_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DWSBusinessEdgeRow:
    """One collapsed formal direct edge projection for DWS."""

    row_key: str
    business_edge_key: str
    environment: str
    source_profile: str
    program_key: str
    program_name: str
    source_dataset_key: str
    source_table: str
    target_dataset_key: str
    target_table: str
    collapse_depth: int
    physical_derivation_hash: str
    source_hash: str | None
    pipeline_version: str | None
    batch_id: str
    observed_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DWSIssueRow:
    """Issue #36 fact/policy projection for ``dwp.lineage_issue``."""

    row_key: str
    stable_issue_key: str
    environment: str
    source_profile: str
    program_key: str
    program_name: str
    issue_type: str
    confidence: str
    rule_version: str
    severity: str
    disposition: str
    policy_version: str
    node_key: str | None
    branch_sink: str | None
    message: str
    evidence_json: str
    batch_id: str
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime
    disposition_updated_at: datetime | None
    disposition_updated_by: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DWSPublishResult:
    """DWS publish summary; ``edge_count`` remains the physical edge count."""

    batch_id: str
    edge_count: int
    issue_count: int
    previous_batch_id: str | None
    program_count: int = 0
    business_edge_count: int = 0


@dataclass(slots=True)
class DWSPublishMetrics:
    """Bounded timings and counts shared with cron observability."""

    prepare_ms: int = 0
    insert_ms: int = 0
    validate_ms: int = 0
    active_switch_ms: int = 0
    commit_ms: int = 0
    prepared_edge_rows: int = 0
    prepared_business_rows: int = 0
    prepared_issue_rows: int = 0
    prepared_program_rows: int = 0
    validated_edge_rows: int = 0
    validated_business_rows: int = 0
    validated_issue_rows: int = 0
    validated_program_rows: int = 0
    evidence_serialization_calls: int = 0


@dataclass(frozen=True, slots=True)
class _DWSBatchRow:
    batch_id: str
    snapshot_mode: str
    complete_snapshot: bool
    snapshot_scope: str | None
    pipeline_version: str | None
    observed_at: datetime
    previous_batch_id: str | None
    publish_status: str | None
    published_at: datetime | None
    program_count: int
    edge_count: int
    issue_count: int
    is_active: bool
    created_at: datetime | None
    updated_at: datetime | None


@dataclass(frozen=True, slots=True)
class _DWSPreparedCandidate:
    batch: MaterializationBatch
    previous_batch_id: str | None
    batch_row: _DWSBatchRow
    physical_rows: tuple[DWSPhysicalEdgeRow, ...]
    business_rows: tuple[DWSBusinessEdgeRow, ...]
    issue_rows: tuple[DWSIssueRow, ...]
    program_state_rows: tuple[tuple[object, ...], ...]


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _key_text(value: object, field_name: str) -> str:
    text = _required_text(value, field_name)
    if len(text) > DWS_KEY_MAX_LENGTH:
        raise ValueError(f"{field_name} exceeds DWS key length")
    if KEY_SEPARATOR in text:
        raise ValueError(f"{field_name} contains the stable-key separator")
    return text


def _stored_required_text(value: object, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} must not be NULL")
    return _required_text(str(value), field_name)


def _stored_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(str(value), field_name)


def _stored_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"TRUE", "T", "1"}:
            return True
        if normalized in {"FALSE", "F", "0"}:
            return False
    raise ValueError(f"{field_name} is not a valid boolean")


def _stored_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field_name} is not a valid integer")
    try:
        parsed = int(value) if isinstance(value, float) else int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid integer") from exc
    if isinstance(value, float) and parsed != value:
        raise ValueError(f"{field_name} is not a valid integer")
    return parsed


def _timestamp_text(
    value: datetime | None, field_name: str = "timestamp"
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None")
    return value.isoformat()


def _timestamp_param(
    value: datetime | None, field_name: str = "timestamp"
) -> str | None:
    """Return ISO-8601 text for the DWS ``TIMESTAMP WITH TIME ZONE`` cast.

    JayDeBeApi/JDBC may bind a Python string as ``VARCHAR``.  The SQL write
    statements therefore apply ``TIMESTAMPTZ_PARAM_SQL`` at every timestamp
    placeholder instead of relying on driver-side datetime inference.  Keeping
    the original ISO-8601 offset in this value preserves timezone semantics;
    ``None`` remains SQL ``NULL``.
    """

    return _timestamp_text(value, field_name)


_TIMESTAMP_OFFSET_SUFFIX_RE = re.compile(
    r"(?P<sign>[+-])(?P<hours>\d{2})"
    r"(?:(?::(?P<colon_minutes>\d{2}))|(?P<compact_minutes>\d{2}))?$"
)
_TIMESTAMP_TIME_PREFIX_RE = re.compile(
    r"(?:T| )\d{2}:\d{2}(?::\d{2}(?:[.,]\d{1,6})?)?$"
)


def _normalize_timestamp_text(value: str) -> str:
    if value.endswith("Z"):
        return value[:-1] + "+00:00"

    match = _TIMESTAMP_OFFSET_SUFFIX_RE.search(value)
    if (
        match is None
        or _TIMESTAMP_TIME_PREFIX_RE.search(value[: match.start()]) is None
    ):
        return value

    minutes = (
        match.group("colon_minutes")
        or match.group("compact_minutes")
        or "00"
    )
    return (
        f"{value[: match.start()]}{match.group('sign')}"
        f"{match.group('hours')}:{minutes}"
    )


def _parse_datetime(value: object, field_name: str) -> datetime:
    """Parse one DWS timestamp without guessing a timezone for naive values."""

    if isinstance(value, datetime):
        parsed = value
    else:
        if value is None:
            raise ValueError(f"{field_name} must not be NULL")
        text = str(value).strip()
        if not text:
            raise ValueError(f"{field_name} is not a valid timestamp")
        text = _normalize_timestamp_text(text)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} is not a valid timestamp") from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed


def _decode_json(value: object) -> Mapping[str, object] | str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError("stored DWS evidence is not valid JSON") from exc
    if decoded is None or isinstance(decoded, (str, dict)):
        return decoded
    raise ValueError("stored DWS evidence must be a JSON object, string, or null")


def _evidence_json(value: Mapping[str, object] | str | None) -> str:
    return _canonical_json(value)


def _stable_hash(prefix: str, *parts: str) -> str:
    values = (_required_text(prefix, "key prefix"),)
    values += tuple(_required_text(part, "key part") for part in parts)
    if any(KEY_SEPARATOR in value for value in values):
        raise ValueError("stable-key values must not contain the separator")
    return hashlib.sha256(KEY_SEPARATOR.join(values).encode("utf-8")).hexdigest()


def program_key(identity: ProgramIdentity | ProgramSource | ProgramState) -> str:
    """Return deterministic #38 ProgramIdentity key."""

    if isinstance(identity, (ProgramSource, ProgramState)):
        identity = identity.identity
    if not isinstance(identity, ProgramIdentity):
        raise TypeError(
            "identity must be a ProgramIdentity, ProgramSource, or ProgramState"
        )
    return _stable_hash(
        "program",
        identity.environment,
        identity.source_profile,
        identity.program_name,
    )


def dataset_key(environment: str, dataset_name: str) -> str | None:
    """Return a DatasetIdentity key, or ``None`` for TMP/unresolved nodes."""

    identity = DatasetIdentity.from_name(environment, dataset_name)
    if identity is None:
        return None
    return _stable_hash(
        "dataset",
        identity.environment,
        identity.canonical_schema,
        identity.canonical_table,
    )


def _row_key(table_name: str, batch_id: str, stable_key: str) -> str:
    return _stable_hash("row", table_name, batch_id, stable_key)


def physical_edge_key(
    environment: str,
    source_profile: str,
    program_name: str,
    source_table: str,
    target_table: str,
    source_node_kind: str = PhysicalNodeKind.FORMAL_ASSET.value,
    target_node_kind: str = PhysicalNodeKind.FORMAL_ASSET.value,
) -> str:
    """Return the batch-independent identity of one direct physical relation."""

    source_kind = PhysicalNodeKind(source_node_kind).value
    target_kind = PhysicalNodeKind(target_node_kind).value
    return _stable_hash(
        "physical-edge",
        environment,
        source_profile,
        program_key(ProgramIdentity(environment, source_profile, program_name)),
        source_table,
        target_table,
        source_kind,
        target_kind,
    )


def business_edge_key(edge: LineageEdge) -> str:
    """Return the batch-independent identity of an existing business edge."""

    if not isinstance(edge, LineageEdge):
        raise TypeError("edge must be a LineageEdge")
    if edge.program_name is None:
        raise ValueError("business edge program_name must be non-empty")
    return _stable_hash(
        "business-edge",
        edge.environment,
        edge.source_profile,
        program_key(
            ProgramIdentity(edge.environment, edge.source_profile, edge.program_name)
        ),
        edge.source_table,
        edge.target_table,
    )


def issue_stable_key(issue: LineageIssue) -> str:
    """Use the existing Issue #36 stable key, deriving it only when absent."""

    if not isinstance(issue, LineageIssue):
        raise TypeError("issue must be a LineageIssue")
    if issue.stable_key is not None and issue.stable_key.strip():
        return _key_text(issue.stable_key, "stable_issue_key")
    return _key_text(
        AuditFact.from_issue(issue).stable_issue_identity,
        "stable_issue_key",
    )


def _node_kind(node_key: str, node_map: Mapping[str, Any]) -> str:
    node = node_map.get(node_key)
    if node is not None and getattr(node, "kind", None) is not None:
        return PhysicalNodeKind(node.kind).value
    return (
        PhysicalNodeKind.TEMPORARY_ASSET.value
        if is_temporary_asset(node_key)
        else PhysicalNodeKind.FORMAL_ASSET.value
    )


def _dataset_key_for_node(
    environment: str,
    node_key: str,
    node_kind: str,
) -> str | None:
    if node_kind != PhysicalNodeKind.FORMAL_ASSET.value:
        return None
    return dataset_key(environment, node_key)


def _pipeline_version_for_batch(batch: MaterializationBatch) -> str:
    values = {
        state.pipeline_version.strip()
        for state in batch.program_states
        if isinstance(state.pipeline_version, str) and state.pipeline_version.strip()
    }
    if not values:
        return LINEAGE_PIPELINE_VERSION
    if len(values) != 1:
        raise ValueError(
            "all program states in a DWS batch must share pipeline_version"
        )
    return next(iter(values))


def _scope_value(
    value: SnapshotScope
    | ProgramIdentity
    | ProgramSource
    | ProgramState
    | tuple[str, str],
) -> tuple[str, str]:
    if isinstance(value, SnapshotScope):
        return value.key
    if isinstance(value, ProgramIdentity):
        return value.scope
    if isinstance(value, (ProgramSource, ProgramState)):
        return value.identity.scope
    if isinstance(value, tuple) and len(value) == 2:
        return (
            _required_text(value[0], "snapshot environment"),
            _required_text(value[1], "snapshot source_profile"),
        )
    raise TypeError("snapshot scopes must contain supported identity/scope values")


def _snapshot_scope_json(
    scopes: Iterable[
        SnapshotScope | ProgramIdentity | ProgramSource | ProgramState | tuple[str, str]
    ]
    | None,
    batch: MaterializationBatch,
) -> str:
    values = (
        tuple(_scope_value(scope) for scope in scopes)
        if scopes is not None
        else tuple(
            (state.environment, state.source_profile) for state in batch.program_states
        )
    )
    unique = sorted(set(values))
    return _canonical_json(
        [
            {"environment": environment, "source_profile": source_profile}
            for environment, source_profile in unique
        ]
    )


def _collapse_depth(edge: LineageEdge) -> int:
    evidence = edge.evidence
    if isinstance(evidence, Mapping):
        explicit = evidence.get("collapse_depth")
        if explicit is not None:
            if isinstance(explicit, bool) or not isinstance(explicit, int):
                raise ValueError("collapse_depth must be an integer")
            if explicit < 1:
                raise ValueError("collapse_depth must be >= 1")
            return explicit
        paths = evidence.get("physical_paths")
        depths: list[int] = []
        if isinstance(paths, (list, tuple)):
            for path in paths:
                if not isinstance(path, Mapping):
                    continue
                nodes = path.get("nodes")
                if isinstance(nodes, (list, tuple)) and len(nodes) >= 2:
                    depths.append(len(nodes) - 1)
        if depths:
            return min(depths)
        pairs = evidence.get("physical_edge_pairs")
        if isinstance(pairs, (list, tuple)) and pairs:
            return 1 if len(pairs) == 1 else len(pairs)
    return 1


def _physical_row_from_edge(
    dag: ProgramPhysicalDAG,
    edge: PhysicalEdge,
    *,
    batch_id: str,
    observed_at: datetime,
    pipeline_version: str,
    previous: DWSPhysicalEdgeRow | None,
    node_map: Mapping[str, Any] | None = None,
) -> DWSPhysicalEdgeRow:
    source = dag.program_source
    identity = source.identity
    program_name = identity.program_name
    environment = identity.environment
    source_profile = identity.source_profile
    resolved_node_map = dag.node_map if node_map is None else node_map
    source_kind = _node_kind(edge.source, resolved_node_map)
    target_kind = _node_kind(edge.target, resolved_node_map)
    stable_key = physical_edge_key(
        environment,
        source_profile,
        program_name,
        edge.source,
        edge.target,
        source_kind,
        target_kind,
    )
    first_seen_at = previous.first_seen_at if previous is not None else observed_at
    last_changed_at = previous.last_changed_at if previous is not None else observed_at
    created_at = previous.created_at if previous is not None else observed_at
    return DWSPhysicalEdgeRow(
        row_key=_row_key("lineage_edge", batch_id, stable_key),
        edge_key=stable_key,
        environment=environment,
        source_profile=source_profile,
        program_key=program_key(identity),
        program_name=program_name,
        source_table=edge.source,
        target_table=edge.target,
        source_node_kind=source_kind,
        target_node_kind=target_kind,
        source_dataset_key=_dataset_key_for_node(environment, edge.source, source_kind),
        target_dataset_key=_dataset_key_for_node(environment, edge.target, target_kind),
        evidence_type=_required_text(edge.evidence_type, "evidence_type"),
        evidence_json=_evidence_json(edge.evidence),
        source_hash=source.source_hash,
        pipeline_version=pipeline_version,
        batch_id=batch_id,
        observed_at=observed_at,
        first_seen_at=first_seen_at,
        last_seen_at=observed_at,
        last_changed_at=last_changed_at,
        is_active=False,
        created_at=created_at,
        updated_at=observed_at,
    )


def _rebase_physical_row(
    row: DWSPhysicalEdgeRow,
    *,
    batch_id: str,
    observed_at: datetime,
) -> DWSPhysicalEdgeRow:
    return replace(
        row,
        row_key=_row_key("lineage_edge", batch_id, row.edge_key),
        batch_id=batch_id,
        observed_at=observed_at,
        last_seen_at=observed_at,
        is_active=False,
        updated_at=observed_at,
    )


def _prepare_physical_rows(
    batch: MaterializationBatch,
    physical_dags: Iterable[ProgramPhysicalDAG],
    previous_rows: Iterable[DWSPhysicalEdgeRow],
    *,
    pipeline_version: str,
) -> tuple[DWSPhysicalEdgeRow, ...]:
    dags = tuple(physical_dags)
    if any(not isinstance(dag, ProgramPhysicalDAG) for dag in dags):
        raise TypeError("physical_dags must contain ProgramPhysicalDAG values")
    rebuilt_programs: dict[str, ProgramPhysicalDAG] = {}
    for dag in dags:
        if any(not isinstance(edge, PhysicalEdge) for edge in dag.edges):
            raise TypeError("ProgramPhysicalDAG.edges must contain PhysicalEdge values")
        key = program_key(dag.program_source.identity)
        if key in rebuilt_programs:
            raise ValueError("physical_dags contains duplicate ProgramIdentity values")
        rebuilt_programs[key] = dag

    current_programs = {program_key(state.identity) for state in batch.program_states}
    unknown_programs = set(rebuilt_programs) - current_programs
    if unknown_programs:
        raise ValueError("physical_dags contains a program absent from the candidate")

    previous_by_key: dict[str, DWSPhysicalEdgeRow] = {}
    for row in previous_rows:
        if row.edge_key in previous_by_key:
            raise ValueError("active physical projection contains duplicate edge_key")
        previous_by_key[row.edge_key] = row

    fresh: list[DWSPhysicalEdgeRow] = []
    for dag in dags:
        node_map = dag.node_map
        for edge in dag.edges:
            source_kind = _node_kind(edge.source, node_map)
            target_kind = _node_kind(edge.target, node_map)
            stable_key = physical_edge_key(
                dag.program_source.environment,
                dag.program_source.source_profile,
                dag.program_source.program_name,
                edge.source,
                edge.target,
                source_kind,
                target_kind,
            )
            fresh.append(
                _physical_row_from_edge(
                    dag,
                    edge,
                    batch_id=batch.batch_id,
                    observed_at=batch.observed_at,
                    pipeline_version=pipeline_version,
                    previous=previous_by_key.get(stable_key),
                    node_map=node_map,
                )
            )

    retained = [
        _rebase_physical_row(
            row,
            batch_id=batch.batch_id,
            observed_at=batch.observed_at,
        )
        for row in previous_by_key.values()
        if row.program_key in current_programs
        and row.program_key not in rebuilt_programs
    ]
    return tuple(
        sorted(
            (*retained, *fresh),
            key=lambda row: (
                row.environment,
                row.source_profile,
                row.program_name,
                row.source_table,
                row.target_table,
                row.edge_key,
            ),
        )
    )


def _business_row_from_edge(
    edge: LineageEdge,
    *,
    batch_id: str,
    observed_at: datetime,
    pipeline_version: str,
    previous: DWSBusinessEdgeRow | None,
) -> DWSBusinessEdgeRow:
    if is_temporary_asset(edge.source_table) or is_temporary_asset(edge.target_table):
        raise ValueError("business lineage endpoints must be formal assets")
    if edge.program_name is None:
        raise ValueError("business edge program_name must be non-empty")
    source_identity = edge.source_dataset_identity
    target_identity = edge.target_dataset_identity
    if source_identity is None or target_identity is None:
        raise ValueError(
            "business lineage endpoints must be formal DatasetIdentity values"
        )
    program_identity = ProgramIdentity(
        edge.environment,
        edge.source_profile,
        edge.program_name,
    )
    stable_key = business_edge_key(edge)
    physical_derivation_hash = None
    if isinstance(edge.evidence, Mapping):
        previous_hash = edge.evidence.get("physical_derivation_hash")
        if (
            previous is not None
            and isinstance(previous_hash, str)
            and previous_hash.strip() == previous.physical_derivation_hash
        ):
            physical_derivation_hash = previous_hash.strip()
    if physical_derivation_hash is None:
        physical_derivation_hash = _stable_hash(
            "physical-derivation", _evidence_json(edge.evidence)
        )
    first_seen_at = (
        previous.first_seen_at
        if previous is not None
        else (edge.observed_at or observed_at)
    )
    last_changed_at = previous.last_changed_at if previous is not None else observed_at
    created_at = previous.created_at if previous is not None else observed_at
    return DWSBusinessEdgeRow(
        row_key=_row_key("lineage_business_edge", batch_id, stable_key),
        business_edge_key=stable_key,
        environment=program_identity.environment,
        source_profile=program_identity.source_profile,
        program_key=program_key(program_identity),
        program_name=program_identity.program_name,
        source_dataset_key=_stable_hash(
            "dataset",
            source_identity.environment,
            source_identity.canonical_schema,
            source_identity.canonical_table,
        ),
        source_table=source_identity.canonical_name,
        target_dataset_key=_stable_hash(
            "dataset",
            target_identity.environment,
            target_identity.canonical_schema,
            target_identity.canonical_table,
        ),
        target_table=target_identity.canonical_name,
        collapse_depth=_collapse_depth(edge),
        physical_derivation_hash=physical_derivation_hash,
        source_hash=edge.source_hash,
        pipeline_version=pipeline_version,
        batch_id=batch_id,
        observed_at=observed_at,
        first_seen_at=first_seen_at,
        last_seen_at=observed_at,
        last_changed_at=last_changed_at,
        is_active=False,
        created_at=created_at,
        updated_at=observed_at,
    )


def _issue_row_from_issue(
    issue: LineageIssue,
    *,
    batch_id: str,
    observed_at: datetime,
    pipeline_version: str,
    previous: DWSIssueRow | None,
) -> DWSIssueRow:
    stable_key = issue_stable_key(issue)
    program_identity = ProgramIdentity(
        issue.environment,
        issue.source_profile,
        issue.program_name,
    )
    first_seen_at = (
        previous.first_seen_at
        if previous is not None
        else (issue.first_seen_at or observed_at)
    )
    last_changed_at = previous.last_changed_at if previous is not None else observed_at
    created_at = previous.created_at if previous is not None else observed_at
    return DWSIssueRow(
        row_key=_row_key("lineage_issue", batch_id, stable_key),
        stable_issue_key=stable_key,
        environment=program_identity.environment,
        source_profile=program_identity.source_profile,
        program_key=program_key(program_identity),
        program_name=program_identity.program_name,
        issue_type=IssueType(issue.issue_type).value,
        confidence=AuditConfidence(issue.confidence).value,
        rule_version=_required_text(issue.rule_version, "rule_version"),
        severity=_required_text(issue.severity, "severity"),
        disposition=IssueDisposition(issue.disposition).value,
        policy_version=_required_text(issue.policy_version, "policy_version"),
        node_key=issue.node_key,
        branch_sink=issue.branch_sink,
        message=_required_text(issue.message, "message"),
        evidence_json=_evidence_json(issue.evidence),
        batch_id=batch_id,
        first_seen_at=first_seen_at,
        last_seen_at=issue.last_seen_at or observed_at,
        last_changed_at=last_changed_at,
        disposition_updated_at=issue.disposition_updated_at,
        disposition_updated_by=issue.disposition_updated_by,
        is_active=False,
        created_at=created_at,
        updated_at=observed_at,
    )


def _program_state_row(
    state: ProgramState,
    *,
    batch_id: str,
    observed_at: datetime,
) -> tuple[object, ...]:
    stable_key = program_key(state.identity)
    return (
        _row_key("lineage_program_state", batch_id, stable_key),
        stable_key,
        state.environment,
        state.source_profile,
        state.program_name,
        state.source_hash,
        state.pipeline_version,
        batch_id,
        _timestamp_param(state.first_seen_at, "first_seen_at"),
        _timestamp_param(state.last_seen_at, "last_seen_at"),
        _timestamp_param(state.last_changed_at, "last_changed_at"),
        False,
        _timestamp_param(observed_at, "created_at"),
        _timestamp_param(observed_at, "updated_at"),
    )


def _physical_values(row: DWSPhysicalEdgeRow) -> tuple[object, ...]:
    return (
        row.row_key,
        row.edge_key,
        row.environment,
        row.source_profile,
        row.program_key,
        row.program_name,
        row.source_table,
        row.target_table,
        row.source_node_kind,
        row.target_node_kind,
        row.source_dataset_key,
        row.target_dataset_key,
        row.evidence_type,
        row.evidence_json,
        row.source_hash,
        row.pipeline_version,
        row.batch_id,
        _timestamp_param(row.observed_at, "observed_at"),
        _timestamp_param(row.first_seen_at, "first_seen_at"),
        _timestamp_param(row.last_seen_at, "last_seen_at"),
        _timestamp_param(row.last_changed_at, "last_changed_at"),
        row.is_active,
        _timestamp_param(row.created_at, "created_at"),
        _timestamp_param(row.updated_at, "updated_at"),
    )


def _business_values(row: DWSBusinessEdgeRow) -> tuple[object, ...]:
    return (
        row.row_key,
        row.business_edge_key,
        row.environment,
        row.source_profile,
        row.program_key,
        row.program_name,
        row.source_dataset_key,
        row.source_table,
        row.target_dataset_key,
        row.target_table,
        row.collapse_depth,
        row.physical_derivation_hash,
        row.source_hash,
        row.pipeline_version,
        row.batch_id,
        _timestamp_param(row.observed_at, "observed_at"),
        _timestamp_param(row.first_seen_at, "first_seen_at"),
        _timestamp_param(row.last_seen_at, "last_seen_at"),
        _timestamp_param(row.last_changed_at, "last_changed_at"),
        row.is_active,
        _timestamp_param(row.created_at, "created_at"),
        _timestamp_param(row.updated_at, "updated_at"),
    )


def _issue_values(row: DWSIssueRow) -> tuple[object, ...]:
    return (
        row.row_key,
        row.stable_issue_key,
        row.environment,
        row.source_profile,
        row.program_key,
        row.program_name,
        row.issue_type,
        row.confidence,
        row.rule_version,
        row.severity,
        row.disposition,
        row.policy_version,
        row.node_key,
        row.branch_sink,
        row.message,
        row.evidence_json,
        row.batch_id,
        _timestamp_param(row.first_seen_at, "first_seen_at"),
        _timestamp_param(row.last_seen_at, "last_seen_at"),
        _timestamp_param(row.last_changed_at, "last_changed_at"),
        _timestamp_param(row.disposition_updated_at, "disposition_updated_at"),
        row.disposition_updated_by,
        row.is_active,
        _timestamp_param(row.created_at, "created_at"),
        _timestamp_param(row.updated_at, "updated_at"),
    )


def _validate_physical_row(row: DWSPhysicalEdgeRow) -> DWSPhysicalEdgeRow:
    _key_text(row.row_key, "row_key")
    _key_text(row.edge_key, "edge_key")
    expected_program_key = program_key(
        ProgramIdentity(row.environment, row.source_profile, row.program_name)
    )
    if row.program_key != expected_program_key:
        raise ValueError("stored physical edge program_key is inconsistent")
    expected_edge_key = physical_edge_key(
        row.environment,
        row.source_profile,
        row.program_name,
        row.source_table,
        row.target_table,
        row.source_node_kind,
        row.target_node_kind,
    )
    if row.edge_key != expected_edge_key:
        raise ValueError("stored physical edge stable identity is inconsistent")
    if row.row_key != _row_key("lineage_edge", row.batch_id, row.edge_key):
        raise ValueError("stored physical edge row_key is inconsistent")
    for node, kind, dataset in (
        (row.source_table, row.source_node_kind, row.source_dataset_key),
        (row.target_table, row.target_node_kind, row.target_dataset_key),
    ):
        if kind not in {
            PhysicalNodeKind.FORMAL_ASSET.value,
            PhysicalNodeKind.TEMPORARY_ASSET.value,
        }:
            raise ValueError("stored physical edge node kind is invalid")
        if is_temporary_asset(node) and kind != PhysicalNodeKind.TEMPORARY_ASSET.value:
            raise ValueError("stored physical edge node kind is inconsistent")
        if dataset != _dataset_key_for_node(row.environment, node, kind):
            raise ValueError("stored physical edge dataset key is inconsistent")
    _decode_json(row.evidence_json)
    return row


def _validate_business_row(row: DWSBusinessEdgeRow) -> DWSBusinessEdgeRow:
    _key_text(row.row_key, "row_key")
    _key_text(row.business_edge_key, "business_edge_key")
    if row.row_key != _row_key(
        "lineage_business_edge", row.batch_id, row.business_edge_key
    ):
        raise ValueError("stored business edge row_key is inconsistent")
    expected_program_key = program_key(
        ProgramIdentity(row.environment, row.source_profile, row.program_name)
    )
    if row.program_key != expected_program_key:
        raise ValueError("stored business edge program_key is inconsistent")
    if row.collapse_depth < 1:
        raise ValueError("stored business edge collapse_depth is invalid")
    source_identity = DatasetIdentity.from_name(row.environment, row.source_table)
    target_identity = DatasetIdentity.from_name(row.environment, row.target_table)
    if source_identity is None or target_identity is None:
        raise ValueError("stored business edge endpoint is not a DatasetIdentity")
    if (
        row.source_table != source_identity.canonical_name
        or row.target_table != target_identity.canonical_name
        or is_temporary_asset(row.source_table)
        or is_temporary_asset(row.target_table)
    ):
        raise ValueError("stored business edge endpoint is not formal")
    if row.source_dataset_key != dataset_key(row.environment, row.source_table):
        raise ValueError("stored business source_dataset_key is inconsistent")
    if row.target_dataset_key != dataset_key(row.environment, row.target_table):
        raise ValueError("stored business target_dataset_key is inconsistent")
    expected_key = _stable_hash(
        "business-edge",
        row.environment,
        row.source_profile,
        row.program_key,
        row.source_table,
        row.target_table,
    )
    if row.business_edge_key != expected_key:
        raise ValueError("stored business edge stable identity is inconsistent")
    _key_text(row.physical_derivation_hash, "physical_derivation_hash")
    return row


def _validate_issue_row(row: DWSIssueRow) -> DWSIssueRow:
    _key_text(row.row_key, "row_key")
    _key_text(row.stable_issue_key, "stable_issue_key")
    if row.row_key != _row_key("lineage_issue", row.batch_id, row.stable_issue_key):
        raise ValueError("stored issue row_key is inconsistent")
    expected_program_key = program_key(
        ProgramIdentity(row.environment, row.source_profile, row.program_name)
    )
    if row.program_key != expected_program_key:
        raise ValueError("stored issue program_key is inconsistent")
    IssueType(row.issue_type)
    AuditConfidence(row.confidence)
    IssueDisposition(row.disposition)
    _required_text(row.rule_version, "rule_version")
    _required_text(row.policy_version, "policy_version")
    _required_text(row.severity, "severity")
    _required_text(row.message, "message")
    _decode_json(row.evidence_json)
    return row


def _batch_from_row(row: Any) -> _DWSBatchRow:
    try:
        parsed = _DWSBatchRow(
            batch_id=_key_text(
                _stored_required_text(row[0], "batch_id"),
                "batch_id",
            ),
            snapshot_mode=_stored_required_text(row[1], "snapshot_mode"),
            complete_snapshot=_stored_bool(row[2], "complete_snapshot"),
            snapshot_scope=_stored_optional_text(row[3], "snapshot_scope"),
            pipeline_version=_stored_optional_text(row[4], "pipeline_version"),
            observed_at=_parse_datetime(row[5], "observed_at"),
            previous_batch_id=_stored_optional_text(row[6], "previous_batch_id"),
            publish_status=_stored_optional_text(row[7], "publish_status"),
            published_at=(
                None if row[8] is None else _parse_datetime(row[8], "published_at")
            ),
            program_count=_stored_int(row[9], "program_count"),
            edge_count=_stored_int(row[10], "edge_count"),
            issue_count=_stored_int(row[11], "issue_count"),
            is_active=_stored_bool(row[12], "is_active"),
            created_at=None
            if row[13] is None
            else _parse_datetime(row[13], "created_at"),
            updated_at=None
            if row[14] is None
            else _parse_datetime(row[14], "updated_at"),
        )
        if parsed.snapshot_mode not in {"FULL", "PARTIAL"}:
            raise ValueError("snapshot_mode is invalid")
        if (parsed.snapshot_mode == "FULL") != parsed.complete_snapshot:
            raise ValueError("snapshot metadata is inconsistent")
        if parsed.publish_status not in {"CANDIDATE", "PUBLISHED", "RETIRED"}:
            raise ValueError("publish_status is invalid")
        if parsed.complete_snapshot and not parsed.snapshot_scope:
            raise ValueError("complete snapshot must have a snapshot scope")
        if parsed.is_active and parsed.publish_status != "PUBLISHED":
            raise ValueError("active batch must be PUBLISHED")
        if min(parsed.program_count, parsed.edge_count, parsed.issue_count) < 0:
            raise ValueError("batch counts must be non-negative")
        return parsed
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored DWS batch metadata is invalid") from exc


def _physical_from_row(row: Any) -> DWSPhysicalEdgeRow:
    try:
        parsed = DWSPhysicalEdgeRow(
            row_key=_stored_required_text(row[0], "row_key"),
            edge_key=_stored_required_text(row[1], "edge_key"),
            environment=_stored_required_text(row[2], "environment"),
            source_profile=_stored_required_text(row[3], "source_profile"),
            program_key=_stored_required_text(row[4], "program_key"),
            program_name=_stored_required_text(row[5], "program_name"),
            source_table=_stored_required_text(row[6], "source_table"),
            target_table=_stored_required_text(row[7], "target_table"),
            source_node_kind=_stored_required_text(row[8], "source_node_kind"),
            target_node_kind=_stored_required_text(row[9], "target_node_kind"),
            source_dataset_key=_stored_optional_text(row[10], "source_dataset_key"),
            target_dataset_key=_stored_optional_text(row[11], "target_dataset_key"),
            evidence_type=_stored_required_text(row[12], "evidence_type"),
            evidence_json=_stored_required_text(row[13], "evidence_json"),
            source_hash=_stored_optional_text(row[14], "source_hash"),
            pipeline_version=_stored_optional_text(row[15], "pipeline_version"),
            batch_id=_stored_required_text(row[16], "batch_id"),
            observed_at=_parse_datetime(row[17], "observed_at"),
            first_seen_at=_parse_datetime(row[18], "first_seen_at"),
            last_seen_at=_parse_datetime(row[19], "last_seen_at"),
            last_changed_at=_parse_datetime(row[20], "last_changed_at"),
            is_active=_stored_bool(row[21], "is_active"),
            created_at=_parse_datetime(row[22], "created_at"),
            updated_at=_parse_datetime(row[23], "updated_at"),
        )
        return _validate_physical_row(parsed)
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored DWS physical edge is invalid") from exc


def _business_from_row(row: Any) -> DWSBusinessEdgeRow:
    try:
        depth = _stored_int(row[10], "collapse_depth")
        parsed = DWSBusinessEdgeRow(
            row_key=_stored_required_text(row[0], "row_key"),
            business_edge_key=_stored_required_text(row[1], "business_edge_key"),
            environment=_stored_required_text(row[2], "environment"),
            source_profile=_stored_required_text(row[3], "source_profile"),
            program_key=_stored_required_text(row[4], "program_key"),
            program_name=_stored_required_text(row[5], "program_name"),
            source_dataset_key=_stored_required_text(row[6], "source_dataset_key"),
            source_table=_stored_required_text(row[7], "source_table"),
            target_dataset_key=_stored_required_text(row[8], "target_dataset_key"),
            target_table=_stored_required_text(row[9], "target_table"),
            collapse_depth=depth,
            physical_derivation_hash=_key_text(
                _stored_required_text(row[11], "physical_derivation_hash"),
                "physical_derivation_hash",
            ),
            source_hash=_stored_optional_text(row[12], "source_hash"),
            pipeline_version=_stored_optional_text(row[13], "pipeline_version"),
            batch_id=_stored_required_text(row[14], "batch_id"),
            observed_at=_parse_datetime(row[15], "observed_at"),
            first_seen_at=_parse_datetime(row[16], "first_seen_at"),
            last_seen_at=_parse_datetime(row[17], "last_seen_at"),
            last_changed_at=_parse_datetime(row[18], "last_changed_at"),
            is_active=_stored_bool(row[19], "is_active"),
            created_at=_parse_datetime(row[20], "created_at"),
            updated_at=_parse_datetime(row[21], "updated_at"),
        )
        return _validate_business_row(parsed)
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored DWS business edge is invalid") from exc


def _issue_from_row(row: Any) -> DWSIssueRow:
    try:
        parsed = DWSIssueRow(
            row_key=_stored_required_text(row[0], "row_key"),
            stable_issue_key=_stored_required_text(row[1], "stable_issue_key"),
            environment=_stored_required_text(row[2], "environment"),
            source_profile=_stored_required_text(row[3], "source_profile"),
            program_key=_stored_required_text(row[4], "program_key"),
            program_name=_stored_required_text(row[5], "program_name"),
            issue_type=_stored_required_text(row[6], "issue_type"),
            confidence=_stored_required_text(row[7], "confidence"),
            rule_version=_stored_required_text(row[8], "rule_version"),
            severity=_stored_required_text(row[9], "severity"),
            disposition=_stored_required_text(row[10], "disposition"),
            policy_version=_stored_required_text(row[11], "policy_version"),
            node_key=_stored_optional_text(row[12], "node_key"),
            branch_sink=_stored_optional_text(row[13], "branch_sink"),
            message=_stored_required_text(row[14], "message"),
            evidence_json=_stored_required_text(row[15], "evidence_json"),
            batch_id=_stored_required_text(row[16], "batch_id"),
            first_seen_at=_parse_datetime(row[17], "first_seen_at"),
            last_seen_at=_parse_datetime(row[18], "last_seen_at"),
            last_changed_at=_parse_datetime(row[19], "last_changed_at"),
            disposition_updated_at=(
                None
                if row[20] is None
                else _parse_datetime(row[20], "disposition_updated_at")
            ),
            disposition_updated_by=_stored_optional_text(
                row[21], "disposition_updated_by"
            ),
            is_active=_stored_bool(row[22], "is_active"),
            created_at=_parse_datetime(row[23], "created_at"),
            updated_at=_parse_datetime(row[24], "updated_at"),
        )
        return _validate_issue_row(parsed)
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored DWS issue is invalid") from exc


def _program_state_from_row(row: Any) -> ProgramState:
    try:
        return ProgramState(
            environment=_stored_required_text(row[2], "environment"),
            source_profile=_stored_required_text(row[3], "source_profile"),
            program_name=_stored_required_text(row[4], "program_name"),
            source_hash=_stored_optional_text(row[5], "source_hash"),
            pipeline_version=_stored_optional_text(row[6], "pipeline_version"),
            first_seen_at=_parse_datetime(row[8], "first_seen_at"),
            last_seen_at=_parse_datetime(row[9], "last_seen_at"),
            last_changed_at=(
                None if row[10] is None else _parse_datetime(row[10], "last_changed_at")
            ),
            batch_id=_stored_required_text(row[7], "batch_id"),
            is_active=_stored_bool(row[11], "is_active"),
        )
    except (IndexError, TypeError, ValueError) as exc:
        raise ValueError("stored DWS program state is invalid") from exc


def _business_to_edge(row: DWSBusinessEdgeRow) -> LineageEdge:
    evidence: dict[str, object] = {"collapse_depth": row.collapse_depth}
    if row.physical_derivation_hash:
        evidence["physical_derivation_hash"] = row.physical_derivation_hash
    return LineageEdge(
        environment=row.environment,
        source_profile=row.source_profile,
        source_table=row.source_table,
        target_table=row.target_table,
        program_name=row.program_name,
        evidence_type="physical_dag",
        source_hash=row.source_hash,
        batch_id=row.batch_id,
        observed_at=row.observed_at,
        updated_at=row.updated_at,
        is_active=row.is_active,
        evidence=evidence,
    )


def _issue_to_issue(row: DWSIssueRow) -> LineageIssue:
    return LineageIssue(
        environment=row.environment,
        source_profile=row.source_profile,
        program_name=row.program_name,
        issue_type=IssueType(row.issue_type),
        severity=row.severity,
        message=row.message,
        node_key=row.node_key,
        branch_sink=row.branch_sink,
        evidence=_decode_json(row.evidence_json),
        batch_id=row.batch_id,
        first_seen_at=row.first_seen_at,
        last_seen_at=row.last_seen_at,
        is_active=row.is_active,
        stable_key=row.stable_issue_key,
        confidence=AuditConfidence(row.confidence),
        rule_version=row.rule_version,
        disposition=IssueDisposition(row.disposition),
        policy_version=row.policy_version,
        disposition_updated_at=row.disposition_updated_at,
        disposition_updated_by=row.disposition_updated_by,
    )


class DWSMaterializationStore:
    """Atomic DWS repository using the existing GaussDB JDBC connection helper."""

    backend_name = "dws"
    supports_physical_edges = True

    def __init__(
        self,
        profile: str | None = None,
        *,
        connection: Any | None = None,
        connection_factory: Callable[[str], Any] | None = None,
    ) -> None:
        if profile is not None and not isinstance(profile, str):
            raise TypeError("profile must be a string or None")
        if connection_factory is not None and not callable(connection_factory):
            raise TypeError("connection_factory must be callable or None")
        self.profile = (
            profile.strip()
            if isinstance(profile, str) and profile.strip()
            else os.getenv("PYTOOLS_LINEAGE_DWS_PROFILE", "").strip() or None
        )
        if connection is None and self.profile is None:
            raise ValueError(
                "DWS materialization needs a database profile or injected connection"
            )
        self._connection = connection
        self._connection_factory = connection_factory or _connect_with_profile

    @property
    def connection(self) -> Any | None:
        return self._connection

    @contextmanager
    def _connection_scope(self) -> Iterator[Any]:
        if self._connection is not None:
            yield self._connection
            return
        if self.profile is None:
            raise RuntimeError("DWS materialization has no database profile")
        connection = self._connection_factory(self.profile)
        if connection is None:
            raise RuntimeError("DWS connection factory returned no connection")
        try:
            yield connection
        finally:
            _close_quietly(connection)

    @staticmethod
    @contextmanager
    def _cursor_scope(connection: Any) -> Iterator[Any]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            _close_quietly(cursor)

    @staticmethod
    def _execute(cursor: Any, sql: str, params: Iterable[object] = ()) -> Any:
        # SQL is selected from fixed module constants; runtime values are bound.
        # pi-lens-ignore: python-sql-injection
        return cursor.execute(sql, tuple(params))

    @staticmethod
    def _executemany(cursor: Any, sql: str, rows: Iterable[Iterable[object]]) -> None:
        values = tuple(tuple(row) for row in rows)
        if values:
            # SQL is selected from fixed module constants; row values are bound.
            # pi-lens-ignore: python-sql-injection
            cursor.executemany(sql, values)

    def _fetch_rows(
        self,
        connection: Any,
        select_sql: str,
        join_sql: str,
        where_sql: str,
        params: Iterable[object],
        order_sql: str,
        converter: Callable[[Any], Any],
    ) -> tuple[Any, ...]:
        sql = select_sql + join_sql + where_sql + order_sql
        with self._cursor_scope(connection) as cursor:
            self._execute(cursor, sql, params)
            rows = cursor.fetchall()
        return tuple(converter(row) for row in rows)

    @staticmethod
    def _where_for_batch_and_active(
        alias: str,
        *,
        batch_id: str | None,
        active_only: bool,
    ) -> tuple[str, tuple[object, ...]]:
        conditions: list[str] = []
        params: list[object] = []
        if alias == "s":
            batch_condition = "s.batch_id = ?"
            active_condition = "s.is_active = TRUE"
        elif alias == "e":
            batch_condition = "e.batch_id = ?"
            active_condition = "e.is_active = TRUE"
        elif alias == "i":
            batch_condition = "i.batch_id = ?"
            active_condition = "i.is_active = TRUE"
        else:
            raise ValueError("unsupported DWS fact alias")
        if batch_id is not None:
            conditions.append(batch_condition)
            params.append(_required_text(batch_id, "batch_id"))
        if active_only:
            conditions.append(active_condition)
        if not conditions:
            return "", ()
        return " WHERE " + " AND ".join(conditions), tuple(params)

    def _fetch_program_state_rows(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[tuple[Any, ...], ...]:
        where, params = self._where_for_batch_and_active(
            "s", batch_id=batch_id, active_only=active_only
        )
        join = ACTIVE_STATE_JOIN if active_only else ""
        return self._fetch_rows(
            connection,
            PROGRAM_STATE_SELECT_SQL,
            join,
            where,
            params,
            " ORDER BY s.environment, s.source_profile, s.program_name, s.row_key",
            lambda row: tuple(row),
        )

    def _fetch_program_states(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[ProgramState, ...]:
        states: list[ProgramState] = []
        seen_row_keys: set[str] = set()
        seen_program_keys: set[str] = set()
        for raw in self._fetch_program_state_rows(
            connection,
            batch_id=batch_id,
            active_only=active_only,
        ):
            row_key = _key_text(
                _stored_required_text(raw[0], "row_key"),
                "row_key",
            )
            stored_program_key = _key_text(
                _stored_required_text(raw[1], "program_key"),
                "program_key",
            )
            state = _program_state_from_row(raw)
            state_batch_id = _required_text(state.batch_id, "batch_id")
            expected_program_key = program_key(state.identity)
            if stored_program_key != expected_program_key:
                raise ValueError("stored program state program_key is inconsistent")
            if row_key != _row_key(
                "lineage_program_state", state_batch_id, expected_program_key
            ):
                raise ValueError("stored program state row_key is inconsistent")
            if row_key in seen_row_keys or stored_program_key in seen_program_keys:
                raise ValueError("stored program state contains duplicate identity")
            seen_row_keys.add(row_key)
            seen_program_keys.add(stored_program_key)
            states.append(state)
        return tuple(states)

    def _fetch_physical_rows(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSPhysicalEdgeRow, ...]:
        where, params = self._where_for_batch_and_active(
            "e", batch_id=batch_id, active_only=active_only
        )
        join = ACTIVE_BATCH_JOIN if active_only else ""
        return self._fetch_rows(
            connection,
            PHYSICAL_EDGE_SELECT_SQL,
            join,
            where,
            params,
            " ORDER BY e.environment, e.source_profile, e.program_name, "
            "e.source_table, e.target_table, e.edge_key",
            _physical_from_row,
        )

    def _fetch_business_rows(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSBusinessEdgeRow, ...]:
        where, params = self._where_for_batch_and_active(
            "e", batch_id=batch_id, active_only=active_only
        )
        join = ACTIVE_BATCH_JOIN if active_only else ""
        return self._fetch_rows(
            connection,
            BUSINESS_EDGE_SELECT_SQL,
            join,
            where,
            params,
            " ORDER BY e.environment, e.source_profile, e.program_name, "
            "e.source_table, e.target_table, e.business_edge_key",
            _business_from_row,
        )

    def _fetch_issue_rows(
        self,
        connection: Any,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSIssueRow, ...]:
        where, params = self._where_for_batch_and_active(
            "i", batch_id=batch_id, active_only=active_only
        )
        join = ACTIVE_ISSUE_JOIN if active_only else ""
        return self._fetch_rows(
            connection,
            ISSUE_SELECT_SQL,
            join,
            where,
            params,
            " ORDER BY i.environment, i.source_profile, i.program_name, "
            "i.issue_type, i.stable_issue_key, i.row_key",
            _issue_from_row,
        )

    def _fetch_active_batch_row(self, connection: Any) -> _DWSBatchRow | None:
        with self._cursor_scope(connection) as cursor:
            self._execute(cursor, BATCH_SELECT_SQL + " WHERE is_active = TRUE")
            rows = cursor.fetchall()
        if len(rows) > 1:
            raise ValueError("DWS contains more than one active batch")
        if not rows:
            return None
        active = _batch_from_row(rows[0])
        if active.publish_status != "PUBLISHED":
            raise ValueError("active batch must have PUBLISHED status")
        return active

    def _fetch_batch_row(
        self,
        connection: Any,
        batch_id: str,
    ) -> _DWSBatchRow | None:
        batch_id = _required_text(batch_id, "batch_id")
        with self._cursor_scope(connection) as cursor:
            self._execute(
                cursor,
                BATCH_SELECT_SQL + " WHERE batch_id = ?",
                (batch_id,),
            )
            rows = cursor.fetchall()
        if len(rows) > 1:
            raise ValueError("DWS contains duplicate batch metadata")
        return None if not rows else _batch_from_row(rows[0])

    def _prepare_candidate(
        self,
        batch: MaterializationBatch,
        *,
        physical_dags: Iterable[ProgramPhysicalDAG],
        previous_batch_id: str | None,
        previous_physical: Iterable[DWSPhysicalEdgeRow],
        previous_business: Iterable[DWSBusinessEdgeRow],
        previous_issues: Iterable[DWSIssueRow],
        complete_snapshot: bool,
        snapshot_scopes: Iterable[
            SnapshotScope
            | ProgramIdentity
            | ProgramSource
            | ProgramState
            | tuple[str, str]
        ]
        | None,
        snapshot_mode: str | None,
        pipeline_version: str,
        instrumentation: DWSPublishMetrics | None,
    ) -> _DWSPreparedCandidate:
        if not isinstance(batch, MaterializationBatch):
            raise TypeError("batch must be a MaterializationBatch")
        if any(not isinstance(edge, LineageEdge) for edge in batch.edges):
            raise TypeError("batch.edges must contain LineageEdge values")
        if any(not isinstance(issue, LineageIssue) for issue in batch.issues):
            raise TypeError("batch.issues must contain LineageIssue values")
        if any(not isinstance(state, ProgramState) for state in batch.program_states):
            raise TypeError("batch.program_states must contain ProgramState values")
        if any(
            item.batch_id is not None and item.batch_id != batch.batch_id
            for item in (*batch.edges, *batch.issues, *batch.program_states)
        ):
            raise ValueError(
                "candidate fact batch_id does not match MaterializationBatch"
            )
        if not isinstance(complete_snapshot, bool):
            raise TypeError("complete_snapshot must be a boolean")
        resolved_mode = (
            snapshot_mode.strip().upper()
            if isinstance(snapshot_mode, str) and snapshot_mode.strip()
            else ("FULL" if complete_snapshot else "PARTIAL")
        )
        if resolved_mode not in {"FULL", "PARTIAL"}:
            raise ValueError("snapshot_mode must be FULL or PARTIAL")
        if (resolved_mode == "FULL") != complete_snapshot:
            raise ValueError("snapshot_mode and complete_snapshot disagree")
        if complete_snapshot and snapshot_scopes is None:
            raise ValueError("complete snapshot requires explicit snapshot_scopes")

        previous_physical = tuple(previous_physical)
        previous_business = tuple(previous_business)
        previous_issues = tuple(previous_issues)
        previous_business_by_key: dict[str, DWSBusinessEdgeRow] = {}
        for row in previous_business:
            if row.business_edge_key in previous_business_by_key:
                raise ValueError(
                    "active business projection contains duplicate business_edge_key"
                )
            previous_business_by_key[row.business_edge_key] = row
        previous_issue_by_key: dict[str, DWSIssueRow] = {}
        for row in previous_issues:
            if row.stable_issue_key in previous_issue_by_key:
                raise ValueError(
                    "active issue projection contains duplicate stable_issue_key"
                )
            previous_issue_by_key[row.stable_issue_key] = row
        physical_rows = _prepare_physical_rows(
            batch,
            physical_dags,
            previous_physical,
            pipeline_version=pipeline_version,
        )
        business_rows = tuple(
            _business_row_from_edge(
                edge,
                batch_id=batch.batch_id,
                observed_at=batch.observed_at,
                pipeline_version=pipeline_version,
                previous=previous_business_by_key.get(business_edge_key(edge)),
            )
            for edge in batch.edges
        )
        issue_rows = tuple(
            _issue_row_from_issue(
                issue,
                batch_id=batch.batch_id,
                observed_at=batch.observed_at,
                pipeline_version=pipeline_version,
                previous=previous_issue_by_key.get(issue_stable_key(issue)),
            )
            for issue in batch.issues
        )
        program_rows = tuple(
            _program_state_row(
                state,
                batch_id=batch.batch_id,
                observed_at=batch.observed_at,
            )
            for state in batch.program_states
        )
        observed_at = batch.observed_at
        snapshot_scope = _snapshot_scope_json(snapshot_scopes, batch)
        if complete_snapshot and snapshot_scope == "[]":
            raise ValueError("complete snapshot requires a non-empty snapshot scope")
        batch_row = _DWSBatchRow(
            batch_id=batch.batch_id,
            snapshot_mode=resolved_mode,
            complete_snapshot=complete_snapshot,
            snapshot_scope=snapshot_scope,
            pipeline_version=pipeline_version,
            observed_at=observed_at,
            previous_batch_id=previous_batch_id,
            publish_status="CANDIDATE",
            published_at=None,
            program_count=len(program_rows),
            edge_count=len(physical_rows),
            issue_count=len(issue_rows),
            is_active=False,
            created_at=observed_at,
            updated_at=observed_at,
        )
        if instrumentation is not None:
            instrumentation.prepared_edge_rows = len(physical_rows)
            instrumentation.prepared_business_rows = len(business_rows)
            instrumentation.prepared_issue_rows = len(issue_rows)
            instrumentation.prepared_program_rows = len(program_rows)
            instrumentation.evidence_serialization_calls = (
                len(physical_rows) + len(business_rows) + len(issue_rows)
            )
        return _DWSPreparedCandidate(
            batch=batch,
            previous_batch_id=previous_batch_id,
            batch_row=batch_row,
            physical_rows=physical_rows,
            business_rows=business_rows,
            issue_rows=issue_rows,
            program_state_rows=program_rows,
        )

    @staticmethod
    def _batch_values(row: _DWSBatchRow) -> tuple[object, ...]:
        return (
            row.batch_id,
            row.snapshot_mode,
            row.complete_snapshot,
            row.snapshot_scope,
            row.pipeline_version,
            _timestamp_param(row.observed_at, "observed_at"),
            row.previous_batch_id,
            row.publish_status,
            _timestamp_param(row.published_at, "published_at"),
            row.program_count,
            row.edge_count,
            row.issue_count,
            row.is_active,
            _timestamp_param(row.created_at, "created_at"),
            _timestamp_param(row.updated_at, "updated_at"),
        )

    def _insert_candidate(
        self,
        connection: Any,
        candidate: _DWSPreparedCandidate,
    ) -> None:
        with self._cursor_scope(connection) as cursor:
            self._execute(
                cursor, INSERT_BATCH_SQL, self._batch_values(candidate.batch_row)
            )
            self._executemany(
                cursor,
                INSERT_PROGRAM_STATE_SQL,
                candidate.program_state_rows,
            )
            self._executemany(
                cursor,
                INSERT_PHYSICAL_EDGE_SQL,
                (_physical_values(row) for row in candidate.physical_rows),
            )
            self._executemany(
                cursor,
                INSERT_BUSINESS_EDGE_SQL,
                (_business_values(row) for row in candidate.business_rows),
            )
            self._executemany(
                cursor,
                INSERT_ISSUE_SQL,
                (_issue_values(row) for row in candidate.issue_rows),
            )

    @staticmethod
    def _count(
        connection: Any,
        table_name: str,
        batch_id: str,
    ) -> int:
        queries = {
            "lineage_program_state": "SELECT COUNT(*) FROM dwp.lineage_program_state WHERE batch_id = ?",
            "lineage_edge": "SELECT COUNT(*) FROM dwp.lineage_edge WHERE batch_id = ?",
            "lineage_business_edge": "SELECT COUNT(*) FROM dwp.lineage_business_edge WHERE batch_id = ?",
            "lineage_issue": "SELECT COUNT(*) FROM dwp.lineage_issue WHERE batch_id = ?",
        }
        sql = queries.get(table_name)
        if sql is None:
            raise ValueError("unsupported DWS count table")
        cursor = connection.cursor()
        try:
            # SQL is selected from a fixed table-name map; batch_id is bound.
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql, (batch_id,))
            row = cursor.fetchone()
        finally:
            _close_quietly(cursor)
        if row is None:
            raise ValueError(f"count query returned no row for {table_name}")
        return int(row[0])

    @staticmethod
    def _count_active(connection: Any, table_name: str) -> int:
        queries = {
            "lineage_program_state": "SELECT COUNT(*) FROM dwp.lineage_program_state WHERE is_active = TRUE",
            "lineage_edge": "SELECT COUNT(*) FROM dwp.lineage_edge WHERE is_active = TRUE",
            "lineage_business_edge": "SELECT COUNT(*) FROM dwp.lineage_business_edge WHERE is_active = TRUE",
            "lineage_issue": "SELECT COUNT(*) FROM dwp.lineage_issue WHERE is_active = TRUE",
        }
        sql = queries.get(table_name)
        if sql is None:
            raise ValueError("unsupported DWS active count table")
        cursor = connection.cursor()
        try:
            # SQL is selected from a fixed table-name map; no runtime identifier is used.
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql)
            row = cursor.fetchone()
        finally:
            _close_quietly(cursor)
        if row is None:
            raise ValueError(f"active count query returned no row for {table_name}")
        return int(row[0])

    @staticmethod
    def _validate_nonempty_keys(
        row_key: str,
        stable_key: str,
        *,
        stable_name: str,
        seen_row_keys: set[str],
        seen_stable_keys: set[str],
    ) -> None:
        _key_text(row_key, "row_key")
        _key_text(stable_key, stable_name)
        if row_key in seen_row_keys:
            raise ValueError(f"candidate contains duplicate row_key: {stable_name}")
        if stable_key in seen_stable_keys:
            raise ValueError(
                f"candidate contains duplicate stable identity: {stable_name}"
            )
        seen_row_keys.add(row_key)
        seen_stable_keys.add(stable_key)

    def _validate_candidate_in_transaction(
        self,
        connection: Any,
        candidate: _DWSPreparedCandidate,
        instrumentation: DWSPublishMetrics | None = None,
    ) -> None:
        batch = candidate.batch
        batch_row = candidate.batch_row
        _key_text(batch.batch_id, "batch_id")
        if batch_row.batch_id != batch.batch_id:
            raise ValueError("candidate batch metadata has a different batch_id")
        if batch_row.is_active:
            raise ValueError("candidate batch must be inactive before validation")
        if batch_row.publish_status != "CANDIDATE":
            raise ValueError("candidate batch must have CANDIDATE status")
        if batch_row.pipeline_version != _pipeline_version_for_batch(batch):
            raise ValueError("candidate batch pipeline_version is inconsistent")

        state_keys: set[tuple[str, str, str]] = set()
        state_program_keys: set[str] = set()
        state_details: dict[str, tuple[object, object]] = {}
        state_row_keys: set[str] = set()
        for values in candidate.program_state_rows:
            row_key = str(values[0])
            stable_key = str(values[1])
            self._validate_nonempty_keys(
                row_key,
                stable_key,
                stable_name="program_key",
                seen_row_keys=state_row_keys,
                seen_stable_keys=state_program_keys,
            )
            identity = (
                _required_text(str(values[2]), "environment"),
                _required_text(str(values[3]), "source_profile"),
                _required_text(str(values[4]), "program_name"),
            )
            if identity in state_keys:
                raise ValueError("candidate batch contains duplicate ProgramIdentity")
            state_keys.add(identity)
            expected_key = program_key(ProgramIdentity(*identity))
            if stable_key != expected_key:
                raise ValueError("program_key does not match ProgramIdentity")
            if row_key != _row_key("lineage_program_state", batch.batch_id, stable_key):
                raise ValueError("program state row_key is inconsistent")
            if bool(values[11]):
                raise ValueError("candidate program state must be inactive")
            state_details[stable_key] = (values[5], values[6])

        physical_row_keys: set[str] = set()
        physical_keys: set[str] = set()
        for row in candidate.physical_rows:
            self._validate_nonempty_keys(
                row.row_key,
                row.edge_key,
                stable_name="edge_key",
                seen_row_keys=physical_row_keys,
                seen_stable_keys=physical_keys,
            )
            if row.batch_id != batch.batch_id:
                raise ValueError("physical edge batch_id does not match candidate")
            if row.row_key != _row_key("lineage_edge", batch.batch_id, row.edge_key):
                raise ValueError("physical edge row_key is inconsistent")
            expected_program = program_key(
                ProgramIdentity(row.environment, row.source_profile, row.program_name)
            )
            if (
                row.program_key != expected_program
                or expected_program not in state_program_keys
            ):
                raise ValueError("physical edge program identity is inconsistent")
            state_source_hash, state_pipeline = state_details[expected_program]
            if row.source_hash != state_source_hash:
                raise ValueError("physical edge source_hash is inconsistent")
            if state_pipeline is not None and row.pipeline_version != state_pipeline:
                raise ValueError("physical edge pipeline_version is inconsistent")
            if row.source_node_kind not in {
                PhysicalNodeKind.FORMAL_ASSET.value,
                PhysicalNodeKind.TEMPORARY_ASSET.value,
            } or row.target_node_kind not in {
                PhysicalNodeKind.FORMAL_ASSET.value,
                PhysicalNodeKind.TEMPORARY_ASSET.value,
            }:
                raise ValueError("physical edge node kind is invalid")
            if (
                is_temporary_asset(row.source_table)
                and row.source_node_kind != PhysicalNodeKind.TEMPORARY_ASSET.value
            ) or (
                is_temporary_asset(row.target_table)
                and row.target_node_kind != PhysicalNodeKind.TEMPORARY_ASSET.value
            ):
                raise ValueError("TMP physical endpoint must be temporary_asset")
            expected_edge = physical_edge_key(
                row.environment,
                row.source_profile,
                row.program_name,
                row.source_table,
                row.target_table,
                row.source_node_kind,
                row.target_node_kind,
            )
            if row.edge_key != expected_edge:
                raise ValueError("physical edge stable identity is inconsistent")
            for table, node, kind, actual in (
                (
                    "source",
                    row.source_table,
                    row.source_node_kind,
                    row.source_dataset_key,
                ),
                (
                    "target",
                    row.target_table,
                    row.target_node_kind,
                    row.target_dataset_key,
                ),
            ):
                expected_dataset = _dataset_key_for_node(row.environment, node, kind)
                if actual != expected_dataset:
                    raise ValueError(
                        f"physical {table}_dataset_key is inconsistent with node kind"
                    )
            _decode_json(row.evidence_json)
            if row.is_active:
                raise ValueError("candidate physical edge must be inactive")

        business_row_keys: set[str] = set()
        business_keys: set[str] = set()
        for row in candidate.business_rows:
            self._validate_nonempty_keys(
                row.row_key,
                row.business_edge_key,
                stable_name="business_edge_key",
                seen_row_keys=business_row_keys,
                seen_stable_keys=business_keys,
            )
            if row.batch_id != batch.batch_id:
                raise ValueError("business edge batch_id does not match candidate")
            if row.row_key != _row_key(
                "lineage_business_edge", batch.batch_id, row.business_edge_key
            ):
                raise ValueError("business edge row_key is inconsistent")
            if (
                isinstance(row.collapse_depth, bool)
                or not isinstance(row.collapse_depth, int)
                or row.collapse_depth < 1
            ):
                raise ValueError("collapse_depth must be an integer >= 1")
            source_identity = DatasetIdentity.from_name(
                row.environment, row.source_table
            )
            target_identity = DatasetIdentity.from_name(
                row.environment, row.target_table
            )
            if source_identity is None or target_identity is None:
                raise ValueError(
                    "business edge endpoints must be formal DatasetIdentity values"
                )
            if is_temporary_asset(row.source_table) or is_temporary_asset(
                row.target_table
            ):
                raise ValueError("TMP endpoint is forbidden in business lineage")
            expected_source_key = dataset_key(row.environment, row.source_table)
            expected_target_key = dataset_key(row.environment, row.target_table)
            if (
                row.source_dataset_key != expected_source_key
                or row.target_dataset_key != expected_target_key
            ):
                raise ValueError("business DatasetIdentity key is inconsistent")
            expected_program = program_key(
                ProgramIdentity(row.environment, row.source_profile, row.program_name)
            )
            if (
                row.program_key != expected_program
                or expected_program not in state_program_keys
            ):
                raise ValueError("business edge program identity is inconsistent")
            state_source_hash, state_pipeline = state_details[expected_program]
            if row.source_hash != state_source_hash:
                raise ValueError("business edge source_hash is inconsistent")
            if state_pipeline is not None and row.pipeline_version != state_pipeline:
                raise ValueError("business edge pipeline_version is inconsistent")
            expected_edge = _stable_hash(
                "business-edge",
                row.environment,
                row.source_profile,
                row.program_key,
                row.source_table,
                row.target_table,
            )
            if row.business_edge_key != expected_edge:
                raise ValueError("business edge stable identity is inconsistent")
            _key_text(row.physical_derivation_hash, "physical_derivation_hash")
            if row.is_active:
                raise ValueError("candidate business edge must be inactive")

        issue_row_keys: set[str] = set()
        issue_keys: set[str] = set()
        for row in candidate.issue_rows:
            self._validate_nonempty_keys(
                row.row_key,
                row.stable_issue_key,
                stable_name="stable_issue_key",
                seen_row_keys=issue_row_keys,
                seen_stable_keys=issue_keys,
            )
            if row.batch_id != batch.batch_id:
                raise ValueError("issue batch_id does not match candidate")
            if row.row_key != _row_key(
                "lineage_issue", batch.batch_id, row.stable_issue_key
            ):
                raise ValueError("issue row_key is inconsistent")
            expected_program = program_key(
                ProgramIdentity(row.environment, row.source_profile, row.program_name)
            )
            if row.program_key != expected_program or (
                expected_program not in state_program_keys
                and row.disposition != IssueDisposition.RESOLVED.value
            ):
                raise ValueError("issue program identity is inconsistent")
            IssueType(row.issue_type)
            AuditConfidence(row.confidence)
            IssueDisposition(row.disposition)
            _required_text(row.rule_version, "rule_version")
            _required_text(row.policy_version, "policy_version")
            _required_text(row.severity, "severity")
            _required_text(row.message, "message")
            _decode_json(row.evidence_json)
            if row.is_active:
                raise ValueError("candidate issue must be inactive")

        stored_counts = {
            "lineage_program_state": self._count(
                connection, "lineage_program_state", batch.batch_id
            ),
            "lineage_edge": self._count(connection, "lineage_edge", batch.batch_id),
            "lineage_business_edge": self._count(
                connection, "lineage_business_edge", batch.batch_id
            ),
            "lineage_issue": self._count(connection, "lineage_issue", batch.batch_id),
        }
        expected_counts = {
            "lineage_program_state": len(candidate.program_state_rows),
            "lineage_edge": len(candidate.physical_rows),
            "lineage_business_edge": len(candidate.business_rows),
            "lineage_issue": len(candidate.issue_rows),
        }
        if candidate.business_rows and not candidate.physical_rows:
            raise ValueError("business projection cannot exist without physical rows")
        if stored_counts != expected_counts:
            raise ValueError(
                "candidate row counts do not match prepared batch: "
                f"stored={stored_counts!r} expected={expected_counts!r}"
            )
        if batch_row.program_count != stored_counts["lineage_program_state"]:
            raise ValueError("batch program_count does not match rows")
        if batch_row.edge_count != stored_counts["lineage_edge"]:
            raise ValueError("batch edge_count does not match physical rows")
        if batch_row.issue_count != stored_counts["lineage_issue"]:
            raise ValueError("batch issue_count does not match rows")

        stored_batch = self._fetch_batch_row(connection, batch.batch_id)
        if stored_batch is None:
            raise ValueError("candidate batch metadata is missing")
        expected_metadata = (
            candidate.batch_row.snapshot_mode,
            candidate.batch_row.complete_snapshot,
            candidate.batch_row.snapshot_scope,
            candidate.batch_row.pipeline_version,
            candidate.batch_row.observed_at,
            candidate.batch_row.previous_batch_id,
            candidate.batch_row.publish_status,
            candidate.batch_row.published_at,
            candidate.batch_row.program_count,
            candidate.batch_row.edge_count,
            candidate.batch_row.issue_count,
            candidate.batch_row.is_active,
        )
        stored_metadata = (
            stored_batch.snapshot_mode,
            stored_batch.complete_snapshot,
            stored_batch.snapshot_scope,
            stored_batch.pipeline_version,
            stored_batch.observed_at,
            stored_batch.previous_batch_id,
            stored_batch.publish_status,
            stored_batch.published_at,
            stored_batch.program_count,
            stored_batch.edge_count,
            stored_batch.issue_count,
            stored_batch.is_active,
        )
        if stored_metadata != expected_metadata:
            raise ValueError(
                "stored candidate batch metadata does not match prepared batch"
            )
        if stored_batch.is_active or stored_batch.publish_status != "CANDIDATE":
            raise ValueError("candidate batch is active or not in CANDIDATE state")
        active_batch = self._fetch_active_batch_row(connection)
        if active_batch is not None and active_batch.publish_status != "PUBLISHED":
            raise ValueError("active batch must have PUBLISHED status")
        if (
            None if active_batch is None else active_batch.batch_id
        ) != candidate.previous_batch_id:
            raise ValueError("previous active batch changed before active switch")

        if instrumentation is not None:
            instrumentation.validated_edge_rows = len(candidate.physical_rows)
            instrumentation.validated_business_rows = len(candidate.business_rows)
            instrumentation.validated_issue_rows = len(candidate.issue_rows)
            instrumentation.validated_program_rows = len(candidate.program_state_rows)

    @staticmethod
    def _call_stage_hook(stage_hook: Callable[[str], Any] | None, stage: str) -> None:
        if stage_hook is not None:
            stage_hook(stage)

    def _active_switch(
        self,
        connection: Any,
        candidate: _DWSPreparedCandidate,
    ) -> None:
        observed = _timestamp_param(candidate.batch.observed_at, "observed_at")
        with self._cursor_scope(connection) as cursor:
            self._execute(cursor, DEACTIVATE_PHYSICAL_SQL)
            self._execute(cursor, DEACTIVATE_BUSINESS_SQL)
            self._execute(cursor, DEACTIVATE_ISSUE_SQL)
            self._execute(cursor, DEACTIVATE_STATE_SQL)
            self._execute(cursor, RETIRE_BATCH_SQL, (observed,))
            self._execute(
                cursor,
                ACTIVATE_BATCH_SQL,
                (observed, observed, candidate.batch.batch_id),
            )
            self._execute(cursor, ACTIVATE_PHYSICAL_SQL, (candidate.batch.batch_id,))
            self._execute(cursor, ACTIVATE_BUSINESS_SQL, (candidate.batch.batch_id,))
            self._execute(cursor, ACTIVATE_ISSUE_SQL, (candidate.batch.batch_id,))
            self._execute(cursor, ACTIVATE_STATE_SQL, (candidate.batch.batch_id,))
        active_batch = self._fetch_active_batch_row(connection)
        if (
            active_batch is None
            or active_batch.batch_id != candidate.batch.batch_id
            or active_batch.publish_status != "PUBLISHED"
        ):
            raise ValueError("active batch switch did not publish the candidate")
        expected_active_counts = {
            "lineage_program_state": len(candidate.program_state_rows),
            "lineage_edge": len(candidate.physical_rows),
            "lineage_business_edge": len(candidate.business_rows),
            "lineage_issue": sum(
                row.disposition != IssueDisposition.RESOLVED.value
                for row in candidate.issue_rows
            ),
        }
        actual_active_counts = {
            table_name: self._count_active(connection, table_name)
            for table_name in expected_active_counts
        }
        if actual_active_counts != expected_active_counts:
            raise ValueError(
                "active projection counts do not match candidate: "
                f"actual={actual_active_counts!r} expected={expected_active_counts!r}"
            )

    def publish(
        self,
        batch: MaterializationBatch,
        *,
        physical_dags: Iterable[ProgramPhysicalDAG] = (),
        complete_snapshot: bool = False,
        snapshot_scopes: Iterable[
            SnapshotScope
            | ProgramIdentity
            | ProgramSource
            | ProgramState
            | tuple[str, str]
        ]
        | None = None,
        snapshot_mode: str | None = None,
        stage_hook: Callable[[str], Any] | None = None,
        instrumentation: DWSPublishMetrics | None = None,
    ) -> DWSPublishResult:
        """Publish one candidate atomically through the DWS logical snapshot gate."""

        if not isinstance(batch, MaterializationBatch):
            raise TypeError("batch must be a MaterializationBatch")
        _key_text(batch.batch_id, "batch_id")
        if instrumentation is not None and not isinstance(
            instrumentation, DWSPublishMetrics
        ):
            raise TypeError("instrumentation must be DWSPublishMetrics or None")
        if any(
            item.batch_id is not None and item.batch_id != batch.batch_id
            for item in (*batch.edges, *batch.issues, *batch.program_states)
        ):
            raise ValueError(
                "candidate fact batch_id does not match MaterializationBatch"
            )
        pipeline_version = _pipeline_version_for_batch(batch)
        with self._connection_scope() as connection:
            restore_autocommit = _begin_transaction(connection)
            try:
                previous_row = self._fetch_active_batch_row(connection)
                previous_batch_id = (
                    None if previous_row is None else previous_row.batch_id
                )
                previous_state_rows = self._fetch_program_states(
                    connection, active_only=True
                )
                previous_physical = self._fetch_physical_rows(
                    connection, active_only=True
                )
                previous_business = self._fetch_business_rows(
                    connection, active_only=True
                )
                previous_issues = self._fetch_issue_rows(connection, active_only=True)
                visible_active_counts = {
                    "lineage_program_state": len(previous_state_rows),
                    "lineage_edge": len(previous_physical),
                    "lineage_business_edge": len(previous_business),
                    "lineage_issue": len(previous_issues),
                }
                stored_active_counts = {
                    table_name: self._count_active(connection, table_name)
                    for table_name in visible_active_counts
                }
                if stored_active_counts != visible_active_counts:
                    raise ValueError(
                        "active projections are not attached to the active batch: "
                        f"stored={stored_active_counts!r} "
                        f"visible={visible_active_counts!r}"
                    )
                previous_issue_values = tuple(
                    _issue_to_issue(row) for row in previous_issues
                )
                reconciled = reconcile_issue_lifecycle(
                    previous_issue_values,
                    batch.issues,
                    observed_at=batch.observed_at,
                )
                reconciled_issues = tuple(
                    replace(issue, batch_id=batch.batch_id)
                    for issue in (
                        *reconciled.current_issues,
                        *(record.issue for record in reconciled.resolved),
                    )
                )
                prepared_batch = replace(batch, issues=reconciled_issues)
                prepare_started = (
                    perf_counter() if instrumentation is not None else None
                )
                candidate = self._prepare_candidate(
                    prepared_batch,
                    physical_dags=physical_dags,
                    previous_batch_id=previous_batch_id,
                    previous_physical=previous_physical,
                    previous_business=previous_business,
                    previous_issues=previous_issues,
                    complete_snapshot=complete_snapshot,
                    snapshot_scopes=snapshot_scopes,
                    snapshot_mode=snapshot_mode,
                    pipeline_version=pipeline_version,
                    instrumentation=instrumentation,
                )
                if instrumentation is not None and prepare_started is not None:
                    instrumentation.prepare_ms = int(
                        (perf_counter() - prepare_started) * 1000
                    )
                insert_started = perf_counter() if instrumentation is not None else None
                self._insert_candidate(connection, candidate)
                if instrumentation is not None and insert_started is not None:
                    instrumentation.insert_ms = int(
                        (perf_counter() - insert_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_candidate_insert")
                validate_started = (
                    perf_counter() if instrumentation is not None else None
                )
                self._validate_candidate_in_transaction(
                    connection,
                    candidate,
                    instrumentation,
                )
                if instrumentation is not None and validate_started is not None:
                    instrumentation.validate_ms = int(
                        (perf_counter() - validate_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_validate")
                switch_started = perf_counter() if instrumentation is not None else None
                self._active_switch(connection, candidate)
                if instrumentation is not None and switch_started is not None:
                    instrumentation.active_switch_ms = int(
                        (perf_counter() - switch_started) * 1000
                    )
                self._call_stage_hook(stage_hook, "after_active_switch")
                commit_started = perf_counter() if instrumentation is not None else None
                _commit(connection)
                if instrumentation is not None and commit_started is not None:
                    instrumentation.commit_ms = int(
                        (perf_counter() - commit_started) * 1000
                    )
            except Exception:
                _rollback(connection)
                raise
            finally:
                restore_autocommit()

        return DWSPublishResult(
            batch_id=candidate.batch.batch_id,
            edge_count=len(candidate.physical_rows),
            issue_count=len(candidate.issue_rows),
            previous_batch_id=candidate.previous_batch_id,
            program_count=len(candidate.program_state_rows),
            business_edge_count=len(candidate.business_rows),
        )

    publish_batch = publish

    def validate_candidate(self, batch_id: str) -> None:
        """Validate an already inserted inactive candidate without activating it."""

        batch_id = _required_text(batch_id, "batch_id")
        with self._connection_scope() as connection:
            row = self._fetch_batch_row(connection, batch_id)
            if row is None:
                raise ValueError("candidate batch does not exist")
            if row.is_active:
                raise ValueError("candidate batch must be inactive")
            business_rows = self._fetch_business_rows(connection, batch_id=batch_id)
            issues = self._fetch_issue_rows(connection, batch_id=batch_id)
            raw_state_rows = self._fetch_program_state_rows(
                connection,
                batch_id=batch_id,
            )
            states = self._fetch_program_states(connection, batch_id=batch_id)
            physical_rows = self._fetch_physical_rows(connection, batch_id=batch_id)
            batch = MaterializationBatch(
                batch_id=batch_id,
                observed_at=row.observed_at,
                edges=tuple(_business_to_edge(item) for item in business_rows),
                issues=tuple(_issue_to_issue(item) for item in issues),
                program_states=states,
            )
            candidate = _DWSPreparedCandidate(
                batch=batch,
                previous_batch_id=row.previous_batch_id,
                batch_row=row,
                physical_rows=physical_rows,
                business_rows=business_rows,
                issue_rows=issues,
                program_state_rows=tuple(
                    (
                        _stored_required_text(raw[0], "row_key"),
                        _stored_required_text(raw[1], "program_key"),
                        item.environment,
                        item.source_profile,
                        item.program_name,
                        item.source_hash,
                        item.pipeline_version,
                        item.batch_id,
                        _timestamp_param(item.first_seen_at, "first_seen_at"),
                        _timestamp_param(item.last_seen_at, "last_seen_at"),
                        _timestamp_param(item.last_changed_at, "last_changed_at"),
                        item.is_active,
                        _timestamp_param(
                            row.created_at or row.observed_at, "created_at"
                        ),
                        _timestamp_param(
                            row.updated_at or row.observed_at, "updated_at"
                        ),
                    )
                    for raw, item in zip(raw_state_rows, states)
                ),
            )
            self._validate_candidate_in_transaction(connection, candidate)

    def get_active_batch_id(self) -> str | None:
        with self._connection_scope() as connection:
            row = self._fetch_active_batch_row(connection)
        return None if row is None else row.batch_id

    def list_batch_metadata(self) -> tuple[BatchMetadata, ...]:
        with self._connection_scope() as connection:
            with self._cursor_scope(connection) as cursor:
                self._execute(
                    cursor,
                    BATCH_SELECT_SQL + " ORDER BY observed_at, batch_id",
                )
                rows = cursor.fetchall()
        values = []
        for raw in rows:
            row = _batch_from_row(raw)
            values.append(
                BatchMetadata(
                    batch_id=row.batch_id,
                    observed_at=row.observed_at,
                    published_at=row.published_at,
                    edge_count=row.edge_count,
                    issue_count=row.issue_count,
                    program_count=row.program_count,
                    is_active=row.is_active,
                )
            )
        return tuple(values)

    list_batches = list_batch_metadata

    def get_batch_metadata(self, batch_id: str) -> BatchMetadata | None:
        batch_id = _required_text(batch_id, "batch_id")
        with self._connection_scope() as connection:
            row = self._fetch_batch_row(connection, batch_id)
        if row is None:
            return None
        return BatchMetadata(
            batch_id=row.batch_id,
            observed_at=row.observed_at,
            published_at=row.published_at,
            edge_count=row.edge_count,
            issue_count=row.issue_count,
            program_count=row.program_count,
            is_active=row.is_active,
        )

    def read_program_states(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[ProgramState, ...]:
        with self._connection_scope() as connection:
            return self._fetch_program_states(
                connection,
                batch_id=batch_id,
                active_only=active_only,
            )

    def read_physical_edges(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[DWSPhysicalEdgeRow, ...]:
        with self._connection_scope() as connection:
            return self._fetch_physical_rows(
                connection,
                batch_id=batch_id,
                active_only=active_only,
            )

    def read_edges(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[LineageEdge, ...]:
        with self._connection_scope() as connection:
            rows = self._fetch_business_rows(
                connection,
                batch_id=batch_id,
                active_only=active_only,
            )
        return tuple(_business_to_edge(row) for row in rows)

    read_business_edges = read_edges

    def read_issues(
        self,
        *,
        batch_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[LineageIssue, ...]:
        with self._connection_scope() as connection:
            rows = self._fetch_issue_rows(
                connection,
                batch_id=batch_id,
                active_only=active_only,
            )
        return tuple(_issue_to_issue(row) for row in rows)

    def _read_neighbor_edges(
        self,
        *,
        environment: str,
        table: str,
        source_profile: str | None,
        outgoing: bool,
    ) -> tuple[LineageEdge, ...]:
        environment = _required_text(environment, "environment")
        table = _required_text(table, "table")
        source_profile = (
            None
            if source_profile is None
            else _required_text(source_profile, "source_profile")
        )
        if DatasetIdentity.from_name(environment, table) is None:
            raise ValueError("table must be a qualified formal schema.table")
        table_condition = "e.source_table = ?" if outgoing else "e.target_table = ?"
        conditions = [
            "e.is_active = TRUE",
            "e.environment = ?",
            table_condition,
        ]
        params: list[object] = [environment, table.strip().upper()]
        if source_profile is not None:
            conditions.append("e.source_profile = ?")
            params.append(source_profile)
        where = " WHERE " + " AND ".join(conditions)
        with self._connection_scope() as connection:
            rows = self._fetch_rows(
                connection,
                BUSINESS_EDGE_SELECT_SQL,
                ACTIVE_BATCH_JOIN,
                where,
                tuple(params),
                " ORDER BY e.environment, e.source_profile, e.source_table, "
                "e.target_table, e.business_edge_key",
                _business_from_row,
            )
        return tuple(_business_to_edge(row) for row in rows)

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        return self._read_neighbor_edges(
            environment=environment,
            table=source_table,
            source_profile=source_profile,
            outgoing=True,
        )

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        return self._read_neighbor_edges(
            environment=environment,
            table=target_table,
            source_profile=source_profile,
            outgoing=False,
        )


DWSMaterializationWriter = DWSMaterializationStore


def _close_quietly(resource: Any) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        return


def _begin_transaction(connection: Any) -> Callable[[], None]:
    """Disable autocommit when the DB-API/JDBC wrapper exposes that boundary."""

    jconn = getattr(connection, "jconn", None)
    old_value: object = None
    restore_value_available = False

    def restore_noop() -> None:
        return

    restore: Callable[[], None] = restore_noop
    get_auto_commit = getattr(jconn, "getAutoCommit", None)
    set_auto_commit = getattr(jconn, "setAutoCommit", None)
    if jconn is not None and callable(get_auto_commit):
        if not callable(set_auto_commit):
            raise RuntimeError("JDBC connection does not expose setAutoCommit")
        try:
            old_value = bool(get_auto_commit())
            restore_value_available = True
        except Exception as exc:
            raise RuntimeError("failed to read JDBC autocommit") from exc
        try:
            set_auto_commit(False)
        except Exception as exc:
            raise RuntimeError("failed to disable JDBC autocommit") from exc

        def restore_jdbc() -> None:
            if restore_value_available:
                try:
                    set_auto_commit(old_value)
                except Exception as exc:
                    raise RuntimeError("failed to restore JDBC autocommit") from exc

        restore = restore_jdbc
    elif hasattr(connection, "autocommit"):
        try:
            old_value = connection.autocommit
            restore_value_available = True
            connection.autocommit = False
        except Exception as exc:
            raise RuntimeError("failed to disable DB-API autocommit") from exc

        def restore_dbapi() -> None:
            if restore_value_available:
                try:
                    connection.autocommit = old_value
                except Exception as exc:
                    raise RuntimeError("failed to restore DB-API autocommit") from exc

        restore = restore_dbapi
    else:
        begin = getattr(connection, "begin", None)
        if callable(begin):
            try:
                begin()
            except Exception as exc:
                raise RuntimeError("failed to begin DWS transaction") from exc
        else:
            cursor = connection.cursor()
            try:
                # The fallback is only used when the DB-API wrapper exposes no
                # autocommit control; explicit BEGIN preserves atomic publish.
                cursor.execute("BEGIN")
            except Exception as exc:
                raise RuntimeError("failed to begin DWS transaction") from exc
            finally:
                _close_quietly(cursor)
    return restore


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if callable(commit):
        commit()
        return
    jconn = getattr(connection, "jconn", None)
    if jconn is not None and callable(getattr(jconn, "commit", None)):
        jconn.commit()
        return
    raise RuntimeError("DWS connection does not expose commit")


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()
        return
    jconn = getattr(connection, "jconn", None)
    if jconn is not None and callable(getattr(jconn, "rollback", None)):
        jconn.rollback()
        return
    raise RuntimeError("DWS connection does not expose rollback")


__all__ = [
    "DWSBusinessEdgeRow",
    "DWSIssueRow",
    "DWSMaterializationStore",
    "DWSMaterializationWriter",
    "DWSPublishMetrics",
    "DWSPublishResult",
    "DWSPhysicalEdgeRow",
    "DWS_SCHEMA",
    "DWS_TABLES",
    "business_edge_key",
    "dataset_key",
    "issue_stable_key",
    "physical_edge_key",
    "program_key",
]
