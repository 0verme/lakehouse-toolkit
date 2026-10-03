from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class RecvDwfRecord:
    recv_plan: str
    table_name: str
    data_source: str
    recv_job_name: str = ""
    ods_job_name: str = ""


@dataclass(frozen=True, slots=True)
class SchemaConfigRecord:
    schema_key: str
    db_schema: str


@dataclass(frozen=True, slots=True)
class DapUpstreamSystem:
    identity: str
    upstream_system_id: int


@dataclass(frozen=True, slots=True)
class DwoSource:
    """Resolved DWO physical relation.

    ``recv_namespace`` is the DWO landing namespace derived from the full
    ``recv_plan`` + ``data_source`` pair. ``db_schema`` is source-database schema
    metadata taken from ``p_schema_config`` for the selected ``data_source`` and
    is ``None`` when that metadata is absent or ambiguous. It never carries the
    DWO namespace prefix and never participates in upstream identity.
    """

    physical_table: str
    recv_namespace: str
    source_table: str
    db_schema: str | None = None


@dataclass(frozen=True, slots=True)
class MappingField:
    source_field: str
    target_field: str
    mapping_rule: str
    field_order: int
    physical_source_table: str
    db_schema: str | None
    program: str
    evidence: tuple[str, ...] = ()

    def to_contract(self) -> dict[str, object]:
        return {
            "sourceField": self.source_field,
            "targetField": self.target_field,
            "mappingRule": self.mapping_rule,
            "fieldOrder": self.field_order,
        }


@dataclass(frozen=True, slots=True)
class MappingItem:
    source_system_identity: str
    source_system_id: int
    source_table: str
    target_table: str
    fields: tuple[MappingField, ...]

    @property
    def identity(self) -> tuple[str, str, str]:
        """Stable business identity; never includes an instance-local DAP primary key."""
        return (
            self.source_system_identity.casefold(),
            self.source_table.casefold(),
            self.target_table.casefold(),
        )

    @property
    def dap_identity(self) -> tuple[int, str, str]:
        return (
            self.source_system_id,
            self.source_table.casefold(),
            self.target_table.casefold(),
        )

    def to_contract(self) -> dict[str, object]:
        return {
            "sourceSystemId": self.source_system_id,
            "sourceTable": self.source_table,
            "targetLayer": "DWF",
            "targetTable": self.target_table,
            "fields": [field.to_contract() for field in self.fields],
        }


@dataclass(slots=True)
class AuditResult:
    items: tuple[MappingItem, ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)
    resolved: list[dict[str, Any]] = field(default_factory=list)
    unresolved: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Resolution:
    status: str
    reason: str | None = None
    record: RecvDwfRecord | None = None
    source: DwoSource | None = None
    upstream_system_id: int | None = None
    evidence: tuple[str, ...] = ()


__all__ = [
    "AuditResult",
    "DapUpstreamSystem",
    "DwoSource",
    "MappingField",
    "MappingItem",
    "RecvDwfRecord",
    "Resolution",
    "SchemaConfigRecord",
]
