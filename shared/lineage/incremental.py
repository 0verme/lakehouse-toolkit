"""Phase 7 incremental API compatibility module.

Implementation lives in :mod:`shared.lineage.evolution`; this narrow facade
keeps the public concept discoverable without creating a second planner.
"""

from .domain import ProgramIdentity, ProgramSource, ProgramState
from .evolution import (  # pyright: ignore[reportMissingImports]
    LINEAGE_PIPELINE_VERSION,
    IncrementalPlan,
    IncrementalStatus,
    PipelineVersionMigrationRequired,
    SnapshotScope,
    build_program_states,
    plan_incremental,
    program_identity_key,
    validate_pipeline_version_migration,
)

__all__ = [
    "IncrementalPlan",
    "IncrementalStatus",
    "LINEAGE_PIPELINE_VERSION",
    "PipelineVersionMigrationRequired",
    "ProgramIdentity",
    "ProgramSource",
    "ProgramState",
    "SnapshotScope",
    "build_program_states",
    "plan_incremental",
    "program_identity_key",
    "validate_pipeline_version_migration",
]
