"""Phase 7 incremental API compatibility module.

Implementation lives in :mod:`shared.lineage.evolution`; this narrow facade
keeps the public concept discoverable without creating a second planner.
"""

from .domain import ProgramIdentity, ProgramSource, ProgramState
from .evolution import (  # pyright: ignore[reportMissingImports]
    IncrementalPlan,
    IncrementalStatus,
    SnapshotScope,
    build_program_states,
    plan_incremental,
    program_identity_key,
)

__all__ = [
    "IncrementalPlan",
    "IncrementalStatus",
    "ProgramIdentity",
    "ProgramSource",
    "ProgramState",
    "SnapshotScope",
    "build_program_states",
    "plan_incremental",
    "program_identity_key",
]
