"""Phase 7 history/diff API compatibility module.

The storage adapter remains :class:`SQLiteMaterializationStore`; this module
only exposes the pure lifecycle and business-graph projections.
"""

from .evolution import (  # pyright: ignore[reportMissingImports]
    BatchMetadata,
    BusinessLineageEdge,
    EnvironmentLineageDiff,
    IssueLifecycle,
    IssueLifecycleResult,
    IssueLifecycleStatus,
    LineageBatchDiff,
    detect_broken_lineage_branches,
    diff_environments,
    diff_lineage_batches,
    issue_identity_key,
    reconcile_issue_lifecycle,
)

__all__ = [
    "BatchMetadata",
    "BusinessLineageEdge",
    "EnvironmentLineageDiff",
    "IssueLifecycle",
    "IssueLifecycleResult",
    "IssueLifecycleStatus",
    "LineageBatchDiff",
    "detect_broken_lineage_branches",
    "diff_environments",
    "diff_lineage_batches",
    "issue_identity_key",
    "reconcile_issue_lifecycle",
]
