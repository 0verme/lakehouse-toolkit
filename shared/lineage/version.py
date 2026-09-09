"""Explicit semantic versions for the lineage materialization pipeline."""

# Bump this value whenever parser, Physical DAG, audit, or materialization
# semantics change in a way that makes previously persisted program facts stale.
# This is intentionally maintained in source code and is not derived from Git.
LINEAGE_PIPELINE_VERSION = (
    "lineage-pipeline-v6-program-namespace-and-audit-target-mismatch-semantics"
)

__all__ = ["LINEAGE_PIPELINE_VERSION"]
