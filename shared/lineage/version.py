"""Explicit semantic versions for the lineage materialization pipeline."""

# Bump this value whenever parser, Physical DAG, audit, or materialization
# semantics change in a way that makes previously persisted program facts stale.
# This is intentionally maintained in source code and is not derived from Git.
# v13 excludes statically empty SELECT Query Blocks from persisted Physical/Business
# lineage while preserving their sanitized Audit Issue facts.
LINEAGE_PIPELINE_VERSION = "lineage-pipeline-v13-static-empty-query"

__all__ = ["LINEAGE_PIPELINE_VERSION"]
