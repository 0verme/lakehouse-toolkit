"""Explicit semantic versions for the lineage materialization pipeline."""

# Bump this value whenever parser, Physical DAG, audit, or materialization
# semantics change in a way that makes previously persisted program facts stale.
# This is intentionally maintained in source code and is not derived from Git.
# v12 binds exact same-name unqualified SQL write targets to qualified authority,
# changing persisted Physical/Business target identities even when source_hash is unchanged.
LINEAGE_PIPELINE_VERSION = "lineage-pipeline-v12-authoritative-target-binding"

__all__ = ["LINEAGE_PIPELINE_VERSION"]
