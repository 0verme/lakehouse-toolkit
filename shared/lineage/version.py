"""Explicit semantic versions for the lineage materialization pipeline."""

# Bump this value whenever parser, Physical DAG, audit, or materialization
# semantics change in a way that makes previously persisted program facts stale.
# This is intentionally maintained in source code and is not derived from Git.
# v13 excludes statically empty SELECT Query Blocks from persisted Physical/Business
# lineage while preserving their sanitized Audit Issue facts.
# v14 collapses program-local UNCLASSIFIED_FORMAL intermediates that are proven
# by Physical DAG degree (in_degree > 0 and out_degree > 0), and adds explicit
# audit blockers for unclassified formal source/sink boundaries.
# v15 keeps statically proven SQL object identifiers from ``.replace()`` /
# ``.format()`` / f-string templates while replacing dynamic values with an
# opaque literal placeholder; dynamic identifier contexts still fail closed.
LINEAGE_PIPELINE_VERSION = "lineage-pipeline-v15-dynamic-literal-template"

__all__ = ["LINEAGE_PIPELINE_VERSION"]
