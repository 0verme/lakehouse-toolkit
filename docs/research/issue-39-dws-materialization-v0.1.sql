-- Issue #39 DWS Materialization Schema v0.3 / writer contract
--
-- EXECUTABLE SMOKE SCHEMA
--
-- This public contract is based on the completed DWS reconnaissance.
-- Goal: keep the first production schema simple enough to CREATE / INSERT / SELECT reliably.
--
-- Intentionally omitted in v0.3 smoke schema:
--   - PRIMARY KEY
--   - UNIQUE constraints / unique indexes
--   - CHECK constraints
--   - PARTITION definitions
--   - partial indexes
--   - foreign keys
--
-- These constraints are deferred until runtime writer semantics and real DWS behavior
-- have been validated with actual lineage data.
--
-- Target:
--   Engine: GaussDB / DWS
--   Database: supplied by deployment configuration (no real database name in public DDL)
--   Schema: dwp
--
-- Notes:
--   1. All object references are schema-qualified.
--   2. lineage_edge stores physical direct lineage and may contain TMP endpoints.
--   3. lineage_business_edge stores TMP-collapsed formal business lineage.
--   4. lineage_closure is NOT created here; it belongs to Issue #40.
--   5. Uniqueness and lifecycle correctness are enforced by the writer in this phase.

-- ============================================================================
-- 0. Batch control
-- ============================================================================

CREATE TABLE dwp.lineage_batch (
    batch_id             VARCHAR(128) NOT NULL,
    snapshot_mode        VARCHAR(16) NOT NULL,
    complete_snapshot    BOOLEAN NOT NULL,
    snapshot_scope       TEXT,
    pipeline_version     VARCHAR(256),

    observed_at          TIMESTAMP(6) WITH TIME ZONE,
    previous_batch_id    VARCHAR(128),

    publish_status       VARCHAR(16),
    published_at         TIMESTAMP(6) WITH TIME ZONE,

    program_count        BIGINT DEFAULT 0,
    edge_count           BIGINT DEFAULT 0,
    issue_count          BIGINT DEFAULT 0,

    is_active            BOOLEAN DEFAULT FALSE,

    created_at           TIMESTAMP(6) WITH TIME ZONE,
    updated_at           TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH(batch_id);


-- ============================================================================
-- Issue #85 extension: configured schedule lineage
-- ============================================================================
--
-- This table intentionally does not use lineage_batch as its active control:
-- lineage_batch has one global SQL-lineage active boundary, while schedule
-- ingestion is an independently publishable source. The schedule writer still
-- reuses the #84 DWS connection/transaction boundary and keeps its own
-- batch_id/is_active history in this single fact table.

CREATE TABLE dwp.lineage_schedule_edge (
    row_key              VARCHAR(128) NOT NULL,
    schedule_edge_key    VARCHAR(128) NOT NULL,

    environment          VARCHAR(128) NOT NULL,
    source_profile       VARCHAR(256) NOT NULL,

    process_name         VARCHAR(512) NOT NULL,
    project_version_key  VARCHAR(256) NOT NULL,

    raw_source_table     VARCHAR(512) NOT NULL,
    raw_target_table     VARCHAR(512) NOT NULL,

    source_table         VARCHAR(512) NOT NULL,
    target_table         VARCHAR(512) NOT NULL,

    batch_id             VARCHAR(128) NOT NULL,
    observed_at          TIMESTAMP(6) WITH TIME ZONE,

    first_seen_at        TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at         TIMESTAMP(6) WITH TIME ZONE,
    last_changed_at      TIMESTAMP(6) WITH TIME ZONE,

    is_active            BOOLEAN DEFAULT FALSE,

    created_at           TIMESTAMP(6) WITH TIME ZONE,
    updated_at           TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = COLUMN)
DISTRIBUTE BY HASH(schedule_edge_key);


-- ============================================================================
-- Issue #111 extension: reconciliation suppression audit
-- ============================================================================
--
-- This is a presentation-classification audit projection.  It never replaces
-- the raw MATCH / SQL_ONLY / SCHEDULE_ONLY reconciliation fact and is written
-- by the explicit suppression materialization boundary, not by a UI renderer.
-- Stable suppression_key excludes both batch ids; row_key identifies one
-- materialized observation.  Lifecycle correctness is enforced by the writer.

CREATE TABLE dwp.lineage_reconciliation_suppression (
    row_key                  VARCHAR(128) NOT NULL,
    suppression_key          VARCHAR(128) NOT NULL,

    environment              VARCHAR(128) NOT NULL,
    sql_source_profile       VARCHAR(256) NOT NULL,
    schedule_source_profile  VARCHAR(256) NOT NULL,

    source_table             VARCHAR(512) NOT NULL,
    target_table             VARCHAR(512) NOT NULL,

    raw_status               VARCHAR(32) NOT NULL,
    suppression_reason       VARCHAR(64) NOT NULL,

    sql_batch_id             VARCHAR(128) NOT NULL,
    schedule_batch_id        VARCHAR(128) NOT NULL,
    classifier_version       VARCHAR(128) NOT NULL,

    observed_at              TIMESTAMP(6) WITH TIME ZONE,

    first_seen_at            TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at             TIMESTAMP(6) WITH TIME ZONE,

    is_active                BOOLEAN DEFAULT FALSE,

    created_at               TIMESTAMP(6) WITH TIME ZONE,
    updated_at               TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH(suppression_key);


-- ============================================================================
-- 1. Program state
-- ============================================================================

CREATE TABLE dwp.lineage_program_state (
    row_key             VARCHAR(128) NOT NULL,
    program_key         VARCHAR(128) NOT NULL,

    environment         VARCHAR(128) NOT NULL,
    source_profile      VARCHAR(256) NOT NULL,
    program_name        VARCHAR(512) NOT NULL,

    source_hash         VARCHAR(128),
    pipeline_version    VARCHAR(256),

    batch_id            VARCHAR(128) NOT NULL,

    first_seen_at       TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at        TIMESTAMP(6) WITH TIME ZONE,
    last_changed_at     TIMESTAMP(6) WITH TIME ZONE,

    is_active           BOOLEAN DEFAULT FALSE,

    created_at          TIMESTAMP(6) WITH TIME ZONE,
    updated_at          TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH(program_key);


-- ============================================================================
-- 2. Physical direct lineage
-- ============================================================================
--
-- One row represents one direct edge observed inside a program DAG.
--
-- Examples:
--   DWF.A -> TMP_A
--   TMP_A -> TMP_B
--   TMP_B -> DWM.RESULT_A
--
-- TMP / unresolved physical nodes may have NULL dataset_key values.

CREATE TABLE dwp.lineage_edge (
    row_key                 VARCHAR(128) NOT NULL,
    edge_key                VARCHAR(128) NOT NULL,

    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,

    program_key             VARCHAR(128) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,

    source_table            VARCHAR(512) NOT NULL,
    target_table            VARCHAR(512) NOT NULL,

    -- Values: formal_asset / temporary_asset; TMP rows keep NULL dataset keys.
    source_node_kind        VARCHAR(32),
    target_node_kind        VARCHAR(32),

    source_dataset_key      VARCHAR(128),
    target_dataset_key      VARCHAR(128),

    evidence_type           VARCHAR(128),
    evidence_json           TEXT,

    source_hash             VARCHAR(128),
    pipeline_version        VARCHAR(256),

    batch_id                VARCHAR(128) NOT NULL,
    observed_at             TIMESTAMP(6) WITH TIME ZONE,

    first_seen_at           TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,

    is_active               BOOLEAN DEFAULT FALSE,

    created_at              TIMESTAMP(6) WITH TIME ZONE,
    updated_at              TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = COLUMN)
DISTRIBUTE BY HASH(edge_key);


-- ============================================================================
-- 3. Collapsed business lineage
-- ============================================================================
--
-- Derived from physical lineage by collapsing safe TMP paths.
--
-- Example:
--   Physical:
--     DWF.A -> TMP_A -> TMP_B -> DWM.RESULT_A
--
--   Business:
--     DWF.A -> DWM.RESULT_A
--     collapse_depth = 3
--
-- TMP must not be exposed as a business endpoint.

CREATE TABLE dwp.lineage_business_edge (
    row_key                  VARCHAR(128) NOT NULL,
    business_edge_key        VARCHAR(128) NOT NULL,

    environment              VARCHAR(128) NOT NULL,
    source_profile           VARCHAR(256) NOT NULL,

    program_key              VARCHAR(128) NOT NULL,
    program_name             VARCHAR(512) NOT NULL,

    source_dataset_key       VARCHAR(128) NOT NULL,
    source_table             VARCHAR(512) NOT NULL,

    target_dataset_key       VARCHAR(128) NOT NULL,
    target_table             VARCHAR(512) NOT NULL,

    collapse_depth           INTEGER NOT NULL,
    physical_derivation_hash VARCHAR(128),

    source_hash              VARCHAR(128),
    pipeline_version         VARCHAR(256),

    batch_id                 VARCHAR(128) NOT NULL,
    observed_at              TIMESTAMP(6) WITH TIME ZONE,

    first_seen_at            TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at             TIMESTAMP(6) WITH TIME ZONE,
    last_changed_at          TIMESTAMP(6) WITH TIME ZONE,

    is_active                BOOLEAN DEFAULT FALSE,

    created_at               TIMESTAMP(6) WITH TIME ZONE,
    updated_at               TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = COLUMN)
DISTRIBUTE BY HASH(business_edge_key);


-- ============================================================================
-- 4. Audit issues
-- ============================================================================

CREATE TABLE dwp.lineage_issue (
    row_key                 VARCHAR(128) NOT NULL,
    stable_issue_key        VARCHAR(128) NOT NULL,

    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,

    program_key             VARCHAR(128) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,

    issue_type              VARCHAR(128) NOT NULL,

    -- Confidence values follow Issue #36: HIGH / MEDIUM / LOW / UNKNOWN.
    confidence              VARCHAR(16) NOT NULL,
    rule_version            VARCHAR(256) NOT NULL,

    -- Severity is policy-defined; disposition values follow Issue #36:
    -- OPEN / ACCEPTED / FALSE_POSITIVE / RESOLVED.
    severity                VARCHAR(32) NOT NULL,
    disposition             VARCHAR(32) NOT NULL,
    policy_version          VARCHAR(256) NOT NULL,

    node_key                VARCHAR(512),
    branch_sink             VARCHAR(512),

    message                 TEXT,
    evidence_json           TEXT,

    batch_id                VARCHAR(128) NOT NULL,

    first_seen_at           TIMESTAMP(6) WITH TIME ZONE,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,

    disposition_updated_at  TIMESTAMP(6) WITH TIME ZONE,
    disposition_updated_by  VARCHAR(256),

    is_active               BOOLEAN DEFAULT FALSE,

    created_at              TIMESTAMP(6) WITH TIME ZONE,
    updated_at              TIMESTAMP(6) WITH TIME ZONE
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH(stable_issue_key);


-- ============================================================================
-- Deferred to later phases
-- ============================================================================
--
-- Issue #40:
--   dwp.lineage_closure
--
-- Future optimization after real workload evidence:
--   - primary / unique constraints
--   - indexes
--   - range partitions
--   - retention policy
--   - active-batch acceleration
--   - distribution tuning
--
-- Current priority:
--   CREATE -> INSERT -> SELECT -> publish lifecycle -> real workload benchmark
