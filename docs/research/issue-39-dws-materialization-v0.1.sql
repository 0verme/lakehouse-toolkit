-- Issue #39 DWS Materialization Schema v0.1 DESIGN DRAFT
--
-- DO NOT EXECUTE IN PRODUCTION.
-- This file is a contract/design artifact only. It contains no migration,
-- connection, credential, or production execution logic.
--
-- Target evidence: GaussDB 8.1.3 / PostgreSQL 9.2.4 compatible, UTF8,
-- database czcb, schema dwp. Every production object reference is explicit:
-- no search_path and no dependency on current_schema (which is public).
--
-- Key generation, candidate validation, active switching, retention and
-- partition-boundary provisioning are writer/deployment responsibilities.
-- The DDL intentionally keeps the five tables independent of any dataset
-- registry. Business endpoints must be validated against DatasetIdentity before
-- insertion; SQL name-prefix checks are not a substitute for that validation.

-- ---------------------------------------------------------------------------
-- 0. Control table: one snapshot consistency boundary
-- ---------------------------------------------------------------------------
CREATE TABLE dwp.lineage_batch (
    row_key                 VARCHAR(128) NOT NULL,
    batch_id                VARCHAR(128) NOT NULL,
    snapshot_mode           VARCHAR(16) NOT NULL,
    complete_snapshot       BOOLEAN NOT NULL,
    snapshot_scope          TEXT NOT NULL,
    pipeline_version        VARCHAR(256) NOT NULL,
    observed_at             TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    previous_batch_id       VARCHAR(128),
    publish_status           VARCHAR(16) NOT NULL DEFAULT 'CANDIDATE',
    published_at            TIMESTAMP(6) WITH TIME ZONE,
    program_count           BIGINT NOT NULL DEFAULT 0,
    physical_edge_count     BIGINT NOT NULL DEFAULT 0,
    business_edge_count     BIGINT NOT NULL DEFAULT 0,
    issue_count             BIGINT NOT NULL DEFAULT 0,
    is_active               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_lineage_batch PRIMARY KEY (row_key),
    CONSTRAINT uq_lineage_batch_batch_id UNIQUE (batch_id),
    CONSTRAINT ck_lineage_batch_snapshot_mode
        CHECK (snapshot_mode IN ('FULL', 'PARTIAL')),
    CONSTRAINT ck_lineage_batch_snapshot_completeness
        CHECK (
            (snapshot_mode = 'FULL' AND complete_snapshot = TRUE)
            OR (snapshot_mode = 'PARTIAL' AND complete_snapshot = FALSE)
        ),
    CONSTRAINT ck_lineage_batch_publish_status
        CHECK (publish_status IN ('CANDIDATE', 'PUBLISHED', 'RETIRED')),
    CONSTRAINT ck_lineage_batch_counts
        CHECK (
            program_count >= 0
            AND physical_edge_count >= 0
            AND business_edge_count >= 0
            AND issue_count >= 0
        )
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH (row_key);

CREATE UNIQUE INDEX dwp.uq_lineage_batch_active
    ON dwp.lineage_batch (is_active)
    WHERE is_active = TRUE;

COMMENT ON TABLE dwp.lineage_batch IS
    'Issue #39 design: atomic lineage snapshot control boundary; not a runtime run.';
COMMENT ON COLUMN dwp.lineage_batch.row_key IS
    'Physical row key; deterministic hash of table and batch identity.';
COMMENT ON COLUMN dwp.lineage_batch.batch_id IS
    'Stable identity of one candidate/published snapshot; not scheduler execution.';
COMMENT ON COLUMN dwp.lineage_batch.snapshot_scope IS
    'Canonical JSON text for environment/source_profile complete-snapshot scope.';
COMMENT ON COLUMN dwp.lineage_batch.business_edge_count IS
    'Count of lineage_business_edge rows in the same batch; must not be inferred from physical edges.';

-- ---------------------------------------------------------------------------
-- 1. Program state: #38 ProgramIdentity across materialization snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE dwp.lineage_program_state (
    row_key                 VARCHAR(128) NOT NULL,
    program_key             VARCHAR(128) NOT NULL,
    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,
    source_hash             VARCHAR(128),
    pipeline_version        VARCHAR(256),
    first_seen_at           TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,
    batch_id                VARCHAR(128) NOT NULL,
    is_active               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_lineage_program_state PRIMARY KEY (row_key),
    CONSTRAINT uq_lineage_program_state_batch_key
        UNIQUE (batch_id, program_key),
    CONSTRAINT ck_lineage_program_state_hash
        CHECK (source_hash IS NULL OR LENGTH(TRIM(source_hash)) > 0),
    CONSTRAINT ck_lineage_program_state_identity
        CHECK (
            LENGTH(TRIM(environment)) > 0
            AND LENGTH(TRIM(source_profile)) > 0
            AND LENGTH(TRIM(program_name)) > 0
            AND LENGTH(TRIM(program_key)) > 0
        )
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH (row_key)
PARTITION BY RANGE (last_seen_at)
(
    PARTITION p_lineage_program_state_seed
        VALUES LESS THAN (TIMESTAMP WITH TIME ZONE '2099-01-01 00:00:00+00'),
    PARTITION p_lineage_program_state_max
        VALUES LESS THAN (MAXVALUE)
);

CREATE UNIQUE INDEX dwp.uq_lineage_program_state_active_key
    ON dwp.lineage_program_state (environment, source_profile, program_key)
    WHERE is_active = TRUE;
CREATE INDEX dwp.ix_lineage_program_state_batch_active
    ON dwp.lineage_program_state (batch_id, is_active);
CREATE INDEX dwp.ix_lineage_program_state_scope
    ON dwp.lineage_program_state (environment, source_profile, is_active);

COMMENT ON TABLE dwp.lineage_program_state IS
    'Issue #38 static ProgramIdentity state; does not represent scheduler execution.';
COMMENT ON COLUMN dwp.lineage_program_state.program_key IS
    'Stable hash of environment/source_profile/program_name; not source_hash or batch_id.';
COMMENT ON COLUMN dwp.lineage_program_state.last_changed_at IS
    'Changes on source-content or pipeline semantic change; last_seen changes on every observation.';

-- ---------------------------------------------------------------------------
-- 2. Physical direct lineage: source of truth, TMP endpoints allowed
-- ---------------------------------------------------------------------------
CREATE TABLE dwp.lineage_edge (
    row_key                 VARCHAR(128) NOT NULL,
    edge_key                VARCHAR(128) NOT NULL,
    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,
    program_key             VARCHAR(128) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,
    source_table            VARCHAR(512) NOT NULL,
    target_table            VARCHAR(512) NOT NULL,
    source_node_kind        VARCHAR(32) NOT NULL,
    target_node_kind        VARCHAR(32) NOT NULL,
    source_dataset_key      VARCHAR(128),
    target_dataset_key      VARCHAR(128),
    evidence_type           VARCHAR(128) NOT NULL,
    evidence_json           TEXT,
    source_hash             VARCHAR(128),
    pipeline_version        VARCHAR(256),
    batch_id                VARCHAR(128) NOT NULL,
    observed_at             TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    first_seen_at           TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,
    is_active               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_lineage_edge PRIMARY KEY (row_key),
    CONSTRAINT uq_lineage_edge_batch_key
        UNIQUE (batch_id, edge_key),
    CONSTRAINT ck_lineage_edge_node_kind
        CHECK (
            source_node_kind IN ('FORMAL_ASSET', 'TEMPORARY_ASSET', 'UNRESOLVED')
            AND target_node_kind IN ('FORMAL_ASSET', 'TEMPORARY_ASSET', 'UNRESOLVED')
        ),
    CONSTRAINT ck_lineage_edge_endpoints
        CHECK (LENGTH(TRIM(source_table)) > 0 AND LENGTH(TRIM(target_table)) > 0),
    CONSTRAINT ck_lineage_edge_identity
        CHECK (
            LENGTH(TRIM(environment)) > 0
            AND LENGTH(TRIM(source_profile)) > 0
            AND LENGTH(TRIM(program_key)) > 0
            AND LENGTH(TRIM(edge_key)) > 0
        )
)
WITH (ORIENTATION = COLUMN)
DISTRIBUTE BY HASH (row_key)
PARTITION BY RANGE (observed_at)
(
    PARTITION p_lineage_edge_seed
        VALUES LESS THAN (TIMESTAMP WITH TIME ZONE '2099-01-01 00:00:00+00'),
    PARTITION p_lineage_edge_max
        VALUES LESS THAN (MAXVALUE)
);

CREATE INDEX dwp.ix_lineage_edge_source_active
    ON dwp.lineage_edge (
        environment, source_profile, source_table, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_edge_target_active
    ON dwp.lineage_edge (
        environment, source_profile, target_table, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_edge_program
    ON dwp.lineage_edge (
        environment, source_profile, program_key, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_edge_stable_key
    ON dwp.lineage_edge (edge_key);

COMMENT ON TABLE dwp.lineage_edge IS
    'Issue #39 physical direct edge source of truth; TMP, cycles and self-reference are retained.';
COMMENT ON COLUMN dwp.lineage_edge.source_table IS
    'Physical node label, not necessarily a DatasetIdentity; preserve schema and TMP label.';
COMMENT ON COLUMN dwp.lineage_edge.source_dataset_key IS
    'Nullable DatasetIdentity projection; NULL for TMP or unresolved physical nodes.';
COMMENT ON COLUMN dwp.lineage_edge.evidence_json IS
    'Bounded deterministic provenance JSON text; never full script or connection data.';

-- ---------------------------------------------------------------------------
-- 3. Derived business lineage: formal endpoints only, same batch as physical
-- ---------------------------------------------------------------------------
CREATE TABLE dwp.lineage_business_edge (
    row_key                 VARCHAR(128) NOT NULL,
    business_edge_key       VARCHAR(128) NOT NULL,
    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,
    program_key             VARCHAR(128) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,
    source_dataset_key      VARCHAR(128) NOT NULL,
    source_table            VARCHAR(512) NOT NULL,
    target_dataset_key      VARCHAR(128) NOT NULL,
    target_table            VARCHAR(512) NOT NULL,
    collapse_depth          INTEGER,
    path_count              NUMERIC(38, 0),
    physical_derivation_hash VARCHAR(128) NOT NULL,
    source_hash             VARCHAR(128),
    pipeline_version        VARCHAR(256),
    batch_id                VARCHAR(128) NOT NULL,
    observed_at             TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    first_seen_at           TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,
    is_active               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_lineage_business_edge PRIMARY KEY (row_key),
    CONSTRAINT uq_lineage_business_edge_batch_key
        UNIQUE (batch_id, business_edge_key),
    CONSTRAINT ck_lineage_business_edge_formal_endpoints
        CHECK (
            LENGTH(TRIM(source_table)) > 0
            AND LENGTH(TRIM(target_table)) > 0
            AND source_dataset_key IS NOT NULL
            AND target_dataset_key IS NOT NULL
        ),
    CONSTRAINT ck_lineage_business_edge_depth
        CHECK (collapse_depth IS NULL OR collapse_depth >= 1),
    CONSTRAINT ck_lineage_business_edge_path_count
        CHECK (path_count IS NULL OR path_count >= 1),
    CONSTRAINT ck_lineage_business_edge_identity
        CHECK (
            LENGTH(TRIM(environment)) > 0
            AND LENGTH(TRIM(source_profile)) > 0
            AND LENGTH(TRIM(program_key)) > 0
            AND LENGTH(TRIM(business_edge_key)) > 0
        )
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH (row_key)
PARTITION BY RANGE (observed_at)
(
    PARTITION p_lineage_business_edge_seed
        VALUES LESS THAN (TIMESTAMP WITH TIME ZONE '2099-01-01 00:00:00+00'),
    PARTITION p_lineage_business_edge_max
        VALUES LESS THAN (MAXVALUE)
);

CREATE INDEX dwp.ix_lineage_business_edge_source_active
    ON dwp.lineage_business_edge (
        environment, source_profile, source_table, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_business_edge_target_active
    ON dwp.lineage_business_edge (
        environment, source_profile, target_table, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_business_edge_program
    ON dwp.lineage_business_edge (
        environment, source_profile, program_key, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_business_edge_stable_key
    ON dwp.lineage_business_edge (business_edge_key);

COMMENT ON TABLE dwp.lineage_business_edge IS
    'Issue #39 derived formal business edge; never a physical source of truth and never a TMP endpoint.';
COMMENT ON COLUMN dwp.lineage_business_edge.collapse_depth IS
    'Proposed physical direct-edge hop count; nullable until v0.1 depth decision is frozen.';
COMMENT ON COLUMN dwp.lineage_business_edge.path_count IS
    'Optional exact distinct safe physical path count; NULL means not safely computed, never a sample length.';
COMMENT ON COLUMN dwp.lineage_business_edge.physical_derivation_hash IS
    'Hash of canonical contributing physical edges; not part of business_edge_key.';

-- ---------------------------------------------------------------------------
-- 4. Issues: physical/business diagnostics and #36 compatibility slots
-- ---------------------------------------------------------------------------
CREATE TABLE dwp.lineage_issue (
    row_key                 VARCHAR(128) NOT NULL,
    stable_issue_key        VARCHAR(128) NOT NULL,
    issue_layer             VARCHAR(32) NOT NULL DEFAULT 'PHYSICAL',
    environment             VARCHAR(128) NOT NULL,
    source_profile          VARCHAR(256) NOT NULL,
    program_key             VARCHAR(128) NOT NULL,
    program_name            VARCHAR(512) NOT NULL,
    issue_type              VARCHAR(128) NOT NULL,
    severity                VARCHAR(32),
    disposition             VARCHAR(32),
    rule_version            VARCHAR(256),
    policy_version          VARCHAR(256),
    node_key                VARCHAR(512),
    branch_sink             VARCHAR(512),
    message                 TEXT NOT NULL,
    evidence_json           TEXT,
    batch_id                VARCHAR(128) NOT NULL,
    first_seen_at           TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_seen_at            TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_changed_at         TIMESTAMP(6) WITH TIME ZONE,
    is_active               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_lineage_issue PRIMARY KEY (row_key),
    CONSTRAINT uq_lineage_issue_batch_key
        UNIQUE (batch_id, stable_issue_key),
    CONSTRAINT ck_lineage_issue_layer
        CHECK (issue_layer IN ('PHYSICAL', 'BUSINESS')),
    CONSTRAINT ck_lineage_issue_identity
        CHECK (
            LENGTH(TRIM(environment)) > 0
            AND LENGTH(TRIM(source_profile)) > 0
            AND LENGTH(TRIM(program_key)) > 0
            AND LENGTH(TRIM(issue_type)) > 0
            AND LENGTH(TRIM(stable_issue_key)) > 0
        )
)
WITH (ORIENTATION = ROW)
DISTRIBUTE BY HASH (row_key)
PARTITION BY RANGE (last_seen_at)
(
    PARTITION p_lineage_issue_seed
        VALUES LESS THAN (TIMESTAMP WITH TIME ZONE '2099-01-01 00:00:00+00'),
    PARTITION p_lineage_issue_max
        VALUES LESS THAN (MAXVALUE)
);

CREATE INDEX dwp.ix_lineage_issue_active_scope
    ON dwp.lineage_issue (
        environment, source_profile, program_key, issue_type, batch_id, is_active
    );
CREATE INDEX dwp.ix_lineage_issue_stable_key
    ON dwp.lineage_issue (stable_issue_key);
CREATE INDEX dwp.ix_lineage_issue_branch
    ON dwp.lineage_issue (environment, source_profile, branch_sink, is_active);

COMMENT ON TABLE dwp.lineage_issue IS
    'Physical/business diagnostics with nullable #36 policy compatibility slots.';
COMMENT ON COLUMN dwp.lineage_issue.severity IS
    'Reserved compatibility slot; final severity policy belongs to Issue #36.';
COMMENT ON COLUMN dwp.lineage_issue.disposition IS
    'Reserved compatibility slot; final disposition policy belongs to Issue #36.';
COMMENT ON COLUMN dwp.lineage_issue.rule_version IS
    'Reserved compatibility slot; final rule version contract belongs to Issue #36.';
COMMENT ON COLUMN dwp.lineage_issue.policy_version IS
    'Reserved compatibility slot; final policy version contract belongs to Issue #36.';

-- ---------------------------------------------------------------------------
-- 5. Publish/query notes (not executable migration)
-- ---------------------------------------------------------------------------
-- A writer must validate all five tables with the same batch_id before switching:
--   dwp.lineage_batch
--   dwp.lineage_program_state
--   dwp.lineage_edge
--   dwp.lineage_business_edge
--   dwp.lineage_issue
--
-- Active business query shape (parameters remain bound values):
--
-- SELECT be.source_table, be.target_table, be.program_key
-- FROM dwp.lineage_business_edge AS be
-- JOIN dwp.lineage_batch AS b
--   ON b.batch_id = be.batch_id AND b.is_active = TRUE
-- WHERE be.is_active = TRUE
--   AND be.environment = :environment;
--
-- No closure table is created by Issue #39. No unqualified table name,
-- current_schema lookup, search_path mutation, production migration, or runtime
-- imp_lineage_edge change is part of this design draft.
