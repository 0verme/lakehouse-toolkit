# ODS / DWO → DWF Field Mapping Collector

## Scope and ownership

This is an independent field-mapping collector. `jobs/crontab/imp_dws_comments.py` and its cron remain unchanged and are neither imported nor reused.

- **lakehouse-toolkit** scans DWF projects, extracts statically identifiable SQL, resolves source/target/field mappings, validates the import payload, and writes local audit reports.
- **DAP** validates the Field Mapping Import contract, authorizes the caller, upserts, persists, and displays the data.
- The collector never connects to DAP's database, never connects to source-system JDBC endpoints, and never deletes, truncates, or replaces data. Import mode is always `upsert`.

The checked-in DAP contract reference is `backend/app/contracts/field_mapping.py`; current API behavior was cross-checked against its FastAPI route/service. The importer uses `POST /api/field-mappings/import`. The DAP instance-local `upstreamSystemId` is looked up at runtime by exact `recv_plan` ↔ upstream `id` match from `GET /api/upstreams/systems`; no DAP primary key is hardcoded. The stable business identity in collector/audit data is `recv_plan + sourceTable + targetTable`.

## Metadata input and authoritative identity

`--metadata-json` is a runtime JSON snapshot containing only the two whitelisted data sets needed for resolution:

```json
{
  "recv_dwf": [
    {
      "recv_plan": "<RECV_PLAN>",
      "table_name": "DWF.DWF_<LOGICAL_TABLE>",
      "data_source": "<DATA_SOURCE>",
      "recv_job_name": "<RECV_JOB>",
      "ods_job_name": "<ODS_JOB>"
    }
  ],
  "schema_config": [
    {"schema_key": "<DATA_SOURCE>", "db_schema": "<AUTHORITATIVE_DB_SCHEMA>"}
  ]
}
```

Prepare this file from an authorized, read-only metadata export. Do not include `jdbc_url`, `db_user`, hosts, passwords, or any other connection details. The collector does not connect to source-system databases.

DWF matching normalizes history names only for comparison:

- `DWF.F_<LOGICAL>` → `<LOGICAL>`
- `DWF.DWF_<LOGICAL>` → `<LOGICAL>`
- source project/table `DWF_<LOGICAL>` → `<LOGICAL>`

DAP's final `targetTable` uses the canonical source-code form `DWF_<LOGICAL>`; history `F_*` names are never emitted as targets. Program evidence is an exact normalized `ods_job_name` ↔ Python filename match (numeric program prefix, `JOB_`, duplicated leading `DWS_`, and `_DAY`/`_NIGHT` suffix normalized). Logical target is the next evidence; the physical DWO schema match is then verified against `p_schema_config.db_schema`. Any evidence disagreement or unresolved multiplicity is reported as unresolved/conflict, never selected by row order.

DWO table parsing uses the authoritative `db_schema` dictionary and longest exact prefix. For example, `DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE` plus schema `DEMO_SCHEMA_A` produces `sourceTable=DEMO_SOURCE_TABLE`; `physicalSourceTable` remains in audit output. `sourceTable` never contains the DWO technical prefix. Multiple source systems may legitimately feed one DWF target.

## SQL extraction and safe projection

Python SQL candidate extraction reuses `shared.lineage.physical_dag._extract_python_candidates_with_reason`; SQL statements and projections use SQLGlot AST. This stays a Field Mapping business projection and does not write or alter common lineage materializations.

Supported safe projections include `INSERT INTO ... SELECT`, aliases, quoted/case-varied identifiers, `CAST` and single/multi-input expressions, `JOIN`, simple CTEs, multiple Python files in a project, multiple DWO inputs, and DWF temporary-table passthrough. DWF intermediate/TMP fields are recursively traced to their DWO leaf fields; TMP tables are not emitted as source tables. For expressions with multiple upstream fields (for example `COALESCE` or `CASE`), each actual input field is emitted against the same target field; the DAP contract allows distinct sourceField/targetField pairs. Unqualified ambiguous fields, unknown relations, cycles, or unsupported SQL are audited and excluded. `MERGE` is explicitly unsupported in this first safe projection and never produces guessed mappings.

`sourceType` and `sourceComment` are omitted because source database metadata is not crawled. Mapping rules are `DIRECT`, `RENAME`, or the DAP contract's `待补充` for expressions.

## Execution modes

The default mode is `local-dry-run`, so running the collector does not write data. Each run writes `summary.json`, `resolved.csv`, `unresolved.csv`, and `conflicts.csv` under a timestamped, gitignored `runtime/field_mapping_sync/` directory. Add `--include-payload` to save `payload.json` there. Reports retain physical source names and business evidence but never connection URLs or credentials; do not copy real reports into Git.

DAP upstream systems may be resolved by the read-only API GET or, for local dry-run only, a previously downloaded `--upstreams-json` response with an `items` array.

### 1. Local dry-run

Scans source, reads metadata, parses SQL, resolves identities, validates each local payload, and writes audit artifacts. It does not call the Field Mapping Import endpoint.

```powershell
$env:DAP_API_BASE_URL = 'http://127.0.0.1:15099'
# Set this locally to the authorized DWF source root; do not commit the path.
$env:DWF_SOURCE_ROOT = '<path-to-DWS_DWF>'
python -B jobs/crontab/sync_ods_dwf_field_mappings.py `
  --directory $env:DWF_SOURCE_ROOT `
  --metadata-json '<secure-local-path>\field-mapping-metadata.json' `
  --mode local-dry-run --login --batch-size 100
```

`--login` prompts for username and uses a non-echoing password prompt. It calls `/api/auth/login` and `/api/auth/me`; password values are not logged or accepted on the command line. Alternatively inject an already obtained signed session cookie at runtime with `DAP_SESSION_COOKIE`. Never commit a local config or secret.

### 2. DAP server dry-run

Calls the actual import endpoint with `dryRun=true`; DAP validates the full contract without persistence:

```powershell
python -B jobs/crontab/sync_ods_dwf_field_mappings.py `
  --directory $env:DWF_SOURCE_ROOT `
  --metadata-json '<secure-local-path>\field-mapping-metadata.json' `
  --api-base-url $env:DAP_API_BASE_URL `
  --mode server-dry-run --login --batch-size 100
```

### 3. Real sync (Windows localhost only)

```powershell
python -B jobs/crontab/sync_ods_dwf_field_mappings.py `
  --directory $env:DWF_SOURCE_ROOT `
  --metadata-json '<secure-local-path>\field-mapping-metadata.json' `
  --api-base-url $env:DAP_API_BASE_URL `
  --mode real-sync --login --batch-size 100
```

Real import has a hard code guard: only `localhost` or `127.0.0.1` URLs are accepted. There is no remote-write override; real POSTs bypass environment proxies and do not follow redirects, so a loopback write cannot be routed to a remote service through proxy/redirect settings. Batches are serial and default to 100 mapping items (DAP contract maximum 500 items and 1,000 fields per item). After real sync, the runner reads `/api/field-mappings/stats`, `/tables`, and `/fields` and records reconciliation counts.

Production service-token/M2M authentication and Linux production cutover are TODOs for a separate issue. This collector does not change Linux cron configuration.

## Audit summary

`summary.json` records actual scan counts (`project_count`, `python_file_count`, `dwo_physical_table_count`), metadata row counts, DAP upstream count, resolved project/table/field mapping counts, unresolved reasons (`no_program`, `no_final_target`, `no_recv_dwf`, `no_schema_config`, `no_dwo_source`, `unknown_upstream_system`, `unsupported_sql`, `multi_source_field_unsupported`), conflict reasons (`program_metadata_conflict`, `multiple_recv_plan_conflict`, `multiple_data_source_conflict`, `schema_match_conflict`), and failed requests/items. Unresolved, conflict, unsupported, and contract-invalid records never enter import batches.
