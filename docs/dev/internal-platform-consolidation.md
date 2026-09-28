# Internal Platform Consolidation

## Source and runtime policy

- `0verme/lakehouse-toolkit` is the sole maintained source repository.
- Linux runs a checked-out/released copy under an operator-configured `PYTOOL_ROOT`; the deployed tree is not a second development branch.
- The runtime interpreter is supplied as `PYTOOL_PYTHON` by deployment configuration. Neither its absolute path nor machine-specific tool paths belong in this repository.
- The temporary lineage installation is a transition aid only. Retire it only after the cutover checks below pass.
- Application code, examples, tests, and configuration templates are source. Local configuration, credentials, JDBC binaries, private metadata, logs, databases, generated exports, caches, and runtime state are deployment data and must remain outside Git.

## Inventory method and result

The inventory compared the tracked repository tree at the consolidation baseline with the exported historical runtime tree. It uses relative paths and byte-content SHA-256 comparisons for same-path files; it does not use timestamps or file sizes to choose a version. The full path-level TSV is kept in the local workspace control plane rather than committed because historical paths and names can disclose internal structure. It contains no file contents, credentials, or hashes of deployment-only files.

The inventory has 29,671 union entries (287 tracked repository paths and 29,441 historical-runtime files). Mutually exclusive primary classifications:

| Classification | Count | Treatment |
| --- | ---: | --- |
| `SAME` | 8 | Same relative path and identical bytes. |
| `DIFFERENT` | 48 | Same source path, different bytes; decisions below use imports, behavior, tests, and safety boundaries. |
| `HOME_ONLY` | 66 | Historical source-path classification only; not a migration scope or cutover blocker by itself. |
| `LAKEHOUSE_ONLY` | 229 | Keep the current repository implementation, including the complete lineage module. |
| `DEPLOYMENT_ONLY` | 2,942 | Preserve locally; never publish as source. |
| `GENERATED` | 26,371 | Virtual environment, bytecode, and caches; do not publish. |
| `SENSITIVE` | 3 | Historical local configuration files; keep out of Git. |
| `UNKNOWN` | 4 | Classification uncertainty only; owner scope decisions apply, and nothing is automatically deleted. |

Some paths also carry secondary heuristic tags. In particular, 48 historical text files triggered a conservative credential/connection/private-address/absolute-path detector. A hit is not proof of a live credential. It creates a review gate only for code that is explicitly selected for migration; it does not override `DO_NOT_MIGRATE` or block cutover for excluded capabilities. The detector records indicator names only, never matched values.

The raw machine inventory is an audit artifact, not a deployment list. It is regenerated when either input tree changes.

## Semantic merge decisions

### Shared database boundary: `shared/db/gaussdb.py`

- Historical behavior: read `configs/database.yaml`, accepted inline `password`, used the GaussDB JDBC driver and `gaussdb200.jar` defaults, exposed profile-based query/execute helpers, and closed cursors/connections.
- Repository behavior: preferred `configs/database.local.yaml`, supported `database.yaml` and the public example, accepted `password_env`, dynamically loaded JayDeBeApi, supported bound parameters, and had safer error/cleanup handling.
- Shared result: preserve the repository loader and parameter-aware API while restoring the legacy GaussDB driver/JAR defaults and resolving relative JAR paths against the repository root. Absolute operator-supplied JAR paths remain unchanged. Existing function calls without `params` remain compatible.
- Lineage already uses this same `connect_with_profile` boundary for DWS reads/writes. SQL-source MySQL profiles remain separate by design; they are not duplicate GaussDB configurations.
- Production contract: keep one authoritative local database profile file. For an installation that has only the historical `database.yaml`, retain it and do not create a second `database.local.yaml` during migration. If both files already exist, reconcile them before deployment; the loader intentionally treats the local file as an override. Passwords, URLs, hosts, and absolute JAR paths are never copied into examples or Git.

### Tool registry and webadmin

- Historical behavior: `tools.yaml` held public and deployment-specific paths/hosts/ports together; the webadmin manager loaded only that file.
- Repository behavior: curated `configs/tools.yaml` contains portable defaults, `tools.local.yaml` overrides matching tool names, and webadmin already restricts log reads and manager actions.
- Shared result: `shared/config/tool_registry.py` now provides one loader for webadmin, its process manager, and PyWebIO title/port resolution. Local entries override matching public entries and may append local-only entries. This allows deployment-specific registrations without copying internal endpoints or paths into the tracked registry.
- Keep the current repository registry as the base; it already registers `lineage_reconciliation` and `lineage_explorer`. Do not replace it with the historical full registry. Migrate required local values into the ignored local overlay.

### PyWebIO helper

- Historical behavior: title lookup used the public registry, the default bind host was all interfaces, and error rendering performed a path-specific string replacement.
- Repository behavior: added local registry support, CSS/theme integration, CLI host/port handling, and safe loopback defaults.
- Shared result: keep the repository helper and shared registry lookup; escape error text before rendering HTML. Production bind address and public URL belong only in the local tool overlay. This avoids silently exposing a new service on all interfaces while allowing the operator to configure the historical network behavior explicitly.

### Cron bootstrap

The two `_bootstrap.py` versions are behaviorally equivalent after parsing (the byte difference is formatting/encoding only). Keep the repository version; the existing direct-script and module invocation patterns both resolve the project root. A regression test now checks idempotent path insertion.

`jobs/crontab/imp_schema_config.py` is a substantive divergence: the historical script hardcodes working-copy roots and database targets and builds SQL from file data, while the repository version uses configurable paths/profile/table identifiers and bound insert values. Keep the repository contract. The f-string SQL formatter was also adjusted to compile on the supported Python versions, with identifier and SQL-output tests; it does not perform a DB write in tests.

### `configs/tools.yaml`

The repository file is the portable base and already includes both lineage pages. The historical file contains local interpreter paths, internal URL/bind values, and many additional tools. These values must be migrated selectively into `configs/tools.local.yaml`; local-only tools are accepted by the new shared loader. Do not import historical endpoints or credentials into the base registry.

### Owner scope decision: HCYT/NUPS

HCYT/NUPS are explicitly `DO_NOT_MIGRATE`. Their historical rule and UI differences do not create a porting, security-refactor, test, PR, or production cutover requirement for this consolidation. Existing historical files are left untouched; this decision does not authorize deleting them.

## HOME_ONLY disposition: source classification, not migration scope

`HOME_ONLY` means only that a path existed in the historical export and not in the repository snapshot. It does **not** mean that all 66 paths must be migrated, reviewed, retired, or resolved before production cutover. Owner decisions for this consolidation are:

| Capability | Decision | Scope effect |
| --- | --- | --- |
| HCYT / NUPS | `DO_NOT_MIGRATE` | No port, code/security changes, tests, or new PR. Not a cutover blocker. |
| Cigen | `DO_NOT_MIGRATE` | No port, code/security changes, tests, or new PR. Not a cutover blocker. |
| Interface Manager | `DO_NOT_MIGRATE` | No port or remediation work in this consolidation. Not a cutover blocker. |
| Metric Portal | `DO_NOT_MIGRATE` | No port or remediation work in this consolidation. Not a cutover blocker. |
| backup / yamlbak / historical backup content | `DO_NOT_MIGRATE` | Never copy into the repository; preserve existing files. No automatic cleanup is authorized. Not a cutover blocker. |
| Internal cron / tools | `REVIEW_ACTIVE_RUNTIME` | Identify only entries required by the future unified runtime; uncertain entries remain `NEEDS_RUNTIME_CONFIRMATION`. |
| Historical Shark cycle-check duplicate | `COMPARE_WITH_JOBGRAPH` | Complete the explicit parity review below; do not remove historical files. |

`DO_NOT_MIGRATE` is not permission to delete production files. The historical tree and backup artifacts are not changed or cleaned by this work. Only active runtime requirements and the Shark comparison, alongside deployment validation, can hold production cutover.

### Internal cron / tools: unresolved source candidates

The local inventory records relative paths, but the historical source tree is not available in this review workspace. No historical source contents, production crontab, or ignored local tool registry were read. Therefore the source inventory alone cannot establish whether any listed file is still active, who invokes it, or whether a file is an entry point or a helper. These candidates are all `NEEDS_RUNTIME_CONFIRMATION`; that label records uncertainty and does **not** assert that every file is needed or active.

| Relative path | Tool/task name | Candidate entry file | Called by | Why unresolved |
| --- | --- | --- | --- | --- |
| `jobs/cms/cms_compare.py` | `cms_compare` | Same file; entry status unverified | Unknown | Inventory path only; source and caller evidence unavailable. |
| `jobs/cms/cms_compare_create.py` | `cms_compare_create` | Same file; entry status unverified | Unknown | Inventory path only; source and caller evidence unavailable. |
| `jobs/crontab/czcb_sc.py` | `czcb_sc` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/imp_dwuprr.py` | `imp_dwuprr` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/imp_dwuprr_local_send.py` | `imp_dwuprr_local_send` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/imp_moia_dws.py` | `imp_moia_dws` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/imp_seachar_directories.py` | `imp_seachar_directories` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/import_mapping_to_sqlite.py` | `import_mapping_to_sqlite` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/svn_fine.py` | `svn_fine` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/crontab/unzip_moia.py` | `unzip_moia` | Same file; entry status unverified | Unknown | No approved crontab/runtime schedule evidence. |
| `jobs/deveff/_bootstrap.py` | `_bootstrap` helper | Helper candidate; caller unknown | Unknown | Inventory path only; source call graph unavailable. |
| `jobs/deveff/auto_publishlist.py` | `auto_publishlist` | Same file; entry status unverified | Unknown | Inventory path only; source and caller evidence unavailable. |
| `jobs/deveff/deveff.py` | `deveff` | Same file; entry status unverified | Unknown | Inventory path only; source and caller evidence unavailable. |
| `shared/lineage/didp_lineage.py` | `didp_lineage` helper | Entry/helper status unknown | Unknown | Inventory path only; source call graph unavailable. |
| `shared/lineage/job_lineage.py` | `job_lineage` helper | Entry/helper status unknown | Unknown | Inventory path only; source call graph unavailable. |
| `tools/cms/cms_comments.py` | `cms_comments` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/cms_compare.py` | `cms_compare` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/cms_compare_create.py` | `cms_compare_create` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/cms_compareklq.py` | `cms_compareklq` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/didp_lineage_roamer.py` | `didp_lineage_roamer` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/didp_schedule_diff.py` | `didp_schedule_diff` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/didp_sql_upstream_to_dwf.py` | `didp_sql_upstream_to_dwf` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/dws_create_cms.py` | `dws_create_cms` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/seachar_didp_mysql.py` | `seachar_didp_mysql` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/xueyuan_xd.py` | `xueyuan_xd` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/cms/xueyuan_xd2.py` | `xueyuan_xd2` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/czcb/auto_svn.py` | `auto_svn` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/misc/auto_test_job.py` | `auto_test_job` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/misc/tiaopao.py` | `tiaopao` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/misc/xueyuan.py` | `xueyuan` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/misc/xueyuan_sql.py` | `xueyuan_sql` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/misc/yilaii.py` | `yilaii` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/search/seachar_didp.py` | `seachar_didp` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/search/seachar_yuan.py` | `seachar_yuan` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |
| `tools/sql/cms_create_foreign_table.py` | `cms_create_foreign_table` | Same file; entry status unverified | Unknown | Inventory path only; local registry/manual caller unavailable. |

Phase 2A must obtain an owner-approved runtime inventory (for example, approved crontab entries, the deployed local tool registry, or documented manual workflows) and match each active task to its source entry and dependency chain. Only tasks shown to be required are candidates for migration; do not retire or migrate based on file presence alone.

### Shark vs `tools/jobgraph` parity

Historical inventory paths are `tools/shark/job_dependency_cycle_check.py`, `tools/shark/tiaopao_214.py`, `tools/shark/tiaopao_224.py`, and `tools/shark/tiaopao_31.py`. Their source contents and call graph are not available in this review, so these filenames cannot establish behavior or active use.

| Comparison | Current `tools/jobgraph` evidence | Historical Shark evidence | Result |
| --- | --- | --- | --- |
| Input | Reads job/dependency pairs from the configured logical `relations` table; ignores null job names. | Unknown; source unavailable. | Not comparable yet. |
| Output | PyWebIO preview, `EVT` dependency warnings, and cycle list (bounded to 50). | Unknown. | Not comparable yet. |
| Core rules | Normalizes names to uppercase, de-duplicates edges, flags `EVT` dependencies, then calls `find_cycles`. | Unknown. | Parity unproven. |
| Page / CLI entry | `tools/jobgraph/job_dependency_cycle_check.py` starts a PyWebIO page and is registered as `job_dependency_cycle`; the tracked registry currently sets it disabled. No parity test was found. | Historical page/CLI entry and registration status unknown. | Not comparable yet. |
| Dependencies / callers | Uses `pymysql`, `shared.graph.dependency`, metadata-table configuration, and required MySQL credentials. | Unknown. | Call graph and input contract unproven. |
| Unique capability | This implementation covers its own configured cycle check only. | The three `tiaopao_*` files may be separate capabilities, but filenames do not prove their behavior. | Historical-only capability status unknown. |

**Decision: `NEEDS_RUNTIME_CONFIRMATION`.** There is not enough code evidence to claim `REPLACED_BY_JOBGRAPH` or `KEEP_BOTH`. Phase 2B must compare historical source behavior, inputs/outputs, entry points and callers against jobgraph. Do not delete historical Shark files in this phase.

## Phase sequence and production cutover gate

PR #146 is **Phase 1 / 公共平台桥接层**. It provides shared registry, webadmin/tool-manager/PyWebIO integration, escaping, GaussDB compatibility, schema-config cron compatibility, dependency/ignore updates, docs and tests. **Merge #146 does not equal production cutover** and does not authorize deployment.

Production cutover remains on hold only for these actual remaining workstreams:

- Phase 2A: confirm which internal cron/tools are active and required, then migrate only those.
- Phase 2B: complete the Shark/jobgraph parity decision.
- Phase 3: validate the target runtime, production-local configuration and database profile/JAR, `tools.local.yaml`, lineage scopes, reviewed deployment manifest, webadmin/lineage smoke, approved crontab switch, and one approved daily run with idempotency/result checks.

HCYT/NUPS, Cigen, Interface Manager, Metric Portal, and backup/yamlbak content are not cutover blockers and are not part of these phases.

## Lineage completeness and management integration

The repository already carries the full lineage implementation rather than a five-file runtime patch: `shared/lineage/` (32 files), `tools/lineage/` (7 files), the lineage/schedule cron entrypoints (5 files), and the associated tests. The modules resolve their DWS access through `shared.db.gaussdb`; the Explorer and reconciliation pages call the existing domain/query services rather than opening a second portal.

The original Streamlit webadmin remains the sole management home. Its registry links the existing pages by their configured ports/titles; the local overlay supplies server bind/public URL and interpreter values. No second Lakehouse landing page is introduced.

## Linux deployment manifest

Deployment uses an approved source release and an operator-provided `PYTOOL_ROOT` / `PYTOOL_PYTHON`. Do not source code from a second runtime tree. The repository does not encode the production absolute paths.

### Positive deployment allowlist

At an approved future cutover, deploy only from a clean, reviewed repository release. Build a positive manifest from tracked source files under `apps/`, `jobs/`, `shared/`, and `tools/`, plus the explicit public runtime files below. Do not sync the release root and rely on an expanding `--exclude` list. No recursive deletion is permitted.

The public config templates currently tracked for operator reference are `configs/audit_datasource.example.yaml`, `configs/database.example.yaml`, `configs/lineage_providers.example.yaml`, `configs/migrate/clusters.example.json`, `configs/svn.example.yaml`, and `configs/svn_inventory.example.yaml`. Include only those templates approved for the release; never deploy local config files.

A future cutover operator may generate and inspect an allowlist like this. These commands are documentation only and were **not** run in this PR:

```bash
MANIFEST=$(mktemp)
{
  git -C "$RELEASE_ROOT" ls-files -- apps/ jobs/ shared/ tools/
  printf '%s\n' \
    configs/tools.yaml \
    configs/audit_datasource.example.yaml \
    configs/database.example.yaml \
    configs/lineage_providers.example.yaml \
    configs/migrate/clusters.example.json \
    configs/svn.example.yaml \
    configs/svn_inventory.example.yaml \
    requirements.txt
} | LC_ALL=C sort -u > "$MANIFEST"

# Review the exact prospective changes first; this is a dry run.
rsync -ain --files-from="$MANIFEST" "$RELEASE_ROOT/" "$PYTOOL_ROOT/"

# Only after explicit deployment approval and manifest review:
rsync -ai --files-from="$MANIFEST" "$RELEASE_ROOT/" "$PYTOOL_ROOT/"
rm -f "$MANIFEST"
```

The manifest contains file paths only; it does not include `.git`, local configuration (`database.yaml`, `*.local.yaml`, `svn.yaml`, local cluster files), runtime state, logs, databases/SQLite, JARs, workbooks, caches, or backups. Review same-path changes in the dry-run output before applying: an allowlisted file may replace an existing file at that path. Files outside the allowlist are left untouched. Do not add `--delete`.

## Files to preserve

Preserve these in place and outside Git:

- `configs/database.yaml`, any existing `configs/database.local.yaml`, `configs/lineage_providers.local.yaml`, `configs/tools.local.yaml`, `configs/svn.yaml`, `configs/migrate/clusters.json`, and all other local configuration.
- `data/`, `logs/`, `runtime/`, `tmp/`, `backup_sc/`, `send_files/`, virtual environments, generated outputs, local SQLite/database files, and internal workspaces.
- All JDBC/JAR binaries, including the operator-managed GaussDB driver. Never copy a bundled/local JAR into the repository.
- Historical resource data such as local `resources/xlsx/*.xlsx`; the repository-tracked mapping workbook is an empty scaffold and must not replace the historical populated mapping data. New local workbooks under this path are now ignored by Git.
- Historical source/recovery files outside the approved allowlist, including `DO_NOT_MIGRATE` capabilities and unresolved cron/tool or Shark files. Preserve them in place; this is not authorization for production cleanup.

## Configuration migration steps

1. Back up the current local configs outside the release tree. Do not include their contents in an issue, PR, artifact, or Git commit.
2. Keep the existing database profile file as the single contract. The repository loader accepts legacy inline passwords and `password_env`; preserve existing profile names referenced by lineage scopes. Confirm the selected `jar_path` resolves to the operator-managed JDBC file. If a second local DB file exists, reconcile it before starting services; do not leave two divergent profile sets.
3. Create/retain ignored `configs/lineage_providers.local.yaml` with the approved source profiles and environment scopes. `scopes[].dws_profile` must name a profile in the authoritative database config. The public example is a template only and is not a production replacement.
4. Keep tracked `configs/tools.yaml` as the portable base. Translate only required local bind host, public URL, interpreter, port, and enablement values to ignored `configs/tools.local.yaml`. For still-supported local-only scripts, add a local entry only after its source exists in the deployed source tree. The shared loader merges local overrides and appends local-only entries.
5. Do not copy historical values into `configs/*.example.*`; never put a real password, JDBC URL/host, internal URL, or driver JAR in Git.

## Cron migration

Do not modify a production crontab from the source PR. Back up the current user's crontab, replace the old lineage entry only after the target tree/config is validated, and verify the resulting list. The historical form was:

```cron
0 0,12 * * * /usr/bin/flock -n /tmp/lineage_daily.lock -c 'cd "$TEMP_LINEAGE_ROOT" && "$PYTOOL_PYTHON" -B -m jobs.crontab.imp_lineage_daily --environment "$LINEAGE_ENVIRONMENT"' >> "$TEMP_LINEAGE_ROOT/runtime/lineage_daily.log" 2>&1
```

The target form is:

```cron
0 0,12 * * * /usr/bin/flock -n /tmp/lineage_daily.lock -c 'cd "$PYTOOL_ROOT" && "$PYTOOL_PYTHON" -B -m jobs.crontab.imp_lineage_daily --environment "$LINEAGE_ENVIRONMENT"' >> "$PYTOOL_ROOT/logs/lineage_daily.log" 2>&1
```

Before installing either form, substitute the shell variables with operator-approved absolute values; cron does not guarantee an interactive shell environment. Keep the single-lock behavior and redirect logs into the unified runtime log tree. After installation, confirm no active lineage cron points to the retired runtime.

`imp_lineage_daily` intentionally has no dry-run flag: it materializes snapshots and suppression rows. `--help` is a safe CLI smoke. A real one-environment execution is a write operation and must run only after DB-owner approval and a verified config/backup; then inspect the bounded per-step summary and verify idempotency.

## Web startup and registration

1. Start the existing `webadmin` tool through its manager, using the locally configured interpreter, port, bind host, and public URL.
2. Start/check `lineage_reconciliation` and `lineage_explorer` through that same manager. They are entries in the tracked registry, not separate homepages.
3. Open the existing webadmin URL and select **SQL / 调度血缘对账** or **血缘查询 / Explorer**. Use the operator-configured public base URL; the tracked localhost default is not a production URL.
4. Confirm the local scope config exposes only approved environments and that a page load does not attempt to publish suppression data. Reconciliation writes occur only through the explicit daily/publish commands.

## Validation commands

Set `PYTOOL_ROOT` and `PYTOOL_PYTHON` in the shell from approved deployment values first.

### 1. Import/config smoke (no DB connection)

```bash
cd "$PYTOOL_ROOT"
"$PYTOOL_PYTHON" -B - <<'PY'
from pathlib import Path
from apps.webadmin.manager import tool_manager
from jobs.crontab._bootstrap import ensure_project_root_on_path
from shared.config.tool_registry import load_tool_configuration
from shared.db.gaussdb import load_db_profiles
from shared.lineage.environment_scope import load_lineage_environment_scopes

assert Path.cwd().resolve() == Path(ensure_project_root_on_path()).resolve()
profiles = load_db_profiles()
scopes = load_lineage_environment_scopes()
assert profiles and scopes
assert all(scope.dws_profile in profiles for scope in scopes)
config = load_tool_configuration('configs/tools.yaml', 'configs/tools.local.yaml')
names = {tool['name'] for tool in config.get('tools', [])}
assert {'lineage_reconciliation', 'lineage_explorer'} <= names
assert tool_manager.get_tool('webadmin')
print('import/config smoke: PASS')
PY
```

### 2. Target-scoped suppression dry-run

Use the operator-approved environment and target, substituted locally:

```bash
cd "$PYTOOL_ROOT"
"$PYTOOL_PYTHON" -B \
  -m jobs.crontab.imp_lineage_suppression \
  --environment "$LINEAGE_ENVIRONMENT" \
  --target "$LINEAGE_TARGET" \
  --dry-run
```

This mode reads/classifies but does not publish. It must retain the previously accepted suppression/actionability outcomes below.

### 3. Tool registration, command, and daily CLI safe smoke

```bash
cd "$PYTOOL_ROOT"
"$PYTOOL_PYTHON" -B - <<'PY'
from pathlib import Path
from apps.webadmin.manager.tool_manager import build_command, get_tool

for name, script in (
    ('lineage_reconciliation', 'reconcile_sql_schedule_web.py'),
    ('lineage_explorer', 'lineage_explorer_web.py'),
):
    tool = get_tool(name)
    assert (Path(tool['workdir']) / script).is_file()
    command = build_command(tool)
    assert str(Path(tool['workdir']) / script) in command
print('lineage web registry/command smoke: PASS')
PY
"$PYTOOL_PYTHON" -B -m jobs.crontab.imp_lineage_daily --help
```

Then use the original manager to start/check the two pages and verify the reconciliation page opens from the existing webadmin. The daily `--help` command is the non-mutating CLI smoke; do not invent a daily dry-run flag.

### Cron check

```bash
crontab -l | grep -F "$TEMP_LINEAGE_ROOT" && echo 'FAIL: legacy path remains' || echo 'PASS: no legacy lineage path'
```

## Rollback boundary and retirement criteria

Before the first deployment, make an operator-owned backup of code and local configuration outside the active tree. Rollback means restore the prior `/home` source/config snapshot and restore the previous crontab entry; do not roll back or delete DWS audit rows, production data, or local runtime directories as part of a code rollback.

Do not retire the temporary lineage runtime until all of these are evidenced:

1. Home-root lineage CLI passes.
2. Home-root lineage web page starts and passes a browser smoke.
3. The original webadmin remains healthy and both lineage entries are reachable from it.
4. The daily cron is switched to the home root and no active crontab references the temporary runtime.
5. At least one scheduled daily run from the home root reports success and its publish/reconciliation results are checked.

After those conditions, the temporary runtime may leave the running state. Physical deletion remains an operator decision.
