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
| `HOME_ONLY` | 66 | Historical source candidates; not implicitly deleted or copied. |
| `LAKEHOUSE_ONLY` | 229 | Keep the current repository implementation, including the complete lineage module. |
| `DEPLOYMENT_ONLY` | 2,942 | Preserve locally; never publish as source. |
| `GENERATED` | 26,371 | Virtual environment, bytecode, and caches; do not publish. |
| `SENSITIVE` | 3 | Historical local configuration files; keep out of Git. |
| `UNKNOWN` | 4 | Legacy backups or an undecodable filename; retain pending owner review. |

Some paths also carry secondary tags. In particular, 48 historical text files triggered a conservative credential/connection/private-address/absolute-path heuristic. A heuristic hit is not itself proof of a live credential, but it is a mandatory source-review and sanitization gate. The detector records only indicator names, never matched values.

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

### Same-path SVN review and batch-code differences

The repository contains its tested Lakehouse/upstream rule architecture; the historical copy contains additional HCYT/NUPS rules and different UI routing. They are not equivalent implementations and have not been silently discarded or copied. HCYT/NUPS remain `PORT_TO_REPO` candidates, gated on configuration extraction, representative fixtures/tests, and an adapter into the current app. Historical cron/migration variants also have different contracts; retain them pending their owner and schedule inventory rather than choosing a version by age.

## Home-only capability disposition

No historical-only application or operational job was deleted from the export or blindly copied into Git. The exported tree is untouched.

| Capability group | Disposition | Evidence / next safe action |
| --- | --- | --- |
| Cigen root-management app | `PORT_TO_REPO` candidate | It is registered in the historical tool registry, but source review found deployment-specific connection/path material and one undecodable source file. Externalize settings, restore clean UTF-8 source, and add fixtures before porting. |
| HCYT/NUPS SVN rules and pages | `PORT_TO_REPO` candidate | Distinct from the current Lakehouse rule set. Keep both capabilities; sanitize private configuration, add representative fixtures, and integrate as explicit project adapters before replacing the historical app. |
| Interface manager | `NEEDS_CONFIRMATION` | It handles downstream endpoint/account data and uses local SQLite state; confirm active ownership and add an access-control/data-path review before source publication. Preserve its database file. |
| Metric portal | `NEEDS_CONFIRMATION` / `DEPRECATE_CANDIDATE` | The non-MVP code has unauthenticated write operations; the MVP contains a weak default administrator credential. Confirm whether this is active, then harden or retire it explicitly. Preserve local database files. |
| CMS/development-efficiency and internal cron/tools | `NEEDS_CONFIRMATION` | Operational status, owners, schedules, and secret/config sources cannot be established by file presence. Several sources contain private-address/path or credential-like indicators. Externalize configuration and test before porting. |
| Historical Shark cycle-check duplicate | `DEPRECATE_CANDIDATE` | The repository has a separately registered and tested `tools/jobgraph` implementation. Compare expected output before disabling the older entry. |
| `*.pybak`, `tools.yamlbak`, archived YAML, unknown text | `DEPRECATE_CANDIDATE` / `UNKNOWN` | Do not delete automatically; confirm ownership and whether they are required recovery artifacts. |

These are the remaining deployment gates. Until each active HOME_ONLY capability is ported or explicitly retired, a full source-directory overwrite is unsafe. The current change makes the shared runtime contract ready but does not claim that every historical business module has been consolidated.

## Lineage completeness and management integration

The repository already carries the full lineage implementation rather than a five-file runtime patch: `shared/lineage/` (32 files), `tools/lineage/` (7 files), the lineage/schedule cron entrypoints (5 files), and the associated tests. The modules resolve their DWS access through `shared.db.gaussdb`; the Explorer and reconciliation pages call the existing domain/query services rather than opening a second portal.

The original Streamlit webadmin remains the sole management home. Its registry links the existing pages by their configured ports/titles; the local overlay supplies server bind/public URL and interpreter values. No second Lakehouse landing page is introduced.

## Linux deployment manifest

Deployment uses an approved source release and an operator-provided `PYTOOL_ROOT` / `PYTOOL_PYTHON`. Do not source code from a second runtime tree. The repository does not encode the production absolute paths.

### Files/directories to deploy after the HOME_ONLY gate is closed

- Source directories: `apps/`, `jobs/`, `shared/`, `tools/`.
- Tracked public runtime configuration: `configs/tools.yaml`, `configs/*.example.yaml`, and `configs/migrate/*.example.json` (where present).
- Runtime dependency contract: `requirements.txt`.
- Documentation and tests may be copied for audit, but are not required by the running web processes.
- Do not use recursive deletion. Deploy by explicit source paths and review the file list before applying. Until HCYT/NUPS and other active HOME_ONLY code is merged or retired, exclude their conflicting paths from any staging sync and do not declare the source cutover complete.

A non-destructive staged copy can use `rsync -a` without `--delete`, with explicit exclusions for local configuration/state (shown as shell variables so the real path is not stored here):

```bash
rsync -a \
  --exclude='/.git/' \
  --exclude='/configs/database.yaml' \
  --exclude='/configs/database.local.yaml' \
  --exclude='/configs/lineage_providers.local.yaml' \
  --exclude='/configs/tools.local.yaml' \
  --exclude='/configs/svn.yaml' \
  --exclude='/configs/migrate/clusters.json' \
  --exclude='/data/' --exclude='/logs/' --exclude='/runtime/' \
  --exclude='/tmp/' --exclude='/backup_sc/' --exclude='/send_files/' \
  --exclude='/venv/' --exclude='/resources/jars/*.jar' \
  --exclude='/resources/xlsx/' \
  "$RELEASE_ROOT/" "$PYTOOL_ROOT/"
```

This is deliberately not a green light to overwrite unresolved same-path business code. Review the staged diff and exclusions against the local inventory before the copy. Do not run with `--delete`.

## Files to preserve

Preserve these in place and outside Git:

- `configs/database.yaml`, any existing `configs/database.local.yaml`, `configs/lineage_providers.local.yaml`, `configs/tools.local.yaml`, `configs/svn.yaml`, `configs/migrate/clusters.json`, and all other local configuration.
- `data/`, `logs/`, `runtime/`, `tmp/`, `backup_sc/`, `send_files/`, virtual environments, generated outputs, local SQLite/database files, and internal workspaces.
- All JDBC/JAR binaries, including the operator-managed GaussDB driver. Never copy a bundled/local JAR into the repository.
- Historical resource data such as local `resources/xlsx/*.xlsx`; the repository-tracked mapping workbook is an empty scaffold and must not replace the historical populated mapping data. New local workbooks under this path are now ignored by Git.
- Any source or local recovery file marked `NEEDS_CONFIRMATION`, `UNKNOWN`, or `DEPRECATE_CANDIDATE` until its owner makes a decision.

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
