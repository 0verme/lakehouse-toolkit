# Field Mapping Resolver Windows UAT

This probe compares an unmodified checkout (`before`, normally the current main at PR base) with the candidate checkout using the same fixed 100-project source sample, metadata snapshots, Python executable, and benchmark helper. It is offline: `--upstreams-json` avoids DAP calls and no API writes are made.

The helper accepts `--repository-root` separately from its own script path so a copy from the candidate checkout can run against both code revisions. Point the variables below to the existing fixed probe sample and the same local, approved metadata snapshots; do not put credentials or SQL text in the command/output.

```powershell
$python = 'C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe'
$before = 'D:\PycharmProjects\lakehouse-toolkit-main-before'
$after = 'D:\PycharmProjects\lakehouse-toolkit-pr'
$probe = 'D:\PycharmProjects\lakehouse-toolkit\uat\field-mapping-probe-100'
$metadata = 'D:\PycharmProjects\lakehouse-toolkit\uat\metadata.json'
$upstreams = 'D:\PycharmProjects\lakehouse-toolkit\uat\upstreams.json'
$script = Join-Path $after 'tools\field_mapping\benchmark_resolver.py'

foreach ($repo in @($before, $after)) {
    Write-Host "=== Field Mapping probe: $repo ==="
    & $python $script `
        --repository-root $repo `
        --directory $probe `
        --metadata-json $metadata `
        --upstreams-json $upstreams `
        --progress-every 100
    if ($LASTEXITCODE -ne 0) { throw "Probe failed for $repo" }
}
```

Compare `audit_result_sha256` (must be identical), the full `audit_summary`, `elapsed_seconds`, `resolve_calls`, `resolve_seconds`, `resolver_percent`, `normalize_program_name_calls`, and `avg_resolve_ms`. The script emits summary/counters only, not SQL or metadata row contents.

The fixed 100-project baseline to preserve is:

- 100 projects, 104 Python files
- 96.99 seconds total; 1457 resolves; 93.89 seconds in resolver (96.8%)
- 10,966,199 `normalize_program_name` calls
- resolved: 24 projects, 25 table mappings, 781 field mappings
- unresolved: no_program=2, no_final_target=37, no_recv_dwf=11, no_schema_config=633, no_dwo_source=0, unknown_upstream_system=0, unsupported_sql=25, multi_source_field_unsupported=0
- conflicts: program_metadata_conflict=0, multiple_recv_plan_conflict=0, multiple_data_source_conflict=0, schema_match_conflict=7, upstream_system_conflict=0, field_mapping_conflict=0

Expect a large reduction in resolver time and normalization calls without any digest, summary, or audit-evidence change. The preferred `<20s` total is a target, not a CI wall-clock gate. After merge, run this fixed 100-project A/B probe in the Windows UAT environment; do not proceed directly to the 4020-project full local dry-run, server-dry-run, or real-sync.
