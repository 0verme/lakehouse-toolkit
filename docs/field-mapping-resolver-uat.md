# Field Mapping Resolver Windows UAT

This UAT validates the recv-namespace upstream identity model after the resolver
refactor. It is offline: `--upstreams-json` avoids DAP calls and no API writes are
made. Metadata snapshots stay outside Git and must never include credentials or
SQL text in committed files.

## Full 4020-project local dry-run

Run the normal collector entry point against the complete DWF source root. This
compares the candidate checkout against the previous main checkout; use one
`--report-root` per run so the audit bundles stay separate.

```powershell
$python = 'C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe'
$before = 'D:\PycharmProjects\lakehouse-toolkit-main-before'
$after = 'D:\PycharmProjects\lakehouse-toolkit-pr'
$source = '<path-to-DWS_DWF>'
$metadata = '<secure-local-path>\field-mapping-metadata.json'
$upstreams = '<secure-local-path>\upstreams.json'

& $python (Join-Path $after 'jobs\crontab\sync_ods_dwf_field_mappings.py') `
    --directory $source `
    --metadata-json $metadata `
    --upstreams-json $upstreams `
    --mode local-dry-run `
    --report-root (Join-Path $after 'runtime\field_mapping_sync_uat') `
    --progress-every 100
```

Inspect the generated `summary.json`, `resolved.csv`, `unresolved.csv`, and
`conflicts.csv` and compare against the previous checkout run.

## Expected classification change

The old model split `DWO_<db_schema>_<source_table>` from
`p_schema_config.db_schema`, so `no_schema_config` and `schema_match_conflict`
dominated the schema-related diagnostics. The new model uses
`recv_plan + full data_source -> recv namespace -> DWO_<recv_namespace>_<source_table>`
and reports:

- unresolved: `recv_namespace_unresolved`, `no_recv_namespace_match`
- conflict: `multiple_recv_namespace_conflict`

`program_metadata_conflict`, `multiple_data_source_conflict`, and
`schema_match_conflict` are no longer produced by the resolver identity chain.
`program_metadata_conflict` intentionally remains absent; `ods_job_name` stays
auxiliary evidence only.

The probe baseline from the internal full 4020 walk was:

- about 1996 unique schema-related mappings; about 1929 (96.6%) are explainable by
  the new model with a unique recv namespace identity;
- remaining boundaries: 66 `NONE` (`no_recv_namespace_match`) and 1 `MULTIPLE`
  (`multiple_recv_namespace_conflict`, the `DWO_DTSELL_PUB_OPER_LOG` case);
- about 626 / 627 existing resolved mappings are explainable by the new model;
- the only known regression is `PLAN_SA_RECV_WD_DEFENSOR_WD_DEFENSOR_DAY` with
  physical `DWO_DEFENSOR_REPORT_REQUEST`, which is allowed to degrade to
  `UNRESOLVED` / diagnostic because `db_schema` no longer provides a fallback.

Do not hardcode these counts into CI. Use them only as an order-of-magnitude check
after the Windows UAT; the authoritative result is the rerun on the real metadata
snapshot.

## Performance

The resolver precomputes the logical-target index, per-record recv namespace, and
normalized program evidence once in `MetadataResolver.__init__`; `resolve()` only
walks the candidates of the requested logical target and caches results. For the
offline benchmark helper use:

```powershell
& $python (Join-Path $after 'tools\field_mapping\benchmark_resolver.py') `
    --repository-root $after `
    --directory $source `
    --metadata-json $metadata `
    --upstreams-json $upstreams `
    --progress-every 100
```

Compare `elapsed_seconds`, `resolve_calls`, `resolve_seconds`,
`resolver_percent`, `normalize_program_name_calls`,
`normalize_logical_target_calls`, `derive_recv_namespace_calls`, and
`avg_resolve_ms` with the previous main baseline. The audit digest is expected to
change because the resolver model changed; only the performance counters and the
reason distribution should be compared across revisions.
