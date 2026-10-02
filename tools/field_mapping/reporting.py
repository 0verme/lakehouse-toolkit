from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .dap_client import build_import_payload, split_batches
from .models import AuditResult

SUMMARY_DEFAULTS: dict[str, Any] = {
    "project_count": 0,
    "python_file_count": 0,
    "dwo_physical_table_count": 0,
    "metadata": {
        "recv_dwf_rows": 0,
        "schema_config_rows": 0,
        "upstream_system_count": 0,
    },
    "resolution": {
        "resolved_projects": 0,
        "resolved_table_mappings": 0,
        "resolved_field_mappings": 0,
    },
    "unresolved": {
        "no_program": 0,
        "no_final_target": 0,
        "no_recv_dwf": 0,
        "no_schema_config": 0,
        "no_dwo_source": 0,
        "unknown_upstream_system": 0,
        "unsupported_sql": 0,
        "multi_source_field_unsupported": 0,
    },
    "conflict": {
        "program_metadata_conflict": 0,
        "multiple_recv_plan_conflict": 0,
        "multiple_data_source_conflict": 0,
        "schema_match_conflict": 0,
        "upstream_system_conflict": 0,
        "field_mapping_conflict": 0,
    },
    "failed": 0,
}


def new_report_directory(root: str | Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(root).expanduser() / stamp
    suffix = 1
    while path.exists():
        path = Path(root).expanduser() / f"{stamp}-{suffix:02d}"
        suffix += 1
    path.mkdir(parents=True, exist_ok=False)
    return path


def normalized_summary(summary: dict[str, Any]) -> dict[str, Any]:
    result = json.loads(json.dumps(SUMMARY_DEFAULTS))
    for key, value in summary.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key].update(value)
        else:
            result[key] = value
    return result


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    preferred = (
        "reason",
        "sourceSystemIdentity",
        "sourceSystemId",
        "dataSource",
        "project",
        "program",
        "physicalSourceTable",
        "dbSchema",
        "sourceTable",
        "sourceField",
        "targetTable",
        "targetField",
        "fieldOrder",
        "mappingRule",
        "evidence",
        "detail",
    )
    keys = set().union(*(row.keys() for row in rows)) if rows else set()
    columns = [name for name in preferred if name in keys]
    columns.extend(sorted(keys - set(columns)))
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=columns or ["reason"], extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in row.items()})


def _csv_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return "" if value is None else str(value)


def write_audit_report(
    report_dir: str | Path,
    audit: AuditResult,
    *,
    payload_batch_size: int | None = None,
    payload_dry_run: bool = True,
) -> Path:
    directory = Path(report_dir).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "summary.json").write_text(
        json.dumps(normalized_summary(audit.summary), ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )
    _write_csv(directory / "resolved.csv", audit.resolved)
    _write_csv(directory / "unresolved.csv", audit.unresolved)
    _write_csv(directory / "conflicts.csv", audit.conflicts)
    if audit.failed:
        _write_csv(directory / "failed.csv", audit.failed)
    if payload_batch_size is not None and audit.items:
        payload = {
            "executionMode": "local-dry-run"
            if payload_dry_run
            else "server-dry-run/real-sync",
            "batches": [
                build_import_payload(batch, dry_run=payload_dry_run)
                for batch in split_batches(audit.items, payload_batch_size)
            ],
        }
        (directory / "payload.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return directory


__all__ = ["new_report_directory", "normalized_summary", "write_audit_report"]
