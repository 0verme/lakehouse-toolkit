"""Orchestrate local audit, DAP server dry-run, and localhost-only upsert."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

if __package__:
    from ._bootstrap import ensure_project_root_on_path
else:
    from _bootstrap import ensure_project_root_on_path

PROJECT_ROOT = Path(ensure_project_root_on_path())

from tools.field_mapping.collector import DEFAULT_PROGRESS_EVERY, collect_workspace
from tools.field_mapping.dap_client import (
    DEFAULT_BATCH_SIZE,
    FieldMappingApiClient,
    FieldMappingApiError,
    build_import_payload,
    is_local_write_url,
    split_batches,
    submit_batches,
    validate_mapping_item,
)
from tools.field_mapping.metadata_resolver import (
    MetadataInputError,
    MetadataResolver,
    load_metadata_snapshot,
    load_upstream_snapshot,
)
from tools.field_mapping.reporting import (
    new_report_directory,
    normalized_summary,
    write_audit_report,
)

EXECUTION_MODES = ("local-dry-run", "server-dry-run", "real-sync")


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect DWO → DWF field mappings and upsert through the DAP import API."
    )
    parser.add_argument("--directory", required=True, help="DWF project source root")
    parser.add_argument(
        "--metadata-json",
        required=True,
        help="whitelisted p_recv_dwf / p_schema_config JSON export",
    )
    parser.add_argument(
        "--upstreams-json",
        help="optional DAP upstream-systems snapshot for local dry-run only",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("DAP_API_BASE_URL", ""),
        help="DAP base URL; GET is used to resolve current upstream IDs",
    )
    parser.add_argument("--mode", choices=EXECUTION_MODES, default="local-dry-run")
    parser.add_argument(
        "--sql-dialect",
        default=os.environ.get("PYTOOLS_DAP_MAPPING_SQL_DIALECT", "mysql"),
    )
    parser.add_argument("--batch-size", type=positive_int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--progress-every",
        type=positive_int,
        default=DEFAULT_PROGRESS_EVERY,
        help="emit collector progress every N projects",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="prompt for DAP username/password; password is not echoed",
    )
    parser.add_argument(
        "--report-root", default=str(PROJECT_ROOT / "runtime" / "field_mapping_sync")
    )
    parser.add_argument(
        "--include-payload",
        action="store_true",
        help="write the constructed batches to gitignored report output",
    )
    return parser


def _runtime_client(
    args: argparse.Namespace, *, write_mode: bool
) -> FieldMappingApiClient:
    if not args.api_base_url:
        raise ValueError("--api-base-url or DAP_API_BASE_URL is required")
    cookie = os.environ.get("DAP_SESSION_COOKIE", "").strip()
    if args.login:
        if cookie:
            raise ValueError("use either DAP_SESSION_COOKIE or --login, not both")
        username = input("DAP username: ").strip()
        password = getpass.getpass("DAP password: ")
        client = FieldMappingApiClient(args.api_base_url)
        try:
            client.login(username, password)
        except Exception:
            client.close()
            raise
        finally:
            password = ""
        return client
    if write_mode and not cookie:
        raise ValueError(
            "provide DAP_SESSION_COOKIE or use --login for authenticated import"
        )
    return FieldMappingApiClient(args.api_base_url, session_cookie=cookie)


def _resolve_upstreams(args: argparse.Namespace, client: FieldMappingApiClient | None):
    if args.upstreams_json:
        if args.mode != "local-dry-run":
            raise ValueError("--upstreams-json is permitted only for local-dry-run")
        return load_upstream_snapshot(args.upstreams_json)
    if client is None:
        raise ValueError(
            "local-dry-run requires --upstreams-json or a DAP API URL for GET /api/upstreams/systems"
        )
    return client.get_upstream_systems()


def _filter_contract_items(audit):
    valid = []
    invalid_rows = []
    for item in audit.items:
        try:
            validate_mapping_item(item.to_contract())
        except (TypeError, ValueError) as error:
            invalid_rows.append(
                {
                    "reason": "contract_validation",
                    "sourceSystemIdentity": item.source_system_identity,
                    "sourceSystemId": item.source_system_id,
                    "sourceTable": item.source_table,
                    "targetTable": item.target_table,
                    "detail": str(error),
                }
            )
        else:
            valid.append(item)
    audit.items = tuple(valid)
    audit.unresolved.extend(invalid_rows)
    audit.summary["unresolved"]["contract_validation"] = len(invalid_rows)
    audit.summary["resolution"]["resolved_table_mappings"] = len(
        {item.identity for item in valid}
    )
    audit.summary["resolution"]["resolved_field_mappings"] = sum(
        len(item.fields) for item in valid
    )
    valid_identities = {item.dap_identity for item in valid}
    audit.resolved = [
        row
        for row in audit.resolved
        if (
            row.get("sourceSystemId"),
            str(row.get("sourceTable", "")).casefold(),
            str(row.get("targetTable", "")).casefold(),
        )
        in valid_identities
    ]


def _reconcile_dap(client: FieldMappingApiClient) -> dict[str, object]:
    stats_response = client.get_mapping_stats()
    stats = stats_response.get("data", stats_response)
    if not isinstance(stats, dict):
        raise FieldMappingApiError(
            "INVALID_RESPONSE_CONTRACT", "DAP stats response is invalid"
        )
    tables = client.get_mapping_tables()
    fields = client.get_mapping_fields()
    expected_tables = stats.get("sourceTableCount")
    expected_fields = stats.get("fieldCount")
    return {
        "stats": stats,
        "tables_read": len(tables),
        "fields_read": len(fields),
        "tables_match_stats": expected_tables == len(tables)
        if isinstance(expected_tables, int)
        else None,
        "fields_match_stats": expected_fields == len(fields)
        if isinstance(expected_fields, int)
        else None,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 500:
        parser.error("--batch-size must be between 1 and DAP's 500-item limit")
    if args.mode == "real-sync" and not is_local_write_url(args.api_base_url):
        print(
            "[blocked] real DAP writes are restricted to localhost/127.0.0.1",
            file=sys.stderr,
        )
        return 2
    if args.mode != "local-dry-run" and args.upstreams_json:
        parser.error("--upstreams-json is permitted only for local-dry-run")

    client: FieldMappingApiClient | None = None
    try:
        recv_dwf_rows, schema_rows = load_metadata_snapshot(args.metadata_json)
        needs_client = not args.upstreams_json or args.mode != "local-dry-run"
        if needs_client:
            client = _runtime_client(args, write_mode=args.mode != "local-dry-run")
        upstream_payload = _resolve_upstreams(args, client)
        resolver = MetadataResolver(recv_dwf_rows, schema_rows, upstream_payload)
        audit = collect_workspace(
            args.directory,
            resolver,
            dialect=args.sql_dialect,
            progress_every=args.progress_every,
        )
        _filter_contract_items(audit)
        audit.summary["execution_mode"] = args.mode
        # Validate every serialized request locally before any DAP import call.
        for batch in split_batches(audit.items, args.batch_size):
            build_import_payload(batch, dry_run=args.mode != "real-sync")
        report_dir = new_report_directory(args.report_root)
        write_audit_report(
            report_dir,
            audit,
            payload_batch_size=args.batch_size if args.include_payload else None,
            payload_dry_run=args.mode != "real-sync",
        )

        if args.mode != "local-dry-run" and audit.items:
            assert client is not None
            api_summary = submit_batches(
                audit.items,
                client,
                batch_size=args.batch_size,
                dry_run=args.mode == "server-dry-run",
            )
            audit.summary["api"] = {
                key: value for key, value in api_summary.items() if key != "failedItems"
            }
            audit.summary["failed"] = api_summary["failed"]
            audit.failed.extend(api_summary["failedItems"])
            if args.mode == "real-sync":
                try:
                    audit.summary["dap_reconciliation"] = _reconcile_dap(client)
                except FieldMappingApiError as error:
                    audit.summary["dap_reconciliation"] = {
                        "errorCode": error.code,
                        "message": error.message,
                    }
                    audit.summary["failed"] += 1
                    audit.failed.append(
                        {
                            "stage": "dap_reconciliation",
                            "errorCode": error.code,
                            "message": error.message,
                        }
                    )
            write_audit_report(
                report_dir,
                audit,
                payload_batch_size=args.batch_size if args.include_payload else None,
                payload_dry_run=args.mode != "real-sync",
            )

        print(
            json.dumps(normalized_summary(audit.summary), ensure_ascii=False, indent=2)
        )
        print(f"REPORT_DIR={report_dir}")
        return 1 if audit.summary.get("failed", 0) else 0
    except (MetadataInputError, FieldMappingApiError, OSError, ValueError) as error:
        print(f"[error] {error}", file=sys.stderr)
        return 2
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
