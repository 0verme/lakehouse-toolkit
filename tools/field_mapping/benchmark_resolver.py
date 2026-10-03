"""Offline A/B probe for Field Mapping resolver performance and audit equivalence."""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an offline Field Mapping collector probe against a selected "
            "repository revision and report resolver counters plus AuditResult digest."
        )
    )
    parser.add_argument("--repository-root", required=True)
    parser.add_argument("--directory", required=True, help="fixed source-code probe")
    parser.add_argument("--metadata-json", required=True)
    parser.add_argument("--upstreams-json", required=True)
    parser.add_argument("--sql-dialect", default="mysql")
    parser.add_argument("--progress-every", type=_positive_int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    repository_root = Path(args.repository_root).expanduser().resolve()
    if not repository_root.is_dir():
        raise SystemExit("repository root must be an existing directory")
    sys.path.insert(0, str(repository_root))

    # Import only after adding the requested code root, so this one probe script can
    # compare an unmodified checkout and the candidate checkout with identical logic.
    from tools.field_mapping import collector as collector_module
    from tools.field_mapping import metadata_resolver as resolver_module

    recv_rows, schema_rows = resolver_module.load_metadata_snapshot(args.metadata_json)
    upstream_payload = resolver_module.load_upstream_snapshot(args.upstreams_json)
    counters = {
        "resolve_calls": 0,
        "resolve_seconds": 0.0,
        "normalize_program_name_calls": 0,
        "normalize_logical_target_calls": 0,
        "derive_recv_namespace_calls": 0,
    }

    original_program_normalizer = resolver_module.normalize_program_name
    original_target_normalizer = resolver_module.normalize_logical_target
    original_recv_namespace_resolver = resolver_module.derive_recv_namespace
    original_collector_target_normalizer = collector_module.normalize_logical_target

    def count_program_name(value: str) -> str:
        counters["normalize_program_name_calls"] += 1
        return original_program_normalizer(value)

    def count_logical_target(value: str) -> str:
        counters["normalize_logical_target_calls"] += 1
        return original_target_normalizer(value)

    def count_recv_namespace(recv_plan: str, data_source: str) -> str | None:
        counters["derive_recv_namespace_calls"] += 1
        return original_recv_namespace_resolver(recv_plan, data_source)

    resolver_module.normalize_program_name = count_program_name
    resolver_module.normalize_logical_target = count_logical_target
    resolver_module.derive_recv_namespace = count_recv_namespace
    collector_module.normalize_logical_target = count_logical_target
    started_at = time.perf_counter()
    try:
        resolver = resolver_module.MetadataResolver(
            recv_rows, schema_rows, upstream_payload
        )
        original_resolve = resolver.resolve

        def timed_resolve(
            *, target: str, program_names, physical_source: str
        ):
            counters["resolve_calls"] += 1
            resolve_started_at = time.perf_counter()
            try:
                return original_resolve(
                    target=target,
                    program_names=program_names,
                    physical_source=physical_source,
                )
            finally:
                counters["resolve_seconds"] += (
                    time.perf_counter() - resolve_started_at
                )

        resolver.resolve = timed_resolve
        collect_options: dict[str, Any] = {"dialect": args.sql_dialect}
        if "progress_every" in inspect.signature(
            collector_module.collect_workspace
        ).parameters:
            collect_options["progress_every"] = args.progress_every
        audit = collector_module.collect_workspace(
            args.directory, resolver, **collect_options
        )
        elapsed_seconds = time.perf_counter() - started_at
    finally:
        resolver_module.normalize_program_name = original_program_normalizer
        resolver_module.normalize_logical_target = original_target_normalizer
        resolver_module.derive_recv_namespace = original_recv_namespace_resolver
        collector_module.normalize_logical_target = original_collector_target_normalizer

    serialized_audit = json.dumps(
        asdict(audit), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    result = {
        "repository_root": str(repository_root),
        "elapsed_seconds": round(elapsed_seconds, 3),
        **{
            key: round(value, 6) if key == "resolve_seconds" else value
            for key, value in counters.items()
        },
        "avg_resolve_ms": round(
            counters["resolve_seconds"] * 1000 / counters["resolve_calls"], 3
        )
        if counters["resolve_calls"]
        else 0.0,
        "resolver_percent": round(
            counters["resolve_seconds"] * 100 / elapsed_seconds, 2
        )
        if elapsed_seconds
        else 0.0,
        "audit_result_sha256": hashlib.sha256(serialized_audit).hexdigest(),
        "audit_summary": audit.summary,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
