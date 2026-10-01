"""Export one active toolkit business lineage scope as DAP Contract V1 JSON.

This command is deliberately local-only: it never authenticates to or sends
requests to DAP.  ``--dry-run`` reports the exact export and DAP capacity checks.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from shared.lineage.dap_contract import (
    DAPLineageCapacityLimits,
    DAPLineagePreflight,
    build_dap_lineage_contract,
    preflight_dap_lineage_contract,
    serialize_dap_lineage_contract,
)
from shared.lineage.environment_scope import (
    LineageEnvironmentScopeError,
    load_lineage_environment_scope_resolver,
)
from shared.lineage.materialization_dws import (
    DWSActiveSnapshotMetadata,
    DWSBusinessEdgeRow,
    DWSMaterializationStore,
)


class DAPExportNotReadyError(RuntimeError):
    """The active toolkit snapshot cannot safely become a DAP replace payload."""


class ActiveBusinessLineageReader(Protocol):
    def get_active_snapshot_metadata(self) -> DWSActiveSnapshotMetadata | None: ...

    def read_business_rows(
        self,
        *,
        batch_id: str,
        active_only: bool,
        environment: str,
        source_profile: str,
    ) -> tuple[DWSBusinessEdgeRow, ...]: ...


@dataclass(frozen=True, slots=True)
class DAPLineageExport:
    contract: dict[str, object]
    serialized: str
    preflight: DAPLineagePreflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export one active toolkit business lineage scope as DAP Contract V1; "
            "this command never publishes to DAP."
        )
    )
    parser.add_argument(
        "--environment",
        required=True,
        metavar="ENVIRONMENT",
        help="exact lineage environment to export; no environment is inferred",
    )
    parser.add_argument(
        "--source-profile",
        required=True,
        metavar="PROFILE",
        help="exact business-lineage source profile to export",
    )
    parser.add_argument(
        "--dws-profile",
        default=None,
        metavar="DWS_PROFILE",
        help=(
            "DWS connection profile; otherwise resolve from the exact configured "
            "environment scope"
        ),
    )
    parser.add_argument(
        "--scope-config",
        type=Path,
        default=None,
        metavar="PATH",
        help="optional lineage environment-scope YAML used to resolve DWS profile",
    )
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument(
        "--output",
        type=Path,
        metavar="FILE",
        help=(
            "write a deterministic DAP V1 JSON file "
            "(existing files are not overwritten)"
        ),
    )
    output.add_argument(
        "--dry-run",
        action="store_true",
        help="print statistics and DAP compatibility preflight without writing JSON",
    )
    return parser


def _resolve_dws_profile(
    *,
    environment: str,
    explicit_profile: str | None,
    scope_config: Path | None,
) -> str:
    if explicit_profile is not None and explicit_profile.strip():
        return explicit_profile.strip()
    try:
        resolver = load_lineage_environment_scope_resolver(config_path=scope_config)
        return resolver.resolve(environment).dws_profile
    except LineageEnvironmentScopeError as error:
        raise DAPExportNotReadyError(
            "DWS profile unavailable: pass --dws-profile or configure this exact "
            "environment in the lineage scope config"
        ) from error


def export_active_business_lineage(
    *,
    reader: ActiveBusinessLineageReader,
    environment: str,
    source_profile: str,
    limits: DAPLineageCapacityLimits,
) -> DAPLineageExport:
    """Read and project exactly one complete active environment/profile scope."""

    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("environment must be a non-empty string")
    if not isinstance(source_profile, str) or not source_profile.strip():
        raise ValueError("source_profile must be a non-empty string")
    environment = environment.strip()
    source_profile = source_profile.strip()
    active = reader.get_active_snapshot_metadata()
    if active is None:
        raise DAPExportNotReadyError(
            "no active toolkit lineage snapshot is available"
        )
    if not active.is_active:
        raise DAPExportNotReadyError("active toolkit snapshot metadata is not active")
    if not active.complete_snapshot or active.snapshot_mode != "FULL":
        raise DAPExportNotReadyError(
            "active toolkit batch is not a complete FULL snapshot; "
            "DAP V1 replace requires a complete snapshot"
        )
    if (environment, source_profile) not in active.snapshot_scope:
        raise DAPExportNotReadyError(
            "requested environment/source-profile is not declared in the active batch"
        )

    rows = reader.read_business_rows(
        batch_id=active.batch_id,
        active_only=True,
        environment=environment,
        source_profile=source_profile,
    )
    confirmed_active = reader.get_active_snapshot_metadata()
    if (
        confirmed_active is None
        or confirmed_active.batch_id != active.batch_id
        or not confirmed_active.is_active
        or not confirmed_active.complete_snapshot
        or confirmed_active.snapshot_mode != "FULL"
    ):
        raise DAPExportNotReadyError(
            "active toolkit batch changed while reading; retry against one stable batch"
        )

    contract = build_dap_lineage_contract(
        rows,
        active_snapshot=active,
        environment=environment,
        source_profile=source_profile,
    )
    preflight = preflight_dap_lineage_contract(
        contract,
        environment=environment,
        source_profile=source_profile,
        toolkit_batch_id=active.batch_id,
        limits=limits,
    )
    return DAPLineageExport(
        contract=contract,
        serialized=serialize_dap_lineage_contract(contract),
        preflight=preflight,
    )


def _safe_output_path(output: Path | None) -> Path | None:
    if output is None:
        return None
    if "\x00" in str(output):
        raise ValueError("--output must not contain a NUL character")
    root = Path.cwd().resolve()
    candidate = output.expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "--output must stay inside the current working directory"
        ) from error
    if candidate.exists():
        raise FileExistsError("--output already exists; refusing to overwrite it")
    return candidate


def _write_payload(output: Path, serialized: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as handle:
        handle.write(serialized.encode("utf-8"))


def _print_summary(preflight: DAPLineagePreflight) -> None:
    summary = preflight.as_dict()
    print(str(summary["status"]))
    for key in (
        "environment",
        "source_profile",
        "toolkit_batch_id",
        "table_nodes",
        "task_nodes",
        "total_nodes",
        "business_edges",
        "dap_edges",
        "diagnostic_count",
        "payload_bytes",
        "payload_megabytes",
        "contract_version",
    ):
        value = summary[key]
        if key == "payload_megabytes":
            value = f"{value:.6f}"
        print(f"{key}: {value}")
    capacity = summary["capacity"]
    assert isinstance(capacity, dict)
    for name in ("nodes", "edges", "payload_bytes"):
        values = capacity[name]
        assert isinstance(values, dict)
        state = "PASS" if values["used"] <= values["limit"] else "OVER"
        print(f"capacity_{name}: {values['used']}/{values['limit']} {state}")
    for error in preflight.errors:
        print(f"preflight_error: {error}")


def cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        output = _safe_output_path(args.output)
        dws_profile = _resolve_dws_profile(
            environment=args.environment,
            explicit_profile=args.dws_profile,
            scope_config=args.scope_config,
        )
        store = DWSMaterializationStore(profile=dws_profile)
        exported = export_active_business_lineage(
            reader=store,
            environment=args.environment,
            source_profile=args.source_profile,
            limits=DAPLineageCapacityLimits(),
        )
        _print_summary(exported.preflight)
        if not exported.preflight.ready:
            return 2
        if output is not None:
            _write_payload(output, exported.serialized)
            print(f"output: {output}")
        return 0
    except DAPExportNotReadyError as error:
        print(f"NON-READY: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        # Avoid printing database driver messages that could contain connection
        # details; the class name is sufficient for a safe operator diagnostic.
        print(f"ERROR {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
