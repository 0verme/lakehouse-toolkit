"""Verify production Python sources in already checked-out SVN workspaces."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from shared.lineage.svn_inventory import (
    READ_ERROR,
    ROOT_NOT_DIRECTORY,
    ROOT_NOT_FOUND,
    SUCCESS,
    SVNInventoryConfigError,
    SVNProfile,
    SVNScanResult,
    build_svn_verification_report,
    load_svn_profiles,
    scan_svn_profile,
    write_json_report,
)

DEFAULT_OUTPUT = Path("artifacts/lineage_verification/svn_report.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scan local SVN working copies for validated production Python layouts "
            "without contacting SVN."
        )
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="local-only YAML containing svn_profiles",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="scan at most sample-limit Python files per profile",
    )
    parser.add_argument(
        "--sample-limit",
        type=_non_negative_int,
        default=20,
        help="maximum Python files scanned per profile in sample mode (default: 20)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="sanitized JSON report path",
    )
    return parser


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("must be a non-negative integer") from None
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _emit_progress(
    profile_name: str, processed: int, resolved: int, errors: int
) -> None:
    print(
        "stage=svn_scan_progress "
        f"profile={profile_name} processed={processed} "
        f"resolved={resolved} errors={errors}",
        flush=True,
    )


def _emit_profile_result(result: SVNScanResult) -> None:
    layer_text = ",".join(
        f"{layer}:{count}" for layer, count in result.layer_counts.items()
    )
    print(
        "stage=svn_scan "
        f"profile={result.profile_name} "
        f"environment={result.environment} "
        f"status={result.status} "
        f"matched_files={result.matched_program_files} "
        f"primary_resolved={result.primary_target_resolved} "
        f"primary_unresolved={result.primary_target_unresolved} "
        f"primary_rate={result.primary_resolved_rate:.2f}% "
        f"readable={result.readable_files} "
        f"read_failed={result.read_errors + result.decode_errors} "
        f"elapsed_ms={result.elapsed_ms:.2f} "
        f"layers={layer_text}",
        flush=True,
    )


def _scan_profile(
    profile: SVNProfile, *, sample_only: bool, sample_limit: int
) -> SVNScanResult:
    try:
        return scan_svn_profile(
            profile,
            sample_only=sample_only,
            sample_limit=sample_limit,
            progress_callback=_emit_progress,
        )
    except FileNotFoundError:
        return SVNScanResult.empty(profile, ROOT_NOT_FOUND)
    except NotADirectoryError:
        return SVNScanResult.empty(profile, ROOT_NOT_DIRECTORY)
    except OSError:
        return SVNScanResult.empty(profile, READ_ERROR)


def _load_profiles_or_report(config_path: Path) -> list[SVNProfile] | None:
    try:
        return load_svn_profiles(config_path)
    except SVNInventoryConfigError as exc:
        print(
            f"stage=config status=FAILED error=CONFIG_ERROR reason={exc.reason}",
            flush=True,
        )
        return None


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    profiles = _load_profiles_or_report(args.config)
    if profiles is None:
        return 2

    results: list[SVNScanResult] = []
    for profile in profiles:
        result = _scan_profile(
            profile,
            sample_only=args.sample_only,
            sample_limit=args.sample_limit,
        )
        results.append(result)
        _emit_profile_result(result)

    report = build_svn_verification_report(
        results,
        sample_only=args.sample_only,
        sample_limit=args.sample_limit,
    )
    try:
        write_json_report(report, args.output)
    except (OSError, TypeError, ValueError):
        print("stage=report status=FAILED error=REPORT_ERROR", flush=True)
        return 2

    return 0 if all(result.status == SUCCESS for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
