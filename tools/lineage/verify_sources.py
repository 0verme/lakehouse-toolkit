"""Run the sanitized lineage source verification harness."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from shared.lineage.providers import (
    LineageConfigError,
    LineageDependencyError,
    load_mysql_process_profiles,
)
from shared.lineage.verification import (  # pyright: ignore[reportMissingImports]
    DEFAULT_REPORT_PATH,
    SnapshotStatus,
    VerificationErrorCode,
    build_verification_report,
    verify_mysql_profiles,
    verify_production_provider,
    write_json_report,
    write_markdown_report,
)

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify configured lineage metadata sources and write a sanitized report."
        )
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="local-only lineage provider YAML; no default or fallback is used",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="JSON output under artifacts/lineage_verification/",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        help="optional Markdown output under artifacts/lineage_verification/",
    )
    parser.add_argument(
        "--sample-limit",
        type=_non_negative_int,
        default=20,
        help="bounded sample size; default: 20",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="stop after the bounded sample and mark snapshot PARTIAL",
    )
    parser.add_argument(
        "--include-production",
        action="store_true",
        help="explicitly invoke the legacy PROD metadata loader",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging()

    try:
        if not args.config.exists():
            _log_config_failure("CONFIG_NOT_FOUND")
            return 2
        if not args.config.is_file():
            _log_config_failure("CONFIG_READ_ERROR")
            return 2
    except OSError:
        _log_config_failure("CONFIG_READ_ERROR")
        return 2

    try:
        profiles = load_mysql_process_profiles(args.config)
    except Exception as exc:
        if isinstance(exc, LineageDependencyError):
            _log_dependency_failure(exc)
        elif isinstance(exc, LineageConfigError):
            _log_config_failure(
                exc.code,
                reason=exc.reason,
                line=exc.line,
                column=exc.column,
            )
        elif isinstance(exc, FileNotFoundError):
            _log_config_failure("CONFIG_NOT_FOUND")
        elif isinstance(exc, OSError):
            _log_config_failure("CONFIG_READ_ERROR")
        elif isinstance(exc, ModuleNotFoundError) and exc.name == "yaml":
            _log_dependency_failure(LineageDependencyError("PyYAML"))
        else:
            _log_unexpected_config_failure(exc)
        return 2
    if not profiles:
        _log_config_failure(
            "CONFIG_INVALID",
            reason="mysql_process_profiles must contain at least one profile",
        )
        return 2

    results = verify_mysql_profiles(
        profiles,
        sample_limit=args.sample_limit,
        sample_only=args.sample_only,
    )
    production = (
        verify_production_provider(
            sample_limit=args.sample_limit,
            sample_only=args.sample_only,
        )
        if args.include_production
        else None
    )
    report = build_verification_report(
        results,
        production_provider=production,
    )

    for result in results:
        LOGGER.info(
            "profile=%s environment=%s stage=metadata status=%s rows=%s elapsed_ms=%s",
            result.profile_name,
            result.environment,
            result.error_code,
            result.row_count,
            result.elapsed_ms,
        )
    if production is not None:
        LOGGER.info(
            "stage=production_provider status=%s rows=%s elapsed_ms=%s",
            production.error_code,
            production.row_count,
            production.elapsed_ms,
        )

    try:
        write_json_report(report, args.output)
        if args.markdown_output is not None:
            write_markdown_report(report, args.markdown_output)
    except (OSError, ValueError):
        LOGGER.error("stage=report status=FAILED")
        return 2

    return 0 if _report_is_complete(report) else 1


def _report_is_complete(report) -> bool:
    if any(
        result.error_code != VerificationErrorCode.SUCCESS.value
        or result.snapshot_status != SnapshotStatus.COMPLETE.value
        for result in report.profiles
    ):
        return False
    production = report.production_provider
    return production.error_code in {
        VerificationErrorCode.SUCCESS.value,
        VerificationErrorCode.NOT_RUN.value,
    } and production.snapshot_status in {
        SnapshotStatus.COMPLETE.value,
        SnapshotStatus.NOT_RUN.value,
    }


def _log_dependency_failure(error: LineageDependencyError) -> None:
    LOGGER.error(
        'stage=dependency status=FAILED dependency=%s '
        'hint="run: python -m pip install -r requirements.txt"',
        error.dependency,
    )


def _log_config_failure(
    error: str,
    *,
    reason: str | None = None,
    line: int | None = None,
    column: int | None = None,
) -> None:
    fields = [f"stage=config status=FAILED error={error}"]
    if line is not None:
        fields.append(f"line={line}")
    if column is not None:
        fields.append(f"column={column}")
    if reason:
        fields.append(f"reason={reason}")
    LOGGER.error(" ".join(fields))


def _log_unexpected_config_failure(error: Exception) -> None:
    LOGGER.error(
        "stage=config status=FAILED error=UNEXPECTED exception=%s",
        type(error).__name__,
    )


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("must be a non-negative integer") from None
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


if __name__ == "__main__":
    raise SystemExit(main())
