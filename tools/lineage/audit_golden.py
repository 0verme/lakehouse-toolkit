"""Audit Golden Corpus 的 JSONL 抽样、校验和 metrics CLI。"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from shared.lineage.audit_golden import (
    CORPUS_VERSION,
    calculate_metrics,
    read_candidate_manifest,
    read_corpus,
    sample_candidates,
    write_corpus,
)


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("must be a non-negative integer") from None
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.lineage.audit_golden",
        description="Sample, validate, and calculate metrics for a sanitized audit corpus.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample", help="sample a candidate JSONL manifest")
    sample.add_argument("--input", required=True, type=Path)
    sample.add_argument("--output", required=True, type=Path)
    sample.add_argument("--seed", required=True, type=int)
    sample.add_argument("--per-type", type=_non_negative_int, default=10)
    sample.add_argument("--negative-control-count", type=_non_negative_int, default=1)
    sample.add_argument("--sampling-group", default="default")
    sample.add_argument("--corpus-version", default=CORPUS_VERSION)

    validate = subparsers.add_parser("validate", help="validate a corpus JSONL manifest")
    validate.add_argument("--input", required=True, type=Path)
    validate.add_argument(
        "--require-labels",
        action="store_true",
        help="also require fact labels before annotation is considered complete",
    )

    metrics = subparsers.add_parser("metrics", help="calculate corpus metrics")
    metrics.add_argument("--input", required=True, type=Path)
    metrics.add_argument("--output", type=Path)

    return parser


def _safe_metrics_path(output_path: Path) -> Path:
    root = (Path.cwd() / "artifacts" / "lineage_audit_golden").resolve()
    resolved = (
        output_path if output_path.is_absolute() else Path.cwd() / output_path
    ).resolve()
    if root not in resolved.parents:
        raise ValueError("metrics output must stay under artifacts/lineage_audit_golden")
    return resolved


def _write_metrics(path: Path | None, value: dict[str, object]) -> None:
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if path is None:
        sys.stdout.write(rendered)
        return
    safe_path = _safe_metrics_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    safe_path.write_text(rendered, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "sample":
            candidates = read_candidate_manifest(args.input)
            samples = sample_candidates(
                candidates,
                seed=args.seed,
                per_issue_type=args.per_type,
                negative_control_count=args.negative_control_count,
                sampling_group=args.sampling_group,
                corpus_version=args.corpus_version,
            )
            write_corpus(args.output, samples)
            return 0
        if args.command == "validate":
            samples = read_corpus(args.input, require_labels=args.require_labels)
            print(
                json.dumps(
                    {
                        "valid": True,
                        "sample_count": len(samples),
                        "require_labels": args.require_labels,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "metrics":
            report = calculate_metrics(read_corpus(args.input, require_labels=True))
            _write_metrics(args.output, report.to_dict())
            return 0
    except (OSError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
