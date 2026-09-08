from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from shared.lineage.audit import audit_program_physical_dag, compute_lineage_issue_stable_key
from shared.lineage.audit_golden import (
    AuditCandidate,
    BusinessDispositionLabel,
    CorpusFormatError,
    CorpusSample,
    DuplicateSampleError,
    FactLabel,
    UnknownIssueTypeError,
    calculate_metrics,
    candidate_from_issue,
    compute_golden_fingerprint,
    negative_control_candidate,
    read_corpus,
    sample_candidates,
    summarize_issue_evidence,
    validate_corpus,
    write_candidate_manifest,
    write_corpus,
)
from shared.lineage.domain import IssueType, LineageIssue, ProgramSource
from shared.lineage.physical_dag import build_program_physical_dag
from tests.fixtures.lineage.audit_golden_programs import SYNTHETIC_PROGRAMS
from tests.fixtures.lineage.phase4_audit_programs import (
    CYCLE_PROGRAM,
    NORMAL_PROGRAM,
    ORPHAN_BRANCH_PROGRAM,
    SELF_REFERENCE_PROGRAM,
)
from tools.lineage import audit_golden as audit_golden_cli

EXPECTED_TARGET = "DWA.DEMO_RESULT"
OBSERVED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def build_dag(script_code: str, *, expected_target: str | None = EXPECTED_TARGET, name: str):
    return build_program_physical_dag(
        ProgramSource(
            environment="DEV",
            source_profile="synthetic_fixture",
            program_name=name,
            script_code=script_code,
            expected_target=expected_target,
        )
    )


def manual_sample(
    name: str,
    issue_type: IssueType | None,
    label: FactLabel,
    *,
    accepted: bool = False,
    negative_control: bool = False,
) -> CorpusSample:
    return CorpusSample(
        sample_id=f"manual-{name}",
        issue_type=issue_type,
        fact_label=label,
        business_disposition_label=(
            BusinessDispositionLabel.ACCEPTED if accepted else None
        ),
        fingerprint=compute_golden_fingerprint(f"manual:{name}"),
        evidence_summary=summarize_issue_evidence(
            {"statement_indices": [0], "expected_target": "redacted"}
        ),
        annotation_reason="synthetic_annotation",
        source_kind="synthetic_fixture",
        corpus_version="audit-golden-v1",
        negative_control=negative_control,
    )


def synthetic_candidates() -> tuple[AuditCandidate, ...]:
    candidates: list[AuditCandidate] = []
    for name, script in SYNTHETIC_PROGRAMS.items():
        expected_target = None if name in {"self_reference", "cycle_detected"} else EXPECTED_TARGET
        result = audit_program_physical_dag(
            build_dag(script, expected_target=expected_target, name=f"DEMO_{name.upper()}"),
            observed_at=OBSERVED_AT,
            batch_id="synthetic-batch",
        )
        candidates.extend(
            candidate_from_issue(issue, source_kind="synthetic_audit")
            for issue in result.issues
        )
        if name == "normal_negative_control":
            if result.issues:
                raise AssertionError("normal synthetic program must be a negative control")
            candidates.append(
                negative_control_candidate(
                    "synthetic:normal_negative_control",
                    source_kind="synthetic_negative_control",
                )
            )

    broken = LineageIssue(
        environment="DEV",
        source_profile="synthetic_fixture",
        program_name="DEMO_BROKEN_BRANCH",
        issue_type=IssueType.LINEAGE_BRANCH_BROKEN,
        severity="HIGH",
        message="synthetic transition evidence",
        branch_sink="TMP.BROKEN",
        evidence={
            "expected_target": EXPECTED_TARGET,
            "branch_sink": "TMP.BROKEN",
            "previous_valid_target": True,
        },
        first_seen_at=OBSERVED_AT,
        last_seen_at=OBSERVED_AT,
        stable_key=compute_lineage_issue_stable_key(
            "DEV",
            "synthetic_fixture",
            "DEMO_BROKEN_BRANCH",
            IssueType.LINEAGE_BRANCH_BROKEN,
            branch_sink="TMP.BROKEN",
        ),
    )
    candidates.append(candidate_from_issue(broken, source_kind="synthetic_history"))
    return tuple(candidates)


class AuditGoldenCorpusTests(unittest.TestCase):
    def test_current_issue_type_inventory_includes_history_derived_type(self):
        self.assertEqual(
            {issue_type.value for issue_type in IssueType},
            {
                "ORPHAN_BRANCH",
                "MULTI_SINK_CANDIDATE",
                "TARGET_NOT_FOUND",
                "TARGET_MISMATCH",
                "CYCLE_DETECTED",
                "SELF_REFERENCE",
                "LINEAGE_BRANCH_BROKEN",
            },
        )

    def test_deterministic_sampling_is_order_independent_and_replayable(self):
        candidates = synthetic_candidates()
        first = sample_candidates(
            candidates,
            seed=35,
            per_issue_type=1,
            negative_control_count=1,
            sampling_group="synthetic",
        )
        second = sample_candidates(
            reversed(candidates),
            seed=35,
            per_issue_type=1,
            negative_control_count=1,
            sampling_group="synthetic",
        )

        self.assertEqual(
            [sample.to_dict() for sample in first],
            [sample.to_dict() for sample in second],
        )
        self.assertEqual(
            {sample.issue_type for sample in first if sample.issue_type is not None},
            set(IssueType),
        )
        self.assertEqual(
            sum(sample.negative_control for sample in first),
            1,
        )

    def test_fixed_seed_changes_selection_identity_without_using_builtin_hash(self):
        candidates = tuple(
            AuditCandidate(
                fingerprint=compute_golden_fingerprint(f"candidate:{index}"),
                issue_type=IssueType.ORPHAN_BRANCH,
                evidence_summary={},
                source_kind="synthetic_fixture",
            )
            for index in range(5)
        )
        seed_one = sample_candidates(
            candidates,
            seed=1,
            per_issue_type=2,
            sampling_group="seed-test",
        )
        seed_two = sample_candidates(
            candidates,
            seed=2,
            per_issue_type=2,
            sampling_group="seed-test",
        )

        self.assertNotEqual(
            [sample.sample_id for sample in seed_one],
            [sample.sample_id for sample in seed_two],
        )
        self.assertEqual(len(seed_one), 2)
        self.assertEqual(len(seed_two), 2)

    def test_sample_all_when_a_stratum_is_smaller_than_requested(self):
        candidates = (
            AuditCandidate(
                fingerprint=compute_golden_fingerprint("only-one"),
                issue_type=IssueType.CYCLE_DETECTED,
                evidence_summary={},
                source_kind="synthetic_fixture",
            ),
        )
        samples = sample_candidates(candidates, seed=35, per_issue_type=10)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].issue_type, IssueType.CYCLE_DETECTED)

    def test_candidate_fingerprint_and_evidence_summary_are_stable_and_sanitized(self):
        dag = build_dag(ORPHAN_BRANCH_PROGRAM, name="DEMO_REAL_NAME_NOT_EXPORTED")
        issue = audit_program_physical_dag(dag, observed_at=OBSERVED_AT).issues[0]
        first = candidate_from_issue(issue)
        second = candidate_from_issue(issue)

        self.assertEqual(first.fingerprint, second.fingerprint)
        encoded = json.dumps(first.to_dict(), ensure_ascii=False, sort_keys=True)
        self.assertNotIn("DEMO_REAL_NAME_NOT_EXPORTED", encoded)
        self.assertNotIn("DWA.DEMO_RESULT", encoded)
        self.assertNotIn("script_code", encoded)
        self.assertTrue(
            set(first.evidence_summary).issubset(
                {
                    "evidence_key_count",
                    "statement_index_count",
                    "edge_count",
                    "node_count",
                    "sink_count",
                    "formal_sink_count",
                    "temporary_sink_count",
                    "written_target_count",
                    "entry_source_count",
                    "has_expected_target",
                    "expected_target_written",
                    "expected_target_is_sink",
                    "has_previous_valid_target",
                    "has_previous_broken_evidence",
                }
            )
        )

    def test_each_supported_label_serializes_and_round_trips(self):
        samples = (
            manual_sample("tp", IssueType.ORPHAN_BRANCH, FactLabel.TRUE_POSITIVE, accepted=True),
            manual_sample("fp", IssueType.ORPHAN_BRANCH, FactLabel.FALSE_POSITIVE),
            manual_sample("ambiguous", IssueType.ORPHAN_BRANCH, FactLabel.AMBIGUOUS),
            manual_sample(
                "no-issue",
                None,
                FactLabel.NO_ISSUE,
                negative_control=True,
            ),
        )
        for sample in samples:
            with self.subTest(sample=sample.sample_id):
                decoded = CorpusSample.from_dict(sample.to_dict())
                self.assertEqual(decoded, sample)

        self.assertEqual(
            samples[0].to_dict()["business_disposition_label"],
            "ACCEPTED",
        )
        self.assertEqual(samples[0].to_dict()["label"], "TRUE_POSITIVE")

    def test_invalid_label_and_unknown_issue_type_are_rejected(self):
        record = manual_sample("invalid", IssueType.ORPHAN_BRANCH, FactLabel.TRUE_POSITIVE).to_dict()
        record["label"] = "ACCEPTED"
        with self.assertRaises(CorpusFormatError):
            CorpusSample.from_dict(record)

        with self.assertRaises(UnknownIssueTypeError):
            AuditCandidate(
                fingerprint=compute_golden_fingerprint("unknown-type"),
                issue_type="NOT_A_REAL_ISSUE",
                evidence_summary={},
                source_kind="synthetic_fixture",
            )

    def test_duplicate_sample_id_and_fingerprint_are_rejected(self):
        sample = manual_sample("duplicate", IssueType.ORPHAN_BRANCH, FactLabel.TRUE_POSITIVE)
        with self.assertRaises(DuplicateSampleError):
            validate_corpus((sample, sample))

        same_fingerprint = replace(sample, sample_id="manual-different-id")
        with self.assertRaises(DuplicateSampleError):
            validate_corpus((sample, same_fingerprint))

    def test_metrics_exclude_ambiguous_from_precision_and_keep_acceptance_separate(self):
        samples = (
            manual_sample("orphan-tp", IssueType.ORPHAN_BRANCH, FactLabel.TRUE_POSITIVE),
            manual_sample("orphan-fp", IssueType.ORPHAN_BRANCH, FactLabel.FALSE_POSITIVE),
            manual_sample("orphan-amb", IssueType.ORPHAN_BRANCH, FactLabel.AMBIGUOUS),
            manual_sample(
                "target-accepted",
                IssueType.TARGET_MISMATCH,
                FactLabel.TRUE_POSITIVE,
                accepted=True,
            ),
            manual_sample("normal", None, FactLabel.NO_ISSUE, negative_control=True),
        )
        report = calculate_metrics(samples)
        overall = report.overall
        orphan = report.by_issue_type[IssueType.ORPHAN_BRANCH]

        self.assertEqual(overall.sample_count, 5)
        self.assertEqual(overall.fact_sample_count, 4)
        self.assertEqual(overall.true_positive, 2)
        self.assertEqual(overall.false_positive, 1)
        self.assertEqual(overall.ambiguous, 1)
        self.assertEqual(overall.accepted, 1)
        self.assertAlmostEqual(cast(float, overall.precision), 2 / 3)
        self.assertAlmostEqual(cast(float, overall.false_positive_rate), 1 / 3)
        self.assertAlmostEqual(cast(float, overall.ambiguous_rate), 1 / 4)
        self.assertEqual(overall.negative_control_count, 1)
        self.assertEqual(overall.negative_control_no_issue, 1)
        self.assertEqual(orphan.sample_count, 3)
        self.assertEqual(orphan.true_positive, 1)
        self.assertEqual(orphan.false_positive, 1)
        self.assertIsNone(report.by_issue_type[IssueType.CYCLE_DETECTED].precision)
        self.assertEqual(set(report.by_issue_type), set(IssueType))

    def test_empty_corpus_has_zero_counts_and_no_fake_precision(self):
        report = calculate_metrics(())
        self.assertEqual(report.overall.sample_count, 0)
        self.assertEqual(report.overall.true_positive, 0)
        self.assertEqual(report.overall.false_positive, 0)
        self.assertIsNone(report.overall.precision)
        self.assertIsNone(report.overall.false_positive_rate)
        self.assertEqual(set(report.by_issue_type), set(IssueType))

    def test_negative_control_is_a_normal_program_with_no_audit_issue(self):
        result = audit_program_physical_dag(
            build_dag(NORMAL_PROGRAM, name="DEMO_NEGATIVE_CONTROL"),
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(result.issues, ())
        candidate = negative_control_candidate(
            "synthetic:DEMO_NEGATIVE_CONTROL",
            source_kind="synthetic_negative_control",
        )
        sample = sample_candidates(
            (candidate,),
            seed=35,
            per_issue_type=0,
            negative_control_count=1,
            sampling_group="negative-control",
        )[0]
        labeled = replace(
            sample,
            fact_label=FactLabel.NO_ISSUE,
            annotation_reason="normal_program_no_issue",
        )
        metrics = calculate_metrics((labeled,))
        self.assertEqual(metrics.overall.negative_control_no_issue, 1)
        self.assertEqual(metrics.overall.fact_sample_count, 0)

    def test_sensitive_fields_are_rejected_from_export_schema(self):
        candidate = negative_control_candidate("synthetic:privacy", source_kind="synthetic")
        record = candidate.to_dict()
        record["program_name"] = "REAL_PROGRAM"
        with self.assertRaises(CorpusFormatError):
            AuditCandidate.from_dict(record)

        sample = manual_sample("privacy", IssueType.ORPHAN_BRANCH, FactLabel.TRUE_POSITIVE)
        self.assertNotIn("program_name", sample.to_dict())
        self.assertNotIn("message", sample.to_dict())
        self.assertNotIn("script_code", sample.to_dict())

    def test_synthetic_program_inventory_replays_all_current_issue_types(self):
        candidates = synthetic_candidates()
        self.assertEqual(
            {candidate.issue_type for candidate in candidates if candidate.issue_type is not None},
            set(IssueType),
        )
        self.assertTrue(any(candidate.negative_control for candidate in candidates))
        self.assertEqual(
            audit_program_physical_dag(
                build_dag(CYCLE_PROGRAM, expected_target=None, name="DEMO_CYCLE_REPLAY"),
                observed_at=OBSERVED_AT,
            ).issues[0].issue_type,
            IssueType.CYCLE_DETECTED,
        )
        self.assertEqual(
            audit_program_physical_dag(
                build_dag(SELF_REFERENCE_PROGRAM, expected_target=None, name="DEMO_SELF_REPLAY"),
                observed_at=OBSERVED_AT,
            ).issues[0].issue_type,
            IssueType.SELF_REFERENCE,
        )

    def test_committed_synthetic_corpus_is_labeled_and_replayable(self):
        corpus_path = (
            Path(__file__).parents[1]
            / "fixtures"
            / "lineage"
            / "audit_golden_corpus.jsonl"
        )
        corpus = read_corpus(corpus_path, require_labels=True)
        self.assertEqual(len(corpus), 8)
        self.assertEqual(
            {sample.issue_type for sample in corpus if sample.issue_type is not None},
            set(IssueType),
        )
        self.assertEqual(
            sum(sample.negative_control for sample in corpus),
            1,
        )
        self.assertNotIn("DEMO_", corpus_path.read_text(encoding="utf-8"))

        replay = sample_candidates(
            synthetic_candidates(),
            seed=35,
            per_issue_type=1,
            negative_control_count=1,
            sampling_group="synthetic-regression",
        )
        self.assertEqual(
            [(sample.sample_id, sample.fingerprint) for sample in corpus],
            [(sample.sample_id, sample.fingerprint) for sample in replay],
        )

    def test_jsonl_round_trip_and_cli_sample_validate_metrics(self):
        candidates = (
            AuditCandidate(
                fingerprint=compute_golden_fingerprint("cli:orphan"),
                issue_type=IssueType.ORPHAN_BRANCH,
                evidence_summary={},
                source_kind="synthetic_cli",
            ),
            negative_control_candidate("cli:normal", source_kind="synthetic_cli"),
        )
        with TemporaryDirectory() as directory:
            candidate_path = f"{directory}/candidates.jsonl"
            corpus_path = f"{directory}/corpus.jsonl"
            write_candidate_manifest(candidate_path, candidates)
            self.assertEqual(
                audit_golden_cli.main(
                    [
                        "sample",
                        "--input",
                        candidate_path,
                        "--output",
                        corpus_path,
                        "--seed",
                        "35",
                        "--per-type",
                        "1",
                        "--negative-control-count",
                        "1",
                        "--sampling-group",
                        "cli",
                    ]
                ),
                0,
            )
            sampled = read_corpus(corpus_path)
            labeled = tuple(
                replace(
                    sample,
                    fact_label=(
                        FactLabel.NO_ISSUE
                        if sample.negative_control
                        else FactLabel.TRUE_POSITIVE
                    ),
                    annotation_reason=(
                        "normal_program_no_issue"
                        if sample.negative_control
                        else "synthetic_cli_true_positive"
                    ),
                )
                for sample in sampled
            )
            write_corpus(corpus_path, labeled, require_labels=True)
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(
                    audit_golden_cli.main(
                        ["validate", "--input", corpus_path, "--require-labels"]
                    ),
                    0,
                )
                self.assertEqual(
                    audit_golden_cli.main(["metrics", "--input", corpus_path]),
                    0,
                )
            self.assertIn('"false_positive_rate": 0.0', output.getvalue())


if __name__ == "__main__":
    unittest.main()
