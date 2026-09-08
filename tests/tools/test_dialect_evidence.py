from __future__ import annotations

import unittest

from tools.research.dialect_evidence import (
    Decision,
    DialectFeature,
    DialectObservation,
    EvidenceState,
    FailureCategory,
    FailureObservation,
    PROFILE_LABELS,
    ReplayStatus,
    build_report,
    classify_failure_reason,
    render_matrix,
)


PROGRAM_COUNTS = {
    "profile_A": 5387,
    "profile_B": 8329,
    "profile_C": 3651,
    "profile_D": 3131,
}
REPLAY_STATUSES = {profile: ReplayStatus.PASS for profile in PROFILE_LABELS}


class DialectEvidenceTests(unittest.TestCase):
    def test_failure_classifier_keeps_dialect_separate_from_other_causes(self):
        self.assertEqual(
            classify_failure_reason("SQL_ARGUMENT_DYNAMIC"),
            FailureCategory.DYNAMIC_SQL,
        )
        self.assertEqual(
            classify_failure_reason("SQL_CALL_NOT_RECOGNIZED"),
            FailureCategory.PYTHON_WRAPPER,
        )
        self.assertEqual(
            classify_failure_reason("TARGET_WITHOUT_SOURCE"),
            FailureCategory.TARGET_AUTHORITY,
        )
        self.assertEqual(
            classify_failure_reason("PROGRAM_NAME_TARGET_UNRESOLVED"),
            FailureCategory.PROGRAM_NAME,
        )
        self.assertEqual(
            classify_failure_reason("PARSER_BUG_CONFIRMED"),
            FailureCategory.PARSER_BUG,
        )
        self.assertEqual(
            classify_failure_reason("DIALECT_SYNTAX_GAP_CONFIRMED"),
            FailureCategory.DIALECT_SYNTAX,
        )
        self.assertEqual(
            classify_failure_reason("PYTHON_PARSE_FAILED"),
            FailureCategory.UNKNOWN,
        )
        self.assertEqual(
            classify_failure_reason("UNSUPPORTED_STATEMENT"),
            FailureCategory.UNKNOWN,
        )

    def test_unknown_feature_matrix_does_not_turn_replay_pass_into_support(self):
        report = build_report(
            program_counts=PROGRAM_COUNTS,
            replay_statuses=REPLAY_STATUSES,
            failures=(
                FailureObservation(
                    scope="aggregate",
                    reason="PARSER_BUG_CONFIRMED",
                    count=1,
                ),
            ),
        )

        self.assertEqual(report.decision, Decision.INSUFFICIENT_EVIDENCE)
        self.assertEqual(tuple(item.profile for item in report.profiles), PROFILE_LABELS)
        for profile in report.profiles:
            self.assertEqual(profile.replay_status, ReplayStatus.PASS)
            self.assertTrue(
                all(
                    item.state is EvidenceState.UNKNOWN
                    and item.count is None
                    for item in profile.observations
                )
            )
        self.assertEqual(
            report.failure_counts["aggregate"][FailureCategory.PARSER_BUG.value],
            1,
        )
        self.assertIsNone(
            report.failure_counts["aggregate"][FailureCategory.DIALECT_SYNTAX.value]
        )

        matrix = render_matrix(report)
        self.assertIn("profile_A", matrix)
        self.assertIn("PASS", matrix)
        self.assertIn("UNKNOWN", matrix)

    def test_confirmed_feature_observation_is_bounded_to_one_profile(self):
        report = build_report(
            program_counts=PROGRAM_COUNTS,
            replay_statuses=REPLAY_STATUSES,
            observations=(
                DialectObservation(
                    profile="profile_A",
                    feature=DialectFeature.CTE,
                    state=EvidenceState.SUPPORTED,
                    count=7,
                ),
                DialectObservation(
                    profile="profile_B",
                    feature=DialectFeature.MERGE,
                    state=EvidenceState.FAILED,
                    count=2,
                ),
            ),
            decision=Decision.MINIMAL_HOOK_ONLY,
        )

        profile_a = report.profiles[0]
        profile_b = report.profiles[1]
        self.assertEqual(profile_a.observations[0].state, EvidenceState.SUPPORTED)
        self.assertEqual(profile_a.observations[0].count, 7)
        self.assertEqual(profile_b.observations[1].state, EvidenceState.FAILED)
        self.assertEqual(profile_b.observations[1].count, 2)
        self.assertEqual(profile_a.observations[1].state, EvidenceState.UNKNOWN)

    def test_invalid_or_duplicate_evidence_is_rejected(self):
        with self.assertRaises(ValueError):
            DialectObservation(
                profile="profile_A",
                feature=DialectFeature.CTE,
                state=EvidenceState.UNKNOWN,
                count=1,
            )

        observation = DialectObservation(
            profile="profile_A",
            feature=DialectFeature.CTE,
            state=EvidenceState.OBSERVED,
            count=1,
        )
        with self.assertRaises(ValueError):
            build_report(
                program_counts=PROGRAM_COUNTS,
                replay_statuses=REPLAY_STATUSES,
                observations=(observation, observation),
            )


if __name__ == "__main__":
    unittest.main()
