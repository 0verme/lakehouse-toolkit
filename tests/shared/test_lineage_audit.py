from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timezone
from typing import cast

from shared.lineage.audit import (  # pyright: ignore[reportMissingImports]
    ProgramLineageAuditor,
    audit_program_physical_dag,
    compute_lineage_issue_stable_key,
    issue_severity,
)
from shared.lineage.domain import IssueType, LineageIssue, ProgramSource
from shared.lineage.lineage_builder import normalize_table_name
from shared.lineage.physical_dag import (
    ProgramPhysicalDAG,
    build_program_physical_dag,
)
from tests.fixtures.lineage.phase4_audit_programs import (  # pyright: ignore[reportMissingImports]
    CYCLE_PROGRAM,
    FORMAL_INTERMEDIATE_PROGRAM,
    MERGED_SOURCES_PROGRAM,
    MULTI_SINK_PROGRAM,
    MULTIPLE_CYCLES_PROGRAM,
    NORMAL_PROGRAM,
    ORPHAN_BRANCH_PROGRAM,
    SELF_REFERENCE_PROGRAM,
    TARGET_MISMATCH_PROGRAM,
    TARGET_NOT_FOUND_PROGRAM,
    TMP_REACHES_TARGET_PROGRAM,
    UNKNOWN_TARGET_PROGRAM,
)

EXPECTED_TARGET = normalize_table_name("DWA.DEMO_RESULT")


def build_dag(
    script_code: str,
    *,
    expected_target: str | None = EXPECTED_TARGET,
    program_name: str = "DEMO_PROGRAM_PHASE4",
) -> ProgramPhysicalDAG:
    source = ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name=program_name,
        script_code=script_code,
        expected_target=expected_target,
    )
    return build_program_physical_dag(source)


def issue_of(result, issue_type: IssueType) -> LineageIssue:
    matches = [issue for issue in result.issues if issue.issue_type is issue_type]
    if len(matches) != 1:
        raise AssertionError(f"expected one {issue_type.value}, got {len(matches)}")
    return matches[0]


def evidence_of(issue: LineageIssue) -> dict[str, object]:
    evidence = issue.evidence
    if not isinstance(evidence, dict):
        raise AssertionError("audit issue evidence must be a dict")
    return cast(dict[str, object], evidence)


def issue_signature(result) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            issue.issue_type,
            issue.node_key,
            issue.branch_sink,
            issue.stable_key,
            issue.evidence,
        )
        for issue in result.issues
    )


class LineageAuditTests(unittest.TestCase):
    def test_normal_program_has_no_issues(self):
        result = audit_program_physical_dag(build_dag(NORMAL_PROGRAM))

        self.assertEqual(result.issues, ())
        self.assertEqual(
            result.target_reachable_nodes,
            (
                EXPECTED_TARGET,
                "ODS.DEMO_A",
                "ODS.DEMO_B",
                "ODS.DEMO_C",
                "TMP_1",
                "TMP_2",
            ),
        )
        self.assertEqual(result.orphan_branch_sinks, ())

    def test_orphan_branch_is_aggregated_by_terminal_sink(self):
        result = audit_program_physical_dag(build_dag(ORPHAN_BRANCH_PROGRAM))

        orphan = issue_of(result, IssueType.ORPHAN_BRANCH)
        self.assertEqual(
            {issue.issue_type for issue in result.issues},
            {IssueType.MULTI_SINK_CANDIDATE, IssueType.ORPHAN_BRANCH},
        )
        self.assertEqual(
            sum(issue.issue_type is IssueType.ORPHAN_BRANCH for issue in result.issues),
            1,
        )
        self.assertEqual(orphan.branch_sink, "TMP_X2")
        evidence = evidence_of(orphan)
        self.assertEqual(evidence["branch_sink"], "TMP_X2")
        self.assertEqual(
            evidence["branch_nodes"],
            ["ODS.DEMO_X", "TMP_X1", "TMP_X2"],
        )
        self.assertEqual(
            evidence["branch_edge_pairs"],
            [["ODS.DEMO_X", "TMP_X1"], ["TMP_X1", "TMP_X2"]],
        )
        self.assertEqual(evidence["entry_sources"], ["ODS.DEMO_X"])
        self.assertEqual(result.orphan_branch_sinks, ("TMP_X2",))

    def test_multiple_sinks_reports_candidate_and_orphan_branch(self):
        result = audit_program_physical_dag(build_dag(MULTI_SINK_PROGRAM))

        self.assertEqual(
            {issue.issue_type for issue in result.issues},
            {IssueType.MULTI_SINK_CANDIDATE, IssueType.ORPHAN_BRANCH},
        )
        self.assertEqual(
            sum(
                issue.issue_type is IssueType.MULTI_SINK_CANDIDATE
                for issue in result.issues
            ),
            1,
        )
        multi_sink = issue_of(result, IssueType.MULTI_SINK_CANDIDATE)
        multi_evidence = evidence_of(multi_sink)
        expected_sinks = [
            normalize_table_name("DWA.DEMO_OTHER"),
            EXPECTED_TARGET,
        ]
        self.assertEqual(multi_evidence["sink_count"], 2)
        self.assertEqual(multi_evidence["sinks"], expected_sinks)
        self.assertEqual(multi_evidence["sorted_sinks"], expected_sinks)
        self.assertEqual(multi_evidence["formal_sinks"], expected_sinks)
        self.assertEqual(multi_evidence["temporary_sinks"], [])
        self.assertEqual(multi_evidence["expected_target"], EXPECTED_TARGET)
        orphan = issue_of(result, IssueType.ORPHAN_BRANCH)
        self.assertEqual(orphan.branch_sink, normalize_table_name("DWA.DEMO_OTHER"))

    def test_target_not_found_does_not_turn_every_branch_into_orphan(self):
        result = audit_program_physical_dag(build_dag(TARGET_NOT_FOUND_PROGRAM))

        self.assertEqual(
            [issue.issue_type for issue in result.issues],
            [IssueType.TARGET_NOT_FOUND],
        )
        target_issue = issue_of(result, IssueType.TARGET_NOT_FOUND)
        evidence = evidence_of(target_issue)
        self.assertEqual(evidence["expected_target"], EXPECTED_TARGET)
        self.assertEqual(evidence["written_targets"], ["TMP_1"])
        self.assertEqual(evidence["sinks"], ["TMP_1"])
        self.assertEqual(result.target_reachable_nodes, ())
        self.assertEqual(result.orphan_branch_sinks, ())

    def test_target_mismatch_has_priority_over_target_not_found(self):
        result = audit_program_physical_dag(build_dag(TARGET_MISMATCH_PROGRAM))

        self.assertEqual(
            [issue.issue_type for issue in result.issues],
            [IssueType.TARGET_MISMATCH],
        )
        mismatch = issue_of(result, IssueType.TARGET_MISMATCH)
        evidence = evidence_of(mismatch)
        self.assertEqual(evidence["expected_target"], EXPECTED_TARGET)
        self.assertEqual(
            evidence["actual_formal_sinks"],
            [normalize_table_name("DWA.DEMO_OTHER")],
        )
        self.assertEqual(
            evidence["all_sinks"], [normalize_table_name("DWA.DEMO_OTHER")]
        )
        self.assertFalse(evidence["expected_target_written"])

    def test_existing_expected_sink_has_no_target_issue(self):
        result = audit_program_physical_dag(build_dag(NORMAL_PROGRAM))

        self.assertNotIn(
            IssueType.TARGET_NOT_FOUND,
            {issue.issue_type for issue in result.issues},
        )
        self.assertNotIn(
            IssueType.TARGET_MISMATCH,
            {issue.issue_type for issue in result.issues},
        )

    def test_expected_target_none_skips_target_and_orphan_detectors(self):
        result = audit_program_physical_dag(
            build_dag(UNKNOWN_TARGET_PROGRAM, expected_target=None)
        )

        self.assertEqual(
            [issue.issue_type for issue in result.issues],
            [IssueType.MULTI_SINK_CANDIDATE],
        )
        self.assertIsNone(result.expected_target)
        self.assertEqual(result.target_reachable_nodes, ())
        self.assertEqual(result.orphan_branch_sinks, ())
        self.assertNotIn(
            IssueType.TARGET_NOT_FOUND,
            {issue.issue_type for issue in result.issues},
        )
        self.assertNotIn(
            IssueType.TARGET_MISMATCH,
            {issue.issue_type for issue in result.issues},
        )
        self.assertNotIn(
            IssueType.ORPHAN_BRANCH,
            {issue.issue_type for issue in result.issues},
        )

    def test_expected_written_but_not_final_is_target_mismatch(self):
        script = """
        execute("INSERT INTO DWA.DEMO_EXPECTED SELECT * FROM ODS.DEMO_A")
        execute("INSERT INTO DWA.DEMO_OTHER SELECT * FROM DWA.DEMO_EXPECTED")
        """
        expected = normalize_table_name("DWA.DEMO_EXPECTED")
        result = audit_program_physical_dag(
            build_dag(script, expected_target="DWA.DEMO_EXPECTED")
        )

        mismatch = issue_of(result, IssueType.TARGET_MISMATCH)
        evidence = evidence_of(mismatch)
        self.assertEqual(evidence["expected_target"], expected)
        self.assertTrue(evidence["expected_target_written"])
        self.assertFalse(evidence["expected_target_is_sink"])
        self.assertEqual(
            evidence["actual_formal_sinks"],
            [normalize_table_name("DWA.DEMO_OTHER")],
        )
        self.assertNotIn(
            IssueType.TARGET_NOT_FOUND,
            {issue.issue_type for issue in result.issues},
        )

    def test_self_reference_is_reported_once_without_single_node_cycle_issue(self):
        result = audit_program_physical_dag(
            build_dag(SELF_REFERENCE_PROGRAM, expected_target=None)
        )

        self.assertEqual(
            [issue.issue_type for issue in result.issues],
            [IssueType.SELF_REFERENCE],
        )
        self_issue = issue_of(result, IssueType.SELF_REFERENCE)
        self.assertEqual(self_issue.node_key, normalize_table_name("DWM.DEMO_SELF"))
        evidence = evidence_of(self_issue)
        self.assertEqual(evidence["source"], self_issue.node_key)
        self.assertEqual(evidence["target"], self_issue.node_key)
        self.assertIn("statement_indices", evidence)
        self.assertIn("edge", evidence)
        self.assertNotIn(IssueType.CYCLE_DETECTED, result.issue_types)

    def test_cycle_is_one_issue_per_multi_node_scc(self):
        result = audit_program_physical_dag(
            build_dag(CYCLE_PROGRAM, expected_target=None)
        )

        self.assertEqual(
            [issue.issue_type for issue in result.issues],
            [IssueType.CYCLE_DETECTED],
        )
        cycle = issue_of(result, IssueType.CYCLE_DETECTED)
        evidence = evidence_of(cycle)
        self.assertEqual(evidence["cycle_nodes"], ["TMP_CYCLE_1", "TMP_CYCLE_2"])
        self.assertEqual(
            evidence["cycle_edge_pairs"],
            [
                ["TMP_CYCLE_1", "TMP_CYCLE_2"],
                ["TMP_CYCLE_2", "TMP_CYCLE_1"],
            ],
        )
        cycle_edges = cast(list[object], evidence["cycle_edges"])
        self.assertEqual(len(cycle_edges), 2)

    def test_multiple_independent_cycles_have_distinct_issues_and_keys(self):
        result = audit_program_physical_dag(
            build_dag(MULTIPLE_CYCLES_PROGRAM, expected_target=None)
        )

        cycles = [
            issue
            for issue in result.issues
            if issue.issue_type is IssueType.CYCLE_DETECTED
        ]
        self.assertEqual(len(cycles), 2)
        self.assertEqual(
            [evidence_of(issue)["cycle_nodes"] for issue in cycles],
            [
                ["TMP_CYCLE_A", "TMP_CYCLE_B"],
                ["TMP_CYCLE_X", "TMP_CYCLE_Y", "TMP_CYCLE_Z"],
            ],
        )
        self.assertNotEqual(cycles[0].stable_key, cycles[1].stable_key)

    def test_tmp_branch_reaching_target_is_not_orphan(self):
        result = audit_program_physical_dag(build_dag(TMP_REACHES_TARGET_PROGRAM))

        self.assertEqual(result.issues, ())
        self.assertIn("TMP_X1", result.target_reachable_nodes)
        self.assertIn("TMP_X2", result.target_reachable_nodes)

    def test_multiple_sources_into_one_branch_are_not_orphan(self):
        result = audit_program_physical_dag(build_dag(MERGED_SOURCES_PROGRAM))

        self.assertEqual(result.issues, ())

    def test_formal_intermediate_asset_does_not_stop_reachability(self):
        result = audit_program_physical_dag(build_dag(FORMAL_INTERMEDIATE_PROGRAM))

        self.assertEqual(result.issues, ())
        self.assertIn(
            normalize_table_name("DWM.DEMO_MIDDLE"),
            result.target_reachable_nodes,
        )

    def test_audit_does_not_mutate_physical_dag(self):
        dag = build_dag(ORPHAN_BRANCH_PROGRAM)
        before = (dag.nodes, dag.edges, dag.steps, dag.sinks)

        audit_program_physical_dag(dag)

        self.assertEqual((dag.nodes, dag.edges, dag.steps, dag.sinks), before)

    def test_edge_order_and_sink_order_do_not_change_result_order_or_evidence(self):
        dag = build_dag(MULTI_SINK_PROGRAM)
        reordered = replace(
            dag,
            edges=tuple(reversed(dag.edges)),
            sinks=tuple(reversed(dag.sinks)),
        )

        first = audit_program_physical_dag(dag)
        second = audit_program_physical_dag(reordered)

        self.assertEqual(issue_signature(first), issue_signature(second))
        self.assertEqual(first.orphan_branch_sinks, second.orphan_branch_sinks)

    def test_stable_key_is_semantic_and_deterministic(self):
        key_one = compute_lineage_issue_stable_key(
            "DEV",
            "fixture",
            "DEMO_PROGRAM_PHASE4",
            IssueType.ORPHAN_BRANCH,
            branch_sink="TMP_X2",
        )
        key_two = compute_lineage_issue_stable_key(
            "DEV",
            "fixture",
            "DEMO_PROGRAM_PHASE4",
            IssueType.ORPHAN_BRANCH,
            branch_sink="TMP_X2",
        )
        self.assertEqual(key_one, key_two)
        self.assertNotEqual(
            key_one,
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "DEMO_PROGRAM_PHASE4",
                IssueType.ORPHAN_BRANCH,
                branch_sink="TMP_OTHER",
            ),
        )
        self.assertNotEqual(
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "DEMO_PROGRAM_PHASE4",
                IssueType.SELF_REFERENCE,
                node_key="DWM.DEMO_A",
            ),
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "DEMO_PROGRAM_PHASE4",
                IssueType.SELF_REFERENCE,
                node_key="DWM.DEMO_B",
            ),
        )
        self.assertEqual(
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "DEMO_PROGRAM_PHASE4",
                IssueType.CYCLE_DETECTED,
                cycle_nodes=("TMP_2", "TMP_1"),
            ),
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "DEMO_PROGRAM_PHASE4",
                IssueType.CYCLE_DETECTED,
                cycle_nodes=("TMP_1", "TMP_2"),
            ),
        )

    def test_lifecycle_fields_use_one_injected_observation_time(self):
        script = "\n".join(
            (
                NORMAL_PROGRAM,
                ORPHAN_BRANCH_PROGRAM,
                MULTI_SINK_PROGRAM,
                SELF_REFERENCE_PROGRAM,
                CYCLE_PROGRAM,
            )
        )
        observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        result = audit_program_physical_dag(
            build_dag(script),
            observed_at=observed_at,
            batch_id="batch-demo-4",
        )

        self.assertGreaterEqual(len(result.issues), 4)
        for issue in result.issues:
            self.assertEqual(issue.first_seen_at, observed_at)
            self.assertEqual(issue.last_seen_at, observed_at)
            self.assertTrue(issue.is_active)
            self.assertEqual(issue.batch_id, "batch-demo-4")
            self.assertIsNotNone(issue.stable_key)

    def test_severity_policy_is_central_and_stable(self):
        expected = {
            IssueType.TARGET_NOT_FOUND: "HIGH",
            IssueType.TARGET_MISMATCH: "HIGH",
            IssueType.CYCLE_DETECTED: "HIGH",
            IssueType.SELF_REFERENCE: "HIGH",
            IssueType.ORPHAN_BRANCH: "MEDIUM",
            IssueType.MULTI_SINK_CANDIDATE: "MEDIUM",
        }

        for issue_type, severity in expected.items():
            self.assertEqual(issue_severity(issue_type), severity)

        result = audit_program_physical_dag(build_dag(ORPHAN_BRANCH_PROGRAM))
        self.assertEqual(
            issue_of(result, IssueType.ORPHAN_BRANCH).severity,
            "MEDIUM",
        )

    def test_auditor_facade_and_function_have_same_contract(self):
        dag = build_dag(ORPHAN_BRANCH_PROGRAM)
        observed_at = datetime(2026, 1, 3, tzinfo=timezone.utc)

        from_function = audit_program_physical_dag(dag, observed_at=observed_at)
        from_facade = ProgramLineageAuditor().audit(dag, observed_at=observed_at)
        from_callable = ProgramLineageAuditor()(dag, observed_at=observed_at)

        self.assertEqual(issue_signature(from_function), issue_signature(from_facade))
        self.assertEqual(issue_signature(from_function), issue_signature(from_callable))


if __name__ == "__main__":
    unittest.main()
