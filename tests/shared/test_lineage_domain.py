from __future__ import annotations

import unittest
from datetime import datetime, timezone

from shared.lineage.domain import (
    IssueType,
    LineageEdge,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramNameDiagnostic,
    ProgramSource,
    expected_processing_order,
    extract_program_declared_target_token,
    group_program_sources_by_logical_target,
    is_formal_asset,
    is_temporary_asset,
    normalize_declared_target_from_program_name,
    normalize_legacy_program_namespace,
    parse_declared_primary_target,
    parse_program_name,
)


class LineageDomainTests(unittest.TestCase):
    def test_program_source_keeps_provider_fields_and_hash(self):
        source = ProgramSource(
            environment="DEV",
            source_profile="mysql_dev_a",
            program_name="DEMO_PROGRAM_SUMMARY",
            script_code="insert into DWM.DEMO_B select * from ODS.DEMO_A",
            expected_target="DWM.DEMO_B",
            source_hash="sha256:demo-hash",
        )

        self.assertEqual(source.environment, "DEV")
        self.assertEqual(source.source_profile, "mysql_dev_a")
        self.assertEqual(source.program_name, "DEMO_PROGRAM_SUMMARY")
        self.assertEqual(source.expected_target, "DWM.DEMO_B")
        self.assertEqual(source.source_hash, "sha256:demo-hash")
        self.assertIsInstance(source.script_code, str)

    def test_program_source_allows_unknown_expected_target(self):
        source = ProgramSource(
            environment="PROD",
            source_profile="metadata_demo",
            program_name="DEMO_PROGRAM_UNKNOWN_TARGET",
            script_code="select 1",
        )

        self.assertIsNone(source.expected_target)
        self.assertIsNone(source.source_hash)

    def test_program_source_target_authority_prefers_explicit_target(self):
        source = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWS_DWM.RESULT_A:00",
            script_code="select 1",
            expected_target="DWM.EXPLICIT_TARGET",
        )

        self.assertIsNone(source.logical_target)
        self.assertEqual(source.resolved_target, "DWM.EXPLICIT_TARGET")
        self.assertIsNone(source.step_seq)

    def test_program_identity_and_logical_processing_unit_keep_distinct_boundaries(
        self,
    ):
        first = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWM.RESULT_A:3:00",
            script_code="select 1",
        )
        second = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWM.RESULT_A:4:XYZ",
            script_code="select 1",
        )

        self.assertNotEqual(first.identity, second.identity)
        self.assertEqual(first.resolved_target, "DWM.RESULT_A")
        self.assertEqual(second.resolved_target, "DWM.RESULT_A")
        self.assertEqual(
            first.logical_processing_unit_key,
            second.logical_processing_unit_key,
        )
        self.assertEqual(first.opaque_suffix, "00")
        self.assertEqual(second.opaque_suffix, "XYZ")

    def test_program_name_target_first_parser_keeps_direct_dataset_name(self):
        parsed = parse_program_name("005:DWM.RESULT_A:1:00")

        self.assertEqual(parsed.legacy_marker, "005")
        self.assertEqual(parsed.logical_target, "DWM.RESULT_A")
        self.assertEqual(parsed.step_seq, 1)
        self.assertEqual(parsed.opaque_suffix, "00")
        self.assertEqual(
            parsed.diagnostics,
            (ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,),
        )
        self.assertEqual(
            parse_declared_primary_target("005:DWM.RESULT_A:1:00"),
            "DWM.RESULT_A",
        )
        self.assertEqual(
            extract_program_declared_target_token("005:DWM.RESULT_A:1:00"),
            "DWM.RESULT_A",
        )
        self.assertEqual(
            normalize_declared_target_from_program_name("DWM.RESULT_A"),
            "DWM.RESULT_A",
        )

    def test_program_name_parser_normalizes_confirmed_legacy_names(self):
        cases = (
            ("005:DM.RESULT_A:1:00", "DM.RESULT_A", 1),
            ("005:DWS_DM.RESULT_A:1:00", "DM.RESULT_A", 1),
            ("005:DLK_DLO.RESULT_A:1:00", "DLO.RESULT_A", 1),
            ("005:DWS_DM.RESULT_A:2:00", "DM.RESULT_A", 2),
            ("005:DWS_DM.RESULT_A:1:PRC_RESULT_A", "DM.RESULT_A", 1),
        )

        for value, expected_target, expected_step in cases:
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertEqual(parsed.logical_target, expected_target)
                self.assertEqual(parsed.step_seq, expected_step)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,
                    parsed.diagnostics,
                )

        self.assertEqual(
            normalize_legacy_program_namespace("ABC_DM.RESULT_A"),
            "ABC_DM.RESULT_A",
        )

    def test_program_name_parser_keeps_ambiguous_three_part_shapes_unknown(self):
        for value in (
            "005:DWS_DM.RESULT_A:00",
            "005:DLK_DLO.RESULT_A:00",
            "005:DWS_DWM.RESULT_A:00",
            "005:DWM.RESULT_A:00",
            "005:ABC_DWM.RESULT_A:00",
        ):
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertIsNone(parsed.logical_target)
                self.assertIsNone(parsed.step_seq)
                self.assertIsNone(parsed.opaque_suffix)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_FORMAT_UNSUPPORTED,
                    parsed.diagnostics,
                )
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_STEP_MISSING,
                    parsed.diagnostics,
                )
                self.assertNotIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,
                    parsed.diagnostics,
                )
                self.assertNotIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_STEP_INVALID,
                    parsed.diagnostics,
                )

        self.assertIsNone(parse_declared_primary_target("005:ABC_DWM.RESULT_A:00"))

        custom_suffix = parse_program_name("005:DWM.RESULT_A:1:ABCD")
        self.assertEqual(custom_suffix.logical_target, "DWM.RESULT_A")
        self.assertEqual(custom_suffix.step_seq, 1)
        self.assertIn(
            ProgramNameDiagnostic.PROGRAM_NAME_SUFFIX_NONSTANDARD,
            custom_suffix.diagnostics,
        )

    def test_program_name_parser_rejects_unsafe_target_without_guessing(self):
        for value in (
            "",
            "005::1:00",
            "005:INVALID:1:00",
            "004:DEMO_DWM.RESULT_A:1:00",
        ):
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertIsNone(parsed.logical_target)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_INVALID,
                    parsed.diagnostics,
                )

        invalid_step = parse_program_name("005:DEMO_DWM.RESULT_A:0:00")
        self.assertEqual(invalid_step.logical_target, "DEMO_DWM.RESULT_A")
        self.assertIsNone(invalid_step.step_seq)
        self.assertIn(
            ProgramNameDiagnostic.PROGRAM_NAME_STEP_INVALID,
            invalid_step.diagnostics,
        )

    def test_program_name_steps_group_by_target_and_sort_without_scheduler_fact(self):
        sources = tuple(
            ProgramSource(
                environment="DEV",
                source_profile="fixture",
                program_name=name,
                script_code="select 1",
            )
            for name in (
                "005:DWM.RESULT_A:2:00",
                "005:DWM.RESULT_A:1:00",
                "005:DWM.RESULT_A:00",
                "005:DWM.RESULT_B:1:00",
            )
        )
        groups = group_program_sources_by_logical_target(sources)

        self.assertEqual(tuple(groups), ("DWM.RESULT_A", "DWM.RESULT_B"))
        self.assertEqual(
            [source.step_seq for source in groups["DWM.RESULT_A"]],
            [1, 2],
        )
        self.assertEqual(
            expected_processing_order(groups["DWM.RESULT_A"]),
            (1, 2),
        )
        self.assertEqual(
            [source.program_name for source in groups["DWM.RESULT_A"]],
            [
                "005:DWM.RESULT_A:1:00",
                "005:DWM.RESULT_A:2:00",
            ],
        )

    def test_physical_edge_direction_is_upstream_to_downstream(self):
        edge = PhysicalEdge(source="ODS.DEMO_A", target="DWM.DEMO_B")

        self.assertEqual(edge.source, "ODS.DEMO_A")
        self.assertEqual(edge.target, "DWM.DEMO_B")
        self.assertNotEqual(edge.source, edge.target)

    def test_physical_dag_keeps_tmp_nodes(self):
        nodes = {
            node.node_key: node
            for node in (
                PhysicalNode("ODS.DEMO_A", "ODS.DEMO_A"),
                PhysicalNode("TMP1", "TMP1"),
                PhysicalNode("DWM.DEMO_B", "DWM.DEMO_B"),
            )
        }
        edges = (
            PhysicalEdge(source="ODS.DEMO_A", target="TMP1"),
            PhysicalEdge(source="TMP1", target="DWM.DEMO_B"),
        )

        self.assertEqual(nodes["TMP1"].kind, PhysicalNodeKind.TEMPORARY_ASSET)
        self.assertTrue(nodes["TMP1"].is_temporary)
        self.assertTrue(nodes["DWM.DEMO_B"].is_formal)
        self.assertEqual(
            [(edge.source, edge.target) for edge in edges],
            [("ODS.DEMO_A", "TMP1"), ("TMP1", "DWM.DEMO_B")],
        )

    def test_temporary_name_rules_are_conservative_and_extensible(self):
        self.assertTrue(is_temporary_asset("TMP_1"))
        self.assertTrue(is_temporary_asset("TMP_STAGE_X"))
        self.assertTrue(is_temporary_asset("DWM.TMP1"))
        self.assertFalse(is_temporary_asset("DWM.DEMO_C"))
        self.assertFalse(is_temporary_asset("DWM.TMPORARY_BUSINESS"))
        self.assertFalse(is_temporary_asset("DEMO_TMP_1"))
        self.assertTrue(
            is_temporary_asset(
                "DWM.DEMO_STAGE_X",
                rules=(lambda name: name.endswith("STAGE_X"),),
            )
        )
        self.assertTrue(is_formal_asset("DWM.DEMO_C"))
        self.assertFalse(is_formal_asset("TMP_STAGE_X"))

    def test_lineage_edge_represents_direct_formal_asset_fact(self):
        observed_at = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
        updated_at = datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc)
        edge = LineageEdge(
            environment="DEV",
            source_profile="mysql_dev_a",
            source_table="DWM.DEMO_B",
            target_table="DWA.DEMO_C",
            program_name="DEMO_PROGRAM_C",
            job_key="DEMO_JOB_C",
            evidence_type="physical_dag",
            source_hash="sha256:demo-hash",
            batch_id="batch-demo-1",
            observed_at=observed_at,
            updated_at=updated_at,
            is_active=True,
        )

        self.assertEqual(edge.source_table, "DWM.DEMO_B")
        self.assertEqual(edge.target_table, "DWA.DEMO_C")
        self.assertEqual(edge.source_hash, "sha256:demo-hash")
        self.assertEqual(edge.batch_id, "batch-demo-1")
        self.assertEqual(edge.observed_at, observed_at)
        self.assertEqual(edge.updated_at, updated_at)
        self.assertTrue(is_formal_asset(edge.source_table))
        self.assertTrue(is_formal_asset(edge.target_table))

    def test_lineage_edge_rejects_tmp_endpoint_by_default(self):
        with self.assertRaises(ValueError):
            LineageEdge(
                environment="DEV",
                source_profile="mysql_dev_a",
                source_table="TMP_1",
                target_table="DWA.DEMO_C",
            )

    def test_issue_type_contains_all_frozen_values(self):
        self.assertEqual(
            {item.value for item in IssueType},
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

    def test_lineage_issue_preserves_branch_and_lifecycle_fields(self):
        issue = LineageIssue(
            environment="DEV",
            source_profile="mysql_dev_a",
            program_name="DEMO_PROGRAM_C",
            issue_type=IssueType.ORPHAN_BRANCH,
            severity="warning",
            branch_sink="TMP_STAGE_X",
            message="分支未到达 expected target",
            evidence={"path": ["ODS.DEMO_A", "TMP_STAGE_X"]},
            batch_id="batch-demo-1",
            first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            is_active=True,
        )

        self.assertIs(issue.issue_type, IssueType.ORPHAN_BRANCH)
        self.assertIsNone(issue.node_key)
        self.assertEqual(issue.branch_sink, "TMP_STAGE_X")
        evidence = issue.evidence
        self.assertIsInstance(evidence, dict)
        if not isinstance(evidence, dict):
            self.fail("expected mapping evidence")
        self.assertEqual(evidence["path"], ["ODS.DEMO_A", "TMP_STAGE_X"])
        self.assertEqual(issue.batch_id, "batch-demo-1")
        self.assertTrue(issue.is_active)


if __name__ == "__main__":
    unittest.main()
