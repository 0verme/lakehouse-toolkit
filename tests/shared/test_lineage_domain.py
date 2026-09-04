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
    ProgramSource,
    is_formal_asset,
    is_temporary_asset,
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
