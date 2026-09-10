from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "docs" / "research" / "issue-39-dws-materialization-v0.1.sql"
DOC_PATH = ROOT / "docs" / "research" / "issue-39-dws-materialization-schema.md"
MATRIX_PATH = ROOT / "docs" / "research" / "issue-39-dws-lifecycle-matrix.json"
TABLE_NAMES = (
    "lineage_batch",
    "lineage_schedule_edge",
    "lineage_program_state",
    "lineage_edge",
    "lineage_business_edge",
    "lineage_issue",
)
ORIENTATIONS = {
    "lineage_batch": "ROW",
    "lineage_schedule_edge": "COLUMN",
    "lineage_program_state": "ROW",
    "lineage_edge": "COLUMN",
    "lineage_business_edge": "COLUMN",
    "lineage_issue": "ROW",
}


def table_block(ddl: str, table_name: str) -> str:
    marker = f"CREATE TABLE dwp.{table_name} ("
    start = ddl.index(marker)
    end = ddl.index("\nWITH (ORIENTATION", start)
    return ddl[start:end]


class DwsSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ddl = DDL_PATH.read_text(encoding="utf-8")
        cls.doc = DOC_PATH.read_text(encoding="utf-8")
        cls.matrix = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))

    def test_ddl_declares_five_writer_tables_and_schedule_extension(self):
        tables = tuple(
            re.findall(r"^CREATE TABLE ([^\s(]+) \(", self.ddl, re.MULTILINE)
        )
        self.assertEqual(tables, tuple(f"dwp.{name}" for name in TABLE_NAMES))
        self.assertIn("lineage_business_edge", self.ddl)
        self.assertNotRegex(
            self.ddl,
            r"(?im)^\s*CREATE TABLE\s+(?:dwp\.)?lineage_closure\b",
        )
        self.assertNotRegex(self.ddl, r"(?i)\bSET\s+search_path\b")
        self.assertNotRegex(self.ddl, r"(?i)\bSELECT\s+current_schema\b")

    def test_every_table_uses_declared_distribution_and_orientation(self):
        self.assertEqual(self.ddl.count("DISTRIBUTE BY HASH("), len(TABLE_NAMES))
        for table_name, orientation in ORIENTATIONS.items():
            block = table_block(self.ddl, table_name)
            table_start = self.ddl.index(f"CREATE TABLE dwp.{table_name}")
            self.assertIn(
                f"WITH (ORIENTATION = {orientation})",
                self.ddl[table_start:],
            )
            self.assertRegex(
                block,
                r"\b(row_key|batch_id|schedule_edge_key|program_key|edge_key|business_edge_key|stable_issue_key)\b",
            )

    def test_each_fact_separates_row_and_stable_identity_keys(self):
        required_keys = {
            "lineage_batch": ("batch_id",),
            "lineage_schedule_edge": ("row_key", "schedule_edge_key"),
            "lineage_program_state": ("row_key", "program_key"),
            "lineage_edge": ("row_key", "edge_key"),
            "lineage_business_edge": ("row_key", "business_edge_key"),
            "lineage_issue": ("row_key", "stable_issue_key"),
        }
        for table_name, keys in required_keys.items():
            block = table_block(self.ddl, table_name)
            for key in keys:
                with self.subTest(table=table_name, key=key):
                    self.assertRegex(block, rf"\b{re.escape(key)}\b")

    def test_schedule_edge_preserves_raw_comparison_and_history_contract(self):
        schedule = table_block(self.ddl, "lineage_schedule_edge")
        for field in (
            "environment",
            "source_profile",
            "process_name",
            "project_version_key",
            "raw_source_table",
            "raw_target_table",
            "source_table",
            "target_table",
            "batch_id",
            "observed_at",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "is_active",
            "created_at",
            "updated_at",
        ):
            with self.subTest(field=field):
                self.assertRegex(schedule, rf"\b{field}\b")
        self.assertIn("DISTRIBUTE BY HASH(schedule_edge_key)", self.ddl)

    def test_physical_and_business_edges_have_distinct_endpoint_contracts(self):
        physical = table_block(self.ddl, "lineage_edge")
        self.assertIn("source_dataset_key      VARCHAR(128)", physical)
        self.assertIn("target_dataset_key      VARCHAR(128)", physical)
        self.assertIn("source_node_kind", physical)
        self.assertIn("target_node_kind", physical)
        self.assertIn("temporary_asset", self.ddl)
        business = table_block(self.ddl, "lineage_business_edge")
        self.assertRegex(business, r"source_dataset_key\s+VARCHAR\(128\) NOT NULL")
        self.assertRegex(business, r"target_dataset_key\s+VARCHAR\(128\) NOT NULL")
        self.assertIn("physical_derivation_hash", business)
        self.assertIn("collapse_depth", business)

    def test_lineage_edge_preserves_direct_evidence_and_lifecycle_contract(self):
        edge = table_block(self.ddl, "lineage_edge")
        for field in (
            "environment",
            "source_profile",
            "program_key",
            "program_name",
            "source_table",
            "target_table",
            "evidence_type",
            "evidence_json",
            "source_hash",
            "pipeline_version",
            "batch_id",
            "observed_at",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "is_active",
            "created_at",
            "updated_at",
        ):
            with self.subTest(field=field):
                self.assertRegex(edge, rf"\b{field}\b")
        batch = table_block(self.ddl, "lineage_batch")
        self.assertIn("edge_count", batch)
        self.assertNotIn("physical_edge_count", batch)
        self.assertNotIn("business_edge_count", batch)

    def test_lineage_issue_reuses_issue36_fact_policy_and_manual_audit_contract(self):
        issue = table_block(self.ddl, "lineage_issue")
        for field_pattern in (
            r"confidence\s+VARCHAR\(16\) NOT NULL",
            r"rule_version\s+VARCHAR\(256\) NOT NULL",
            r"severity\s+VARCHAR\(32\) NOT NULL",
            r"disposition\s+VARCHAR\(32\) NOT NULL",
            r"policy_version\s+VARCHAR\(256\) NOT NULL",
            r"disposition_updated_at\s+TIMESTAMP",
            r"disposition_updated_by\s+VARCHAR\(256\)",
        ):
            with self.subTest(field=field_pattern):
                self.assertRegex(issue, field_pattern)
        self.assertIn("HIGH / MEDIUM / LOW / UNKNOWN", issue)
        self.assertIn("OPEN / ACCEPTED / FALSE_POSITIVE / RESOLVED", issue)
        self.assertNotIn("issue_layer", issue)
        self.assertNotIn("lifecycle_status", issue)

    def test_all_create_and_index_statements_are_schema_qualified(self):
        statements = (
            line.strip()
            for line in self.ddl.splitlines()
            if line.strip()
            .upper()
            .startswith(
                ("CREATE TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX", "COMMENT ON")
            )
        )
        for statement in statements:
            with self.subTest(statement=statement):
                self.assertIn("dwp.", statement)

    def test_design_doc_preserves_runtime_boundary_and_issue36_contract(self):
        for phrase in (
            "ProgramPhysicalDAG",
            "formal direct `LineageEdge`",
            "TMP",
            "Issue #40",
            "Issue #36",
            "writer contract v0.3",
            "dwp.lineage_business_edge",
            "SQLite → DWS compatibility matrix",
            "DISTRIBUTE BY HASH(",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.doc)

    def test_lifecycle_matrix_covers_required_same_batch_cases(self):
        required_ids = {
            "initial_empty_success",
            "program_build_success",
            "tmp_chain_collapse",
            "formal_boundary_stop",
            "known_negative_materialization",
            "lineage_materialization_failure",
            "failed_publish",
            "duplicate_program_identity",
            "duplicate_lineage_edge_identity",
            "cross_profile_isolation",
            "inactive_edge_contamination",
            "partial_snapshot_scoped_deletion",
            "full_snapshot_scoped_disappearance",
            "incremental_reuse",
            "source_hash_change",
            "pipeline_version_rebuild",
            "tmp_rename",
            "physical_path_change_same_endpoint",
        }
        cases = {case["id"]: case for case in self.matrix["cases"]}
        self.assertTrue(required_ids.issubset(cases))
        self.assertIn("lineage_business_edge", json.dumps(self.matrix))
        for case in cases.values():
            with self.subTest(case=case["id"]):
                self.assertTrue(case["same_batch"])
                self.assertIsInstance(case["publish"], bool)
                self.assertIn("program_physical_dag_result", case)
                self.assertIn("lineage_edge_result", case)
                self.assertIn("issue_result", case)

    def test_lifecycle_matrix_encodes_fail_closed_and_scoped_deletion(self):
        cases = {case["id"]: case for case in self.matrix["cases"]}
        collapse_failure = cases["lineage_materialization_failure"]
        self.assertFalse(collapse_failure["publish"])
        self.assertTrue(collapse_failure["rollback"])
        self.assertIn("previous", collapse_failure["active_result"])

        failed_publish = cases["failed_publish"]
        self.assertFalse(failed_publish["publish"])
        self.assertTrue(failed_publish["rollback"])

        partial = cases["partial_snapshot_scoped_deletion"]
        self.assertIn("outside", partial["deletion_authority"])
        full = cases["full_snapshot_scoped_disappearance"]
        self.assertIn("declared", full["deletion_authority"])

    def test_lifecycle_matrix_covers_empty_duplicate_isolation_and_inactive_guards(
        self,
    ):
        cases = {case["id"]: case for case in self.matrix["cases"]}
        empty = cases["initial_empty_success"]
        self.assertTrue(empty["publish"])
        self.assertIn("zero", empty["lineage_edge_result"])

        for case_id in (
            "duplicate_program_identity",
            "duplicate_lineage_edge_identity",
        ):
            with self.subTest(case=case_id):
                self.assertTrue(cases[case_id]["reject_duplicate_stable_identity"])
                self.assertFalse(cases[case_id]["publish"])

        self.assertTrue(cases["cross_profile_isolation"]["publish"])
        self.assertTrue(
            cases["inactive_edge_contamination"]["query_requires_active_batch_join"]
        )

    def test_frozen_lineage_edge_lifecycle_semantics(self):
        invariants = "\n".join(self.matrix["invariants"])
        for invariant in (
            "formal direct endpoints",
            "TMP endpoints are forbidden",
            "do not update formal business-edge last_changed_at",
        ):
            with self.subTest(invariant=invariant):
                self.assertIn(invariant, invariants)
        for case_id in ("tmp_chain_collapse", "formal_boundary_stop"):
            self.assertIn(
                "TMP",
                next(
                    case["input"]
                    for case in self.matrix["cases"]
                    if case["id"] == case_id
                ),
            )
        for case_id in ("tmp_rename", "physical_path_change_same_endpoint"):
            case = next(case for case in self.matrix["cases"] if case["id"] == case_id)
            self.assertTrue(case["stable_lineage_identity"])
            self.assertIn("last_changed_at remains unchanged", case["active_result"])

    def test_dws_writer_preserves_physical_and_business_projections(self):
        self.assertIn("lineage_business_edge", self.ddl)
        self.assertIn("lineage_business_edge", json.dumps(self.matrix))
        self.assertIn("raw physical direct edge", self.doc)
        self.assertIn("formal direct", self.doc)


if __name__ == "__main__":
    unittest.main()
