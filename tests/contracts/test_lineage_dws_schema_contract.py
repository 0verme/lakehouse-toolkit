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
    "lineage_program_state",
    "lineage_edge",
    "lineage_business_edge",
    "lineage_issue",
)
ORIENTATIONS = {
    "lineage_batch": "ROW",
    "lineage_program_state": "ROW",
    "lineage_edge": "COLUMN",
    "lineage_business_edge": "ROW",
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

    def test_ddl_declares_exactly_the_five_qualified_tables(self):
        tables = tuple(
            re.findall(r"^CREATE TABLE ([^\s(]+) \(", self.ddl, re.MULTILINE)
        )
        self.assertEqual(tables, tuple(f"dwp.{name}" for name in TABLE_NAMES))
        self.assertNotRegex(
            self.ddl,
            r"(?im)^\s*CREATE TABLE\s+(?:dwp\.)?lineage_closure\b",
        )
        self.assertNotRegex(self.ddl, r"(?i)\bSET\s+search_path\b")
        self.assertNotRegex(self.ddl, r"(?i)\bSELECT\s+current_schema\b")

    def test_every_table_uses_row_key_hash_distribution_and_declared_orientation(self):
        self.assertEqual(
            self.ddl.count("DISTRIBUTE BY HASH (row_key)"), len(TABLE_NAMES)
        )
        for table_name, orientation in ORIENTATIONS.items():
            block = table_block(self.ddl, table_name)
            self.assertIn("row_key", block)
            self.assertIn("PRIMARY KEY (row_key)", block)
            table_start = self.ddl.index(f"CREATE TABLE dwp.{table_name}")
            self.assertIn(
                f"WITH (ORIENTATION = {orientation})",
                self.ddl[table_start:],
            )

    def test_each_fact_separates_row_and_stable_identity_keys(self):
        required_keys = {
            "lineage_batch": ("row_key", "batch_id"),
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

    def test_physical_and_business_columns_have_distinct_endpoint_contracts(self):
        physical = table_block(self.ddl, "lineage_edge")
        business = table_block(self.ddl, "lineage_business_edge")

        self.assertIn("source_node_kind", physical)
        self.assertIn("target_node_kind", physical)
        self.assertIn("source_dataset_key", physical)
        self.assertIn("target_dataset_key", physical)
        self.assertIn("TEMPORARY_ASSET", physical)

        self.assertIn("source_dataset_key      VARCHAR(128) NOT NULL", business)
        self.assertIn("target_dataset_key      VARCHAR(128) NOT NULL", business)
        self.assertRegex(
            business,
            r"collapse_depth\s+INTEGER NOT NULL",
        )
        self.assertIn("CHECK (collapse_depth >= 1)", business)
        self.assertNotRegex(business, r"\bpath_count\b")
        self.assertIn("physical_derivation_hash", business)
        self.assertNotIn("source_node_kind", business)
        self.assertNotIn("target_node_kind", business)

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
        self.assertIn("HIGH', 'MEDIUM', 'LOW', 'UNKNOWN", issue)
        self.assertIn("OPEN', 'ACCEPTED', 'FALSE_POSITIVE', 'RESOLVED", issue)
        self.assertNotIn("issue_layer", issue)
        self.assertNotIn("lifecycle_status", issue)

    def test_business_edge_has_required_lifecycle_fields(self):
        business = table_block(self.ddl, "lineage_business_edge")
        for field in (
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
                self.assertRegex(business, rf"\b{field}\b")

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

    def test_design_doc_preserves_physical_business_closure_boundary(self):
        for phrase in (
            "Physical lineage：`lineage_edge`",
            "Business lineage：`lineage_business_edge`",
            "Closure：Issue #40",
            "TMP / intermediate table",
            "source of truth",
            "同一 batch",
            "Issue #36",
            "建议独立 Issue",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.doc)

    def test_lifecycle_matrix_covers_required_same_batch_cases(self):
        required_ids = {
            "initial_empty_success",
            "program_build_success",
            "business_collapse_failure",
            "failed_publish",
            "duplicate_program_identity",
            "duplicate_physical_edge_identity",
            "duplicate_business_edge_identity",
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
        for case in cases.values():
            with self.subTest(case=case["id"]):
                self.assertTrue(case["same_batch"])
                self.assertIsInstance(case["publish"], bool)
                self.assertIn("physical_result", case)
                self.assertIn("business_result", case)

    def test_lifecycle_matrix_encodes_fail_closed_and_scoped_deletion(self):
        cases = {case["id"]: case for case in self.matrix["cases"]}
        collapse_failure = cases["business_collapse_failure"]
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
        self.assertIn("zero", empty["physical_result"])
        self.assertIn("zero", empty["business_result"])

        for case_id in (
            "duplicate_program_identity",
            "duplicate_physical_edge_identity",
            "duplicate_business_edge_identity",
        ):
            with self.subTest(case=case_id):
                self.assertTrue(cases[case_id]["reject_duplicate_stable_identity"])
                self.assertFalse(cases[case_id]["publish"])

        self.assertTrue(cases["cross_profile_isolation"]["publish"])
        self.assertTrue(
            cases["inactive_edge_contamination"]["query_requires_active_batch_join"]
        )

    def test_frozen_business_lifecycle_semantics(self):
        business = table_block(self.ddl, "lineage_business_edge")
        self.assertRegex(business, r"collapse_depth\s+INTEGER NOT NULL")
        self.assertNotRegex(business, r"\bpath_count\b")
        self.assertIn("last_changed_at", business)
        invariants = "\n".join(self.matrix["invariants"])
        for invariant in (
            "Every published business row has collapse_depth >= 1",
            "does not persist path_count",
            "do not update business last_changed_at",
        ):
            with self.subTest(invariant=invariant):
                self.assertIn(invariant, invariants)
        for case_id in ("tmp_rename", "physical_path_change_same_endpoint"):
            self.assertIn(
                "last_changed_at remains unchanged",
                next(
                    case["active_result"]
                    for case in self.matrix["cases"]
                    if case["id"] == case_id
                ),
            )

    def test_lifecycle_matrix_covers_derived_rebuild_without_identity_guessing(self):
        cases = {case["id"]: case for case in self.matrix["cases"]}
        for case_id in ("source_hash_change", "pipeline_version_rebuild"):
            with self.subTest(case=case_id):
                self.assertTrue(cases[case_id]["rebuild"])
                self.assertTrue(cases[case_id]["same_batch"])

        self.assertTrue(cases["tmp_rename"]["stable_business_identity"])
        self.assertTrue(
            cases["physical_path_change_same_endpoint"]["stable_business_identity"]
        )
        self.assertIn("derivation", cases["tmp_rename"]["business_result"])


if __name__ == "__main__":
    unittest.main()
