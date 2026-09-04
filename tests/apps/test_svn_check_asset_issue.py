import unittest
from unittest.mock import patch

from apps.svn_check.core.asset_issue import (
    build_issue_hash_key,
    build_issue_key,
    create_audit_asset_issue,
    dedupe_issues,
)
from apps.svn_check.services.portal_link_builder import build_portal_link


class AuditAssetIssueTests(unittest.TestCase):
    def test_build_issue_hash_key_is_short_and_stable(self):
        issue_key = build_issue_key(
            issue_type="root_missing",
            source_module="lakehouse",
            source_file="demo.sql",
            schema_name="dwm",
            table_name="m_demo",
            field_name="cust_id",
            root_word="cust",
        )
        self.assertEqual(
            build_issue_hash_key(issue_key), build_issue_hash_key(issue_key)
        )
        self.assertEqual(len(build_issue_hash_key(issue_key)), 12)

    def test_create_audit_asset_issue_normalizes_names(self):
        issue = create_audit_asset_issue(
            issue_type="root_missing",
            issue_title="词根待维护",
            issue_desc="存在未维护词根",
            asset_type="root",
            source_module="lakehouse",
            source_file=" demo.sql ",
            severity="warning",
            suggestion="去维护",
            portal_module="root-management",
            action_label="去维护词根",
            schema_name="dwm",
            table_name="m_demo",
            field_name="cust_id",
            root_word="cust",
        )
        self.assertEqual(issue.issue_type, "ROOT_MISSING")
        self.assertEqual(issue.schema_name, "DWM")
        self.assertEqual(issue.table_name, "M_DEMO")
        self.assertEqual(issue.field_name, "CUST_ID")
        self.assertEqual(issue.root_word, "CUST")

    def test_dedupe_issues_removes_duplicate_issue_keys(self):
        issue_a = create_audit_asset_issue(
            issue_type="root_missing",
            issue_title="词根待维护",
            issue_desc="存在未维护词根",
            asset_type="root",
            source_module="lakehouse",
            source_file="demo.sql",
            severity="warning",
            suggestion="去维护",
            portal_module="root-management",
            action_label="去维护词根",
            root_word="cust",
        )
        issue_b = create_audit_asset_issue(
            issue_type="root_missing",
            issue_title="词根待维护",
            issue_desc="存在未维护词根",
            asset_type="root",
            source_module="lakehouse",
            source_file="demo.sql",
            severity="warning",
            suggestion="去维护",
            portal_module="root-management",
            action_label="去维护词根",
            root_word="cust",
        )
        self.assertEqual(len(dedupe_issues([issue_a, issue_b])), 1)

    def test_build_portal_link_uses_urlencode_for_root_query(self):
        issue = create_audit_asset_issue(
            issue_type="root_missing",
            issue_title="词根待维护",
            issue_desc="存在未维护词根",
            asset_type="root",
            source_module="lakehouse",
            source_file="demo.sql",
            severity="warning",
            suggestion="去维护",
            portal_module="root-management",
            action_label="去维护词根",
            root_word="客户 主档",
        )
        self.assertEqual(
            build_portal_link(issue),
            "http://localhost:5099/root-management?q=%E5%AE%A2%E6%88%B7+%E4%B8%BB%E6%A1%A3",
        )

    def test_build_portal_link_uses_qualified_table_name_for_review(self):
        issue = create_audit_asset_issue(
            issue_type="asset_table_review",
            issue_title="资产表待核对",
            issue_desc="SQL中引用了待核对资产表",
            asset_type="table",
            source_module="fine",
            source_file="demo.cpt",
            severity="warning",
            suggestion="去核对",
            portal_module="data-warehouse",
            action_label="去核对资产表",
            schema_name="dwm",
            table_name="m_demo",
        )
        self.assertEqual(
            build_portal_link(issue),
            "http://localhost:5099/data-warehouse?q=DWM.M_DEMO",
        )

    @patch.dict(
        "os.environ", {"ASSET_PORTAL_BASE_URL": "http://127.0.0.1:5099/"}, clear=False
    )
    def test_build_portal_link_prefers_env_base_url(self):
        issue = create_audit_asset_issue(
            issue_type="root_missing",
            issue_title="词根待维护",
            issue_desc="存在未维护词根",
            asset_type="root",
            source_module="lakehouse",
            source_file="demo.sql",
            severity="warning",
            suggestion="去维护",
            portal_module="root-management",
            action_label="去维护词根",
            root_word="cust",
        )
        self.assertEqual(
            build_portal_link(issue),
            "http://127.0.0.1:5099/root-management?q=CUST",
        )


if __name__ == "__main__":
    unittest.main()
