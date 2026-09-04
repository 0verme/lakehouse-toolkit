import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))

_python_rule: Any = __import__("core.lakehouse.python_rule", fromlist=["*"])
_sql_rule: Any = __import__("core.lakehouse.sql_rule", fromlist=["*"])
build_asset_table_review_issues = _python_rule.build_asset_table_review_issues
collect_created_table_review_issues = _sql_rule.collect_created_table_review_issues


class LakehouseAssetTableReviewCollectionTests(unittest.TestCase):
    def test_build_asset_table_review_issues_dedupes_tables_and_builds_links(self):
        issues = build_asset_table_review_issues(
            ["dwm.m_demo", "DWM.M_DEMO", "tmp.mid_table"],
            source_module="lakehouse",
            source_file="demo/program.py",
        )
        issue_tables = sorted(
            f"{issue.schema_name}.{issue.table_name}"
            if issue.schema_name
            else issue.table_name
            for issue in issues
        )
        self.assertEqual(issue_tables, ["DWM.M_DEMO", "TMP.MID_TABLE"])
        self.assertTrue(
            all(issue.issue_type == "ASSET_TABLE_REVIEW" for issue in issues)
        )
        self.assertTrue(
            all(issue.portal_module == "data-warehouse" for issue in issues)
        )
        self.assertTrue(
            all(
                issue.portal_url.startswith("http://localhost:5099/data-warehouse?q=")
                for issue in issues
            )
        )

    @patch("core.lakehouse.sql_rule.read_data_from_file")
    def test_collect_created_table_review_issues_uses_created_tables(
        self, mock_read_data
    ):
        mock_read_data.return_value = """
        CREATE TABLE DWM.M_NEW_TABLE (
            CUST_ID VARCHAR(32)
        );
        CREATE TABLE TMP.MID_STEP (
            STEP_ID VARCHAR(32)
        );
        """
        issues = collect_created_table_review_issues(
            "demo/dws.sql", source_module="lakehouse"
        )
        issue_tables = sorted(
            f"{issue.schema_name}.{issue.table_name}"
            if issue.schema_name
            else issue.table_name
            for issue in issues
        )
        self.assertEqual(issue_tables, ["DWM.M_NEW_TABLE", "TMP.MID_STEP"])


if __name__ == "__main__":
    unittest.main()
