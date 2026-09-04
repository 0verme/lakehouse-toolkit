import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))

_ddl_rule: Any = __import__("core.lakehouse.ddl_rule", fromlist=["*"])
collect_root_missing_issues = _ddl_rule.collect_root_missing_issues


class LakehouseRootIssueCollectionTests(unittest.TestCase):
    @patch("core.lakehouse.ddl_rule.all_term_roots", return_value=[("CUST",), ("ID",)])
    def test_collect_root_missing_issues_builds_root_issue_for_table_and_column(
        self, _mock_term_roots
    ):
        sql_text = """
        CREATE TABLE DWM.M_CUST_RISK (
            CUST_SCORE DECIMAL(18,2),
            RISK_TAG VARCHAR(32)
        );
        """
        issues = collect_root_missing_issues(
            sql_text,
            source_module="lakehouse",
            source_file="demo/dws.sql",
        )
        issue_roots = sorted(issue.root_word for issue in issues)
        self.assertEqual(issue_roots, ["RISK", "RISK", "SCORE", "TAG"])
        self.assertTrue(all(issue.issue_type == "ROOT_MISSING" for issue in issues))
        self.assertTrue(
            all(issue.portal_module == "root-management" for issue in issues)
        )
        self.assertTrue(
            all(
                issue.portal_url.startswith("http://localhost:5099/root-management?q=")
                for issue in issues
            )
        )


if __name__ == "__main__":
    unittest.main()
