import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from jobs.crontab._bootstrap import ensure_project_root_on_path


class CronBootstrapTests(unittest.TestCase):
    def test_adds_project_root_to_path_idempotently(self):
        project_root = str(Path(__file__).resolve().parents[2])
        isolated_path = ["/external/site-packages"]
        with patch.object(sys, "path", isolated_path):
            first_result = ensure_project_root_on_path()
            second_result = ensure_project_root_on_path()
            self.assertEqual(first_result, project_root)
            self.assertEqual(second_result, project_root)
            self.assertEqual(sys.path.count(project_root), 1)


if __name__ == "__main__":
    unittest.main()
