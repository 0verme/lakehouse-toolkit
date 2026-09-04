import tempfile
import unittest
from pathlib import Path

from apps.svn_check.services.workspace_service import load_local_workspace


class WorkspaceServiceTests(unittest.TestCase):
    def test_load_local_workspace_scans_files_with_stable_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir)
            (workspace_root / "b_dir").mkdir()
            (workspace_root / "a_dir").mkdir()
            (workspace_root / "a_dir" / "a.sql").write_text(
                "select 1;", encoding="utf-8"
            )
            (workspace_root / "b_dir" / "b.py").write_text(
                "print('ok')", encoding="utf-8"
            )
            (workspace_root / "root.txt").write_text("demo", encoding="utf-8")

            ignored_dirs = [
                workspace_root / ".git",
                workspace_root / ".svn",
                workspace_root / "__pycache__",
                workspace_root / ".idea",
                workspace_root / ".vscode",
            ]
            for ignored_dir in ignored_dirs:
                ignored_dir.mkdir()
                (ignored_dir / "ignored.txt").write_text("ignored", encoding="utf-8")

            workspace_info = load_local_workspace(str(workspace_root))

            self.assertEqual(workspace_info["source_type"], "local")
            self.assertEqual(
                workspace_info["workspace_root"], str(workspace_root.resolve())
            )
            self.assertEqual(
                workspace_info["branch_changed_files"],
                ["a_dir/a.sql", "b_dir/b.py", "root.txt"],
            )
            self.assertEqual(
                workspace_info["exported_paths"],
                [
                    str((workspace_root / "a_dir" / "a.sql").resolve()),
                    str((workspace_root / "b_dir" / "b.py").resolve()),
                    str((workspace_root / "root.txt").resolve()),
                ],
            )
            self.assertTrue(
                all(
                    Path(path_str).is_absolute()
                    for path_str in workspace_info["exported_paths"]
                )
            )
            self.assertEqual(workspace_info["trunk_changed_files"], [])
            self.assertEqual(workspace_info["trunk_conflict_files"], [])

    def test_load_local_workspace_raises_clear_error_for_missing_directory(self):
        missing_dir = r"E:\definitely_missing_workspace_dir_for_tests"
        with self.assertRaisesRegex(
            FileNotFoundError, "Local workspace does not exist"
        ):
            load_local_workspace(missing_dir)

    def test_load_local_workspace_raises_clear_error_for_empty_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir, self.assertRaisesRegex(
            ValueError, "Local workspace has no files to audit"
        ):
            load_local_workspace(temp_dir)


if __name__ == "__main__":
    unittest.main()
