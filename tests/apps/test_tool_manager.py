import importlib
import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

tool_manager = importlib.import_module("apps.webadmin.manager.tool_manager")
WORK_TMP = Path("runtime/temp/tests_tool_manager")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def make_temp_dir():
    path = WORK_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class ToolManagerTests(unittest.TestCase):
    def test_load_tools_and_get_tool(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "tools.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "tools": [
                            {
                                "name": "webadmin",
                                "type": "streamlit",
                                "workdir": "apps/webadmin",
                                "script": "app.py",
                                "host": "127.0.0.1",
                                "port": 8501,
                                "log": "logs/webadmin.log",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(tool_manager, "CONFIG_PATH", config_path):
                tools = tool_manager.load_tools()
                tool = tool_manager.get_tool("webadmin")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(len(tools), 1)
        self.assertEqual(tool["type"], "streamlit")
        self.assertEqual(tool["port"], 8501)

    def test_build_command_for_streamlit_relative_path(self):
        tool = {
            "name": "webadmin",
            "type": "streamlit",
            "workdir": "apps/webadmin",
            "script": "app.py",
            "host": "127.0.0.1",
            "port": 8501,
        }
        cmd = tool_manager.build_command(tool)
        self.assertEqual(cmd[0:4], [sys.executable, "-m", "streamlit", "run"])
        self.assertTrue(cmd[4].replace("\\", "/").endswith("apps/webadmin/app.py"))
        self.assertIn("--server.port", cmd)
        self.assertIn("8501", cmd)

    def test_build_command_for_python_relative_path(self):
        tool = {
            "name": "sql-tool",
            "type": "python",
            "workdir": "tools/sql",
            "script": "run_sql.py",
            "host": "127.0.0.1",
            "port": 8020,
            "python": "python",
        }
        cmd = tool_manager.build_command(tool)
        self.assertEqual(cmd[0], sys.executable)
        self.assertTrue(cmd[1].replace("\\", "/").endswith("tools/sql/run_sql.py"))
        self.assertEqual(cmd[-4:], ["--host", "127.0.0.1", "--port", "8020"])

    def test_resolve_path_keeps_absolute_path(self):
        path = tool_manager.resolve_path(str(Path("apps/webadmin").resolve()))
        self.assertTrue(path.is_absolute())
        self.assertTrue(str(path).replace("\\", "/").endswith("apps/webadmin"))

    def test_pid_file_uses_configured_pid_dir(self):
        tmp = make_temp_dir()
        try:
            pid_dir = tmp / "pids"
            with patch.object(tool_manager, "PID_DIR", pid_dir):
                self.assertEqual(tool_manager.pid_file("demo"), pid_dir / "demo.pid")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
