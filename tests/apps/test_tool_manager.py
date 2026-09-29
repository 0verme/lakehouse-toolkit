import importlib
import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

tool_manager = importlib.import_module("apps.webadmin.manager.tool_manager")
PORT_RANGES = {
    "tools/search": (7001, 7099),
    "tools/crypto": (8001, 8099),
    "tools/sql": (8101, 8199),
    "tools/cms": (8201, 8299),
    "tools/misc": (8301, 8399),
    "tools/jobgraph": (8401, 8499),
    "tools/czcb": (8501, 8599),
    "tools/lineage": (8601, 8699),
}
EXPECTED_PUBLIC_PORTS = {
    "xlsx_sql_tables": 8308,
    "xlsx_dependency_splitter": 8309,
    "workspace_search": 7005,
    "workspace_lineage": 7006,
    "table_lineage": 7007,
    "job_dependency_cycle": 8425,
    "lineage_reconciliation": 8601,
    "lineage_explorer": 8602,
}
WORK_TMP = Path("runtime/temp/tests_tool_manager")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def make_temp_dir():
    path = WORK_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class ToolManagerTests(unittest.TestCase):
    def assert_tool_port_contract(self, tools):
        used_ports = {}
        for tool in tools:
            port = int(tool.get("port") or 0)
            if port == 0:
                continue

            self.assertNotIn(
                port,
                used_ports,
                f"duplicate listener port {port}: "
                f"{used_ports.get(port)} and {tool['name']}",
            )
            used_ports[port] = tool["name"]

            port_range = PORT_RANGES.get(tool.get("workdir"))
            if port_range:
                self.assertLessEqual(port_range[0], port, tool["name"])
                self.assertLessEqual(port, port_range[1], tool["name"])

    def test_public_tools_follow_workdir_port_contract(self):
        data = tool_manager.load_tool_configuration(tool_manager.CONFIG_PATH)
        tools = data.get("tools", [])
        self.assert_tool_port_contract(tools)

        by_name = {tool["name"]: tool for tool in tools}
        for name, port in EXPECTED_PUBLIC_PORTS.items():
            self.assertEqual(by_name[name]["port"], port, name)
        self.assertEqual(by_name["xlsx_sql_tables"]["group"], "misc")
        self.assertEqual(by_name["xlsx_dependency_splitter"]["group"], "misc")
        self.assertEqual(by_name["job_dependency_cycle"]["group"], "jobgraph")

    def test_port_zero_and_apps_are_outside_fixed_tool_port_ranges(self):
        self.assert_tool_port_contract(
            [
                {"name": "no-listener", "workdir": "tools/misc", "port": 0},
                {"name": "legacy-app", "workdir": "apps/webadmin", "port": 9999},
            ]
        )
        command = tool_manager.build_command(
            {
                "type": "python",
                "workdir": "tools/misc",
                "script": "cli.py",
                "port": 0,
            }
        )
        self.assertNotIn("--port", command)

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
            with (
                patch.object(tool_manager, "CONFIG_PATH", config_path),
                patch.object(tool_manager, "LOCAL_CONFIG_PATH", tmp / "tools.local.yaml"),
            ):
                tools = tool_manager.load_tools()
                tool = tool_manager.get_tool("webadmin")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(len(tools), 1)
        self.assertEqual(tool["type"], "streamlit")
        self.assertEqual(tool["port"], 8501)

    def test_load_tools_appends_local_only_private_tool(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "tools.yaml"
            local_config_path = tmp / "tools.local.yaml"
            config_path.write_text(
                json.dumps({"tools": [{"name": "webadmin", "port": 8500}]}),
                encoding="utf-8",
            )
            local_config_path.write_text(
                json.dumps(
                    {
                        "tools": [
                            {
                                "name": "lineage_reconciliation",
                                "workdir": "tools/lineage",
                                "script": "reconcile_sql_schedule_web.py",
                                "port": 8603,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(tool_manager, "CONFIG_PATH", config_path),
                patch.object(tool_manager, "LOCAL_CONFIG_PATH", local_config_path),
            ):
                tools = tool_manager.load_tools()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(
            [tool["name"] for tool in tools], ["webadmin", "lineage_reconciliation"]
        )

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
            "port": 8101,
            "python": "python",
        }
        cmd = tool_manager.build_command(tool)
        self.assertEqual(cmd[0], sys.executable)
        self.assertTrue(cmd[1].replace("\\", "/").endswith("tools/sql/run_sql.py"))
        self.assertEqual(cmd[-4:], ["--host", "127.0.0.1", "--port", "8101"])

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
