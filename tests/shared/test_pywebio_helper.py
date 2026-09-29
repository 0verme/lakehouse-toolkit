import json
import shutil
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

pywebio_stub = types.ModuleType("pywebio")
pywebio_stub.__dict__.update(
    {
        "config": lambda **kwargs: None,
        "start_server": lambda *args, **kwargs: None,
        "__path__": [],
    }
)
input_stub = types.ModuleType("pywebio.input")
input_stub.__dict__.update({"TEXT": "text", "textarea": lambda *args, **kwargs: ""})
output_stub = types.ModuleType("pywebio.output")
output_stub.__dict__.update(
    {
        "put_html": lambda *args, **kwargs: None,
        "put_markdown": lambda *args, **kwargs: None,
        "put_text": lambda *args, **kwargs: None,
    }
)
sys.modules.update(
    {
        "pywebio": pywebio_stub,
        "pywebio.input": input_stub,
        "pywebio.output": output_stub,
    }
)

pywebio_helper = __import__("shared.ui.pywebio_helper", fromlist=["*"])


WORK_TMP = Path("runtime/temp/tests_pywebio_helper")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def make_temp_dir():
    path = WORK_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class PywebioHelperTests(unittest.TestCase):
    def test_lineage_pages_are_in_the_public_management_registry(self):
        pywebio_helper.load_tools_config.cache_clear()
        try:
            tools = {
                tool["name"]: tool for tool in pywebio_helper.load_tools_config()
            }
        finally:
            pywebio_helper.load_tools_config.cache_clear()

        self.assertIn("lineage_reconciliation", tools)
        self.assertIn("lineage_explorer", tools)
        self.assertEqual(tools["lineage_explorer"]["title"], "血缘查询 / Explorer")

    def test_resolve_registered_port_from_local_only_tool_config(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "tools.yaml"
            local_config_path = tmp / "tools.local.yaml"
            config_path.write_text(json.dumps({"tools": []}), encoding="utf-8")
            local_config_path.write_text(
                json.dumps(
                    {
                        "tools": [
                            {
                                "name": "lineage_reconciliation",
                                "title": "SQL / 调度血缘对账",
                                "workdir": "tools/lineage",
                                "script": "reconcile_sql_schedule_web.py",
                                "port": 8603,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            main_module = types.SimpleNamespace(
                __file__="tools/lineage/reconcile_sql_schedule_web.py"
            )
            with (
                patch.object(pywebio_helper, "TOOLS_CONFIG_PATH", config_path),
                patch.object(
                    pywebio_helper, "LOCAL_TOOLS_CONFIG_PATH", local_config_path
                ),
                patch.dict(sys.modules, {"__main__": main_module}),
            ):
                pywebio_helper.load_tools_config.cache_clear()
                port = pywebio_helper.resolve_registered_port()
                title = pywebio_helper.resolve_tool_title("default")
        finally:
            pywebio_helper.load_tools_config.cache_clear()
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(port, 8603)
        self.assertEqual(title, "SQL / 调度血缘对账")

    def test_put_red_text_escapes_untrusted_markup(self):
        with patch.object(pywebio_helper, "put_markdown") as put_markdown:
            pywebio_helper.put_red_text("<script>alert(1)</script>")

        self.assertEqual(
            put_markdown.call_args.args[0],
            "<p style=\"color:red;\">&lt;script&gt;alert(1)&lt;/script&gt;</p>",
        )

    def test_resolve_registered_port_from_tools_config(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "tools.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "tools": [
                            {
                                "name": "job_downstream_zs",
                                "title": "追数下游生成工具",
                                "workdir": "tools/misc",
                                "script": "job_downstream_zs.py",
                                "port": 8307,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            main_module = types.SimpleNamespace(
                __file__="tools/misc/job_downstream_zs.py"
            )
            with patch.object(pywebio_helper, "TOOLS_CONFIG_PATH", config_path):
                pywebio_helper.load_tools_config.cache_clear()
                with patch.dict(sys.modules, {"__main__": main_module}):
                    port = pywebio_helper.resolve_registered_port()
        finally:
            pywebio_helper.load_tools_config.cache_clear()
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(port, 8307)


if __name__ == "__main__":
    unittest.main()
