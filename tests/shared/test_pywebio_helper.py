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
                                "port": 8301,
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

        self.assertEqual(port, 8301)


if __name__ == "__main__":
    unittest.main()
