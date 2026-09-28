import json
import tempfile
import unittest
from pathlib import Path

from shared.config.tool_registry import load_tool_configuration


class ToolRegistryTests(unittest.TestCase):
    def test_local_config_overrides_public_and_appends_private_tools(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            public_config = root / "tools.yaml"
            local_config = root / "tools.local.yaml"
            public_config.write_text(
                json.dumps(
                    {
                        "public_base_url": "http://localhost",
                        "tools": [
                            {"name": "webadmin", "port": 8500, "enable": True}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            local_config.write_text(
                json.dumps(
                    {
                        "public_base_url": "http://deployment.invalid",
                        "tools": [
                            {"name": "webadmin", "host": "127.0.0.1"},
                            {
                                "name": "private_tool",
                                "workdir": "apps/private_tool",
                                "script": "app.py",
                                "port": 8501,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_tool_configuration(public_config, local_config)

        self.assertEqual(config["public_base_url"], "http://deployment.invalid")
        self.assertEqual(
            config["tools"],
            [
                {
                    "name": "webadmin",
                    "port": 8500,
                    "enable": True,
                    "host": "127.0.0.1",
                },
                {
                    "name": "private_tool",
                    "workdir": "apps/private_tool",
                    "script": "app.py",
                    "port": 8501,
                },
            ],
        )

    def test_missing_local_config_keeps_public_registry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            public_config = root / "tools.yaml"
            public_config.write_text(
                json.dumps({"tools": [{"name": "webadmin"}]}),
                encoding="utf-8",
            )

            config = load_tool_configuration(
                public_config, root / "missing-tools.local.yaml"
            )

        self.assertEqual(config["tools"], [{"name": "webadmin"}])


if __name__ == "__main__":
    unittest.main()
