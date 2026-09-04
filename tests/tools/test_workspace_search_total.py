from __future__ import annotations

import sys
import types
import unittest
from typing import Any

pywebio_stub = types.ModuleType("pywebio")
pywebio_stub.__dict__.update(
    {
        "config": lambda **kwargs: None,
        "start_server": lambda *args, **kwargs: None,
        "__path__": [],
    }
)
input_stub = types.ModuleType("pywebio.input")
input_stub.__dict__.update(
    {
        "TEXT": "text",
        "checkbox": lambda *args, **kwargs: None,
        "input_group": lambda *args, **kwargs: None,
        "textarea": lambda *args, **kwargs: None,
    }
)
output_stub = types.ModuleType("pywebio.output")
output_stub.__dict__.update(
    {
        "put_file": lambda *args, **kwargs: None,
        "put_html": lambda *args, **kwargs: None,
        "put_markdown": lambda *args, **kwargs: None,
        "put_progressbar": lambda *args, **kwargs: None,
        "put_text": lambda *args, **kwargs: None,
        "set_progressbar": lambda *args, **kwargs: None,
    }
)
sys.modules.update(
    {
        "pywebio": pywebio_stub,
        "pywebio.input": input_stub,
        "pywebio.output": output_stub,
    }
)

_workspace_search: Any = __import__(
    "tools.search.workspace_search_total", fromlist=["*"]
)
build_result_table = _workspace_search.build_result_table


class BuildResultTableTests(unittest.TestCase):
    def setUp(self):
        self.context = {
            "settings": type(
                "Settings",
                (),
                {
                    "lakehouse_http_root": "http://example.test/lakehouse",
                    "upstream_http_root": "http://example.test/upstream",
                    "fine_http_root": "http://example.test/fine",
                    "upstream_url": "examples/upstream",
                    "fine_url": "examples/reports",
                },
            )(),
        }

    def test_builds_detail_rows_for_display_and_export(self):
        rows = build_result_table(
            ["DWF.F_DEMO_TABLE", "DEMO_ID"],
            [["湖仓", "demo.sql", "/demo/workspace/demo.sql"]],
            downstream_only=False,
            context=self.context,
        )

        self.assertEqual(["结果类型", "搜索结果", "下载链接"], rows[0])
        self.assertEqual(["湖仓", "demo.sql"], rows[1][:2])
        self.assertIn("http://example.test/lakehouse/demo.sql", rows[1][2])

    def test_builds_empty_detail_row(self):
        rows = build_result_table(
            ["DWF.F_DEMO_TABLE"],
            [],
            downstream_only=False,
            context=self.context,
        )

        self.assertEqual(["-", "未找到依赖对象", ""], rows[1])

    def test_builds_downstream_summary(self):
        rows = build_result_table(
            ["DWF.F_DEMO_TABLE"],
            [["湖仓", "demo.sql", "demo.sql"]],
            downstream_only=True,
            context=self.context,
        )

        self.assertEqual(
            [["关键字", "下游依赖"], ["DWF.F_DEMO_TABLE", "有下游"]],
            rows,
        )


if __name__ == "__main__":
    unittest.main()
