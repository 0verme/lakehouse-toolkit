import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))


from ui import public_stream  # noqa: E402


class PublicStreamMetadataTests(unittest.TestCase):
    def test_load_disabled_registered_result_tables_fail_open(self):
        with patch(
            "ui.public_stream.all_disabled_result_tables",
            side_effect=RuntimeError("missing table"),
        ), patch("ui.public_stream.st.warning") as warning:
            result = public_stream.load_disabled_registered_result_tables()
        self.assertEqual(result, set())
        warning.assert_called_once()
        self.assertIn("部分元数据不可用", warning.call_args.args[0])

    def test_load_result_table_sys_name_map_fail_open(self):
        with patch(
            "ui.public_stream.all_result_table_sys_names",
            side_effect=RuntimeError("query failed"),
        ), patch("ui.public_stream.st.warning") as warning:
            result = public_stream.load_result_table_sys_name_map()
        self.assertEqual(result, {})
        warning.assert_called_once()
        self.assertIn("结果表来源系统映射", warning.call_args.args[0])

    def test_load_result_table_sys_name_map_filters_invalid_rows(self):
        rows = [
            ("dws.t_demo", "核心系统"),
            ("DWS.T_DEMO", "核心系统"),
            ("dws.t_demo", "业务示例系统"),
            ("", "ignored"),
            ("dws.t_tmp", None),
            ("dws.t_other", "  "),
        ]
        with patch("ui.public_stream.all_result_table_sys_names", return_value=rows):
            result = public_stream.load_result_table_sys_name_map()
        self.assertEqual(result, {"DWS.T_DEMO": ["核心系统", "业务示例系统"]})


if __name__ == "__main__":
    unittest.main()
