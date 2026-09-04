import unittest
from datetime import datetime

from tools.misc.job_downstream_zs import (
    append_suffix,
    build_export_text,
    build_sheet_rows,
    build_split_zip_bytes,
    build_xls_bytes,
    create_export_filename,
    create_run_suffix,
    split_rows_preserving_dependencies,
    transform_rows,
)


class JobDownstreamZsTests(unittest.TestCase):
    def test_create_run_suffix_uses_timestamp_format(self):
        suffix = create_run_suffix(datetime(2026, 6, 8, 14, 30, 45))
        self.assertEqual(suffix, "20260608143045")

    def test_create_export_filename_is_short_and_stable(self):
        name = create_export_filename(
            [
                "JOB_DWS_DWS_DWF_F_AGT_SAVB_BASICINFO_H_00_DAY",
                "JOB_DWS_DWS_DWF_F_AGT_ACCR_SAVEBUSI_BOOK_H_00_DAY",
            ]
        )
        self.assertTrue(name.startswith("PLAN_DWS_ZS_"))
        self.assertTrue(name.endswith(".xls"))

    def test_transform_rows_keeps_all_columns_and_cleans_ab(self):
        columns = ["a", "b", "c", "ab", "ac"]
        rows = [
            {"a": "PLAN_A", "b": "x1", "c": "ABC", "ab": "", "ac": "memo_a"},
            {
                "a": "PLAN_B",
                "b": "x2",
                "c": "JOB_1",
                "ab": "33:ABC|33:EXT_JOB",
                "ac": "memo_b",
            },
            {"a": "PLAN_C", "b": "x3", "c": "JOB_2", "ab": "33:JOB_1", "ac": "memo_c"},
            {"a": "PLAN_X", "b": "x4", "c": "EXT_JOB", "ab": "", "ac": "memo_x"},
        ]

        result, not_found = transform_rows(
            ["ABC"], columns, rows, suffix="20260608143045"
        )

        self.assertEqual(not_found, [])
        self.assertEqual(
            [row["c"] for row in result],
            ["ABC_20260608143045", "JOB_1_20260608143045", "JOB_2_20260608143045"],
        )
        self.assertEqual(result[0]["a"], "PLAN_DWS_ZS_20260608143045")
        self.assertEqual(result[1]["a"], "PLAN_DWS_ZS_20260608143045")
        self.assertEqual(result[1]["ab"], "33:ABC_20260608143045")
        self.assertEqual(result[2]["ab"], "33:JOB_1_20260608143045")
        self.assertEqual(result[1]["b"], "SEQ_DWS_ZS_20260608143045")
        self.assertEqual(result[1]["ac"], "memo_b")
        self.assertEqual(set(result[0].keys()), set(columns))

    def test_transform_rows_supports_multiple_input_jobs(self):
        columns = ["a", "c", "ab"]
        rows = [
            {"a": "PLAN_A", "c": "ABC", "ab": ""},
            {"a": "PLAN_B", "c": "JOB_1", "ab": "33:ABC"},
            {"a": "PLAN_C", "c": "XYZ", "ab": ""},
            {"a": "PLAN_D", "c": "JOB_2", "ab": "33:XYZ|33:JOB_1"},
        ]

        result, not_found = transform_rows(
            ["ABC", "XYZ"], columns, rows, suffix="20260608143045"
        )

        self.assertEqual(not_found, [])
        self.assertEqual(
            [row["c"] for row in result],
            [
                "ABC_20260608143045",
                "JOB_1_20260608143045",
                "JOB_2_20260608143045",
                "XYZ_20260608143045",
            ],
        )
        self.assertEqual(
            result[2]["ab"], "33:XYZ_20260608143045|33:JOB_1_20260608143045"
        )
        self.assertTrue(all("b" not in row for row in result))

    def test_transform_rows_can_exclude_start_jobs_from_result(self):
        columns = ["a", "c", "ab"]
        rows = [
            {"a": "PLAN_A", "c": "ABC", "ab": ""},
            {"a": "PLAN_B", "c": "JOB_1", "ab": "33:ABC"},
            {"a": "PLAN_C", "c": "JOB_2", "ab": "33:JOB_1"},
        ]

        result, not_found = transform_rows(
            ["ABC"],
            columns,
            rows,
            include_start_jobs=False,
            suffix="20260608143045",
        )

        self.assertEqual(not_found, [])
        self.assertEqual(
            [row["c"] for row in result],
            ["JOB_1_20260608143045", "JOB_2_20260608143045"],
        )
        self.assertEqual(result[0]["ab"], "")
        self.assertEqual(result[1]["ab"], "33:JOB_1_20260608143045")

    def test_transform_rows_can_exclude_send_day_plan_rows(self):
        columns = ["a", "c", "ab"]
        rows = [
            {"a": "PLAN_A", "c": "ABC", "ab": ""},
            {"a": "PUSH_SEND1_DAY", "c": "JOB_1", "ab": "33:ABC"},
            {"a": "PUSH_SEND2_DAY", "c": "JOB_2", "ab": "33:JOB_1"},
            {"a": "PLAN_C", "c": "JOB_3", "ab": "33:JOB_2|33:ABC"},
        ]

        result, not_found = transform_rows(
            ["ABC"],
            columns,
            rows,
            suffix="20260608143045",
            exclude_send_day_plan=True,
        )

        self.assertEqual(not_found, [])
        self.assertEqual(
            [row["c"] for row in result], ["ABC_20260608143045", "JOB_3_20260608143045"]
        )
        self.assertEqual(result[1]["ab"], "33:ABC_20260608143045")

    def test_build_export_text_uses_original_column_count(self):
        columns = ["a", "b", "c", "ab"]
        rows = [
            {
                "a": "PLAN_DWS_ZS_20260608143045",
                "b": "SEQ_DWS_ZS_20260608143045",
                "c": "ABC_20260608143045",
                "ab": "",
            },
        ]

        text = build_export_text(columns, rows)

        self.assertEqual(text.splitlines()[0], "a\tb\tc\tab")
        self.assertEqual(
            text.splitlines()[1],
            "PLAN_DWS_ZS_20260608143045\tSEQ_DWS_ZS_20260608143045\tABC_20260608143045\t",
        )

    def test_build_sheet_rows_keeps_header_and_values(self):
        columns = ["a", "c"]
        rows = [
            {"a": "PLAN_DWS_ZS_20260608143045", "c": "ABC_20260608143045"},
            {"a": "PLAN_DWS_ZS_20260608143045", "c": "JOB_1_20260608143045"},
        ]

        sheet_rows = build_sheet_rows(columns, rows)

        self.assertEqual(sheet_rows[0], ["a", "c"])
        self.assertEqual(
            sheet_rows[1], ["PLAN_DWS_ZS_20260608143045", "ABC_20260608143045"]
        )
        self.assertEqual(
            sheet_rows[2], ["PLAN_DWS_ZS_20260608143045", "JOB_1_20260608143045"]
        )

    def test_build_xls_bytes_returns_content(self):
        rows = [
            ["a", "c"],
            ["PLAN_DWS_ZS_20260608143045", "ABC_20260608143045"],
        ]

        content = build_xls_bytes("job_downstream_zs", rows)

        self.assertTrue(content.startswith(b"\xd0\xcf\x11\xe0"))
        self.assertGreater(len(content), 100)

    def test_split_rows_preserving_dependencies_duplicates_required_rows(self):
        columns = ["a", "b", "c", "ab"]
        rows = [
            {"a": "PLAN", "b": "SEQ", "c": "ROOT", "ab": ""},
            {"a": "PLAN", "b": "SEQ", "c": "JOB_1", "ab": "33:ROOT"},
            {"a": "PLAN", "b": "SEQ", "c": "JOB_2", "ab": "33:ROOT"},
            {"a": "PLAN", "b": "SEQ", "c": "JOB_3", "ab": "33:ROOT"},
        ]

        chunks = split_rows_preserving_dependencies(columns, rows, max_rows_per_file=2)

        self.assertEqual(len(chunks), 3)
        self.assertEqual([row["c"] for row in chunks[0]], ["ROOT", "JOB_1"])
        self.assertEqual([row["c"] for row in chunks[1]], ["ROOT", "JOB_2"])
        self.assertEqual([row["c"] for row in chunks[2]], ["ROOT", "JOB_3"])

    def test_split_rows_preserving_dependencies_keeps_ab_internal(self):
        columns = ["a", "c", "ab"]
        rows = [
            {"a": "PLAN", "c": "ROOT", "ab": ""},
            {"a": "PLAN", "c": "MID_1", "ab": "33:ROOT"},
            {"a": "PLAN", "c": "MID_2", "ab": "33:ROOT"},
        ]

        chunks = split_rows_preserving_dependencies(columns, rows, max_rows_per_file=2)

        for chunk in chunks:
            job_names = {row["c"] for row in chunk}
            for row in chunk:
                for dep in row["ab"].split("|") if row["ab"] else []:
                    self.assertIn(dep.split(":", 1)[1], job_names)

    def test_split_rows_preserving_dependencies_raises_when_single_chain_too_long(self):
        columns = ["a", "c", "ab"]
        rows = [
            {"a": "PLAN", "c": "ROOT", "ab": ""},
            {"a": "PLAN", "c": "MID", "ab": "33:ROOT"},
            {"a": "PLAN", "c": "LEAF", "ab": "33:MID"},
        ]

        with self.assertRaises(ValueError):
            split_rows_preserving_dependencies(columns, rows, max_rows_per_file=2)

    def test_build_split_zip_bytes_returns_zip_content(self):
        columns = ["a", "c", "ab"]
        chunks = [
            [{"a": "PLAN", "c": "ROOT", "ab": ""}],
            [{"a": "PLAN", "c": "MID", "ab": "33:ROOT"}],
        ]

        content = build_split_zip_bytes(columns, chunks, "job_downstream_zs_demo.xls")

        self.assertTrue(content.startswith(b"PK"))

    def test_append_suffix_is_idempotent(self):
        self.assertEqual(
            append_suffix("PLAN_A", "_20260608143045"), "PLAN_A_20260608143045"
        )
        self.assertEqual(
            append_suffix("PLAN_A_20260608143045", "_20260608143045"),
            "PLAN_A_20260608143045",
        )


if __name__ == "__main__":
    unittest.main()
