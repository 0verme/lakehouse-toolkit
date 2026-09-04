import unittest
from unittest.mock import patch

from shared.lineage.asset_tables import (
    AssetMappingLoadError,
    build_asset_plan_map,
    classify_asset_tables,
    load_asset_plan_map,
)


class AssetTablesTests(unittest.TestCase):
    def test_build_asset_plan_map_normalizes_values_and_skips_empty_rows(self):
        result = build_asset_plan_map(
            [
                (" demo_plan_ingest_day ", " dwf.f_demo_event "),
                (None, "DWF.EMPTY_PLAN"),
                ("PLAN_X", None),
            ]
        )

        self.assertEqual(result, {"DWF.F_DEMO_EVENT": "DEMO_PLAN_INGEST_DAY"})

    @patch("shared.lineage.asset_tables.select_sql_with_profile")
    def test_load_asset_plan_map_queries_once(self, select_sql):
        select_sql.return_value = [("DEMO_PLAN_INGEST_DAY", "DWF.F_DEMO_EVENT")]

        result = load_asset_plan_map()

        self.assertEqual(result, {"DWF.F_DEMO_EVENT": "DEMO_PLAN_INGEST_DAY"})
        select_sql.assert_called_once()

    @patch("shared.lineage.asset_tables.select_sql_with_profile", return_value=None)
    def test_load_asset_plan_map_raises_for_query_failure(self, _select_sql):
        with self.assertRaises(AssetMappingLoadError):
            load_asset_plan_map()

    def test_classify_asset_tables_dedupes_categories(self):
        plan_by_table = {
            "DWF.F_DEMO_EVENT": "DEMO_PLAN_INGEST_DAY",
            "DWF.F_DEMO_EXPORT": "DEMO_PLAN_EXPORT_DAY",
            "DWF.F_OTHER": "DEMO_PLAN_OTHER",
        }

        result = classify_asset_tables(
            [
                " dwf.f_demo_export ",
                "DWF.F_DEMO_EVENT",
                "DWF.F_DEMO_EVENT",
                "DWF.F_OTHER",
            ],
            plan_by_table,
        )

        self.assertEqual(result, ("demo_export", "demo_ingest"))


if __name__ == "__main__":
    unittest.main()
