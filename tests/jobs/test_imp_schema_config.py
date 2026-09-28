import unittest

from jobs.crontab.imp_schema_config import build_create_table_sql


class SchemaConfigImportTests(unittest.TestCase):
    def test_build_create_table_sql_quotes_validated_columns(self):
        sql = build_create_table_sql(
            "demo_meta.schema_config", ["source_name", "record_type"]
        )

        self.assertEqual(
            sql,
            'CREATE TABLE demo_meta.schema_config (\n'
            '    "source_file" TEXT,\n'
            '    "source_name" TEXT,\n'
            '    "record_type" TEXT\n'
            ");",
        )

    def test_build_create_table_sql_rejects_unsafe_identifiers(self):
        with self.assertRaises(ValueError):
            build_create_table_sql("demo_meta.schema_config; DROP TABLE users", [])
        with self.assertRaises(ValueError):
            build_create_table_sql("demo_meta.schema_config", ["bad name"])


if __name__ == "__main__":
    unittest.main()
