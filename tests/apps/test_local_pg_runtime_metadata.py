import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))


def clear_test_modules():
    prefixes = (
        "services.db_profile",
        "services.db_service",
        "core.public_data",
        "shared.db.postgres",
        "shared.lineage.mapping_sqlite",
    )
    for name in list(sys.modules):
        if name.startswith(prefixes):
            sys.modules.pop(name, None)


class LocalPgRuntimeMetadataTests(unittest.TestCase):
    def setUp(self):
        clear_test_modules()

    def tearDown(self):
        clear_test_modules()

    def test_db_service_routes_default_demo_reads_to_local_pg(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            db_service = importlib.import_module("services.db_service")
            with patch(
                "shared.db.postgres.fetch_all", return_value=[("demo",)]
            ) as fetch_all:
                rows = db_service.select_sql("select * from demo_meta.jobs")
            self.assertEqual(rows, [("demo",)])
            fetch_all.assert_called_once()

    def test_public_data_get_job2_uses_postgres_safe_case_expression(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            public_data = importlib.import_module("core.public_data")
            with patch(
                "shared.db.postgres.fetch_all", return_value=[("DEMO_JOB", "启用")]
            ) as fetch_all:
                rows = public_data.get_job2()
            self.assertEqual(rows, [("DEMO_JOB", "启用")])
            self.assertIn("case", fetch_all.call_args.args[0].lower())

    def test_public_data_all_plan_routes_demo_plan_rows_to_local_pg(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            public_data = importlib.import_module("core.public_data")
            demo_rows = [("DEMO_PLAN_DAY", "", "Demo day plan", "", "", "")]
            with patch(
                "shared.db.postgres.fetch_all", return_value=demo_rows
            ) as fetch_all:
                rows = public_data.all_plan()
            self.assertEqual(rows, demo_rows)
            self.assertIn("from demo_meta.plans", fetch_all.call_args.args[0].lower())

    def test_public_data_all_job_routes_demo_jobs_to_local_pg(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            public_data = importlib.import_module("core.public_data")
            demo_rows = [
                ("DEMO_PLAN_DAY", "DEMO_SEQ_DAY", "DEMO_JOB_DWM_ACCT") + ("",) * 25,
                ("DEMO_PLAN_DAY", "DEMO_SEQ_DAY", "DEMO_JOB_PUSH_RESULT") + ("",) * 25,
            ]
            with patch(
                "shared.db.postgres.fetch_all", return_value=demo_rows
            ) as fetch_all:
                rows = public_data.all_job()
            self.assertEqual(rows, demo_rows)
            self.assertEqual(
                {row[2] for row in rows}, {"DEMO_JOB_DWM_ACCT", "DEMO_JOB_PUSH_RESULT"}
            )
            self.assertIn("from demo_meta.jobs", fetch_all.call_args.args[0].lower())

    def test_public_data_all_program_routes_demo_programs_to_local_pg(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            public_data = importlib.import_module("core.public_data")
            demo_rows = [
                (
                    "DEMO_PROGRAM_DWM_ACCT",
                    "DEMO_PROGRAM_DWM_ACCT",
                    "Demo DWM account program",
                    "python",
                    "/demo/WORKSPACE/DWM/DWM.M_DEMO_ACCT/demo_job.py",
                    "",
                    "",
                    "",
                    "",
                    "",
                ),
                (
                    "DEMO_PROGRAM_PUSH_RESULT",
                    "DEMO_PROGRAM_PUSH_RESULT",
                    "Demo push result program",
                    "python",
                    "/demo/WORKSPACE/DWE/DWP.DWE_DEMO_PUSH_RESULT/demo_push.py",
                    "",
                    "",
                    "",
                    "",
                    "",
                ),
            ]
            with patch(
                "shared.db.postgres.fetch_all", return_value=demo_rows
            ) as fetch_all:
                rows = public_data.all_program()
            self.assertEqual(rows, demo_rows)
            self.assertEqual(
                {row[1] for row in rows},
                {"DEMO_PROGRAM_DWM_ACCT", "DEMO_PROGRAM_PUSH_RESULT"},
            )
            self.assertIn(
                "from demo_meta.programs", fetch_all.call_args.args[0].lower()
            )

    def test_mapping_sqlite_load_registered_result_tables_uses_local_pg_when_available(
        self,
    ):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            mapping_sqlite = importlib.import_module("shared.lineage.mapping_sqlite")
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("DWM.M_DEMO_ACCT",), ("DWP.DWE_DEMO_PUSH_RESULT",)],
            ) as fetch_all:
                rows = mapping_sqlite.load_registered_result_tables()
            self.assertEqual(rows, {"DWM.M_DEMO_ACCT", "DWP.DWE_DEMO_PUSH_RESULT"})
            fetch_all.assert_called_once()


if __name__ == "__main__":
    unittest.main()
