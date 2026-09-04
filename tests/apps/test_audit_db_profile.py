import importlib
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

DEMO_OUTPUT_A = str(Path("runtime") / "fixtures" / "a.csv")
DEMO_OUTPUT_B = str(Path("runtime") / "fixtures" / "b.csv")


SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))


if importlib.util.find_spec("psycopg") is None:
    psycopg_stub = types.ModuleType("psycopg")
    psycopg_stub.connect = lambda *args, **kwargs: None
    sys.modules.setdefault("psycopg", psycopg_stub)


def clear_test_modules():
    prefixes = (
        "services.db_profile",
        "services.audit_metadata_service",
        "shared.db.postgres",
        "shared.db.gaussdb",
        "core.public_data",
    )
    for name in list(sys.modules):
        if name.startswith(prefixes):
            sys.modules.pop(name, None)


class AuditDbProfileTests(unittest.TestCase):
    def setUp(self):
        clear_test_modules()

    def tearDown(self):
        clear_test_modules()

    def test_resolver_uses_demo_profile_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            db_profile = importlib.import_module("services.db_profile")
            self.assertEqual(db_profile.get_active_audit_profile_name(), "demo_local")
            self.assertTrue(db_profile.is_postgres_profile())

    def test_resolver_prefers_explicit_demo_profile(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            db_profile = importlib.import_module("services.db_profile")
            self.assertEqual(db_profile.get_active_audit_profile_name(), "demo_local")
            self.assertEqual(db_profile.get_active_backend(), "postgres_native")

    def test_resolver_raises_clear_error_for_unknown_profile(self):
        with patch.dict(
            os.environ, {"AUDIT_DB_PROFILE": "missing_profile"}, clear=True
        ):
            db_profile = importlib.import_module("services.db_profile")
            with self.assertRaisesRegex(
                KeyError, "audit datasource profile not found: missing_profile"
            ):
                db_profile.get_active_audit_profile_name()

    def test_public_data_all_term_roots_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_term_roots",
            return_value=[("CUST",), ("ACCT",)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(public_data.all_term_roots(), [("CUST",), ("ACCT",)])

    def test_local_pg_list_view_names_uses_postgres_backend(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("demo_view",), ("ops_view",)],
            ) as fetch_all:
                rows = audit_metadata_service.list_view_names()
            self.assertEqual(rows, [("DEMO_VIEW",), ("OPS_VIEW",)])
            fetch_all.assert_called_once()
            self.assertIn("information_schema.views", fetch_all.call_args.args[0])

    def test_local_pg_list_function_names_uses_postgres_backend(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("demo_func",), ("ops_func",)],
            ) as fetch_all:
                rows = audit_metadata_service.list_function_names()
            self.assertEqual(rows, [("DEMO_FUNC",), ("OPS_FUNC",)])
            fetch_all.assert_called_once()
            self.assertIn("pg_proc", fetch_all.call_args.args[0])

    def test_public_data_all_view_names_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_view_names",
            return_value=[("DWS_DEMO_VIEW",), ("OPS_VIEW",)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_view_names(), [("DWS_DEMO_VIEW",), ("OPS_VIEW",)]
            )

    def test_public_data_all_function_names_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_function_names",
            return_value=[("DWS_DEMO_FUNC",), ("OPS_FUNC",)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_function_names(), [("DWS_DEMO_FUNC",), ("OPS_FUNC",)]
            )

    def test_local_pg_list_para_table_names_uses_demo_model(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("para_demo",), ("ods_ref",)],
            ) as fetch_all:
                rows = audit_metadata_service.list_para_table_names()
            self.assertEqual(rows, [("PARA_DEMO",), ("ODS_REF",)])
            self.assertIn("demo_meta.reference_tables", fetch_all.call_args.args[0])

    def test_public_data_all_para_table_lists_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_para_table_names",
            return_value=[("PARA_DEMO",), ("ODS_REF",)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_para_table_lists(), [("PARA_DEMO",), ("ODS_REF",)]
            )

    def test_local_pg_list_recv_mapping_plans_uses_demo_model(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("recv_day",), ("recv_hour",)],
            ) as fetch_all:
                rows = audit_metadata_service.list_recv_mapping_plans()
            self.assertEqual(rows, [("RECV_DAY",), ("RECV_HOUR",)])
            self.assertIn("demo_meta.receive_plans", fetch_all.call_args.args[0])

    def test_public_data_all_recv_mapping_plans_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_recv_mapping_plans",
            return_value=[("RECV_DAY",), ("RECV_HOUR",)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_recv_mapping_plans(), [("RECV_DAY",), ("RECV_HOUR",)]
            )

    def test_local_pg_list_job_outfiles_uses_demo_model(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("job_a", DEMO_OUTPUT_A), ("job_b", DEMO_OUTPUT_B)],
            ) as fetch_all:
                rows = audit_metadata_service.list_job_outfiles()
            self.assertEqual(rows, [("job_a", DEMO_OUTPUT_A), ("job_b", DEMO_OUTPUT_B)])
            self.assertIn("demo_meta.job_outputs", fetch_all.call_args.args[0])

    def test_public_data_all_job_outfile_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_job_outfiles",
            return_value=[("job_a", DEMO_OUTPUT_A)],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(public_data.all_job_outfile(), [("job_a", DEMO_OUTPUT_A)])

    def test_local_pg_list_result_table_sys_names_uses_demo_model(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("DWS.T_DEMO", "DEMO_SOURCE")],
            ) as fetch_all:
                rows = audit_metadata_service.list_result_table_sys_names()
            self.assertEqual(rows, [("DWS.T_DEMO", "DEMO_SOURCE")])
            self.assertIn("demo_meta.result_receipts", fetch_all.call_args.args[0])

    def test_public_data_all_result_table_sys_names_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_result_table_sys_names",
            return_value=[("DWS.T_DEMO", "DEMO_SOURCE")],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_result_table_sys_names(),
                [("DWS.T_DEMO", "DEMO_SOURCE")],
            )

    def test_local_pg_list_result_table_recv_details_uses_demo_model(self):
        with patch.dict(os.environ, {"AUDIT_DB_PROFILE": "demo_local"}, clear=True):
            audit_metadata_service = importlib.import_module(
                "services.audit_metadata_service"
            )
            with patch(
                "shared.db.postgres.fetch_all",
                return_value=[("DWS.T_DEMO", "DEMO_PLAN", "DEMO_SOURCE")],
            ) as fetch_all:
                rows = audit_metadata_service.list_result_table_recv_details()
            self.assertEqual(rows, [("DWS.T_DEMO", "DEMO_PLAN", "DEMO_SOURCE")])
            self.assertIn("demo_meta.result_receipts", fetch_all.call_args.args[0])

    def test_public_data_all_result_table_recv_details_keeps_tuple_rows(self):
        with patch(
            "services.audit_metadata_service.list_result_table_recv_details",
            return_value=[("DWS.T_DEMO", "DEMO_PLAN", "DEMO_SOURCE")],
        ):
            public_data = importlib.import_module("core.public_data")
            self.assertEqual(
                public_data.all_result_table_recv_details(),
                [("DWS.T_DEMO", "DEMO_PLAN", "DEMO_SOURCE")],
            )

    def test_postgres_backend_import_does_not_connect(self):
        postgres = importlib.import_module("shared.db.postgres")
        with patch.object(
            postgres.psycopg,
            "connect",
            side_effect=AssertionError("should not connect on import"),
        ):
            importlib.reload(postgres)


if __name__ == "__main__":
    unittest.main()
