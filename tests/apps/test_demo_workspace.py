import importlib
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

SVN_CHECK_APP_ROOT = Path(__file__).resolve().parents[2] / "apps" / "svn_check"
if str(SVN_CHECK_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(SVN_CHECK_APP_ROOT))


pd: Any = importlib.import_module("pandas")
_file_utils: Any = importlib.import_module("core.lakehouse.file_utils")
_python_rule: Any = importlib.import_module("core.lakehouse.python_rule")
_schedule_rule: Any = importlib.import_module("core.lakehouse.schedule_rule")
_re_service: Any = importlib.import_module("services.re_service")
_workspace_service: Any = importlib.import_module("services.workspace_service")
all_program_df = _file_utils.all_program_df
get_lakehouse_type = _file_utils.get_lakehouse_type
rule_dws_py = _python_rule.rule_dws_py
rule_excle_job = _schedule_rule.rule_excle_job
rule_excle_plan = _schedule_rule.rule_excle_plan
rule_excle_seq = _schedule_rule.rule_excle_seq
build_dependency_table_lookup = _re_service.build_dependency_table_lookup
build_job_outfile_lookup = _re_service.build_job_outfile_lookup
build_program_lookup = _re_service.build_program_lookup
build_wide_table_lineage_summary = _re_service.build_wide_table_lineage_summary
get_program_lookup_result = _re_service.get_program_lookup_result
get_yilai_table_from_lookup = _re_service.get_yilai_table_from_lookup
load_xls_to_df = _re_service.load_xls_to_df
merge_job_program = _re_service.merge_job_program
load_local_workspace = _workspace_service.load_local_workspace


FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "demo_workspace"
)

DEMO_JOB_ROWS = [
    (
        "DEMO_PLAN_DAY",
        "DEMO_SEQ_DAY",
        "DEMO_JOB_DWM_ACCT",
        "Demo account build job",
        "DEMO_PROGRAM_DWM_ACCT",
        "DEMO_DOMAIN",
        "99",
        "",
        "",
        "SYS_EVERYDAY_CALENDAR",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "1",
        "",
        "-schkey:2:schkey=DEMO_CORE:0",
        "",
        "33:DEMO_JOB_PUSH_RESULT",
    ),
    (
        "DEMO_PLAN_DAY",
        "DEMO_SEQ_DAY",
        "DEMO_JOB_PUSH_RESULT",
        "Demo push result job",
        "DEMO_PROGRAM_PUSH_RESULT",
        "DEMO_DOMAIN",
        "99",
        "",
        "",
        "SYS_EVERYDAY_CALENDAR",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "1",
        "",
        "-outfile:2:outfile=/demo/out/DEMO_JOB_PUSH_RESULT.csv:0",
        "",
        "",
    ),
]

DEMO_PROGRAM_ROWS = [
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

DEMO_JOB_OUTFILE_ROWS = [
    ("DEMO_JOB_PUSH_RESULT", "/demo/out/DEMO_JOB_PUSH_RESULT.csv"),
]

DEMO_RESULT_TABLE_RECV_DETAILS = {
    "DWP.DWE_DEMO_PUSH_RESULT": [
        {
            "recv_plan": "DEMO_PLAN_RECV_DAY",
            "source_system": "DEMO SOURCE SYSTEM",
        }
    ]
}


class LocalLakehouseDemoWorkspaceTests(unittest.TestCase):
    def _load_fixture_workspace_parts(self):
        workspace_info = load_local_workspace(str(FIXTURE_DIR))
        exported_paths = workspace_info["exported_paths"]
        (
            dws_url,
            _hive_url,
            _schema_configs,
            _sbin_lists,
            _recv_lists,
            _dwo_lists,
            _dwf_lists,
            _dlo_meta,
            _dlo_lists,
            py_lists,
            plan_xls,
            seq_xls,
            job_xls,
            program_xls,
            _cale_xls,
        ) = get_lakehouse_type(exported_paths)
        return {
            "workspace_info": workspace_info,
            "exported_paths": exported_paths,
            "dws_url": dws_url,
            "py_lists": py_lists,
            "plan_xls": plan_xls,
            "seq_xls": seq_xls,
            "job_xls": job_xls,
            "program_xls": program_xls,
        }

    def test_fixture_workspace_contains_minimum_lakehouse_inputs(self):
        parts = self._load_fixture_workspace_parts()
        self.assertTrue(parts["dws_url"].endswith("dws.sql"))
        self.assertTrue(parts["py_lists"])
        self.assertTrue(parts["plan_xls"].endswith(".xls"))
        self.assertTrue(parts["seq_xls"].endswith(".xls"))
        self.assertTrue(parts["job_xls"].endswith(".xls"))
        self.assertTrue(parts["program_xls"].endswith(".xls"))

    def test_fixture_schedule_rules_execute_with_demo_metadata(self):
        parts = self._load_fixture_workspace_parts()
        plan_df = load_xls_to_df(parts["plan_xls"]).iloc[:, [0, 4]].fillna("")
        seq_df = load_xls_to_df(parts["seq_xls"]).iloc[:, [0, 1, 2]].fillna("")
        job_df = load_xls_to_df(parts["job_xls"]).fillna("")
        plan_df.columns = ["计划名", "前置依赖"]
        seq_df.columns = ["计划名", "作业流名", "作业流描述"]

        with (
            patch(
                "core.lakehouse.schedule_rule.all_plan",
                return_value=[("DEMO_PLAN_DAY", "", "Demo day plan", "", "", "")],
            ),
            patch(
                "core.lakehouse.schedule_rule.all_recv_mapping_plans",
                return_value=[("DEMO_PLAN_RECV_DAY",)],
            ),
            patch("core.lakehouse.schedule_rule.all_real_seq", return_value=[]),
            patch(
                "core.lakehouse.schedule_rule.all_job_outfile",
                return_value=[
                    ("DEMO_JOB_PUSH_RESULT", "/demo/out/DEMO_JOB_PUSH_RESULT.csv")
                ],
            ),
        ):
            plan_result_text, plan_warn_text, plan_cnt, r_plan = rule_excle_plan(
                plan_df
            )
            seq_result_text, seq_warn_text, seq_cnt = rule_excle_seq(seq_df)
            job_result_text, job_warn_text, job_cnt = rule_excle_job(
                job_df, r_plan=r_plan, job_rows=DEMO_JOB_ROWS
            )

        self.assertEqual(plan_result_text, "")
        self.assertEqual(plan_warn_text, "")
        self.assertEqual(plan_cnt, 0)
        self.assertIn("DEMO_PLAN_DAY", r_plan)
        self.assertEqual(seq_result_text, "")
        self.assertEqual(seq_warn_text, "")
        self.assertEqual(seq_cnt, 0)
        self.assertIsInstance(job_result_text, str)
        self.assertIsInstance(job_warn_text, str)
        self.assertEqual(job_cnt, 0)

    def test_fixture_program_rule_executes_on_demo_script(self):
        parts = self._load_fixture_workspace_parts()
        py_path = parts["py_lists"][0]
        with (
            patch("core.lakehouse.python_rule.all_sstb", return_value=[]),
            patch(
                "core.lakehouse.python_rule.all_view_names",
                return_value=[("DWP.V_DEMO_CUSTOMER",)],
            ),
            patch(
                "core.lakehouse.python_rule.all_function_names",
                return_value=[("DWP.DEMO_AMOUNT_BUCKET",)],
            ),
            patch("core.lakehouse.python_rule.run_dws_ddl_rules", return_value=[]),
            patch("core.lakehouse.python_rule.all_tab_partitions", return_value=[[0]]),
        ):
            result_text, warn_text, cnt, sql_table = rule_dws_py(py_path)

        self.assertIsInstance(result_text, str)
        self.assertIsInstance(warn_text, str)
        self.assertIsInstance(cnt, int)
        self.assertEqual(sql_table, [])
        self.assertGreaterEqual(cnt, 1)
        self.assertTrue(result_text)

    def test_fixture_wide_table_chain_maps_job_to_program_and_dependency_table(self):
        parts = self._load_fixture_workspace_parts()
        job_df = load_xls_to_df(parts["job_xls"]).fillna("")
        program_df = load_xls_to_df(parts["program_xls"]).fillna("")
        mergejob_df = pd.concat(
            [job_df, pd.DataFrame(DEMO_JOB_ROWS, columns=job_df.columns)],
            ignore_index=True,
        )

        with patch(
            "core.lakehouse.file_utils.all_program", return_value=DEMO_PROGRAM_ROWS
        ):
            mergeprogram_df = all_program_df(program_df)

        program_path_col = mergeprogram_df.columns[4]
        mergetotal_df = merge_job_program(mergejob_df, mergeprogram_df)
        program_lookup = build_program_lookup(
            mergetotal_df, program_path_col, tail_levels=4
        )
        lookup_result = get_program_lookup_result(
            program_lookup, parts["py_lists"][0], tail_levels=4
        )
        dependency_lookup = build_dependency_table_lookup(mergetotal_df)
        dependency_tables = get_yilai_table_from_lookup(
            lookup_result[2], dependency_lookup
        )

        self.assertEqual(lookup_result[0], "DEMO_JOB_DWM_ACCT")
        self.assertEqual(lookup_result[1], "SYS_EVERYDAY_CALENDAR")
        self.assertEqual(lookup_result[2], "33:DEMO_JOB_PUSH_RESULT")
        self.assertEqual(dependency_tables, ["DWP.DWE_DEMO_PUSH_RESULT"])

    def test_build_wide_table_lineage_summary_returns_demo_chain(self):
        parts = self._load_fixture_workspace_parts()
        job_df = load_xls_to_df(parts["job_xls"]).fillna("")
        program_df = load_xls_to_df(parts["program_xls"]).fillna("")
        mergejob_df = pd.concat(
            [job_df, pd.DataFrame(DEMO_JOB_ROWS, columns=job_df.columns)],
            ignore_index=True,
        )

        with patch(
            "core.lakehouse.file_utils.all_program", return_value=DEMO_PROGRAM_ROWS
        ):
            mergeprogram_df = all_program_df(program_df)

        mergetotal_df = merge_job_program(mergejob_df, mergeprogram_df)
        summary = build_wide_table_lineage_summary(
            mergetotal_df,
            parts["py_lists"][0],
            job_outfile_lookup=build_job_outfile_lookup(DEMO_JOB_OUTFILE_ROWS),
            result_table_recv_detail_map=DEMO_RESULT_TABLE_RECV_DETAILS,
            tail_levels=4,
        )

        self.assertEqual(summary["plan_name"], "DEMO_PLAN_DAY")
        self.assertEqual(summary["job_name"], "DEMO_JOB_DWM_ACCT")
        self.assertEqual(summary["program_name"], "DEMO_PROGRAM_DWM_ACCT")
        self.assertEqual(summary["result_table"], "DWM.M_DEMO_ACCT")
        self.assertEqual(summary["dependency_jobs"], ["DEMO_JOB_PUSH_RESULT"])
        self.assertEqual(
            summary["dependency_result_tables"], ["DWP.DWE_DEMO_PUSH_RESULT"]
        )
        self.assertEqual(summary["recv_plan"], "DEMO_PLAN_RECV_DAY")
        self.assertEqual(summary["source_system"], "DEMO SOURCE SYSTEM")
        self.assertEqual(summary["outfile"], "/demo/out/DEMO_JOB_PUSH_RESULT.csv")
        self.assertEqual(summary["missing_steps"], [])

    def test_build_wide_table_lineage_summary_returns_missing_steps_when_dependency_is_absent(
        self,
    ):
        parts = self._load_fixture_workspace_parts()
        job_df = load_xls_to_df(parts["job_xls"]).fillna("")
        program_df = load_xls_to_df(parts["program_xls"]).fillna("")
        mergejob_df = pd.concat(
            [job_df, pd.DataFrame(DEMO_JOB_ROWS[:1], columns=job_df.columns)],
            ignore_index=True,
        )

        with patch("core.lakehouse.file_utils.all_program", return_value=[]):
            mergeprogram_df = all_program_df(program_df)

        mergetotal_df = merge_job_program(mergejob_df, mergeprogram_df)
        summary = build_wide_table_lineage_summary(
            mergetotal_df,
            parts["py_lists"][0],
            job_outfile_lookup={},
            result_table_recv_detail_map={},
            tail_levels=4,
        )

        self.assertTrue(summary["missing_steps"])
        self.assertEqual(summary["dependency_jobs"], ["DEMO_JOB_PUSH_RESULT"])
        self.assertEqual(summary["dependency_result_tables"], [])
        self.assertEqual(summary["recv_plan"], "")
        self.assertEqual(summary["source_system"], "")
        self.assertEqual(summary["outfile"], "")


if __name__ == "__main__":
    unittest.main()
