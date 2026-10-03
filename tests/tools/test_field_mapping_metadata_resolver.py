from __future__ import annotations

import ast
import contextlib
import io
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from jobs.crontab import sync_ods_dwf_field_mappings as entry
from tools.field_mapping import benchmark_resolver, metadata_resolver
from tools.field_mapping.collector import collect_workspace
from tools.field_mapping.metadata_resolver import (
    MetadataResolver,
    derive_recv_namespace,
    match_dwo_source,
)
from tools.field_mapping.models import RecvDwfRecord, SchemaConfigRecord


def _systems(*items):
    return {"items": [{"id": name, "upstreamSystemId": value} for name, value in items]}


def _resolver(recv, schemas=(), systems=()):
    return MetadataResolver(recv, schemas, _systems(*systems))


class RecvNamespaceDerivationTests(unittest.TestCase):
    REAL_INTERNAL_CASES = (
        ("PLAN_SA_RECV_ABS5_CBS_ABS5_DAY", "CBS_ABS5", "ABS5"),
        ("PLAN_SA_RECV_CBS_CBS_KUANYE_DAY", "CBS_KUANYE", "CBS"),
        ("PLAN_SA_RECV_KUANYE_KUANYENEW_DAY", "KUANYENEW", "KUANYE"),
        ("PLAN_SA_RECV_KUANYET15_KUANYENEWT15_DAY", "KUANYENEWT15", "KUANYET15"),
        ("PLAN_SA_RECV_CBS_CBS_CBSRUN_PRO", "CBS_CBSRUN", "CBS"),
        ("PLAN_SA_RECV_WD_DEFENSOR_WD_DEFENSOR_DAY", "WD_DEFENSOR", "WD_DEFENSOR"),
        (
            "PLAN_SA_RECV_NUPS_DATA_NUPS_DATA_SOURCE_DAY",
            "NUPS_DATA_SOURCE",
            "NUPS_DATA",
        ),
    )

    def test_real_internal_cases_use_full_plan_and_full_data_source(self):
        for recv_plan, data_source, expected in self.REAL_INTERNAL_CASES:
            with self.subTest(recv_plan=recv_plan):
                self.assertEqual(
                    expected, derive_recv_namespace(recv_plan, data_source)
                )

    def test_suffix_is_not_hardcoded_to_day_or_night(self):
        for suffix in ("DAY", "NIGHT", "PRO", "WEEKLY", ""):
            plan = (
                f"PLAN_SA_RECV_CBS_CBS_CBSRUN_{suffix}"
                if suffix
                else "PLAN_SA_RECV_CBS_CBS_CBSRUN"
            )
            with self.subTest(suffix=suffix):
                self.assertEqual("CBS", derive_recv_namespace(plan, "CBS_CBSRUN"))

    def test_fails_closed_when_namespace_cannot_be_derived(self):
        self.assertIsNone(derive_recv_namespace("DEMO_SYSTEM_A", "DEMO_SYSTEM_A"))
        self.assertIsNone(
            derive_recv_namespace("PLAN_SA_RECV_CBS_OTHER_DAY", "CBS_KUANYE")
        )
        self.assertIsNone(derive_recv_namespace("PLAN_SA_RECV_ABS5_CBS_ABS5_DAY", ""))
        self.assertIsNone(
            derive_recv_namespace("PLAN_SA_RECV__CBS_ABS5_DAY", "CBS_ABS5")
        )

    def test_multiple_legal_splits_fail_closed(self):
        self.assertIsNone(derive_recv_namespace("PLAN_SA_RECV_A_B_A_B_DAY", "B"))
        self.assertIsNone(derive_recv_namespace("PLAN_SA_RECV_A_DS_DS", "DS"))


class MatchDwoSourceTests(unittest.TestCase):
    def test_namespace_boundary_match_and_multi_token_namespace(self):
        source = match_dwo_source("DWO.DWO_CBS_BTH_MCHT_AUTO_CHANGE", "CBS")
        self.assertEqual("CBS", source.recv_namespace)
        self.assertEqual("BTH_MCHT_AUTO_CHANGE", source.source_table)
        self.assertEqual("DWO.DWO_CBS_BTH_MCHT_AUTO_CHANGE", source.physical_table)
        self.assertIsNone(source.db_schema)

        multi = match_dwo_source("DWO.DWO_NUPS_DATA_PISA_A_B", "NUPS_DATA")
        self.assertEqual("NUPS_DATA", multi.recv_namespace)
        self.assertEqual("PISA_A_B", multi.source_table)

    def test_naive_prefix_is_rejected(self):
        for physical in ("DWO.DWO_CBSX_TABLE", "DWO.DWO_CBS2_TABLE", "DWO.DWO_CB_TABLE"):
            with self.subTest(physical=physical):
                self.assertIsNone(match_dwo_source(physical, "CBS"))

    def test_empty_source_table_and_non_dwo_relation_are_rejected(self):
        self.assertIsNone(match_dwo_source("DWO.DWO_CBS", "CBS"))
        self.assertIsNone(match_dwo_source("SCHEMA.DWO_CBS_X", "CBS"))
        self.assertIsNone(match_dwo_source("DWO.DWO_CBS_X", ""))


def _resolve(
    *,
    target: str,
    recv_plan: str,
    data_source: str,
    physical: str,
    db_schema: str | None = None,
    ods_job_name: str = "",
    program_names=(),
    systems=None,
    upstream: int = 101,
):
    recv = [RecvDwfRecord(recv_plan, target, data_source, ods_job_name=ods_job_name)]
    schemas = [SchemaConfigRecord(data_source, db_schema)] if db_schema else []
    upstreams = (
        [{"id": recv_plan, "upstreamSystemId": upstream}]
        if systems is None
        else [
            item
            if isinstance(item, dict)
            else {"id": item[0], "upstreamSystemId": item[1]}
            for item in systems
        ]
    )
    return MetadataResolver(recv, schemas, {"items": upstreams}).resolve(
        target=target, program_names=program_names, physical_source=physical
    )


class MetadataResolverModelTests(unittest.TestCase):
    def test_case_a_traditional_alignment(self):
        result = _resolve(
            target="DWF.DWF_AB_ACCT_IMAGE_DOSSIER",
            recv_plan="PLAN_SA_RECV_ABS5_CBS_ABS5_DAY",
            data_source="CBS_ABS5",
            db_schema="ABS5",
            physical="DWO.DWO_ABS5_AB_ACCT_IMAGE_DOSSIER",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertEqual("ABS5", result.source.recv_namespace)
        self.assertEqual("AB_ACCT_IMAGE_DOSSIER", result.source.source_table)
        self.assertEqual("ABS5", result.source.db_schema)
        self.assertEqual(101, result.upstream_system_id)
        self.assertEqual("PLAN_SA_RECV_ABS5_CBS_ABS5_DAY", result.record.recv_plan)
        self.assertIn("recv_namespace", result.evidence)

    def test_case_b_legacy_kuanye_recv_namespace_differs_from_db_schema(self):
        result = _resolve(
            target="DWF.DWF_BTH_MCHT_AUTO_CHANGE",
            recv_plan="PLAN_SA_RECV_CBS_CBS_KUANYE_DAY",
            data_source="CBS_KUANYE",
            db_schema="KUANYE",
            physical="DWO.DWO_CBS_BTH_MCHT_AUTO_CHANGE",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertEqual("CBS", result.source.recv_namespace)
        self.assertEqual("BTH_MCHT_AUTO_CHANGE", result.source.source_table)
        self.assertEqual("KUANYE", result.source.db_schema)

    def test_case_c_new_kuanye_recv_namespace_differs_from_db_schema(self):
        result = _resolve(
            target="DWF.DWF_D_DEVICE_OPERATE_LOG",
            recv_plan="PLAN_SA_RECV_KUANYE_KUANYENEW_DAY",
            data_source="KUANYENEW",
            db_schema="BMP",
            physical="DWO.DWO_KUANYE_D_DEVICE_OPERATE_LOG",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertEqual("KUANYE", result.source.recv_namespace)
        self.assertEqual("D_DEVICE_OPERATE_LOG", result.source.source_table)
        self.assertEqual("BMP", result.source.db_schema)

    def test_case_d_pro_suffix_is_supported(self):
        result = _resolve(
            target="DWF.DWF_COMC_PROFESSION_NEWCODE",
            recv_plan="PLAN_SA_RECV_CBS_CBS_CBSRUN_PRO",
            data_source="CBS_CBSRUN",
            physical="DWO.DWO_CBS_COMC_PROFESSION_NEWCODE",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertEqual("CBS", result.source.recv_namespace)
        self.assertEqual("COMC_PROFESSION_NEWCODE", result.source.source_table)

    def test_case_e_program_mismatch_does_not_block_unique_recv_namespace(self):
        recv = [
            RecvDwfRecord(
                "PLAN_SA_RECV_ACS_ACS_CBSRUN_DAY",
                "DWF.DWF_EVT_ACCD_ACCOUNT_LIST",
                "ACS_CBSRUN",
                ods_job_name="JOB_DWS_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_00_DAY",
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_CBS_CBS_CBSRUN_DAY",
                "DWF.DWF_EVT_ACCD_ACCOUNT_LIST",
                "CBS_CBSRUN",
                ods_job_name="JOB_DWS_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_00_DAY",
            ),
        ]
        resolver = _resolver(
            recv,
            (),
            (
                ("PLAN_SA_RECV_ACS_ACS_CBSRUN_DAY", 11),
                ("PLAN_SA_RECV_CBS_CBS_CBSRUN_DAY", 22),
            ),
        )
        program = ("005_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_1_00.py",)
        args = {"target": "DWF.DWF_EVT_ACCD_ACCOUNT_LIST", "program_names": program}
        acs = resolver.resolve(
            **args, physical_source="DWO.DWO_ACS_ACCD_ACCOUNT_LIST"
        )
        cbs = resolver.resolve(
            **args, physical_source="DWO.DWO_CBS_ACCD_ACCOUNT_LIST"
        )
        self.assertEqual(
            ("RESOLVED", "ACS_CBSRUN", 11, "ACS"),
            (
                acs.status,
                acs.record.data_source,
                acs.upstream_system_id,
                acs.source.recv_namespace,
            ),
        )
        self.assertEqual(
            ("RESOLVED", "CBS_CBSRUN", 22, "CBS"),
            (
                cbs.status,
                cbs.record.data_source,
                cbs.upstream_system_id,
                cbs.source.recv_namespace,
            ),
        )
        self.assertNotIn("ods_job_name", acs.evidence)

    def test_case_f_equivalent_duplicate_rows_do_not_conflict(self):
        recv = [
            RecvDwfRecord(
                "PLAN_SA_RECV_CBS_CBS_KUANYE_DAY",
                "DWF.DWF_BTH_MCHT_AUTO_CHANGE",
                "CBS_KUANYE",
                ods_job_name="JOB_Z_DAY",
            ),
            RecvDwfRecord(
                "plan_sa_recv_cbs_cbs_kuanye_day",
                "DWF.DWF_BTH_MCHT_AUTO_CHANGE",
                "cbs_kuanye",
                ods_job_name="JOB_A_DAY",
            ),
        ]
        result = _resolver(
            recv, (), (("PLAN_SA_RECV_CBS_CBS_KUANYE_DAY", 7),)
        ).resolve(
            target="DWF.DWF_BTH_MCHT_AUTO_CHANGE",
            program_names=(),
            physical_source="DWO.DWO_CBS_BTH_MCHT_AUTO_CHANGE",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertEqual("cbs_kuanye", result.record.data_source)

    def test_case_g_multiple_recv_namespace_identities_conflict(self):
        recv = [
            RecvDwfRecord(
                "PLAN_SA_RECV_DTSELL_DTSELL_CZCB_BC_DAY",
                "DWF.DWF_PUB_OPER_LOG",
                "DTSELL_CZCB_BC",
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DTSELL_DTSELL_CZCB_MB_DAY",
                "DWF.DWF_PUB_OPER_LOG",
                "DTSELL_CZCB_MB",
            ),
        ]
        result = _resolver(
            recv,
            (),
            (
                ("PLAN_SA_RECV_DTSELL_DTSELL_CZCB_BC_DAY", 1),
                ("PLAN_SA_RECV_DTSELL_DTSELL_CZCB_MB_DAY", 2),
            ),
        ).resolve(
            target="DWF.DWF_PUB_OPER_LOG",
            program_names=(),
            physical_source="DWO.DWO_DTSELL_PUB_OPER_LOG",
        )
        self.assertEqual(
            ("CONFLICT", "multiple_recv_namespace_conflict"),
            (result.status, result.reason),
        )

    def test_case_h_unresolved_tfs_is_not_guessed_by_first_token(self):
        result = _resolve(
            target="DWF.DWF_BOP_N",
            recv_plan="PLAN_SA_RECV_TFS_RCPMIS_KF_TFS_RCPMIS_KF_ODS_DAY",
            data_source="TFS_RCPMIS_KF_ODS",
            physical="DWO.DWO_TFS_DL_BOP_N",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_recv_namespace_match"), (result.status, result.reason)
        )

    def test_case_i_wd_defensor_db_schema_only_match_stays_diagnostic(self):
        result = _resolve(
            target="DWF.DWF_REPORT_REQUEST",
            recv_plan="PLAN_SA_RECV_WD_DEFENSOR_WD_DEFENSOR_DAY",
            data_source="WD_DEFENSOR",
            db_schema="DEFENSOR",
            physical="DWO.DWO_DEFENSOR_REPORT_REQUEST",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_recv_namespace_match"), (result.status, result.reason)
        )

    def test_case_j_missing_schema_config_still_resolves_with_honest_null_db_schema(
        self,
    ):
        result = _resolve(
            target="DWF.DWF_AB_ACCT_IMAGE_DOSSIER",
            recv_plan="PLAN_SA_RECV_ABS5_CBS_ABS5_DAY",
            data_source="CBS_ABS5",
            db_schema=None,
            physical="DWO.DWO_ABS5_AB_ACCT_IMAGE_DOSSIER",
        )
        self.assertEqual(("RESOLVED", None), (result.status, result.reason))
        self.assertIsNone(result.source.db_schema)

    def test_multiple_db_schema_rows_for_one_data_source_never_pick_one(self):
        recv = [
            RecvDwfRecord(
                "PLAN_SA_RECV_ABS5_CBS_ABS5_DAY",
                "DWF.DWF_AB_ACCT_IMAGE_DOSSIER",
                "CBS_ABS5",
            )
        ]
        resolver = _resolver(
            recv,
            (
                SchemaConfigRecord("CBS_ABS5", "ABS5"),
                SchemaConfigRecord("CBS_ABS5", "ABS5_OTHER"),
            ),
            (("PLAN_SA_RECV_ABS5_CBS_ABS5_DAY", 5),),
        )
        result = resolver.resolve(
            target="DWF.DWF_AB_ACCT_IMAGE_DOSSIER",
            program_names=(),
            physical_source="DWO.DWO_ABS5_AB_ACCT_IMAGE_DOSSIER",
        )
        self.assertEqual("RESOLVED", result.status)
        self.assertIsNone(result.source.db_schema)

    def test_no_recv_dwf(self):
        result = _resolver(
            [
                RecvDwfRecord(
                    "PLAN_SA_RECV_ABS5_CBS_ABS5_DAY",
                    "DWF.DWF_OTHER_TARGET",
                    "CBS_ABS5",
                )
            ],
            (),
            (),
        ).resolve(
            target="DWF.DWF_TARGET",
            program_names=(),
            physical_source="DWO.DWO_ABS5_AB_ACCT_IMAGE_DOSSIER",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_recv_dwf"), (result.status, result.reason)
        )

    def test_recv_namespace_unresolved_when_no_candidate_follows_the_anchor_shape(
        self,
    ):
        result = _resolve(
            target="DWF.DWF_TARGET",
            recv_plan="DEMO_SYSTEM_A",
            data_source="DEMO_SYSTEM_A",
            physical="DWO.DWO_DEMO_SCHEMA_A_SOURCE",
        )
        self.assertEqual(
            ("UNRESOLVED", "recv_namespace_unresolved"),
            (result.status, result.reason),
        )

    def test_no_recv_namespace_match_when_some_candidates_are_underivable(self):
        recv = [
            RecvDwfRecord("DEMO_SYSTEM_A", "DWF.DWF_TARGET", "DEMO_SYSTEM_A"),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_B_OTHER_SYSTEM_DAY",
                "DWF.DWF_TARGET",
                "OTHER_SYSTEM",
            ),
        ]
        result = _resolver(recv, (), ()).resolve(
            target="DWF.DWF_TARGET",
            program_names=(),
            physical_source="DWO.DWO_DEMO_SCHEMA_A_SOURCE",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_recv_namespace_match"), (result.status, result.reason)
        )

    def test_no_dwo_source_for_non_dwo_physical_relation(self):
        result = _resolve(
            target="DWF.DWF_TARGET",
            recv_plan="PLAN_SA_RECV_ABS5_CBS_ABS5_DAY",
            data_source="CBS_ABS5",
            physical="DWF.DWF_TARGET",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_dwo_source"), (result.status, result.reason)
        )

    def test_unknown_upstream_system_and_upstream_system_conflict(self):
        args = {
            "target": "DWF.DWF_BTH_MCHT_AUTO_CHANGE",
            "recv_plan": "PLAN_SA_RECV_CBS_CBS_KUANYE_DAY",
            "data_source": "CBS_KUANYE",
            "physical": "DWO.DWO_CBS_BTH_MCHT_AUTO_CHANGE",
        }
        unknown = _resolve(**args, systems=[])
        self.assertEqual(
            ("UNRESOLVED", "unknown_upstream_system"),
            (unknown.status, unknown.reason),
        )
        self.assertEqual("CBS", unknown.source.recv_namespace)
        conflicted = _resolve(
            **args,
            systems=(
                ("PLAN_SA_RECV_CBS_CBS_KUANYE_DAY", 7),
                ("plan_sa_recv_cbs_cbs_kuanye_day", 8),
            ),
        )
        self.assertEqual(
            ("CONFLICT", "upstream_system_conflict"),
            (conflicted.status, conflicted.reason),
        )

    def test_cache_returns_identical_resolution_and_keeps_inputs_separate(self):
        recv = [
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",
                "DWF.DWF_DEMO_TARGET",
                "DEMO_SYSTEM_A",
                ods_job_name="JOB_DEMO_DAY",
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_B_DEMO_SYSTEM_B_DAY",
                "DWF.DWF_DEMO_OTHER",
                "DEMO_SYSTEM_B",
                ods_job_name="JOB_OTHER_DAY",
            ),
        ]
        resolver = _resolver(
            recv,
            (),
            (
                ("PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY", 10),
                ("PLAN_SA_RECV_DEMO_SCHEMA_B_DEMO_SYSTEM_B_DAY", 20),
            ),
        )
        first = resolver.resolve(
            target="DWF.F_DEMO_TARGET",
            program_names=("005_DEMO.py", "JOB_DEMO_DAY", "005_DEMO.py"),
            physical_source='"DWO"."DWO_DEMO_SCHEMA_A_SOURCE"',
        )
        equivalent = resolver.resolve(
            target="DWF.DWF_DEMO_TARGET",
            program_names=("JOB_DEMO_DAY", "005_DEMO.py"),
            physical_source="DWO.DWO_DEMO_SCHEMA_A_SOURCE",
        )
        other_target = resolver.resolve(
            target="DWF.DWF_DEMO_OTHER",
            program_names=("JOB_OTHER_DAY",),
            physical_source="DWO.DWO_DEMO_SCHEMA_B_SOURCE",
        )
        self.assertEqual(
            ("RESOLVED", None),
            (first.status, first.reason),
        )
        self.assertIs(first, equivalent)
        self.assertEqual(("RESOLVED", None), (other_target.status, other_target.reason))
        self.assertEqual(2, len(resolver._resolution_cache))
        self.assertIn("ods_job_name", first.evidence)


class CollectorModelTests(unittest.TestCase):
    @staticmethod
    def _write_project(root: Path, name: str, target: str, physical: str) -> None:
        project = root / name
        project.mkdir()
        (project / "mapping.sql").write_text(
            f"INSERT INTO {target} (ID) SELECT s.ID FROM {physical} s",
            encoding="utf-8",
        )

    @staticmethod
    def _collect(root: Path, resolver: MetadataResolver):
        with contextlib.redirect_stderr(io.StringIO()):
            return collect_workspace(root, resolver)

    def test_collector_audits_recv_namespace_and_configured_db_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._write_project(
                root,
                "DWS_DWF.DWF_DEMO_TARGET",
                "DWF.DWF_DEMO_TARGET",
                "DWO.DWO_DEMO_SCHEMA_A_SOURCE",
            )
            resolver = _resolver(
                [
                    RecvDwfRecord(
                        "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",
                        "DWF.DWF_DEMO_TARGET",
                        "DEMO_SYSTEM_A",
                    )
                ],
                (SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A"),),
                (("PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY", 101),),
            )
            audit = self._collect(root, resolver)

        self.assertEqual(1, audit.summary["resolution"]["resolved_field_mappings"])
        self.assertEqual("DEMO_SCHEMA_A", audit.resolved[0]["dbSchema"])
        self.assertEqual(
            "table_name;recv_namespace;dap_upstream_system",
            audit.resolved[0]["evidence"],
        )

    def test_collector_keeps_db_schema_honest_without_schema_config(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._write_project(
                root,
                "DWS_DWF.DWF_DEMO_TARGET",
                "DWF.DWF_DEMO_TARGET",
                "DWO.DWO_DEMO_SCHEMA_A_SOURCE",
            )
            resolver = _resolver(
                [
                    RecvDwfRecord(
                        "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",
                        "DWF.DWF_DEMO_TARGET",
                        "DEMO_SYSTEM_A",
                    )
                ],
                (),
                (("PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY", 101),),
            )
            audit = self._collect(root, resolver)

        self.assertEqual(1, audit.summary["resolution"]["resolved_field_mappings"])
        self.assertIsNone(audit.resolved[0]["dbSchema"])

    def test_collector_summary_uses_recv_namespace_reason_names(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._write_project(
                root, "p-no-match", "DWF.DWF_BOP_N", "DWO.DWO_TFS_DL_BOP_N"
            )
            self._write_project(
                root,
                "p-unresolved-ns",
                "DWF.DWF_OTHER_TARGET",
                "DWO.DWO_WHATEVER_X",
            )
            self._write_project(
                root,
                "p-conflict",
                "DWF.DWF_PUB_OPER_LOG",
                "DWO.DWO_DTSELL_PUB_OPER_LOG",
            )
            self._write_project(
                root,
                "p-no-recv",
                "DWF.DWF_NOT_IN_METADATA",
                "DWO.DWO_DEMO_SCHEMA_A_SOURCE",
            )
            self._write_project(
                root,
                "p-unknown-system",
                "DWF.DWF_UNKNOWN_SYSTEM",
                "DWO.DWO_DEMO_SCHEMA_A_SOURCE",
            )
            recv = [
                RecvDwfRecord(
                    "PLAN_SA_RECV_TFS_RCPMIS_KF_TFS_RCPMIS_KF_ODS_DAY",
                    "DWF.F_BOP_N",
                    "TFS_RCPMIS_KF_ODS",
                ),
                RecvDwfRecord("DEMO_SYSTEM_A", "DWF.F_OTHER_TARGET", "DEMO_SYSTEM_A"),
                RecvDwfRecord(
                    "PLAN_SA_RECV_DTSELL_DTSELL_CZCB_BC_DAY",
                    "DWF.F_PUB_OPER_LOG",
                    "DTSELL_CZCB_BC",
                ),
                RecvDwfRecord(
                    "PLAN_SA_RECV_DTSELL_DTSELL_CZCB_MB_DAY",
                    "DWF.F_PUB_OPER_LOG",
                    "DTSELL_CZCB_MB",
                ),
                RecvDwfRecord(
                    "PLAN_SA_RECV_DEMO_SCHEMA_A_MISSING_SYSTEM_DAY",
                    "DWF.F_UNKNOWN_SYSTEM",
                    "MISSING_SYSTEM",
                ),
            ]
            resolver = _resolver(
                recv, (), (("PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY", 1),)
            )
            audit = self._collect(root, resolver)

        summary = audit.summary
        self.assertEqual(1, summary["unresolved"]["no_recv_namespace_match"])
        self.assertEqual(1, summary["unresolved"]["recv_namespace_unresolved"])
        self.assertEqual(1, summary["unresolved"]["no_recv_dwf"])
        self.assertEqual(1, summary["unresolved"]["unknown_upstream_system"])
        self.assertEqual(1, summary["conflict"]["multiple_recv_namespace_conflict"])
        self.assertNotIn("no_schema_config", summary["unresolved"])
        self.assertNotIn("schema_match_conflict", summary["conflict"])

    def test_collector_resolves_when_program_metadata_does_not_match(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "DWS_DWF.DWF_EVT_ACCD_ACCOUNT_LIST"
            project.mkdir()
            (project / "005_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_1_00.py").write_text(
                "def run(execute):\n"
                '    execute("INSERT INTO DWF.DWF_EVT_ACCD_ACCOUNT_LIST (ID) "\n'
                '            "SELECT s.ID FROM DWO.DWO_ACS_ACCD_ACCOUNT_LIST s")\n',
                encoding="utf-8",
            )
            recv = [
                RecvDwfRecord(
                    "PLAN_SA_RECV_ACS_ACS_CBSRUN_DAY",
                    "DWF.DWF_EVT_ACCD_ACCOUNT_LIST",
                    "ACS_CBSRUN",
                    ods_job_name="JOB_DWS_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_00_DAY",
                ),
                RecvDwfRecord(
                    "PLAN_SA_RECV_CBS_CBS_CBSRUN_DAY",
                    "DWF.DWF_EVT_ACCD_ACCOUNT_LIST",
                    "CBS_CBSRUN",
                    ods_job_name="JOB_DWS_DWS_DWF_F_EVT_ACCD_ACCOUNT_LIST_00_DAY",
                ),
            ]
            resolver = _resolver(
                recv,
                (),
                (
                    ("PLAN_SA_RECV_ACS_ACS_CBSRUN_DAY", 11),
                    ("PLAN_SA_RECV_CBS_CBS_CBSRUN_DAY", 22),
                ),
            )
            audit = self._collect(root, resolver)

        self.assertEqual(1, len(audit.items))
        self.assertEqual(
            "PLAN_SA_RECV_ACS_ACS_CBSRUN_DAY",
            audit.items[0].source_system_identity,
        )
        self.assertEqual("ACCD_ACCOUNT_LIST", audit.items[0].source_table)


class MetadataResolverComplexityTests(unittest.TestCase):
    def test_3762_rows_and_1000_repeated_lookups_do_not_rescan_metadata(self):
        metadata_rows = 3762
        resolve_calls = 1000
        recv = [
            RecvDwfRecord(
                (
                    "PLAN_SA_RECV_DEMO_SCHEMA_A_SOURCE_0_DAY"
                    if index == 0
                    else f"PLAN_SA_RECV_NS_{index}_SOURCE_{index}_DAY"
                ),
                "DWF.F_TARGET" if index == 0 else f"DWF.DWF_TARGET_{index}",
                "SOURCE_0" if index == 0 else f"SOURCE_{index}",
                ods_job_name=(
                    "JOB_DEMO_DAY" if index == 0 else f"JOB_OTHER_{index}_DAY"
                ),
            )
            for index in range(metadata_rows)
        ]
        schemas = [SchemaConfigRecord("SOURCE_0", "DEMO_SCHEMA_A")]
        systems = _systems(("PLAN_SA_RECV_DEMO_SCHEMA_A_SOURCE_0_DAY", 123))
        args = {
            "target": "DWF.DWF_TARGET",
            "program_names": ("005_DEMO.py", "JOB_DEMO_DAY"),
            "physical_source": "DWO.DWO_DEMO_SCHEMA_A_SOURCE_0",
        }

        original_program_normalizer = metadata_resolver.normalize_program_name
        original_target_normalizer = metadata_resolver.normalize_logical_target
        original_namespace_resolver = metadata_resolver.derive_recv_namespace
        program_results = {
            value: original_program_normalizer(value)
            for value in [*(row.ods_job_name for row in recv), *args["program_names"]]
        }
        target_results = {
            value: original_target_normalizer(value)
            for value in [*(row.table_name for row in recv), args["target"]]
        }
        namespace_results = {
            (row.recv_plan, row.data_source): original_namespace_resolver(
                row.recv_plan, row.data_source
            )
            for row in recv
        }
        counts = Counter()

        def counted_program_name(value):
            counts["normalize_program_name"] += 1
            return program_results[value]

        def counted_logical_target(value):
            counts["normalize_logical_target"] += 1
            return target_results[value]

        def counted_recv_namespace(recv_plan, data_source):
            counts["derive_recv_namespace"] += 1
            return namespace_results[(recv_plan, data_source)]

        with (
            patch.object(
                metadata_resolver, "normalize_program_name", counted_program_name
            ),
            patch.object(
                metadata_resolver, "normalize_logical_target", counted_logical_target
            ),
            patch.object(
                metadata_resolver, "derive_recv_namespace", counted_recv_namespace
            ),
        ):
            counts.clear()
            indexed = MetadataResolver(recv, schemas, systems)
            init_counts = counts.copy()
            with patch.object(
                indexed, "_resolve_indexed", wraps=indexed._resolve_indexed
            ) as indexed_resolve:
                actual = None
                for _ in range(resolve_calls):
                    actual = indexed.resolve(**args)
            after = counts.copy()

        self.assertEqual(("RESOLVED", None), (actual.status, actual.reason))
        self.assertEqual(1, indexed_resolve.call_count)
        self.assertEqual(1, len(indexed._resolution_cache))
        self.assertEqual(metadata_rows, init_counts["derive_recv_namespace"])
        self.assertEqual(metadata_rows, after["derive_recv_namespace"])
        self.assertEqual(5_762, after["normalize_program_name"])
        self.assertEqual(4_762, after["normalize_logical_target"])
        self.assertLess(after["normalize_program_name"], metadata_rows * 2)
        self.assertLess(after["normalize_logical_target"], metadata_rows * 2)


class FieldMappingCliCompatibilityAndProgressTests(unittest.TestCase):
    def test_dap_client_does_not_import_or_annotate_typing_self(self):
        source_path = (
            Path(__file__).resolve().parents[2]
            / "tools"
            / "field_mapping"
            / "dap_client.py"
        )
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        self.assertTrue(
            any(
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(alias.name == "annotations" for alias in node.names)
                for node in tree.body
            )
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "typing":
                self.assertNotIn("Self", {alias.name for alias in node.names})
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "Self"
                and isinstance(node.value, ast.Name)
                and node.value.id == "typing"
            ):
                self.fail("typing.Self is not available in Python 3.10")
        enter = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "__enter__"
        )
        self.assertIsInstance(enter.returns, ast.Name)
        self.assertEqual("FieldMappingApiClient", enter.returns.id)

    def test_progress_every_defaults_to_100_and_is_configurable(self):
        required = ["--directory", "src", "--metadata-json", "metadata.json"]
        self.assertEqual(
            100, entry.build_argument_parser().parse_args(required).progress_every
        )
        self.assertEqual(
            25,
            entry.build_argument_parser()
            .parse_args([*required, "--progress-every", "25"])
            .progress_every,
        )

    def test_benchmark_helper_reports_counters_summary_and_audit_digest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "DWS_DWF.DWF_DEMO_TARGET"
            project.mkdir()
            (project / "mapping.sql").write_text(
                "INSERT INTO DWF.F_DEMO_TARGET (ID) "
                "SELECT s.ID FROM DWO.DWO_DEMO_SCHEMA_A_SOURCE s",
                encoding="utf-8",
            )
            metadata_path = root / "metadata.json"
            metadata_path.write_text(
                '{"recv_dwf":[{"recv_plan":"PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",'
                '"table_name":"DWF.DWF_DEMO_TARGET",'
                '"data_source":"DEMO_SYSTEM_A","ods_job_name":""}],'
                '"schema_config":[{"schema_key":"DEMO_SYSTEM_A","db_schema":"DEMO_SCHEMA_A"}]}',
                encoding="utf-8",
            )
            upstreams_path = root / "upstreams.json"
            upstreams_path.write_text(
                '{"items":[{"id":"PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",'
                '"upstreamSystemId":101}]}',
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                code = benchmark_resolver.main(
                    [
                        "--repository-root",
                        str(Path(__file__).resolve().parents[2]),
                        "--directory",
                        str(root),
                        "--metadata-json",
                        str(metadata_path),
                        "--upstreams-json",
                        str(upstreams_path),
                    ]
                )

        result = json.loads(stdout.getvalue())
        self.assertEqual(0, code)
        self.assertEqual(1, result["resolve_calls"])
        self.assertEqual(
            1, result["audit_summary"]["resolution"]["resolved_field_mappings"]
        )
        self.assertEqual(1, result["derive_recv_namespace_calls"])
        self.assertEqual(64, len(result["audit_result_sha256"]))
        self.assertIn("[collector] projects=1/1", stderr.getvalue())

    def test_collector_emits_only_bounded_project_progress(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("project-a", "project-b", "project-c"):
                (root / name).mkdir()
            resolver = MetadataResolver([], [], _systems())
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                audit = collect_workspace(root, resolver, progress_every=2)

        lines = stderr.getvalue().splitlines()
        self.assertEqual(2, len(lines))
        self.assertRegex(lines[0], r"^\[collector\] projects=2/3 elapsed=\d+\.\d+s$")
        self.assertRegex(lines[1], r"^\[collector\] projects=3/3 elapsed=\d+\.\d+s$")
        self.assertEqual(3, audit.summary["unresolved"]["no_program"])


if __name__ == "__main__":
    unittest.main()
