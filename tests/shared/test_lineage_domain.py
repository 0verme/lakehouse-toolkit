from __future__ import annotations

import unittest
from datetime import datetime, timezone

from shared.lineage.domain import (
    DEFAULT_TEMPORARY_ASSET_RULES,
    PROGRAM_INVENTORY_PREFIXES,
    PROGRAM_NAME_LEGACY_MARKER,
    DatasetIdentity,
    IssueType,
    LineageEdge,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramNameDiagnostic,
    ProgramSource,
    expected_processing_order,
    extract_program_declared_target_token,
    extract_program_target_hint,
    group_program_sources_by_logical_target,
    is_business_asset,
    is_formal_asset,
    is_technical_asset,
    is_temporary_asset,
    normalize_lineage_schema,
    normalize_declared_target_from_program_name,
    normalize_legacy_program_namespace,
    normalize_program_inventory_target,
    parse_declared_primary_target,
    parse_program_name,
)


class LineageDomainTests(unittest.TestCase):
    def test_program_source_keeps_provider_fields_and_hash(self):
        source = ProgramSource(
            environment="DEV",
            source_profile="mysql_dev_a",
            program_name="DEMO_PROGRAM_SUMMARY",
            script_code="insert into DWM.DEMO_B select * from ODS.DEMO_A",
            expected_target="DWM.DEMO_B",
            source_hash="sha256:demo-hash",
        )

        self.assertEqual(source.environment, "DEV")
        self.assertEqual(source.source_profile, "mysql_dev_a")
        self.assertEqual(source.program_name, "DEMO_PROGRAM_SUMMARY")
        self.assertEqual(source.expected_target, "DWM.DEMO_B")
        self.assertEqual(source.source_hash, "sha256:demo-hash")
        self.assertIsInstance(source.script_code, str)

    def test_program_source_allows_unknown_expected_target(self):
        source = ProgramSource(
            environment="PROD",
            source_profile="metadata_demo",
            program_name="DEMO_PROGRAM_UNKNOWN_TARGET",
            script_code="select 1",
        )

        self.assertIsNone(source.expected_target)
        self.assertIsNone(source.source_hash)

    def test_program_source_target_authority_prefers_explicit_target(self):
        source = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWS_DM.RESULT_A:00",
            script_code="select 1",
            expected_target="DWM.EXPLICIT_TARGET",
        )

        self.assertIsNone(source.logical_target)
        self.assertEqual(source.target_hint, "DM.RESULT_A")
        self.assertEqual(source.resolved_target, "DWM.EXPLICIT_TARGET")
        self.assertIsNone(source.step_seq)

    def test_program_identity_and_logical_processing_unit_keep_distinct_boundaries(
        self,
    ):
        first = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWM.RESULT_A:3:00",
            script_code="select 1",
        )
        second = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWM.RESULT_A:4:XYZ",
            script_code="select 1",
        )

        self.assertNotEqual(first.identity, second.identity)
        self.assertEqual(first.resolved_target, "DWM.RESULT_A")
        self.assertEqual(second.resolved_target, "DWM.RESULT_A")
        self.assertEqual(
            first.logical_processing_unit_key,
            second.logical_processing_unit_key,
        )
        self.assertEqual(first.opaque_suffix, "00")
        self.assertEqual(second.opaque_suffix, "XYZ")

    def test_program_inventory_target_reuses_legacy_namespace_mapping(self):
        self.assertEqual(
            normalize_program_inventory_target("005:DWS_DWF.RESULT_A:00"),
            "DWF.RESULT_A",
        )
        self.assertEqual(
            normalize_program_inventory_target("005:DWM.RESULT_A:1:00"),
            "DWM.RESULT_A",
        )
        self.assertIsNone(normalize_program_inventory_target("DEMO_PROGRAM"))
        # 后续 segment 属于 Program Inventory 不解释的部分：既不作为 step，也不影响
        # 第二段 inventory target。
        self.assertEqual(
            normalize_program_inventory_target("005:DWM.RESULT_A:1:00:EXTRA"),
            "DWM.RESULT_A",
        )

    def test_program_inventory_prefix_is_the_single_005_authority(self):
        # Program Inventory 不是第二套 marker registry：只有 canonical 005。
        self.assertEqual(PROGRAM_INVENTORY_PREFIXES, frozenset({"005"}))
        self.assertEqual(PROGRAM_NAME_LEGACY_MARKER, "005")

    def test_program_inventory_accepts_tmp_named_program_result(self):
        # 真实 DEV214 evidence：TMP_ 命名不能阻止 005 Program Result authority。
        self.assertEqual(
            normalize_program_inventory_target(
                "005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00"
            ),
            "DWP.TMP_P_REPORT_KYW_LIST",
        )
        self.assertEqual(
            normalize_program_inventory_target("005:DWS_DWP.TMP_X:1:00"),
            "DWP.TMP_X",
        )
        self.assertEqual(
            normalize_program_inventory_target("005:DWS_DWP.TMP_X"),
            "DWP.TMP_X",
        )

    def test_program_inventory_ignores_non_005_prefix_without_failing(self):
        # 001 / 002 / ABC / 无首段 / 无冒号名字都不提供 Program Inventory
        # evidence，但不是异常：返回 None 且不 fail open。
        for program_name in (
            "001:DWF.RESULT_A:anything",
            "001:DWS_DWF.RESULT_A:anything",
            "001:AECIF_ECIF.CUS_BAS_ENT:tail",
            "001:",
            "001:ABC",
            "001:DWS_DWF.TMP_RESULT:1:00",
            "002:DWF.RESULT_A:anything",
            "003:DWS_DWF.RESULT_A:1:00",
            "ABC:DWF.RESULT_A:anything",
            ":DWF.RESULT_A:anything",
            "DEMO_PROGRAM",
        ):
            with self.subTest(program_name=program_name):
                self.assertIsNone(normalize_program_inventory_target(program_name))

    def test_program_inventory_does_not_change_issue_44_canonical_grammar(self):
        parsed = parse_program_name("001:DWF.RESULT_A:1:00")

        self.assertEqual(PROGRAM_NAME_LEGACY_MARKER, "005")
        self.assertEqual(parsed.legacy_marker, "001")
        self.assertIsNone(parsed.logical_target)
        self.assertIsNone(parsed.target_hint)
        self.assertIsNone(parsed.step_seq)
        self.assertEqual(
            parsed.diagnostics,
            (
                ProgramNameDiagnostic.PROGRAM_NAME_MARKER_INVALID,
                ProgramNameDiagnostic.PROGRAM_NAME_TARGET_INVALID,
            ),
        )
        self.assertEqual(
            parse_program_name("005:DWM.RESULT_A:1:00").logical_target,
            "DWM.RESULT_A",
        )

    def test_program_inventory_malformed_005_target_fails_open(self):
        # 只有明确进入 005 协议却无法给出合法 target 的情况才是
        # authoritative evidence 异常。
        for program_name in (
            "005:",
            "005:ABC",
            "005:not-qualified",
            "005:DWF.",
            "005:.TABLE_A",
        ):
            with self.subTest(program_name=program_name):
                with self.assertRaisesRegex(
                    ValueError, "program inventory target is not a qualified table"
                ):
                    normalize_program_inventory_target(program_name)

    def test_issue_44_grammar_accepts_tmp_named_program_result(self):
        # Issue #44 的 005 grammar 保持不变，但 TMP 命名不再让 target 失效。
        for program_name, logical_target, step_seq in (
            ("005:DWS_DWP.TMP_X:1:00", "DWP.TMP_X", 1),
            (
                "005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00",
                "DWP.TMP_P_REPORT_KYW_LIST",
                1,
            ),
            ("005:DWP.TEMP_A:2:00", "DWP.TEMP_A", 2),
        ):
            with self.subTest(program_name=program_name):
                parsed = parse_program_name(program_name)
                self.assertEqual(parsed.legacy_marker, "005")
                self.assertEqual(parsed.logical_target, logical_target)
                self.assertEqual(parsed.step_seq, step_seq)
                self.assertEqual(parsed.opaque_suffix, "00")
                self.assertEqual(
                    parsed.diagnostics,
                    (ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,),
                )
        self.assertEqual(
            normalize_legacy_program_namespace("DWS_DWP.TMP_X"), "DWP.TMP_X"
        )
        self.assertEqual(
            normalize_declared_target_from_program_name("DWS_DWP.TMP_P_REPORT_KYW_LIST"),
            "DWP.TMP_P_REPORT_KYW_LIST",
        )

    def test_program_name_target_first_parser_keeps_direct_dataset_name(self):
        parsed = parse_program_name("005:DWM.RESULT_A:1:00")

        self.assertEqual(parsed.legacy_marker, "005")
        self.assertEqual(parsed.logical_target, "DWM.RESULT_A")
        self.assertEqual(parsed.target_hint, "DWM.RESULT_A")
        self.assertEqual(parsed.step_seq, 1)
        self.assertEqual(parsed.opaque_suffix, "00")
        self.assertEqual(
            parsed.diagnostics,
            (ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,),
        )
        self.assertEqual(
            parse_declared_primary_target("005:DWM.RESULT_A:1:00"),
            "DWM.RESULT_A",
        )
        self.assertEqual(
            extract_program_declared_target_token("005:DWM.RESULT_A:1:00"),
            "DWM.RESULT_A",
        )
        self.assertEqual(
            normalize_declared_target_from_program_name("DWM.RESULT_A"),
            "DWM.RESULT_A",
        )

    def test_program_name_parser_normalizes_confirmed_legacy_names(self):
        cases = (
            ("005:DWS_DM.RESULT_A:1:00", "DM.RESULT_A", 1),
            ("005:DWS_DWM.RESULT_A:1:00", "DWM.RESULT_A", 1),
            ("005:DWS_DWA.RESULT_A:1:00", "DWA.RESULT_A", 1),
            ("005:DWS_DWP.RESULT_A:1:00", "DWP.RESULT_A", 1),
            ("005:DWS_DWD.RESULT_A:1:00", "DWD.RESULT_A", 1),
            ("005:DWS_DWF.RESULT_A:1:00", "DWF.RESULT_A", 1),
            ("005:DWS_DWUPRR.RESULT_A:1:00", "DWUPRR.RESULT_A", 1),
            ("005:DLK_DLO.RESULT_A:1:00", "DLO.RESULT_A", 1),
            ("005:DWS_DWM.RESULT_A:2:00", "DWM.RESULT_A", 2),
            ("005:DWS_DWM.RESULT_A:1:PRC_RESULT_A", "DWM.RESULT_A", 1),
        )

        for value, expected_target, expected_step in cases:
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertEqual(parsed.logical_target, expected_target)
                self.assertEqual(parsed.step_seq, expected_step)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,
                    parsed.diagnostics,
                )

        self.assertEqual(
            normalize_legacy_program_namespace("DWS_ABC.RESULT_A"),
            "DWS_ABC.RESULT_A",
        )

    def test_program_name_namespace_normalization_groups_multi_step_sources(self):
        sources = tuple(
            ProgramSource(
                environment="DEV",
                source_profile="fixture",
                program_name=f"005:DWS_DWM.RESULT_A:{step}:00",
                script_code="select 1",
            )
            for step in (2, 1)
        )

        groups = group_program_sources_by_logical_target(sources)

        self.assertEqual(tuple(groups), ("DWM.RESULT_A",))
        self.assertEqual(
            [source.step_seq for source in groups["DWM.RESULT_A"]],
            [1, 2],
        )
        self.assertEqual(
            [source.program_name for source in groups["DWM.RESULT_A"]],
            [
                "005:DWS_DWM.RESULT_A:1:00",
                "005:DWS_DWM.RESULT_A:2:00",
            ],
        )

    def test_program_name_parser_keeps_ambiguous_three_part_shapes_unknown(self):
        cases = (
            ("005:DWS_DM.RESULT_A:00", "DM.RESULT_A"),
            ("005:DLK_DLO.RESULT_A:00", "DLO.RESULT_A"),
            ("005:DWS_DWM.RESULT_A:00", "DWM.RESULT_A"),
            ("005:DWM.RESULT_A:00", "DWM.RESULT_A"),
            ("005:ABC_DWM.RESULT_A:00", "ABC_DWM.RESULT_A"),
            ("005:DWS_ABC.RESULT_A:00", "DWS_ABC.RESULT_A"),
        )
        for value, expected_hint in cases:
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertIsNone(parsed.logical_target)
                self.assertEqual(parsed.target_hint, expected_hint)
                if value.startswith("005:DWS_ABC."):
                    self.assertNotEqual(parsed.target_hint, "ABC.RESULT_A")
                self.assertIsNone(parsed.step_seq)
                self.assertIsNone(parsed.opaque_suffix)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_FORMAT_UNSUPPORTED,
                    parsed.diagnostics,
                )
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_STEP_MISSING,
                    parsed.diagnostics,
                )
                self.assertNotIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_RESOLVED,
                    parsed.diagnostics,
                )
                self.assertNotIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_STEP_INVALID,
                    parsed.diagnostics,
                )

        self.assertIsNone(parse_declared_primary_target("005:ABC_DWM.RESULT_A:00"))
        self.assertEqual(
            extract_program_target_hint("005:DWS_DM.RESULT_A:00"),
            "DM.RESULT_A",
        )

        custom_suffix = parse_program_name("005:DWM.RESULT_A:1:ABCD")
        self.assertEqual(custom_suffix.logical_target, "DWM.RESULT_A")
        self.assertEqual(custom_suffix.step_seq, 1)
        self.assertIn(
            ProgramNameDiagnostic.PROGRAM_NAME_SUFFIX_NONSTANDARD,
            custom_suffix.diagnostics,
        )

    def test_program_name_parser_rejects_unsafe_target_without_guessing(self):
        for value in (
            "",
            "005::1:00",
            "005:INVALID:1:00",
            "004:DEMO_DWM.RESULT_A:1:00",
        ):
            with self.subTest(value=value):
                parsed = parse_program_name(value)
                self.assertIsNone(parsed.logical_target)
                self.assertIsNone(parsed.target_hint)
                self.assertIn(
                    ProgramNameDiagnostic.PROGRAM_NAME_TARGET_INVALID,
                    parsed.diagnostics,
                )

        invalid_step = parse_program_name("005:DEMO_DWM.RESULT_A:0:00")
        self.assertEqual(invalid_step.logical_target, "DEMO_DWM.RESULT_A")
        self.assertIsNone(invalid_step.step_seq)
        self.assertIn(
            ProgramNameDiagnostic.PROGRAM_NAME_STEP_INVALID,
            invalid_step.diagnostics,
        )

    def test_program_name_steps_group_by_target_and_sort_without_scheduler_fact(self):
        sources = tuple(
            ProgramSource(
                environment="DEV",
                source_profile="fixture",
                program_name=name,
                script_code="select 1",
            )
            for name in (
                "005:DWM.RESULT_A:2:00",
                "005:DWM.RESULT_A:1:00",
                "005:DWM.RESULT_A:00",
                "005:DWM.RESULT_B:1:00",
            )
        )
        groups = group_program_sources_by_logical_target(sources)

        self.assertEqual(tuple(groups), ("DWM.RESULT_A", "DWM.RESULT_B"))
        self.assertEqual(
            [source.step_seq for source in groups["DWM.RESULT_A"]],
            [1, 2],
        )
        self.assertEqual(
            expected_processing_order(groups["DWM.RESULT_A"]),
            (1, 2),
        )
        self.assertEqual(
            [source.program_name for source in groups["DWM.RESULT_A"]],
            [
                "005:DWM.RESULT_A:1:00",
                "005:DWM.RESULT_A:2:00",
            ],
        )

    def test_physical_edge_direction_is_upstream_to_downstream(self):
        edge = PhysicalEdge(source="ODS.DEMO_A", target="DWM.DEMO_B")

        self.assertEqual(edge.source, "ODS.DEMO_A")
        self.assertEqual(edge.target, "DWM.DEMO_B")
        self.assertNotEqual(edge.source, edge.target)

    def test_physical_node_kind_ignores_table_name(self):
        nodes = {
            node.node_key: node
            for node in (
                PhysicalNode("ODS.DEMO_A", "ODS.DEMO_A"),
                PhysicalNode("TMP1", "TMP1"),
                PhysicalNode("DWM.DEMO_B", "DWM.DEMO_B"),
            )
        }
        edges = (
            PhysicalEdge(source="ODS.DEMO_A", target="TMP1"),
            PhysicalEdge(source="TMP1", target="DWM.DEMO_B"),
        )

        # 没有显式 temporary evidence 时使用中性默认值，表名不参与分类。
        self.assertEqual(nodes["TMP1"].kind, PhysicalNodeKind.FORMAL_ASSET)
        self.assertFalse(nodes["TMP1"].is_temporary)
        self.assertTrue(nodes["TMP1"].is_formal)
        self.assertTrue(nodes["DWM.DEMO_B"].is_formal)
        # 显式 kind / CREATE TEMP fact 仍可产生 temporary 节点。
        explicit = PhysicalNode("TMP1", "TMP1", PhysicalNodeKind.TEMPORARY_ASSET)
        self.assertTrue(explicit.is_temporary)
        self.assertTrue(PhysicalNode("SESSION_STAGE", "SESSION_STAGE").is_formal)
        self.assertEqual(
            [(edge.source, edge.target) for edge in edges],
            [("ODS.DEMO_A", "TMP1"), ("TMP1", "DWM.DEMO_B")],
        )

    def test_temporary_naming_is_not_evidence(self):
        for asset_name in (
            "TMP_1",
            "TMP_STAGE_X",
            "DWM.TMP1",
            "DWM.TEMP_A",
            "DWP.STG_A",
            "DWM.TEST_A",
            "DWM.A_TMP",
            "TMP_P_REPORT_KYW_LIST",
        ):
            with self.subTest(asset_name=asset_name):
                self.assertFalse(is_temporary_asset(asset_name))
                self.assertTrue(is_formal_asset(asset_name))
        self.assertFalse(is_temporary_asset("DWM.DEMO_C"))
        self.assertEqual(DEFAULT_TEMPORARY_ASSET_RULES, ())
        # 只有调用方显式提供的证据型规则才可能产生 temporary 分类。
        self.assertTrue(
            is_temporary_asset(
                "DWM.DEMO_STAGE_X",
                rules=(lambda name: name.endswith("STAGE_X"),),
            )
        )

    def test_lineage_edge_represents_direct_formal_asset_fact(self):
        observed_at = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
        updated_at = datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc)
        edge = LineageEdge(
            environment="DEV",
            source_profile="mysql_dev_a",
            source_table="DWM.DEMO_B",
            target_table="DWA.DEMO_C",
            program_name="DEMO_PROGRAM_C",
            job_key="DEMO_JOB_C",
            evidence_type="physical_dag",
            source_hash="sha256:demo-hash",
            batch_id="batch-demo-1",
            observed_at=observed_at,
            updated_at=updated_at,
            is_active=True,
        )

        self.assertEqual(edge.source_table, "DWM.DEMO_B")
        self.assertEqual(edge.target_table, "DWA.DEMO_C")
        self.assertEqual(edge.source_hash, "sha256:demo-hash")
        self.assertEqual(edge.batch_id, "batch-demo-1")
        self.assertEqual(edge.observed_at, observed_at)
        self.assertEqual(edge.updated_at, updated_at)
        self.assertTrue(is_formal_asset(edge.source_table))
        self.assertTrue(is_formal_asset(edge.target_table))

    def test_lineage_edge_accepts_tmp_named_business_endpoint(self):
        # TMP_ 名称不再拒绝正式 LineageEdge endpoint；能否成为正式 edge 由
        # business boundary / Program Result / schema boundary 规则决定。
        edge = LineageEdge(
            environment="DEV214",
            source_profile="mysql_dev_a",
            source_table="DWP.TMP_P_REPORT_KYW_LIST",
            target_table="DWM.RESULT_A",
        )
        self.assertEqual(edge.source_table, "DWP.TMP_P_REPORT_KYW_LIST")
        self.assertEqual(edge.target_table, "DWM.RESULT_A")
        self.assertEqual(
            edge.source_dataset_identity,
            DatasetIdentity("DEV214", "DWP", "TMP_P_REPORT_KYW_LIST"),
        )
        # 未限定 schema 的引用仍必须失败，但这与 TMP 命名无关。
        with self.assertRaisesRegex(ValueError, "qualified schema.table"):
            LineageEdge(
                environment="DEV",
                source_profile="mysql_dev_a",
                source_table="TMP_1",
                target_table="DWA.DEMO_C",
            )

    def test_dataset_identity_accepts_tmp_named_qualified_table(self):
        identity = DatasetIdentity("DEV214", "DWP", "TMP_P_REPORT_KYW_LIST")

        self.assertEqual(identity.canonical_name, "DWP.TMP_P_REPORT_KYW_LIST")
        self.assertEqual(identity.key, ("DEV214", "DWP", "TMP_P_REPORT_KYW_LIST"))
        for asset_name in (
            "DWM.TMP_X",
            "DWP.TEMP_A",
            "DWF.STG_A",
            "DWM.TEST_A",
            "DWM.A_TMP",
        ):
            with self.subTest(asset_name=asset_name):
                resolved = DatasetIdentity.from_name("DEV214", asset_name)
                self.assertIsNotNone(resolved)
                assert resolved is not None
                self.assertEqual(resolved.canonical_name, asset_name)

    def test_tmp_naming_does_not_change_business_or_technical_boundary(self):
        for asset_name in (
            "DWP.TMP_X",
            "DWM.TEMP_A",
            "DWF.STG_A",
            "DWM.TEST_A",
            "DWM.A_TMP",
            "DWP.TMP_P_REPORT_KYW_LIST",
        ):
            with self.subTest(asset_name=asset_name):
                self.assertTrue(is_business_asset(asset_name))
                self.assertFalse(is_technical_asset(asset_name))
        # DLO/DWO 边界只由显式 schema registry 决定，命名不参与。
        self.assertFalse(is_business_asset("DLO.TMP_X"))
        self.assertTrue(is_technical_asset("DLO.TMP_X"))
        self.assertFalse(is_business_asset("DWO.TEMP_A"))
        self.assertTrue(is_technical_asset("DWO.TEMP_A"))

    def test_business_asset_boundary_reuses_registered_schema_wrappers(self):
        cases = {
            "DWF.DEMO_A": True,
            "DWS_DWF.DEMO_A": True,
            "DWM.DEMO_A": True,
            "DLO.DEMO_A": False,
            "DWO.DEMO_A": False,
            "DWS_DLO.DEMO_A": False,
            "DWS_DWO.DEMO_A": False,
            "DLK_DLO.DEMO_A": False,
        }
        for asset_name, expected_business in cases.items():
            with self.subTest(asset_name=asset_name):
                self.assertEqual(is_business_asset(asset_name), expected_business)
                self.assertEqual(
                    is_technical_asset(asset_name), not expected_business
                    if "DLO" in asset_name or "DWO" in asset_name
                    else False,
                )
        self.assertEqual(normalize_lineage_schema("DWS_DWO"), "DWO")
        self.assertEqual(normalize_lineage_schema("DLK_DLO"), "DLO")
        self.assertTrue(is_business_asset("DLO.DEMO_A", environment="DEV") is False)

    def test_lineage_edge_rejects_pre_business_endpoint(self):
        with self.assertRaisesRegex(ValueError, "Business Assets"):
            LineageEdge(
                environment="DEV",
                source_profile="mysql_dev_a",
                source_table="DLO.DEMO_A",
                target_table="DWF.DEMO_C",
            )
        with self.assertRaisesRegex(ValueError, "Business Assets"):
            LineageEdge(
                environment="DEV",
                source_profile="mysql_dev_a",
                source_table="DWF.DEMO_A",
                target_table="DWS_DWO.DEMO_C",
            )

    def test_issue_type_contains_all_frozen_values(self):
        self.assertEqual(
            {item.value for item in IssueType},
            {
                "ORPHAN_BRANCH",
                "MULTI_SINK_CANDIDATE",
                "TARGET_NOT_FOUND",
                "TARGET_MISMATCH",
                "CYCLE_DETECTED",
                "SELF_REFERENCE",
                "LINEAGE_BRANCH_BROKEN",
            },
        )

    def test_lineage_issue_preserves_branch_and_lifecycle_fields(self):
        issue = LineageIssue(
            environment="DEV",
            source_profile="mysql_dev_a",
            program_name="DEMO_PROGRAM_C",
            issue_type=IssueType.ORPHAN_BRANCH,
            severity="warning",
            branch_sink="TMP_STAGE_X",
            message="分支未到达 expected target",
            evidence={"path": ["ODS.DEMO_A", "TMP_STAGE_X"]},
            batch_id="batch-demo-1",
            first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            is_active=True,
        )

        self.assertIs(issue.issue_type, IssueType.ORPHAN_BRANCH)
        self.assertIsNone(issue.node_key)
        self.assertEqual(issue.branch_sink, "TMP_STAGE_X")
        evidence = issue.evidence
        self.assertIsInstance(evidence, dict)
        if not isinstance(evidence, dict):
            self.fail("expected mapping evidence")
        self.assertEqual(evidence["path"], ["ODS.DEMO_A", "TMP_STAGE_X"])
        self.assertEqual(issue.batch_id, "batch-demo-1")
        self.assertTrue(issue.is_active)


if __name__ == "__main__":
    unittest.main()
