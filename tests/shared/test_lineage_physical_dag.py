from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

import shared.lineage.physical_dag as physical_dag_module
from shared.lineage.domain import PhysicalNodeKind, ProgramSource
from shared.lineage.materialization import materialize_program
from shared.lineage.lineage_builder import normalize_table_name
from shared.lineage.physical_dag import (  # pyright: ignore[reportMissingImports]
    SQLExtractionReason,
    build_program_physical_dag,
    extract_sql_steps,
)

ROOT_DIR = Path(__file__).resolve().parents[2]


def program(script_code: str, expected_target: str | None = "DWA.DEMO_RESULT"):
    return ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name="DEMO_PROGRAM_PHASE3",
        script_code=script_code,
        expected_target=expected_target,
    )


def edge_pairs(dag):
    return {(edge.source, edge.target) for edge in dag.edges}


def node_names(dag):
    return {node.asset_name for node in dag.nodes}


class PhysicalDAGTests(unittest.TestCase):
    def test_sql_physical_dag_preserves_sql_schema_namespace(self):
        dag = build_program_physical_dag(
            program(
                '''
                execute("""
                INSERT INTO DWS_DWM.RESULT_A
                SELECT *
                FROM DWF.TABLE_A a
                JOIN DWM.TABLE_B b ON a.id = b.id
                JOIN DWUPRR.TABLE_C c ON a.id = c.id
                JOIN DWS_DWF.TABLE_X x ON a.id = x.id
                """)
                ''',
                expected_target=None,
            )
        )

        expected_target = "DWS_DWM.RESULT_A"
        expected_sources = {
            "DWF.TABLE_A",
            "DWM.TABLE_B",
            "DWUPRR.TABLE_C",
            "DWS_DWF.TABLE_X",
        }
        self.assertEqual({edge.source for edge in dag.edges}, expected_sources)
        self.assertEqual(
            edge_pairs(dag),
            {(source, expected_target) for source in expected_sources},
        )
        self.assertNotIn("DWS_DWF.TABLE_A", node_names(dag))
        self.assertNotIn("DWS_DWM.TABLE_B", node_names(dag))

    def test_core_fixture_keeps_every_program_step_and_tmp_node(self):
        fixture_path = ROOT_DIR / "tests" / "fixtures" / "lineage" / "phase3_program.py"
        dag = build_program_physical_dag(
            program(fixture_path.read_text(encoding="utf-8"))
        )

        expected_nodes = {
            "ODS.DEMO_A",
            normalize_table_name("DWF.DEMO_B"),
            "TMP_1",
            normalize_table_name("DWM.DEMO_C"),
            "TMP_2",
            normalize_table_name("DWA.DEMO_D"),
            normalize_table_name("DWA.DEMO_RESULT"),
        }
        expected_edges = {
            ("ODS.DEMO_A", "TMP_1"),
            (normalize_table_name("DWF.DEMO_B"), "TMP_1"),
            ("TMP_1", "TMP_2"),
            (normalize_table_name("DWM.DEMO_C"), "TMP_2"),
            ("TMP_2", normalize_table_name("DWA.DEMO_RESULT")),
            (
                normalize_table_name("DWA.DEMO_D"),
                normalize_table_name("DWA.DEMO_RESULT"),
            ),
        }

        self.assertEqual(node_names(dag), expected_nodes)
        self.assertEqual(edge_pairs(dag), expected_edges)
        self.assertEqual(
            [node.asset_name for node in dag.nodes],
            [
                "ODS.DEMO_A",
                normalize_table_name("DWF.DEMO_B"),
                "TMP_1",
                normalize_table_name("DWM.DEMO_C"),
                "TMP_2",
                normalize_table_name("DWA.DEMO_D"),
                normalize_table_name("DWA.DEMO_RESULT"),
            ],
        )
        self.assertEqual(dag.sinks, (normalize_table_name("DWA.DEMO_RESULT"),))
        self.assertEqual(dag.expected_target, normalize_table_name("DWA.DEMO_RESULT"))
        self.assertEqual(len(dag.steps), 3)
        self.assertEqual(dag.node_map["TMP_1"].kind, PhysicalNodeKind.TEMPORARY_ASSET)
        self.assertEqual(dag.node_map["TMP_2"].kind, PhysicalNodeKind.TEMPORARY_ASSET)

    def test_each_source_of_a_step_gets_its_own_upstream_edge(self):
        dag = build_program_physical_dag(
            program(
                'execute("INSERT INTO TMP_1 SELECT * FROM ODS.A JOIN DWF.B ON 1 = 1")',
                expected_target=None,
            )
        )

        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.A", "TMP_1"),
                (normalize_table_name("DWF.B"), "TMP_1"),
            },
        )
        self.assertNotIn("ODS.A", {edge.target for edge in dag.edges})
        self.assertTrue(all(edge.source != edge.target for edge in dag.edges))

    def test_formal_intermediate_asset_is_not_collapsed(self):
        dag = build_program_physical_dag(
            program(
                """
                execute("INSERT INTO DWM.DEMO_B SELECT * FROM ODS.DEMO_A")
                execute("CREATE TEMP TABLE TMP_1 AS SELECT * FROM DWM.DEMO_B")
                execute("INSERT INTO DWA.DEMO_C SELECT * FROM TMP_1")
                """
            )
        )
        edges = edge_pairs(dag)

        self.assertEqual(
            edges,
            {
                ("ODS.DEMO_A", normalize_table_name("DWM.DEMO_B")),
                (normalize_table_name("DWM.DEMO_B"), "TMP_1"),
                ("TMP_1", normalize_table_name("DWA.DEMO_C")),
            },
        )
        self.assertNotIn(("ODS.DEMO_A", normalize_table_name("DWA.DEMO_C")), edges)
        self.assertIn(normalize_table_name("DWM.DEMO_B"), node_names(dag))

    def test_isolated_branch_is_retained_and_becomes_another_sink(self):
        dag = build_program_physical_dag(
            program(
                """
                execute("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                execute("CREATE TEMP TABLE TMP_UNUSED AS SELECT * FROM ODS.DEMO_X")
                """
            )
        )

        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT")),
                ("ODS.DEMO_X", "TMP_UNUSED"),
            },
        )
        self.assertEqual(
            dag.sinks,
            (normalize_table_name("DWA.DEMO_RESULT"), "TMP_UNUSED"),
        )
        self.assertIn("TMP_UNUSED", node_names(dag))

    def test_multiple_sinks_are_facts_not_issues(self):
        dag = build_program_physical_dag(
            program(
                """
                execute("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                execute("INSERT INTO DWA.DEMO_OTHER SELECT * FROM ODS.DEMO_B")
                """
            )
        )

        self.assertEqual(
            dag.sinks,
            (
                normalize_table_name("DWA.DEMO_RESULT"),
                normalize_table_name("DWA.DEMO_OTHER"),
            ),
        )
        self.assertFalse(hasattr(dag, "issues"))

    def test_cte_aliases_are_not_physical_nodes(self):
        dag = build_program_physical_dag(
            program(
                '''
                execute("""
                WITH base AS (
                    SELECT * FROM ODS.DEMO_A
                ), joined AS (
                    SELECT * FROM base JOIN DWF.DEMO_B b ON base.id = b.id
                )
                INSERT INTO DWM.DEMO_C
                SELECT * FROM joined
                """)
                '''
            )
        )

        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.DEMO_A", normalize_table_name("DWM.DEMO_C")),
                (
                    normalize_table_name("DWF.DEMO_B"),
                    normalize_table_name("DWM.DEMO_C"),
                ),
            },
        )
        self.assertNotIn("BASE", node_names(dag))
        self.assertNotIn("JOINED", node_names(dag))

    def test_sql_aliases_are_not_assets(self):
        dag = build_program_physical_dag(
            program(
                'execute("INSERT INTO DWM.DEMO_C SELECT * FROM ODS.DEMO_A a JOIN DWF.DEMO_B b ON a.id = b.id")'
            )
        )

        self.assertNotIn("A", node_names(dag))
        self.assertNotIn("B", node_names(dag))
        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.DEMO_A", normalize_table_name("DWM.DEMO_C")),
                (
                    normalize_table_name("DWF.DEMO_B"),
                    normalize_table_name("DWM.DEMO_C"),
                ),
            },
        )

    def test_expression_level_from_is_not_a_physical_source(self):
        cases = (
            (
                """INSERT INTO dwd.target_table
                SELECT EXTRACT(DAY FROM a.pm_end_time)
                FROM dwd.source_table a""",
                "DWD.TARGET_TABLE",
                "dwd.source_table",
                "A.PM_END_TIME",
            ),
            (
                """CREATE TABLE dwd.tmp_a AS
                SELECT EXTRACT(YEAR FROM a.next_repay_dt)
                FROM dwd.source_a a""",
                "DWD.TMP_A",
                "dwd.source_a",
                "A.NEXT_REPAY_DT",
            ),
            (
                """INSERT INTO dwp.result_table
                SELECT SUBSTRING(b.inputdate FROM 1 FOR 8)
                FROM dwf.input_table b""",
                "DWP.RESULT_TABLE",
                "dwf.input_table",
                "B.INPUTDATE",
            ),
            (
                """INSERT INTO dwm.result_table
                SELECT TRIM(BOTH ' ' FROM t.endtime)
                FROM dwd.event_table t""",
                "DWM.RESULT_TABLE",
                "dwd.event_table",
                "T.ENDTIME",
            ),
        )

        for sql, target, raw_source, forbidden_source in cases:
            with self.subTest(target=target):
                dag = build_program_physical_dag(
                    program(f'execute("""{sql}""")', expected_target=None)
                )
                normalized_source = normalize_table_name(raw_source)

                self.assertEqual(len(dag.steps), 1)
                self.assertEqual(dag.steps[0].sources, (normalized_source,))
                self.assertEqual(dag.steps[0].raw_sources, (raw_source,))
                self.assertEqual(
                    edge_pairs(dag),
                    {(normalized_source, normalize_table_name(target))},
                )
                self.assertNotIn(forbidden_source, node_names(dag))

                evidence = cast(dict[str, object], dag.edges[0].evidence)
                self.assertEqual(evidence["raw_source"], raw_source)
                self.assertEqual(evidence["normalized_source"], normalized_source)

    def test_relation_context_preserves_join_using_known_and_unknown_sources(self):
        cases = (
            (
                """INSERT INTO dwf.result
                SELECT EXTRACT(DAY FROM a.created_at), b.id
                FROM dwd.a a
                JOIN dwm.b b ON a.id = b.id""",
                "DWF.RESULT",
                ("dwd.a", "dwm.b"),
                ("A.CREATED_AT", "B.ID"),
            ),
            (
                "INSERT INTO dwa.result SELECT * FROM (SELECT * FROM dwd.derived_source) q",
                "DWA.RESULT",
                ("dwd.derived_source",),
                (),
            ),
            (
                "MERGE INTO dwa.result t USING dwp.table_c c ON t.id = c.id",
                "DWA.RESULT",
                ("dwp.table_c",),
                (),
            ),
            (
                "INSERT INTO dwa.result SELECT * FROM dwuprr.ncms_table",
                "DWA.RESULT",
                ("dwuprr.ncms_table",),
                (),
            ),
            (
                "INSERT INTO dwa.result SELECT * FROM dwssds.some_table",
                "DWA.RESULT",
                ("dwssds.some_table",),
                (),
            ),
        )

        for sql, target, raw_sources, forbidden_sources in cases:
            with self.subTest(target=target):
                dag = build_program_physical_dag(
                    program(f'execute("""{sql}""")', expected_target=None)
                )
                normalized_sources = tuple(
                    normalize_table_name(raw_source) for raw_source in raw_sources
                )

                self.assertEqual(dag.steps[0].sources, normalized_sources)
                self.assertEqual(dag.steps[0].raw_sources, raw_sources)
                self.assertEqual(
                    edge_pairs(dag),
                    {
                        (source, normalize_table_name(target))
                        for source in normalized_sources
                    },
                )
                for forbidden_source in forbidden_sources:
                    self.assertNotIn(forbidden_source, node_names(dag))

    def test_comments_and_sql_literals_are_ignored(self):
        dag = build_program_physical_dag(
            program(
                '''
                logger.info("FROM ODS.NOT_SQL")
                execute("""
                -- FROM ODS.FAKE_COMMENT
                /* JOIN DWM.FAKE_COMMENT */
                INSERT OVERWRITE TABLE `DWA`.`DEMO_RESULT`
                SELECT *
                FROM `ODS`.`DEMO_A` a
                JOIN "DWF"."DEMO_B" b ON a.id = b.id
                WHERE message = '-- FROM ODS.FAKE_LITERAL /*'
                """)
                '''
            )
        )

        self.assertNotIn("ODS.NOT_SQL", node_names(dag))
        self.assertNotIn("ODS.FAKE_COMMENT", node_names(dag))
        self.assertNotIn("DWM.FAKE_COMMENT", node_names(dag))
        self.assertNotIn("ODS.FAKE_LITERAL", node_names(dag))
        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT")),
                (
                    normalize_table_name("DWF.DEMO_B"),
                    normalize_table_name("DWA.DEMO_RESULT"),
                ),
            },
        )

    def test_select_without_write_target_does_not_use_expected_target(self):
        dag = build_program_physical_dag(
            program("SELECT * FROM ODS.DEMO_A", expected_target="DWA.DEMO_RESULT")
        )

        self.assertEqual(dag.edges, ())
        self.assertEqual(dag.sinks, ())
        self.assertNotIn(normalize_table_name("DWA.DEMO_RESULT"), node_names(dag))
        self.assertEqual(dag.steps[0].statement_type, "select")

    def test_expected_target_none_is_preserved_as_unknown(self):
        dag = build_program_physical_dag(
            program(
                'execute("INSERT INTO DWM.DEMO_C SELECT * FROM ODS.DEMO_A")',
                expected_target=None,
            )
        )

        self.assertIsNone(dag.expected_target)
        self.assertEqual(dag.sinks, (normalize_table_name("DWM.DEMO_C"),))

    def test_ambiguous_program_name_target_stays_unknown(self):
        source = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="005:DWM.DEMO_C:00",
            script_code='execute("INSERT INTO DWM.DEMO_C SELECT * FROM ODS.DEMO_A")',
        )
        dag = build_program_physical_dag(source)

        self.assertIsNone(source.logical_target)
        self.assertEqual(source.target_hint, "DWM.DEMO_C")
        self.assertIsNone(dag.expected_target)
        self.assertEqual(dag.program_source.target_hint, "DWM.DEMO_C")
        self.assertEqual(dag.sinks, (normalize_table_name("DWM.DEMO_C"),))
        self.assertGreater(len(dag.edges), 0)

    def test_self_reference_is_kept(self):
        dag = build_program_physical_dag(
            program(
                'execute("INSERT OVERWRITE TABLE DWM.DEMO_A SELECT * FROM DWM.DEMO_A JOIN ODS.DEMO_B ON 1 = 1")'
            )
        )

        self.assertIn(
            (
                normalize_table_name("DWM.DEMO_A"),
                normalize_table_name("DWM.DEMO_A"),
            ),
            edge_pairs(dag),
        )
        self.assertIn(
            ("ODS.DEMO_B", normalize_table_name("DWM.DEMO_A")), edge_pairs(dag)
        )

    def test_cycle_edges_are_kept_without_cycle_detection(self):
        dag = build_program_physical_dag(
            program(
                """
                execute("INSERT INTO TMP_1 SELECT * FROM TMP_2")
                execute("INSERT INTO TMP_2 SELECT * FROM TMP_1")
                """
            )
        )

        self.assertEqual(
            edge_pairs(dag),
            {("TMP_2", "TMP_1"), ("TMP_1", "TMP_2")},
        )
        self.assertEqual(dag.sinks, ())

    def test_quoted_identifiers_normalize_without_merging_schema_names(self):
        dag = build_program_physical_dag(
            program(
                """
                execute("INSERT INTO DWM.DEMO_A SELECT * FROM DWM.DEMO_B")
                execute('INSERT INTO `DWS_DWM`.`DEMO_A` SELECT * FROM "DWM"."DEMO_B"')
                execute("INSERT INTO [DWM].[DEMO_A] SELECT * FROM [DWM].[DEMO_B]")
                """
            )
        )

        self.assertEqual(
            node_names(dag),
            {"DWM.DEMO_A", "DWM.DEMO_B", "DWS_DWM.DEMO_A"},
        )
        self.assertEqual(
            edge_pairs(dag),
            {
                ("DWM.DEMO_B", "DWM.DEMO_A"),
                ("DWM.DEMO_B", "DWS_DWM.DEMO_A"),
            },
        )
        self.assertEqual(len(dag.edges), 2)

        dwm_edge = next(edge for edge in dag.edges if edge.target == "DWM.DEMO_A")
        dwm_evidence = cast(dict[str, object], dwm_edge.evidence)
        self.assertIsInstance(dwm_evidence, dict)
        self.assertEqual(dwm_evidence["statement_indices"], [0, 2])
        dwm_occurrences = cast(list[object], dwm_evidence["occurrences"])
        self.assertEqual(len(dwm_occurrences), 2)

        dws_edge = next(edge for edge in dag.edges if edge.target == "DWS_DWM.DEMO_A")
        dws_evidence = cast(dict[str, object], dws_edge.evidence)
        self.assertIsInstance(dws_evidence, dict)
        self.assertEqual(dws_evidence["statement_indices"], [1])

    def test_unqualified_asset_names_are_supported_without_alias_nodes(self):
        dag = build_program_physical_dag(
            program(
                'execute("INSERT INTO TMP1 SELECT * FROM A a JOIN B b ON a.id = b.id")'
            )
        )

        self.assertEqual(edge_pairs(dag), {("A", "TMP1"), ("B", "TMP1")})
        self.assertNotIn("a", node_names(dag))
        self.assertNotIn("b", node_names(dag))

    def test_create_table_view_and_merge_targets_are_supported(self):
        steps = extract_sql_steps(
            """
            execute("CREATE TABLE DWM.DEMO_A (id int)")
            execute("CREATE TEMPORARY TABLE SESSION_STAGE AS SELECT * FROM DWM.DEMO_A")
            execute("CREATE OR REPLACE VIEW DWA.DEMO_VIEW AS SELECT * FROM SESSION_STAGE")
            execute("MERGE INTO DWA.DEMO_RESULT t USING SESSION_STAGE s ON t.id = s.id")
            """
        )
        dag = build_program_physical_dag(
            program(
                """
                execute("CREATE TABLE DWM.DEMO_A (id int)")
                execute("CREATE TEMPORARY TABLE SESSION_STAGE AS SELECT * FROM DWM.DEMO_A")
                execute("CREATE OR REPLACE VIEW DWA.DEMO_VIEW AS SELECT * FROM SESSION_STAGE")
                execute("MERGE INTO DWA.DEMO_RESULT t USING SESSION_STAGE s ON t.id = s.id")
                """
            )
        )

        self.assertEqual(
            [step.statement_type for step in steps],
            ["create_table", "create_table", "create_view", "merge"],
        )
        self.assertEqual(
            edge_pairs(dag),
            {
                (normalize_table_name("DWM.DEMO_A"), "SESSION_STAGE"),
                ("SESSION_STAGE", normalize_table_name("DWA.DEMO_VIEW")),
                ("SESSION_STAGE", normalize_table_name("DWA.DEMO_RESULT")),
            },
        )
        self.assertEqual(
            dag.node_map["SESSION_STAGE"].kind,
            PhysicalNodeKind.TEMPORARY_ASSET,
        )

    def test_malformed_python_literal_recovers_sql_dag_and_lineage(self):
        malformed_path = (
            ROOT_DIR
            / "tests"
            / "fixtures"
            / "lineage"
            / "python_sql_ast_parse_recovery.py"
        )
        success_path = (
            ROOT_DIR / "tests" / "fixtures" / "lineage" / "python_sql_ast_success.py"
        )
        malformed_script = malformed_path.read_text(encoding="utf-8")
        with self.assertRaises(SyntaxError):
            ast.parse(malformed_script)

        expected_target = normalize_table_name("DEMO_DWM.RESULT_A")
        recovered_dag = build_program_physical_dag(
            program(malformed_script, expected_target=expected_target)
        )
        self.assertEqual(
            recovered_dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_RECOVERED.value,
        )
        self.assertEqual(recovered_dag.sql_candidate_count, 1)
        self.assertEqual(len(recovered_dag.steps), 1)
        step = recovered_dag.steps[0]
        self.assertEqual(step.statement_type, "insert")
        self.assertEqual(step.target, expected_target)
        self.assertEqual(step.sources, ("DEMO_DWF.SOURCE_A",))
        self.assertEqual(
            edge_pairs(recovered_dag),
            {("DEMO_DWF.SOURCE_A", expected_target)},
        )

        materialization = materialize_program(recovered_dag)
        self.assertEqual(len(materialization.edges), 1)
        self.assertEqual(materialization.edges[0].source_table, "DEMO_DWF.SOURCE_A")
        self.assertEqual(materialization.edges[0].target_table, expected_target)

        success_dag = build_program_physical_dag(
            program(
                success_path.read_text(encoding="utf-8"),
                expected_target=expected_target,
            )
        )
        self.assertEqual(
            success_dag.sql_extraction_reason,
            SQLExtractionReason.CANDIDATE_FOUND.value,
        )
        self.assertEqual(
            success_dag.sql_candidate_count, recovered_dag.sql_candidate_count
        )
        self.assertEqual(success_dag.steps, recovered_dag.steps)
        self.assertEqual(success_dag.edge_pairs, recovered_dag.edge_pairs)

    def test_legacy_recovery_preserves_do_and_run_wrappers(self):
        script = r'''sql = """
-- legacy comment contains \N
insert into DEMO_DWM.RESULT_A
select *
from DEMO_DWF.SOURCE_A
"""
executor.do(sql)
executor.run(sql)
'''
        with self.assertRaises(SyntaxError):
            ast.parse(script)

        dag = build_program_physical_dag(
            program(script, expected_target="DEMO_DWM.RESULT_A")
        )

        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_RECOVERED.value,
        )
        self.assertEqual(dag.sql_candidate_count, 2)
        self.assertEqual(len(dag.steps), 2)
        self.assertEqual(dag.edge_pairs, {("DEMO_DWF.SOURCE_A", "DEMO_DWM.RESULT_A")})

    def test_legacy_recovery_rejects_plain_text_and_dynamic_literals(self):
        plain_text = r'''notes = """
plain text contains \N but is not SQL
"""
executor.do(notes)
'''
        plain_dag = build_program_physical_dag(
            program(plain_text, expected_target=None)
        )
        self.assertEqual(plain_dag.steps, ())
        self.assertEqual(plain_dag.edges, ())
        self.assertEqual(
            plain_dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_FAILED.value,
        )

        dynamic = r'''sql = """
-- legacy comment contains \N
insert into DEMO_DWM.RESULT_A
select *
from DEMO_DWF.SOURCE_A
"""
sql = runtime_sql
executor.do(sql)
'''
        dynamic_dag = build_program_physical_dag(program(dynamic, expected_target=None))
        self.assertEqual(dynamic_dag.steps, ())
        self.assertEqual(dynamic_dag.edges, ())
        self.assertEqual(
            dynamic_dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_FAILED.value,
        )

        f_string = r'''sql = f"""
-- legacy comment contains \N
insert into {runtime_target}
select *
from DEMO_DWF.SOURCE_A
"""
executor.do(sql)
'''
        f_string_dag = build_program_physical_dag(
            program(f_string, expected_target=None)
        )
        self.assertEqual(f_string_dag.steps, ())
        self.assertEqual(f_string_dag.edges, ())
        self.assertEqual(
            f_string_dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_FAILED.value,
        )

    def test_broken_legacy_quote_is_rejected_without_crashing(self):
        script = r'''sql = """
-- legacy comment contains \N
insert into DEMO_DWM.RESULT_A
select *
from DEMO_DWF.SOURCE_A
executor.do(sql)
'''
        dag = build_program_physical_dag(program(script, expected_target=None))
        self.assertEqual(dag.steps, ())
        self.assertEqual(dag.edges, ())
        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.PYTHON_PARSE_FAILED.value,
        )

    def test_legacy_recovery_runs_only_after_python_parse_failure(self):
        success_path = (
            ROOT_DIR / "tests" / "fixtures" / "lineage" / "python_sql_ast_success.py"
        )
        success_script = success_path.read_text(encoding="utf-8")
        with patch.object(
            physical_dag_module, "_recover_legacy_sql_candidates"
        ) as recover:
            dag = build_program_physical_dag(program(success_script))
            recover.assert_not_called()
        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.CANDIDATE_FOUND.value,
        )

        malformed_path = (
            ROOT_DIR
            / "tests"
            / "fixtures"
            / "lineage"
            / "python_sql_ast_parse_recovery.py"
        )
        with patch.object(
            physical_dag_module,
            "_recover_legacy_sql_candidates",
            wraps=physical_dag_module._recover_legacy_sql_candidates,
        ) as recover:
            build_program_physical_dag(
                program(malformed_path.read_text(encoding="utf-8"))
            )
            recover.assert_called_once()

    def test_profiled_db_wrappers_and_fixture_return_are_supported(self):
        fixture_path = (
            ROOT_DIR
            / "tests"
            / "fixtures"
            / "demo_workspace"
            / "WORKSPACE"
            / "DWM"
            / "DWM.M_DEMO_ACCT"
            / "demo_job.py"
        )
        fixture_dag = build_program_physical_dag(
            program(fixture_path.read_text(encoding="utf-8"))
        )
        wrapper_dag = build_program_physical_dag(
            program(
                """
                from shared.db.gaussdb import fetch_all, run_sql_with_profile
                fetch_all("demo", "SELECT * FROM ODS.DEMO_A")
                run_sql_with_profile("demo", "INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                """
            )
        )
        legacy_wrapper_dag = build_program_physical_dag(
            program(
                """
                select_sql("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                select_mysql_sql("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                """
            )
        )

        self.assertEqual(fixture_dag.sql_candidate_count, 1)
        self.assertEqual(
            fixture_dag.sql_extraction_reason,
            SQLExtractionReason.CANDIDATE_FOUND.value,
        )
        self.assertEqual(len(fixture_dag.steps), 2)
        self.assertEqual(wrapper_dag.sql_candidate_count, 2)
        self.assertEqual(len(wrapper_dag.steps), 2)
        self.assertEqual(legacy_wrapper_dag.sql_candidate_count, 2)
        self.assertEqual(
            edge_pairs(legacy_wrapper_dag),
            {("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT"))},
        )
        self.assertEqual(
            edge_pairs(wrapper_dag),
            {("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT"))},
        )

    def test_internal_do_run_wrappers_require_sql_like_static_text(self):
        dag = build_program_physical_dag(
            program(
                """
                executor.do("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")
                executor.run("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_B")
                do("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_C")
                run("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_D")
                """
            )
        )

        self.assertEqual(dag.sql_candidate_count, 4)
        self.assertEqual(len(dag.steps), 4)
        self.assertEqual(
            edge_pairs(dag),
            {
                ("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT")),
                ("ODS.DEMO_B", normalize_table_name("DWA.DEMO_RESULT")),
                ("ODS.DEMO_C", normalize_table_name("DWA.DEMO_RESULT")),
                ("ODS.DEMO_D", normalize_table_name("DWA.DEMO_RESULT")),
            },
        )

        non_sql_dag = build_program_physical_dag(
            program(
                """
                executor.do("refresh cache")
                run(0)
                """,
                expected_target=None,
            )
        )
        self.assertEqual(non_sql_dag.sql_candidate_count, 0)
        self.assertEqual(non_sql_dag.steps, ())
        self.assertEqual(non_sql_dag.edges, ())
        self.assertEqual(
            non_sql_dag.sql_extraction_reason,
            SQLExtractionReason.SQL_ARGUMENT_NOT_SQL.value,
        )

        dynamic_dag = build_program_physical_dag(
            program(
                """
                executor.do(runtime_sql)
                """,
                expected_target=None,
            )
        )
        self.assertEqual(dynamic_dag.sql_candidate_count, 0)
        self.assertEqual(
            dynamic_dag.sql_extraction_reason,
            SQLExtractionReason.SQL_ARGUMENT_DYNAMIC.value,
        )

    def test_format_keeps_static_sql_structure_when_values_are_dynamic(self):
        fixture_path = (
            ROOT_DIR
            / "tests"
            / "fixtures"
            / "lineage"
            / "python_sql_executor_wrappers.py"
        )
        dag = build_program_physical_dag(
            program(fixture_path.read_text(encoding="utf-8"), expected_target=None)
        )

        self.assertEqual(dag.sql_candidate_count, 2)
        self.assertEqual(len(dag.steps), 2)
        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.CANDIDATE_FOUND.value,
        )
        self.assertIn(normalize_table_name("DWD.D_GJFK_ALGJ"), node_names(dag))
        self.assertIn(normalize_table_name("ODS.SOURCE_A"), node_names(dag))
        self.assertEqual(
            edge_pairs(dag),
            {
                (
                    normalize_table_name("ODS.SOURCE_A"),
                    normalize_table_name("DWD.D_GJFK_ALGJ"),
                )
            },
        )
        self.assertEqual(len(dag.edges), 1)

        static_dag = build_program_physical_dag(
            program(
                '''
                do("""
                INSERT INTO DWA.DEMO_RESULT
                SELECT * FROM ODS.DEMO_A
                WHERE dt = '{DATE}'
                """.format(DATE="20240101"))
                ''',
                expected_target=None,
            )
        )
        self.assertEqual(static_dag.sql_candidate_count, 1)
        self.assertEqual(
            edge_pairs(static_dag),
            {("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT"))},
        )

    def test_non_string_returns_are_not_reported_as_dynamic(self):
        for return_value in ("0", "1", "None"):
            with self.subTest(return_value=return_value):
                dag = build_program_physical_dag(
                    program(
                        f"""
                        def main():
                            return {return_value}
                        """,
                        expected_target=None,
                    )
                )

                self.assertEqual(dag.sql_candidate_count, 0)
                self.assertEqual(dag.steps, ())
                self.assertEqual(
                    dag.sql_extraction_reason,
                    SQLExtractionReason.SQL_RETURN_NOT_SQL.value,
                )

        failure_dag = build_program_physical_dag(
            program(
                """
                def main():
                    execute(dynamic_sql)
                    return 0
                """,
                expected_target=None,
            )
        )
        self.assertEqual(
            failure_dag.sql_extraction_reason,
            SQLExtractionReason.SQL_ARGUMENT_DYNAMIC.value,
        )

    def test_unknown_wrapper_is_reported_without_guessing(self):
        dag = build_program_physical_dag(
            program(
                'execute_with_retry("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")'
            )
        )

        self.assertEqual(dag.steps, ())
        self.assertEqual(dag.edges, ())
        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.SQL_CALL_NOT_RECOGNIZED.value,
        )

    def test_dynamic_sql_without_static_values_is_not_guessed(self):
        dag = build_program_physical_dag(
            program(
                """
                def run(target, source):
                    sql = f"INSERT INTO {target} SELECT * FROM {source}"
                    execute(sql)
                """
            )
        )

        self.assertEqual(dag.steps, ())
        self.assertEqual(dag.nodes, ())
        self.assertEqual(dag.edges, ())
        self.assertEqual(
            dag.sql_extraction_reason,
            SQLExtractionReason.SQL_ARGUMENT_DYNAMIC.value,
        )

    def test_static_f_string_values_can_be_resolved(self):
        dag = build_program_physical_dag(
            program(
                """
                target = "DWA.DEMO_RESULT"
                source = "ODS.DEMO_A"
                sql = f"INSERT INTO {target} SELECT * FROM {source}"
                execute(sql)
                """
            )
        )

        self.assertEqual(
            edge_pairs(dag),
            {("ODS.DEMO_A", normalize_table_name("DWA.DEMO_RESULT"))},
        )


if __name__ == "__main__":
    unittest.main()
