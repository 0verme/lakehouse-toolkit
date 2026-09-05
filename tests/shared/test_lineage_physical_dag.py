from __future__ import annotations

import unittest
from pathlib import Path

from shared.lineage.domain import PhysicalNodeKind, ProgramSource
from shared.lineage.lineage_builder import normalize_table_name
from shared.lineage.physical_dag import (  # pyright: ignore[reportMissingImports]
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

    def test_quoted_case_and_schema_aliases_share_normalized_nodes(self):
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
            {normalize_table_name("DWM.DEMO_A"), normalize_table_name("DWM.DEMO_B")},
        )
        self.assertEqual(len(dag.edges), 1)
        evidence = dag.edges[0].evidence
        self.assertIsInstance(evidence, dict)
        self.assertEqual(evidence["statement_indices"], [0, 1, 2])
        self.assertEqual(len(evidence["occurrences"]), 3)

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
