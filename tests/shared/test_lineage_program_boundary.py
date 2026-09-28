from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from shared.lineage.domain import IssueType, ProgramSource
from shared.lineage.materialization import materialize_program
from shared.lineage.physical_dag import build_program_physical_dag
from shared.lineage.program_boundary import (
    ProgramBoundaryBusinessEdge,
    ProgramBoundaryProgram,
    build_program_boundary_projections,
)
from shared.lineage.reconciliation import (
    ReconciliationStatus,
    ReconciliationTiming,
    reconcile_active_dws_lineage,
)

ENVIRONMENT = "DEMO_DEV"
SQL_PROFILE = "DEMO_SQL_PROFILE"
SCHEDULE_PROFILE = "DEMO_SCHEDULE_PROFILE"
TARGET = "DEMO_DWM.RESULT"
OBSERVED_AT = datetime(2026, 9, 20, 8, 9, 10, tzinfo=timezone.utc)


def program(
    name: str,
    *,
    key: str,
) -> ProgramBoundaryProgram:
    return ProgramBoundaryProgram(
        environment=ENVIRONMENT,
        source_profile=SQL_PROFILE,
        program_key=key,
        program_name=name,
    )


def edge(
    program_name: str,
    program_key: str,
    source: str,
    target: str,
) -> ProgramBoundaryBusinessEdge:
    return ProgramBoundaryBusinessEdge(
        environment=ENVIRONMENT,
        source_profile=SQL_PROFILE,
        program_key=program_key,
        program_name=program_name,
        source_table=source,
        target_table=target,
    )


def complex_edges(
    program_name: str, program_key: str
) -> tuple[ProgramBoundaryBusinessEdge, ...]:
    return (
        edge(program_name, program_key, "DEMO_DWF.A", "DEMO_DWM.TMP_X"),
        edge(program_name, program_key, "DEMO_DWF.B", "DEMO_DWM.TMP_X"),
        edge(program_name, program_key, "DEMO_DWF.C", "DEMO_DWM.ABC_Y"),
        edge(program_name, program_key, "DEMO_DWM.TMP_X", "DEMO_DWM.ABC_Y"),
        edge(program_name, program_key, "DEMO_DWM.ABC_Y", "DEMO_DWM.WORK_Z"),
        edge(program_name, program_key, "DEMO_DWF.D", "DEMO_DWM.WORK_Z"),
        edge(program_name, program_key, "DEMO_DWM.WORK_Z", TARGET),
    )


class ProgramBoundaryProjectionTests(unittest.TestCase):
    def test_static_empty_query_does_not_trigger_result_source_fallback(self) -> None:
        program_name = f"005:{TARGET}:1:00"
        program_key = "program-static-empty"
        source = ProgramSource(
            environment=ENVIRONMENT,
            source_profile=SQL_PROFILE,
            program_name=program_name,
            expected_target=TARGET,
            script_code="""
                INSERT INTO DEMO_DWM.TMP_00 SELECT * FROM DEMO_DWF.A;
                INSERT INTO DEMO_DWM.TMP_04 SELECT * FROM DEMO_DWM.TMP_00;
                INSERT INTO DEMO_DWM.TMP_04_0 SELECT * FROM DEMO_DWM.TMP_04;
                INSERT INTO DEMO_DWM.RESULT SELECT * FROM DEMO_DWM.TMP_04_0;
                CREATE TABLE DEMO_DWM.TMP_04_0 AS
                    SELECT * FROM DEMO_DWM.RESULT WHERE 1 = 2;
            """,
        )
        dag = build_program_physical_dag(source)
        materialization = materialize_program(dag, batch_id="batch-static-empty")

        self.assertNotIn((TARGET, "DEMO_DWM.TMP_04_0"), dag.edge_pairs)
        self.assertNotIn(
            (TARGET, "DEMO_DWM.TMP_04_0"),
            {
                (item.source_table, item.target_table)
                for item in materialization.edges
            },
        )
        self.assertIn(
            IssueType.STATIC_EMPTY_QUERY,
            {item.issue_type for item in materialization.issues},
        )

        boundary_program = program(program_name, key=program_key)
        boundary_edges = tuple(
            ProgramBoundaryBusinessEdge(
                environment=ENVIRONMENT,
                source_profile=SQL_PROFILE,
                program_key=program_key,
                program_name=program_name,
                source_table=item.source_table,
                target_table=item.target_table,
            )
            for item in materialization.edges
        )
        projection = build_program_boundary_projections(
            (boundary_program,), boundary_edges, target_tables=(TARGET,)
        )[0]
        self.assertFalse(projection.used_direct_fallback)
        self.assertNotIn("PROGRAM_RESULT_USED_AS_SOURCE", projection.diagnostics)
        self.assertEqual(
            [item.source_table for item in projection.dependencies],
            ["DEMO_DWF.A"],
        )

        sql_reader = _BoundaryReader(
            (projection,),
            side="sql",
            batch_id="batch-sql-static-empty",
            scope=((ENVIRONMENT, SQL_PROFILE),),
        )
        schedule_reader = _BoundaryReader(
            (),
            side="schedule",
            batch_id="batch-schedule-static-empty",
            scope=((ENVIRONMENT, SCHEDULE_PROFILE),),
            schedule_sources=("DEMO_DWF.A",),
        )
        reconciliation = reconcile_active_dws_lineage(
            sql_reader,
            schedule_reader,
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_tables=(TARGET,),
            apply_suppression=False,
        )
        self.assertEqual(len(reconciliation.rows), 1)
        self.assertIs(reconciliation.rows[0].status, ReconciliationStatus.MATCH)

    def test_true_result_source_edge_keeps_existing_boundary_protection(self) -> None:
        name = f"005:{TARGET}:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-real-result-source"),),
            (
                edge(name, "program-real-result-source", "DEMO_DWF.A", TARGET),
                edge(name, "program-real-result-source", TARGET, "DEMO_DWM.OTHER"),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("PROGRAM_RESULT_USED_AS_SOURCE", projection.diagnostics)
        self.assertEqual(
            [item.source_table for item in projection.dependencies],
            ["DEMO_DWF.A"],
        )

    def test_simple_program_keeps_direct_external_inputs(self) -> None:
        name = f"005:{TARGET}:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-simple"),),
            (
                edge(name, "program-simple", "DEMO_DWF.A", TARGET),
                edge(name, "program-simple", "DEMO_DWF.B", TARGET),
                edge(name, "program-simple", "DEMO_DWF.C", TARGET),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertFalse(projection.used_direct_fallback)
        self.assertEqual(
            [dependency.source_table for dependency in projection.dependencies],
            ["DEMO_DWF.A", "DEMO_DWF.B", "DEMO_DWF.C"],
        )
        self.assertTrue(
            all(
                dependency.target_table == TARGET
                for dependency in projection.dependencies
            )
        )

    def test_complex_program_projects_external_inputs_to_result(self) -> None:
        name = f"005:{TARGET}:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-complex"),),
            complex_edges(name, "program-complex"),
            target_tables=(TARGET,),
        )[0]

        self.assertFalse(projection.used_direct_fallback)
        self.assertEqual(
            {dependency.source_table for dependency in projection.dependencies},
            {"DEMO_DWF.A", "DEMO_DWF.B", "DEMO_DWF.C", "DEMO_DWF.D"},
        )
        self.assertNotIn(
            "DEMO_DWM.WORK_Z",
            {dependency.source_table for dependency in projection.dependencies},
        )

    def test_intermediate_names_do_not_change_projection(self) -> None:
        name = f"005:{TARGET}:1:00"
        base = complex_edges(name, "program-names")
        renamed = tuple(
            ProgramBoundaryBusinessEdge(
                environment=item.environment,
                source_profile=item.source_profile,
                program_key=item.program_key,
                program_name=item.program_name,
                source_table=item.source_table.replace("TMP_X", "HELLO")
                .replace("ABC_Y", "WORK_X")
                .replace("WORK_Z", "TEST_Z"),
                target_table=item.target_table.replace("TMP_X", "HELLO")
                .replace("ABC_Y", "WORK_X")
                .replace("WORK_Z", "TEST_Z"),
            )
            for item in base
        )
        first = build_program_boundary_projections(
            (program(name, key="program-names"),), base, target_tables=(TARGET,)
        )[0]
        second = build_program_boundary_projections(
            (program(name, key="program-names"),), renamed, target_tables=(TARGET,)
        )[0]

        self.assertEqual(
            {dependency.source_table for dependency in first.dependencies},
            {dependency.source_table for dependency in second.dependencies},
        )

    def test_tmp_named_program_result_is_authoritative(self) -> None:
        target = "DWP.TMP_RESULT"
        name = "005:DWS_DWP.TMP_RESULT:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-tmp-result"),),
            (edge(name, "program-tmp-result", "DEMO_DWF.A", target),),
            target_tables=(target,),
        )[0]

        self.assertEqual(projection.target_table, target)
        self.assertFalse(projection.used_direct_fallback)
        self.assertEqual(projection.dependencies[0].source_table, "DEMO_DWF.A")

    def test_multistep_programs_share_one_logical_boundary(self) -> None:
        first_name = f"005:{TARGET}:1:00"
        second_name = f"005:{TARGET}:2:00"
        programs = (
            program(second_name, key="program-step-2"),
            program(first_name, key="program-step-1"),
        )
        edges = (
            edge(first_name, "program-step-1", "DEMO_DWF.A", "DEMO_DWM.WORK_1"),
            edge(second_name, "program-step-2", "DEMO_DWF.B", "DEMO_DWM.WORK_2"),
            edge(second_name, "program-step-2", "DEMO_DWM.WORK_1", TARGET),
            edge(second_name, "program-step-2", "DEMO_DWM.WORK_2", TARGET),
        )
        projection = build_program_boundary_projections(
            programs, edges, target_tables=(TARGET,)
        )[0]

        self.assertEqual(
            {dependency.source_table for dependency in projection.dependencies},
            {"DEMO_DWF.A", "DEMO_DWF.B"},
        )
        self.assertEqual(
            projection.program_keys,
            ("program-step-1", "program-step-2"),
        )

    def test_two_programs_with_shared_dataset_do_not_cross_join(self) -> None:
        target_one = "DEMO_DWM.RESULT_1"
        target_two = "DEMO_DWM.RESULT_2"
        p1 = program(f"005:{target_one}:1:00", key="program-1")
        p2 = program(f"005:{target_two}:1:00", key="program-2")
        edges = (
            edge(p1.program_name, p1.program_key, "DEMO_DWF.A", "DEMO_DWM.SHARED"),
            edge(p1.program_name, p1.program_key, "DEMO_DWM.SHARED", target_one),
            edge(p2.program_name, p2.program_key, "DEMO_DWM.SHARED", target_two),
        )

        projections = build_program_boundary_projections(
            (p1, p2), edges, target_tables=(target_one, target_two)
        )

        self.assertEqual(
            {
                projection.target_table: {
                    dependency.source_table for dependency in projection.dependencies
                }
                for projection in projections
            },
            {
                target_one: {"DEMO_DWF.A"},
                target_two: {"DEMO_DWM.SHARED"},
            },
        )

    def test_m_sxed_stage_cycle_projects_all_external_inputs(self) -> None:
        target = "DWM.M_SXED"
        name = "005:DWS_DWM.M_SXED:1:01"
        program_key = "program-m-sxed-cycle"
        tmp_sh = "DWM.TMP_SH"
        tmp_sxed = "DWM.TMP_SXED"
        edges = (
            edge(name, program_key, "DWF.EXT_A", tmp_sh),
            edge(name, program_key, "DWF.EXT_B", tmp_sxed),
            edge(name, program_key, "DWF.EXT_C", tmp_sxed),
            edge(name, program_key, tmp_sxed, tmp_sh),
            edge(name, program_key, tmp_sh, tmp_sxed),
            edge(name, program_key, tmp_sxed, tmp_sxed),
            edge(name, program_key, tmp_sxed, target),
        )
        projection = build_program_boundary_projections(
            (program(name, key=program_key),), edges, target_tables=(target,)
        )[0]

        self.assertFalse(projection.used_direct_fallback)
        self.assertNotIn("PROGRAM_CYCLE", projection.diagnostics)
        self.assertEqual(
            {
                (dependency.source_table, dependency.target_table)
                for dependency in projection.dependencies
            },
            {
                ("DWF.EXT_A", target),
                ("DWF.EXT_B", target),
                ("DWF.EXT_C", target),
            },
        )

    def test_self_reference_does_not_trigger_cross_node_cycle(self) -> None:
        name = f"005:{TARGET}:1:00"
        tmp = "DEMO_DWM.TMP_X"
        edges = (
            edge(name, "program-self-loop", "DEMO_DWF.A", tmp),
            edge(name, "program-self-loop", tmp, tmp),
            edge(name, "program-self-loop", tmp, TARGET),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-self-loop"),),
            edges,
            target_tables=(TARGET,),
        )[0]

        self.assertFalse(projection.used_direct_fallback)
        self.assertNotIn("PROGRAM_CYCLE", projection.diagnostics)
        self.assertEqual(
            [
                (item.source_table, item.target_table)
                for item in projection.dependencies
            ],
            [("DEMO_DWF.A", TARGET)],
        )

    def test_disconnected_cycle_does_not_leak_source_into_result(self) -> None:
        name = f"005:{TARGET}:1:00"
        edges = (
            edge(name, "program-disconnected-cycle", "DEMO_DWF.EXT_BAD", "DEMO_DWM.X"),
            edge(name, "program-disconnected-cycle", "DEMO_DWM.X", "DEMO_DWM.Y"),
            edge(name, "program-disconnected-cycle", "DEMO_DWM.Y", "DEMO_DWM.X"),
            edge(name, "program-disconnected-cycle", "DEMO_DWF.EXT_GOOD", "DEMO_DWM.Z"),
            edge(name, "program-disconnected-cycle", "DEMO_DWM.Z", TARGET),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-disconnected-cycle"),),
            edges,
            target_tables=(TARGET,),
        )[0]

        self.assertFalse(projection.used_direct_fallback)
        self.assertEqual(
            [
                (item.source_table, item.target_table)
                for item in projection.dependencies
            ],
            [("DEMO_DWF.EXT_GOOD", TARGET)],
        )

    def test_cyclic_projection_is_independent_of_intermediate_names(self) -> None:
        target = "DWM.M_SXED"
        name = "005:DWS_DWM.M_SXED:1:01"
        program_key = "program-cycle-name-independent"
        base_edges = (
            ("DWF.EXT_A", "DWM.TMP_SH"),
            ("DWF.EXT_B", "DWM.TMP_SXED"),
            ("DWM.TMP_SXED", "DWM.TMP_SH"),
            ("DWM.TMP_SH", "DWM.TMP_SXED"),
            ("DWM.TMP_SXED", target),
        )
        renamed_edges = tuple(
            (
                source.replace("DWM.TMP_SH", "DWM.A").replace("DWM.TMP_SXED", "DWM.B"),
                destination.replace("DWM.TMP_SH", "DWM.A").replace(
                    "DWM.TMP_SXED", "DWM.B"
                ),
            )
            for source, destination in base_edges
        )

        def project(pairs):
            graph = tuple(
                edge(name, program_key, source, destination)
                for source, destination in pairs
            )
            return build_program_boundary_projections(
                (program(name, key=program_key),), graph, target_tables=(target,)
            )[0]

        tmp_projection = project(base_edges)
        ordinary_name_projection = project(renamed_edges)
        expected_sources = {"DWF.EXT_A", "DWF.EXT_B"}
        for projection in (tmp_projection, ordinary_name_projection):
            self.assertFalse(projection.used_direct_fallback)
            self.assertEqual(
                {item.source_table for item in projection.dependencies},
                expected_sources,
            )

    def test_self_reference_is_preserved_only_as_evidence(self) -> None:
        name = f"005:{TARGET}:1:00"
        edges = (
            edge(name, "program-self", "DEMO_DWF.A", TARGET),
            edge(name, "program-self", TARGET, TARGET),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-self"),), edges, target_tables=(TARGET,)
        )[0]

        self.assertEqual(
            {
                (dependency.source_table, dependency.target_table)
                for dependency in projection.dependencies
            },
            {(TARGET, TARGET), ("DEMO_DWF.A", TARGET)},
        )
        self.assertNotIn(
            (TARGET, TARGET),
            {
                (dependency.source_table, dependency.target_table)
                for dependency in projection.dependencies
                if dependency.source_table != TARGET
            },
        )

    def test_result_used_in_later_step_does_not_create_result_self_edge(self) -> None:
        name = f"005:{TARGET}:1:00"
        edges = (
            edge(name, "program-result-read", "DEMO_DWF.A", TARGET),
            edge(name, "program-result-read", TARGET, "DEMO_DWM.WORK"),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-result-read"),),
            edges,
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("PROGRAM_RESULT_USED_AS_SOURCE", projection.diagnostics)
        self.assertEqual(
            [
                (item.source_table, item.target_table)
                for item in projection.dependencies
            ],
            [("DEMO_DWF.A", TARGET)],
        )

    def test_duplicate_program_step_still_uses_direct_fallback(self) -> None:
        name = f"005:{TARGET}:1:00"
        programs = (
            program(name, key="program-step-duplicate-a"),
            program(name, key="program-step-duplicate-b"),
        )
        projection = build_program_boundary_projections(
            programs,
            (
                edge(name, "program-step-duplicate-a", "DEMO_DWF.A", TARGET),
                edge(name, "program-step-duplicate-b", "DEMO_DWF.B", TARGET),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("DUPLICATE_PROGRAM_STEP", projection.diagnostics)
        self.assertEqual(
            {item.source_table for item in projection.dependencies},
            {"DEMO_DWF.A", "DEMO_DWF.B"},
        )

    def test_ambiguous_program_identity_still_uses_direct_fallback(self) -> None:
        first_name = f"005:{TARGET}:1:00"
        second_name = f"005:{TARGET}:2:00"
        ambiguous_key = "program-ambiguous"
        projection = build_program_boundary_projections(
            (
                program(first_name, key=ambiguous_key),
                program(second_name, key=ambiguous_key),
            ),
            (
                edge(first_name, ambiguous_key, "DEMO_DWF.A", "DEMO_DWM.WORK"),
                edge(second_name, ambiguous_key, "DEMO_DWM.WORK", TARGET),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("AMBIGUOUS_PROGRAM_IDENTITY", projection.diagnostics)
        self.assertEqual(
            [item.source_table for item in projection.dependencies],
            ["DEMO_DWM.WORK"],
        )

    def test_edge_provenance_mismatch_still_uses_direct_fallback(self) -> None:
        name = f"005:{TARGET}:1:00"
        mismatched_name = "005:DEMO_DWM.OTHER:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-provenance"),),
            (
                edge(name, "program-provenance", "DEMO_DWF.A", "DEMO_DWM.WORK"),
                edge(
                    mismatched_name,
                    "program-provenance",
                    "DEMO_DWF.BAD",
                    "DEMO_DWM.WORK",
                ),
                edge(name, "program-provenance", "DEMO_DWM.WORK", TARGET),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("PROGRAM_EDGE_PROVENANCE_MISMATCH", projection.diagnostics)
        self.assertEqual(
            [item.source_table for item in projection.dependencies],
            ["DEMO_DWM.WORK"],
        )

    def test_missing_authoritative_result_still_uses_direct_fallback(self) -> None:
        name = f"005:{TARGET}:1:00"
        projection = build_program_boundary_projections(
            (program(name, key="program-no-result-edge"),),
            (
                edge(
                    name,
                    "program-no-result-edge",
                    "DEMO_DWF.A",
                    "DEMO_DWM.WORK",
                ),
            ),
            target_tables=(TARGET,),
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("PROGRAM_RESULT_NOT_OBSERVED", projection.diagnostics)
        self.assertEqual(projection.dependencies, ())

    def test_dlo_dwo_edges_do_not_create_boundary_bypass(self) -> None:
        name = f"005:{TARGET}:1:00"
        edges = (
            edge(name, "program-boundary-layer", "DLO.RAW", "DEMO_DWF.X"),
            edge(name, "program-boundary-layer", "DWO.WORK", TARGET),
            edge(name, "program-boundary-layer", "DEMO_DWF.A", TARGET),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-boundary-layer"),),
            edges,
            target_tables=(TARGET,),
        )[0]

        self.assertEqual(
            [
                (item.source_table, item.target_table)
                for item in projection.dependencies
            ],
            [("DEMO_DWF.A", TARGET)],
        )

    def test_non_005_or_malformed_program_falls_back_without_guessing(self) -> None:
        target = TARGET
        programs = (
            program("001:DEMO_DWM.RESULT:1:00", key="program-non-005"),
            program("005:DEMO_DWM.RESULT:broken:00", key="program-malformed"),
        )
        edges = (
            edge(
                programs[0].program_name, programs[0].program_key, "DEMO_DWF.A", target
            ),
            edge(
                programs[1].program_name,
                programs[1].program_key,
                "DEMO_DWF.B",
                target,
            ),
        )
        projection = build_program_boundary_projections(
            programs, edges, target_tables=(target,)
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("NO_AUTHORITATIVE_PROGRAM", projection.diagnostics)
        self.assertEqual(
            {dependency.source_table for dependency in projection.dependencies},
            {"DEMO_DWF.A", "DEMO_DWF.B"},
        )


class ProgramBoundaryReconciliationContractTests(unittest.TestCase):
    def test_complex_projection_matches_program_level_schedule(self) -> None:
        name = f"005:{TARGET}:1:00"
        sql_reader = _BoundaryReader(
            (
                build_program_boundary_projections(
                    (program(name, key="program-complex"),),
                    complex_edges(name, "program-complex"),
                    target_tables=(TARGET,),
                )[0],
            ),
            side="sql",
            batch_id="batch-sql",
            scope=((ENVIRONMENT, SQL_PROFILE),),
        )
        schedule_reader = _BoundaryReader(
            tuple(build_program_boundary_projections((), (), target_tables=(TARGET,))),
            side="schedule",
            batch_id="batch-schedule",
            scope=((ENVIRONMENT, SCHEDULE_PROFILE),),
            schedule_sources=("DEMO_DWF.A", "DEMO_DWF.B", "DEMO_DWF.C", "DEMO_DWF.D"),
        )
        timing = ReconciliationTiming()

        result = reconcile_active_dws_lineage(
            sql_reader,
            schedule_reader,
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_tables=(TARGET,),
            timing=timing,
        )

        self.assertEqual(len(result.rows), 4)
        self.assertTrue(
            all(row.status is ReconciliationStatus.MATCH for row in result.rows)
        )
        self.assertEqual(result.match_count, 4)
        self.assertEqual(result.schedule_only_count, 0)
        self.assertGreaterEqual(timing.sql_boundary_projection_rows, 4)

    def test_raw_reconciliation_projects_cycle_inputs_to_authoritative_result(
        self,
    ) -> None:
        target = "DWM.M_SXED"
        name = "005:DWS_DWM.M_SXED:1:01"
        program_key = "program-raw-cycle"
        tmp_sh = "DWM.TMP_SH"
        tmp_sxed = "DWM.TMP_SXED"
        edges = (
            edge(name, program_key, "DWF.EXT_A", tmp_sh),
            edge(name, program_key, "DWF.EXT_B", tmp_sxed),
            edge(name, program_key, "DWF.EXT_C", tmp_sxed),
            edge(name, program_key, tmp_sxed, tmp_sh),
            edge(name, program_key, tmp_sh, tmp_sxed),
            edge(name, program_key, tmp_sxed, tmp_sxed),
            edge(name, program_key, tmp_sxed, target),
        )
        projection = build_program_boundary_projections(
            (program(name, key=program_key),), edges, target_tables=(target,)
        )[0]
        sql_reader = _BoundaryReader(
            (projection,),
            side="sql",
            batch_id="batch-sql-raw-cycle",
            scope=((ENVIRONMENT, SQL_PROFILE),),
        )
        schedule_reader = _BoundaryReader(
            (),
            side="schedule",
            batch_id="batch-schedule-raw-cycle",
            scope=((ENVIRONMENT, SCHEDULE_PROFILE),),
        )

        result = reconcile_active_dws_lineage(
            sql_reader,
            schedule_reader,
            environment=ENVIRONMENT,
            sql_source_profile=SQL_PROFILE,
            schedule_source_profile=SCHEDULE_PROFILE,
            target_tables=(target,),
            apply_suppression=False,
        )

        self.assertEqual(
            {
                (row.source_table, row.target_table)
                for row in result.rows
                if row.status is ReconciliationStatus.SQL_ONLY
            },
            {
                ("DWF.EXT_A", target),
                ("DWF.EXT_B", target),
                ("DWF.EXT_C", target),
            },
        )
        self.assertEqual(len(result.rows), 3)
        self.assertEqual(result.sql_only_count, 3)


class _BoundaryReader:
    def __init__(
        self,
        projections,
        *,
        side: str,
        batch_id: str,
        scope: tuple[tuple[str, str], ...],
        schedule_sources: tuple[str, ...] = (),
    ) -> None:
        self.projections = tuple(projections)
        self.side = side
        self.batch_id = batch_id
        self.scope = scope
        self.schedule_sources = schedule_sources

    def get_active_snapshot_metadata(self):
        return SimpleNamespace(
            batch_id=self.batch_id,
            observed_at=OBSERVED_AT,
            snapshot_scope=self.scope,
            is_active=True,
        )

    def get_active_batch_id(self):
        return self.batch_id

    def get_batch_metadata(self, batch_id):
        return self.get_active_snapshot_metadata()

    def read_edges(self, **kwargs):
        raise AssertionError("Program Boundary test must not read direct SQL edges")

    def read_rows(self, **kwargs):
        raise AssertionError("Program Boundary test must not read schedule rows")

    def read_program_boundary_projection(self, **kwargs):
        return self.projections

    def read_reconciliation_projection(self, *, target_tables=None, **kwargs):
        if self.side != "schedule":
            raise AssertionError("SQL must use Program Boundary projection")
        from shared.lineage.reconciliation import ReconciliationFactProjection

        return tuple(
            ReconciliationFactProjection(source, TARGET, 1, 1)
            for source in self.schedule_sources
        )


if __name__ == "__main__":
    unittest.main()
