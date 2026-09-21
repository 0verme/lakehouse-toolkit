from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

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

    def test_cycle_falls_back_to_visible_direct_target_facts(self) -> None:
        name = f"005:{TARGET}:1:00"
        edges = (
            edge(name, "program-cycle", "DEMO_DWF.A", "DEMO_DWM.X"),
            edge(name, "program-cycle", "DEMO_DWM.X", "DEMO_DWM.Y"),
            edge(name, "program-cycle", "DEMO_DWM.Y", "DEMO_DWM.X"),
            edge(name, "program-cycle", "DEMO_DWM.X", TARGET),
        )
        projection = build_program_boundary_projections(
            (program(name, key="program-cycle"),), edges, target_tables=(TARGET,)
        )[0]

        self.assertTrue(projection.used_direct_fallback)
        self.assertIn("PROGRAM_CYCLE", projection.diagnostics)
        self.assertEqual(
            [dependency.source_table for dependency in projection.dependencies],
            ["DEMO_DWM.X"],
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
