from __future__ import annotations

import sqlite3
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from shared.lineage.audit import LineageAuditResult, audit_program_physical_dag
from shared.lineage.domain import IssueType, LineageIssue, ProgramSource
from shared.lineage.lineage_builder import normalize_table_name
from shared.lineage.materialization import (  # pyright: ignore[reportMissingImports]
    MaterializationBatch,
    materialize_batch,
    materialize_program,
)
from shared.lineage.materialization_sqlite import (  # pyright: ignore[reportMissingImports]
    SQLiteMaterializationStore,
)
from shared.lineage.physical_dag import (
    ProgramPhysicalDAG,
    build_program_physical_dag,
)
from tests.fixtures.lineage.phase5_materialization_programs import (  # pyright: ignore[reportMissingImports]
    CYCLE_PROGRAM,
    DIRECT_FORMAL_EDGE_PROGRAM,
    DUPLICATE_PHYSICAL_PATHS_PROGRAM,
    FORMAL_BOUNDARY_PROGRAM,
    MULTI_HOP_TMP_PROGRAM,
    MULTI_SOURCE_PROGRAM,
    MULTIPLE_FORMAL_BOUNDARIES_PROGRAM,
    ORPHAN_BRANCH_PROGRAM,
    SELF_REFERENCE_PROGRAM,
    SINGLE_TMP_PROGRAM,
    TMP_FANOUT_PROGRAM,
    UNKNOWN_TARGET_PROGRAM,
)

EXPECTED_RESULT = normalize_table_name("DWA.DEMO_RESULT")
EXPECTED_MIDDLE = normalize_table_name("DWA.DEMO_MIDDLE")
EXPECTED_REPORT = normalize_table_name("DM.DEMO_REPORT")
OBSERVED_AT = datetime(2026, 1, 5, 10, 11, 12, tzinfo=timezone.utc)


def build_dag(
    script_code: str,
    *,
    expected_target: str | None = EXPECTED_RESULT,
    program_name: str = "DEMO_PROGRAM_PHASE5",
) -> ProgramPhysicalDAG:
    source = ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name=program_name,
        script_code=script_code,
        expected_target=expected_target,
        source_hash=f"sha256:{program_name.lower()}",
    )
    return build_program_physical_dag(source)


def audit_dag(
    dag: ProgramPhysicalDAG,
    *,
    batch_id: str = "batch-test",
) -> LineageAuditResult:
    return audit_program_physical_dag(
        dag,
        observed_at=OBSERVED_AT,
        batch_id=batch_id,
    )


def edge_pairs(result) -> set[tuple[str, str]]:
    return {(edge.source_table, edge.target_table) for edge in result.edges}


def issue_of(issues: tuple[LineageIssue, ...], issue_type: IssueType) -> LineageIssue:
    matches = [issue for issue in issues if issue.issue_type is issue_type]
    if len(matches) != 1:
        raise AssertionError(f"expected one {issue_type.value}, got {len(matches)}")
    return matches[0]


class LineageMaterializationTests(unittest.TestCase):
    def test_single_tmp_collapses_to_one_formal_edge(self):
        result = materialize_program(
            build_dag(SINGLE_TMP_PROGRAM),
            batch_id="batch-001",
            observed_at=OBSERVED_AT,
            job_key="DEMO_JOB_SINGLE",
        )

        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        edge = result.edges[0]
        self.assertEqual(edge.program_name, "DEMO_PROGRAM_PHASE5")
        self.assertEqual(edge.job_key, "DEMO_JOB_SINGLE")
        self.assertEqual(edge.environment, "DEV")
        self.assertEqual(edge.source_profile, "fixture")
        self.assertEqual(edge.batch_id, "batch-001")
        self.assertTrue(edge.is_active)
        self.assertEqual(edge.observed_at, OBSERVED_AT)
        self.assertEqual(edge.updated_at, OBSERVED_AT)
        evidence = cast(dict[str, object], edge.evidence)
        self.assertEqual(evidence["collapsed_tmp_nodes"], ["TMP_1"])
        self.assertEqual(evidence["path_count"], 1)

    def test_multi_hop_tmp_collapses_without_transitive_formal_edges(self):
        result = materialize_program(
            build_dag(MULTI_HOP_TMP_PROGRAM),
            batch_id="batch-002",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["collapsed_tmp_nodes"], ["TMP_1", "TMP_2", "TMP_3"])

    def test_multi_source_tmp_fan_in_keeps_each_formal_source(self):
        result = materialize_program(
            build_dag(MULTI_SOURCE_PROGRAM),
            batch_id="batch-003",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(
            edge_pairs(result),
            {
                ("ODS.DEMO_A", EXPECTED_RESULT),
                (normalize_table_name("DWF.DEMO_B"), EXPECTED_RESULT),
                (normalize_table_name("DWM.DEMO_STAGE"), EXPECTED_RESULT),
                (normalize_table_name("DWA.DEMO_DIM"), EXPECTED_RESULT),
            },
        )

    def test_formal_asset_boundary_is_not_collapsed(self):
        result = materialize_program(
            build_dag(FORMAL_BOUNDARY_PROGRAM),
            batch_id="batch-004",
            observed_at=OBSERVED_AT,
        )
        stage = normalize_table_name("DWM.DEMO_STAGE")

        self.assertEqual(
            edge_pairs(result),
            {
                ("ODS.DEMO_A", stage),
                (stage, EXPECTED_RESULT),
            },
        )
        self.assertNotIn(("ODS.DEMO_A", EXPECTED_RESULT), edge_pairs(result))

    def test_multiple_formal_boundaries_each_stop_collapse(self):
        result = materialize_program(
            build_dag(
                MULTIPLE_FORMAL_BOUNDARIES_PROGRAM,
                expected_target="DM.DEMO_REPORT",
            ),
            batch_id="batch-005",
            observed_at=OBSERVED_AT,
        )
        stage = normalize_table_name("DWM.DEMO_STAGE")

        self.assertEqual(
            edge_pairs(result),
            {
                ("ODS.DEMO_A", stage),
                (stage, EXPECTED_MIDDLE),
                (EXPECTED_MIDDLE, EXPECTED_REPORT),
            },
        )
        self.assertNotIn(("ODS.DEMO_A", EXPECTED_MIDDLE), edge_pairs(result))
        self.assertNotIn(("ODS.DEMO_A", EXPECTED_REPORT), edge_pairs(result))

    def test_duplicate_physical_paths_are_deduplicated_with_merged_evidence(self):
        result = materialize_program(
            build_dag(DUPLICATE_PHYSICAL_PATHS_PROGRAM),
            batch_id="batch-006",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(len(result.edges), 1)
        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 2)
        self.assertEqual(evidence["collapsed_tmp_nodes"], ["TMP_1", "TMP_2"])

    def test_tmp_fanout_paths_merge_into_one_direct_edge(self):
        result = materialize_program(
            build_dag(TMP_FANOUT_PROGRAM),
            batch_id="batch-006-fanout",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 2)
        self.assertEqual(evidence["collapsed_tmp_nodes"], ["TMP_1", "TMP_2", "TMP_3"])

    def test_orphan_branch_is_excluded_but_issue_is_materialized(self):
        result = materialize_program(
            build_dag(ORPHAN_BRANCH_PROGRAM),
            batch_id="batch-007",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        self.assertNotIn(("ODS.DEMO_X", EXPECTED_RESULT), edge_pairs(result))
        orphan = issue_of(result.issues, IssueType.ORPHAN_BRANCH)
        self.assertEqual(orphan.branch_sink, "TMP_UNUSED_2")
        self.assertEqual(orphan.batch_id, "batch-007")
        self.assertEqual(orphan.last_seen_at, OBSERVED_AT)
        self.assertTrue(orphan.is_active)

    def test_direct_formal_edge_is_preserved(self):
        result = materialize_program(
            build_dag(DIRECT_FORMAL_EDGE_PROGRAM),
            batch_id="batch-008",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(edge_pairs(result), {("ODS.DEMO_A", EXPECTED_RESULT)})
        self.assertEqual(
            cast(dict[str, object], result.edges[0].evidence)["collapsed_tmp_nodes"],
            [],
        )

    def test_unknown_target_does_not_create_target_or_orphan_issue(self):
        result = materialize_program(
            build_dag(UNKNOWN_TARGET_PROGRAM, expected_target=None),
            batch_id="batch-009",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(
            edge_pairs(result),
            {
                ("ODS.DEMO_A", EXPECTED_RESULT),
                ("ODS.DEMO_B", normalize_table_name("DWA.DEMO_OTHER")),
            },
        )
        issue_types = {issue.issue_type for issue in result.issues}
        self.assertEqual(issue_types, {IssueType.MULTI_SINK_CANDIDATE})
        self.assertNotIn(IssueType.TARGET_NOT_FOUND, issue_types)
        self.assertNotIn(IssueType.TARGET_MISMATCH, issue_types)
        self.assertNotIn(IssueType.ORPHAN_BRANCH, issue_types)

    def test_cycle_and_self_reference_have_visited_protection(self):
        cycle = materialize_program(
            build_dag(CYCLE_PROGRAM, expected_target=None),
            batch_id="batch-010",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(cycle.edges, ())
        self.assertEqual(
            {issue.issue_type for issue in cycle.issues},
            {IssueType.CYCLE_DETECTED},
        )

        self_reference = materialize_program(
            build_dag(SELF_REFERENCE_PROGRAM, expected_target=None),
            batch_id="batch-011",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(
            edge_pairs(self_reference),
            {
                (
                    normalize_table_name("DWM.DEMO_SELF"),
                    normalize_table_name("DWM.DEMO_SELF"),
                )
            },
        )
        self.assertEqual(len(self_reference.edges), 1)
        self.assertEqual(
            self_reference.edges[0].source_table,
            normalize_table_name("DWM.DEMO_SELF"),
        )
        self.assertEqual(
            self_reference.edges[0].target_table,
            normalize_table_name("DWM.DEMO_SELF"),
        )
        self.assertEqual(
            [issue.issue_type for issue in self_reference.issues],
            [IssueType.SELF_REFERENCE],
        )

    def test_materialization_does_not_mutate_physical_dag(self):
        dag = build_dag(ORPHAN_BRANCH_PROGRAM)
        before = (dag.nodes, dag.edges, dag.steps, dag.sinks, dag.expected_target)

        materialize_program(dag, batch_id="batch-012", observed_at=OBSERVED_AT)

        self.assertEqual(
            (dag.nodes, dag.edges, dag.steps, dag.sinks, dag.expected_target),
            before,
        )

    def test_same_input_has_deterministic_edges_evidence_and_issues(self):
        dag = build_dag(DUPLICATE_PHYSICAL_PATHS_PROGRAM)
        reversed_dag = replace(
            dag,
            edges=tuple(reversed(dag.edges)),
            sinks=tuple(reversed(dag.sinks)),
        )
        first = materialize_program(dag, batch_id="batch-013", observed_at=OBSERVED_AT)
        second = materialize_program(
            reversed_dag,
            batch_id="batch-013",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(first.edges, second.edges)
        self.assertEqual(first.issues, second.issues)

    def test_batch_deduplicates_edges_and_keeps_one_batch_id(self):
        first_dag = build_dag(
            SINGLE_TMP_PROGRAM,
            program_name="DEMO_PROGRAM_A",
        )
        second_dag = build_dag(
            SINGLE_TMP_PROGRAM,
            program_name="DEMO_PROGRAM_B",
        )
        audits = [audit_dag(first_dag), audit_dag(second_dag)]
        batch = materialize_batch(
            audits,
            batch_id="batch-014",
            observed_at=OBSERVED_AT,
            job_keys={"DEMO_PROGRAM_A": "JOB_A", "DEMO_PROGRAM_B": "JOB_B"},
        )

        self.assertEqual(len(batch.edges), 2)
        self.assertEqual(
            {(edge.program_name, edge.job_key) for edge in batch.edges},
            {("DEMO_PROGRAM_A", "JOB_A"), ("DEMO_PROGRAM_B", "JOB_B")},
        )
        self.assertTrue(all(edge.batch_id == "batch-014" for edge in batch.edges))
        self.assertTrue(all(issue.batch_id == "batch-014" for issue in batch.issues))


class SQLiteMaterializationTests(unittest.TestCase):
    def test_schema_publish_and_structured_evidence_roundtrip(self):
        dag = build_dag(ORPHAN_BRANCH_PROGRAM, program_name="DEMO_PROGRAM_SQLITE")
        audit = audit_dag(dag, batch_id="batch-015")
        batch = materialize_batch(
            [audit], batch_id="batch-015", observed_at=OBSERVED_AT
        )

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            store = SQLiteMaterializationStore(db_path)
            publish_result = store.publish(batch)

            self.assertEqual(publish_result.batch_id, "batch-015")
            self.assertEqual(store.get_active_batch_id(), "batch-015")
            edges = store.read_edges(active_only=True)
            issues = store.read_issues(active_only=True)
            self.assertEqual(len(edges), 1)
            self.assertEqual(edges[0].source_table, "ODS.DEMO_A")
            self.assertEqual(edges[0].target_table, EXPECTED_RESULT)
            self.assertEqual(edges[0].program_name, "DEMO_PROGRAM_SQLITE")
            self.assertEqual(edges[0].environment, "DEV")
            self.assertEqual(edges[0].source_profile, "fixture")
            self.assertEqual(
                edges[0].source_hash,
                "sha256:demo_program_sqlite",
            )
            self.assertEqual(edges[0].batch_id, "batch-015")
            self.assertTrue(edges[0].is_active)
            self.assertIsInstance(edges[0].evidence, dict)
            self.assertEqual(
                cast(dict[str, object], edges[0].evidence)["path_count"], 1
            )
            orphan = issue_of(issues, IssueType.ORPHAN_BRANCH)
            self.assertEqual(
                orphan.stable_key,
                issue_of(batch.issues, IssueType.ORPHAN_BRANCH).stable_key,
            )
            self.assertEqual(orphan.issue_type, IssueType.ORPHAN_BRANCH)
            self.assertEqual(orphan.batch_id, "batch-015")
            self.assertEqual(orphan.node_key, None)
            self.assertIsInstance(orphan.evidence, dict)

            with sqlite3.connect(db_path) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                self.assertTrue(
                    {"lineage_batch", "lineage_edge", "lineage_issue"}.issubset(tables)
                )
                self.assertNotIn("lineage_closure", tables)
                edge_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(lineage_edge)")
                }
                self.assertTrue(
                    {
                        "environment",
                        "source_profile",
                        "source_table",
                        "target_table",
                        "program_name",
                        "job_key",
                        "evidence_type",
                        "source_hash",
                        "batch_id",
                        "observed_at",
                        "updated_at",
                        "is_active",
                    }.issubset(edge_columns)
                )
                issue_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(lineage_issue)")
                }
                self.assertTrue(
                    {
                        "environment",
                        "source_profile",
                        "program_name",
                        "issue_type",
                        "severity",
                        "stable_key",
                        "node_key",
                        "branch_sink",
                        "message",
                        "evidence",
                        "batch_id",
                        "first_seen_at",
                        "last_seen_at",
                        "is_active",
                    }.issubset(issue_columns)
                )
            connection.close()

    def test_failed_publish_preserves_previous_complete_active_batch(self):
        first_audit = audit_dag(
            build_dag(SINGLE_TMP_PROGRAM, program_name="DEMO_PROGRAM_BATCH_ONE"),
            batch_id="batch-016",
        )
        second_audit = audit_dag(
            build_dag(
                DIRECT_FORMAL_EDGE_PROGRAM, program_name="DEMO_PROGRAM_BATCH_TWO"
            ),
            batch_id="batch-017",
        )
        first_batch = materialize_batch(
            [first_audit], batch_id="batch-016", observed_at=OBSERVED_AT
        )
        second_batch = materialize_batch(
            [second_audit],
            batch_id="batch-017",
            observed_at=OBSERVED_AT.replace(day=6),
        )

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(first_batch)

            def fail_after_switch(stage: str) -> None:
                if stage == "after_active_switch":
                    raise RuntimeError("injected publish failure")

            with self.assertRaisesRegex(RuntimeError, "injected publish failure"):
                store.publish(second_batch, stage_hook=fail_after_switch)

            self.assertEqual(store.get_active_batch_id(), "batch-016")
            active_edges = store.read_edges(active_only=True)
            self.assertEqual(
                {
                    (edge.batch_id, edge.source_table, edge.target_table)
                    for edge in active_edges
                },
                {("batch-016", "ODS.DEMO_A", EXPECTED_RESULT)},
            )
            self.assertEqual(store.read_edges(batch_id="batch-017"), ())
            self.assertEqual(store.read_issues(batch_id="batch-017"), ())

            result = store.publish(second_batch)
            self.assertEqual(result.previous_batch_id, "batch-016")
            self.assertEqual(store.get_active_batch_id(), "batch-017")
            self.assertTrue(
                all(edge.is_active for edge in store.read_edges(active_only=True))
            )
            self.assertTrue(
                all(
                    not edge.is_active
                    for edge in store.read_edges(batch_id="batch-016")
                )
            )

    def test_invalid_candidate_is_rolled_back_before_active_switch(self):
        valid_audit = audit_dag(
            build_dag(SINGLE_TMP_PROGRAM, program_name="DEMO_PROGRAM_VALID"),
            batch_id="batch-018",
        )
        valid_batch = materialize_batch(
            [valid_audit], batch_id="batch-018", observed_at=OBSERVED_AT
        )
        invalid_edge = replace(
            valid_batch.edges[0],
            evidence={"bad": object()},
        )
        invalid_batch = MaterializationBatch(
            batch_id="batch-019",
            observed_at=OBSERVED_AT,
            edges=(invalid_edge,),
        )

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(valid_batch)
            with self.assertRaises(TypeError):
                store.publish(invalid_batch)
            self.assertEqual(store.get_active_batch_id(), "batch-018")
            self.assertEqual(store.read_edges(batch_id="batch-019"), ())


class CrontabMaterializationTests(unittest.TestCase):
    def test_crontab_entrypoint_accepts_injected_public_provider(self):
        from jobs.crontab.imp_lineage_edge import main

        source = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="DEMO_PROGRAM_CRON",
            script_code=SINGLE_TMP_PROGRAM,
            expected_target="DWA.DEMO_RESULT",
        )

        class FixtureProvider:
            def iter_program_sources(self):
                yield source

        with TemporaryDirectory() as directory:
            result = main(
                [FixtureProvider()],
                db_path=Path(directory) / "lineage.db",
                batch_id="batch-020",
                observed_at=OBSERVED_AT,
            )
            self.assertEqual(result, 0)
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            self.assertEqual(store.get_active_batch_id(), "batch-020")
            self.assertEqual(len(store.read_edges(active_only=True)), 1)


if __name__ == "__main__":
    unittest.main()
