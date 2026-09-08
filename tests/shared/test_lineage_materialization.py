from __future__ import annotations

import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from unittest.mock import patch

from shared.lineage.audit import LineageAuditResult, audit_program_physical_dag
from shared.lineage.domain import (
    IssueType,
    LineageIssue,
    PhysicalEdge,
    PhysicalNode,
    PhysicalNodeKind,
    ProgramSource,
)
from shared.lineage.lineage_builder import normalize_table_name
import shared.lineage.materialization as materialization_module
from shared.lineage.materialization import (  # pyright: ignore[reportMissingImports]
    LineageEvidenceError,
    LineagePathEnumerationError,
    MaterializationBatch,
    materialize_batch,
    materialize_program,
)
from shared.lineage.materialization_sqlite import (  # pyright: ignore[reportMissingImports]
    CURRENT_SCHEMA_VERSION,
    SQLiteMaterializationStore,
)
from shared.lineage.physical_dag import (
    ProgramPhysicalDAG,
    build_program_physical_dag,
)
from tests.fixtures.lineage.pathological_materialization import (
    make_dense_pathological_dag,
    make_diamond_dag,
    make_high_fanin_dag,
    make_high_fanout_dag,
    make_linear_tmp_dag,
    make_mixed_pathological_dag,
    make_parallel_tmp_dag,
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


def evidence_items(evidence: dict[str, object], name: str) -> list[object]:
    value = evidence.get(name)
    if not isinstance(value, list):
        raise AssertionError(f"evidence.{name} must be a list")
    return value


def legacy_collapsed_paths(
    dag: ProgramPhysicalDAG,
    included_nodes: set[str],
) -> tuple[tuple[tuple[str, ...], tuple[PhysicalEdge, ...]], ...]:
    """Test-only copy of the pre-streaming all-path enumerator."""

    node_map = materialization_module._node_map(dag)
    adjacency_lists: dict[str, list[PhysicalEdge]] = {}
    for edge in dag.edges:
        adjacency_lists.setdefault(edge.source, []).append(edge)
    adjacency: dict[str, tuple[PhysicalEdge, ...]] = {
        source: tuple(sorted(items, key=materialization_module._physical_edge_sort_key))
        for source, items in adjacency_lists.items()
    }
    formal_starts = sorted(
        node
        for node in included_nodes
        if not materialization_module._is_temporary(node, node_map)
    )
    paths: dict[tuple[str, ...], tuple[PhysicalEdge, ...]] = {}
    traversed_states = 0
    for start in formal_starts:
        pending: list[tuple[str, tuple[str, ...], tuple[PhysicalEdge, ...]]] = [
            (start, (start,), ())
        ]
        while pending:
            traversed_states += 1
            if traversed_states > materialization_module.MAX_COLLAPSED_TRAVERSAL_STATES:
                raise LineagePathEnumerationError(
                    "collapsed path traversal exceeds maximum state count"
                )
            current, path, path_edges = pending.pop()
            for edge in adjacency.get(current, ()):
                next_node = edge.target
                if next_node not in included_nodes:
                    continue
                if materialization_module._is_temporary(next_node, node_map):
                    if next_node in path:
                        continue
                    pending.append(
                        (next_node, path + (next_node,), path_edges + (edge,))
                    )
                    continue
                completed_path = path + (next_node,)
                if completed_path in paths:
                    continue
                if len(paths) >= materialization_module.MAX_COLLAPSED_PATHS:
                    raise LineagePathEnumerationError(
                        "collapsed physical path count exceeds maximum"
                    )
                paths[completed_path] = path_edges + (edge,)
    return tuple(
        sorted(
            paths.items(),
            key=lambda item: (
                materialization_module._canonical_json(item[0]),
                tuple(
                    materialization_module._physical_edge_sort_key(edge)
                    for edge in item[1]
                ),
            ),
        )
    )


def legacy_collapse_paths_to_edges(
    dag: ProgramPhysicalDAG,
    audit: LineageAuditResult,
    *,
    batch_id: str,
    observed_at: datetime,
    job_key: str | None = None,
) -> tuple:
    """Test-only oracle copied from the pre-streaming path merge algorithm."""

    grouped = {}
    paths = legacy_collapsed_paths(
        dag,
        materialization_module._included_nodes(audit),
    )
    for path, physical_edges in paths:
        edge = materialization_module._lineage_edge_from_path(
            dag,
            path,
            physical_edges,
            batch_id=batch_id,
            observed_at=observed_at,
            job_key=job_key,
        )
        identity = materialization_module._edge_identity(edge)
        entry = grouped.get(identity)
        if entry is None:
            accumulator = materialization_module._EdgeEvidenceAccumulator()
            grouped[identity] = (edge, accumulator)
        else:
            accumulator = entry[1]
        accumulator.add_evidence(edge.evidence)

    edges = [
        replace(edge, evidence=accumulator.finalize())
        for edge, accumulator in grouped.values()
    ]
    return tuple(
        sorted(
            edges,
            key=lambda edge: (
                materialization_module._edge_identity(edge),
                edge.source_hash or "",
                edge.evidence_type,
                materialization_module._canonical_json(edge.evidence),
            ),
        )
    )


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

    def test_pathological_paths_keep_full_count_and_bounded_evidence(self):
        for path_count in (10, 100, 500, 1000):
            dag = make_parallel_tmp_dag(path_count)
            result = materialize_program(
                dag,
                audit_dag(dag),
                batch_id=f"batch-paths-{path_count}",
                observed_at=OBSERVED_AT,
            )

            self.assertEqual(len(result.edges), 1)
            edge = result.edges[0]
            self.assertEqual(edge.source_table, "ODS.DEMO_SOURCE")
            self.assertEqual(edge.target_table, "DWA.DEMO_RESULT")
            evidence = cast(dict[str, object], edge.evidence)
            self.assertEqual(evidence["path_count"], path_count)
            self.assertEqual(
                len(evidence_items(evidence, "physical_paths")),
                min(path_count, materialization_module.MAX_PHYSICAL_PATHS),
            )
            self.assertEqual(
                evidence["physical_paths_truncated"],
                path_count > materialization_module.MAX_PHYSICAL_PATHS,
            )
            self.assertLessEqual(
                len(evidence_items(evidence, "physical_paths")),
                materialization_module.MAX_PHYSICAL_PATHS,
            )
            self.assertLessEqual(
                len(evidence_items(evidence, "physical_edge_pairs")),
                materialization_module.MAX_PHYSICAL_EDGE_PAIRS,
            )
            self.assertLessEqual(
                len(evidence_items(evidence, "collapsed_tmp_nodes")),
                materialization_module.MAX_COLLAPSED_TMP_NODES,
            )
            self.assertLessEqual(
                len(evidence_items(evidence, "statement_indices")),
                materialization_module.MAX_STATEMENT_INDICES,
            )

    def test_pathological_path_sample_is_deterministic(self):
        dag = make_parallel_tmp_dag(1000)
        reversed_dag = replace(
            dag,
            nodes=tuple(reversed(dag.nodes)),
            edges=tuple(reversed(dag.edges)),
        )
        first = materialize_program(
            dag,
            audit_dag(dag),
            batch_id="batch-path-determinism",
            observed_at=OBSERVED_AT,
        )
        second = materialize_program(
            reversed_dag,
            audit_dag(reversed_dag),
            batch_id="batch-path-determinism",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(first.edges, second.edges)
        evidence = cast(dict[str, object], first.edges[0].evidence)
        self.assertTrue(evidence["physical_paths_truncated"])
        self.assertEqual(
            len(evidence_items(evidence, "physical_paths")),
            materialization_module.MAX_PHYSICAL_PATHS,
        )

    def test_merging_identical_bounded_evidence_does_not_double_path_count(self):
        dag = make_parallel_tmp_dag(1000)
        result = materialize_program(
            dag,
            audit_dag(dag),
            batch_id="batch-merge-bounded",
            observed_at=OBSERVED_AT,
        )
        evidence = cast(dict[str, object], result.edges[0].evidence)

        merged = materialization_module._merge_edge_evidence(evidence, evidence)

        self.assertEqual(merged["path_count"], 1000)
        self.assertEqual(
            len(evidence_items(merged, "physical_paths")),
            materialization_module.MAX_PHYSICAL_PATHS,
        )
        self.assertTrue(merged["physical_paths_truncated"])

    def test_diamond_paths_are_collapsed_without_transitive_formal_edges(self):
        dag = make_diamond_dag(4)
        result = materialize_program(
            dag,
            audit_dag(dag),
            batch_id="batch-diamond",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in result.edges},
            {("ODS.DEMO_SOURCE", "DWA.DEMO_RESULT")},
        )
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 16)
        self.assertEqual(len(evidence_items(evidence, "physical_paths")), 16)
        self.assertFalse(evidence["physical_paths_truncated"])

    def test_streaming_accumulator_preserves_all_evidence_summaries(self):
        dag = make_parallel_tmp_dag(3)
        result = materialize_program(
            dag,
            audit_dag(dag),
            batch_id="batch-summary",
            observed_at=OBSERVED_AT,
        )
        evidence = cast(dict[str, object], result.edges[0].evidence)

        self.assertEqual(evidence["path_count"], 3)
        self.assertEqual(
            {
                tuple(pair)
                for pair in evidence_items(evidence, "physical_edge_pairs")
                if isinstance(pair, list)
            },
            {("ODS.DEMO_SOURCE", f"TMP_BRANCH_{index:04d}") for index in range(3)}
            | {(f"TMP_BRANCH_{index:04d}", "DWA.DEMO_RESULT") for index in range(3)},
        )
        self.assertEqual(
            set(evidence_items(evidence, "collapsed_tmp_nodes")),
            {f"TMP_BRANCH_{index:04d}" for index in range(3)},
        )
        self.assertEqual(
            set(evidence_items(evidence, "statement_indices")), set(range(6))
        )
        self.assertEqual(len(evidence_items(evidence, "physical_paths")), 3)
        self.assertFalse(evidence["physical_paths_truncated"])
        self.assertFalse(evidence["physical_edge_pairs_truncated"])
        self.assertFalse(evidence["collapsed_tmp_nodes_truncated"])
        self.assertFalse(evidence["statement_indices_truncated"])

    def test_streaming_materialization_builds_only_final_lineage_edges(self):
        dag = make_diamond_dag(6)
        calls = 0
        original_add_path = materialization_module._EdgeEvidenceAccumulator.add_path

        def counted_add_path(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            return original_add_path(self, *args, **kwargs)

        with patch.object(
            materialization_module._EdgeEvidenceAccumulator,
            "add_path",
            counted_add_path,
        ):
            with patch.object(
                materialization_module,
                "_lineage_edge_from_path",
                side_effect=AssertionError("per-path LineageEdge construction"),
            ):
                with patch.object(
                    materialization_module,
                    "_edge_evidence",
                    side_effect=AssertionError("per-path evidence construction"),
                ):
                    with patch.object(
                        materialization_module._EdgeEvidenceAccumulator,
                        "add_evidence",
                        side_effect=AssertionError("per-path evidence merge"),
                    ):
                        result = materialize_program(
                            dag,
                            audit_dag(dag),
                            batch_id="batch-streaming",
                            observed_at=OBSERVED_AT,
                        )

        self.assertEqual(len(result.edges), 1)
        self.assertEqual(calls, 64)
        self.assertEqual(
            cast(dict[str, object], result.edges[0].evidence)["path_count"],
            64,
        )

    def test_streaming_output_matches_legacy_materialization_oracle(self):
        cases = (
            ("linear", make_linear_tmp_dag(4)),
            ("diamond", make_diamond_dag(4)),
            ("fanout", make_high_fanout_dag(5)),
            ("fanin", make_high_fanin_dag(5)),
        )
        for name, dag in cases:
            with self.subTest(case=name):
                audit = audit_dag(dag, batch_id="batch-oracle")
                result = materialize_program(
                    dag,
                    audit,
                    batch_id="batch-oracle",
                    observed_at=OBSERVED_AT,
                )
                self.assertEqual(
                    result.edges,
                    legacy_collapse_paths_to_edges(
                        dag,
                        audit,
                        batch_id="batch-oracle",
                        observed_at=OBSERVED_AT,
                    ),
                )

    def test_duplicate_physical_edge_pairs_keep_legacy_representative(self):
        source_name = "ODS.DEMO_DUPLICATE_SOURCE"
        temporary_name = "TMP.DEMO_DUPLICATE"
        target_name = "DWA.DEMO_DUPLICATE_RESULT"
        source = ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="DEMO_DUPLICATE_EDGE_PAIRS",
            script_code="",
            expected_target=target_name,
            source_hash="sha256:demo-duplicate-edge-pairs",
        )
        dag = ProgramPhysicalDAG(
            program_source=source,
            nodes=(
                PhysicalNode(source_name, source_name),
                PhysicalNode(
                    temporary_name,
                    temporary_name,
                    PhysicalNodeKind.TEMPORARY_ASSET,
                ),
                PhysicalNode(target_name, target_name),
            ),
            edges=(
                PhysicalEdge(
                    source_name,
                    temporary_name,
                    evidence={"statement_index": 1},
                ),
                PhysicalEdge(
                    source_name,
                    temporary_name,
                    evidence={"statement_index": 99},
                ),
                PhysicalEdge(
                    temporary_name,
                    target_name,
                    evidence={"statement_index": 2},
                ),
                PhysicalEdge(
                    temporary_name,
                    target_name,
                    evidence={"statement_index": 98},
                ),
            ),
            steps=(),
            sinks=(target_name,),
            expected_target=target_name,
        )
        audit = audit_dag(dag, batch_id="batch-duplicate-edge-pairs")
        result = materialize_program(
            dag,
            audit,
            batch_id="batch-duplicate-edge-pairs",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(
            result.edges,
            legacy_collapse_paths_to_edges(
                dag,
                audit,
                batch_id="batch-duplicate-edge-pairs",
                observed_at=OBSERVED_AT,
            ),
        )
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 1)
        self.assertEqual(set(evidence_items(evidence, "statement_indices")), {2, 99})

    def test_dense_pathological_graph_keeps_exact_count_without_full_enumeration(self):
        dag = make_dense_pathological_dag()
        outgoing = {}
        incoming = {}
        for edge in dag.edges:
            outgoing[edge.source] = outgoing.get(edge.source, 0) + 1
            incoming[edge.target] = incoming.get(edge.target, 0) + 1
        self.assertEqual((len(dag.nodes), len(dag.edges)), (43, 407))
        self.assertEqual(max(outgoing.values()), 36)
        self.assertEqual(max(incoming.values()), 39)
        self.assertEqual(sum(count > 1 for count in outgoing.values()), 35)
        self.assertEqual(sum(count > 1 for count in incoming.values()), 36)
        self.assertEqual(len({(edge.source, edge.target) for edge in dag.edges}), 407)

        audit = audit_dag(dag, batch_id="batch-dense")
        with self.assertRaises(LineagePathEnumerationError):
            tuple(
                legacy_collapsed_paths(
                    dag,
                    materialization_module._included_nodes(audit),
                )
            )

        with patch.object(
            materialization_module,
            "_collapsed_paths",
            side_effect=AssertionError("dense graph must not enumerate all paths"),
        ):
            result = materialize_program(
                dag,
                audit,
                batch_id="batch-dense",
                observed_at=OBSERVED_AT,
            )
        reversed_dag = replace(dag, edges=tuple(reversed(dag.edges)))
        reversed_result = materialize_program(
            reversed_dag,
            audit_dag(reversed_dag, batch_id="batch-dense"),
            batch_id="batch-dense",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(result.edges, reversed_result.edges)
        self.assertEqual(len(result.edges), 1)
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 766097916)
        self.assertEqual(len(evidence_items(evidence, "physical_paths")), 100)
        self.assertTrue(evidence["physical_paths_truncated"])
        self.assertEqual(len(evidence_items(evidence, "physical_edge_pairs")), 200)
        self.assertTrue(evidence["physical_edge_pairs_truncated"])
        self.assertEqual(len(evidence_items(evidence, "collapsed_tmp_nodes")), 41)
        self.assertEqual(len(evidence_items(evidence, "statement_indices")), 200)
        self.assertTrue(evidence["statement_indices_truncated"])

    def test_mixed_pathological_graph_matches_target_shape_and_count(self):
        dag = make_mixed_pathological_dag()
        self.assertEqual((len(dag.nodes), len(dag.edges)), (154, 350))
        result = materialize_program(
            dag,
            audit_dag(dag, batch_id="batch-mixed"),
            batch_id="batch-mixed",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(len(result.edges), 1)
        evidence = cast(dict[str, object], result.edges[0].evidence)
        self.assertEqual(evidence["path_count"], 20480)
        self.assertEqual(len(evidence_items(evidence, "physical_paths")), 100)
        self.assertTrue(evidence["physical_paths_truncated"])

    def test_materialization_canonicalization_scales_with_path_count(self):
        original_canonical = materialization_module._canonical_json_safe
        measurements = []

        for path_count in (10, 100, 500, 1000):
            calls = 0

            def counted_canonical(value):
                nonlocal calls
                calls += 1
                return original_canonical(value)

            dag = make_parallel_tmp_dag(path_count)
            audit = audit_dag(dag)
            with patch.object(
                materialization_module,
                "_canonical_json_safe",
                counted_canonical,
            ):
                result = materialize_program(
                    dag,
                    audit,
                    batch_id=f"batch-scaling-{path_count}",
                    observed_at=OBSERVED_AT,
                )
            evidence = cast(dict[str, object], result.edges[0].evidence)
            measurements.append((path_count, calls, evidence["path_count"]))
            self.assertEqual(evidence["path_count"], path_count)
            self.assertGreater(calls, 0)
            self.assertLessEqual(
                calls,
                (materialization_module.MAX_PHYSICAL_PATHS + 1) * 64
                + len(dag.edges) * 64,
            )

        self.assertEqual(
            [path_count for path_count, _, _ in measurements],
            [10, 100, 500, 1000],
        )

    def test_json_safe_rejects_recursive_deep_and_oversized_evidence(self):
        recursive: dict[str, object] = {}
        recursive["self"] = recursive
        with self.assertRaisesRegex(LineageEvidenceError, "recursive cycle"):
            materialization_module._json_safe(recursive)

        deep: object = "leaf"
        for _ in range(materialization_module.MAX_EVIDENCE_DEPTH + 1):
            deep = [deep]
        with self.assertRaisesRegex(LineageEvidenceError, "maximum nesting depth"):
            materialization_module._json_safe(deep)

        oversized = [None] * (materialization_module.MAX_EVIDENCE_COLLECTION_SIZE + 1)
        with self.assertRaisesRegex(LineageEvidenceError, "maximum size"):
            materialization_module._json_safe(oversized)

    def test_explicit_path_enumeration_limit_fails_as_controlled_python_error(self):
        dag = make_diamond_dag(3)
        audit = audit_dag(dag)
        with patch.object(materialization_module, "MAX_COLLAPSED_PATHS", 4):
            with self.assertRaises(LineagePathEnumerationError):
                tuple(
                    materialization_module._collapsed_paths(
                        dag,
                        materialization_module._included_nodes(audit),
                    )
                )

    def test_explicit_path_traversal_limit_fails_before_unbounded_branch_expansion(
        self,
    ):
        dag = make_diamond_dag(4)
        audit = audit_dag(dag)
        with patch.object(
            materialization_module,
            "MAX_COLLAPSED_TRAVERSAL_STATES",
            4,
        ):
            with self.assertRaises(LineagePathEnumerationError):
                tuple(
                    materialization_module._collapsed_paths(
                        dag,
                        materialization_module._included_nodes(audit),
                    )
                )

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
    def test_old_program_state_schema_is_migrated_without_losing_legacy_state(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "legacy-lineage.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.executescript(
                    """
                    CREATE TABLE lineage_batch (
                        batch_id TEXT PRIMARY KEY,
                        observed_at TEXT NOT NULL,
                        published_at TEXT,
                        edge_count INTEGER NOT NULL,
                        issue_count INTEGER NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE lineage_program_state (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        environment TEXT NOT NULL,
                        source_profile TEXT NOT NULL,
                        program_name TEXT NOT NULL,
                        source_hash TEXT,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL,
                        last_changed_at TEXT,
                        batch_id TEXT NOT NULL,
                        is_active INTEGER NOT NULL
                    );
                    INSERT INTO lineage_batch(
                        batch_id, observed_at, published_at, edge_count,
                        issue_count, is_active
                    ) VALUES (
                        'batch-legacy', '2026-01-01T00:00:00+00:00',
                        '2026-01-01T00:00:00+00:00', 0, 0, 1
                    );
                    INSERT INTO lineage_program_state(
                        environment, source_profile, program_name, source_hash,
                        first_seen_at, last_seen_at, last_changed_at, batch_id,
                        is_active
                    ) VALUES (
                        'DEV', 'fixture', 'PROGRAM_LEGACY', 'legacy-hash',
                        '2026-01-01T00:00:00+00:00',
                        '2026-01-01T00:00:00+00:00', NULL,
                        'batch-legacy', 1
                    );
                    """
                )

            store = SQLiteMaterializationStore(db_path)
            states = store.read_program_states(active_only=True)
            with closing(sqlite3.connect(db_path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(lineage_program_state)"
                    )
                }
                schema_version = connection.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(len(states), 1)
        self.assertIsNone(states[0].pipeline_version)
        self.assertIn("pipeline_version", columns)
        self.assertEqual(schema_version, CURRENT_SCHEMA_VERSION)

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
                program_state_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(lineage_program_state)"
                    )
                }
                self.assertIn("pipeline_version", program_state_columns)
            connection.close()

    def test_bounded_evidence_roundtrips_through_sqlite(self):
        dag = make_parallel_tmp_dag(1000)
        batch = materialize_batch(
            [audit_dag(dag)],
            batch_id="batch-bounded-sqlite",
            observed_at=OBSERVED_AT,
        )

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(batch)
            [edge] = store.read_edges(active_only=True)

        evidence = cast(dict[str, object], edge.evidence)
        self.assertEqual(evidence["path_count"], 1000)
        self.assertEqual(
            len(evidence_items(evidence, "physical_paths")),
            materialization_module.MAX_PHYSICAL_PATHS,
        )
        self.assertTrue(evidence["physical_paths_truncated"])

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
