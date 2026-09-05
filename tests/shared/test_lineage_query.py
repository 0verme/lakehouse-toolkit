from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from shared.lineage import (
    BlastRadiusResult,
    LineageDirection,
    LineageEdge,
    LineageQueryService,
    MaterializationBatch,
    SQLiteMaterializationStore,
    analyze_blast_radius,
    query_downstream,
    query_upstream,
)
from tests.fixtures.lineage.phase6_query_edges import (
    BRANCH_EDGES,
    CYCLE_EDGES,
    ENVIRONMENT_EDGES,
    MULTIPLE_PROGRAM_SAME_EDGE,
    MULTIPLE_SOURCE_PROFILE_EDGES,
    SELF_REFERENCE_EDGES,
    SIMPLE_CHAIN_EDGES,
    demo_edge,
    fanout_edges,
)

OBSERVED_AT = datetime(2026, 2, 6, 10, 11, 12, tzinfo=timezone.utc)


def publish_store(
    directory: str,
    edges: tuple[LineageEdge, ...],
    *,
    batch_id: str = "batch-phase6",
) -> SQLiteMaterializationStore:
    store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
    store.publish(
        MaterializationBatch(
            batch_id=batch_id,
            observed_at=OBSERVED_AT,
            edges=edges,
        )
    )
    return store


class FixtureEdgeReader:
    """不依赖 SQLite 的 public query-core mock。"""

    def __init__(self, edges: tuple[LineageEdge, ...]) -> None:
        self.edges = edges

    def read_outgoing_edges(
        self,
        *,
        environment: str,
        source_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        return tuple(
            edge
            for edge in self.edges
            if edge.environment == environment
            and edge.source_table == source_table
            and (source_profile is None or edge.source_profile == source_profile)
        )

    def read_incoming_edges(
        self,
        *,
        environment: str,
        target_table: str,
        source_profile: str | None = None,
    ) -> tuple[LineageEdge, ...]:
        return tuple(
            edge
            for edge in self.edges
            if edge.environment == environment
            and edge.target_table == target_table
            and (source_profile is None or edge.source_profile == source_profile)
        )


class LineageQueryTests(unittest.TestCase):
    def test_chain_upstream_downstream_and_depth_semantics(self):
        with TemporaryDirectory() as directory:
            store = publish_store(directory, SIMPLE_CHAIN_EDGES)
            service = LineageQueryService(store)

            downstream = service.query_downstream("ODS.DEMO_A", "DEV")
            self.assertEqual(
                [(node.table, node.depth) for node in downstream.nodes],
                [
                    ("ODS.DEMO_A", 0),
                    ("DWM.DEMO_B", 1),
                    ("DWA.DEMO_C", 2),
                    ("DM.DEMO_D", 3),
                ],
            )
            self.assertEqual(
                [(edge.source, edge.target) for edge in downstream.edges],
                [
                    ("DWA.DEMO_C", "DM.DEMO_D"),
                    ("DWM.DEMO_B", "DWA.DEMO_C"),
                    ("ODS.DEMO_A", "DWM.DEMO_B"),
                ],
            )
            self.assertFalse(downstream.truncated)

            upstream = query_upstream(
                store,
                "DM.DEMO_D",
                "DEV",
                depth=3,
            )
            self.assertEqual(
                [(node.table, node.depth) for node in upstream.nodes],
                [
                    ("DM.DEMO_D", 0),
                    ("DWA.DEMO_C", 1),
                    ("DWM.DEMO_B", 2),
                    ("ODS.DEMO_A", 3),
                ],
            )
            self.assertFalse(upstream.truncated)

            depth_limited = service.query_downstream("ODS.DEMO_A", "DEV", depth=1)
            self.assertEqual(
                [node.table for node in depth_limited.nodes],
                ["ODS.DEMO_A", "DWM.DEMO_B"],
            )
            self.assertTrue(depth_limited.truncated)

            root_only = service.query_downstream("ODS.DEMO_A", "DEV", depth=0)
            self.assertEqual([node.table for node in root_only.nodes], ["ODS.DEMO_A"])
            self.assertEqual(root_only.edges, ())
            self.assertTrue(root_only.truncated)

    def test_natural_depth_boundary_is_not_truncated(self):
        edges = SIMPLE_CHAIN_EDGES[:2]
        with TemporaryDirectory() as directory:
            result = query_downstream(
                publish_store(directory, edges),
                "ODS.DEMO_A",
                "DEV",
                depth=2,
            )

        self.assertEqual(
            [node.table for node in result.nodes],
            ["ODS.DEMO_A", "DWM.DEMO_B", "DWA.DEMO_C"],
        )
        self.assertFalse(result.truncated)

    def test_branch_dedup_and_blast_radius(self):
        with TemporaryDirectory() as directory:
            service = LineageQueryService(publish_store(directory, BRANCH_EDGES))
            result = service.query_downstream("ODS.DEMO_A", "DEV")
            impact = service.analyze_blast_radius("ODS.DEMO_A", "DEV")

        self.assertEqual(
            {node.table for node in result.nodes},
            {"ODS.DEMO_A", "DWM.DEMO_B", "DWM.DEMO_C", "DWA.DEMO_D"},
        )
        self.assertEqual(
            {(edge.source, edge.target) for edge in result.edges},
            {
                ("ODS.DEMO_A", "DWM.DEMO_B"),
                ("ODS.DEMO_A", "DWM.DEMO_C"),
                ("DWM.DEMO_B", "DWA.DEMO_D"),
                ("DWM.DEMO_C", "DWA.DEMO_D"),
            },
        )
        self.assertEqual(impact, BlastRadiusResult("ODS.DEMO_A", 2, 1, 3, 2))
        self.assertEqual(impact.to_dict()["total_impact"], 3)

        upstream_root_impact = analyze_blast_radius(
            publish_store(directory, BRANCH_EDGES),
            "DWA.DEMO_D",
            "DEV",
        )
        self.assertEqual(upstream_root_impact.total_impact, 0)
        self.assertEqual(upstream_root_impact.max_depth, 0)

    def test_cycle_and_self_reference_are_safe_and_stable(self):
        with TemporaryDirectory() as directory:
            store = publish_store(directory, CYCLE_EDGES)
            service = LineageQueryService(store)
            first = service.query_downstream("DWM.DEMO_A", "DEV")
            second = service.query_downstream("DWM.DEMO_A", "DEV")

            self_reference = LineageQueryService(
                publish_store(directory, SELF_REFERENCE_EDGES, batch_id="batch-self")
            ).query_downstream("DWM.DEMO_SELF", "DEV")

        self.assertEqual(first, second)
        self.assertFalse(first.truncated)
        self.assertEqual(
            {node.table for node in first.nodes},
            {"DWM.DEMO_A", "DWM.DEMO_B", "DWM.DEMO_C"},
        )
        self.assertEqual(len(first.edges), 3)
        self.assertEqual(
            [node.table for node in self_reference.nodes], ["DWM.DEMO_SELF"]
        )
        self.assertEqual(
            [(edge.source, edge.target) for edge in self_reference.edges],
            [("DWM.DEMO_SELF", "DWM.DEMO_SELF")],
        )
        self.assertFalse(self_reference.truncated)

    def test_multiple_programs_same_graph_edge_are_projected_once(self):
        with TemporaryDirectory() as directory:
            store = publish_store(directory, MULTIPLE_PROGRAM_SAME_EDGE)
            self.assertEqual(
                len(
                    store.read_outgoing_edges(
                        environment="DEV",
                        source_table="ODS.DEMO_A",
                    )
                ),
                2,
            )
            result = query_downstream(store, "ODS.DEMO_A", "DEV")

        self.assertEqual(len(result.edges), 1)
        self.assertEqual(len(result.nodes), 2)

    def test_environment_isolation_and_cross_profile_default_scope(self):
        with TemporaryDirectory() as directory:
            environment_result = query_downstream(
                publish_store(directory, ENVIRONMENT_EDGES),
                "ODS.DEMO_A",
                "DEV",
            )
            profile_store = publish_store(
                directory,
                MULTIPLE_SOURCE_PROFILE_EDGES,
                batch_id="batch-profiles",
            )
            cross_profile = query_downstream(profile_store, "ODS.DEMO_A", "DEV")
            filtered = query_downstream(
                profile_store,
                "ODS.DEMO_A",
                "DEV",
                source_profile="mysql_dev_a",
            )

        self.assertEqual(
            {(edge.source, edge.target) for edge in environment_result.edges},
            {("ODS.DEMO_A", "DWM.DEMO_B")},
        )
        self.assertEqual(
            [node.table for node in cross_profile.nodes],
            ["ODS.DEMO_A", "DWM.DEMO_B", "DWA.DEMO_C"],
        )
        self.assertEqual(
            [node.table for node in filtered.nodes],
            ["ODS.DEMO_A", "DWM.DEMO_B"],
        )

    def test_max_nodes_counts_root_and_is_deterministic(self):
        edges = fanout_edges()
        with TemporaryDirectory() as directory:
            first = query_downstream(
                publish_store(directory, edges),
                "ODS.DEMO_ROOT",
                "DEV",
                max_nodes=3,
            )
            second = LineageQueryService(
                FixtureEdgeReader(tuple(reversed(edges)))
            ).query_downstream(
                "ODS.DEMO_ROOT",
                "DEV",
                max_nodes=3,
            )

        self.assertEqual(
            [node.table for node in first.nodes],
            [
                "ODS.DEMO_ROOT",
                "DWM.DEMO_FANOUT_001",
                "DWM.DEMO_FANOUT_002",
            ],
        )
        self.assertEqual(first, second)
        self.assertTrue(first.truncated)

    def test_viewer_json_is_stable_and_has_minimum_contract(self):
        first = LineageQueryService(FixtureEdgeReader(BRANCH_EDGES)).query_downstream(
            "ODS.DEMO_A", "DEV"
        )
        second = LineageQueryService(
            FixtureEdgeReader(tuple(reversed(BRANCH_EDGES)))
        ).query(
            "ODS.DEMO_A",
            "DEV",
            LineageDirection.DOWNSTREAM,
        )

        self.assertEqual(first.to_json(), second.to_json())
        payload = json.loads(first.to_json())
        self.assertEqual(set(payload), {"nodes", "edges", "truncated"})
        self.assertNotIn("LineageNode", first.to_json())
        self.assertIsInstance(payload["truncated"], bool)

    def test_empty_active_batch_unknown_table_and_directional_leaf(self):
        with TemporaryDirectory() as directory:
            empty_store = publish_store(directory, (), batch_id="batch-empty")
            service = LineageQueryService(empty_store)
            self.assertEqual(
                service.query_downstream("ODS.DEMO_UNKNOWN", "DEV").to_viewer_dict(),
                {"nodes": [], "edges": [], "truncated": False},
            )

            populated = publish_store(
                directory,
                (demo_edge("ODS.DEMO_A", "DWM.DEMO_B"),),
                batch_id="batch-populated",
            )
            no_upstream = query_upstream(populated, "ODS.DEMO_A", "DEV")
            no_downstream = query_downstream(populated, "DWM.DEMO_B", "DEV")
            unknown = query_downstream(populated, "DWA.DEMO_UNKNOWN", "DEV")

        self.assertEqual([node.table for node in no_upstream.nodes], ["ODS.DEMO_A"])
        self.assertEqual([node.table for node in no_downstream.nodes], ["DWM.DEMO_B"])
        self.assertEqual(no_upstream.edges, ())
        self.assertEqual(no_downstream.edges, ())
        self.assertEqual(unknown.nodes, ())

    def test_active_only_narrow_adapter_ignores_previous_batch(self):
        with TemporaryDirectory() as directory:
            store = publish_store(
                directory,
                (demo_edge("ODS.DEMO_A", "DWM.DEMO_OLD"),),
                batch_id="batch-old",
            )
            store.publish(
                MaterializationBatch(
                    batch_id="batch-new",
                    observed_at=OBSERVED_AT,
                    edges=(demo_edge("ODS.DEMO_A", "DWM.DEMO_NEW"),),
                )
            )
            result = query_downstream(store, "ODS.DEMO_A", "DEV")

        self.assertEqual(
            [node.table for node in result.nodes],
            ["ODS.DEMO_A", "DWM.DEMO_NEW"],
        )
        self.assertNotIn("DWM.DEMO_OLD", {node.table for node in result.nodes})

    def test_invalid_limits_and_direction_fail_explicitly(self):
        reader = FixtureEdgeReader(SIMPLE_CHAIN_EDGES)
        service = LineageQueryService(reader)
        with self.assertRaises(ValueError):
            service.query_downstream("ODS.DEMO_A", "DEV", depth=-1)
        with self.assertRaises(ValueError):
            service.query_downstream("ODS.DEMO_A", "DEV", max_nodes=0)
        with self.assertRaises(ValueError):
            service.query("ODS.DEMO_A", "DEV", "sideways")
        with self.assertRaises(ValueError):
            service.query_downstream("ODS.DEMO_A", "", depth=1)


if __name__ == "__main__":
    unittest.main()
