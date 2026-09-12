from __future__ import annotations

import sqlite3
import unittest

from shared.lineage.dws_query import (
    DWSActiveSnapshotNotFoundError,
    DWSLineageEdgeReader,
)
from shared.lineage.query import (
    LineageDirection,
    LineageQueryService,
    LineageQueryTiming,
    LineageView,
)


class DWSLineageReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.queries: list[str] = []
        self.connection.set_trace_callback(self.queries.append)
        self.connection.execute("ATTACH DATABASE ':memory:' AS dwp")
        self.connection.executescript(
            """
            CREATE TABLE dwp.lineage_batch (
                batch_id TEXT, publish_status TEXT, is_active INTEGER
            );
            CREATE TABLE dwp.lineage_edge (
                environment TEXT, source_profile TEXT, source_table TEXT,
                target_table TEXT, batch_id TEXT, is_active INTEGER
            );
            CREATE TABLE dwp.lineage_business_edge (
                environment TEXT, source_profile TEXT, source_table TEXT,
                target_table TEXT, batch_id TEXT, is_active INTEGER
            );
            """
        )
        self.connection.executemany(
            "INSERT INTO dwp.lineage_batch VALUES (?, ?, ?)",
            (
                ("batch-old", "PUBLISHED", 0),
                ("batch-active", "PUBLISHED", 1),
            ),
        )
        self.connection.executemany(
            "INSERT INTO dwp.lineage_edge VALUES (?, ?, ?, ?, ?, ?)",
            (
                ("DEV214", "mysql_dev", "DWF.A", "DWM.OLD", "batch-old", 1),
                ("DEV214", "mysql_dev", "DWF.A", "DWM.B", "batch-active", 1),
                (
                    "DEV214",
                    "mysql_dev",
                    "DWM.B",
                    "DWP.TMP_FORMAL_RESULT",
                    "batch-active",
                    1,
                ),
                ("DEV215", "mysql_dev", "DWF.A", "DWM.OTHER_ENV", "batch-active", 1),
            ),
        )
        self.connection.executemany(
            "INSERT INTO dwp.lineage_business_edge VALUES (?, ?, ?, ?, ?, ?)",
            (
                ("DEV214", "mysql_dev", "DWF.A", "DWM.B", "batch-active", 1),
                ("DEV214", "mysql_dev", "DWM.B", "DWM.BUSINESS_RESULT", "batch-active", 1),
                ("DEV214", "mysql_dev", "DWF.A", "DWM.OLD", "batch-old", 1),
            ),
        )
        self.connection.commit()

    def tearDown(self) -> None:
        self.connection.close()

    def test_active_business_and_physical_views_push_scope_and_keep_tmp_named_formal_asset(
        self,
    ) -> None:
        reader = DWSLineageEdgeReader(connection=self.connection)
        timing = LineageQueryTiming()
        service = LineageQueryService(reader)

        physical = service.query_downstream(
            "DWF.A",
            "DEV214",
            source_profile="mysql_dev",
            depth=2,
            view=LineageView.PHYSICAL,
            timing=timing,
        )
        business = service.query_downstream(
            "DWF.A",
            "DEV214",
            source_profile="mysql_dev",
            depth=2,
            view=LineageView.BUSINESS,
        )

        self.assertEqual(
            [node.table for node in physical.nodes],
            ["DWF.A", "DWM.B", "DWP.TMP_FORMAL_RESULT"],
        )
        self.assertEqual(
            [node.table for node in business.nodes],
            ["DWF.A", "DWM.B", "DWM.BUSINESS_RESULT"],
        )
        self.assertNotIn("DWM.OLD", {node.table for node in physical.nodes})
        self.assertNotIn("DWM.OTHER_ENV", {node.table for node in physical.nodes})
        self.assertEqual(physical.batch_id, "batch-active")
        self.assertGreaterEqual(timing.active_batch_resolve_ms, 0)
        self.assertGreaterEqual(timing.edge_rows, 2)
        self.assertIn("dwp.lineage_edge", " ".join(self.queries))
        self.assertIn(
            "dwp.lineage_business_edge",
            " ".join(self.queries),
        )
        self.assertTrue(
            all(
                "e.environment =" in query
                for query in self.queries
                if "SELECT e.environment" in query
            )
        )

    def test_query_service_both_reuses_one_request_snapshot_and_merges_direction(self) -> None:
        reader = DWSLineageEdgeReader(connection=self.connection)
        timing = LineageQueryTiming()
        result = LineageQueryService(reader).query(
            "DWM.B",
            "DEV214",
            LineageDirection.BOTH,
            source_profile="mysql_dev",
            depth=1,
            max_nodes=10,
            view=LineageView.PHYSICAL,
            timing=timing,
        )

        self.assertEqual(
            [node.table for node in result.nodes],
            ["DWM.B", "DWF.A", "DWP.TMP_FORMAL_RESULT"],
        )
        self.assertEqual(
            {(edge.source, edge.target) for edge in result.edges},
            {
                ("DWF.A", "DWM.B"),
                ("DWM.B", "DWP.TMP_FORMAL_RESULT"),
            },
        )
        self.assertEqual(result.nodes.count(result.nodes[0]), 1)
        self.assertFalse(result.truncated)
        active_batch_queries = [
            query for query in self.queries if "FROM dwp.lineage_batch" in query
        ]
        self.assertEqual(len(active_batch_queries), 1)

    def test_unknown_root_is_distinct_from_directional_empty_result(self) -> None:
        reader = DWSLineageEdgeReader(connection=self.connection)
        service = LineageQueryService(reader)

        unknown = service.query_downstream(
            "DWF.UNKNOWN",
            "DEV214",
            source_profile="mysql_dev",
        )
        leaf = service.query_upstream(
            "DWF.A",
            "DEV214",
            source_profile="mysql_dev",
        )

        self.assertFalse(unknown.root_found)
        self.assertEqual(unknown.nodes, ())
        self.assertTrue(leaf.root_found)
        self.assertEqual([node.table for node in leaf.nodes], ["DWF.A"])

    def test_missing_active_snapshot_fails_closed(self) -> None:
        self.connection.execute("UPDATE dwp.lineage_batch SET is_active = 0")
        self.connection.commit()
        reader = DWSLineageEdgeReader(connection=self.connection)

        with self.assertRaises(DWSActiveSnapshotNotFoundError):
            reader.read_outgoing_edges(
                environment="DEV214",
                source_table="DWF.A",
                source_profile="mysql_dev",
            )


if __name__ == "__main__":
    unittest.main()
