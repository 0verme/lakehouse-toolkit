from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from shared.lineage.domain import LineageEdge
from shared.lineage.environment_scope import (
    LineageEnvironmentScope,
    LineageEnvironmentScopeResolver,
)
from shared.lineage.query import LineageDirection, LineageView
from tools.lineage.lineage_explorer_web import (
    LineageExplorerConnectionError,
    LineageExplorerDomainError,
    build_explorer_request,
    build_graph_html,
    build_graph_payload,
    build_summary,
    execute_explorer_query,
    map_explorer_error,
)


class _FixtureReader:
    def __init__(self, edges: tuple[LineageEdge, ...]):
        self.edges = edges

    def read_outgoing_edges(self, *, environment, source_table, source_profile=None):
        return tuple(
            edge
            for edge in self.edges
            if edge.environment == environment
            and edge.source_table == source_table
            and (source_profile is None or edge.source_profile == source_profile)
        )

    def read_incoming_edges(self, *, environment, target_table, source_profile=None):
        return tuple(
            edge
            for edge in self.edges
            if edge.environment == environment
            and edge.target_table == target_table
            and (source_profile is None or edge.source_profile == source_profile)
        )

    def contains_node(self, *, environment, table, source_profile=None):
        return any(
            edge.environment == environment
            and (source_profile is None or edge.source_profile == source_profile)
            and table in {edge.source_table, edge.target_table}
            for edge in self.edges
        )


class LineageExplorerWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.resolver = LineageEnvironmentScopeResolver(
            (
                LineageEnvironmentScope(
                    name="dev214",
                    environment="DEV214",
                    sql_source_profile="mysql_dev214",
                    schedule_source_profile="mysql_dev214",
                    label="DEV214",
                    dws_profile="dws_dev214",
                ),
                LineageEnvironmentScope(
                    name="disabled",
                    environment="DEV_DISABLED",
                    sql_source_profile="mysql_disabled",
                    schedule_source_profile="mysql_disabled",
                    label="disabled",
                    dws_profile="dws_disabled",
                    enabled=False,
                ),
            )
        )

    def test_request_exposes_only_environment_and_enforces_bounded_limits(self):
        scope, request = build_explorer_request(
            self.resolver,
            environment="DEV214",
            root=" dwp.tmp_formal_result ",
            direction="both",
            view="physical",
            depth=2,
            max_nodes=100,
        )

        self.assertEqual(scope.dws_profile, "dws_dev214")
        self.assertEqual(request.environment, "DEV214")
        self.assertEqual(request.root, "DWP.TMP_FORMAL_RESULT")
        self.assertEqual(request.direction, LineageDirection.BOTH)
        self.assertEqual(request.view, LineageView.PHYSICAL)
        self.assertNotIn("profile", request.__annotations__)

        with self.assertRaises(ValueError):
            build_explorer_request(
                self.resolver,
                environment="DEV214",
                root="not-qualified",
                direction="downstream",
                view="business",
            )
        with self.assertRaises(ValueError):
            build_explorer_request(
                self.resolver,
                environment="DEV214",
                root="DWF.A",
                direction="downstream",
                view="business",
                max_nodes=5001,
            )

    def test_execute_query_is_environment_scoped_and_preserves_tmp_named_asset(self):
        edges = (
            LineageEdge(
                environment="DEV214",
                source_profile="mysql_dev214",
                source_table="DWF.A",
                target_table="DWP.TMP_FORMAL_RESULT",
            ),
            LineageEdge(
                environment="DEV215",
                source_profile="mysql_dev214",
                source_table="DWF.A",
                target_table="DWM.OTHER_ENV",
            ),
        )
        scope, request = build_explorer_request(
            self.resolver,
            environment="DEV214",
            root="DWF.A",
            direction="downstream",
            view="business",
            depth=1,
            max_nodes=100,
        )
        result = execute_explorer_query(
            scope,
            request,
            connection=object(),
            reader_factory=lambda **_: _FixtureReader(edges),
        )

        self.assertEqual(
            [node.table for node in result.nodes],
            ["DWF.A", "DWP.TMP_FORMAL_RESULT"],
        )
        self.assertEqual(result.environment, "DEV214")
        self.assertFalse(result.truncated)
        graph = build_graph_payload(result)
        self.assertEqual(graph["root"], "DWF.A")
        self.assertIn("DWP.TMP_FORMAL_RESULT", str(graph))
        self.assertIn("data-action=\"fit\"", build_graph_html(graph))
        self.assertIn("DWP.TMP_FORMAL_RESULT", build_graph_html(graph))
        self.assertIn("nodes = `2`", build_summary(result))

    def test_unknown_root_maps_to_distinct_error(self):
        scope, request = build_explorer_request(
            self.resolver,
            environment="DEV214",
            root="DWF.UNKNOWN",
            direction="downstream",
            view="business",
        )
        with self.assertRaises(LineageExplorerDomainError) as raised:
            execute_explorer_query(
                scope,
                request,
                connection=object(),
                reader_factory=lambda **_: _FixtureReader(()),
            )

        failure = map_explorer_error(raised.exception)
        self.assertEqual(failure.code, "LINEAGE_ROOT_NOT_FOUND")
        self.assertIn("不存在", failure.message)

    def test_connection_failure_maps_without_profile_fallback(self):
        scope, request = build_explorer_request(
            self.resolver,
            environment="DEV214",
            root="DWF.A",
            direction="downstream",
            view="business",
        )

        def fail_connection(_profile):
            raise OSError("connection unavailable")

        with self.assertRaises(LineageExplorerConnectionError) as raised:
            execute_explorer_query(
                scope,
                request,
                connection_factory=fail_connection,
            )
        failure = map_explorer_error(raised.exception)
        self.assertEqual(failure.code, "DWS_CONNECTION_FAILED")
        self.assertNotIn("connection unavailable", failure.message)

    def test_disabled_environment_maps_without_fallback(self):
        with self.assertRaises(ValueError) as raised:
            build_explorer_request(
                self.resolver,
                environment="DEV_DISABLED",
                root="DWF.A",
                direction="downstream",
                view="business",
            )
        failure = map_explorer_error(raised.exception)
        self.assertEqual(failure.code, "DISABLED_LINEAGE_ENVIRONMENT")

    def test_tools_registry_contains_formal_explorer_entry(self):
        root = Path(__file__).resolve().parents[2]
        config = yaml.safe_load((root / "configs" / "tools.yaml").read_text(encoding="utf-8"))
        matches = [item for item in config["tools"] if item["name"] == "lineage_explorer"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["title"], "血缘探索")
        self.assertEqual(matches[0]["workdir"], "tools/lineage")
        self.assertEqual(matches[0]["script"], "lineage_explorer_web.py")


if __name__ == "__main__":
    unittest.main()
