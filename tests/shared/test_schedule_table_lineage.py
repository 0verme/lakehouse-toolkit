from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest

from shared.lineage.schedule_table_lineage import (
    DOWNSTREAM,
    UPSTREAM,
    build_downstream_job_index,
    build_job_index,
    build_table_job_map,
    find_table_candidates,
    is_dwf_table,
    trace_downstream_tables,
    trace_table_lineage,
    trace_upstream_tables,
)


class ScheduleTableLineageTest(unittest.TestCase):
    def setUp(self):
        self.job_index = build_job_index(
            [
                ("PLAN_ROOT", "JOB_ROOT", "33:JOB_MID|20:EVENT_X"),
                ("PLAN_MID", "JOB_MID", "33:JOB_DWF"),
                ("PLAN_DWF", "JOB_DWF", "33:JOB_BEFORE_DWF"),
                ("PLAN_OLD", "JOB_BEFORE_DWF", ""),
            ],
            [
                ("DWM.T_ROOT", "JOB_ROOT"),
                ("DWD.T_MID", "JOB_MID"),
                ("DWF.T_SOURCE", "JOB_DWF"),
                ("DWO.T_TOO_FAR", "JOB_BEFORE_DWF"),
            ],
        )
        self.table_job_map = build_table_job_map(self.job_index)

    def test_table_job_mapping_uses_program_table_name(self):
        self.assertEqual(self.table_job_map["DWM.T_ROOT"], ["JOB_ROOT"])
        self.assertEqual(self.job_index["JOB_MID"].table_names, {"DWD.T_MID"})

    def test_upstream_stops_at_dwf_and_edges_follow_data_flow(self):
        trace = trace_upstream_tables(
            "dwm.t_root", self.job_index, self.table_job_map, max_depth=10
        )
        edge_pairs = {(edge.source_table, edge.target_table) for edge in trace.edges}

        self.assertEqual(trace.nodes, {"DWM.T_ROOT", "DWD.T_MID", "DWF.T_SOURCE"})
        self.assertEqual(trace.dwf_tables, {"DWF.T_SOURCE"})
        self.assertEqual(
            edge_pairs,
            {("DWD.T_MID", "DWM.T_ROOT"), ("DWF.T_SOURCE", "DWD.T_MID")},
        )
        self.assertNotIn("DWO.T_TOO_FAR", trace.nodes)

    def test_unmapped_upstream_job_is_transparently_skipped(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_ROOT", "33:JOB_NO_TABLE"),
                ("P2", "JOB_NO_TABLE", "33:JOB_DWF"),
                ("P3", "JOB_DWF", ""),
            ],
            [("DWM.T_ROOT", "JOB_ROOT"), ("DWS_DWF.T_SOURCE", "JOB_DWF")],
        )
        trace = trace_upstream_tables("DWM.T_ROOT", job_index, max_depth=10)

        self.assertEqual(trace.dwf_tables, {"DWS_DWF.T_SOURCE"})
        self.assertEqual(trace.unmapped_jobs, {"JOB_NO_TABLE"})
        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in trace.edges},
            {("DWS_DWF.T_SOURCE", "DWM.T_ROOT")},
        )

    def test_downstream_returns_all_tables_and_marks_natural_terminal(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_DWF", ""),
                ("P2", "JOB_DWD_A", "33:JOB_DWF"),
                ("P3", "JOB_NO_TABLE", "33:JOB_DWD_A"),
                ("P4", "JOB_DWM", "33:JOB_NO_TABLE"),
                ("P5", "JOB_DM", "33:JOB_DWM"),
                ("P6", "JOB_DWD_B", "33:JOB_DWF"),
            ],
            [
                ("DWF.T_SOURCE", "JOB_DWF"),
                ("DWD.T_A", "JOB_DWD_A"),
                ("DWM.T_RESULT", "JOB_DWM"),
                ("DM.T_END", "JOB_DM"),
                ("DWD.T_B", "JOB_DWD_B"),
            ],
        )

        trace = trace_downstream_tables("dwf.t_source", job_index, max_depth=10)

        self.assertEqual(
            trace.nodes,
            {"DWF.T_SOURCE", "DWD.T_A", "DWM.T_RESULT", "DM.T_END", "DWD.T_B"},
        )
        self.assertEqual(trace.terminal_tables, {"DM.T_END", "DWD.T_B"})
        self.assertEqual(trace.unmapped_jobs, {"JOB_NO_TABLE"})
        self.assertIn(
            ("DWD.T_A", "DWM.T_RESULT"),
            {(edge.source_table, edge.target_table) for edge in trace.edges},
        )
        self.assertNotIn("DWF.T_SOURCE", trace.dwf_tables)

    def test_downstream_job_index_reverses_only_33_dependencies(self):
        index = build_downstream_job_index(self.job_index)
        self.assertEqual(index["JOB_MID"], ["JOB_ROOT"])
        self.assertNotIn("EVENT_X", index)

    def test_downstream_cycle_is_detected_and_branch_stops(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_A", "33:JOB_C"),
                ("P2", "JOB_B", "33:JOB_A"),
                ("P3", "JOB_C", "33:JOB_B"),
            ],
            [
                ("DWF.T_A", "JOB_A"),
                ("DWD.T_B", "JOB_B"),
                ("DWM.T_C", "JOB_C"),
            ],
        )

        trace = trace_downstream_tables("DWF.T_A", job_index, max_depth=10)

        self.assertEqual(len(trace.cycles), 1)
        self.assertEqual(trace.nodes, {"DWF.T_A", "DWD.T_B", "DWM.T_C"})
        self.assertFalse(trace.truncated)

    def test_max_depth_truncates_by_job_hop(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_A", ""),
                ("P2", "JOB_B", "33:JOB_A"),
                ("P3", "JOB_C", "33:JOB_B"),
            ],
            [("DWF.T_A", "JOB_A"), ("DWD.T_B", "JOB_B"), ("DWM.T_C", "JOB_C")],
        )

        trace = trace_downstream_tables("DWF.T_A", job_index, max_depth=1)

        self.assertEqual(trace.nodes, {"DWF.T_A", "DWD.T_B"})
        self.assertTrue(trace.truncated)
        self.assertEqual(trace.terminal_tables, set())

    def test_duplicate_relations_are_deduplicated(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_A", ""),
                ("P2", "JOB_B1", "33:JOB_A"),
                ("P3", "JOB_B2", "33:JOB_A"),
            ],
            [
                ("DWF.T_A", "JOB_A"),
                ("DWD.T_B", "JOB_B1"),
                ("DWD.T_B", "JOB_B2"),
            ],
        )

        trace = trace_downstream_tables("DWF.T_A", job_index, max_depth=10)

        self.assertEqual(len(trace.edges), 1)
        self.assertEqual(
            (trace.edges[0].source_table, trace.edges[0].target_table),
            ("DWF.T_A", "DWD.T_B"),
        )

    def test_many_jobs_per_table_and_many_tables_per_job(self):
        job_index = build_job_index(
            [
                ("P1", "JOB_ROOT_1", ""),
                ("P2", "JOB_ROOT_2", ""),
                ("P3", "JOB_NEXT_1", "33:JOB_ROOT_1"),
                ("P4", "JOB_NEXT_2", "33:JOB_ROOT_2"),
            ],
            [
                ("DWF.T_ROOT", "JOB_ROOT_1"),
                ("DWF.T_ROOT", "JOB_ROOT_2"),
                ("DWD.T_A", "JOB_NEXT_1"),
                ("DWD.T_B", "JOB_NEXT_1"),
                ("DWD.T_C", "JOB_NEXT_2"),
            ],
        )

        trace = trace_downstream_tables("DWF.T_ROOT", job_index, max_depth=10)

        self.assertEqual(
            build_table_job_map(job_index)["DWF.T_ROOT"], ["JOB_ROOT_1", "JOB_ROOT_2"]
        )
        self.assertEqual(trace.nodes, {"DWF.T_ROOT", "DWD.T_A", "DWD.T_B", "DWD.T_C"})
        self.assertEqual(trace.terminal_tables, {"DWD.T_A", "DWD.T_B", "DWD.T_C"})

    def test_find_candidates_normalizes_case_and_supports_fuzzy_match(self):
        self.assertEqual(
            find_table_candidates("`dwm.t_root`", self.table_job_map), ["DWM.T_ROOT"]
        )
        self.assertEqual(
            find_table_candidates("t_", self.table_job_map),
            [
                "DWD.T_MID",
                "DWF.T_SOURCE",
                "DWM.T_ROOT",
                "DWO.T_TOO_FAR",
            ],
        )
        self.assertTrue(is_dwf_table("DWF.T_SOURCE"))
        self.assertTrue(is_dwf_table("DWS_DWF.T_SOURCE"))
        self.assertFalse(is_dwf_table("DWD.T_MID"))

    def test_graph_edges_and_column_layout_follow_direction(self):
        upstream_graph = trace_table_lineage(
            "DWM.T_ROOT", self.job_index, max_depth=10, direction=UPSTREAM
        ).to_graph_dict()
        downstream_graph = trace_table_lineage(
            "DWF.T_SOURCE", self.job_index, max_depth=10, direction=DOWNSTREAM
        ).to_graph_dict()
        upstream_cols = {node["label"]: node["col"] for node in upstream_graph["nodes"]}
        downstream_cols = {
            node["label"]: node["col"] for node in downstream_graph["nodes"]
        }

        self.assertEqual(upstream_cols["DWF.T_SOURCE"], 0)
        self.assertEqual(upstream_cols["DWM.T_ROOT"], 2)
        self.assertIn(
            ("table:DWF.T_SOURCE", "table:DWD.T_MID"), upstream_graph["edges"]
        )
        self.assertEqual(downstream_cols["DWF.T_SOURCE"], 0)
        self.assertEqual(downstream_cols["DWM.T_ROOT"], 2)
        self.assertIn(
            ("table:DWF.T_SOURCE", "table:DWD.T_MID"), downstream_graph["edges"]
        )

    def test_offline_graph_html_has_no_template_placeholders_or_job_nodes(self):
        script = textwrap.dedent(
            """
            from shared.lineage.schedule_table_lineage import build_job_index, trace_downstream_tables
            from tools.search.table_lineage_roamer import build_graph_html
            from tools.search import table_upstream_to_dwf as table_tool

            jobs = build_job_index(
                [('P1', 'JOB_A', ''), ('P2', 'JOB_B', '33:JOB_A')],
                [('DWF.T_A', 'JOB_A'), ('DWD.T_B', 'JOB_B')],
            )
            graph = trace_downstream_tables('DWF.T_A', jobs, max_depth=10).to_graph_dict()
            html = build_graph_html(graph, 7)
            for placeholder in ('__GRAPH__', '__ROOT_ID__', '__MARKER_ID__', '__TYPE_LABEL__'):
                assert placeholder not in html, placeholder
            assert 'JOB_A' not in html and 'JOB_B' not in html
            assert 'table-arrow-7' in html
            assert '\\u4e0b\\u6e38\\u68c0\\u67e5' in html
            assert '\\u672b\\u7aef\\u8868' in html
            table_tool.put_black_text = lambda *args, **kwargs: None
            table_tool.put_red_text = lambda *args, **kwargs: None
            table_tool.put_table = lambda *args, **kwargs: None
            section = table_tool.analyze_one(
                'DWF.T_A',
                jobs,
                table_tool.build_table_job_map(jobs),
                10,
                table_tool.DOWNSTREAM,
            )
            rendered = repr(section)
            assert section['rows'][0] == ['\\u68c0\\u67e5\\u65b9\\u5411', '\\u4e0b\\u6e38\\u68c0\\u67e5']
            assert '\\u4e0b\\u6e38\\u8868' in rendered and '\\u662f\\u5426\\u672b\\u7aef\\u8868' in rendered
            assert 'JOB_A' not in rendered and 'JOB_B' not in rendered
            print('offline-page-ok')
            """
        )
        # The command and script are fixed test inputs; shell execution is disabled.
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-B", "-c", script],
            cwd=".",
            capture_output=True,
            timeout=20,
            check=False,
        )
        stdout = result.stdout.decode("utf-8", errors="replace")
        stderr = result.stderr.decode("utf-8", errors="replace")

        self.assertEqual(result.returncode, 0, stdout + stderr)
        self.assertIn("offline-page-ok", stdout)


if __name__ == "__main__":
    unittest.main()
