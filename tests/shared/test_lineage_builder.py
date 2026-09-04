import unittest

from shared.lineage.lineage_builder import (
    ProcessInfo,
    ScheduleTimeInfo,
    apply_schedule_times_to_graph,
    build_lineage_graph,
    build_lineage_graph_with_targeted_schedule_times,
    build_schedule_time_sql_for_tables,
    collect_lineage_table_names,
    is_known_result_table,
    is_self_lineage,
    is_terminal_upstream_table,
    merge_schedule_time_row,
    normalize_table_name,
    parse_schedule_time_seconds,
)


def process(target: str, script_code: str) -> ProcessInfo:
    return ProcessInfo(
        source_table="process_registry",
        process_name=f"DEMO_PROJECT: {target}",
        script_code=script_code,
    )


def schedule(
    table_name: str, schedule_time: str, job_name: str | None = None
) -> ScheduleTimeInfo:
    normalized_table = normalize_table_name(table_name)
    return ScheduleTimeInfo(
        table_name=normalized_table,
        job_name=job_name or normalized_table.split(".", 1)[-1],
        schedule_time=schedule_time,
        sort_time_seconds=parse_schedule_time_seconds(schedule_time),
    )


def graph_data(
    input_name: str,
    process_infos: list[ProcessInfo],
    result_tables: set[str],
    schedule_time_map: dict[str, ScheduleTimeInfo] | None = None,
):
    graph, candidates = build_lineage_graph(
        input_name,
        process_infos,
        max_depth=8,
        result_table_names=result_tables,
        schedule_time_map=schedule_time_map,
    )
    if candidates:
        raise AssertionError(f"unexpected lineage candidates: {candidates}")
    if graph is None:
        raise AssertionError("lineage graph was not built")
    return graph.to_dict()


def labels(data: dict) -> set[str]:
    return {item["label"] for item in data["nodes"]}


def edge_set(data: dict) -> set[tuple[str, str]]:
    return {tuple(item) for item in data["edges"]}


def node_labels_in_col(data: dict, col: int, node_type: str | None = None) -> list[str]:
    rows = [item for item in data["nodes"] if item["col"] == col]
    if node_type is not None:
        rows = [item for item in rows if item["type"] == node_type]
    return [item["label"] for item in rows]


def reach(start: str, edges: set[tuple[str, str]]) -> set[str]:
    adjacent: dict[str, list[str]] = {}
    for from_id, to_id in edges:
        adjacent.setdefault(from_id, []).append(to_id)
    seen: set[str] = set()
    stack = list(adjacent.get(start, []))
    while stack:
        node_id = stack.pop()
        if node_id in seen:
            continue
        seen.add(node_id)
        stack.extend(adjacent.get(node_id, []))
    return seen


class dataLineageTests(unittest.TestCase):
    def test_build_lineage_graph_uses_script_code_edges(self):
        process_infos = [
            process(
                "DWS_DWM.T_ROOT",
                "insert into dws_dwm.t_root select * from dws_dwd.t_mid join dws_dwa.t_leaf on 1=1",
            ),
            process("DWS_DWD.T_MID", "select * from dws_dwf.t_src"),
        ]

        data = graph_data(
            "DWS_DWM.T_ROOT",
            process_infos,
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_MID", "DWS_DWA.T_LEAF", "DWS_DWF.T_SRC"},
        )
        nodes = {item["label"]: item for item in data["nodes"]}
        edges = edge_set(data)

        self.assertEqual(nodes["DWS_DWM.T_ROOT"]["type"], "root_table")
        self.assertEqual(nodes["DWS_DWD.T_MID"]["type"], "table")
        self.assertEqual(nodes["DWS_DWA.T_LEAF"]["type"], "source_table")
        self.assertEqual(nodes["DWS_DWF.T_SRC"]["type"], "source_table")
        self.assertIn(
            (
                "table:DWS_DWD.T_MID",
                "process:process_registry:DEMO_PROJECT: DWS_DWM.T_ROOT",
            ),
            edges,
        )
        self.assertIn(
            (
                "process:process_registry:DEMO_PROJECT: DWS_DWM.T_ROOT",
                "table:DWS_DWM.T_ROOT",
            ),
            edges,
        )
        self.assertLess(nodes["DWS_DWD.T_MID"]["col"], nodes["DWS_DWM.T_ROOT"]["col"])

    def test_non_result_tables_are_filtered_from_nodes_and_edges(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_mid join dws_dwd.work_stage on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_MID"},
        )

        self.assertIn("DWS_DWD.T_MID", labels(data))
        self.assertNotIn("DWS_DWD.WORK_STAGE", labels(data))
        self.assertFalse(
            any("WORK_STAGE" in part for edge in data["edges"] for part in edge)
        )
        self.assertIn("已过滤非结果表节点 1 个", " ".join(data["warnings"]))

    def test_non_tmp_code_or_parameter_table_is_filtered_by_whitelist(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.code_status_map join dws_dwd.para_value_dict on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT"},
        )

        self.assertNotIn("DWS_DWD.CODE_STATUS_MAP", labels(data))
        self.assertNotIn("DWS_DWD.PARA_VALUE_DICT", labels(data))
        self.assertEqual(len(data["nodes"]), 1)
        self.assertIn("已过滤非结果表节点 2 个", " ".join(data["warnings"]))

    def test_tmp_named_table_is_kept_when_registered_as_result_table(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [process("DWS_DWM.T_ROOT", "select * from dws_dwd.order_tmp_result")],
            {"DWS_DWM.T_ROOT", "DWS_DWD.ORDER_TMP_RESULT"},
        )

        self.assertIn("DWS_DWD.ORDER_TMP_RESULT", labels(data))
        self.assertFalse(any("非结果表" in item for item in data["warnings"]))
        self.assertTrue(
            is_known_result_table("dws_dwd.order_tmp_result", {"ORDER_TMP_RESULT"})
        )

    def test_self_lineage_is_filtered_before_cycle_and_counts(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [process("DWS_DWM.T_ROOT", "select * from dws_dwm.t_root")],
            {"DWS_DWM.T_ROOT"},
        )

        self.assertEqual(labels(data), {"DWS_DWM.T_ROOT"})
        self.assertEqual(data["edges"], [])
        self.assertEqual(data["cycles"], [])
        self.assertIn("已过滤自调用链路 1 条", " ".join(data["warnings"]))
        self.assertTrue(is_self_lineage("DWS_DWM.T_ROOT", "T_ROOT"))

    def test_dwf_table_is_terminal_and_dwo_is_not_expanded(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process("DWS_DWM.T_ROOT", "select * from dws_dwf.t_dwf"),
                process("DWS_DWF.T_DWF", "select * from dws_dwo.t_dwo"),
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWF.T_DWF", "DWS_DWO.T_DWO"},
        )

        self.assertIn("DWS_DWF.T_DWF", labels(data))
        self.assertNotIn("DWS_DWO.T_DWO", labels(data))
        self.assertIn("上游追溯已在 DWF 层截止", " ".join(data["warnings"]))
        self.assertTrue(is_terminal_upstream_table("DWS_DWF.T_DWF"))

    def test_dwo_direct_input_is_allowed(self):
        data = graph_data(
            "DWS_DWO.T_DWO",
            [process("DWS_DWO.T_DWO", "select * from dws_dwa.t_base")],
            {"DWS_DWO.T_DWO", "DWS_DWA.T_BASE"},
        )

        self.assertIn("DWS_DWO.T_DWO", labels(data))
        self.assertIn("DWS_DWA.T_BASE", labels(data))
        self.assertIn(
            (
                "table:DWS_DWA.T_BASE",
                "process:process_registry:DEMO_PROJECT: DWS_DWO.T_DWO",
            ),
            edge_set(data),
        )

    def test_statistics_are_based_on_filtered_graph(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_mid join dws_dwd.code_status_map on 1=1 join dws_dwm.t_root on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_MID"},
        )
        edges = edge_set(data)
        root_id = "table:DWS_DWM.T_ROOT"
        mid_id = "table:DWS_DWD.T_MID"

        self.assertEqual(len(data["nodes"]), 3)
        self.assertEqual(len(data["edges"]), 2)
        self.assertEqual(
            reach(mid_id, edges),
            {"process:process_registry:DEMO_PROJECT: DWS_DWM.T_ROOT", root_id},
        )
        self.assertEqual(reach(root_id, edges), set())
        self.assertEqual(data["cycles"], [])
        self.assertEqual(
            sum("已过滤非结果表节点" in item for item in data["warnings"]), 1
        )
        self.assertEqual(
            sum("已过滤自调用链路" in item for item in data["warnings"]), 1
        )

    def test_same_col_process_nodes_are_sorted_by_schedule_time(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_late join dws_dwd.t_early on 1=1",
                ),
                process("DWS_DWD.T_LATE", "select * from dws_dwf.t_late_src"),
                process("DWS_DWD.T_EARLY", "select * from dws_dwf.t_early_src"),
            ],
            {
                "DWS_DWM.T_ROOT",
                "DWS_DWD.T_LATE",
                "DWS_DWD.T_EARLY",
                "DWS_DWF.T_LATE_SRC",
                "DWS_DWF.T_EARLY_SRC",
            },
            schedule_time_map={
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWD.T_LATE": schedule("DWS_DWD.T_LATE", "2026-07-10 09:30:00"),
                "DWS_DWD.T_EARLY": schedule("DWS_DWD.T_EARLY", "2026-07-10 08:15:00"),
            },
        )

        process_col = next(
            item["col"] for item in data["nodes"] if item["label"] == "T_EARLY"
        )
        self.assertEqual(
            node_labels_in_col(data, process_col, "process"), ["T_EARLY", "T_LATE"]
        )

    def test_same_col_table_nodes_are_sorted_by_producer_schedule_time(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_late join dws_dwd.t_early on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_LATE", "DWS_DWD.T_EARLY"},
            schedule_time_map={
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWD.T_LATE": schedule("DWS_DWD.T_LATE", "2026-07-10 09:30:00"),
                "DWS_DWD.T_EARLY": schedule("DWS_DWD.T_EARLY", "2026-07-10 08:15:00"),
            },
        )

        table_col = next(
            item["col"] for item in data["nodes"] if item["label"] == "DWS_DWD.T_EARLY"
        )
        self.assertEqual(
            node_labels_in_col(data, table_col, "source_table"),
            ["DWS_DWD.T_EARLY", "DWS_DWD.T_LATE"],
        )

    def test_missing_schedule_time_nodes_sort_last_and_fallback_to_label(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_beta join dws_dwd.t_alpha join dws_dwd.t_timed on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_BETA", "DWS_DWD.T_ALPHA", "DWS_DWD.T_TIMED"},
            schedule_time_map={
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWD.T_TIMED": schedule("DWS_DWD.T_TIMED", "2026-07-10 07:45:00"),
            },
        )

        table_col = next(
            item["col"] for item in data["nodes"] if item["label"] == "DWS_DWD.T_TIMED"
        )
        self.assertEqual(
            node_labels_in_col(data, table_col, "source_table"),
            ["DWS_DWD.T_TIMED", "DWS_DWD.T_ALPHA", "DWS_DWD.T_BETA"],
        )

    def test_same_schedule_time_sort_is_stable(self):
        schedule_map = {
            "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
            "DWS_DWD.T_BETA": schedule("DWS_DWD.T_BETA", "2026-07-10 08:00:00"),
            "DWS_DWD.T_ALPHA": schedule("DWS_DWD.T_ALPHA", "2026-07-10 08:00:00"),
        }
        result_tables = {"DWS_DWM.T_ROOT", "DWS_DWD.T_ALPHA", "DWS_DWD.T_BETA"}
        forward = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_beta join dws_dwd.t_alpha on 1=1",
                )
            ],
            result_tables,
            schedule_time_map=schedule_map,
        )
        reverse = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_alpha join dws_dwd.t_beta on 1=1",
                )
            ],
            result_tables,
            schedule_time_map=schedule_map,
        )

        table_col = next(
            item["col"]
            for item in forward["nodes"]
            if item["label"] == "DWS_DWD.T_ALPHA"
        )
        self.assertEqual(
            node_labels_in_col(forward, table_col, "source_table"),
            ["DWS_DWD.T_ALPHA", "DWS_DWD.T_BETA"],
        )
        self.assertEqual(
            node_labels_in_col(reverse, table_col, "source_table"),
            ["DWS_DWD.T_ALPHA", "DWS_DWD.T_BETA"],
        )

    def test_filtering_happens_before_schedule_sorting(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_keep join dws_dwd.code_status_map join dws_dwm.t_root on 1=1",
                )
            ],
            {"DWS_DWM.T_ROOT", "DWS_DWD.T_KEEP"},
            schedule_time_map={
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWD.T_KEEP": schedule("DWS_DWD.T_KEEP", "2026-07-10 08:00:00"),
                "DWS_DWD.CODE_STATUS_MAP": schedule(
                    "DWS_DWD.CODE_STATUS_MAP", "2026-07-10 07:00:00"
                ),
            },
        )

        self.assertEqual(node_labels_in_col(data, 0), ["DWS_DWD.T_KEEP"])
        self.assertNotIn("DWS_DWD.CODE_STATUS_MAP", labels(data))
        self.assertEqual(len(data["nodes"]), 3)
        self.assertEqual(len(data["edges"]), 2)

    def test_dwuppr_alias_matches_registered_result_table(self):
        data = graph_data(
            "DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO",
            [
                process(
                    "DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO",
                    "select * from dws_dwd.t_mid",
                )
            ],
            {"DWP.DEMO_EXPORT_USER_INFO", "DWS_DWD.T_MID"},
        )

        self.assertIn("DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO", labels(data))
        self.assertFalse(any("非结果表节点" in item for item in data["warnings"]))

    def test_root_target_not_in_registry_is_still_expanded_when_process_is_found(self):
        data = graph_data(
            "DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO",
            [
                process(
                    "DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO",
                    "select * from dws_dwd.t_mid",
                )
            ],
            {"DWS_DWD.T_MID"},
        )

        self.assertIn("DWS_DWE.DEMO_CATALOG_CONTR_USER_INFO", labels(data))
        self.assertIn("DWS_DWD.T_MID", labels(data))
        self.assertEqual(len(data["edges"]), 2)
        self.assertTrue(
            any("根目标表未命中结果表白名单" in item for item in data["warnings"])
        )

    def test_normalize_table_name_removes_quotes_and_normalizes_case(self):
        self.assertEqual(
            normalize_table_name('  `dws_dwd` . "t_mid"  '), "DWS_DWD.T_MID"
        )
        self.assertTrue(is_known_result_table("'dws_dwd'.'t_mid'", {"DWS_DWD.T_MID"}))

    def test_collect_lineage_table_names_ignores_process_nodes(self):
        graph, _ = build_lineage_graph(
            "DWS_DWM.T_ROOT",
            [process("DWS_DWM.T_ROOT", "select * from dws_dwd.t_mid")],
            max_depth=8,
            result_table_names={"DWS_DWM.T_ROOT", "DWS_DWD.T_MID"},
        )

        self.assertIsNotNone(graph)
        self.assertEqual(
            collect_lineage_table_names(graph), {"DWS_DWM.T_ROOT", "DWS_DWD.T_MID"}
        )

    def test_targeted_schedule_loader_only_queries_filtered_graph_tables(self):
        captured: dict[str, object] = {}

        def loader(table_names, profile="demo"):
            captured["profile"] = profile
            captured["tables"] = set(table_names)
            return {name: schedule(name, "2026-07-10 08:00:00") for name in table_names}

        graph, candidates = build_lineage_graph_with_targeted_schedule_times(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwd.t_keep join dws_dwd.code_status_map join dws_dwm.t_root on 1=1",
                )
            ],
            max_depth=8,
            result_table_names={"DWS_DWM.T_ROOT", "DWS_DWD.T_KEEP"},
            schedule_time_loader=loader,
        )

        self.assertEqual(candidates, [])
        self.assertIsNotNone(graph)
        self.assertEqual(captured["profile"], "demo")
        self.assertEqual(captured["tables"], {"DWS_DWM.T_ROOT", "DWS_DWD.T_KEEP"})
        self.assertNotIn("DWS_DWD.CODE_STATUS_MAP", captured["tables"])
        self.assertTrue(
            all(
                node.detail["调度时间"] == "2026-07-10 08:00:00"
                for node in graph.nodes
                if node.type != "process"
            )
        )

    def test_schedule_time_sql_includes_table_aliases(self):
        sql = build_schedule_time_sql_for_tables({"DWS_DWM.M_DEMO_SUMMARY"})

        self.assertIn("'DWS_DWM.M_DEMO_SUMMARY'", sql)
        self.assertIn("'DWM.M_DEMO_SUMMARY'", sql)
        self.assertIn("'M_DEMO_SUMMARY'", sql)
        self.assertIn("demo_meta.result_receipts", sql)
        self.assertIn("upper(trim(p.target_table)) in", sql)
        self.assertNotIn("substr(p.target_table, 5)", sql)

    def test_schedule_row_with_plain_schema_name_maps_back_to_dws_node(self):
        result: dict[str, ScheduleTimeInfo] = {}

        merge_schedule_time_row(
            result,
            "DWM.M_DEMO_SUMMARY",
            "JOB_DEMO_SUMMARY",
            "2026-07-10 11:30:00",
        )

        self.assertIn("DWS_DWM.M_DEMO_SUMMARY", result)
        self.assertIn("DWM.M_DEMO_SUMMARY", result)
        self.assertEqual(
            result["DWS_DWM.M_DEMO_SUMMARY"].schedule_time, "2026-07-10 11:30:00"
        )
        self.assertEqual(
            result["DWM.M_DEMO_SUMMARY"].schedule_time, "2026-07-10 11:30:00"
        )

    def test_schedule_row_with_short_name_maps_to_requested_dws_node(self):
        result: dict[str, ScheduleTimeInfo] = {}

        merge_schedule_time_row(
            result,
            "M_DEMO_DETAIL",
            "JOB_DEMO_DETAIL",
            "2026-07-10 11:45:00",
            requested_table_names={"DWS_DWM.M_DEMO_DETAIL"},
        )

        self.assertEqual(
            result["DWS_DWM.M_DEMO_DETAIL"].schedule_time,
            "2026-07-10 11:45:00",
        )

    def test_schema_and_short_schedule_rows_populate_dws_node_detail(self):
        for returned_name in ("DWM.M_DEMO_DETAIL", "M_DEMO_DETAIL"):
            with self.subTest(returned_name=returned_name):
                graph, _ = build_lineage_graph(
                    "DWS_DWM.M_DEMO_DETAIL",
                    [process("DWS_DWM.M_DEMO_DETAIL", "")],
                    result_table_names={"DWS_DWM.M_DEMO_DETAIL"},
                )
                result: dict[str, ScheduleTimeInfo] = {}
                merge_schedule_time_row(
                    result,
                    returned_name,
                    "JOB_DEMO_DETAIL",
                    "2026-07-10 11:45:00",
                    requested_table_names={"DWS_DWM.M_DEMO_DETAIL"},
                )

                apply_schedule_times_to_graph(graph, result)

                root = next(node for node in graph.nodes if node.type == "root_table")
                self.assertEqual(root.schedule_time, "2026-07-10 11:45:00")
                self.assertEqual(root.detail["调度时间"], "2026-07-10 11:45:00")

    def test_apply_schedule_times_populates_process_table_and_source_fallback(self):
        graph, _ = build_lineage_graph(
            "DWS_DWM.T_ROOT",
            [
                process("DWS_DWM.T_ROOT", "select * from dws_dwd.t_mid"),
                process("DWS_DWD.T_MID", "select * from dws_dwf.t_source"),
            ],
            max_depth=8,
            result_table_names={"DWS_DWM.T_ROOT", "DWS_DWD.T_MID", "DWS_DWF.T_SOURCE"},
        )
        self.assertIsNotNone(graph)
        apply_schedule_times_to_graph(
            graph,
            {
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWD.T_MID": schedule("DWS_DWD.T_MID", "2026-07-10 09:00:00"),
            },
        )
        nodes = {node.id: node for node in graph.nodes}
        self.assertEqual(
            nodes["table:DWS_DWM.T_ROOT"].detail["调度时间"], "2026-07-10 10:00:00"
        )
        self.assertEqual(
            nodes["process:process_registry:DEMO_PROJECT: DWS_DWM.T_ROOT"].detail[
                "调度时间"
            ],
            "2026-07-10 10:00:00",
        )
        self.assertEqual(
            nodes["table:DWS_DWD.T_MID"].detail["调度时间"], "2026-07-10 09:00:00"
        )
        self.assertEqual(
            nodes["table:DWS_DWF.T_SOURCE"].detail["调度时间"], "2026-07-10 09:00:00"
        )

    def test_f_layer_slowest_three_nodes_are_marked_with_alert_levels(self):
        data = graph_data(
            "DWS_DWM.T_ROOT",
            [
                process(
                    "DWS_DWM.T_ROOT",
                    "select * from dws_dwf.t_a join dws_dwf.t_b join dws_dwf.t_c join dws_dwf.t_d on 1=1",
                )
            ],
            {
                "DWS_DWM.T_ROOT",
                "DWS_DWF.T_A",
                "DWS_DWF.T_B",
                "DWS_DWF.T_C",
                "DWS_DWF.T_D",
            },
            schedule_time_map={
                "DWS_DWM.T_ROOT": schedule("DWS_DWM.T_ROOT", "2026-07-10 10:00:00"),
                "DWS_DWF.T_A": schedule("DWS_DWF.T_A", "2026-07-10 08:00:00"),
                "DWS_DWF.T_B": schedule("DWS_DWF.T_B", "2026-07-10 09:00:00"),
                "DWS_DWF.T_C": schedule("DWS_DWF.T_C", "2026-07-10 07:00:00"),
                "DWS_DWF.T_D": schedule("DWS_DWF.T_D", "2026-07-10 10:30:00"),
            },
        )

        nodes = {item["label"]: item for item in data["nodes"]}
        self.assertEqual(nodes["DWS_DWF.T_D"]["alert_rank"], 1)
        self.assertEqual(nodes["DWS_DWF.T_D"]["alert_level"], "red")
        self.assertEqual(nodes["DWS_DWF.T_B"]["alert_rank"], 2)
        self.assertEqual(nodes["DWS_DWF.T_B"]["alert_level"], "orange")
        self.assertEqual(nodes["DWS_DWF.T_A"]["alert_rank"], 3)
        self.assertEqual(nodes["DWS_DWF.T_A"]["alert_level"], "amber")
        self.assertIsNone(nodes["DWS_DWF.T_C"]["alert_rank"])
        self.assertEqual(nodes["DWS_DWF.T_C"]["alert_level"], "")
        self.assertIsNone(nodes["DWS_DWM.T_ROOT"]["alert_rank"])


if __name__ == "__main__":
    unittest.main()
