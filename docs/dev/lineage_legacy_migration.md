# Lineage Phase 7：Legacy Lineage Inventory

本清单基于仓库当前 import/reference 扫描（`rg`）和
[`lineage_call_graph.md`](lineage_call_graph.md)，只记录真实存在的入口。Phase 7
不把“都和 lineage 这个词有关”当作重复实现：字段级映射、调度依赖、DWF 截止、
HTML schedule graph 和正式业务 `lineage_edge` 是不同 contract。

## Inventory

| Path | Entry | Current caller / registration | Current capability | Replacement | Decision | Migration risk |
| --- | --- | --- | --- | --- | --- | --- |
| `jobs/crontab/imp_lineage_edge.py` | `main()` / `materialize_sources()` | `configs/tools.yaml` 外的 crontab/manual entry；测试直接注入 Provider | Provider → Physical DAG → Audit → TMP collapse → formal edge publish | `plan_incremental()` + complete candidate + existing `SQLiteMaterializationStore.publish()` | **MIGRATE（完成）** | 低；保留 `build_candidate_batch()` 兼容入口，publish 仍 atomic |
| `shared/lineage/lineage_builder.py` | `load_process_infos()`, `normalize_table_name()`, `build_lineage_graph*()` | `ProductionProvider`、`physical_dag.py`、`tools/integrations/lineage_roamer.py`、测试 | legacy metadata loader、normalization、旧 process/table graph 和 schedule enrichment | Provider adapter、Phase 3 Physical DAG、Phase 6 Query 分别覆盖不同部分 | **KEEP / compatibility** | 高；仍是 Provider 与 normalization 的真实依赖，不能删除 |
| `tools/integrations/lineage_roamer.py` | `main()`, `analyze_one()` | 手动 PyWebIO tool；import `shared.lineage.lineage_builder` | 旧 process graph + targeted schedule time + HTML 漫游 | `LineageQueryService` 可替代 formal table graph，但不提供 schedule time/process nodes | **KEEP** | 中；用户依赖 schedule/HTML 语义，不能无损替换 |
| `tools/integrations/sql_upstream_to_layer.py` | `main()`, `trace_to_dwf_by_sql()`, `trace_to_dwf_by_schedule()` | 当前仓库无其他 caller；手动脚本 | 独立复制 legacy SQL/process/schedule 追踪并截止到 DWF | formal `imp_lineage_edge` + Query 只覆盖正式 table graph，不覆盖其 DWF report | **DEPRECATE（保留兼容）** | 中；没有 repo-wide caller，但可能有公开用户手动调用，暂无强删 |
| `tools/integrations/schedule_diff.py` | schedule diff entry | 手动 integration；静态扫描确认其独立调用链 | 比较 SQL 实际引用与 relations 配置，不发布正式 edge | Audit/History 可提供 issue/batch facts，但不等价于 config diff | **KEEP** | 低；不是重复的 formal lineage query |
| `tools/integrations/mysql_dependency_search.py` | metadata substring search | 手动 integration | 按代码字符串搜索 process metadata，不构图 | 无；Query 需要已 materialized edge | **KEEP** | 低 |
| `shared/lineage/schedule_table_lineage.py` | `trace_table_lineage()` | `tools/search/table_lineage_roamer.py`、`table_upstream_to_dwf.py`、测试 | job dependency/table trace，支持 DWF cutoff 与 terminal 标记 | Query BFS 不包含 job dependency、DWF cutoff 或 schedule metadata | **KEEP** | 中；方向相同但业务 contract 不同 |
| `tools/search/table_lineage_roamer.py` | `main()`, `analyze_one()` | `configs/tools.yaml` 注册 `table_lineage_roamer.py` | schedule-based HTML table lineage | Query Viewer contract 不含 schedule-specific node metadata | **KEEP** | 中 |
| `tools/search/table_upstream_to_dwf.py` | `main()`, `analyze_one()` | 手动 PyWebIO tool | schedule lineage 表格/导出，并以 DWF 为边界 | formal Query 不提供 DWF cutoff/export contract | **KEEP** | 中 |
| `shared/lineage/mapping_sqlite.py` | `walk_downstream_in_sqlite()` | `tools/search/workspace_lineage_search.py`、`import_mapping_to_sqlite.py`、`apps/svn_check` | Excel 字段级 mapping SQLite 与 workspace/reporting 搜索 | formal `lineage_edge` 是表级业务事实，不能替换字段级 mapping | **KEEP** | 高；误迁移会改变字段搜索结果 |
| `tools/search/workspace_lineage_search.py` | `app()` | `configs/tools.yaml` 注册 `workspace_lineage_search.py` | 字段级 mapping + workspace/Reporting/SEND 搜索 | 无直接 replacement | **KEEP** | 高 |
| `jobs/crontab/imp_dws_comments.py` | mapping import entry | crontab/manual | 从 SQL comments/import 生成 `asset_mappings` | 不属于 formal edge materialization | **KEEP** | 中 |
| `jobs/crontab/imp_recv_dwf.py` | receipt import entry | crontab/manual | receive plan/job dependency → `result_receipts` | 不属于 formal edge materialization | **KEEP** | 高 |
| `jobs/crontab/imp_send_lineage.py` | SEND lineage entry | crontab/manual | SEND → unload → process job chain | node 是 job，不是 formal table source/target | **KEEP** | 高 |
| `apps/svn_check/services/re_service.py` | `build_wide_table_lineage_summary()` | `apps/svn_check/ui/lakehouse_stream.py` | 审查页面的宽表依赖摘要 | 不是 Physical DAG 或 formal edge | **KEEP** | 高 |

## 实际迁移与未删除项

已迁移：

- `jobs/crontab/imp_lineage_edge.py` 不再每次对全部 `ProgramSource` 直接解析；
  它先加载 active `ProgramState`，只把 `NEW/CHANGED` 送入既有 builder/audit/
  materialization，再合并 unchanged facts 后 atomic publish。
- `shared.lineage.evolution` 只提供纯 planner、history reconciliation 和 graph
  diff；没有复制 Phase 3～6 算法。

保留：

- `shared/lineage/lineage_builder.py` 作为 `ProductionProvider` adapter、Physical
  DAG normalization 依赖和旧 schedule tool compatibility；
- schedule/job dependency、字段 mapping、审查摘要和 SEND/receive 入口；它们的
  输出不是统一表级 `lineage_edge`，强行迁移会改变公开行为。

Deprecated 但未移除：

- `tools/integrations/sql_upstream_to_layer.py`：当前没有仓库内 caller，但仍是
  可手动执行的公开脚本。后续若移除，必须先完成外部调用者确认、compatibility
  adapter 和回滚文档。

本阶段没有 `REMOVE` 项，也没有修改 `lineage-viewer` 或生产旧入口。删除前仍须
再次执行 repo-wide reference search 并补充行为验证。
