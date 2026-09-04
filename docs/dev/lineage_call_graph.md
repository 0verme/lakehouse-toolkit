# 现有血缘调用关系与迁移地图

这是一份基于当前 `main` 的静态盘点，目的是给 Epic #2 的 Phase 2～7 提供
迁移地图。Phase 1 不把新领域模型强行接入旧入口，也不把下面的局部能力误认为
已经完成统一落库链路。

## 总体关系

```text
shared/lineage/lineage_builder.py
  ├─ tools/integrations/lineage_roamer.py
  └─ （同逻辑的独立副本）tools/integrations/sql_upstream_to_layer.py

shared/lineage/schedule_table_lineage.py
  ├─ tools/search/table_lineage_roamer.py
  └─ tools/search/table_upstream_to_dwf.py
       └─ shared/graph/dependency.py

shared/lineage/mapping_sqlite.py
  ├─ tools/search/import_mapping_to_sqlite.py
  ├─ tools/search/workspace_lineage_search.py
  ├─ apps/svn_check/ui/fine_stream.py
  └─ apps/svn_check/ui/lakehouse_stream.py

apps/svn_check/ui/lakehouse_stream.py
  ├─ core/lakehouse/python_rule.py
  ├─ core/lakehouse/sql_rule.py
  └─ services/re_service.py

jobs/crontab/*
  ├─ demo_meta.jobs / programs / processes / relations
  ├─ demo_meta.result_receipts / job_outputs
  └─ demo_meta.asset_mappings
```

## 1. `shared/lineage/lineage_builder.py`

这是当前最完整的 pipeline 程序血缘实现：

1. `load_process_infos()` 查询 `demo_meta.processes`，生成旧版
   `ProcessInfo(source_table, process_name, script_code)`；
2. `normalize_table_name()`、`table_name_aliases()` 处理 `DWS_` schema、短名和
   `DWE/DWP` 兼容别名；
3. `extract_tables_from_code()` 从去注释后的 `FROM/JOIN/USING` 中抽取表名；
4. `build_target_map()` 按 process name 推导 target；
5. `PipelineLineageBuilder` 生成 `LineageGraph`，图中的边是
   `upstream table → process node → target table`；
6. 结果表白名单来自 `shared.lineage.mapping_sqlite.load_registered_result_tables()`，
   非登记表（包括多数 TMP、码值表、辅助表）目前会在图构建时被过滤；
7. `tools/integrations/lineage_roamer.py` 调用 targeted schedule loader 后渲染上游图。

这套逻辑可直接复用 normalization、schema alias、注释清理和基础表名抽取，但
旧 `ProcessInfo` 没有 environment/profile/expected target/hash，且当前过滤策略
不满足“Physical 阶段保留 TMP”的新语义。因此 Phase 1 不改它，Phase 3 再用
adapter 或逐步迁移。

## 2. `shared/lineage/schedule_table_lineage.py`

这是以调度 metadata 为输入的表级上/下游漫游：

- `build_job_index()` 将 `jobs.dependency_text` 解析成每个 job 的依赖作业，并把
  `programs.target_table` 关联到 job；
- `trace_table_lineage()` 通过作业依赖遍历，在向上时于 DWF 截止，在向下时标记
  没有下游的 terminal table；
- `TableEdge(source_table, target_table, ...)` 已明确把 source 作为上游、target
  作为下游；`to_graph_dict()` 同样输出数据流方向；
- `tools/search/table_lineage_roamer.py` 负责 HTML 图形漫游，
  `tools/search/table_upstream_to_dwf.py` 负责表格和导出。

`shared.graph.dependency` 的内部 `build_dependency_graph()` 则保存
`current_job → dependency_job`，这是为了向上追溯的方便表示，不应直接作为新
业务血缘 edge。`build_reverse_dependency_graph()` 再将它反转用于下游查询，
这是需要保留的兼容边界。

## 3. `shared/lineage/mapping_sqlite.py`

这是另一条字段级/映射级链路：

- `tools/search/import_mapping_to_sqlite.py` 读取 Excel 的“源表/目标表/源字段/目标字段”，
  写入运行态 SQLite `lineage_edge`；
- `find_start_nodes_in_sqlite()` 以 source 字段找起点；
- `walk_downstream_in_sqlite()` 查询 `source_* → target_*` 并 BFS 下游；
- `filter_registered_result_nodes()` 再按 `programs.target_table` 筛选结果资产；
- `tools/search/workspace_lineage_search.py` 将 SQLite 下游结果继续用于 workspace、
  Reporting 和 SEND 字段搜索；`fine_stream`/`lakehouse_stream` 只读取登记结果
  表用于审查展示。

这套 SQLite 表也使用 source=上游、target=下游，但它是字段映射 demo，字段集合、
数据库生命周期和 Epic 计划中的正式 `lineage_edge` 不同，Phase 5 前不能直接
当作新的 materialization 表。

## 4. `apps/svn_check` 的程序审查与结果表推导

`apps/svn_check/ui/lakehouse_stream.py` 是审查主入口：

- 读取 PLAN/SEQ/JOB/PROGRAM Excel，并通过 `core.public_data` 查询 jobs、programs、
  结果登记、接入计划、job output、参数表等 metadata；
- `core/lakehouse/python_rule.py::rule_dws_py()` 使用
  `services/re_service.extract_tables()`、`find_dot_strings()`，并调用
  `core/lakehouse/_sql_parser.py` 的函数/视图/DDL helper；
- `get_program_table_name()`、`table_name_from_program_path()` 从程序目录推导结果表，
  `public_stream.normalize_table_name()` 只做大写/去首尾空白；
- `services/re_service.build_wide_table_lineage_summary()` 从 job/program 合并表、
  `dependency_text`、路径、`result_receipts`、`receive_plans` 和 `job_outputs` 组装
  “宽表依赖链路摘要”，这不是 Physical DAG；
- `core/lakehouse/_sql_parser.is_temp_table_statement()` 识别 TEMP DDL，
  `ddl_rule.DWS_TEMP_TABLE_PREFIXES` 当前为 `TMP_`，它们服务于命名/审查规则，
  尚未形成统一 TMP 资产分类。

`tools/integrations/metadata_dependency.py` 走相近的 metadata → 文件读取 →
`shared.text.regex.extract_tables/find_dot_strings` 路径，并使用
`shared.lineage.asset_tables` 的计划映射。它不生成统一 lineage edge。

## 5. integrations 与 jobs 的重复/兼容点

### 重复解析与 normalize

- `tools/integrations/sql_upstream_to_layer.py` 独立复制了旧版
  `ProcessInfo`、MySQL 读取、schema normalize、目标推导、SQL 上游追踪和 schedule
  追踪；它有 SQL/schedule 两种模式，直接在工具内计算到 DWF。
- `tools/integrations/schedule_diff.py` 再次复制 `ProcessInfo`、MySQL 读取、表名
  normalize、SQL 表提取和目标候选推导，用于比较 SQL 实际引用与 `relations`。
- `tools/integrations/mysql_dependency_search.py` 直接读取 processes 并按字符串
  搜索代码，不生成 DAG。

这些副本是 Phase 7 的 legacy cleanup 候选，Phase 1 不删除、不改调用方。

### 调度/结果/映射脚本

- `jobs/crontab/imp_recv_dwf.py` 从 receive plans、jobs、programs 和作业反向依赖
  重建 `result_receipts`，只保留 DWF 接入结果；这是结果登记逻辑，不是新
  `LineageEdge` materialization。
- `jobs/crontab/imp_send_lineage.py` 从 SEND → unload → process job 组装发送链路，
  字段名是 `send_job_name/unload_job_name/process_job_name`，内部方向不能直接
  当作 table source/target。
- `jobs/crontab/imp_dws_comments.py` 从本地 SQL 的 `INSERT INTO`/`FROM` 生成字段
  映射，写入 `asset_mappings`；它的 `source`/`target` 字段语义与新方向一致，但
  仍是局部字段映射导入。
- `jobs/crontab/imp_dws_comments.py`、`imp_recv_dwf.py`、`imp_send_lineage.py`
  目前采用清空后逐行写入等运行逻辑，批次 publish 留给 Phase 5。

## 6. 当前方向矩阵

| 模块 | 当前 source/target 或 adjacency | 与新语义关系 | 处理计划 |
| --- | --- | --- | --- |
| `lineage_builder` | 图边为表/进程上游 → 目标 | 一致，但会过滤未登记 TMP | Phase 3 adapter/迁移 |
| `schedule_table_lineage.TableEdge` | `source_table` → `target_table` | 一致 | 可复用 |
| `mapping_sqlite` | Excel/SQLite `source_*` → `target_*` | 一致，但属于字段映射 demo | Phase 5 前隔离 |
| `shared.graph.dependency` | `current_job` → `dependency_job` | 内部反向 adjacency | 保留，新增 adapter |
| `re_service` | dependency job → path 推导表 | 局部推导，不是 edge | Phase 7 收口 |
| `imp_send_lineage` | SEND/unload/process job 链 | 不是表级 source/target | Phase 7 评估 |
| `schedule_diff` | actual/configured 集合差异 | 不产 direct edge | Phase 7 评估 |

## 7. 后续 Phase 迁移地图

- **Phase 2**：把 `lineage_builder.load_process_infos()`、现有生产 metadata 读取和
  未来 DEV 多 profile 读取包成 Provider，输出 `ProgramSource`；不改变旧入口。
- **Phase 3**：优先复用 `lineage_builder` 的 alias/normalize/表名提取，替换或适配
  `sql_upstream_to_layer`、`schedule_diff` 的副本，输出含 TMP 的 Physical DAG。
- **Phase 4**：把当前 self/cycle/过滤 warning 转为 `LineageIssue`，以 branch sink
  聚合 orphan，并保留 target/sink 异常。
- **Phase 5**：在审计后 collapse TMP，只发布正式资产 direct `LineageEdge`，另存 issue；
  不复用现有字段 SQLite 表作为正式事实表。
- **Phase 6**：让 query、Blast Radius 和 Viewer JSON 只读取 materialized edge；Viewer
  不连接外部 metadata。
- **Phase 7**：使用 hash 做增量，补历史/diff，确认调用后再清理重复实现。

## Phase 1 边界

本次只新增 `shared.lineage.domain` 的领域对象、单元测试和语义文档。没有修改
上述旧入口、SQL、metadata schema、运行态 SQLite 或生产连接配置。
