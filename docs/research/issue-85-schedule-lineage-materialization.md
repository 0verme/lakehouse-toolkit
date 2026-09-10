# Issue #85：开发 MySQL 调度血缘 DWS 落地

## 范围

本 Issue 只读取现有 `mysql_process_profiles` 中已配置的开发环境 MySQL
relation table，将每条：

```text
SRC_TABLE_KEY -> TAR_TABLE_KEY
```

作为 configured schedule lineage fact。`PROCESS_NAME` 和
`PROJECT_VERSION_KEY` 只承担 provenance/evidence，不推导
`PROCESS_A -> PROCESS_B` DAG。SQL 实际血缘与调度配置血缘的最终 reconciliation
属于后续任务。

V1 scope 由 profile 下的 `schedule_lineage` 配置表达：

- `DWD:1.0`、`DWM:1.0`、`DWP:1.0`、`DWA:1.0`、`DM:1.0` 全量纳入；
- `DWUPRR:1.0` 仅纳入 target schema prefix 为 `DWS_DWUPRR.` 的关系；
- `DW_PROJECT`、文件卸数/发送、`HDA`、`NUPS_DATA`、`TEST` 和其它未确认项目排除；
- 真实 table/column/connection 只存在于被忽略的 local config，公开 example 使用
  `DEMO_*` / `demo_meta` 占位值。

## Domain 与 normalization

`ScheduleLineageEdge` 同时保存：

- `raw_source_table` / `raw_target_table`：调度平台原始值；
- `source_table` / `target_table`：comparison identity；
- `environment`、`source_profile`、`process_name`、`project_version_key`：来源边界
  与 provenance。

`schedule.normalize_schedule_table_key()` 只在 schedule ingestion boundary 处理
已确认的 `DWS_<layer>` wrapper：

```text
DWS_DWF.A       -> DWF.A
DWS_DWM.B       -> DWM.B
DWS_DWUPRR.C    -> DWUPRR.C
DEMO_DWF.A      -> DEMO_DWF.A
```

它复用既有显式 legacy namespace registry，但不调用或修改 SQL
`normalize_table_name()`，也不改变严格的 `DatasetIdentity` physical contract。
未知 namespace 不猜测、不通过全字符串删除 `DWS_`。

`schedule_edge_key` 由 environment、source profile、process、project version 和
canonical source/target 组成，不包含 batch、时间或更新时间。同一 process 的重复
source/target 只保留一个 fact；不同 process 即使 source/target 相同也保留不同
provenance。

## DWS table 与 lifecycle

新增唯一事实表：`dwp.lineage_schedule_edge`。它保存 raw/comparison table、
provenance、`batch_id`、`is_active` 以及 first/last/changed timestamps；不创建
`schedule_job`、`schedule_program`、`schedule_closure` 或 `schedule_batch`。

Issue #84 的 `dwp.lineage_batch` 是 SQL lineage 五表的全局 active boundary。如果
schedule job 复用它作为 active control，会在独立 schedule publish 时错误退休 SQL
active snapshot。因此本实现：

1. 复用 `DWSMaterializationStore` 的 `connect_with_profile()` connection scope；
2. 复用同一模块的 DB-API/JDBC `_begin_transaction`、commit、rollback boundary；
3. 在单表内用 `batch_id` / `is_active` 保持 schedule history；
4. 在同一 transaction 中完成 candidate insert、application validation、active switch
   和 commit；
5. 任意 source、insert、validation、switch 或 commit failure rollback，上一 active
   schedule rows 保持有效。

完整 profile replay 只对显式 `(environment, source_profile)` scope 具有删除 authority。
`--limit` 永远是 partial replay，只更新读取到的 facts，不删除未读取的 edge。profile
selection 不会退休其它 profile 的 active facts。

空的完整 schedule snapshot 以零 active fact rows 表示；由于本 Issue 不创建第二张
batch control 表，返回的 batch id 只作为运行 provenance，active reader 对空集合返回
无 active edge。

## 内网验证

公开测试只使用 synthetic rows 和 SQLite/mock DB-API connection。合入后的内网验证
按以下顺序执行：

```text
一个 profile + 小 limit
→ 一个 profile + larger limit
→ 单 profile full
→ 多 DEV profiles
```

每级只回传脱敏 aggregate：source rows、accepted、rejected、normalized、deduplicated、
published edges、active batch、elapsed time；不得回传真实 process/table identity。
