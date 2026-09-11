# SQL 实际血缘与调度配置血缘对账 V1

## 目的

Reconciliation V1 比较同一个 `environment` 内的两侧 active snapshot：

- **SQL Actual**：DWS `dwp.lineage_business_edge` 中已经由现有 SQL
  parser、Physical DAG、Audit、TMP/DLO/DWO collapse 与 Business Asset Boundary
  产出的业务事实；读取 scope 为 `environment + sql_source_profile`；
- **Configured Schedule**：DWS `dwp.lineage_schedule_edge` 中由开发调度
  ingestion 落地的配置事实；读取 scope 为 `environment + schedule_source_profile`。

`environment` 是 business graph boundary。SQL source profile 与 Schedule source
profile 是相互独立的 provenance/read-scope dimension，可以不同，但不能省略任一侧。
Reconciliation 只消费两个 DWS current active snapshot，不重新访问 MySQL、不重新
解析 SQL、不重新构造 Physical DAG，也不重新实现 TMP/DLO/DWO collapse。

## Comparison Identity

比较单位是 Business Table Edge，逻辑 comparison identity 固定为：

```text
(environment, source_table, target_table)
```

内部 deterministic comparison key 使用 `(environment, comparison_target_table,
comparison_source_table)`，仅用于排序和聚合，不改变上述 identity 语义。

`sql_source_profile` 与 `schedule_source_profile` 不属于 MATCH identity，只用于各自
reader scope、provenance 和 result metadata。同一个 `source_table -> target_table`
即使来自多个 SQL program 或多个 schedule process，也只生成一个 row。
`sql_fact_count`、`schedule_fact_count` 以及有界的 `sql_program_count`、
`schedule_process_count` 仅是 provenance 摘要，不参与状态判断。

`normalize_lineage_comparison_table_key()` 只在 compare boundary 使用当前显式
legacy namespace registry：

```text
DWS_DWF.A      -> DWF.A
DWS_DWM.RESULT -> DWM.RESULT
DWS_DWUPRR.A   -> DWUPRR.A
```

它是幂等的；unknown namespace 保持原样。禁止 basename/suffix fuzzy match、schema
guessing 和 `value.replace("DWS_", "")`。该 helper 不调用来改写物理
`DatasetIdentity`，也不修改 `lineage_business_edge` 或 `lineage_schedule_edge` 中
已经保存的事实。

## Row Status

每个业务 table edge 固定为三态：

| Status | 含义 |
| --- | --- |
| `MATCH` | SQL 与 schedule 两边都有 |
| `SQL_ONLY` | SQL 实际调用但调度未配置 |
| `SCHEDULE_ONLY` | 调度已配置但 SQL 未调用 |

只使用 qualified `schema.table` identity。相同 basename 的不同 schema，例如
`SCHEMA_A.TABLE_X` 与 `SCHEMA_B.TABLE_X`，永远是不同 edge。

DLO/DWO/TMP technical-only endpoint 不会由 reconciliation 从 physical facts 中
重新引入。SQL reader 只读取 `lineage_business_edge`；schedule technical-only
edge 在 compare boundary 被排除，不会改写其 DWS source fact。

## Target Summary

用户通常从一个 target 反查上游。每个 target 有一个 summary：

- `sql_source_count`
- `schedule_source_count`
- `match_count`
- `sql_only_count`
- `schedule_only_count`
- `CONSISTENT`：`sql_only_count == 0` 且 `schedule_only_count == 0`
- `DIFFERENT`：否则

V1 不定义 `WARNING`、`ERROR`、`PARTIAL_MATCH`、`UNKNOWN_DIFF` 或 confidence
状态。

## Snapshot 与 Fail-Closed

SQL active reader 复用 `DWSMaterializationStore` 的现有 contract：

```text
active lineage_batch
+ fact batch_id
+ fact is_active
```

schedule active reader 复用 `DWSScheduleLineageStore` 的现有 `is_active` / batch
语义，不绕过 DWS 去读 MySQL。结果保留：

- `sql_batch_id`
- `schedule_batch_id`
- `sql_observed_at`
- `schedule_observed_at`

时间戳只作为 snapshot metadata，不在 V1 规定 freshness threshold。

`lineage_schedule_edge` 当前没有独立 schedule batch control table，所以请求 scope
没有 active schedule row 时无法区分“合法空 snapshot”和“从未成功发布”；V1 必须
返回 `SCHEDULE_ACTIVE_SNAPSHOT_NOT_FOUND`，不能把所有 SQL edge 误标成
`SQL_ONLY`。同理，没有可验证 active SQL batch 时返回
`SQL_ACTIVE_SNAPSHOT_NOT_FOUND`，不能把 schedule edge 误标成 `SCHEDULE_ONLY`。

## Python API

纯内存 compare：

```python
from shared.lineage.reconciliation import (
    ScheduleLineageSnapshot,
    SQLBusinessLineageSnapshot,
    reconcile_lineage_snapshots,
)

result = reconcile_lineage_snapshots(
    SQLBusinessLineageSnapshot(
        batch_id="batch-demo-sql",
        edges=sql_business_edges,
        observed_at=observed_at,
        snapshot_scope=(("DEMO_DEV", "DEMO_SQL_PROFILE"),),
    ),
    ScheduleLineageSnapshot(
        batch_id="batch-demo-schedule",
        edges=schedule_edges,
        observed_at=observed_at,
    ),
    environment="DEMO_DEV",
    sql_source_profile="DEMO_SQL_PROFILE",
    schedule_source_profile="DEMO_SCHEDULE_PROFILE",
    target_table="DEMO_DWM.RESULT",
)
```

脱敏 synthetic edge 示例：

```text
SQL / DEMO_SQL_PROFILE:
  DEMO_DWF.A -> DEMO_DWM.RESULT

Schedule / DEMO_SCHEDULE_PROFILE:
  DEMO_DWF.A -> DEMO_DWM.RESULT

Result: MATCH
```

为了兼容既有 V1 调用，仍可传 `source_profile="DEMO_PROFILE"`；这等价于同时
设置两侧 profile。同时提供旧参数与新参数时，三者值相同则允许；若新旧参数指定了
不同值，API 会 `raise ValueError`，不会默默覆盖。

真实 DWS reader 入口为 `reconcile_active_dws_lineage()`。它只依赖现有两个
repository 的 active reader contract。

## CLI

单 target golden check：

```bash
python -B -m tools.lineage.reconcile_sql_schedule \
  --dws-profile <DWS_PROFILE> \
  --environment <ENVIRONMENT> \
  --sql-profile <SQL_SOURCE_PROFILE> \
  --schedule-profile <SCHEDULE_SOURCE_PROFILE> \
  --target <SCHEMA.TABLE> \
  --format table
```

旧调用仍可使用兼容 shorthand；`--profile A --sql-profile A --schedule-profile A`
也允许，任一值不同则 fail fast：

```bash
python -B -m tools.lineage.reconcile_sql_schedule \
  --dws-profile <DWS_PROFILE> \
  --environment <ENVIRONMENT> \
  --profile <PROFILE> \
  --target <SCHEMA.TABLE> \
  --format table
```

输出接近：

```text
Target: DEMO_DWM.RESULT
Status: DIFFERENT
SQL profile: DEMO_SQL_PROFILE
Schedule profile: DEMO_SCHEDULE_PROFILE
SQL batch: batch-demo-sql
Schedule batch: batch-demo-schedule

SOURCE_TABLE       SQL_ACTUAL   SCHEDULED   STATUS
DEMO_DWF.A         YES          YES         MATCH
DEMO_DWF.B         YES          NO          SQL_ONLY
DEMO_DWF.C         NO           YES         SCHEDULE_ONLY
```

不带 `--target` 时 `table` 输出只给整个 scope 的 aggregate，避免默认把全量真实
identity 打到日志；`json` 和 `csv` 可用于显式本地 report/artifact。全 scope aggregate
包括 environment、SQL/Schedule 两侧 profile、edge 数、row 数、三态计数、target
数、consistent/different target 数、两个 batch id 和 elapsed time。JSON 顶层与每个
row/target summary 都使用 `sql_source_profile` 和 `schedule_source_profile`；CSV
每行也同时输出这两个字段，不再输出单一 `SOURCE_PROFILE`。

## PyWebIO 页面

统一页面入口为：

```text
tools/lineage/reconcile_sql_schedule_web.py
```

页面只让用户选择 environment，并以 textarea 接收每行一个目标表；SQL / Schedule
source profile 与 DWS profile 由 `LineageEnvironmentScopeResolver` 从同一个
lineage deployment config `configs/lineage_providers.local.yaml`（local 缺失时使用
`configs/lineage_providers.example.yaml`）的 `scopes` 根节点解析。公开 example 中的
scope 只引用同文件内已定义的 demo provider profile；真实配置不得提交仓库。

`scopes` 只属于 reconciliation Web / scope resolver 的配置要求。旧的 lineage
provider ingestion、provider verification、SVN verification 与 materialization 仍可
读取没有 `scopes` 的既有 `lineage_providers.local.yaml`；scope resolver 在缺少或
结构非法时返回 `LINEAGE_SCOPE_CONFIG_INVALID` / `LINEAGE_SCOPE_CONFIG_NOT_FOUND`。
部署人员无需创建 `configs/lineage_scopes.local.yaml`。

每个目标表独立调用 `tools.lineage.reconcile_sql_schedule.run()`，该函数继续进入
`reconcile_active_dws_lineage()`。页面不读取源 metadata、不解析 SQL、不读取旧调度
relation 表，也不通过 subprocess 调用 CLI。结果保留正式 `MATCH`、`SQL_ONLY`、
`SCHEDULE_ONLY` status，并将差异行优先展示；缺少任一 active snapshot 时保留正式错误码
并 fail closed。旧 `tools/integrations/schedule_diff.py` 不作为公开工具入口，文件保留
用于 rollback。

## Non-Goals

- 不新增 `lineage_reconciliation`、`lineage_compare`、`lineage_diff_result` 或
  `reconciliation_batch` 表；
- 不改变 SQL parser、Physical DAG、TMP collapse、Business Asset Boundary、
  Schedule ingestion、SQL/schedule DWS fact schema、DatasetIdentity 或 global
  `normalize_table_name()`；
- 不实现 Production Scheduler lineage；
- 不修改 lineage-viewer、data-asset-portal、Streamlit 或其它 UI；
- 不实现 freshness policy、告警、SLA、趋势或 reconciliation history persistence。
