# SQL 实际血缘与调度配置血缘对账 V1

## 目的

Reconciliation V1 面向一个严格的 `environment + source_profile` scope，比较：

- **SQL Actual**：DWS `dwp.lineage_business_edge` 中已经由现有 SQL
  parser、Physical DAG、Audit、TMP/DLO/DWO collapse 与 Business Asset Boundary
  产出的业务事实；
- **Configured Schedule**：DWS `dwp.lineage_schedule_edge` 中由开发调度
  ingestion 落地的配置事实。

Reconciliation 只消费两个 DWS current active snapshot，不重新访问 MySQL、不重新
解析 SQL、不重新构造 Physical DAG，也不重新实现 TMP/DLO/DWO collapse。

## Comparison Identity

比较单位是 Business Table Edge：

```text
(environment, source_profile, comparison_target_table, comparison_source_table)
```

同一个 `source_table -> target_table` 即使来自多个 SQL program 或多个 schedule
process，也只生成一个 row。`sql_fact_count`、`schedule_fact_count` 以及有界的
`sql_program_count`、`schedule_process_count` 仅是 provenance 摘要，不参与状态判断。

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
        snapshot_scope=(("DEMO_ENV", "DEMO_PROFILE"),),
    ),
    ScheduleLineageSnapshot(
        batch_id="batch-demo-schedule",
        edges=schedule_edges,
        observed_at=observed_at,
    ),
    environment="DEMO_ENV",
    source_profile="DEMO_PROFILE",
    target_table="DEMO_DWM.RESULT_A",
)
```

真实 DWS reader 入口为 `reconcile_active_dws_lineage()`。它只依赖现有两个
repository 的 active reader contract。

## CLI

单 target golden check：

```bash
python -B -m tools.lineage.reconcile_sql_schedule \
  --dws-profile <DWS_PROFILE> \
  --environment <DEV_ENVIRONMENT> \
  --profile <SOURCE_PROFILE> \
  --target <TARGET_SCHEMA.TARGET_TABLE> \
  --format table
```

输出接近：

```text
Target: DEMO_DWM.RESULT_A
Status: DIFFERENT

SOURCE_TABLE       SQL_ACTUAL   SCHEDULED   STATUS
DEMO_DWF.A         YES          YES         MATCH
DEMO_DWF.B         YES          NO          SQL_ONLY
DEMO_DWF.C         NO           YES         SCHEDULE_ONLY
```

不带 `--target` 时 `table` 输出只给整个 scope 的 aggregate，避免默认把全量真实
identity 打到日志；`json` 和 `csv` 可用于显式本地 report/artifact。全 scope aggregate
包括 edge 数、row 数、三态计数、target 数、consistent/different target 数、两个
batch id 和 elapsed time。

## Non-Goals

- 不新增 `lineage_reconciliation`、`lineage_compare`、`lineage_diff_result` 或
  `reconciliation_batch` 表；
- 不改变 SQL parser、Physical DAG、TMP collapse、Business Asset Boundary、
  Schedule ingestion、SQL/schedule DWS fact schema、DatasetIdentity 或 global
  `normalize_table_name()`；
- 不实现 Production Scheduler lineage；
- 不修改 lineage-viewer、data-asset-portal、Streamlit 或其它 UI；
- 不实现 freshness policy、告警、SLA、趋势或 reconciliation history persistence。
