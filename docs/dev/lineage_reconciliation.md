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
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -B -m tools.lineage.reconcile_sql_schedule \
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
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -B -m tools.lineage.reconcile_sql_schedule \
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

`scopes` 是 reconciliation Web / scope resolver 与 lineage daily job 的配置要求。旧的
lineage provider ingestion、provider verification、SVN verification 与单任务
materialization 仍可读取没有 `scopes` 的既有 `lineage_providers.local.yaml`；scope
resolver 和 daily job 在缺少或结构非法时返回 `LINEAGE_SCOPE_CONFIG_INVALID` /
`LINEAGE_SCOPE_CONFIG_NOT_FOUND`。部署人员无需创建 `configs/lineage_scopes.local.yaml`。

每个目标表独立调用 `tools.lineage.reconcile_sql_schedule.run()`，该函数继续进入
`reconcile_active_dws_lineage()`。页面不读取源 metadata、不解析 SQL、不读取旧调度
relation 表，也不通过 subprocess 调用 CLI。结果保留正式 `MATCH`、`SQL_ONLY`、
`SCHEDULE_ONLY` status，并将差异行优先展示；缺少任一 active snapshot 时保留正式错误码
并 fail closed。旧 `tools/integrations/schedule_diff.py` 不作为公开工具入口，文件保留
用于 rollback。

## Raw Status 与 Presentation Suppression

Raw reconciliation 的事实三态保持不变：

- `MATCH`：SQL 与 Schedule 都存在；
- `SQL_ONLY`：SQL 实际存在、Schedule 未配置；
- `SCHEDULE_ONLY`：Schedule 存在、SQL 未观察到。

`NO_INTERNAL_PRODUCER` 不是第四个 `ReconciliationStatus`，而是只附着在原始
`SQL_ONLY` 之上的 Presentation Suppression classification。它只能表示：在本次
`environment + sql_source_profile + schedule_source_profile` reconciliation scope
的已验证 active snapshots 中，没有观察到任何生产该 source 的内部 business edge：

```text
raw_status == SQL_ONLY
AND
not exists SQL edge       X -> SOURCE
AND
not exists Schedule edge  Y -> SOURCE
=> suppression_reason = NO_INTERNAL_PRODUCER
```

producer 不要求与当前 target 关联；只要 scope 内存在 `X -> SOURCE`，就不能 suppression。
判断继续复用 `normalize_lineage_comparison_table_key()` 的 qualified
`schema.table` identity；不使用 basename、suffix、LIKE、schema guessing、表名关键字
或人工 whitelist。`TMP` 命名没有 technical 语义；只有 `DLO`、`DWO` technical-only
endpoint 仍由既有 business reconciliation boundary 负责，classifier 不重新引入它们。

**absence of producer != proof of manual table**。该 classification 不声称 source
是手工维护表、码值表或参考表，只记录当前 scope 内没有观察到内部 producer。
`MATCH` 与 `SCHEDULE_ONLY` 永不 suppression；有内部 SQL/Schedule producer 的
`SQL_ONLY` 仍然是 actionable SQL_ONLY。

### Program Inventory 契约

「当前 environment 是否存在声明加工某张表的 active 内部程序」的唯一正式事实源是
`dwp.lineage_program_state`，而不是最近的 SQL / Schedule edge。**只有 canonical
`005` marker 提供 Program Result / Program Inventory 权威证据**，grammar 固定为：

```text
005:<qualified_schema_table>[:...]
```

- 只解释第 1 段 marker 与第 2 段 qualified target；
- 第 3 段起的后续 segment 一律不解释：不作为 step、不授予 canonical target authority、
  不进入 stable identity；
- 复用显式 registry 的 legacy namespace normalization（如
  `DWS_DWP.TMP_X -> DWP.TMP_X`），不新增 basename / fuzzy schema guessing；
- 表名不参与分类：`DWP.TMP_P_REPORT_KYW_LIST` 是合法 Program Result target。

它与 Issue #44 的 canonical grammar 完全一致：`PROGRAM_NAME_LEGACY_MARKER` 为
`"005"`，`parse_program_name()` 的 target authority、step sequencing 与 opaque suffix
语义均不因 Program Inventory 扩大。`PROGRAM_INVENTORY_PREFIXES` 只是由
`PROGRAM_NAME_LEGACY_MARKER` 派生的兼容常量（`frozenset({"005"})`），不是第二套
marker registry。

失败语义区分两类，完整契约见
[`lineage_asset_semantics.md`](lineage_asset_semantics.md)：

- `005` 但第二段无法规范化为 qualified `schema.table` => 抛错，scope fail-open；
- 非 `005`（`001:`、`002:`、`ABC:`、无冒号普通名字）=> 不是 Program Result 声明，
  直接 skip，不提供 `HAS_INTERNAL_PROGRAM` evidence，也不阻止 suppression；
- 错误发生时不得 publish 新 suppression，也不得 retire 旧 active suppression。

```text
ReconciliationSuppressionError
        => scope fail-open
        => 不 publish 新 suppression
        => 不 retire 旧 suppression
```

没有 `:` 的 program name 不是 inventory declaration，不提供 inventory 证据。

## Suppression Audit 与生命周期

纯函数 `classify_reconciliation_suppressions()` 消费
`LineageReconciliationResult`、已验证 SQL active business snapshot 和 Schedule active
snapshot，返回不可变 suppression candidates。它不连接 DWS、不加载 YAML、不调用
PyWebIO，也不修改 raw result。`DWSReconciliationSuppressionStore` 位于明确的
materialization/repository boundary，写入：

```text
dwp.lineage_reconciliation_suppression
```

其中 `suppression_key` 稳定依赖 environment、双侧 profile、source、target 和 reason，
不依赖 batch id；`row_key` 区分一次 SQL/Schedule snapshot observation。audit row 保存
`raw_status`、`suppression_reason`、`classifier_version`、双侧 batch provenance、
`first_seen_at` / `last_seen_at` / `is_active` 等字段。

每次 materialization 以一个 scope 为边界：当前仍成立的 suppression 更新 provenance
并保持 `first_seen_at`，新候选新增，旧 active candidate 设置 `is_active = FALSE`。
错误 scope、reader error、normalization error、metadata ambiguity、snapshot batch
不一致或 stale suppression 均遵循 **uncertain => DO NOT SUPPRESS**；失败时不退休
旧 active row，前台必须继续展示原始 SQL_ONLY。

显式 materialization command：

```bash
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -B -m jobs.crontab.imp_lineage_suppression --dry-run
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -B -m jobs.crontab.imp_lineage_suppression --environment DEV214
```

`--dry-run` 只输出每个 scope 的 bounded summary：`environment`、两侧 batch、raw
`SQL_ONLY` 数、suppressed 数和 actionable `SQL_ONLY` 数，不输出大量真实表名。该选项只属于
suppression 单任务；SQL 与 Schedule 当前没有统一的只读 dry-run 语义，因此 daily 入口不提供
`--dry-run`。PyWebIO
presentation adapter 使用 audit row 前必须同时校验 scope、`raw_status`、reason、
`classifier_version` 以及 `sql_batch_id == 当前 SQL batch`、
`schedule_batch_id == 当前 Schedule batch`。任何无法验证的情况都 fail-open-to-visible。
UI renderer 不能执行 DWS `INSERT`。

### 统一 lineage 日批

生产正式调度只需要维护以下一个入口：

```text
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_daily
```

只运行一个已配置 environment：

```text
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_daily --environment DEV214
```

依赖关系固定为 SQL 与 Schedule 两个 sibling 都成功后才执行 Suppression：

```text
SQL lineage ─────┐
                 ├─ Suppression materialization
Schedule lineage ┘
```

daily 会按现有 `scopes` 选择 enabled scope，不新增 daily 配置文件。单任务补跑仍使用
各自正式 CLI：

```text
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --store dws --dws-profile <DATABASE_PROFILE> --profile <SQL_SOURCE_PROFILE>
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_schedule_lineage --dws-profile <DATABASE_PROFILE> --profile <SCHEDULE_SOURCE_PROFILE>
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_suppression --environment DEV214
```

若 SQL 或 Schedule 任一失败，Suppression 输出 `SKIPPED reason=upstream_failed`，不会读取旧
snapshot 进行派生刷新；其它 environment 仍继续执行，任一 environment FAILED 时 daily
最终返回 non-zero。由于三个子任务的 dry-run 语义不同，daily 第一版不提供 `--dry-run`。

### 可执行入口职责

- `tools/lineage`：只保留 PyWebIO / Web 页面及明确的人工或诊断 CLI；
- `jobs/crontab`：集中放置 SQL lineage、Schedule lineage、Suppression materialization
  和 daily orchestration 等生产 batch 入口；
- `shared/lineage`：继续承载 domain、service、parser、store 和 classifier 等可复用逻辑。

当前 reconciliation Web 仍由 `configs/tools.yaml` 注册，生产 batch 不通过 Web 注册表启动。

审计查询示例：

```sql
SELECT
    environment,
    source_table,
    target_table,
    raw_status,
    suppression_reason,
    sql_batch_id,
    schedule_batch_id,
    classifier_version,
    is_active
FROM dwp.lineage_reconciliation_suppression
WHERE is_active = TRUE
  AND environment = 'DEV214'
ORDER BY target_table, source_table;
```

## Non-Goals

- 不新增 `lineage_reconciliation`、`lineage_compare`、`lineage_diff_result` 或
  `reconciliation_batch` 表；suppression audit 使用明确的
  `lineage_reconciliation_suppression` projection；
- 不改变 SQL parser、Physical DAG、TMP collapse、Business Asset Boundary、
  Schedule ingestion、SQL/schedule DWS fact schema、DatasetIdentity 或 global
  `normalize_table_name()`；
- 不实现 Production Scheduler lineage；
- 不修改 lineage-viewer、data-asset-portal、Streamlit 或其它 UI；
- 不实现 freshness policy、告警、SLA、趋势或 raw reconciliation 三态改写。
