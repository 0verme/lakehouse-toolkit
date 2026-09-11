# Issue #39：DWS Materialization Writer 与生命周期语义

> **状态：writer contract v0.3；默认不连接真实环境。**
>
> 本文冻结公开版 DWS projection、原子发布和查询边界。配套 DDL 位于
> [`issue-39-dws-materialization-v0.1.sql`](issue-39-dws-materialization-v0.1.sql)，
> lifecycle 机器可读矩阵位于 [`issue-39-dws-lifecycle-matrix.json`](issue-39-dws-lifecycle-matrix.json)。
> Writer 通过 `shared/db/gaussdb.py` 的 JayDeBeApi + Huawei Gauss200 JDBC
> connection boundary 工作；密码只允许由环境变量或本地密钥管理器提供。

## 1. 范围与边界

Issue #39 收口为五张 SQL lineage DWS 表；Issue #85 schedule 与 Issue #111
reconciliation suppression 是独立的事实扩展：

```text
dwp.lineage_batch
    ├─ dwp.lineage_program_state
    ├─ dwp.lineage_edge              # raw physical direct edge
    ├─ dwp.lineage_business_edge     # existing Phase 5 formal projection
    └─ dwp.lineage_issue

dwp.lineage_schedule_edge             # Issue #85 independent schedule snapshot
dwp.lineage_reconciliation_suppression # Issue #111 presentation audit
```

- `ProgramPhysicalDAG` 是同一 pipeline 产生的 physical projection，保留 TMP、cycle、
  self-reference 和 orphan facts；它不在本地解析 SQL，也不实现第二套 collapse。
- 现有 `materialize_program` / `build_materialization_batch` 产生的 formal direct
  `LineageEdge` 原样进入 `lineage_business_edge`。因此 `A -> TMP -> B` 只形成一条
  business row `A -> B`，`collapse_depth` 记录折叠 hop 数。
- `lineage_edge` 接收同一 `ProgramPhysicalDAG` 的直接 `PhysicalEdge`，允许 TMP
  endpoint，并保存 node kind 与 bounded `evidence_json`。
- DWS writer 只负责 row/key/lifecycle/validation/transaction；不猜测 DatasetIdentity、
  不从 program name 反推 target、不在 query-time 递归 TMP。
- `lineage_closure` 属于 Issue #40，本 Issue 不创建、不写入、不改变当前 direct-edge
  语义。
- SQLite 仍是 public/demo 默认 backend；`--store dws` 才选择生产 adapter。

### 1.1 同一输入的双 projection

```text
ProgramSource
   └─ build_program_physical_dag
        ├─ PhysicalEdge: A -> TMP1 -> TMP2 -> B
        │    └─ dwp.lineage_edge（3 direct physical rows，TMP 可见）
        └─ existing Phase 5 materialization
             └─ dwp.lineage_business_edge（1 formal row：A -> B）
```

formal boundary 仍由既有 materialization 决定：第一次遇到 formal asset 就停止，
不跨 formal asset 产生 transitive edge。`A -> FORMAL_B -> TMP -> FORMAL_C` 只形成
`A -> FORMAL_B` 与 `FORMAL_B -> FORMAL_C` 两条 business row。

## 2. Identity / key contract

Writer 使用 UTF-8、固定字段顺序、US（U+001F）separator、SHA-256 lowercase hex。
stable key 不使用 Python `hash()`、数据库自增 id、`repr()`、时间或随机 UUID。

| 对象 | stable identity | row key |
| --- | --- | --- |
| batch | `batch_id` | batch control row 直接使用 `batch_id` |
| program | `environment + source_profile + program_name` | `row + lineage_program_state + batch_id + program_key` |
| physical edge | program identity + source/target + node kinds | `row + lineage_edge + batch_id + edge_key` |
| business edge | program identity + formal source/target | `row + lineage_business_edge + batch_id + business_edge_key` |
| issue | Issue #36 `stable_issue_key` | `row + lineage_issue + batch_id + stable_issue_key` |
| suppression | scope + qualified source/target + reason | `row + suppression_key + sql_batch_id + schedule_batch_id` |

`source_profile` 是 program/provenance boundary，`environment` 是 hard boundary。
`DatasetIdentity` 只接受明确的 `environment + schema.table`；TMP 或 unresolved
physical node 的 dataset key 为 `NULL`，但 business row 的两端必须都有 dataset key。

## 3. 五张基线表与扩展表

### 3.1 `dwp.lineage_batch`

control table 记录一次 candidate snapshot：

- `snapshot_mode` 为 `FULL` 或 `PARTIAL`，`complete_snapshot` 和
  `snapshot_scope` 共同决定 disappearance authority；
- `pipeline_version` 来自现有 lineage pipeline，而不是 Git SHA；
- `edge_count` **只统计 `lineage_edge` physical rows**，不擅自改名为
  `physical_edge_count`，也不把 business count 塞入该字段；
- `program_count` / `issue_count` 必须与同一 batch 的事实行数相等；
- `publish_status` 依次为 `CANDIDATE`、`PUBLISHED` 或 `RETIRED`；
- 首次 publish 前可以没有 active batch，正常状态最多一个 active batch。

### 3.2 `dwp.lineage_program_state`

每个 candidate 当前可见 `ProgramIdentity` 一行。`source_hash` 与
`pipeline_version` 服务于既有 incremental planner；正常 rebase 保留
`first_seen_at`，新 snapshot 更新 `last_seen_at`。完整 scope 中消失的 program
不写入新 active candidate，历史 state 保留。

### 3.3 `dwp.lineage_edge`（physical direct）

一行对应一个 `ProgramPhysicalDAG.edges` 的 direct edge：

- `source_table` / `target_table` 保留 normalized physical name；
- `source_node_kind` / `target_node_kind` 区分 `formal_asset` 与 `temporary_asset`；
- TMP endpoint 允许，TMP 的 `source_dataset_key` / `target_dataset_key` 必须为 `NULL`；
- `evidence_json` 只保存单步 evidence，不保存完整 script、凭据、连接串或内部地址；
- `edge_key` 不包含 batch、时间、evidence 或 source hash；TMP rename 若改变物理
  endpoint label，会产生新的 physical identity，formal business key 则由 business
  projection 单独保持稳定；
- `edge_count` 与该表 batch 行数一致。

### 3.4 `dwp.lineage_business_edge`（formal direct）

一行对应既有 `LineageEdge` 的 formal direct semantic：

- 两端必须是明确 `DatasetIdentity`，禁止 TMP、缺 schema 或 unresolved endpoint；
- `collapse_depth >= 1`；`physical_derivation_hash` 是 bounded evidence 的稳定摘要；
- 不从 `lineage_edge` 在 DWS 端重新实现 TMP collapse；writer 只持久化 pipeline
  已经产出的 business projection；
- `business_edge_key` 不包含 batch、时间、evidence、job key 或 source hash；
- `lineage_business_edge` 不参与 Issue #40 closure。

### 3.5 `dwp.lineage_issue`

保存 Issue #36 `AuditFact` 与 `AuditPolicyResult` 的 persistence projection：
`issue_type`、`confidence`、`rule_version`、`message`、`evidence_json`、
`stable_issue_key` 以及 `severity`、`disposition`、`policy_version`、
`disposition_updated_at`、`disposition_updated_by`。`IssueLifecycleStatus` 不新增
为 DWS enum；manual disposition 通过新 batch/history projection 记录，不原地更新
旧 row。materialization failure 会 rollback，不留下半批 issue。

### 3.6 `dwp.lineage_reconciliation_suppression`

该表只审计 Presentation Suppression，不替换或删除 raw reconciliation 三态。V1
只允许 `raw_status = SQL_ONLY` 与 `suppression_reason = NO_INTERNAL_PRODUCER`，其中
后者只能表示当前 `environment + sql_source_profile + schedule_source_profile`
scope 内没有观察到内部 producer，并不证明来源表是手工表、码值表或参考表。

`suppression_key` 由 scope、qualified comparison identity 和 reason 组成，**不包含**
`sql_batch_id` / `schedule_batch_id`；`row_key` 则区分一次双侧 snapshot observation。
writer 在 application boundary 中维护同一 scope 的 active rows、first/last seen、
新 snapshot provenance 和 stale retirement。PyWebIO renderer 不执行 INSERT。

## 4. Physical design

DDL 对所有对象使用显式 `dwp.<table>`，不使用 `SET search_path`、默认 schema 或
运行时拼接对象名。v0.3 smoke schema 暂不依赖 DWS 端 primary/unique/check/partition
约束；writer 在 active switch 前重复执行 key、count、enum、DatasetIdentity、TMP
边界和 batch consistency validation。表的 orientation 与 distribution：

| 表 | orientation | distribution |
| --- | --- | --- |
| `lineage_batch` | ROW | HASH(`batch_id`) |
| `lineage_schedule_edge` | COLUMN | HASH(`schedule_edge_key`) |
| `lineage_reconciliation_suppression` | ROW | HASH(`suppression_key`) |
| `lineage_program_state` | ROW | HASH(`program_key`) |
| `lineage_edge` | COLUMN | HASH(`edge_key`) |
| `lineage_business_edge` | COLUMN | HASH(`business_edge_key`) |
| `lineage_issue` | ROW | HASH(`stable_issue_key`) |

DDL 中对应为 `DISTRIBUTE BY HASH(batch_id)`、`DISTRIBUTE BY HASH(schedule_edge_key)`、
`DISTRIBUTE BY HASH(suppression_key)`、`DISTRIBUTE BY HASH(program_key)`、
`DISTRIBUTE BY HASH(edge_key)`、`DISTRIBUTE BY HASH(business_edge_key)` 和
`DISTRIBUTE BY HASH(stable_issue_key)`。

这只是公开版第一阶段的物理 contract；真实 workload、partition、retention 和
DWS distributed uniqueness 需在脱敏非生产环境单独验证。Issue #40 的 closure 不在
本 DDL 中。

## 5. Atomic publish contract

所有五张表共用同一个 `batch_id`。DWS writer 的单次 publish 语义为：

```text
BEGIN
  read previous active batch and active projections
  reconcile Issue #36 lifecycle in memory
  insert lineage_batch(B, CANDIDATE, inactive)
  insert program_state(B, inactive)
  insert physical lineage_edge(B, inactive)
  insert formal lineage_business_edge(B, inactive)
  insert issue(B, inactive)
  validate batch counts, duplicate keys, program identity,
           physical node kinds, formal DatasetIdentity and TMP boundary
  retire previous batch/facts
  activate B and all five projections
COMMIT
```

候选行在切换前不可作为 current projection。任何 build、audit、已有 TMP collapse、
insert、validation 或 active switch 失败都 `ROLLBACK`；上一成功 batch 继续完整可读。
不执行“先切 edge、再补 business/issue”的两阶段公开状态。`is_active` 只是加速
flag，current 查询必须同时 join active batch。

### 5.1 Empty / negative result

完整且 deterministic 的空结果仍可成功发布：batch active、`program_count` 与
`issue_count` 按实际计数、`edge_count = 0`，五张表中对应事实行数为零。已知 orphan、
cycle、self-reference 等可完整分类的 negative result 随同 batch issue 发布；无法
验证的 collapse、超限或异常则 fail closed，不能替换旧 active。

### 5.2 Incremental / scoped snapshot

- source hash 与 pipeline version 都未变化时，既有 active facts rebase 到新 batch，
  stable keys 不变，row keys 因 batch 变化；
- source hash 缺失/变化或 pipeline version 变化时，重建 physical DAG、audit 和
  materialization，不只刷新一层；
- `PARTIAL`、limit replay 或 scope 外遗漏不授予 deletion authority；
- 只有 `FULL + complete_snapshot + explicit scope` 才能让 scope 内消失的 program
  从新 active candidate 消失；旧 batch 永不被原地删除；restore 不猜测 rename。

## 6. Active / history query contract

当前 physical 查询：

```sql
SELECT e.source_table, e.target_table, e.program_key,
       e.source_node_kind, e.target_node_kind
FROM dwp.lineage_edge AS e
JOIN dwp.lineage_batch AS b
  ON b.batch_id = e.batch_id
 AND b.is_active = TRUE
WHERE e.is_active = TRUE
  AND e.environment = :environment
  AND (:source_profile IS NULL OR e.source_profile = :source_profile);
```

当前 business 查询只需把 `dwp.lineage_edge` 替换为
`dwp.lineage_business_edge` 并选择 formal dataset columns。两者均禁止只依赖
`fact.is_active`；history 查询按显式 `batch_id` 读取，不混入其它 batch。应用层
上下游查询消费 business direct projection；raw physical 查询单独消费 physical
projection；未来 N-hop closure 由 Issue #40 另行定义。

## 7. SQLite → DWS compatibility matrix

| Projection | SQLite reference | DWS writer |
| --- | --- | --- |
| batch | `batch_id`、`edge_count`、`issue_count`、active/history | 五表共享 batch、snapshot metadata、candidate/published/retired gate |
| program state | environment/profile/name、hash/version、first/last/changed | `program_key` + `row_key`，append-by-batch |
| physical edge | 无持久化 raw DAG projection | `lineage_edge`，允许 TMP endpoint，保存 direct evidence |
| business edge | formal `LineageEdge` 由 reference pipeline 产生 | `lineage_business_edge`，只接收既有 formal projection |
| issue | Issue #36 fact/policy compatible fields | `lineage_issue`，同批 lifecycle reconcile 与 active join |
| suppression | raw reconciliation 三态上的 classification observation | `lineage_reconciliation_suppression`，按 scope 独立 materialize/lifecycle |
| closure | 无 | Issue #40 future；本 Issue 不创建 |

SQLite 默认行为和既有调用方保持兼容；DWS backend 是显式选择，不会因为环境变量
或默认配置而静默连接生产数据库。

## 8. Tests / contract lint scope

公开测试应覆盖：

- 五张表、显式 schema、orientation/distribution、`edge_count` 字段与无
  `lineage_closure`；
- physical DAG 的 TMP/cycle/orphan direct rows 与 business formal collapse 的
  同批写入；
- duplicate program/physical/business/issue stable identity、count mismatch、
  invalid node kind、TMP business endpoint、invalid collapse depth；
- candidate → published → retired lifecycle、active-batch join、history isolation；
- rollback 后 previous active 保持不变，空结果成功，partial/full scoped deletion、
  incremental reuse 与 pipeline rebuild；
- SQLite targeted tests 与默认 CLI backend 继续通过；DWS tests 使用 SQLite/mock
  DB-API connection，不连接真实 DWS，不包含内网地址或真实凭据。

## 9. Frozen decisions

| Decision | contract |
| --- | --- |
| physical projection | `ProgramPhysicalDAG` direct `PhysicalEdge` 写入 `lineage_edge`，TMP 可见 |
| business projection | 既有 Phase 5 formal direct `LineageEdge` 写入 `lineage_business_edge` |
| collapse | writer 不重复算法；formal boundary 在已有 materialization pipeline |
| identity | `row_key` 与 batch-independent stable key 分离；`edge_count` 保留原字段名 |
| lifecycle | 五表 append-by-batch、candidate validation、单 active switch、rollback、history |
| issue | 复用 Issue #36 fact/policy 字段和 stable lifecycle，失败 batch 不泄露半批 |
| #40 boundary | 不创建 `lineage_closure`，不实现 global/cross-program N-hop |
| security | 配置只用 placeholder；密码/Token/私钥/连接串不进入源码、DDL 或 evidence |
