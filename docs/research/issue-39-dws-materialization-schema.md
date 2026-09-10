# Issue #39：DWS 正式 Materialization Schema 与生命周期语义

> **状态：design / contract v0.1；不用于生产执行。**
>
> 本文把真实 DWS reconnaissance 转成公开的 schema contract，不连接真实 DWS，
> 不执行 migration，也不改变 `jobs/crontab/imp_lineage_edge.py` 的 runtime 行为。
> 配套设计稿 DDL 位于
> [`issue-39-dws-materialization-v0.1.sql`](issue-39-dws-materialization-v0.1.sql)，
> lifecycle 的机器可读矩阵位于
> [`issue-39-dws-lifecycle-matrix.json`](issue-39-dws-lifecycle-matrix.json)。

## 1. 边界与事实核验

本轮是语义纠偏，不新增 lineage 算法。PR #15 已建立 Phase 5 的
program-scoped TMP collapse，PR #51 只优化了该既有 materialization 的实现；
Issue #39 只冻结 DWS projection，Issue #40 才负责未来的跨 program/global N-hop
closure。PR #82 将 DWS 拆成 raw physical `lineage_edge` 与
`lineage_business_edge`，与现有 runtime contract 不一致；本轮移除这个 semantic
split。

已确认的 target contract：

| 项目 | contract |
| --- | --- |
| Engine | GaussDB 8.1.3 / PostgreSQL 9.2.4 compatible |
| Database | `czcb` |
| target schema | `dwp` |
| encoding | UTF8 |
| current schema | `public`，因此生产 DDL/SQL 必须显式写 `dwp.<table>` |
| existing storage | ROW / COLUMN 均已存在 |
| existing distribution | HASH / ROUND ROBIN 均已存在 |
| v0.1 tuning | 不做查询局部性的复杂 distribution tuning；技术主键 HASH |
| DWO | 本 Issue 不支持 |
| closure | `lineage_closure` 属于 Issue #40，本 Issue 不创建 |

### 1.1 现有 runtime contract

以下事实来自 `shared/lineage/physical_dag.py`、
`shared/lineage/materialization.py`、`shared/lineage/domain.py`、
`shared/lineage/materialization_sqlite.py`、`jobs/crontab/imp_lineage_edge.py` 及
Phase 5 tests；本轮不重写它们：

```text
ProgramPhysicalDAG:
    A -> TMP1 -> TMP2 -> B

Phase 5 materialization / LineageEdge:
    A -> B
```

- `ProgramPhysicalDAG` 保留程序内部完整 physical facts，包括 TMP、cycle、
  self-reference 和 orphan branch；parser/builder 不负责 collapse。
- Phase 5 以程序为 scope，从 formal asset 的 outgoing edge 开始穿过 TMP，第一次
  遇到 formal asset 就生成一条 `LineageEdge` 并停止该路径。
- 因而 TMP collapse 不跨越 formal asset，不产生 `A -> C` 这样的 transitive edge。
- `LineageEdge` 是正式 formal-to-formal direct business fact；其两个 endpoint 必须
  是明确的 `environment + schema.table` `DatasetIdentity`，TMP 不得作为 endpoint。
- SQLite adapter 和 cron 入口已经复用该结果；它们保存 batch/current/history，
  但不提供 raw PhysicalEdge DWS writer。

Required examples：

```text
A -> TMP1 -> TMP2 -> B
=> LineageEdge: A -> B

A -> FORMAL_B -> TMP -> FORMAL_C
=> LineageEdge: A -> FORMAL_B
                 FORMAL_B -> FORMAL_C
=> 不生成 A -> FORMAL_C
```

### 1.2 本轮 DWS model

```text
ProgramSource
    │
    ├─ build ProgramPhysicalDAG
    │       A -> TMP1 -> TMP2 -> B
    │       (TMP / cycle / orphan 保留在 runtime)
    │
    ├─ existing Phase 5 TMP collapse
    │       A -> B
    │       (formal direct LineageEdge)
    │
    └─ publish dwp.lineage_edge
            (同一 formal direct semantic；TMP endpoint forbidden)

future Issue #40
    └─ dwp.lineage_closure：跨 program/global N-hop derived index
```

DWS v0.1 只包含四张表：`lineage_batch`、`lineage_program_state`、
`lineage_edge`、`lineage_issue`。不创建 `dwp.lineage_business_edge`，不创建
raw PhysicalEdge table，也不把 ProgramPhysicalDAG 当成 DWS fact table。DWS
`lineage_edge` 是 current `LineageEdge` 的正式 direct projection；physical path
和 TMP 只可作为受控 provenance/evidence 保留。

## 2. Identity / key contract

所有 DWS facts 都同时保存物理行 key 与跨 batch stable identity。
`lineage_batch` 是 control fact，也按同一规则保存 `row_key` 与 `batch_id`。

### 2.1 Canonical serialization

stable key 由 writer 在 DDL 外生成，使用 UTF8、固定字段顺序、US（U+001F）separator、
SHA-256 lowercase hex（64 个 hex 字符；列预留 `VARCHAR(128)`）。不得使用
Python `hash()`、数据库自增 id、`repr()` 或 batch 内随机 UUID 作为 stable identity。
示意：

```text
program_key = sha256("program" || US || environment || US || source_profile || US || program_name)
edge_key    = sha256("lineage-edge" || US || environment || US || source_profile || US || program_key || US || source_dataset || US || target_dataset)
row_key     = sha256("row" || US || table_name || US || batch_id || US || stable_identity_key)
dataset_key = sha256("dataset" || US || environment || US || canonical_schema || US || canonical_table)
```

`edge_key` 表示当前 formal direct `LineageEdge` identity，不表示某一条 TMP
physical path；TMP label、path sample、batch、时间和 evidence 不进入 stable key。
实际实现必须对长度、UTF8 编码和 null 处理使用单一 shared helper；上面是 contract
而不是要求本 Issue 新增 helper。

### 2.2 每类 key 的边界

| 对象 | physical row key | stable identity key | stable identity 不包含 |
| --- | --- | --- | --- |
| batch | `row_key`，可以由 batch 生成 | `batch_id` | active 状态、时间、计数 |
| program state | `row_key`，含 batch | `program_key = environment + source_profile + program_name` | source hash、pipeline、batch |
| lineage edge | `row_key`，含 batch | `edge_key = program identity + formal source dataset + formal target dataset` | TMP path、batch、时间、evidence、job provenance |
| issue | `row_key`，含 batch | `stable_issue_key`，由 issue type 与稳定 node/branch 语义组成 | message、severity policy、时间、batch |

`program_id` 不作为第二套 identity 引入。#38 已冻结的 canonical program identity
在 DWS 中用 `program_key` 表示；`program_name` 同时保存用于展示和审计。若未来
外部系统提供另一个权威 program id，必须另立 contract，不能把它悄悄塞入
`program_key`。

### 2.3 Dataset boundary

- `DatasetIdentity` 严格是 `environment + canonical_schema + canonical_table`；
- `lineage_edge` 两端只接受明确的两段 `schema.table`，schema 不得被猜测；
- `source_dataset_key` / `target_dataset_key` 是 DatasetIdentity 的稳定技术投影，
  不是 Dataset Registry；
- TMP、缺 schema 或 unresolved node 不能形成 `LineageEdge`，只保留在
  `ProgramPhysicalDAG` 和 bounded evidence/issue；
- environment 是 hard boundary；source_profile 是 #38 的 Program identity 和
  collection provenance boundary，不能在不同 profile 间误合并 program facts。

## 3. 四张 DWS 表

DDL 的列定义是下面语义的机器可读表达；每张 fact 表都遵守 append-by-batch、
active switch 和显式 history 读取规则。

### 3.1 `dwp.lineage_batch`

这是 atomic publish 的 control table，不由事实行反推。关键字段：

- `batch_id`：一次 candidate snapshot 的 stable batch identity；不是 runtime Run；
- `snapshot_mode`：`FULL` 或 `PARTIAL`；`complete_snapshot` 与 scope 一起决定
  disappearance authority；
- `snapshot_scope`：canonical JSON text，记录本批完整扫描的
  `environment/source_profile` scope；不使用默认 schema 或 search path；
- `pipeline_version`：#38 的 semantic version；不是 Git SHA；
- `program_count`、`edge_count`、`issue_count`：publish 前后必须与同一 batch 的
  事实行数相等；空 edge 的成功 batch 也必须有 `lineage_batch` 行；
- `publish_status`：candidate 写入和 active switch 的控制状态。失败 publish 不以
  半批 `FAILED` 行留在 facts 中；失败运行摘要另用 observability contract；
- `is_active`：当前 snapshot 标记。正常状态最多一个 active batch，也允许首次
  publish 前没有 active batch。

`edge_count` 只统计 `dwp.lineage_edge` formal direct rows，不统计
`ProgramPhysicalDAG` 的 raw physical edges。

### 3.2 `dwp.lineage_program_state`

每个 active snapshot 中每个当前可见 ProgramIdentity 一行：

- `program_key` 是稳定 identity；`program_name` 不被 basename 或 target 猜测
  改写；
- `source_hash` 与 `pipeline_version` 用于 `UNCHANGED` / `CHANGED` 判定；
- `first_seen_at` 跨正常增量复用；`last_seen_at` 每个新 snapshot 更新；
- `last_changed_at` 在 source hash 或 pipeline version 语义变化时更新；
- complete scoped snapshot 中消失的 program 不进入新 active candidate，历史 state
  保留；partial snapshot 不得判定 scope 外 `DELETED`；restore 按 #38 的规则作为
  新 active observation，不猜 rename；
- `lineage_program_state` 不表示 scheduler run、worker、attempt 或实际执行成功。

### 3.3 `dwp.lineage_edge`

`lineage_edge` 是 current `LineageEdge` semantic 的正式 DWS projection：

| 分组 | 字段 | 语义 |
| --- | --- | --- |
| key | `row_key`, `edge_key` | 物理行与跨 batch formal direct identity |
| scope | `environment`, `source_profile` | environment hard boundary、profile provenance |
| program | `program_key`, `program_name` | #38 Program identity projection |
| endpoints | `source_dataset_key`, `source_table`, `target_dataset_key`, `target_table` | 两端都是明确的 formal `DatasetIdentity`；TMP endpoint forbidden |
| provenance | `evidence_type`, `evidence_json`, `source_hash`, `pipeline_version` | existing Phase 5 的 bounded collapse/path evidence；禁止完整 script |
| lifecycle | `batch_id`, `observed_at`, `first_seen_at`, `last_seen_at`, `last_changed_at`, `is_active`, `created_at`, `updated_at` | current/history 与 diff/replay |

一行表示一个程序内已经 materialize 的 formal direct fact。多条 physical path
合并到同一 `LineageEdge` 的 deterministic evidence；不为每个 TMP hop 或 path
创建 DWS edge row。`ProgramPhysicalDAG` 中的 cycle、self-reference、orphan
仍由 runtime audit 和 `lineage_issue` 表达，不被 DDL 静默丢弃或提升为 TMP endpoint。

`lineage_edge` 的 formal stable identity 不因 TMP rename、physical route 变化、
source hash/pipeline rebuild 或 evidence 顺序变化而自动改变；若 formal source、
formal target 或 program identity 改变，才是新的 stable direct fact。derived evidence
和 `last_seen_at` / `updated_at` 可以刷新，`last_changed_at` 只表示 formal direct
semantic identity 的建立或真正变化。

### 3.4 `dwp.lineage_issue`

Issue 表保存 Issue #36 `AuditFact` 及 `AuditPolicyResult` 的兼容 persistence
projection，用于 Physical DAG audit 和已完整分类的 materialization negative
result。不可验证的 materialization failure 随 publish transaction rollback，不在
这张事实表中留下半批 issue；失败运行摘要另用 observability contract。

关键字段：

- `stable_issue_key` 是跨 batch lifecycle key，不能依赖 message、evidence、
  confidence、rule_version、severity、disposition、时间或 Python hash；
- `program_key` / `program_name` 使 issue 与 ProgramIdentity 对齐；`node_key` /
  `branch_sink` 可以保留 TMP physical 证据；
- fact 字段使用 Issue #36 已关闭的 contract：`issue_type`、`confidence`、
  `rule_version`、message/evidence 和 stable identity；
- policy projection 字段为 `severity`、`disposition`、`policy_version`；
  `disposition` 只使用 `OPEN`、`ACCEPTED`、`FALSE_POSITIVE`、`RESOLVED`，不把
  policy 结果写回 fact identity；
- `disposition_updated_at` / `disposition_updated_by` 记录人工处置 provenance，
  可以为空；人工处置通过不可变的新 batch/history projection 记录，不原地改写旧 row；
- `IssueLifecycleStatus` 的 `NEW` / `PERSISTING` / `RESOLVED` 是跨 snapshot 的
  reconciliation 结果，不等于 `IssueDisposition`；DWS v0.1 不新增
  `lifecycle_status` 列或新的 enum；
- `first_seen_at` / `last_seen_at` / `last_changed_at` 与 `is_active` 用于
  current/history，不能因为 active switch 物理删除旧 issue；
- `evidence_json` 使用 deterministic JSON text，不保存完整源码、凭据或连接串。

## 4. DWS physical design

### 4.1 ROW / COLUMN 选择

| 表 | v0.1 orientation | 选择原因 | 代价与监控 |
| --- | --- | --- | --- |
| `lineage_batch` | ROW | 小表、单 active lookup、publish 状态切换和计数校验 | 不用于大扫描；无需 COLUMN |
| `lineage_program_state` | ROW | current state 按 identity/profile 查询，增量复用和 active switch 是窄写 | 历史量大后按 `last_seen_at` 分区；关注更新放大 |
| `lineage_edge` | COLUMN | 预计最大、append-by-batch，按 source/target/environment 扫描、审计和 direct graph 查询 | 单节点极低延迟点查不一定优于 ROW；索引和真实 workload 必须基准验证 |
| `lineage_issue` | ROW | issue scope、stable key、active/history 和 policy review 以窄行读取为主 | evidence JSON 较宽，按 scope/index 读取，不把全文 evidence 当索引键 |

不因为撤销 semantic split 而改变已经合理冻结的 `lineage_edge` COLUMN 选择。
这不是机械复制 SQLite schema：SQLite 的 `id INTEGER AUTOINCREMENT`、JSON text
和本地索引都不直接成为 DWS physical contract。

### 4.2 Distribution

四张表统一：

```sql
DISTRIBUTE BY HASH (row_key)
```

理由：`row_key` 是由 batch + stable identity 形成的 cryptographic technical key，
在 batch bulk load 时可把行分散到节点；不按 `batch_id` 分布，避免一次 publish 把
所有行压到单一 hash bucket；不按 `source_table` 分布，避免高频 hub source
skew；不按 `environment` / `source_profile` 分布，避免单 scope 偏斜。

潜在风险：

- 极小的 `lineage_batch` HASH 表有分布管理开销，但 v0.1 优先统一策略；
- 如果 row key writer 退化为顺序值或非均匀值，会产生 skew，必须以 DWS skew
  diagnostics 监控；
- 按 source/target 查找可能需要跨 DN 访问，索引和真实 workload 再调；本 Issue
  不引入复杂 distribution key；
- 每张事实表都冻结为 `PRIMARY KEY (row_key)`，并对同一 batch 内的 stable identity
  建立 `UNIQUE (batch_id, stable_key)` 等价约束；`lineage_batch.batch_id` 唯一，
  active batch 通过 filtered unique index 保证最多一个；同一 stable identity 可以
  在不同历史 batch 重复出现；
- `(batch_id, stable_key)` 的 logical uniqueness 不一定与 `row_key` 分布共址，
  因此 writer 必须在 active switch 前再次做 candidate duplicate validation，
  不能只依赖 DWS constraint。

### 4.3 Partition 与 retention

- `lineage_batch`：不分区，小 control table；
- `lineage_edge`：按 `observed_at` 做 monthly/approved rolling range partition；
- `lineage_program_state` / `lineage_issue`：按 `last_seen_at` 做 rolling range
  partition，保证 active/history 生命周期与 retention 对齐；
- DDL 中的 seed/max partition 只是 design placeholder，真正上线前必须由 DWS
  owner 创建目标月份边界，不得让写入落入未管理的默认分区；
- retention 采用按时间分区的 rolling policy，但 v0.1 不把具体月份写死在 schema；
  部署配置必须明确 history horizon，并至少覆盖 active batch、上一成功 snapshot
  的 rollback window 和正在进行的对账窗口；
- retention 只允许删除已退休且已超出 horizon 的完整历史分区，不能删除 active
  batch；`lineage_issue` 的保留期不得短于其关联事实的对账需要，若 #36 另有更长
  保留要求则取更长者；
- failed candidate 由 transaction rollback 清理，不靠 retention 清半批。

## 5. Publish / Snapshot contract

### 5.1 一个 batch 的 consistency boundary

`lineage_batch.batch_id` 是唯一 consistency boundary。一次 successful publish 的
candidate 必须满足：

```text
ProgramSource
  → ProgramPhysicalDAG / audit
  → existing Phase 5 TMP collapse
  → formal LineageEdge rows(batch = B)
  → lineage_edge rows(batch = B)
  → lineage_program_state / lineage_issue rows(batch = B)
  → validate counts, formal DatasetIdentity endpoints and stable identities
  → one active switch
```

active 查询应同时约束 `fact.is_active = TRUE` 和 `dwp.lineage_batch.is_active = TRUE`，
并按 `fact.batch_id = batch.batch_id` join，而不是相信某一张事实表的 flag 单独正确。

推荐的 publish 事务语义（示意，不是本 Issue 的 runtime implementation）：

```text
BEGIN
  insert dwp.lineage_batch(B, inactive/candidate)
  insert dwp.lineage_program_state(B, inactive)
  insert dwp.lineage_edge(B, inactive)
  insert dwp.lineage_issue(B, inactive)
  validate same batch_id, counts, formal endpoints and stable uniqueness
  deactivate previous dwp.lineage_batch and all three fact tables
  activate B in dwp.lineage_batch and all three fact tables
COMMIT
```

任何 build、audit、TMP collapse、insert、validation 或 active switch 失败都
`ROLLBACK`。上一成功 snapshot 必须继续完整可读；不得留下半批 active data。

### 5.2 Build success / collapse result

| 情况 | ProgramPhysicalDAG/runtime | `lineage_edge` candidate | publish |
| --- | --- | --- | --- |
| DAG/build 成功，现有 TMP collapse 成功 | 完整 physical facts 可供 audit/evidence 使用 | 安全 formal direct rows 写入 B | 允许，同一 B 原子切换 |
| 已知 orphan/cycle/self-reference，audit 能完整分类 | physical facts 保留，诊断进入 issue | 受影响 formal row 缺失，不猜测；valid direct rows 仍属于 B | 仅当结果完整且 deterministic；允许 publish |
| TMP collapse 抛错、路径遍历超限、无法证明结果完整 | 可只存在于内存或 candidate transaction 内 | 不得用不完整结果冒充 B | fail closed，整个 B rollback，不替换旧 active |
| physical build/audit 失败 | 不 publish | 不 publish | fail closed |

`lineage_edge` 只接收现有 `LineageEdge` 的 formal direct output；不允许用 raw
PhysicalEdge 行填充它，也不执行第二套 business collapse 算法。

### 5.3 Empty edge success

完整 snapshot 可以成功但没有任何 formal direct edge：

```text
lineage_batch(B).is_active = TRUE
edge_count                = 0
program_count / issue_count 按实际 candidate 计数
```

不能因为没有 edge 就不写 batch，也不能用上一 batch 的 edge 伪装当前 active
snapshot。若是 partial snapshot，则 scope 外 facts 仍必须 rebase 到 B。

### 5.4 Incremental reuse / source change / pipeline rebuild

- `source_hash` 非空且与 active `program_state` 相同、`pipeline_version` 相同：
  `UNCHANGED`，formal `LineageEdge` / state / issue facts rebase 到新 batch；stable
  keys 不变，`row_key` 因 batch 变化；
- `source_hash` 变化、缺失或 pipeline version 变化：`CHANGED`，同一 ProgramIdentity
  重建 Physical DAG、audit 和 Phase 5 materialization；不能只刷新一层；
- rebuild 输出相同 formal direct key：更新当前 batch 的 evidence/provenance 与
  `last_seen_at`，按稳定 direct identity 保留 `last_changed_at`；DWS 不增加
  `path_count` 专用列，也不以 bounded sample 长度替代完整规模；
- pipeline semantic version 变化必须能触发 rebuild，即使 source hash 相同；
- `job_key` 不进入 DWS stable identity，不能用它决定 reuse。

### 5.5 Program disappearance / restore

- `FULL + complete_snapshot + explicit scope` 才有 scoped disappearance authority；
- scope 内未出现的 program 及其 formal direct/state/issue facts 不进入新 active
  candidate，但旧 batch 保留；
- `PARTIAL`、limit replay、provider error 或 scope 外缺失一律不能判定 deleted；
  未读取 profile/program 的 active facts 原样 rebase；
- restore 不通过名字/hash 相似度猜 rename；按 #38 作为新的 active observation，
  history 仍可按旧 batch 读取。

### 5.6 Rollback

如果 B 在 TMP collapse、校验或 switch 阶段失败：

```text
B 的 candidate rows      = rollback 后不存在/不可见
previous active batch    = 仍是完整 active snapshot
lineage_edge / issue     = 不会分裂或跨 batch active
```

不执行“先切 edge、再补 issue”或任何两阶段公开状态。

## 6. Active / history 查询 contract

业务门户和直接上下游查询都直接消费当前 `dwp.lineage_edge`；不能在 query-time
递归 TMP，也不能把 raw PhysicalDAG 当成当前 DWS edge：

```sql
SELECT e.source_table, e.target_table, e.program_key
FROM dwp.lineage_edge AS e
JOIN dwp.lineage_batch AS b
  ON b.batch_id = e.batch_id
 AND b.is_active = TRUE
WHERE e.is_active = TRUE
  AND e.environment = :environment
  AND (:source_profile IS NULL OR e.source_profile = :source_profile);
```

上例中的 `dwp.` 不是可选风格；`current_schema=public` 时禁止依赖 `search_path`。
`source_table` / `target_table` 已经是 formal `LineageEdge` endpoint，不需要通过
递归 TMP 才能得到直接关系。未来 Issue #40 的跨 program/global N-hop 查询必须从
明确的 direct-edge contract 构建 `lineage_closure`，不在本 Issue 偷换实现。

History 查询按显式 `batch_id` 读取，不把旧 batch 的 inactive edge 混入 active
projection。任何只写 `WHERE is_active = TRUE` 而不 join active batch 的查询，均视为
contract violation。

## 7. SQLite → DWS compatibility matrix

SQLite 继续是 public/demo reference adapter，不是隐式 production contract。它已经
保存与 DWS v0.1 同语义的 formal `LineageEdge`，但没有真实 DWS writer。PR #82 的
raw physical split 不是兼容目标；不存在的 `dwp.lineage_business_edge` 也不属于
本版 matrix。

| DWS table | compatible | transformed | intentionally incompatible | production-only | future |
| --- | --- | --- | --- | --- | --- |
| `lineage_batch` | `batch_id`、`observed_at`、`published_at`、`edge_count`、`issue_count`、active snapshot | SQLite `id`/implicit row identity → `row_key`；新增 snapshot mode/scope、pipeline version 与显式 control metadata | SQLite `edge_count` 不统计 ProgramPhysicalDAG raw edges；不拆出第二种 edge count | DWS ROW/HASH、显式 `dwp.`、partition、publish gate metadata | retention/failed-run observability 可能另立表 |
| `lineage_program_state` | environment、source_profile、program_name、source_hash、pipeline_version、first/last seen、last changed、batch、active | `id INTEGER AUTOINCREMENT` → `row_key`；三元组 → `program_key` | 不把 batch/runtime run 或 job key 当 program identity | DWS distribution、partition、active-batch join | 外部权威 program id 需独立 contract |
| `lineage_edge` | SQLite formal `LineageEdge` 的 environment/profile、program provenance、formal source/target、evidence、source_hash、batch、observed/active；两端同样禁止 TMP | SQLite `id` → `row_key`；runtime formal identity/evidence → DWS `edge_key`、DatasetIdentity keys 与 bounded `evidence_json` | 不把 `PhysicalEdge`、ProgramPhysicalDAG raw row 或 TMP endpoint 改名写入 DWS `lineage_edge`；不新增 raw physical writer | COLUMN/HASH、DWS formal endpoint validation、partition、active-batch join | DWS adapter/backfill 的具体实现另立 Issue |
| `lineage_issue` | environment/profile/program、issue type/message/evidence、stable/first/last/active、batch；以及 #36 fact/policy projection 字段 | `id` → `row_key`；nullable `stable_key` → production `stable_issue_key`；evidence canonicalization；旧 SQLite 缺失 policy 字段按 adapter legacy fallback 读取 | SQLite 的 `IssueLifecycleStatus` reconciliation 不是 DWS 新列；不把 runtime 旧扁平 projection 当成新的 fact/policy identity | #36 的 fact/policy 字段、人工 disposition provenance、DWS active/history publish validation | DWS adapter/backfill 的具体实现另立 Issue |
| `lineage_closure` | 无 | 无 | 本 Issue 不创建、不把 closure 混入 `lineage_edge` | 无 | Issue #40 future cross-program/global N-hop derived index |

## 8. Tests / contract lint scope

本轮测试只验证 design/contract，不连接真实 DWS，也不执行 DDL：

- DDL 只声明四张 `dwp` 表，且无 `lineage_business_edge` / `lineage_closure`；
- `lineage_edge` 的 row/stable key、formal DatasetIdentity endpoints、生命周期、
  HASH distribution 与 COLUMN orientation；
- lifecycle JSON 的 same-batch、empty success、rollback、duplicate key、profile
  isolation、inactive contamination、partial/full disappearance、rebuild 与 restore；
- 现有 `tests/shared/test_lineage_materialization.py` 继续验证：
  `A -> TMP1 -> TMP2 -> B` 只产生 `LineageEdge A -> B`，以及
  `A -> FORMAL_B -> TMP -> FORMAL_C` 只产生两条 formal direct edge；
- 现有 SQLite targeted tests 继续覆盖 failed publish、历史保留、active query、
  duplicate identity 与 partial scoped deletion；
- 不因本 Issue 重跑无关 parser 全量测试，不修改 `imp_lineage_edge` runtime。

## 9. Frozen decisions

本轮把以下语义冻结为 #39 v0.1 contract：

| Decision | v0.1 frozen contract |
| --- | --- |
| runtime physical layer | `ProgramPhysicalDAG` 保留 TMP、cycle、self-reference、orphan 与 raw physical direct facts；本 Issue 不新增 raw PhysicalEdge DWS writer |
| DWS `lineage_edge` | 等于 current formal direct `LineageEdge` semantic；由现有 program-scoped TMP collapse 产出；两个 endpoint 必须是 formal DatasetIdentity；TMP endpoint forbidden |
| formal boundary | collapse 第一次遇到 formal asset 就停止；不跨 formal asset 产生 transitive edge；`A -> FORMAL_B -> TMP -> FORMAL_C` 不生成 `A -> FORMAL_C` |
| row/stable key | `row_key` 可含 batch 并标识物理行；`edge_key` 是 batch-independent formal direct identity；两者必须分离 |
| lifecycle | batch/current/history、source_hash、pipeline_version、partial/full snapshot、rollback、active-batch join 与 retained history 保留 |
| #36 policy alignment | 复用已 CLOSED 的 Issue #36：fact 保存 `issue_type`、`confidence`、`rule_version`、message/evidence/stable identity；policy projection 保存 `severity`、`disposition`、`policy_version`；人工处置保存 `disposition_updated_at` / `disposition_updated_by`，不新增 enum |
| evidence | existing runtime 的 bounded `evidence.path_count` 可以作为 runtime evidence 继续存在；DWS v0.1 不增加专用 `path_count` 列，也不以 sample 长度替代完整规模 |
| materialization failure | collapse 抛错、超限、超时、结果不完整或无法验证时 fail closed：candidate transaction 全部 rollback，旧 active snapshot 保持不变；只有完整、可验证的 negative result 才能带同 batch issue publish |
| physical design | 已冻结的 ROW/COLUMN 与 `DISTRIBUTE BY HASH (row_key)` 尽量保留；`lineage_edge` 继续 COLUMN；本轮不执行真实 DWS DDL |
| #40 boundary | `lineage_closure` 是 future cross-program/global N-hop derived index；本 Issue 不实现 closure、不改变当前 BFS/runtime query contract |
| non-production proof | GaussDB 8.1.3 的 DDL、partition、partial unique index 和 distributed unique 行为必须在非生产环境验证；这不改变本轮逻辑 contract，也不连接真实 DWS |

本轮明确移除 PR #82 引入的 semantic drift：不再将 `lineage_edge` 定义为 raw
Physical Direct Edge，不再要求独立的 `lineage_business_edge`，不新增 BUSINESS_CLOSURE
runtime，不修改 parser、Audit semantics、cron 入口或 SQLite runtime。
