# Issue #39：DWS 正式 Materialization Schema 与生命周期语义

> **状态：design / contract v0.1；不用于生产执行。**
>
> 本文把真实 DWS reconnaissance 转成公开的 schema contract，不连接真实 DWS，
> 不执行 migration，也不改变 `jobs/crontab/imp_lineage_edge.py` 的 runtime 行为。
> 配套设计稿 DDL 位于
> [`issue-39-dws-materialization-v0.1.sql`](issue-39-dws-materialization-v0.1.sql)，
> lifecycle 的机器可读矩阵位于
> [`issue-39-dws-lifecycle-matrix.json`](issue-39-dws-lifecycle-matrix.json)。

## 1. 真实边界与 Issue 更新

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

本轮对 Issue #39 的更新：

1. 增加正式的 `dwp.lineage_business_edge` derived materialization contract；
2. 澄清 TMP：可以是 `lineage_edge` 的 physical node / endpoint，但不得是
   `lineage_business_edge` 的 formal business endpoint；
3. `lineage_edge` 在本 DWS production contract 中是 Physical Direct Edge，保留
   程序内部完整 DAG；`lineage_business_edge` 才是供门户和普通业务上下游使用的
   collapse projection；
4. 不把 business collapse 算法、parser、OpenLineage、column lineage 或
   `lineage_closure` 实现塞进 #39；
5. 当前 SQLite reference adapter 继续保持现有 runtime 语义，不被隐式改造成
   DWS production contract。适配迁移需要独立实现 Issue。

## 2. 三层模型

```text
ProgramSource
    │
    ├─ build physical DAG
    │       DWF.A → TMP_A → TMP_B → DWUPRR.RESULT_A
    │
    ├─ materialize dwp.lineage_edge             (source of truth)
    │       DWF.A → TMP_A
    │       TMP_A → TMP_B
    │       TMP_B → DWUPRR.RESULT_A
    │
    ├─ derive dwp.lineage_business_edge         (same batch)
    │       DWF.A ─────────────────────────────→ DWUPRR.RESULT_A
    │
    └─ future Issue #40: derive lineage_closure from business lineage
```

### 2.1 Physical lineage：`lineage_edge`

`lineage_edge` 是底层 Physical Direct Edge source of truth：

- 一行表示一个程序内已经观察到的 direct physical edge，方向固定为
  `source = upstream`、`target = downstream`；
- TMP / intermediate table 可以作为任意一端；
- cycle、self-reference、orphan branch、unresolved node 不在表层静默删除，
  由 physical edge 加 `lineage_issue` 一起保留；
- `edge_key` 只由 physical direct edge 的稳定语义生成，不包含 batch、时间、
  evidence 或随机 `job_key`；
- 物理节点的 `source_table` / `target_table` 是 node label：正式节点必须保留
  `schema.table`，TMP 也必须保留其原有 schema boundary；禁止 basename-only、
  默认 schema 或 fuzzy 合并；
- 这张表不承担递归查询或 closure 的预计算职责。

`lineage_edge` 的物理 endpoint 不等于 DatasetIdentity endpoint。formal node 可以
投影出 `source_dataset_key` / `target_dataset_key`；TMP 或缺 schema 的节点对应
DatasetIdentity 为 `NULL`，但其 physical label 仍然落库。

### 2.2 Business lineage：`lineage_business_edge`

`lineage_business_edge` 是 **Derived Materialization**，不是 source of truth：

- 从一个程序的当前 physical DAG collapse 得到；
- 只接受可安全证明的 formal `schema.table` source/target；TMP 不得落为 formal
  endpoint；
- 在同一程序中，沿 TMP 继续走，遇到第一个 formal node 就停止；这不是跨
  formal asset 的全量 transitive closure；
- 同一 `business_edge_key` 的多条 physical path 只 materialize 一行，path 的
  统计是派生属性；
- 可从同一 batch 的 `lineage_edge` 重新构建，因此不替代 physical source of
  truth；
- 资产门户、直接业务上下游和后续 impact/closure 默认消费这一层，不必在
  query-time 递归 TMP DAG；
- 无法安全 collapse 时不猜测、不把 TMP 提升为业务资产；可以没有 business row，
  由同 batch 的 diagnostic `lineage_issue` 表达。

### 2.3 Closure：Issue #40 前置 contract

本 Issue 只约定：未来 closure 应基于 `lineage_business_edge` 构建跨程序 N 层关系。
不创建 `dwp.lineage_closure`，不为 closure 冻结额外字段，也不把当前 SQLite 的
BFS API 偷换成 DWS closure 表。

## 3. Identity / key contract

所有 facts 都同时保存物理行 key 与跨 batch stable identity。`lineage_batch` 是
control fact，也按同一规则保存 `row_key` 与 `batch_id`。

### 3.1 Canonical serialization

stable key 由 writer 在 DDL 外生成，使用 UTF8、固定字段顺序、US（U+001F）separator、
SHA-256 lowercase hex（64 个 hex 字符；列预留 `VARCHAR(128)`）。不得使用
Python `hash()`、数据库自增 id、`repr()` 或 batch 内随机 UUID 作为 stable identity。
示意：

```text
program_key       = sha256("program"       || US || environment || US || source_profile || US || program_name)
edge_key          = sha256("physical-edge" || US || environment || US || source_profile || US || program_key || US || source_node || US || target_node)
business_edge_key = sha256("business-edge" || US || environment || US || source_profile || US || program_key || US || source_dataset || US || target_dataset)
row_key           = sha256("row"           || US || table_name || US || batch_id || US || stable_identity_key)
dataset_key       = sha256("dataset"       || US || environment || US || canonical_schema || US || canonical_table)
```

实际实现必须对长度、UTF8 编码和 null 处理使用单一 shared helper；上面是 contract
而不是要求本 Issue 新增 helper。

### 3.2 每类 key 的边界

| 对象 | physical row key | stable identity key | stable identity 不包含 |
| --- | --- | --- | --- |
| batch | `row_key`，可以由 batch 生成 | `batch_id` | active 状态、时间、计数 |
| program state | `row_key`，含 batch | `program_key = environment + source_profile + program_name` | source hash、pipeline、batch |
| physical edge | `row_key`，含 batch | `edge_key = program identity + physical source node + physical target node` | batch、时间、evidence、随机 job key |
| business edge | `row_key`，含 batch | `business_edge_key = program identity + formal source dataset + formal target dataset` | TMP path、batch、时间、path sample |
| issue | `row_key`，含 batch | `stable_issue_key`，由 issue type 与稳定 node/branch 语义组成 | message、severity policy、时间、batch |

`program_id` 不作为第二套 identity 引入。#38 已冻结的 canonical program identity
在 DWS 中用 `program_key` 表示；`program_name` 同时保存用于展示和审计。若未来
外部系统提供另一个权威 program id，必须另立 contract，不能把它悄悄塞入
`program_key`。

### 3.3 Dataset boundary

- `DatasetIdentity` 仍严格是 `environment + canonical_schema + canonical_table`；
- formal endpoint 只接受明确的两段 `schema.table`，schema 不得被猜测；
- `source_dataset_key` / `target_dataset_key` 仅是该 identity 的稳定技术投影，
  不是 Dataset Registry；
- physical TMP / unresolved node 的 dataset key 为 `NULL`，不因此删除 physical
  row；
- business edge 的 source/target 必须同时有 dataset key，且不得是 TMP；
- environment 是 hard boundary；source_profile 是 #38 中 Program identity 和
  collection provenance 的边界，不能在不同 profile 间误合并 program facts。

## 4. 五张 DWS 表

DDL 的列定义是下面语义的机器可读表达；下列生命周期字段在每张 fact 表中都
遵守 append-by-batch、active switch 规则。

### 4.1 `dwp.lineage_batch`

这是 atomic publish 的 control table，不由事实行反推。关键字段：

- `batch_id`：一次 candidate snapshot 的 stable batch identity；不是 runtime Run；
- `snapshot_mode`：`FULL` 或 `PARTIAL`；`complete_snapshot` 与 scope 一起决定
  disappearance authority；
- `snapshot_scope`：canonical JSON text，记录本批完整扫描的
  `environment/source_profile` scope；不使用默认 schema 或 search path；
- `pipeline_version`：#38 的 semantic version；不是 Git SHA；
- `program_count`、`physical_edge_count`、`business_edge_count`、`issue_count`：
  publish 前后必须与同一 batch 的事实行数相等；空 edge 的成功 batch 也必须有
  `lineage_batch` 行；
- `publish_status`：candidate 写入和 active switch 的控制状态。失败 publish 不
  以半批 `FAILED` 行留在 facts 中；需要失败运行日志时使用独立 observability
  contract，不扩充本 Issue 的事实表；
- `is_active`：当前 snapshot 标记。正常状态最多一个 active batch，也允许首次
  publish 前没有 active batch。

### 4.2 `dwp.lineage_program_state`

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

### 4.3 `dwp.lineage_edge`

列分组：

| 分组 | 字段 | 语义 |
| --- | --- | --- |
| key | `row_key`, `edge_key` | 物理行与跨 batch physical identity |
| scope | `environment`, `source_profile` | environment hard boundary、profile provenance |
| program | `program_key`, `program_name` | #38 Program identity projection |
| endpoints | `source_table`, `target_table`, `source_node_kind`, `target_node_kind` | 保留完整 physical node label；允许 TMP、self-reference、cycle、unresolved |
| dataset projection | `source_dataset_key`, `target_dataset_key` | formal `schema.table` 才非空；不做默认 schema inference |
| provenance | `evidence_type`, `evidence_json`, `source_hash`, `pipeline_version` | 轻量 statement/path provenance；禁止完整 script |
| lifecycle | `batch_id`, `observed_at`, `first_seen_at`, `last_seen_at`, `last_changed_at`, `is_active`, `created_at`, `updated_at` | current/history 与 diff/replay |

同一程序内同一 physical direct pair 的多次 statement observation 应在一行中
聚合 evidence，而不是制造重复 `edge_key`。cycle/self-reference/orphan 是合法的
physical fact；诊断在 `lineage_issue`，不通过 DDL `CHECK` 静默删除。

### 4.4 `dwp.lineage_business_edge`

列分组：

| 分组 | 字段 | 语义 |
| --- | --- | --- |
| key | `row_key`, `business_edge_key` | row key 可含 batch；stable key 不含 TMP path |
| scope | `environment`, `source_profile` | 保留 collection profile，避免跨 profile program fact 合并 |
| program | `program_key`, `program_name` | 该 business edge 来自哪个 static program |
| dataset | `source_dataset_key`, `source_table`, `target_dataset_key`, `target_table` | 两端必须是明确的 formal `schema.table` |
| collapse | `collapse_depth`, `path_count` | 派生路径摘要，见 4.4.1；不是 identity |
| derivation | `physical_derivation_hash`, `pipeline_version`, `source_hash` | 能从同 batch physical rows 重建；TMP path 不进入 stable key |
| lifecycle | `batch_id`, `observed_at`, `first_seen_at`, `last_seen_at`, `last_changed_at`, `is_active`, `created_at`, `updated_at` | 与 physical facts 同批发布 |

#### 4.4.1 collapse 与去重口径

v0.1 的 proposed definition（其中带 `proposed` 的部分仍在 unresolved list 中）：

- 从一个程序的 physical DAG 中选择 formal source 到 formal target 的安全路径；
- 经过 TMP 时继续遍历，遇到第一个 formal target 即形成一条 business edge；
- direct formal-to-formal edge 的 `collapse_depth = 1`；
- `DWF.A → TMP1 → TMP2 → DWUPRR.R` 的 `collapse_depth = 3`，即 physical
  direct edge hop 数，**包含 source→TMP、TMP→target 的每一跳**；
- 多条路径按 distinct physical node sequence 去重后只写一行；
- `path_count` 如果保留，表示同一程序、同一 formal source/target、同一选定
  snapshot 内的 distinct safe physical path 数，而不是 sample 数，也不是
  `len(physical_paths)`；重复的 direct physical edge pair 先去重；
- `physical_derivation_hash` 对参与该 business edge 的 canonical physical direct
  edge set 做 hash，供 rebuild/lifecycle 使用，但绝不进入 `business_edge_key`；
- 不跨越另一个 formal asset 生成 transitive edge；不从 business edge 反推
  physical path；
- cycle、self-reference、orphan、missing schema、ambiguous sink 或无法证明
  complete collapse 时不猜测。受影响 business row 可以缺失，并在同 batch 写入
  diagnostic issue；若 collapse 返回的是**不完整/不可验证的失败**而不是一个明确
  的 negative result，publish gate 必须 fail closed（见第 6 节）。

`collapse_depth` 与 `path_count` 都不是 business identity。这样 TMP 改名不会改变
business_edge_key；但如果 physical derivation hash、depth 或 path count 变化，
可以在不改变 stable identity 的情况下记录 derived fact changed。

#### 4.4.2 TMP 改名与 physical path 变化

v0.1 proposed lifecycle：

1. TMP 改名而 formal source/target 不变：`business_edge_key` 不变；physical
   `edge_key` 会按 physical endpoint 变化；
2. `physical_derivation_hash` 包含参与 collapse 的 physical edge topology，因此
   TMP 改名或 topology 变化会被识别为 derived input change；
3. 如果 derivation hash、depth、path_count 和 pipeline version 都没有变化，纯
   evidence 顺序变化不更新 business `last_changed_at`；
4. 如果 derivation hash/depth/path_count 变化，则更新 business
   `last_changed_at`；`last_seen_at` 在每个 active batch 都更新；
5. source_hash 仅变化但 derived projection 完全一致时，program state 和
   physical provenance 仍然 changed，business stable identity 不变；business
   `last_changed_at` 是否跟随 source-only change，保持为 unresolved，默认不把
   source_hash 单独当作业务关系变化。

### 4.5 `dwp.lineage_issue`

Issue 表覆盖 physical audit 与 business collapse diagnostic；`issue_layer` 区分
来源层。不可验证的 publish gate failure 随 batch transaction rollback，不在这张
事实表中留下半批 publish issue；需要保留失败运行摘要时另用 observability
contract。关键字段：

- `stable_issue_key` 是跨 batch lifecycle key，不能依赖 message、时间、severity
  或 Python hash；
- `program_key` / `program_name` 使 issue 与 ProgramIdentity 对齐；`node_key` /
  `branch_sink` 可以保留 TMP physical 证据；
- `severity`、`disposition`、`rule_version`、`policy_version` 是为并行 Issue #36
  预留的 nullable compatibility slots；本 Issue 不冻结 #36 最终值、枚举或通知
  语义；
- `first_seen_at` / `last_seen_at` / `last_changed_at` 与 `is_active` 用于
  current/history。RESOLVED 可以由相邻 historical batch 推导，不能因为 active
  switch 物理删除旧 issue；
- `evidence_json` 使用 deterministic JSON text，不保存完整源码、凭据或连接串。

## 5. DWS physical design

### 5.1 ROW / COLUMN 选择

| 表 | v0.1 orientation | 选择原因 | 代价与监控 |
| --- | --- | --- | --- |
| `lineage_batch` | ROW | 小表、单 active lookup、publish 状态切换和计数校验 | 不用于大扫描；无需 COLUMN |
| `lineage_program_state` | ROW | current state 按 identity/profile 查询，增量复用和 active switch 是窄写 | 历史量大后按 `last_seen_at` 分区；关注更新放大 |
| `lineage_edge` | COLUMN | 预计最大、append-by-batch，按 source/target/environment 扫描、审计和重建 physical DAG | 单节点极低延迟点查不一定优于 ROW；索引和 batch filter 必须基准验证 |
| `lineage_business_edge` | ROW | 门户/上下游是 source/target 的窄查询，行宽小，derived batch 写入和 active 查询优先 | 若未来规模远超预期，再以真实 SLO 评估 COLUMN，不在 #39 预优化 |
| `lineage_issue` | ROW | issue scope、stable key、active/history 和 policy review 以窄行读取为主 | evidence JSON 较宽，按 scope/index 读取，不把全文 evidence 当索引键 |

这不是机械复制 SQLite schema：SQLite 的 `id INTEGER AUTOINCREMENT`、JSON text
和本地索引都不直接成为 DWS physical contract。

### 5.2 Distribution

五张表统一：

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
- `(batch_id, stable_key)` 的 logical uniqueness 不一定与 `row_key` 分布共址，
  因此 writer/publish validation 仍是必需的，不能只依赖 DWS constraint。

### 5.3 Partition 与 retention

- `lineage_batch`：不分区，小 control table；
- `lineage_edge` / `lineage_business_edge`：按 `observed_at` 做 monthly/approved
  rolling range partition；
- `lineage_program_state` / `lineage_issue`：按 `last_seen_at` 做 rolling range
  partition，保证 active/history 生命周期与 retention 对齐；
- DDL 中的 seed/max partition 只是 design placeholder，真正上线前必须由 DWS
  owner 创建目标月份边界，不得让写入落入未管理的默认分区；
- retention 只允许删除已退休历史分区，不能删除 active batch；事实与 issue 的
  retention 应保持可对账，#36 若要求更长 issue retention，以更长者为准；
- failed candidate 由 transaction rollback 清理，不靠 retention 清半批。

## 6. Publish / Snapshot contract

### 6.1 一个 batch 的 consistency boundary

`lineage_batch.batch_id` 是唯一 consistency boundary。一次 successful publish 的
candidate 必须满足：

```text
ProgramSource
  → physical DAG / audit
  → lineage_edge rows(batch = B)
  → business derivation(rows from the same B)
  → lineage_business_edge rows(batch = B)
  → lineage_program_state / lineage_issue rows(batch = B)
  → validate counts and stable identities
  → one active switch
```

禁止 `lineage_edge = N`、`lineage_business_edge = N-1` 的组合。active 查询应同时
约束 `fact.is_active = TRUE` 和 `dwp.lineage_batch.is_active = TRUE`，并按
`fact.batch_id = batch.batch_id` join，而不是相信某一张事实表的 flag 单独正确。

推荐的 publish 事务语义（示意，不是本 Issue 的 runtime implementation）：

```text
BEGIN
  insert dwp.lineage_batch(B, inactive/candidate)
  insert dwp.lineage_program_state(B, inactive)
  insert dwp.lineage_edge(B, inactive)
  insert dwp.lineage_business_edge(B, inactive)
  insert dwp.lineage_issue(B, inactive)
  validate same batch_id, counts, stable uniqueness, formal business endpoints,
           and derivation hashes
  deactivate previous dwp.lineage_batch and all four fact tables
  activate B in dwp.lineage_batch and all four fact tables
COMMIT
```

任何 build、audit、business derivation、insert、validation 或 active switch 失败都
`ROLLBACK`。上一成功 snapshot 必须继续完整可读；不得留下半批 active physical
或 business data。

### 6.2 Build success / collapse result

| 情况 | physical candidate | business candidate | publish |
| --- | --- | --- | --- |
| DAG/build 成功，所有 safe boundary collapse 成功 | 写入 B | 写入 B | 允许，同一 B 原子切换 |
| 有已知 orphan/cycle/self-reference，audit 能完整分类且不能安全生成某条业务边 | physical row 和 issue 写入 B | 受影响边缺失，不猜测；valid edges 仍属于 B | 仅当 derivation 返回 complete negative result；允许 publish |
| business collapse 抛错、路径遍历超限、无法证明结果完整 | physical 可在 candidate 中保留 | 不得用不完整结果冒充 B | fail closed，整个 B rollback，不替换旧 active |
| physical build 失败 | 不 publish | 不 publish | fail closed |

“允许 business edge 缺失”指有明确 diagnostic 的 safe negative result，不是允许
physical/business 使用不同 batch。对不可验证的 collapse failure，选择整个 batch
fail closed，优先保证 portal 不读到伪造的业务关系。

### 6.3 Empty edge success

完整 snapshot 可以成功但没有任何 edge：

```text
lineage_batch(B).is_active = TRUE
physical_edge_count       = 0
business_edge_count       = 0
program_count / issue_count 按实际 candidate 计数
```

不能因为没有 edge 就不写 batch，也不能用上一 batch 的 edge 伪装当前 active
snapshot。若是 partial snapshot，则 scope 外事实仍必须 rebase 到 B。

### 6.4 Incremental reuse / source change / pipeline rebuild

- `source_hash` 非空且与 active `program_state` 相同、`pipeline_version` 相同：
  `UNCHANGED`，physical/business/state/issue facts rebase 到新 batch；stable keys
  不变，`row_key` 因 batch 变化；
- `source_hash` 变化、缺失或 pipeline version 变化：`CHANGED`，同一 ProgramIdentity
  重建 physical 与 business 两层；不能只刷新一层；
- rebuild 输出 business key 相同但 derivation hash/depth/path_count 相同：保留
  business first/last-changed 语义（source-only change 的最终策略见 unresolved）；
- pipeline semantic version 变化必须能触发 rebuild，即便 source hash 相同；
- `job_key` 不进入 stable identity，不能用它决定 reuse。

### 6.5 Program disappearance / restore

- `FULL + complete_snapshot + explicit scope` 才有 scoped disappearance authority；
- scope 内未出现的 program 及其 physical/business/state facts 不进入新 active
  candidate，但旧 batch 保留；
- `PARTIAL`、limit replay、provider error 或 scope 外缺失一律不能判定 deleted；
  未读取 profile/program 的 active facts 原样 rebase；
- restore 不通过名字/hash 相似度猜 rename；按 #38 作为新的 active observation，
  history 仍可按旧 batch 读取。

### 6.6 Rollback

如果 B 在 collapse、校验或 switch 阶段失败：

```text
B 的 candidate rows      = rollback 后不存在/不可见
previous active batch    = 仍是完整 active snapshot
physical/business batch  = 不会分裂
```

不执行“先切 physical、再补 business”的两阶段公开状态。

## 7. Active / history 查询 contract

业务门户的默认查询必须走 `lineage_business_edge`，physical debug/audit 才走
`lineage_edge`。两者都必须显式限定 environment，并绑定同一个 active batch：

```sql
SELECT be.source_table, be.target_table, be.program_key
FROM dwp.lineage_business_edge AS be
JOIN dwp.lineage_batch AS b
  ON b.batch_id = be.batch_id
 AND b.is_active = TRUE
WHERE be.is_active = TRUE
  AND be.environment = :environment
  AND (:source_profile IS NULL OR be.source_profile = :source_profile);
```

上例中的 `dwp.` 不是可选风格；`current_schema=public` 时禁止依赖
`search_path`。Physical debug 查询可读取同一 batch 的 TMP endpoints，但普通业务
查询不得为了找业务关系在 query-time 递归 TMP。

History 查询按显式 `batch_id` 读取，不把旧 batch 的 inactive edge 混入 active
projection。任何只写 `WHERE is_active = TRUE` 而不 join active batch 的查询，均视为
contract violation。

## 8. SQLite → DWS compatibility matrix

SQLite 继续是 reference adapter，不是隐式 production contract。尤其不能把临时
验证表、fixture 或当前 `LineageEdge` Python object 的旧语义当成
`lineage_business_edge` 的 production schema 来源。

| DWS table | compatible | transformed | intentionally incompatible | production-only | future |
| --- | --- | --- | --- | --- | --- |
| `lineage_batch` | `batch_id`、`observed_at`、`published_at`、edge/issue counts、active snapshot | SQLite `id`/implicit row identity → `row_key`；新增 snapshot mode/scope、program/physical/business counts | SQLite 单一 `edge_count` 不能代表 physical 与 business 两层一致性 | DWS ROW/HASH/partition、显式 `dwp.`、publish gate metadata | retention/failed-run observability 可能另立表 |
| `lineage_program_state` | environment、source_profile、program_name、source_hash、pipeline_version、first/last seen、last changed、batch、active | `id INTEGER AUTOINCREMENT` → `row_key`；三元组 → `program_key` | 不把 batch/runtime run 或 job key 当 program identity | DWS distribution、partition、active-batch join | 外部权威 program id 需独立 contract |
| `lineage_edge` | environment/profile、program provenance、source/target label、evidence、source_hash、batch、observed/active | SQLite `LineageEdge` 行 → DWS `row_key`/`edge_key`；SQLite evidence text → DWS bounded text；增加 node kind/dataset key | 当前 SQLite `LineageEdge` 是 formal collapsed edge 且拒绝 TMP；DWS `lineage_edge` 是 physical direct edge，允许 TMP；不能直接 rename table 复用 | COLUMN/HASH、physical cycle/orphan preservation、physical batch contract | SQLite physical adapter / migration 另立 Issue |
| `lineage_business_edge` | 无当前正式 SQLite production contract；只可复用未来 candidate/publish 抽象 | 新增 derived table；由同 batch physical edge 生成；需新 `row_key`/`business_edge_key`/derivation fields | 不把当前 `history.BusinessLineageEdge` diff value object 或临时验证表当 production schema；不把 query-time BFS 当 materialization | DWS formal endpoint、collapse metadata、physical derivation hash、同批 gate | SQLite reference adapter 何时实现由独立 Issue 决定 |
| `lineage_issue` | environment/profile/program、issue type/message/evidence、stable/first/last/active、batch | `id` → `row_key`；nullable `stable_key` → production `stable_issue_key`；evidence canonicalization | #36 的 severity/disposition/policy 枚举不能在 #39 自行冻结 | issue layer 与 DWS active/history publish validation | #36 完成后的 policy alignment/backfill |
| `lineage_closure` | 无 | 无 | 本 Issue 不创建、不把 closure 混入 business edge | 无 | Issue #40 future derived index |

## 9. Tests / contract lint scope

本轮测试只验证 design/contract，不连接真实 DWS，也不实现 collapse：

- DDL table/schema/key/orientation/distribution/closure lint；
- lifecycle JSON 的 same-batch、empty success、rollback、duplicate key、profile
  isolation、inactive contamination、partial/full disappearance、rebuild 与
  derived rebuild cases；
- 现有 SQLite targeted tests 继续覆盖 failed publish、历史保留、active query、
  duplicate identity 与 partial scoped deletion；
- 不因本 Issue 重跑无关 parser 全量测试，不修改 `imp_lineage_edge` runtime。

## 10. Unresolved decisions

| Decision | v0.1 proposal | 当前状态 / fallback |
| --- | --- | --- |
| #36 policy alignment | 保留 nullable `severity`、`disposition`、`rule_version`、`policy_version` slots | **等待 #36**；若冲突，以 #36 最终 contract 为准，不在 #39 回填猜测枚举 |
| `collapse_depth` 定义 | distinct safe path 的 physical direct-edge hop 数；direct=1；示例链=3；多 path 初步取最小 hop | **待冻结**；在决定前可为 NULL，不能用 TMP 名称或 path sample 推导 identity |
| `path_count` 是否进入 v0.1 | nullable `NUMERIC(38,0)`，精确 distinct safe physical path count；不安全时 NULL + issue | **待冻结**；如果真实规模/周期不支持精确统计，去掉字段或只保留 diagnostic，不以 sample 长度替代 |
| physical path change 是否更新 business `last_changed_at` | TMP rename/topology 改变 `physical_derivation_hash`，建议更新；纯 evidence reorder 不更新 | **待业务 owner 决定**；stable business key 始终不变 |
| source_hash-only change 的 business lifecycle | state/physical provenance changed；derived projection 不变时 business key 不变，last_changed 默认不更新 | **待确认**；不得把 source_hash 直接加入 business identity |
| business collapse failure 的 publish 影响 | 明确 negative result 可带 issue 发布；不可验证/异常失败 fail closed 整个 batch | **待实现 Issue 决定**；无论选择哪种，physical/business 绝不能跨 batch active |
| DWS distributed unique enforcement | DDL 写 logical unique；writer 在 candidate validation 强制检查 `(batch_id, stable_key)` | **待 GaussDB 8.1.3 non-prod proof**；不能为验证连接真实生产 |
| history retention horizon | rolling time partitions；active 永不按 retention 删除 | **运维决策未冻结**；issue retention 不短于事实对账需要或 #36 更长策略 |

## 11. 建议独立 Issue：实现 business collapse / publisher

建议标题：

> **lineage: implement fail-closed physical-to-business lineage collapse and same-batch publisher**

建议 scope：

1. 消费同一 candidate batch 的 physical DAG / `lineage_edge`，不重写 parser 或
   DatasetIdentity；
2. 实现 formal boundary、TMP traversal、direct-hop depth、distinct path count、
   deterministic `physical_derivation_hash` 和 stable business key；
3. 对 cycle、self-reference、orphan、missing schema、ambiguous sink 提供可解释
   diagnostic；禁止 basename/fuzzy guess；
4. 实现 candidate validation 与 physical/business 同 batch atomic publish，collapse
   不可验证时 fail closed 并保留上一 active snapshot；
5. 覆盖 direct edge、TMP multi-hop、multi-path dedupe、TMP rename、cross-profile、
   empty batch、partial/full snapshot、rollback 与 rebuild。

Acceptance criteria：

- `DWF.A → TMP1 → TMP2 → DWUPRR.R` 产生一个 formal business edge，TMP 不在业务
  endpoint，physical rows 三条完整保留；
- direct formal edge 的 depth=1；path_count 不把 bounded sample 当完整计数；
- TMP rename 不改变 `business_edge_key`，但按最终 lifecycle decision 处理 derived
  change；
- unsafe collapse 不产生猜测 edge，并有同 batch issue；不可验证失败不切换 active；
- physical 与 business 永远不出现跨 batch active；失败后上一 snapshot 可完整读取。

这个独立 Issue 不在本轮实现，也不包含 `lineage_closure`、parser、OpenLineage 或
column lineage。
