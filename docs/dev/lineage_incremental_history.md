# Lineage Phase 7：增量、历史、diff 与旧链路收口

Phase 7 在 Phase 1～6 的 Provider → Physical DAG → Audit → TMP collapse →
materialization → Query 主链上增加演进能力。它不改变 `LineageEdge` 的方向，
也不向 Viewer JSON 增加历史字段。ProgramIdentity、ProgramState、rename/delete/
restore、Batch 与 runtime boundary 的完整 V1 contract 见
[`lineage_program_identity.md`](lineage_program_identity.md)。

## Program identity 与 source hash

程序 identity 冻结为：

```text
environment / source_profile / program_name
```

identity boundary 只 trim surrounding whitespace，保留现有字段大小写；不要把
`source_hash`、`pipeline_version`、`batch_id` 或不稳定 `job_key` 加入 identity。

例如 `DEV/mysql_dev_a/PROGRAM_DEMO_A` 和
`PROD/production_metadata/PROGRAM_DEMO_A` 是两个程序实例。当前
`ProgramSource` 没有可靠的稳定 `job_key`，所以 Phase 7 不猜测性地把 job key
加入 identity；`job_key` 仍是 edge provenance。

`source_hash` 继续使用 Phase 2 的 `compute_source_hash()` SHA-256 canonical JSON
规则，不重新计算另一种 hash。`None` 或空 hash 永远不能产生 `UNCHANGED`，会
保守地进入 rebuild。

Parser、Physical DAG、primary target 和 audit 规则的语义版本由代码中的
`shared.lineage.version.LINEAGE_PIPELINE_VERSION` 显式维护，当前值为
`lineage-pipeline-v4-program-name-target-authority`。它不是 Git commit SHA。凡是会改变
parser/DAG/audit/materialization 结果的规则升级，都必须在同一变更中把这个 constant
bump 到新的语义版本，并在本文记录原因。本次 v4 收紧固定 `005` program_name 的
`target authority`：只有严格四段 canonical shape 才恢复 logical target，ambiguous
三段保持 unknown；同时保留 v3 的 step/suffix 语义和 Python AST 失败后的保守 legacy
SQL literal recovery。相同 source hash 的旧 v3 facts 也必须 rebuild。

## Incremental planner

`shared.lineage.evolution.plan_incremental()`（也从
`shared.lineage.incremental` 导出）只消费当前 `ProgramSource` 与上一次 active
`ProgramState`，不连接数据库、不解析 SQL：

| 状态 | 条件 | 执行 |
| --- | --- | --- |
| `NEW` | 没有 active identity | parser、DAG、audit、materialize |
| `UNCHANGED` | 当前非空 hash 等于 active state hash，且 pipeline version 相等 | 跳过 parser/DAG/audit，复用旧 facts |
| `CHANGED` | identity 存在但 hash 或 pipeline version 不同，或当前 hash 缺失 | 重建该程序 |
| `DELETED` | 仅完整 snapshot scope 内缺失 | 从新 active candidate 移除，历史保留 |

planner 会拒绝当前 snapshot 的 duplicate identity，也不会把程序重命名猜成
rename；旧 identity 是 `DELETED`，新 identity 是 `NEW`。

### Complete snapshot boundary

Provider 只有在完整扫描成功后才能声明 `complete_snapshot=True`，并通过
`SnapshotScope(environment, source_profile)` 指明边界。部分扫描、Provider
异常或没有明确 scope 时不会推断 `DELETED`。完整 candidate 的组成是：

```text
unchanged facts      → rebase 到新 batch
new/changed facts    → 复用既有 Phase 3/4/5 builder/audit/materialization
outside-scope facts  → 保留
complete scope 中 deleted facts → omit
全部合并             → SQLite atomic publish
```

因此 `950 unchanged + 50 changed` 仍发布完整 snapshot；changed rebuild 中途
失败时，candidate 尚未 publish，旧 active batch 保持不变。

## Program state

SQLite reference adapter 新增 `lineage_program_state`，每个 candidate batch 保存：

```text
environment
source_profile
program_name
source_hash
pipeline_version
first_seen_at
last_seen_at
last_changed_at
batch_id
is_active
```

状态行随 batch append，旧 batch 不被更新；partial unique index 保证当前 active
identity 唯一。`SQLiteMaterializationStore.read_program_states()` 返回当前或
指定 batch 的 state。`MaterializationBatch.program_states` 是兼容性扩展，旧的
不带 state 的手工 batch 仍可 publish，但下一次会按缺少 state 的程序保守重建。

### Pipeline version cache invalidation

planner 只有在以下两个条件同时满足时才返回 `UNCHANGED`：

```text
current.source_hash == previous.source_hash != NULL
AND previous.pipeline_version == LINEAGE_PIPELINE_VERSION
```

因此：源码 hash 相同且版本相同会复用；hash 变化会返回 `CHANGED`；hash 相同
但版本变化也会返回 `CHANGED`。从旧 SQLite schema 迁移的 `ProgramState` 的
`pipeline_version` 保持为 `NULL`，第一次运行会保守 rebuild，成功 publish 后
写入当前版本。迁移只新增 nullable column 和 schema `user_version`，不会更新或
删除历史 batch、`lineage_edge`、`lineage_issue`。

需要 bump pipeline version 的场景包括：parser 提取规则、primary target 解析、
Physical DAG 节点/边语义、audit 判定、TMP collapse 或 materialization evidence
语义发生改变；仅改变日志文案或运行参数不需要 bump。

## Issue lifecycle

`reconcile_issue_lifecycle(previous, current, observed_at=...)` 以现有
`LineageIssue.stable_key`（没有 stable key 时使用完整 fallback identity）做
reconciliation：

- 首次出现：`first_seen_at = last_seen_at = observed_at`，状态 `NEW`；
- 持续出现：保留旧 `first_seen_at`，更新当前观察的 `last_seen_at`，状态
  `PERSISTING`；
- 当前缺失：旧历史 issue 不删除，返回 `RESOLVED` 记录及推导的 `resolved_at`。

SQLite 中的旧 `lineage_issue` 行不会被回写；resolved 是由两个 historical
snapshot 推导的。`IssueLifecycle.age_days` 可识别持续时间，因而可以查询
`ORPHAN_BRANCH` 持续至少 30 天的 evidence，但本阶段不实现通知系统。

### `LINEAGE_BRANCH_BROKEN`

新增 `IssueType.LINEAGE_BRANCH_BROKEN`，只有满足以下证据才产生：

```text
旧 active snapshot 中同一 environment/profile/program 曾到达 expected target
+
当前同一程序的 audit 出现带 expected target evidence 的 ORPHAN_BRANCH
```

当前普通 orphan 不会自动升级。已产生的 broken issue 在后续仍处于 broken
branch 时会 carry，恢复到目标后由历史 diff 标记 resolved。

## History 与 diff API

`SQLiteMaterializationStore` 支持：

```python
store.list_batch_metadata()
store.read_edges(batch_id="batch-001")
store.read_issues(batch_id="batch-001")
store.read_program_states(batch_id="batch-001")
store.diff_lineage_batches("batch-001", "batch-002")
store.reconcile_issue_lifecycle("batch-001", "batch-002")
```

纯逻辑函数从 `shared.lineage.history` / `shared.lineage.evolution` 导出：

```python
diff_lineage_batches(previous_edges, current_edges)
diff_environments(dev_edges, prod_edges)
```

batch graph diff 的正式业务 identity 是：

```text
environment / canonical source_table / canonical target_table
```

它忽略 `program_name`、`job_key`、`source_profile` 和 evidence，因此同一条
`A → B` 仅因 provenance 改变不会被误报为 removed + added。结果固定排序并返回
`added_edges`、`removed_edges`、`unchanged_edges`。

DEV/PROD diff 默认比较 `DEV` graph 与 `PROD` graph，DEV 的多个
`source_profile` 合并在同一 environment graph 内；`diff_environments()` 和 store
adapter 提供可选的 `dev_source_profile` / `prod_source_profile` 显式过滤参数。
资产 canonicalization 复用
`shared.lineage.lineage_builder.normalize_table_name()`；它只清理格式并保留物理
schema，因此 `DWA.X` 与 `DWS_DWA.X` 保持不同的正式表示。legacy alias 只在
lookup/兼容匹配层扩大候选，不改写 Dataset Identity。

普通 `LineageQueryService`、Viewer JSON 和 Blast Radius 仍只读 active edge；
历史 batch 与 diff 不进入既有 Viewer contract：

```json
{"nodes": [], "edges": [], "truncated": false}
```

## Legacy decision 与 closure

真实调用关系和逐入口决定见
[`lineage_legacy_migration.md`](lineage_legacy_migration.md)。Phase 7 只把
`jobs/crontab/imp_lineage_edge.py` 的全量编排接到增量 candidate executor；
调度漫游、字段映射、DWF 截止和 audit summary 等语义不同的入口保留兼容，
没有证据就不删除。

## 正常增量运行

日常运行直接执行 cron 入口，不传 controlled replay 参数：

```bash
python jobs/crontab/imp_lineage_edge.py
```

它扫描所有配置的 provider，在 active `ProgramState` 上同时比较
`source_hash` 和 `LINEAGE_PIPELINE_VERSION`，只把 `NEW`/`CHANGED` 程序送入
parser/DAG/audit，并把 `UNCHANGED` facts 合并进新的完整 snapshot。所有 provider
成功后才允许 complete snapshot 的 `DELETED` 判定；任一读取或 rebuild 失败都会
保留旧 active batch。

## Controlled replay 与内网验证阶梯

正常定时运行不传 `--profile`、`--limit` 时，仍扫描全部 provider，并沿用
`complete_snapshot` 的生产行为。需要验证新 parser 时，使用 controlled replay：

```bash
python jobs/crontab/imp_lineage_edge.py \
  --profile mysql_dev_a_data \
  --limit 100 \
  --force-rebuild \
  --progress-every 10 \
  --slow-threshold-ms 5000
```

`--profile` 只保留指定 `source_profile`；`--limit` 不截断 provider 读取，而是在
收集完选定 profile 后按 `ProgramIdentity`（environment/source_profile/
program_name）排序取前 N 个，只有这 N 个进入 parser/DAG/audit。日志会给出
`replay_mode`、`selected_profiles`、`source_total`、`replay_total`、
`force_rebuild` 和 `partial_snapshot`，不输出源码、SQL、表名或连接凭据。

带 `--limit` 的 controlled replay 始终按 partial snapshot 发布；sample 外的程序
不能被判定为 `DELETED`。只指定 profile 且不带 `--limit` 时，若该 profile 成功完成
全量 inventory/read，则允许仅在该 profile scope 内进行 disappearance 判断；provider
失败或 diagnostics 存在时仍自动降级为 partial。推荐内网验证阶梯：

```text
100 programs
→ 500 programs
→ one profile
→ all profiles
```

不建议直接对约 2 万程序使用 `--force-rebuild`：当前 parser/DAG/audit 是逐程序
同步路径，已观测到 38 个 rebuild 约耗时 17 分钟；全量会放大故障半径和等待时间，
也会让性能瓶颈难以归因。先用小样本确认结果和耗时，再逐级扩大。

## Slow program 日志定位

默认只输出聚合 build 日志；单程序完整的 DAG、audit、materialization 总耗时超过
5 秒（可用 `--slow-threshold-ms` 调整）时才输出一行：

```text
stage=build_program status=SLOW program_id=<stable-short-hash> \
  source_profile=<safe-profile> ordinal=... elapsed_ms=... \
  dag_ms=... audit_ms=... materialization_ms=... \
  physical_nodes=... physical_edges=... lineage_edges=... issues=...
```

`dag_ms` 高说明优先检查 parser/Physical DAG 提取路径，`audit_ms` 高说明优先检查
Audit 图遍历和 issue 判定，`materialization_ms` 高说明优先检查 TMP collapse 和
bounded evidence finalize。`build status=SUCCESS` 的 `slow_programs`、
`max_program_elapsed_ms`、`avg_program_elapsed_ms` 用于判断是少数长尾还是整体变慢。
受控 replay 需要逐程序起止日志时显式增加 `--diagnostic`，此时还会输出同样脱敏的
`STARTED`/`SUCCESS`。`program_id` 是不含程序名的稳定短 hash，可在同一受控 sample
的重复运行中比对长尾；不应为定位方便而把源码、SQL 或表名写进生产日志。

### `lineage_closure` decision

```text
Decision: NO
```

Phase 6 的 indexed narrow-neighbor reads + bounded BFS 已满足公开 synthetic
benchmark 的 depth/max_nodes 约束；本阶段不创建 `lineage_closure`。后续只有在
目标规模的真实响应 SLO 经 benchmark 证明 BFS 不足时，才重新评估 closure。
benchmark 不使用真实生产 lineage，运行方式见
[`lineage_bfs_benchmark.py`](../../benchmarks/lineage_bfs_benchmark.py)。本机公开
synthetic evidence（Python 3.13.2 / Windows 11 / SQLite，3 次平均）为：

```text
Edges: 1000   Query: downstream   Depth: 7   Max nodes: 300   Approx: 0.055495s/run
Edges: 10000  Query: downstream   Depth: 7   Max nodes: 300   Approx: 0.362386s/run
Closure required: NO
```

该结果受本机硬件、Python、SQLite 和数据形状影响，不是 CI timing gate。
