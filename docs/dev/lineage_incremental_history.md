# Lineage Phase 7：增量、历史、diff 与旧链路收口

Phase 7 在 Phase 1～6 的 Provider → Physical DAG → Audit → TMP collapse →
materialization → Query 主链上增加演进能力。它不改变 `LineageEdge` 的方向，
也不向 Viewer JSON 增加历史字段。

## Program identity 与 source hash

程序 identity 冻结为：

```text
environment / source_profile / program_name
```

例如 `DEV/mysql_dev_a/PROGRAM_DEMO_A` 和
`PROD/production_metadata/PROGRAM_DEMO_A` 是两个程序实例。当前
`ProgramSource` 没有可靠的稳定 `job_key`，所以 Phase 7 不猜测性地把 job key
加入 identity；`job_key` 仍是 edge provenance。

`source_hash` 继续使用 Phase 2 的 `compute_source_hash()` SHA-256 canonical JSON
规则，不重新计算另一种 hash。`None` 或空 hash 永远不能产生 `UNCHANGED`，会
保守地进入 rebuild。

## Incremental planner

`shared.lineage.evolution.plan_incremental()`（也从
`shared.lineage.incremental` 导出）只消费当前 `ProgramSource` 与上一次 active
`ProgramState`，不连接数据库、不解析 SQL：

| 状态 | 条件 | 执行 |
| --- | --- | --- |
| `NEW` | 没有 active identity | parser、DAG、audit、materialize |
| `UNCHANGED` | 当前非空 hash 等于 active state hash | 跳过 parser/DAG/audit，复用旧 facts |
| `CHANGED` | identity 存在但 hash 不同或当前 hash 缺失 | 重建该程序 |
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
`shared.lineage.lineage_builder.normalize_table_name()`，例如 `DWA.X` 与
`DWS_DWA.X` 使用同一正式表示。

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
