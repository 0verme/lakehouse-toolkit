# Lineage Phase 5：TMP 折叠与血缘落库

Phase 5 消费 Phase 3 的 `ProgramPhysicalDAG` 和 Phase 4 的
`LineageAuditResult`，完成纯的 TMP collapse、正式 direct lineage
materialization、issue 落库和完整批次发布。它不重新解析程序、不修改 Physical
DAG，也不替换现有生产入口。

## Physical DAG 与 Business Lineage

Physical DAG 记录程序内部真实执行关系，TMP 节点必须保留：

```text
ODS.A ─────→ TMP1 ─────→ TMP2 ─────→ DWA.F
DWF.B ─────↗
DWM.C ─────────────────↗
DWA.D ─────────────────────────────→ DWA.F
```

正式 `LineageEdge` 只表示正式业务资产之间的**直接**关系：

```text
ODS.A → DWA.F
DWF.B → DWA.F
DWM.C → DWA.F
DWA.D → DWA.F
```

TMP 只存在于 Physical DAG 和 edge evidence 中，不作为正式资产 endpoint 落库。

## TMP Collapse 与正式资产边界

Collapse 从每个正式节点的 outgoing edge 开始：遇到 TMP 就继续沿路径走，第一次
遇到正式节点便生成一条 `U → V` 并停止该路径。它不是 transitive closure。

```text
A(formal) → TMP1 → TMP2 → B(formal) → TMP3 → C(formal)
```

生成：

```text
A → B
B → C
```

不会生成 `A → C`。因此：

```text
ODS.A → DWM.B → TMP1 → DWA.C
```

只 materialize：

```text
ODS.A → DWM.B
DWM.B → DWA.C
```

`DWM.B` 是正式资产边界，不能被 TMP collapse 越过。

一个 batch/program 内相同的
`environment + source_profile + source_table + target_table + program_name + job_key`
只保留一个业务事实；重复 physical path 会合并到同一条 edge 的 deterministic
`evidence.physical_paths`。

## Audit 结果与 orphan

已知 `expected_target` 时，materialization 只使用 Audit 已计算的
`target_reachable_nodes`。无法到达 expected target 的 terminal branch 不进入
`lineage_edge`，但原样以 Phase 4 产生的 `LineageIssue` 进入 `lineage_issue`：

```text
正常 target-reaching branch → lineage_edge
异常 orphan branch          → lineage_issue
```

`expected_target=None` 时不猜测 sink，也不重新产生
`TARGET_NOT_FOUND`、`TARGET_MISMATCH` 或 `ORPHAN_BRANCH`。此时只 materialize
Physical DAG 中已经明确的 formal-to-formal boundary；cycle/self-reference 仍由
Phase 4 issue 表达，并且 collapse 有 visited protection。

## Python API

纯转换和持久化分离：

```python
from shared.lineage import (
    ProgramSource,
    audit_program_physical_dag,
    build_program_physical_dag,
    materialize_batch,
)

source = ProgramSource(...)
dag = build_program_physical_dag(source)
audit = audit_program_physical_dag(
    dag,
    batch_id="batch-001",
    observed_at=observed_at,
)
batch = materialize_batch(
    [audit],
    batch_id="batch-001",
    observed_at=observed_at,
)
```

`materialize_program()` 返回单程序的 `ProgramMaterialization`，包含原始 `dag`、
Audit、`LineageEdge` tuple 和 `LineageIssue` tuple；`collapse_tmp_edges()` 是只
取 edge 的窄入口。相同输入、batch 和 `observed_at` 的输出顺序与 evidence 稳定。
`LineageEdge` 仅增加了可选的结构化 `evidence` 字段，保留 Phase 1–4 构造方式兼容。

## SQLite Reference Store

`shared/lineage/materialization_sqlite.py` 是公开的 reference adapter，默认只使用
`runtime/sqlite/lineage_materialization.db`，测试可以传临时路径或注入
`sqlite3.Connection`。它不依赖内部 Oracle、MySQL、DWS、VPN 或凭据；未来生产
repository 可以复用 `MaterializationBatch`，不必绑定 SQLite。

### `lineage_edge`

| 字段 | 含义 |
| --- | --- |
| `environment` / `source_profile` | 来源环境和 profile |
| `source_table` / `target_table` | 正式上游、正式下游 |
| `program_name` / `job_key` | 程序和可选作业身份 |
| `evidence_type` / `evidence` | provenance 类型和 deterministic JSON |
| `source_hash` | Provider 提供的原值；本阶段不用于增量 rebuild |
| `batch_id` | materialization snapshot |
| `observed_at` / `updated_at` | 本批次统一观察/更新时间 |
| `is_active` | 是否属于当前 active snapshot |

索引覆盖 source、target、`batch_id + is_active`，并在同一 batch 上按业务 identity
建立 unique index。TMP endpoint 在领域对象层即被拒绝。

### `lineage_issue`

表中保存：

```text
environment, source_profile, program_name
issue_type, severity, stable_key
node_key, branch_sink, message, evidence
batch_id, first_seen_at, last_seen_at, is_active
```

索引覆盖 `stable_key`、`batch_id + is_active` 以及 environment/profile/program/
issue_type/active scope。`evidence` 使用 `json.dumps(..., sort_keys=True,
separators=(",", ":"))` 形式保存，不使用 pickle、`repr()` 或 Python `hash()`，也
不会复制完整 `script_code`。

### `lineage_batch`

这是 atomic publish 所需的最小 control table，保存 batch 的观察时间、edge/issue
计数和 active 状态。它也能正确表示“成功发布但本批次没有 edge”的 snapshot；否则
只能从业务行反推 active batch。它不是历史 diff 或 Query API。

Phase 7 在同一 batch contract 下新增 `lineage_program_state`，保存程序 identity、
`source_hash`、`pipeline_version`、first/last seen、last changed 与 active 状态；旧
batch 的 edge、issue 和 program state 都保留为 historical snapshot。`pipeline_version`
来自代码中明确维护的 `LINEAGE_PIPELINE_VERSION`，不是 Git commit SHA；只有 hash
和 pipeline version 都相同才会跳过 parser/DAG/audit。

## Atomic Batch Publish

`SQLiteMaterializationStore.publish()` 在同一个 transaction 中完成：

```text
BEGIN IMMEDIATE
  insert inactive candidate batch/edges/issues/program states
  validate row counts、identity 和 JSON evidence
  deactivate previous batch
  activate new batch
COMMIT
```

任何 build、collapse、audit、insert、validation 或 active switch 异常都会
`ROLLBACK`。因此已有 `batch-001` 时，`batch-002` 失败不会留下空 active dataset、
半个 candidate 或半个 active batch；`batch-001` 仍然可读。成功后旧 batch 保留为
inactive snapshot，当前 active batch 由 `get_active_batch_id()` 标识。

`batch_id` 可显式传入（例如 `batch-001`），未传入时使用 UUID；`observed_at` 也可
注入，单次计算全批次复用同一时间。

## 定时任务入口

`jobs/crontab/imp_lineage_edge.py` 只做窄编排：

```text
ProgramSource provider
    → existing Physical DAG Builder
    → existing Phase 4 Auditor
    → Phase 5 candidate batch
    → SQLite atomic publish
```

它支持注入公开 fixture/mock provider，直接执行时从 local/example provider 配置读取，
不会写入真实连接参数，也不会自动替换旧的 cron 或生产 lineage 入口。

### 定时任务可观测性

定时任务复用了已合并的 progress logging PR #25（`ccb5e60`）的低基数脱敏日志约定，在
`source_load`、`replay`、`incremental_plan`、`build`、`publish` 和 `job` 边界输出已
flush 的阶段状态。build progress 默认每 500 个本轮 rebuild 程序输出一次；日志只包含
count、耗时、受限 batch ID、受控 profile 和异常 class，不输出 program name、源码、
SQL、表名或 connection settings。单程序总耗时超过默认 5 秒时，才输出带稳定短 hash
的 `stage=build_program status=SLOW`，并分别给出
`build_program_physical_dag_ms`、`audit_program_physical_dag_ms` 和
`single_program_total_ms`。build SUCCESS 还汇总 `processed`、`slow_programs`、
`max_program_elapsed_ms` 和 `avg_program_elapsed_ms`。coverage funnel 的聚合行以
`stage=coverage` 单独输出，详见 [`lineage_coverage.md`](lineage_coverage.md)。

直接运行：

```bash
python jobs/crontab/imp_lineage_edge.py
```

本轮只 rebuild 100 个程序时，`build total` 也只会是 100；日志不会为每个
`ProgramSource` 输出一条记录。默认运行与 controlled replay 的区别如下：

- 不传 `--profile`、`--limit`：保持正常全 provider、complete snapshot 运行；
- `--profile SOURCE_PROFILE`：只选定 source profile，默认按 partial snapshot 发布，
  不会因未选 profile 触发 DELETE；
- `--limit N`：收集选定来源后按 `ProgramIdentity` 排序取前 N 个，只让 sample 进入
  parser/DAG/audit，并强制 partial snapshot，sample 外程序不会判定 `DELETED`；
- `--force-rebuild`：只对本次 replay 选中的程序绕过 hash/version reuse，不代表应该
  对全部生产程序直接执行。

例如第一阶梯可运行：

```bash
python jobs/crontab/imp_lineage_edge.py \
  --profile mysql_dev_a_data --limit 100 \
  --force-rebuild --progress-every 10 --slow-threshold-ms 5000
```

推荐内网验证顺序为：`100 programs → 500 programs → one profile → all profiles`。
当前已观测 38 个 rebuild 约耗时 17 分钟；因此不建议直接对约 2 万程序 force
rebuild，也不要为了掩盖瓶颈而盲目并发。每一级先检查 active edge diff、issue、
耗时和 `partial_snapshot`，再进入下一阶段。

日志示例：

```text
stage=job status=STARTED providers=4 replay_mode=normal selected_profiles=- force_rebuild=False partial_snapshot=False
stage=source_load status=SUCCESS sources=20493 elapsed_ms=...
stage=replay status=SELECTED replay_mode=normal selected_profiles=- source_total=20493 replay_total=20493 limit=- force_rebuild=False partial_snapshot=False
stage=incremental_plan status=SUCCESS total=20493 new=23 changed=15 unchanged=20455 deleted=0 rebuild=38 elapsed_ms=...
stage=build status=STARTED total=38 slow_threshold_ms=5000
stage=build status=RUNNING processed=10 total=38 percent=26 elapsed_ms=...
stage=build_program status=SLOW program_id=<stable-short-hash> build_program_physical_dag_ms=... audit_program_physical_dag_ms=... single_program_total_ms=...
stage=build status=SUCCESS processed=38 slow_programs=... max_program_elapsed_ms=... avg_program_elapsed_ms=... edges=... issues=... elapsed_ms=...
stage=publish status=SUCCESS batch_id=batch-... edges=... issues=... previous=- elapsed_ms=...
stage=job status=SUCCESS elapsed_ms=...
```

`stage=job status=SUCCESS` 只会在 SQLite atomic publish 完成后出现。中途的 STARTED/RUNNING
日志只表示计算进度，不表示 snapshot 已经发布；失败时会输出
`status=FAILED exception=<ExceptionClass>` 并保留原有异常传播/non-zero 行为。可用
`build_program status=SLOW` 判断长尾属于 DAG build 还是 audit：前者的
`build_program_physical_dag_ms` 高时检查 parser/DAG 提取，后者的
`audit_program_physical_dag_ms` 高时检查 audit 遍历和 issue 判定；用
`max_program_elapsed_ms`/`avg_program_elapsed_ms` 区分少数长尾与整体变慢。当前实现使用
固定 count progress；如果未来需要在单个程序长时间运行期间提供 heartbeat，可单独增加时间阈值。

## 本阶段边界

Phase 5 本身仍只负责纯 materialization 与 atomic publish；增量、历史、diff、issue
lifecycle 和 legacy decision 由 Phase 7 追加，详见
[`lineage_incremental_history.md`](lineage_incremental_history.md)。

Phase 6 的实现见 [`lineage_query.md`](lineage_query.md)：它只从 active
`lineage_edge` 做窄读取和统一 BFS，不改变 Phase 5 的持久化事实语义；Phase 7 不把
历史结果塞进 Viewer contract。
