# Lineage Phase 5：Business Asset Boundary、TMP/技术节点折叠与血缘落库

Phase 5 消费 Phase 3 的 `ProgramPhysicalDAG` 和 Phase 4 的
`LineageAuditResult`，完成纯的 Business Asset projection、TMP/DLO/DWO collapse、
正式 direct lineage materialization、issue 落库和完整批次发布。它不重新解析程序、
不修改 Physical DAG，也不替换现有生产入口。Business Asset Boundary 的独立 contract
见 [`lineage_business_asset_boundary.md`](lineage_business_asset_boundary.md)。

正式 `LineageEdge` 的 endpoint 遵循
[`lineage_dataset_identity.md`](lineage_dataset_identity.md)：只有
`environment + schema + table` 能形成 DatasetIdentity；缺少 schema 的引用不会被
猜测成正式 edge。`source_profile`、program 和 job 仍保留在 lineage fact identity
中，不能与 Dataset identity 混为一谈。ProgramIdentity / ProgramState 与
`Batch != Runtime Run` 的完整边界见
[`lineage_program_identity.md`](lineage_program_identity.md)。

> **Issue #39 DWS Materialization Writer:** 本文的 Phase 5 Business direct
> `LineageEdge` 仍由既有 materialization 产生；DWS writer 将同一 pipeline 的
> `ProgramPhysicalDAG` direct `PhysicalEdge` 写入 `dwp.lineage_edge`（TMP/DLO/DWO
> endpoint 允许），并将 Business `LineageEdge` 写入 `dwp.lineage_business_edge`（TMP/DLO/DWO endpoint
> 禁止）。writer 不重新解析 SQL 或复制 TMP/DLO/DWO collapse。五张表共用 batch/lifecycle，
> `edge_count` 统计 physical `lineage_edge` rows；跨 program/global N-hop closure
> 属于 Issue #40。详见
> [`issue-39-dws-materialization-schema.md`](../research/issue-39-dws-materialization-schema.md)。

## Physical DAG 与 Business Lineage

Physical DAG 记录程序内部真实执行关系，TMP 节点必须保留：

```text
ODS.A ─────→ TMP1 ─────→ TMP2 ─────→ DWA.F
DWF.B ─────↗
DWM.C ─────────────────↗
DWA.D ─────────────────────────────→ DWA.F
```

正式 `LineageEdge` 只表示 Business Asset 之间的**直接**关系；DLO/DWO 只能留在
Physical DAG 或 Business edge 的 collapse evidence 中：

```text
ODS.A → DWA.F
DWF.B → DWA.F
DWM.C → DWA.F
DWA.D → DWA.F
```

TMP、DLO、DWO 都不能作为 Business endpoint；TMP/DLO/DWO 可作为 physical
`lineage_edge` endpoint 落库，并在 business `lineage_business_edge` 中由既有 collapse
结果隐藏。DLO/DWO 的 boundary 规则与 normalization 见
[`lineage_business_asset_boundary.md`](lineage_business_asset_boundary.md)。

## TMP/DLO/DWO Collapse 与 Business Asset 边界

Collapse 从每个 Business Asset 节点的 outgoing edge 开始：遇到 TMP、DLO 或 DWO
就继续沿路径走，第一次遇到下一个 Business Asset 便生成一条 `U → V` 并停止该路径。
DLO/DWO 不会因为 `PhysicalNodeKind.FORMAL_ASSET` 而变成 Business boundary；该算法
也不是 transitive closure。

```text
A(Business) → TMP1 → DLO.B → DWO.C → B(Business) → TMP3 → C(Business)
```

生成：

```text
A → B
B → C
```

不会生成 `A → C`。因此：

```text
DWF.A → DLO.T1 → DWO.T2 → DWM.B → TMP1 → DWA.C
```

只 materialize：

```text
DWF.A → DWM.B
DWM.B → DWA.C
```

`DWM.B` 是 Business Asset 边界，不能被 TMP/DLO/DWO collapse 越过。若路径为
`DLO.A → DWO.B → DWF.C`，Physical rows 仍保留，但不会伪造 DLO/DWO endpoint 的
Business edge。

一个 batch/program 内相同的
`environment + source_profile + source_table + target_table + program_name + job_key`
只保留一个 `LineageEdge` Business direct fact；重复 physical path 会合并到同一条
business edge 的 deterministic `evidence.physical_paths`。每一条 raw
`PhysicalEdge` 仍独立写入 physical projection。

### Bounded Evidence Contract

`evidence.path_count` 是当前 SQLite/reference runtime 中该 Business edge 发现的完整
collapsed physical path 数量，不是 sample 的长度。它是现有 `LineageEdge` evidence
contract 的 runtime 字段；DWS writer 不增加专用 `path_count` 列，也不能把 bounded
sample 映射成生产计数。为避免一个 edge 携带无限 JSON，`physical_paths` 只保留最多 `100`
条按稳定 traversal 顺序取得的 deterministic representative sample，并用
`physical_paths_truncated` 标识是否还有未保存的 path；explicit fallback 仍会按 bounded
accumulator 保留 canonical 最小 sample。`source`、`target`、程序身份、`path_count` 和
statement evidence summary 不因 sample 截断而丢失。

聚合摘要也有固定边界：`physical_edge_pairs`、`collapsed_tmp_nodes`、
`collapsed_technical_nodes` 和 `statement_indices` 默认各保留最多 `200` 个 canonical
值，并分别用对应的 `*_truncated` 字段表示截断。SQLite 仍将 evidence 作为 JSON 文本保存，
因此没有额外的表迁移；SQLite consumer 必须使用 runtime `path_count` 判断完整规模，
不能用 `len(physical_paths)` 代替。DWS `lineage_edge` 接收 raw direct `PhysicalEdge`；DWS
`lineage_business_edge` 才接收 Business direct `LineageEdge`。两个 projection 使用同
一批次和同一 program pipeline。

Materialization 对无环 TMP/DLO/DWO 可折叠子图使用 deterministic DAG dynamic
programming：Business boundary 的 exact `path_count`、能参与该 boundary 的 physical
edge/node summary 都由 reachability 和拓扑计数得到，不显式保存或遍历全部 collapsed
path；随后只用 bounded
representative traversal 生成最多 `100` 条 sample path。每个 Business edge 只创建一次
`_EdgeEvidenceAccumulator` 和一次最终 `LineageEdge`，不会执行
`path → temporary LineageEdge → merge`。

含 TMP/DLO/DWO 可折叠节点 cycle 的图无法直接把 simple-path 数量替换为普通 DAG DP，
因此保留 explicit simple-path fallback。只有该 fallback 受 `MAX_COLLAPSED_PATHS=100000` 和
`MAX_COLLAPSED_TRAVERSAL_STATES=1000000` 限制，超限抛出 `LineagePathEnumerationError`
并由 job 记录为 `PATHOLOGICAL`；无环 dense graph 不通过降低上限处理，而是保留 exact
`path_count` 并只采 bounded evidence。`_json_safe` 同时拒绝 recursive cycle、超过
`64` 层的 nesting 和超过 `10000` 项的单个 collection，并抛出
`LineageEvidenceError`；它不使用对象 `repr()` 代替 evidence。

### Scaling 验证

`benchmarks/lineage_materialization_benchmark.py` 使用 fictional `DEMO` Physical DAG，
通过 instrumentation 统计 explicit path yield、`_path_evidence`、
`_edge_evidence`、`_lineage_edge_from_path`、temporary `LineageEdge`、accumulator 调用、
canonicalization、sample 和 peak in-flight retained path count，并验证 exact `path_count`、sample 上限、
summary 截断和 JSON 输出大小。结构型 fixture 覆盖 linear chain、diamond chain、high
fan-out、high fan-in、约 `150` 节点/`350` 边/约 `20k` paths 的 mixed graph，以及
`43` 节点/`407` 边、`max_out_degree=36`、`max_in_degree=39`、
`branch_nodes=35`、`merge_nodes=36` 的 dense graph；它不读取数据库，也不是依赖机器速度的
CI timing gate。

公开 benchmark 只使用 fictional `DEMO` nodes、edges 和 profiles，不记录真实
program id、源码尺寸、内部地址、driver error 或连接信息。它覆盖 small dense、high
fan-in、high fan-out、TMP cycle、bounded evidence 和 deterministic JSON，重点验证
full path count 不驱动无限 path object/evidence/LineageEdge 数量；耗时只用于观察，
不作为机器相关的 CI threshold。

运行：

```bash
python benchmarks/lineage_materialization_benchmark.py
```

### Evidence 与 SQLite Publish Scaling

`benchmarks/lineage_serialization_benchmark.py` 使用完全虚构的 `DEMO` edge
candidates，固定运行 `1_000`、`10_000`、`100_000` 条输入，报告 batch build、SQLite
publish 分段耗时、evidence canonicalization/`json.dumps` 次数、prepare/validate
row count 以及 serialization 次数。它同时断言每条 edge 只准备并校验一次，作为
Issue #55 的回归基线；耗时只用于观察 scaling，不设置机器相关的 CI threshold。

```bash
python benchmarks/lineage_serialization_benchmark.py
```

publish 的 `prepare` 阶段生成不可变的 row payload，`insert` 直接绑定这些 payload，
`validate` 复用同一份 candidate snapshot；因此 `lineage_edge` 与 `lineage_issue` 的
canonical JSON 不会在 insert/validation 之间重复序列化。任何阶段失败仍由同一个
SQLite transaction rollback，active batch 切换语义不变。

## Audit 结果与 orphan

已知 `expected_target` 时，materialization 只使用 Audit 已计算的
`target_reachable_nodes`。无法到达 expected target 的 terminal branch 不进入
`lineage_edge`，但原样以 Phase 4 产生的 policy projection `LineageIssue` 进入
`lineage_issue`；history reconciliation 也可将消失的 fact 作为 `RESOLVED`
projection 保留：

```text
正常 target-reaching branch → lineage_edge
异常 orphan branch          → lineage_issue
```

`expected_target=None` 时不猜测 sink，也不重新产生
`TARGET_NOT_FOUND`、`TARGET_MISMATCH` 或 `ORPHAN_BRANCH`。此时只 materialize
Physical DAG 中已经明确的 formal-to-formal boundary；cycle/self-reference 仍由
Phase 4 issue projection 表达，并且 collapse 有 visited protection。

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
| `source_table` / `target_table` | Business Asset 上游、Business Asset 下游 |
| `program_name` / `job_key` | 程序名称和可选作业 provenance；不等于 runtime Job identity |
| `evidence_type` / `evidence` | provenance 类型和 deterministic JSON |
| `source_hash` | Provider 提供的 source/content version 原值；增量语义见 [`lineage_program_identity.md`](lineage_program_identity.md) |
| `batch_id` | materialization/replay snapshot；不是 runtime Run |
| `observed_at` / `updated_at` | 本批次统一观察/更新时间 |
| `is_active` | 是否属于当前 active snapshot |

索引覆盖 source、target、`batch_id + is_active`，并在同一 batch 上按业务 identity
建立 unique index。TMP/DLO/DWO endpoint 在 Business 领域对象层即被拒绝。

### `lineage_issue`

表中保存 AuditFact 与 Issue #36 policy projection 的兼容字段：

```text
environment, source_profile, program_name
issue_type, confidence, rule_version, stable_key
severity, disposition, policy_version
node_key, branch_sink, message, evidence
disposition_updated_at, disposition_updated_by
batch_id, first_seen_at, last_seen_at, is_active
```

`confidence` 是 `HIGH` / `MEDIUM` / `LOW` / `UNKNOWN` 的离散证据充分性，不是
统计概率；`severity`、`disposition`、`policy_version` 属于 policy projection，
不进入 fact stable identity。`IssueLifecycleStatus` 的 `NEW` / `PERSISTING` /
`RESOLVED` 是跨 snapshot reconciliation 结果，不等于 `IssueDisposition` 的
`OPEN` / `ACCEPTED` / `FALSE_POSITIVE` / `RESOLVED`。

索引覆盖 `stable_key`、`batch_id + is_active` 以及 environment/profile/program/
issue_type/active scope。`evidence` 使用 `json.dumps(..., sort_keys=True,
separators=(",", ":"))` 形式保存，不使用 pickle、`repr()` 或 Python `hash()`，也
不会复制完整 `script_code`。上述 DWS 字段语义复用已 CLOSED 的 Issue #36；本节
不改变 SQLite runtime。

### `lineage_batch`

这是 atomic publish 所需的最小 control table，保存 batch 的观察时间、edge/issue
计数和 active 状态。它也能正确表示“成功发布但本批次没有 edge”的 snapshot；否则
只能从业务行反推 active batch。它不是历史 diff 或 Query API。

Phase 7 在同一 batch contract 下新增 `lineage_program_state`，保存程序 identity、
`source_hash`、`pipeline_version`、first/last seen、last changed 与 active 状态；旧
batch 的 edge、issue 和 program state 都保留为 historical snapshot。`pipeline_version`
来自代码中明确维护的 `LINEAGE_PIPELINE_VERSION`，不是 Git commit SHA；只有 hash
和 pipeline version 都相同才会跳过 parser/DAG/audit。

## DWS Materialization Writer

`shared/lineage/materialization_dws.py` 复用同一 `MaterializationBatch`，并由
`imp_lineage_edge.py` 把本轮 rebuilt 的 `ProgramPhysicalDAG` 一并传入。它集中管理
显式 `dwp.<table>` SQL：physical direct rows 写入 `lineage_edge`，Business direct rows
写入 `lineage_business_edge`，program state 与 Issue #36 projection 写入另外两张
事实表。DWS 不在 adapter 内重新执行 parser 或 TMP/DLO/DWO collapse；所有 DWS connection
都来自 `gaussdb.connect_with_profile()`，profile/password 不写入日志。

CLI 默认保持 SQLite；只有 `--store dws --dws-profile <DATABASE_PROFILE>` 才启用
DWS writer。DWS publish 使用 DB-API/JDBC transaction、candidate validation、active
batch join 和 rollback；测试通过 SQLite/mock DB-API connection 覆盖，不连接真实 DWS。
完整字段和五表 contract 见 [`../research/issue-39-dws-materialization-schema.md`](../research/issue-39-dws-materialization-schema.md)。

## Atomic Batch Publish

`SQLiteMaterializationStore.publish()` 或 `DWSMaterializationStore.publish()` 在同一个 transaction 中完成：

```text
BEGIN / DB-API-JDBC transaction
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
    → selected backend atomic publish（默认 SQLite，可显式选择 DWS）
```

它支持注入公开 fixture/mock provider，直接执行时从 local/example provider 配置读取，
不会写入真实连接参数，也不会自动替换旧的 cron 或生产 lineage 入口。

### 定时任务可观测性

定时任务复用了已合并的 progress logging PR #25（`ccb5e60`）的低基数脱敏日志约定，在
`source_load`、`replay`、`incremental_plan`、`build`、`publish` 和 `job` 边界输出已
flush 的阶段状态。build progress 默认每 500 个本轮 rebuild 程序输出一次；日志只包含
count、耗时、受限 batch ID、受控 profile 和异常 class，不输出 program name、源码、
SQL、表名或 connection settings。

默认模式只输出超过阈值的
`stage=build_program status=SLOW`、pathological failure 和 batch summary；SLOW 行带
稳定短 hash、safe `source_profile`、`ordinal`、`elapsed_ms`、`dag_ms`、`audit_ms`、
`materialization_ms`、`physical_nodes`、`physical_edges`、`lineage_edges` 和 `issues`。
其中 `build_program_physical_dag_ms`、`audit_program_physical_dag_ms` 和
`single_program_total_ms` 作为旧 log consumer 的兼容别名保留。使用
`--diagnostic` 才会额外为每个程序输出 `STARTED` 和 `SUCCESS`，用于受控 replay 定位，
不会让正常大规模运行默认产生与程序数成倍的日志。coverage funnel 的聚合行以
`stage=coverage` 单独输出，详见 [`lineage_coverage.md`](lineage_coverage.md)。

直接运行（默认 SQLite）：

```bash
python jobs/crontab/imp_lineage_edge.py --store sqlite
```

DWS 必须显式选择 profile；示例 profile 只使用 placeholder 和环境变量密码：

```bash
python jobs/crontab/imp_lineage_edge.py --store dws --dws-profile <DATABASE_PROFILE>
```

本轮只 rebuild 小规模 sample 时，`build total` 只会是本轮 rebuild 数；日志不会为每个
`ProgramSource` 输出一条记录。默认运行与 controlled replay 的区别如下：

- 不传 `--profile`、`--limit`：保持正常全 provider、complete snapshot 运行；
- `--profile SOURCE_PROFILE`：只选定 source profile；不带 `--limit` 时，只有该
  profile 完整成功扫描才允许其 scope 内的 disappearance / DELETE；其它 profile
  不会受影响；
- `--limit N`：收集选定来源后按 `ProgramIdentity` 排序取前 N 个，只让 sample 进入
  parser/DAG/audit，并强制 partial snapshot，sample 外程序不会判定 `DELETED`；
- `--force-rebuild`：只对本次 replay 选中的程序绕过 hash/version reuse，不代表应该
  对全部生产程序直接执行。

例如第一阶梯可运行：

```bash
python jobs/crontab/imp_lineage_edge.py \
  --store sqlite --profile DEMO_PROFILE --limit 100 \
  --force-rebuild --progress-every 10 --slow-threshold-ms 5000
```

推荐验证顺序为：小规模 `--limit` sample → 单 profile → 多 profile。sample 阶段必须
保持 partial snapshot；单 profile full 阶段不带 `--limit`，由 provider 完整扫描状态
决定是否开放 scoped disappearance。每一级先检查 active edge diff、issue、耗时和
`partial_snapshot`，再扩大范围；不要为了掩盖瓶颈而盲目并发。

日志示例：

```text
stage=job status=STARTED providers=4 replay_mode=normal selected_profiles=- force_rebuild=False partial_snapshot=False
stage=source_load status=SUCCESS sources=<N> elapsed_ms=...
stage=replay status=SELECTED replay_mode=normal selected_profiles=- source_total=<N> replay_total=<N> limit=- force_rebuild=False partial_snapshot=False
stage=incremental_plan status=SUCCESS total=<N> new=<N> changed=<N> unchanged=<N> deleted=0 rebuild=<N> elapsed_ms=...
stage=build status=STARTED total=<N> slow_threshold_ms=5000
stage=build status=RUNNING processed=<N> total=<N> percent=<P> elapsed_ms=...
stage=build_program status=SLOW program_id=<stable-short-hash> build_program_physical_dag_ms=... audit_program_physical_dag_ms=... single_program_total_ms=...
stage=build status=SUCCESS processed=<N> slow_programs=... max_program_elapsed_ms=... avg_program_elapsed_ms=... edges=... issues=... elapsed_ms=...
stage=publish status=SUCCESS batch_id=batch-... edges=... business_edges=... issues=... previous=- elapsed_ms=...
stage=job status=SUCCESS elapsed_ms=...
```

`stage=job status=SUCCESS` 只会在选定 backend 的 atomic publish 完成后出现。中途的 STARTED/RUNNING
日志只表示计算进度，不表示 snapshot 已经发布；失败时会输出
`status=FAILED exception=<ExceptionClass>` 并保留原有异常传播/non-zero 行为。`build`
成功摘要还包括 `program_computation_ms`、`program_materialization_ms`、
`batch_finalize_ms`、`candidate_finalize_ms`、canonicalization/serialization 次数；
`publish` 成功或失败摘要包括 `prepare_ms`、`insert_ms`、`validate_ms`、
`active_switch_ms`、`commit_ms` 及 prepared/validated row count，用于区分计算、
finalize 与 SQLite 写入阶段。可用
`build_program status=SLOW` 判断长尾属于 DAG build、audit 还是 materialization：
`dag_ms` 高时检查 parser/Physical DAG 提取，`audit_ms` 高时检查 audit 遍历和 issue
判定，`materialization_ms` 高时检查 path collapse/evidence finalize；用
`max_program_elapsed_ms`/`avg_program_elapsed_ms` 区分少数长尾与整体变慢。当前实现使用
固定 count progress；如果未来需要在单个程序长时间运行期间提供 heartbeat，可单独增加时间阈值。
Python thread timeout 不能安全中断 CPU-bound pure Python，因此本 Issue 不伪造 thread
watchdog；path enumeration/evidence guard 会先提供 deterministic bounded behavior 或
controlled failure。

## 本阶段边界

可重复的 synthetic scaling benchmark 使用：

```bash
python benchmarks/lineage_materialization_benchmark.py
```

它固定运行 `10`、`100`、`500`、`1000` 条 physical path，输出 elapsed、
canonicalization calls、peak evidence count、完整 `path_count` 和 output bytes；只对
canonicalization operation count 设线性预算，不对机器相关的绝对耗时设 CI 门槛。

Phase 5 本身仍只负责纯 materialization 与 atomic publish；增量、历史、diff、issue
lifecycle 和 legacy decision 由 Phase 7 追加，详见
[`lineage_incremental_history.md`](lineage_incremental_history.md)。

Phase 6 的实现见 [`lineage_query.md`](lineage_query.md)：它只从 active
`lineage_edge` 做窄读取和统一 BFS，不改变 Phase 5 的持久化事实语义；Phase 7 不把
历史结果塞进 Viewer contract。
