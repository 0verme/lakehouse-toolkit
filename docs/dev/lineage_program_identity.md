# ProgramIdentity / 静态 Job Contract V1

本文件冻结 Issue #38 的程序 identity、程序状态和静态 Job 语义。它建立在
[#3 Phase 1 domain model](lineage_domain_model.md)、[#9 Phase 7 incremental/history](lineage_incremental_history.md)
和 [#37 Dataset Identity](lineage_dataset_identity.md) 的已合并实现上。

本 contract 只描述静态程序定义及其 materialization 历史，不引入 scheduler、
runtime collector、OpenLineage SDK 或 RunEvent。

## 核心边界

```text
Program ≈ static Job definition
Batch   != Runtime Run
```

`Program` 是 provider 当前观察到的一份稳定程序定义：它有环境、来源 profile、
程序名称和 source code，可被 parser/DAG/audit/materialization 消费。它不是某一次
调度触发、worker 执行或重试。

`Batch` 是一次 lineage materialization/replay 的 candidate snapshot 标识。一个
batch 可以包含多个程序的 state、edge 和 issue；它用于 candidate、atomic publish
和历史比较，但不表示任何程序实际运行过。当前领域模型没有 runtime execution
对象，也没有 run status、attempt、worker、scheduler runtime id 或执行时间语义。

## ProgramIdentity

### Canonical identity

当前 V1 的 canonical identity 严格只有：

```text
(environment, source_profile, program_name)
```

代码中的 `shared.lineage.domain.ProgramIdentity` 是不可变 value object。三个字段
在 identity boundary 做 surrounding whitespace trim，保留原有大小写；这与
`DatasetIdentity` 对 schema/table 的 `upper()` canonicalization 不同，不能为了
机械对齐而悄然改写已有 program/profile identity。

```python
from shared.lineage import ProgramIdentity

identity = ProgramIdentity(" DEV ", " profile_a ", " DEMO_JOB ")
identity.key
# ("DEV", "profile_a", "DEMO_JOB")
identity.to_dict()
# {
#     "environment": "DEV",
#     "source_profile": "profile_a",
#     "program_name": "DEMO_JOB",
# }
```

`key` 是脱离数据库 surrogate id 的 canonical tuple；`to_dict()` 是稳定 JSON
payload。需要文本传输时，调用方应对该 mapping 使用明确的 canonical JSON 规则
（`ensure_ascii=False`、`sort_keys=True`、紧凑 separators），而不是依赖 dataclass
`repr()`、Python `hash()` 或数据库自增 `id`。

V1 不把以下字段加入 `ProgramIdentity`：

- `platform`；
- `catalog`、database、schema 等 Dataset namespace；
- scheduler runtime id；
- `batch_id`；
- `source_hash`；
- absolute path 或 machine path；
- 当前没有稳定权威来源的 `job_key`。

### Identity truth table

| 变化 | `ProgramIdentity` | 说明 |
| --- | --- | --- |
| `DEV / profile_a / DEMO_JOB` → 相同值 | 相同 | 同一个 static program |
| environment 改变 | 不同 | environment 是 hard graph boundary |
| source_profile 改变 | 不同 | profile 是该 Program contract 的 identity 维度 |
| program_name 改变 | 不同 | 不自动推断 rename |
| 仅 source_hash 改变 | 相同 | 同一程序的新 source/content version |
| 仅 pipeline_version 改变 | 相同 | 同一程序需要按新 pipeline 重新计算 |
| 仅 batch_id 改变 | 相同 | 新 materialization snapshot，不是新程序 |
| 仅 job_key 改变 | 相同 | job key 当前只属于 lineage fact provenance |

因此 synthetic 程序：

```text
DEV / profile_a / DEMO_JOB
DEV / profile_b / DEMO_JOB
```

是两个不同的 static program identity。虽然两者可以拥有相同的 source content 和
相同 `source_hash`，也不能因名称相同而合并；它们分别属于不同 collection
profile。

这与 #37 有意不同：`DatasetIdentity` 是
`environment / canonical_schema / canonical_table`，`source_profile` 对
Dataset 是 provenance/filter，而不是 Dataset identity。两个 identity 不应被
机械复制成同样字段集合。

## ProgramSource 与 ProgramState

### ProgramSource

`ProgramSource` 是 provider 到 parser 的静态输入，必填：

```text
environment
source_profile
program_name
script_code
```

`expected_target` 是可选的 declared target hint；`source_hash` 是 provider 在有
能力时提供的 source/content state。`ProgramSource.identity` 只投影前三个 identity
字段，不把 code、target 或 provider connection 信息投影进去。

当前 provider 使用既有 `compute_source_hash()`：对
`program_name`、`script_code`、`expected_target` 做固定 canonical JSON 后计算
SHA-256。它不包含 environment、source_profile、host、password、connection id、
读取时间或 batch id。因此相同 source content 可以在两个 profile 中得到相同
hash，但仍然是两个不同 `ProgramIdentity`。由于 `program_name` 在 hash 输入中，
rename 通常也会产生不同 hash；但 rename 的判断仍以 identity 变化和 snapshot
lifecycle 为准，不能用 hash 相似度反推 alias。

### ProgramState

`ProgramState` 是某个 static program 在一个 materialization snapshot 中的持久化
状态，字段为：

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

它通过前三个字段重新得到 `ProgramIdentity`。`first_seen_at`、`last_seen_at`、
`last_changed_at` 是观察/版本历史；`batch_id` 和 `is_active` 是 materialization
state。历史 batch 追加保存，当前 active snapshot 通过 active flag 表示。

`pipeline_version=None` 兼容旧 schema 或旧手工 state；planner 对缺少版本的 state
保守地要求 rebuild，成功发布后写入当前 pipeline version。

## 字段分类

| 字段 | 分类 | V1 语义 | 禁止解释 |
| --- | --- | --- | --- |
| `environment` | identity + graph boundary | Program 和 Dataset 的环境隔离边界 | 不自动做 DEV/PROD logical mapping |
| `source_profile` | identity + provenance/filter | Program 的 collection profile；区分同名跨 profile 程序 | 不等价于 Dataset identity 的 schema/catalog |
| `program_name` | identity + fact provenance | static program 的名称；大小写按现有语义保留 | 不自动当作稳定 scheduler job id |
| `source_hash` | source/content version | source code/target hint 的 provider hash；用于增量复用判定 | 不是 ProgramIdentity、batch 或 runtime run |
| `pipeline_version` | pipeline semantic version | parser、Physical DAG、audit、TMP collapse/materialization 语义版本 | 不是 ProgramIdentity，也不是 Git commit SHA |
| `batch_id` | materialization state/provenance | candidate/replay/publish snapshot 的 identity | 不是 ProgramIdentity，也不是 runtime Job Run |
| `job_key` | optional provenance | 当前由 materialization `job_keys` mapping 可选传入，保留在 `LineageEdge` fact；该 mapping 按 `program_name` 查找 | 没有稳定权威来源时不得升级为 identity |

当前 `job_keys` 不是 Provider 输出，也没有 environment/profile-scoped 的权威
registry；同名程序跨 profile 的 mapping 不能证明它们共享一个 Job identity。

当前没有 `runtime execution` 字段。未来若要记录运行，只能新增独立 runtime
contract；不能复用 `batch_id`、`job_key` 或 `ProgramState.is_active` 伪造运行事实。

## Incremental planner contract

`plan_incremental()` 只比较当前 `ProgramSource` 和上一个 active
`ProgramState`，identity 先匹配，再按 hash/version 分类：

| planner status | 条件 | 结果 |
| --- | --- | --- |
| `NEW` | 没有相同 active identity | 进入 parser/DAG/audit/materialization |
| `UNCHANGED` | 当前非空 `source_hash` 等于 state hash，且 pipeline version 相等 | 跳过重建并复用旧 facts |
| `CHANGED` | identity 相同但 hash 改变、hash 缺失或 pipeline version 改变 | 要求 rebuild |
| `DELETED` | 只在声明的 complete snapshot scope 内缺失 | 从新 active candidate 移除，旧历史保留 |

空 hash 或 `None` 永远不能产生 `UNCHANGED`。同一 identity 的状态更新必须保留
`first_seen_at`；只有 changed/new rebuild 才把 `last_changed_at` 更新到本次观察点。

### Source change

```text
same ProgramIdentity
source_hash A → B
```

分类为 `CHANGED`，重建该程序，新的 `ProgramState` 携带 B；这不是 rename，也不
创建第二个静态程序 identity。

### Pipeline change

```text
same ProgramIdentity
same source_hash
pipeline_version 1 → 2
```

分类仍为 `CHANGED`，因为 parser/DAG/audit/materialization 结果可能随语义版本
改变。`pipeline_version` 必须由源码中的
`LINEAGE_PIPELINE_VERSION` 显式维护，不从 Git SHA 推导；只改变日志文案或运行
参数不需要 bump。

## Rename、delete、restore

### Rename

V1 不做 alias 或 rename inference。对：

```text
OLD_PROGRAM → NEW_PROGRAM
```

第一阶段的确定性结果是：

```text
old identity → DELETED（在 complete scoped snapshot 中）
new identity → NEW
```

planner 不比较源码相似度、path、hash 或 target 来猜测两者是同一个程序。若未来
需要 alias，必须另立显式 contract、来源字段和迁移方案，不能在本 V1 中隐式加入。

### Complete delete / restore

对 scope `DEV / fixture`：

```text
active
  → complete scoped snapshot 中消失
  → 不进入新 active candidate，旧 batch/state 变为 inactive
  → 再次出现
  → 作为 NEW identity 建立新的 active state
```

删除不会物理删除旧 `lineage_edge`、issue 或 program state；它们仍可按历史
`batch_id` 读取。restore 因为没有 active state 可匹配，`first_seen_at` 从恢复观察
点重新开始，而不是把旧 identity 自动续接成一次 rename/执行历史。

### Partial replay

partial snapshot、provider 异常或没有明确完整 scope 时，不能产生
`DELETED`。未读取的程序及其 facts 会保留在新 active candidate（state/facts
rebase 到新 batch），因此：

```text
只 replay PROGRAM_A
≠
PROGRAM_B 已删除
```

只有 provider 成功完成明确 `SnapshotScope(environment, source_profile)` 的全量
扫描，才允许在该 scope 内对缺失 identity 判定删除；scope 外 identity 始终保留。

## Batch 与 materialization history

`batch_id` 标识一次 materialization/replay candidate：

```text
ProgramSource(s)
    → planner
    → rebuilt / retained facts
    → MaterializationBatch(batch_id)
    → atomic publish
```

同一个 batch 可以保存许多 ProgramState 和 lineage facts；同一个 ProgramIdentity
也可以出现在多个历史 batch 中。SQLite reference adapter 在 publish 时切换
active batch，旧 batch 的行保留为历史并标记 inactive。失败的 candidate 在 atomic
transaction rollback 后不能替换原 active batch。

这表示 materialization/history 状态，不表示：

- scheduler 是否触发了 Job；
- Job 是否开始、成功、失败或重试；
- 哪台机器或哪个 worker 执行；
- OpenLineage `Run` 或 `RunEvent`。

## 未来 OpenLineage Job mapping（仅 contract boundary）

未来若接入 OpenLineage，建议将静态 `ProgramIdentity` 映射为 Job 的
`namespace/name`：

```text
Job.namespace = lakehouse://<environment>/<source_profile>
Job.name      = <program_name>
```

这是概念映射，不是本 Issue 的 exporter 实现。namespace 的组件在实际编码时必须
使用明确的 URI escaping/canonical serialization；不能直接拼接未经编码的任意
用户输入。

该选择保留 environment hard boundary，也保留 profile 参与 ProgramIdentity 的
区分。`source_hash`、`pipeline_version`、`batch_id` 和 `job_key` 不进入
`namespace/name`：它们未来最多作为 source/version/provenance facet，且必须由
各自 contract 决定。尤其禁止：

```text
batch_id → OpenLineage Run
job_key  → 未验证的 Job identity
```

本 Issue 不引入 OpenLineage SDK、不创建 RunEvent、不收集 runtime execution，也
不实现 #46 exporter。

## Compatibility review

- **#3**：继续使用 `ProgramSource` 的 environment/profile/name/code/target/hash
  字段；`ProgramIdentity` 仍是三元组，没有改变旧 domain object 的字段语义。
- **#9**：保留 `NEW/UNCHANGED/CHANGED/DELETED`、active state、complete scope、
  partial replay、历史 batch 和 atomic publish 规则；hash 相同但 pipeline version
  不同仍 rebuild。
- **#37**：两者都以 environment 作为硬边界、都可脱离数据库 surrogate id 识别，
  但 Dataset 的 `source_profile` 仍是 provenance，而 Program 的 `source_profile`
  是 identity 维度；这是有意的字段差异。
- **现有数据**：不改变已有 ProgramIdentity 的三元组值，不回写历史 identity，不
  引入数据库迁移；`lineage_program_state.pipeline_version` 已按旧 schema 的
  nullable compatibility 规则处理。

## 对 #39 的输入与非目标

#39 可以依赖本文件得到稳定的 static Program/Job identity，并从 active
`ProgramState` 判断 source/pipeline 是否需要重新 materialize；它不能把
`batch_id` 当作 runtime run，也不能把 DWS DDL/materialization writer 的设计
倒灌到本 Issue。

本 Issue 明确不做：

- DWS DDL 或 materialization writer（#39）；
- parser、SQL extraction、Physical DAG 或 Audit 改造；
- lineage closure；
- OpenLineage exporter/RunEvent（#46）；
- scheduler runtime semantics；
- platform/catalog/job dimensions 的臆测扩展。
