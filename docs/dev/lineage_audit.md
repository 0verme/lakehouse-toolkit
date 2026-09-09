# Lineage Phase 4：Physical DAG 审计与异常检测

Phase 4 只消费 Phase 3 的 `ProgramPhysicalDAG`，把已经确认的程序事实转换为
结构化 `LineageIssue`。它是只读观察者，不重新解析 `ProgramSource.script_code`，
也不修复或重写 Physical 图。

## API

```python
from shared.lineage import audit_program_physical_dag, build_program_physical_dag

physical_dag = build_program_physical_dag(program_source)
audit = audit_program_physical_dag(physical_dag, observed_at=observed_at)
```

也可以使用等价的 facade：

```python
from shared.lineage import ProgramLineageAuditor

audit = ProgramLineageAuditor().audit(physical_dag, observed_at=observed_at)
```

`LineageAuditResult` 是 frozen result，包含：

- `dag`：原样返回的输入 `ProgramPhysicalDAG`；
- `issues`：按稳定排序返回的 `tuple[LineageIssue, ...]`；
- `expected_target`：本次审计使用的标准化 target；
- `target_reachable_nodes`：能够沿 `source → target` 方向到达 expected target
  的节点，按名称排序；
- `orphan_branch_sinks`：被识别为孤儿 branch terminal 的 sink，按名称排序。

Audit 不会改变 `dag.nodes`、`dag.edges`、`dag.steps` 或 `dag.sinks`。所有 reverse
adjacency 都是调用期间建立的内部索引，PhysicalEdge 的方向仍然是
`source=upstream`、`target=downstream`。

## 当前 IssueType

当前 `IssueType` 枚举共有七类。`ProgramLineageAuditor` 直接生成前六类；
`LINEAGE_BRANCH_BROKEN` 是现有 evolution/history transition 在旧分支曾到达
expected target、当前变成 orphan 时生成的派生 issue，不是本次 Audit detector 新增的
规则：

| IssueType | 触发语义 | Severity |
| --- | --- | --- |
| `ORPHAN_BRANCH` | 已知且实际写入的 expected target 存在时，某个 terminal branch 无法到达该 target | `MEDIUM` |
| `MULTI_SINK_CANDIDATE` | `dag.sinks` 中有多个终止写入候选 | `MEDIUM` |
| `TARGET_NOT_FOUND` | expected target 未被实际写入，且没有其它明确正式 sink | `HIGH` |
| `TARGET_MISMATCH` | expected target 未被实际写入，且存在其它明确正式 sink 替代它 | `HIGH` |
| `CYCLE_DETECTED` | 一个多节点 strongly connected component（SCC） | `HIGH` |
| `SELF_REFERENCE` | 存在 `A → A` 的 PhysicalEdge | `HIGH` |
| `LINEAGE_BRANCH_BROKEN` | 既有有效 target 分支在后续 snapshot 中断裂，由 evolution/history 派生 | `HIGH` |

Severity 由 `ISSUE_SEVERITY_POLICY` 集中定义，并通过 `issue_severity()` 查询；
同一 `IssueType` 不会由不同 detector 随意赋予不同等级。

## Sink 与 target 规则

审计直接使用 Phase 3 的 `dag.sinks`。节点的 `PhysicalNodeKind` 区分
`formal_sinks` 与 `temporary_sinks`，不会把 `TMP_UNUSED` 误称为正式结果表。
`MULTI_SINK_CANDIDATE` 仍会保留 TMP sink，因为多个终止写入本身是需要审计的事实。

审计同时保留两个不同的 target fact：

- `expected_target_written`：expected target 是否出现在实际 SQL step target 或
  PhysicalEdge target 中，表示业务写入事实；
- `expected_target_is_sink`：expected target 是否出现在 `dag.sinks` 中，表示
  graph-terminal fact。

后者不是前者的替代品。self-loop、downstream write 或其它 branch 都可能让一个
已经写入的 target 不是 graph terminal sink；这不应自动等价于 target 写错。

`expected_target` 的规则如下：

`expected_target` 也可以由 profile 显式启用的
`program_name -> declared primary target` 策略产生；此时它仍只是 primary
result hint，不是唯一结果声明。所有实际 terminal sink 都保留并参与
`MULTI_SINK_CANDIDATE` 审计。

1. `expected_target is None`：没有权威 target，不生成
   `TARGET_NOT_FOUND`、`TARGET_MISMATCH` 或 `ORPHAN_BRANCH`；仍可生成 sink、cycle
   和 self-reference issue。
2. expected target 是 sink：认为 target 已正确成为最终写入，不生成 target issue。
3. expected target 已被写入但不是 sink：不生成 `TARGET_MISMATCH`。self-reference
   由 `SELF_REFERENCE` 表达；存在 downstream 或其它不能到达 expected target 的
   terminal branch 时由 `ORPHAN_BRANCH` 表达；多个 terminal sink 仍由
   `MULTI_SINK_CANDIDATE` 表达。
4. expected target 未被写入：如果存在明确正式 sink，生成一个
   `TARGET_MISMATCH`；否则生成一个 `TARGET_NOT_FOUND`。因此同一事实不会机械地
   同时产生两个 target issue。

例如 `expected_target=DWA.DEMO_RESULT`、实际 sink 为
`DWA.DEMO_OTHER` 且 expected target 从未写入时是 `TARGET_MISMATCH`，不是
`TARGET_NOT_FOUND`。如果实际只写入 `TMP_1`，则是 `TARGET_NOT_FOUND`。
如果 `DWA.DEMO_RESULT` 已写入后又 downstream 写入 `DWA.DEMO_OTHER`，则
expected target 的 `written` fact 为真、`sink` fact 为假；该 terminal branch
由 `ORPHAN_BRANCH` 表达，不再重复生成 `TARGET_MISMATCH`。

## ORPHAN_BRANCH

只有在 expected target 已知且存在可靠写入事实（出现在 SQL step target 或
PhysicalEdge target）时才执行 orphan 检测。审计从 expected target 沿 reverse
edges 做 BFS，得到所有能到达 target 的节点集合；不在该集合中的 terminal sink
按 sink 分别形成 branch。

每个 branch 只产生一个 issue，使用 branch terminal 作为 `branch_sink`。例如：

```text
ODS.DEMO_A → TMP_1 → DWA.DEMO_RESULT
ODS.DEMO_X → TMP_X1 → TMP_X2
```

只产生一个 `branch_sink=TMP_X2` 的 `ORPHAN_BRANCH`，不会为 `ODS.DEMO_X`、
`TMP_X1`、`TMP_X2` 分别报警。反向 branch evidence 会包含稳定排序的
`branch_nodes`、`branch_edges`、`branch_edge_pairs`、`entry_sources` 和
`expected_target`。共享上游节点可以出现在多个 branch 的 evidence 中；branch
的身份仍由 terminal sink 区分。

TMP 不是 orphan 的充分条件。`TMP_X1 → TMP_X2 → DWA.DEMO_RESULT` 能够到达
expected target，因此不会因为节点类型是 temporary asset 而报警。相反，若
`DWA.DEMO_RESULT → DWA.DEMO_OTHER`，`DWA.DEMO_OTHER` 是无法沿业务方向到达
expected target 的 terminal branch，会生成一个 branch-level `ORPHAN_BRANCH`，
而不是把已写入的 expected target 再标成 `TARGET_MISMATCH`。

## Cycle 与 self-reference

`CYCLE_DETECTED` 使用迭代式 Kosaraju SCC 遍历，所有邻接访问都有 visited/assigned
保护，不会因 cycle 无限遍历。每个独立的多节点 SCC 生成一个 issue，
`cycle_nodes`、`cycle_edges` 和 `cycle_edge_pairs` 均稳定排序。

`A → A` 单独生成一个 node-level `SELF_REFERENCE`，`node_key=A`，不会因为单节点
SCC 再生成 `CYCLE_DETECTED`。多节点 SCC 中若同时含有 self edge，则两种 issue
分别表达两种事实，可以同时存在。

## Stable key 与 lifecycle

`LineageIssue.stable_key` 是由 `compute_lineage_issue_stable_key()` 计算的
SHA-256 hex identity，不使用 Python `hash()`、时间或 message 文案。

- Program-level：`environment + source_profile + program_name + issue_type`；
- `SELF_REFERENCE`：再加 `node_key`；
- `ORPHAN_BRANCH`：再加 `branch_sink`；
- `CYCLE_DETECTED`：再加 canonical sorted SCC node set。

因此 sink 列表或 evidence 更新不会仅仅因为 message/evidence 变化而自动创建新的
program-level identity；不同 branch、self node、cycle SCC 会得到不同 key。
`issue_key` 和 `fingerprint` 是该字段的兼容别名。

Audit 入口只生成一次默认 `observed_at`。每条新产生的 issue 都初始化为：

```text
first_seen_at = observed_at
last_seen_at  = observed_at
is_active     = True
```

调用方可以注入固定的 `datetime`，便于测试和批次边界控制。本阶段不读取历史
issue、不继承旧 timestamp、不做 inactive reconciliation、30 天判断或 diff；
这些需要后续 Persistence/History Phase。

## Evidence 与阶段边界

Issue evidence 是结构化 mapping，集合和边都稳定排序，并优先复用 PhysicalEdge
已有的 `statement_index`、`statement_indices`、raw/normalized source-target、
line/column 和 `statement_type`。不会复制完整 `script_code`。

Phase 4 明确不实现：

- TMP Collapse；
- `LineageEdge` materialization；
- `lineage_edge` / `lineage_issue` 数据库表或 batch publish；
- upstream/downstream query、Blast Radius、Viewer、closure；
- incremental、history、diff 或长期 orphan lifecycle。

因此下一个阶段可以直接复用 `audit.dag`、`target_reachable_nodes`、
`orphan_branch_sinks` 和结构化 `issues`，再决定哪些有效 Physical 路径折叠为
正式业务血缘。
