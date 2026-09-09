# Audit Fact / Severity / Disposition Contract（Issue #36）

## Data flow

```text
Physical DAG / SQL evidence
          |
          v
ProgramLineageAuditor.detect_facts()
          |
          v
AuditFact（issue_type / confidence / rule_version / stable identity）
          |
          v
AuditPolicy.evaluate() / replay()
          |
          v
AuditPolicyResult（severity / disposition / policy_version）
          |
          v
LineageIssue（兼容 projection）
          |
          v
SQLite lineage_issue + incremental/history
```

Audit 仍然是生产 lineage 主链中的只读观察者：

```text
ProgramSource → SQL extraction → SQLStep → Physical DAG → Audit → LineageEdge → Materialization
```

Audit 不修改 Physical DAG，不折叠 TMP，也不决定 `LineageEdge` 是否生成。

## Fact contract

`AuditFact` 是 detector 的 canonical 输出，包含：

- `issue_type`：当前冻结的七类事实类型，不因 policy 增加 IssueType；
- `confidence`：`HIGH`、`MEDIUM`、`LOW`、`UNKNOWN` 的离散证据充分性判断，
  不是统计概率，也不表示经过 calibration 的 precision；
- `rule_version`：解释 detector 规则和 fact 语义的版本；规则语义变化时保留旧
  corpus/history 并使用新版本 replay；
- `message`、结构化 `evidence`：解释事实的可变表达；
- `stable_key`：跨进程 stable issue identity。

`AuditFact` 不包含 `severity`、`disposition`、`policy_version`、批次时间或
`is_active`。`severity` 不是 fact，`disposition` 也不是 fact。

当前 `compute_lineage_issue_stable_key()` 的 identity contract 为：

- program-level：`environment + source_profile + program_name + issue_type`；
- `SELF_REFERENCE`：再加 `node_key`；
- `ORPHAN_BRANCH` / `LINEAGE_BRANCH_BROKEN`：再加 `branch_sink`；
- `CYCLE_DETECTED`：再加 canonical sorted SCC node set。

identity 不包含 message、evidence、confidence、rule_version、severity、
`disposition`、时间或 Python `hash()`。因此只有 branch/node/cycle 的核心语义变化
才会改变 identity。

## Policy contract

`AuditPolicy` 只消费 `AuditFact`，输出 `AuditPolicyResult`：

- `severity`：当前风险分级，默认保留现有 `ISSUE_SEVERITY_POLICY`；
- `disposition`：默认 `OPEN`，也可以按 IssueType 配置默认处置；
- `policy_version`：解释本次风险/处置计算的版本。

```python
from shared.lineage import AuditPolicy, IssueDisposition

policy = AuditPolicy(
    severity_by_issue_type={"ORPHAN_BRANCH": "LOW"},
    default_disposition=IssueDisposition.OPEN,
    policy_version="audit-policy-v2",
)
issues = policy.project(facts, batch_id="batch-2", observed_at=observed_at)
```

`audit_program_physical_dag(..., policy=policy)` 和
`materialize_program(..., policy=policy)` 是兼容 facade；它们先得到同一份
facts，再生成 `LineageIssue` projection。`detect_audit_facts()` /
`ProgramLineageAuditor.detect_facts()` 不读取 severity policy。

policy replay 不需要重新解析 SQL 或构建 DAG：

```python
from shared.lineage import replay_audit_policy

replayed = replay_audit_policy(old_issues, policy, batch_id="batch-policy-v2")
```

同一 facts 使用不同 policy 时，fact 集合、evidence 和 stable identity 不变，只有
projection 的 severity/disposition/policy_version 可以变化。

## Disposition contract

`IssueDisposition` 当前只保留四个必要状态：

| 状态 | 语义 |
| --- | --- |
| `OPEN` | fact 当前存在，尚未确认或接受处置；默认 policy 结果 |
| `ACCEPTED` | fact 可以是真实事实，但业务明确接受当前风险；不等于 Golden Corpus 的 `TRUE_POSITIVE` |
| `FALSE_POSITIVE` | detector fact 在业务复核后被判定为误报；保留原始 fact，不删除 history |
| `RESOLVED` | fact 在后续完整 snapshot 中不再出现，或显式标记已解决；这是处置/生命周期投影，不改变 stable identity |

`IssueLifecycleStatus`（`NEW` / `PERSISTING` / `RESOLVED`）描述跨 snapshot 的出现、
持续和消失；`IssueDisposition` 描述业务处置，二者不是同一个 enum。缺失 fact 的
history reconciliation 会生成 `is_active=False`、`disposition=RESOLVED` 的 projection；
fact 再次出现时按当前 policy 重新打开，除非已有人工 `ACCEPTED` / `FALSE_POSITIVE`
决定需要继续保留。

人工处置通过不可变新 batch 进入 persistence/history，不原地更新旧 row：

```python
store.set_issue_disposition(
    stable_key,
    IssueDisposition.ACCEPTED,
    batch_id="batch-manual-1",
    observed_at=observed_at,
    updated_by="reviewer",
)
```

该 API 不是 UI 或工单系统；它只创建带 `disposition_updated_at` /
`disposition_updated_by` 的新 reference snapshot。相同 stable identity 的人工
`ACCEPTED` / `FALSE_POSITIVE` 会在后续 policy replay 和 lifecycle reconciliation
中保留，旧 batch 仍可读取。

## Golden Corpus boundary

Golden Corpus 的 `TRUE_POSITIVE`、`FALSE_POSITIVE`、`AMBIGUOUS`、`NO_ISSUE` 是
**事实检测正确性标签**，用于 precision / false-positive rate；它们不是 runtime
`IssueDisposition`。例如：

```text
label=TRUE_POSITIVE + business_disposition_label=ACCEPTED
```

表示 fact 真实但业务接受风险；`ACCEPTED` 不进入 precision 分母。`candidate_from_fact()`
和现有 `candidate_from_issue()` 都只读取 fact identity/evidence summary，policy
变化不会改变 sanitized candidate fingerprint 或 sample replay。

## SQLite / old data compatibility

SQLite reference schema 从 v2 升级到 v3；迁移只追加 nullable/defaulted columns，旧的
v2 column-list reader/writer 仍可工作，因此旧 batch 可读取，新 policy 可另建 batch
回放，失败 publish 仍由 transaction rollback。`lineage_issue` 增加：

- `confidence`；
- `rule_version`；
- `disposition`；
- `policy_version`；
- `disposition_updated_at`；
- `disposition_updated_by`。

旧 row 不被重写：缺失字段读取为 `UNKNOWN`、`audit-rule-legacy`、`OPEN`、
`audit-policy-legacy` 和空人工审计字段。没有 stable key 的 legacy issue 在
reconciliation/replay 时按同一 semantic key 推导 canonical identity，避免因升级
产生假新增/假解决。

`SQLiteMaterializationStore.replay_issue_policy()` 从旧 batch 复制 edges、issues、
program states，应用新 policy 并以新 batch 原子 publish；旧 batch 和 history 保留。
缺失 fact 的 `RESOLVED` projection 也会写入新 history row，但保持 `active_only`
不可见。普通 incremental run 传入新 policy 时，UNCHANGED 程序只重投影已有 issues，
不触发 Physical DAG rebuild。LineageEdge 的表结构、identity、TMP collapse 和
materialization 语义均未改变。

## Non-goals

- 不新增大量 IssueType；
- 不修改 parser、DatasetIdentity、ProgramIdentity 或 LineageEdge materialization；
- 不建设 DWS DDL、UI、通知或工单系统；
- 不把 confidence 当成统计概率；
- 不把 Audit 变成 materialization gate；
- 不在本 Issue 开始 #39 的 DWS Materialization Contract。
