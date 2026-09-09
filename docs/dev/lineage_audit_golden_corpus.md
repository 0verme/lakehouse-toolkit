# Audit Golden Corpus（Issue #35）

## Purpose

Golden Corpus 用于可重复地抽取 Audit fact、做人工事实标注、计算
`precision` / `false-positive rate`，并把已标注样本作为 detector regression corpus。
它消费 `AuditFact`、兼容的 `LineageIssue` 或已经脱敏的 candidate manifest；不修改
`ProgramLineageAuditor` 的 detection rule、severity policy、evidence 或 issue identity。
`candidate_from_fact()` 与 `candidate_from_issue()` 都只读取 fact 部分。

当前代码的 `IssueType` 枚举共有七类：

| 类型 | 来源 |
| --- | --- |
| `ORPHAN_BRANCH` | `ProgramLineageAuditor` |
| `MULTI_SINK_CANDIDATE` | `ProgramLineageAuditor` |
| `TARGET_NOT_FOUND` | `ProgramLineageAuditor` |
| `TARGET_MISMATCH` | `ProgramLineageAuditor` |
| `CYCLE_DETECTED` | `ProgramLineageAuditor` |
| `SELF_REFERENCE` | `ProgramLineageAuditor` |
| `LINEAGE_BRANCH_BROKEN` | 现有 evolution/history transition 派生 |

因此直接 Audit replay 目前覆盖六类，history transition fixture 额外覆盖
`LINEAGE_BRANCH_BROKEN`。Golden Corpus 不会把它伪装成直接 detector output。

## Fact correctness 与 business acceptance

Corpus 记录明确拆分两个维度：

- `label`：事实检测正确性。Issue sample 使用 `TRUE_POSITIVE`、`FALSE_POSITIVE`
  或 `AMBIGUOUS`；negative control 使用 `NO_ISSUE`。
- `business_disposition_label`：业务接受语义，目前只定义 `ACCEPTED`。

`ACCEPTED` 不是 `TRUE_POSITIVE` 的别名，也不会进入 precision 或
false-positive rate 的分母。一个合法的 accepted-but-true 样本应同时是：

```text
label = TRUE_POSITIVE
business_disposition_label = ACCEPTED
```

Golden Corpus 的 disposition 字段只是人工标注维度；它不是 runtime disposition
policy、policy version、持久化 disposition storage，也不改变 Audit 结果。

## JSONL corpus format

公开格式是 UTF-8 JSONL，每行一条完整记录，`corpus_version` 当前为
`audit-golden-v1`。抽样输出先保留 `label: null`，人工标注后再填写 label 和
`annotation_reason`。`annotation_reason` 是不含业务 identity 的安全 reason code，
不是存放原始 SQL 或表名的自由文本字段。

```json
{"sample_id":"gc-0123456789abcdef0123456789abcdef","issue_type":"ORPHAN_BRANCH","label":"TRUE_POSITIVE","business_disposition_label":"ACCEPTED","fingerprint":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","evidence_summary":{"edge_count":2,"has_expected_target":true,"node_count":3},"annotation_reason":"intentional_multi_output","source_kind":"synthetic_fixture","corpus_version":"audit-golden-v1","sample_seed":35,"sampling_group":"synthetic-regression","negative_control":false}
{"sample_id":"gc-fedcba9876543210fedcba9876543210","issue_type":null,"label":"NO_ISSUE","business_disposition_label":null,"fingerprint":"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210","evidence_summary":{},"annotation_reason":"normal_program_no_issue","source_kind":"synthetic_negative_control","corpus_version":"audit-golden-v1","sample_seed":35,"sampling_group":"synthetic-regression","negative_control":true}
```

字段约束：

- `fingerprint` 是固定 namespace 下的 lowercase SHA-256 hex；不保存真实
  `program_name`、SQL、table、源码、路径、连接或凭据。
- `evidence_summary` 只允许计数和布尔值，例如 edge/node/sink 数量以及
  `expected_target_written`；不会保存 evidence 中的具体 identity value。
- `issue_type: null` 只允许和 `negative_control: true` 一起使用。
- `source_kind` 只能是安全分类 token，例如 `synthetic_audit`、
  `mysql_profile`，不能填 profile 名或路径。
- schema 是严格版本化的，未知字段（包括误放入的 `program_name`）会被拒绝。
- `sample_id`、`sample_seed`、`sampling_group` 一起存在时，`sample_id` 必须能由
  deterministic sampling contract 重算。

从 fact 或兼容 projection 导出 candidate 时使用：

```python
from shared.lineage.audit_golden import candidate_from_fact, candidate_from_issue

candidate = candidate_from_fact(fact, source_kind="lineage_audit")
# 旧调用方仍可使用 candidate_from_issue(issue, source_kind="lineage_audit")
```

这些函数只取 stable issue identity 的二次 fingerprint 和脱敏 evidence summary；
改变 severity/disposition/policy version 不会改变 candidate fingerprint。

## Sampling contract

输入是内网侧生成的 sanitized candidate JSONL。抽样规则为：

1. 按 `IssueType` 分层；每类最多 `N` 条。
2. 在每层按以下 SHA-256 ranking key 升序排序，取前 `N` 条：
   `corpus_format + corpus_version + sampling_group + seed + issue_type + fingerprint`。
3. `negative_control` 单独成层，按 `negative_control_count` 抽取。
4. 候选少于 `N` 时全部抽取；不重复采样凑数。
5. candidate fingerprint 在一个 manifest 中必须唯一；corpus 的 sample id 和
   fingerprint 也都必须唯一。
6. 不使用 Python 内置 `hash()` 或未约定的随机状态。相同输入、`seed`、分组和
   corpus version 第二次 replay 会生成完全相同的 sample identity；变更 seed 或
   分组会生成不同 identity。

CLI：

```bash
python -m tools.lineage.audit_golden sample \
  --input sanitized_candidates.jsonl \
  --output audit_corpus.jsonl \
  --seed 35 \
  --per-type 10 \
  --negative-control-count 2 \
  --sampling-group prod-2026-q1

python -m tools.lineage.audit_golden validate \
  --input audit_corpus.jsonl \
  --require-labels
```

## Metrics

`metrics` 只接受全部完成事实标注的 corpus：

```bash
python -m tools.lineage.audit_golden metrics \
  --input audit_corpus.jsonl \
  --output artifacts/lineage_audit_golden/audit_metrics.json
```

每个当前 `IssueType` 和 `overall` 都输出：

```text
sample_count
true_positive
false_positive
ambiguous
accepted
precision
false_positive_rate
ambiguous_rate
```

定义如下：

```text
fact_sample_count = TP + FP + AMBIGUOUS
precision = TP / (TP + FP)
false_positive_rate = FP / (TP + FP)
ambiguous_rate = AMBIGUOUS / (TP + FP + AMBIGUOUS)
```

`AMBIGUOUS` 不进入 precision 或 false-positive rate 的分母；没有足够的
`TP + FP` 时 ratio 输出 `null`，不输出伪造的 0 或置信度。这里的
`false_positive_rate` 明确是 **已抽取 issue sample 中的 issue-level false-positive
fraction**，等于 `1 - precision`，不是需要完整总体分母的统计学 population-level
false-positive rate。
置信区间留给后续统计工作，不在本 Issue 扩大 scope。

`sample_count` 的 overall 包含 negative control；每个 IssueType 的
`sample_count` 只包含该类型 issue sample。报告另外输出
`negative_control_count`、`negative_control_no_issue` 和
`negative_control_issue_sample_count`，用于检查正常程序是否意外产生告警。
Negative control 不进入任何 IssueType 的 TP/FP 分母。

## Negative controls

公开 corpus 至少保留一个完全 synthetic、Audit 无告警的程序。它的记录使用：

```text
issue_type = null
negative_control = true
label = NO_ISSUE
```

如果真实正常程序却生成了 Audit issue，应以该程序的 issue candidate 记录事实
并标为 `FALSE_POSITIVE`，而不是把告警藏在 negative-control 计数中。

## Synthetic coverage 与 regression

`tests/fixtures/lineage/audit_golden_programs.py` 复用完全虚构的 Phase 4 SQL，覆盖：

- 正常无告警程序（negative control）；
- `ORPHAN_BRANCH`、`MULTI_SINK_CANDIDATE`、`TARGET_NOT_FOUND`、
  `TARGET_MISMATCH`、`SELF_REFERENCE`、`CYCLE_DETECTED`；
- 一个 synthetic history transition candidate 覆盖 `LINEAGE_BRANCH_BROKEN`；
- `AMBIGUOUS` case；
- `TRUE_POSITIVE + ACCEPTED` case。

`tests/fixtures/lineage/audit_golden_corpus.jsonl` 是已标注、可提交的 regression
corpus。它只含 fingerprint、计数/布尔 evidence summary 和标签，不含真实 identity。
测试会用固定 seed replay sample id/fingerprint，并验证七类当前 `IssueType` 均可
被覆盖。Synthetic fixture 暴露出的 detector 行为只作为 evidence；本 Issue 不在
此处修 detector。

## Privacy

真实内网 candidate manifest 和人工标注结果只留在内网。导出前必须：

- 通过 `candidate_from_issue()` 或同等白名单转换；
- 只保留 irreversible fingerprint、IssueType、safe evidence summary、label、
  reason code、统计字段；
- 删除 `program_name`、source profile 名、SQL、table、源码、文件路径、数据库
  连接和所有 credential；
- 在 `validate` 和 Git 提交前检查 JSONL 中不存在敏感字段。

公开仓库只提交 synthetic fixture 和 sanitized regression corpus。

## 内网标注 workflow

1. 从四个 profile 的 Audit issue 聚合结果生成 sanitized candidate manifest；按
   stable issue identity 二次计算 irreversible fingerprint，只输出安全 evidence
   分类。
2. 用固定 `seed`、每类 `N`、negative-control 数量和 `sampling_group` 做
   deterministic sampling。
3. 把输出 JSONL 复制到内网标注位置；人工填写 `label`、`annotation_reason`，并在
   需要时填写 `business_disposition_label=ACCEPTED`。
4. 运行 `validate --require-labels`，检查字段、IssueType、重复 identity 和
   replay identity。
5. 运行 `metrics`，保存 per-type / overall metrics；`ACCEPTED` 单独解读，不混入
   precision。
6. 用相同输入和 seed 第二次 replay，比较 `sample_id` 与 fingerprint 集合。
7. 通过内部审批后，将只含脱敏字段的标注 manifest/metrics 放入内部 regression
   资产；真实数据不进入 Git 或公开 fixture。

## Versioning 与边界

- `corpus_version` 变化表示字段、fingerprint namespace 或 sampling contract
  变化；应新建版本，不覆盖旧结果。
- detector 规则变化时保留旧 corpus，使用新版本重新抽样/标注并在报告中记录
  detector/pipeline revision；不要重写旧标签。
- #35 corpus 本身不承担 runtime disposition policy；Issue #36 的运行时
  `IssueDisposition`、policy version 和 reference persistence 已在独立层实现；
- 本模块仍不实现 DWS schema/materialization 或新的 Audit IssueType，后者属于
  后续 Issue（尤其是 #39）。
