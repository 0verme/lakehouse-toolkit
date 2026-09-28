# Static Empty Query Block（Issue #143）

## 设计边界

Parser 在每个 SELECT Query Block 上生成轻量 Query Block facts。若该 block 的 predicate 可由
v1 bounded evaluator 静态证明恒假：

```text
Data Lineage：该 block 及其子查询 source 不贡献 PhysicalEdge / LineageEdge
Audit：生成 STATIC_EMPTY_QUERY，并沿用 LineageIssue / lineage_issue lifecycle
```

该事实不推断作者是为了复制 schema 还是误写了条件。不引入 Schema Lineage，也不修改
`PROGRAM_RESULT_USED_AS_SOURCE`、Program Boundary ancestor projection 或 fallback 保护。

`SQLStep.sources` 仍保留已解析的引用供诊断；`SQLStep.lineage_sources` 才是 Physical DAG
可传播的数据 source。恒空 CTE/subquery 只屏蔽自身 block/source 子树，不屏蔽 statement 中
其它独立 Query Block 的 source。

## v1 判定范围

可证明恒假的形式：

- 布尔 literal `FALSE`（允许冗余括号）；
- 两侧均为明确数值 literal 或布尔 literal 的 `=` 比较，且值不等，如 `0 = 1`、`1 = 2`、
  `100 = 200`、`FALSE = TRUE`；
- 顶层 `AND` 中任一子表达式已证明恒假；括号内的 `AND` 可递归识别。

不处理 OR 推导、字符串 literal 比较、算术/函数计算、CAST、NULL 三值逻辑、`NOT` 推导、数据类型
coercion 或完整 SQL 优化。`BETWEEN ... AND ...` 中的 `AND` 不会被误作布尔分支；遇到不确定表达式
保留 Data Lineage。恒真、列比较、column-vs-literal 与运行时条件均不会 suppress。

## Audit contract

`STATIC_EMPTY_QUERY` 通过当前 `AuditFact → AuditPolicy → LineageIssue` 进入 active
`dwp.lineage_issue`：

- `rule_version = audit-rule-v2-static-empty-query`
- `policy_version = audit-policy-v2-static-empty-query`
- `confidence = HIGH`、`severity = MEDIUM`、默认 `disposition = OPEN`
- stable identity 在现有 Program identity 上使用 statement/query-block locator
- evidence 只包含 statement/query-block index、statement type、target/source table identifiers、
  normalized false reason 与 `evaluation = CONSTANT_FALSE`；不写 predicate/完整 SQL/literal。
- 人工 disposition 与 first/last seen、后续 RESOLVED 均复用现有生命周期。

## Pipeline version 与完整重算

从 `lineage-pipeline-v12-authoritative-target-binding` bump 到
`lineage-pipeline-v13-static-empty-query`，因为 persisted Physical/Business lineage facts 改变。
相同 source hash 的旧 ProgramState 由既有版本比较识别为 CHANGED；必须跑完整 SQL snapshot，不能
用 `--limit`，也无需用 `--force-rebuild` 绕过版本迁移。

对 DEV214 的手动顺序：

1. 部署包含 v13 的源码/运行依赖，并确认本地 provider/scope 配置仍指向 DEV214 的既有 profile；
2. 全量重跑 SQL lineage（选择 DEV214 SQL profile，禁止 `--limit`）；
3. Schedule lineage 不受 parser/SQL pipeline 版本影响，可保留现有 active batch；
4. 新 SQL active batch 发布成功后，先 suppression `--dry-run` 检查，再正式重跑 suppression；
5. 无独立持久化 daily reconciliation aggregate 需要迁移。可以重新打开 Web，或用
   `tools.lineage.reconcile_sql_schedule` 读取最新 active SQL/Schedule/suppression snapshots；
   `imp_lineage_daily` 是可选统一编排，会额外重跑 Schedule，并在其后读取受影响 target。

示例命令（使用 DEV214 已配置的 profile 名称，不将 credential 写入命令或仓库）：

```bash
python -B -m jobs.crontab.imp_lineage_edge \
  --config configs/lineage_providers.local.yaml \
  --store dws --dws-profile <DWS_PROFILE> \
  --profile <DEV214_SQL_SOURCE_PROFILE>

python -B -m jobs.crontab.imp_lineage_suppression \
  --config configs/lineage_providers.local.yaml --environment DEV214 --dry-run
python -B -m jobs.crontab.imp_lineage_suppression \
  --config configs/lineage_providers.local.yaml --environment DEV214

python -B -m tools.lineage.reconcile_sql_schedule \
  --dws-profile <DWS_PROFILE> --environment DEV214 \
  --sql-profile <DEV214_SQL_SOURCE_PROFILE> \
  --schedule-profile <DEV214_SCHEDULE_SOURCE_PROFILE> \
  --target <SCHEMA.TABLE>
```

SQL job 必须是该 source profile 的完整快照，等待 atomic publish 成功后再跑 suppression。不要手工
删除/修正 active DWS edge，不要只重跑 Schedule，也不要用 partial sample 代替 migration。

## UI 边界

SQL/schedule reconciliation Web 继续只展示 schedule relationship comparison；`STATIC_EMPTY_QUERY`
不是一种 schedule 差异。本次不把 issue 混入 reconciliation rows，也暂不增加 issue-reader/detail
折叠 UI。当前 Web model 只读取 reconciliation result，增加 audit evidence 展示需要新增 DWS issue
读取与目标关联展示，超出核心 correctness 修复范围；issue 已安全 materialize，可用受控 DWS query
或现有 issue tooling 核验。
