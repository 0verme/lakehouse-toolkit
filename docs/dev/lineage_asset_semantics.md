# Lineage 资产语义冻结：Program Result、命名与业务边界

本文冻结 Issue #121 确立的核心血缘语义。它统一 Domain / Parser / Physical DAG /
Business Lineage / DatasetIdentity / Program Inventory / Suppression /
Materialization 的边界，并明确废除历史上的 **TMP 命名推断**。

相关文档：

- [`lineage_program_name.md`](lineage_program_name.md)（Issue #44 canonical grammar）
- [`lineage_dataset_identity.md`](lineage_dataset_identity.md)
- [`lineage_physical_dag.md`](lineage_physical_dag.md)
- [`lineage_materialization.md`](lineage_materialization.md)
- [`lineage_business_asset_boundary.md`](lineage_business_asset_boundary.md)
- [`lineage_reconciliation.md`](lineage_reconciliation.md)

## 1. Program Result Authority

**只有 active `005` program_name 的第二段是正式 Program Result。**

```text
005:<logical_target>:<step_seq>:<opaque_suffix>
```

- `logical_target` 是第二段经过显式 namespace normalization 后的
  `schema.table`，例如 `005:DWS_DWM.RESULT_A:1:00 -> DWM.RESULT_A`。
- namespace normalization 只复用代码中显式维护的 registry
  （`DWS_DWF.X -> DWF.X`、`DWS_DWM.X -> DWM.X`、`DWS_DWP.X -> DWP.X` …）。
- **禁止** fuzzy guessing：不按 basename 猜 schema，不做相似度匹配，不新增隐式
  registry。

`001`、`002`、`ABC`、无冒号普通名字都不提供 Program Result authority，
`parse_program_name()` 仍然只承认 `005`。

### Program Inventory（`dwp.lineage_program_state`）

Program Inventory 的唯一事实源是 active `ProgramState`，grammar 固定为：

```text
005:<qualified_schema_table>[:...]
```

| 输入 | 行为 |
| --- | --- |
| `005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00` | 进入 inventory：`DWP.TMP_P_REPORT_KYW_LIST` |
| `005:` / `005:ABC` / `005:not-qualified` | malformed 005 => `ReconciliationSuppressionError` => scope fail-open |
| `001:` / `002:` / `ABC:` / 普通名字 | 不是 Program Result 声明 => 直接 skip，不报错、不 fail-open |

后果链保持 conservative：

```text
ReconciliationSuppressionError
        => scope fail-open
        => 不 publish 新 suppression
        => 不 retire 旧 suppression
```

`PROGRAM_INVENTORY_PREFIXES` 只是由 `PROGRAM_NAME_LEGACY_MARKER` 派生的兼容常量，
它不再表示“多 prefix Program Result 协议”，值恒为 `frozenset({"005"})`。

### 与 SQL 冲突时的优先级

`005` program_name 与程序真实 SQL 最终 sink 冲突时，**仍然以 005 program_name
为准**。差异属于治理 / 数据质量问题，parser 不得自动改写 target authority。
Audit 仍可产生 `TARGET_MISMATCH` / diagnostic，但 authority 不变。

SQL 存在多个结果表时（`A -> RESULT_A`、`A -> RESULT_B`），只有 `005` 第二段声明
的表获得 Program Result authority；其他 sink 继续作为 SQL / Physical lineage fact
保留。

## 2. Naming Is Not Semantics

**`TMP` / `TEMP` / `STG` / `TEST` 命名本身没有任何血缘语义。**

名称单独出现时不能证明：

```text
temporary asset
formal asset
program result
business asset
technical asset
```

因此核心流程中不存在任何 name-based temporary classification：

| 位置 | 现在只依据 |
| --- | --- |
| `PhysicalNodeKind` | 显式 `CREATE TEMP/TEMPORARY TABLE` fact 或调用方显式传入的 `kind`；否则使用中性默认值 `FORMAL_ASSET` |
| `DatasetIdentity` | `environment + canonical_schema + canonical_table`；`DWP.TMP_X` 与其它 qualified 表名完全等价 |
| `LineageEdge` | business boundary / Program Result / schema boundary；名称不参与拒绝 |
| `is_business_asset()` / `is_technical_asset()` | 只有显式 schema/layer registry（`PRE_BUSINESS_ASSET_SCHEMAS`） |
| Program Inventory / target normalization | `schema.table` syntax validation + 显式 namespace mapping |

- `TMP_X -> DWM.RESULT_A` 但没有 `005:DWM.TMP_X:...` 时，系统只能知道
  “`DWM.TMP_X` 不是已登记的 Program Result”。它可能是手工码值、中间表、所谓临时表
  或其他用途；系统不做命名猜测，也不做特殊处理。
- 某表只被其他程序引用（`program = 005:DWM.RESULT_A:1:00` 且 SQL 经过
  `DWM.TMP_X`）不能证明它是 Program Result；只有存在 `005:DWM.TMP_X:...` 才授予
  Program Result 身份。

### 兼容壳

```text
PhysicalNodeKind.TEMPORARY_ASSET
is_temporary_asset()
TemporaryAssetRule
DEFAULT_TEMPORARY_ASSET_RULES
```

这些 API 仍然保留，但 `DEFAULT_TEMPORARY_ASSET_RULES` 为空元组，默认调用对任何名称
都返回 `False`。只有调用方显式提供证据型规则时 `is_temporary_asset()` 才可能返回
`True`；核心 pipeline 不再依赖它。

当前真实业务不存在 `CREATE TEMP TABLE` / `CREATE TEMPORARY TABLE` 场景，因此本轮
不为“真实 temporary table detection”重新设计规则。显式 DDL fact 仍然可以把
`PhysicalNodeKind.TEMPORARY_ASSET` 标出来，collapse evidence 也继续区分
`collapsed_tmp_nodes` 与 `collapsed_technical_nodes`。

## 3. DLO / DWO Boundary

```text
PRE_BUSINESS_ASSET_SCHEMAS = {"DLO", "DWO"}
```

DLO / DWO 靠近源系统，属于 pre-business 技术层：

- 可以保留在 raw SQL / Physical DAG / parser observation；
- **不得**出现在正式 `LineageEdge`、Business Lineage、DWS formal projection 或
  最终可视化血缘中；
- 不得为了补偿而人工合成 bypass edge。

例：

```text
DLO.A -> DWF.B
DWO.X -> DWF.B
DWF.B -> DWM.C
```

正式 lineage 最终只保留：

```text
DWF.B -> DWM.C
```

即 `DLO.A -> DWF.B`、`DWO.X -> DWF.B` 被排除，也不会生成 `DLO.A -> DWM.C`
之类的 bypass edge。

## 4. Multi-result / program-SQL mismatch

```text
program_name authority wins;
SQL differences are governance/audit evidence.
```

- Program Result 只认 active `005` program_name 第二段；
- SQL 额外 sink、缺失 sink 都不是 parser 修正 authority 的理由；
- 差异进入 audit / diagnostic（`TARGET_MISMATCH`、`TARGET_NOT_FOUND` 等）供治理使用。

## 5. 数据重算影响

本次修复改变了已持久化 fact 的语义，因此 `LINEAGE_PIPELINE_VERSION` 从
`lineage-pipeline-v10-sql-relation-context` bump 到
`lineage-pipeline-v11-asset-naming-semantics`。所有 v10 active `ProgramState` 都会被
incremental planner 视为 stale，partial replay 会被 `PipelineVersionMigrationRequired`
preflight 阻止。

| DWS 对象 | 是否受影响 | 原因 |
| --- | --- | --- |
| `dwp.lineage_program_state` | 不变 | program identity 与 source hash 未变；只 bump `pipeline_version` |
| `dwp.lineage_edge`（physical） | **受影响** | `TMP` 命名节点由 `temporary_asset` 变为 `formal_asset`，且 `source_dataset_key` / `target_dataset_key` 从 NULL 变为 dataset key；`physical_edge_key` 包含 node kind，因此 stable identity 改变 → 旧 row retire、新 row active |
| `dwp.lineage_business_edge` | **受影响** | `TMP` 命名中间节点不再被 collapse：`A -> DWM.TMP_X -> DWM.RESULT_A` 现在产生两条正式 edge，而不是折叠后的 `A -> DWM.RESULT_A` |
| `dwp.lineage_issue` | 受影响（间接） | 输入 Physical DAG / formal sink 集合变化，issue 集合随之重算；stable key 语义不变 |
| `dwp.lineage_schedule_edge` | 不受影响 | schedule ingestion 不调用 `is_temporary_asset()` / `is_business_asset()` |
| `dwp.lineage_reconciliation_suppression` | **必须重跑** | Program Inventory 与 business comparison boundary 同时改变 |

结论：

```text
必须重新跑 SQL lineage materialization（完整快照，不能带 --limit）
不需要重新跑 Schedule lineage materialization
必须重新跑 suppression materialization
历史 active snapshot 由同一个完整 batch 原子替换；不要手工修改 DWS 数据
```

内网验收命令：

```bat
REM Phase 1：inventory / suppression dry-run
C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe -B -m jobs.crontab.imp_lineage_suppression --environment DEV214 --dry-run

REM Phase 2a：只重算 SQL lineage（从 configs/lineage_providers.local.yaml 的 scope 取 DWS profile 与 SQL source profile）
C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe -m jobs.crontab.imp_lineage_edge --store dws --dws-profile <DATABASE_PROFILE> --profile <SQL_SOURCE_PROFILE>

REM Phase 2b：或直接跑统一日批（SQL + Schedule + suppression；Schedule 不受本 Issue 影响，会原样重写）
C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe -m jobs.crontab.imp_lineage_daily --environment DEV214

REM Phase 3：suppression 正式 materialization
C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe -B -m jobs.crontab.imp_lineage_suppression --environment DEV214
```

注意：pipeline version migration 要求完整快照，`imp_lineage_edge` 带 `--limit` 会被
`PipelineVersionMigrationRequired` preflight 直接拒绝（不进入 build/publish），这是预期
保护，不要用 `--limit` 绕过。

### v12：Authoritative unqualified write-target binding（Issue #133）

`LINEAGE_PIPELINE_VERSION` 从 `lineage-pipeline-v11-asset-naming-semantics` 升级到
`lineage-pipeline-v12-authoritative-target-binding`。qualified authoritative Program Result
与同 basename 的 unqualified SQL write target 绑定后，Physical / Business target identity
会变化；例如同一 `source_hash` 在 v11 是 `M_YQDKX`，v12 是 `DWM.M_YQDKX`。因此 v11 active
ProgramState 必须判 stale 并完整 rebuild，不能复用旧 cache。只处理 exact canonical basename
match 的 write target，不推断默认 schema；qualified target、basename 不同的 target、source
relations 均保持原样。

```text
必须完整重算 SQL lineage，不得使用 --limit；migration preflight 继续阻止 partial replay。
是否刷新 Schedule / suppression / reconciliation downstream，按日批 orchestration 与验收范围决定；
不得为本次 target identity 变化修改 Schedule lineage 本身。
```

黄金样本：

```text
DWM.M_JJQD_LIST
  DWF.F_NCMS_ALS_CODE_LIBRARY => 有 005 program 支撑 => SQL_ONLY 保持显示
  DWF.PARA_CODE_MAP          => 无 005 program 支撑 => 可 suppression
  DWM.M_PUB_CODE_INFO_NEW    => 无 005 program 支撑 => 可 suppression

DWP.TMP_P_REPORT_KYW_LIST
  存在 005:DWS_DWP.TMP_P_REPORT_KYW_LIST:...
  => 必须识别为 Program Result，不得因 TMP 名称判 invalid
```

### v13：Static empty Query Block（Issue #143）

`LINEAGE_PIPELINE_VERSION` bump 为 `lineage-pipeline-v13-static-empty-query`。Parser 为 SELECT
Query Block 生成静态恒假事实，恒假 block 与其子查询不再向 Physical DAG / `lineage_edge` /
`lineage_business_edge` 贡献 source edge；其它独立 block 保留。Audit 新增 `STATIC_EMPTY_QUERY`
并复用既有 `dwp.lineage_issue` lifecycle，证据不含 predicate literal 或完整 SQL。

| DWS 对象 | 是否受影响 | 原因 |
| --- | --- | --- |
| `dwp.lineage_program_state` | 更新 version | 同 source hash 的 v12 state 会被判定为 CHANGED 并重建 |
| `dwp.lineage_edge` / `dwp.lineage_business_edge` | **受影响** | 恒假 source edge 不再 active，真实同级/中间 lineage 仍保留 |
| `dwp.lineage_issue` | **受影响** | 新的 STATIC_EMPTY_QUERY facts 随完整 batch materialize；旧 issue lifecycle 由现有机制处理 |
| `dwp.lineage_schedule_edge` | 不受影响 | Schedule parser/materialization 与 SQL Query Block evaluator 独立 |
| `dwp.lineage_reconciliation_suppression` | **必须重跑** | suppression 读取 active SQL 与 Schedule snapshots，新 SQL edge set 可能改变差异分类 |

```text
必须完整重跑 SQL lineage；不要使用 --limit
Schedule lineage 不必重跑
SQL active batch 发布后必须重跑 suppression
之后 Web/CLI reconciliation 直接读取当前 active snapshots，无独立 persisted daily aggregate 迁移
```

详细 v1 判定范围、Issue lifecycle 和 DEV214 顺序见
[`lineage_static_empty_query.md`](lineage_static_empty_query.md)。

### v14：UNCLASSIFIED_FORMAL program-local intermediate 与 boundary blocker（Issue #162）

`LINEAGE_PIPELINE_VERSION` bump 为 `lineage-pipeline-v14-unclassified-formal-boundary`。
Materialization 现在会把同一 Physical DAG 中 `in_degree > 0 and out_degree > 0` 的未分类
formal 节点作为 program-local intermediate，沿真实 Physical DAG 路径折叠并生成
Business → Business edge；Physical DAG 本身保留这些节点，evidence 新增
`collapsed_unclassified_formal_nodes`。`SOURCE_ONLY` / `SINK_ONLY` 的未分类 formal
boundary 不猜 schema、不生成伪 lineage，由 Audit 新增
`UNCLASSIFIED_FORMAL_SOURCE` / `UNCLASSIFIED_FORMAL_SINK` 记录 blocker。

| DWS 对象 | 是否受影响 | 原因 |
| --- | --- | --- |
| `dwp.lineage_program_state` | 更新 version | 同 source hash 的 v13 state 会被判定为 CHANGED 并重建 |
| `dwp.lineage_edge` / `dwp.lineage_business_edge` | **受影响** | 新增可折叠路径会新增 Business direct edge；Physical rows 不变 |
| `dwp.lineage_issue` | **受影响** | 新的 boundary blocker facts 随完整 batch materialize；旧 issue lifecycle 由现有机制处理 |
| `dwp.lineage_schedule_edge` | 不受影响 | Schedule parser/materialization 与 SQL Physical DAG 独立 |
| `dwp.lineage_reconciliation_suppression` | **必须重跑** | suppression 读取 active SQL snapshots，新 SQL edge set 可能改变差异分类 |

```text
必须完整重跑 SQL lineage；不要使用 --limit
Schedule lineage 不必重跑
SQL active batch 发布后必须重跑 suppression
```

详细 collapse 安全边界、evidence contract 与 blocker issue 见
[`lineage_materialization.md`](lineage_materialization.md)、
[`lineage_audit.md`](lineage_audit.md) 和
[`lineage_business_asset_boundary.md`](lineage_business_asset_boundary.md)。

### v15：静态 SQL 模板中的动态 literal（Issue #166）

`LINEAGE_PIPELINE_VERSION` bump 为 `lineage-pipeline-v15-dynamic-literal-template`。
Python SQL extraction 现在区分动态 literal 与动态 identifier：静态 triple-quoted SQL
上的 `.replace()` / `.format()` 链与 f-string 允许把动态值折叠为不透明 literal
placeholder 后继续进入现有 SQL parser；动态 schema/table identifier、无法静态证明的
concat 或函数调用继续 `SQL_ARGUMENT_DYNAMIC`。placeholder 只在完整位于单引号 string
literal 内时被接受，动态值本身不会写入 parser evidence 或 persisted facts。

| DWS 对象 | 是否受影响 | 原因 |
| --- | --- | --- |
| `dwp.lineage_program_state` | 更新 version | 同 source hash 的 v14 state 会被判定为 CHANGED 并重建 |
| `dwp.lineage_edge` / `dwp.lineage_business_edge` | **受影响** | 原先因 `SQL_ARGUMENT_DYNAMIC` 丢失的静态 source/target edge 会恢复 |
| `dwp.lineage_issue` | **受影响** | 随完整 batch materialize；旧 issue lifecycle 由现有机制处理 |
| `dwp.lineage_schedule_edge` | 不受影响 | Schedule parser/materialization 与 SQL Physical DAG 独立 |
| `dwp.lineage_reconciliation_suppression` | **必须重跑** | suppression 读取 active SQL snapshots，新 SQL edge set 可能改变差异分类 |

```text
必须完整重跑 SQL lineage；不要使用 --limit
Schedule lineage 不必重跑
SQL active batch 发布后必须重跑 suppression
```

### v16：动态 SQL 写入目标不可截断为 schema（Issue #168）

`LINEAGE_PIPELINE_VERSION` bump 为 `lineage-pipeline-v16-dynamic-write-target`，
legacy parser contract bump 为 `legacy-parser-v5-dynamic-write-target`。动态标识符只在
INSERT / CREATE / MERGE / UPDATE 等写入目标位置识别；目标不完整时仅隔离该 statement，
不把 `DLO` / `DWM` / `DWP` 前缀当成数据集，不产生 Physical 或 Business Edge。同一程序
内其它可信 SQLStep 保留；静态目标上的动态 SELECT expression 仍生成表级血缘。诊断复用
`SQL_ARGUMENT_DYNAMIC` reason，并通过 `DYNAMIC_WRITE_TARGET_UNRESOLVED` AuditFact 保存
program identity、statement index/type 和固定 reason，不保存 SQL、连接信息或动态值。
DROP / TRUNCATE 继续不生成写入边，DLO/DWO Business Asset Boundary 不变。

| DWS 对象 | 是否受影响 | 原因 |
| --- | --- | --- |
| `dwp.lineage_program_state` | 更新 version | 同 source hash 的 v15 state 会被判定为 CHANGED 并重建 |
| `dwp.lineage_edge` | **受影响** | 截断产生的伪 Physical Edge 退出新 active snapshot |
| `dwp.lineage_business_edge` | **可能受影响** | DWM 等伪目标过去可能产生 Business Edge，必须随全量 replay 重算 |
| `dwp.lineage_issue` | **受影响** | 增加动态目标未解析的安全 AuditFact；旧事实按 batch lifecycle 退出 active |
| `dwp.lineage_schedule_edge` | 不受影响 | Schedule parser/materialization 与 SQL Physical DAG 独立 |
| `dwp.lineage_reconciliation_suppression` | **必须重跑** | active Business edge set 可能改变 |

```text
所有受影响 SQL profile 必须执行完整 snapshot replay；不要使用 --limit / partial replay
不要手工 DELETE 历史行；成功发布的新完整 batch 会原子替换 active snapshot
Schedule lineage 无需重跑
SQL snapshot 发布后重跑 suppression
```

## 6. 回归覆盖

| 场景 | 测试 |
| --- | --- |
| `DWP.TMP_X` 可成为 DatasetIdentity | `tests/shared/test_lineage_domain.py::test_dataset_identity_accepts_tmp_named_qualified_table` |
| `005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00` 进入 inventory | `tests/shared/test_lineage_domain.py::test_program_inventory_accepts_tmp_named_program_result` |
| Issue #44 grammar 支持 TMP target | `tests/shared/test_lineage_domain.py::test_issue_44_grammar_accepts_tmp_named_program_result` |
| `001` / unknown prefix 被忽略 | `test_program_inventory_ignores_non_005_prefix_without_failing`、`test_non_005_inventory_prefix_does_not_block_suppression` |
| malformed `005` fail-open | `test_program_inventory_malformed_005_target_fails_open`、`test_malformed_005_inventory_target_fails_open_with_cause` |
| 005 + 非 005 混合 | `test_mixed_005_and_non_005_inventory_keeps_only_005_targets` |
| UNCLASSIFIED_FORMAL intermediate 折叠 | `tests/shared/test_lineage_materialization.py::UnclassifiedFormalMaterializationTests` |
| UNCLASSIFIED_FORMAL boundary blocker | `tests/shared/test_lineage_audit.py::UnclassifiedFormalAuditTests` |
| NO_LINEAGE_EDGE coverage 分类 | `tests/shared/test_lineage_coverage.py::test_unclassified_intermediate_program_recovers_lineage_edge` |
| 有/无 005 支撑的 TMP source suppression | `test_tmp_named_source_with_active_005_program_is_not_suppressed`、`test_tmp_named_source_without_005_program_follows_normal_rule` |
| 多结果表 / program-SQL mismatch | `test_multi_result_sql_keeps_only_005_declared_program_result`、`test_program_sql_mismatch_keeps_005_authority` |
| DLO / DWO 边界与 bypass edge | `tests/shared/test_lineage_materialization.py::test_dlo_dwo_edges_are_excluded_without_bypass_edge` |
| 命名变体不改变语义 | `test_temporary_naming_is_not_evidence`、`test_tmp_naming_variants_do_not_change_physical_or_business_lineage` |
