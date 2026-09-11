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

## 6. 回归覆盖

| 场景 | 测试 |
| --- | --- |
| `DWP.TMP_X` 可成为 DatasetIdentity | `tests/shared/test_lineage_domain.py::test_dataset_identity_accepts_tmp_named_qualified_table` |
| `005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00` 进入 inventory | `tests/shared/test_lineage_domain.py::test_program_inventory_accepts_tmp_named_program_result` |
| Issue #44 grammar 支持 TMP target | `tests/shared/test_lineage_domain.py::test_issue_44_grammar_accepts_tmp_named_program_result` |
| `001` / unknown prefix 被忽略 | `test_program_inventory_ignores_non_005_prefix_without_failing`、`test_non_005_inventory_prefix_does_not_block_suppression` |
| malformed `005` fail-open | `test_program_inventory_malformed_005_target_fails_open`、`test_malformed_005_inventory_target_fails_open_with_cause` |
| 005 + 非 005 混合 | `test_mixed_005_and_non_005_inventory_keeps_only_005_targets` |
| 有/无 005 支撑的 TMP source suppression | `test_tmp_named_source_with_active_005_program_is_not_suppressed`、`test_tmp_named_source_without_005_program_follows_normal_rule` |
| 多结果表 / program-SQL mismatch | `test_multi_result_sql_keeps_only_005_declared_program_result`、`test_program_sql_mismatch_keeps_005_authority` |
| DLO / DWO 边界与 bypass edge | `tests/shared/test_lineage_materialization.py::test_dlo_dwo_edges_are_excluded_without_bypass_edge` |
| 命名变体不改变语义 | `test_temporary_naming_is_not_evidence`、`test_tmp_naming_variants_do_not_change_physical_or_business_lineage` |
