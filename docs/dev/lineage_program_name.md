# Legacy `program_name` 语义契约

本文固化 lineage 使用的 legacy `ProgramSource.program_name` 解析边界。示例全部
使用公开 synthetic 名称；不能据此推断任何真实 program、SQL、table 或配置。

## Canonical grammar

第一阶段只承认一个固定 legacy marker：

```text
005:<logical_target>:<step_seq>:<opaque_suffix>
```

四段的语义不同：

| 段 | 语义 | 是否进入 Dataset Identity | 是否进入 expected order |
| --- | --- | --- | --- |
| `005` | 固定 legacy marker；没有其它合法 prefix 枚举 | 否 | 否 |
| `<logical_target>` | declared logical final target；四段时是 authoritative，三段时只作为 candidate | 四段时才作为 target authority 输入 | 否 |
| `<step_seq>` | positive integer 的 Program Step 顺序 | 否 | 是，只有 expected evidence |
| `<opaque_suffix>` | 不稳定的 custom metadata，规范值通常为 `00` | 否 | 否 |

不提供可配置的 multi-prefix abstraction 或 suffix whitelist；代码只维护已确认的显式
namespace mapping。`ABCD`、`XYZ` 以及其它非空 suffix 都是可保留的 opaque 值，
不会因为不是 `00` 而使 lineage 失败。

## Legacy program namespace normalization

canonical 四段的 raw target token 只在进入 program-name target authority 时做有限的
namespace normalization。当前已确认且显式支持的 mapping 只有：

```text
DWS_DM.<TABLE>     -> DM.<TABLE>
DWS_DWM.<TABLE>    -> DWM.<TABLE>
DWS_DWA.<TABLE>    -> DWA.<TABLE>
DWS_DWP.<TABLE>    -> DWP.<TABLE>
DWS_DWD.<TABLE>    -> DWD.<TABLE>
DWS_DWF.<TABLE>    -> DWF.<TABLE>
DWS_DWUPRR.<TABLE> -> DWUPRR.<TABLE>
DLK_DLO.<TABLE>    -> DLO.<TABLE>
```

因此 `logical_target` 表示下游使用的 canonical physical `schema.table`，而完整
raw `program_name` 仍保留原始 namespace provenance；模型不另增一个 identity 字段。
未知 namespace（例如 `DWS_ABC.<TABLE>`）保持原值，不根据 suffix、sink 或相似度猜测
physical schema。这个 helper 只服务于 program-name-derived target，DatasetIdentity
仍直接保留 SQL 中观察到的 physical schema。它只做 `schema.table` syntax validation
与显式 namespace mapping，**不做资产命名分类**：`DWS_DWP.TMP_X -> DWP.TMP_X` 是
合法结果，`TMP` / `TEMP` / `STG` / `TEST` 名称不会使 target 失效。完整契约见
[`lineage_asset_semantics.md`](lineage_asset_semantics.md)。

## Conservative recovery

解析入口为 `parse_program_name()`，结果通过 `ProgramSource` 的以下只读属性暴露：

```text
program_name_semantics
logical_target
target_hint
step_seq / program_step_seq
opaque_suffix
program_name_diagnostics
resolved_target
```

只有严格四段、固定 marker、合法 formal target 的 canonical 形态才授予
program-name target authority：

```text
005:DEMO_DWM.RESULT_A:1:00  -> target=DEMO_DWM.RESULT_A, step=1
005:DEMO_DWM.RESULT_A:2:00  -> target=DEMO_DWM.RESULT_A, step=2
005:DEMO_DWM.RESULT_A:1:ABCD -> target=DEMO_DWM.RESULT_A, step=1
005:DWS_DWP.TMP_X:1:00      -> target=DWP.TMP_X, step=1
005:DWS_DWP.TMP_P_REPORT_KYW_LIST:1:00 -> target=DWP.TMP_P_REPORT_KYW_LIST, step=1
```

三段及其它非 canonical 形态（例如 `005:DWS_DWM.RESULT_A:00`、
`005:DWM.RESULT_A:00`）即使第二段看起来像 formal `schema.table`，也不会授予
logical target authority 或恢复 step；三段只额外暴露复用现有 namespace
normalization 的 `target_hint`：

```text
005:DWS_DM.RESULT_A:00
  logical_target=None
  target_hint=DM.RESULT_A
  step_seq=None
```

三段仍记录 `PROGRAM_NAME_FORMAT_UNSUPPORTED` 等格式诊断。`target_hint` 是
candidate，不等同于 `expected_target`，也不等同于由于缺少 SQL graph evidence
而产生的 `TARGET_NOT_FOUND`。

第三段只接受正整数，不设固定最大值，也不要求从 `1` 开始或连续。`0`、负数、
小数和非数字值只产生 `PROGRAM_NAME_STEP_INVALID`；target 仍可保留。

解析诊断与 Audit issue 分离，当前包括：

```text
PROGRAM_NAME_TARGET_RESOLVED
PROGRAM_NAME_INCOMPLETE
PROGRAM_NAME_STEP_MISSING
PROGRAM_NAME_STEP_INVALID
PROGRAM_NAME_TARGET_INVALID
PROGRAM_NAME_SUFFIX_NONSTANDARD
PROGRAM_NAME_MARKER_INVALID
PROGRAM_NAME_FORMAT_INVALID
PROGRAM_NAME_FORMAT_UNSUPPORTED
```

这些诊断用于 data-quality evidence，不改变 `IssueType` severity/disposition。

## Target authority

当 `ProgramSource.expected_target` 有 explicit/provider 值时，它优先；否则仅使用
canonical 四段 `program_name` 的 `logical_target`；三段 `target_hint` 永远不会
写入 `expected_target`。没有 authoritative target 时继续使用已有的 Physical DAG
事实，不把 hint 提升为业务声明。

因此：

```text
explicit/provider target
    -> canonical four-part program-name logical_target
        -> existing Physical DAG evidence

three-part target_hint
    -> only unique exact multi-sink materialization selection
```

这条顺序不会因为 `step_seq` 或 suffix 改变。`005`、step、hint 和 suffix 均不进入
`DatasetIdentity`；`ProgramIdentity` 仍保留完整 raw `program_name`。非 canonical
program name 不会因为存在可解析的第二段而获得 target authority。

## Target hint 与 multi-sink selection

`target_hint` 只允许作为独立 selection fact，不能执行以下替换：

```python
if expected_target is None:
    expected_target = target_hint
```

Audit 保留 `expected_target=None`，并在结果中分开暴露：

```text
TargetSelectionResult(
    authoritative_target=None,
    target_hint="DM.RESULT_A",
    selected_target="DM.RESULT_A",
    selection_mode="UNIQUE_HINT",
)
```

只有以下条件全部满足时，才选择 formal sink 作为
`selected_materialization_target`：

```text
expected_target is None
AND formal_sinks count > 1
AND normalized target_hint exact match formal_sinks
AND exact match count == 1
```

无匹配或多个 exact candidate 时 `selection_mode=NONE` 且不猜。无
authoritative target 的单 formal sink 保持原有 materialization 行为，不因 hint
增加 gating。`MULTI_SINK_CANDIDATE` 是 graph fact，仍然保留；hint selection
不会生成 `TARGET_MISMATCH`、`TARGET_NOT_FOUND` 或新的 authoritative
`ORPHAN_BRANCH` 语义。Materialization 只消费 Audit/Target Selection result，
不会重新解析 `program_name` 或复制 namespace registry。

## Logical processing unit 与 Program Step

以下两个 `ProgramSource`：

```text
005:DEMO_DWM.RESULT_A:3:00
005:DEMO_DWM.RESULT_A:4:XYZ
```

共享一个 `(environment, source_profile, logical_target)` logical processing unit，
但仍是两个不同的 raw ProgramSource / Program Step，不能物理合并或丢失 provenance。
`group_program_sources_by_logical_target()` 只提供不改变 raw source 的 grouping；
`expected_processing_order()` 返回 `(3, 4)` 这样的升序证据。

多 step 的正式 Dataset lineage 可以自然 union：

```text
A ─┐
   ├──> DEMO_DWM.RESULT_A
B ─┘
```

这不表示创建了两个 result Dataset，也不要求每个 step 都写入临时表。

`expected_processing_order` 不是 scheduler dependency。未来接入 scheduler configuration
后，实际配置仍是 scheduler fact source；两者只能做一致性比较。本契约不会创建
Job Run、runtime execution 或真实调度边。

## Sanitized validation

内网复测只允许输出按 `(environment, source_profile)` 聚合的计数：

```text
total_programs
target_resolved
target_unresolved
step_resolved
step_missing
step_invalid
custom_suffix
multi_step_target_count
max_steps_per_target
non_contiguous_step_groups
```

报告不得输出 raw `program_name`、logical target、SQL、源码、连接信息或真实路径。
公开 fixture 与本文示例只使用 `DEMO_*`、`demo_meta` 等 synthetic 占位值。
