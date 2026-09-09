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
| `<logical_target>` | declared logical final target，必须是无歧义的 formal `schema.table` | 只作为 target authority 输入 | 否 |
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
仍直接保留 SQL 中观察到的 physical schema。

## Conservative recovery

解析入口为 `parse_program_name()`，结果通过 `ProgramSource` 的以下只读属性暴露：

```text
program_name_semantics
logical_target
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
```

三段及其它非 canonical 形态（例如 `005:DWS_DWM.RESULT_A:00`、
`005:DWM.RESULT_A:00`）即使第二段看起来像 formal `schema.table`，也不会猜测
logical target 或 step，而是返回 `logical_target=None`、`step_seq=None` 并记录
`PROGRAM_NAME_FORMAT_UNSUPPORTED` 等格式诊断。它们是 program-name target
unresolved，不等同于由于缺少 SQL graph evidence 而产生的 `TARGET_NOT_FOUND`。

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
canonical 四段 `program_name` 的 `logical_target`；两者都没有时保持 unknown，并
继续使用已有的 Physical DAG evidence。程序名 target 只影响 expected target hint，
不替代 Physical DAG 中的其它 formal sinks。

因此：

```text
explicit/provider target
    -> program-name logical_target
        -> existing Physical DAG evidence
```

这条顺序不会因为 `step_seq` 或 suffix 改变。`005`、step 和 suffix 均不进入
`DatasetIdentity`；`ProgramIdentity` 仍保留完整 raw `program_name`。非 canonical
program name 不会因为存在可解析的第二段而获得 target authority。

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
