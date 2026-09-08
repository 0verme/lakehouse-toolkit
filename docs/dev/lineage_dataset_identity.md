# Dataset Identity Contract V1

本文件冻结 Issue #37 的 Dataset Identity / Namespace 语义。它是 physical
Dataset 的 identity contract，不是 Dataset Registry、logical asset 或 DWS DDL
设计。

## Identity

一个正式 physical Dataset 的 identity 只有：

```text
(environment, canonical_schema, canonical_table)
```

代码中的 `shared.lineage.domain.DatasetIdentity` 是不可变 value object。它可以
脱离数据库 surrogate id 创建、比较和稳定序列化：

```python
DatasetIdentity.from_name("DEV200", "DWM.TABLE_A").key
# ("DEV200", "DWM", "TABLE_A")
```

`environment` 是 identity 的第一部分，也是 graph boundary。`DEV200:DWM.TABLE_A`
与 `DEV214:DWM.TABLE_A` 永远是两个不同的 physical Dataset。

## Canonicalization

只有 schema/table identity 值在 DatasetIdentity boundary 做 canonicalization：

1. 去除 surrounding whitespace；
2. `upper()`；
3. 必须是明确的两段 `schema.table`。

因此以下引用相同：

```text
DEV200 / DWM / TABLE_A
DEV200 / dwm / table_a
DEV200 / Dwm / Table_A
```

canonical value 为：

```text
environment = DEV200
schema      = DWM
table       = TABLE_A
```

物理 DAG 和 evidence 仍然保留已有的结构化 provenance 语义；canonicalization
不会把原始 SQL、raw token 或 evidence 文本改写成调试信息。

## Hard Boundary

`environment` 是硬 graph boundary：

- 不跨 environment 合并 Dataset；
- 不跨 environment 合并 lineage edge；
- 查询必须在指定 environment 内进行；
- V1 不自动建立 DEV/PROD 或 DEV200/DEV214 的 logical asset mapping。

同一个 `schema.table` 在不同 environment 中只是同名的两个 physical Dataset，
不是一个拥有多个 environment 的 `LogicalDataset`。

## Provenance

`source_profile` 是 collection source / provenance / filter 维度，不是
DatasetIdentity 的组成部分。相同 environment/schema/table 从不同 profile 采集时，
它们的 `DatasetIdentity` 相同；但现有 `LineageEdge` fact identity 仍保留
`source_profile`、`program_name` 和 `job_key`，因此不会错误合并不同 provenance 的
lineage fact。

`evidence`、`source_hash`、batch 和 observed/updated 时间同样属于 lineage fact
或 provenance，不属于 DatasetIdentity。

## Namespace

V1 不引入下列字段：

- `platform`
- `engine`
- `database_type`
- `catalog`
- `database`
- `cluster`
- `instance`

当前物理 namespace 就是 `environment + schema + table`。没有为了未来扩展而增加
新的 namespace 层级。

## Missing Namespace

只有 `TABLE_A` 而没有 schema 时，`DatasetIdentity.from_name()` 返回 unresolved
（`None`），不会猜测 `DWM`、`DWD`、`PUBLIC` 或其它 default schema。

正式 `LineageEdge` 只接受可解析的 `schema.table` endpoint；materialization 遇到
缺失 schema 的 Physical 引用时保留 Physical/Audit 事实，但不生成正式 Dataset
lineage edge。宁可少生成一条 edge，也不构造错误 identity。

包含额外 `catalog.schema.table` 等层级的引用同样不会被 V1 猜测或折叠到当前
namespace。

## Temporary Objects

TMP / temporary table 仍然属于 `ProgramPhysicalDAG`，用于：

- SQL step connection；
- internal path traversal；
- audit；
- orphan/cycle 诊断和 edge evidence。

TMP 不会因为 DatasetIdentity Contract 自动升级为正式 Dataset asset。最终正式
`LineageEdge` 仍然只表示 physical formal source dataset → physical formal target
dataset；TMP collapse 继续发生在现有 materialization boundary。

## Cross-environment

V1 不设计以下逻辑层：

```text
DWM.A
├── DEV200
└── DEV214
```

也不创建 `LogicalDataset`、`LogicalAsset`、`DatasetAlias` 或
`EnvironmentMapping`。跨 environment 对比只能由显式 diff/query contract 完成，
不能改变 Dataset identity。

## Existing edge identity boundary

Dataset identity 与 lineage fact identity 是两层概念：

```text
DatasetIdentity:
    environment + canonical_schema + canonical_table

Lineage fact identity（现有 contract）:
    environment + source_profile + source_table + target_table
    + program_name + job_key
```

因此同一 physical Dataset edge 可以由多个 program 产生；固化 DatasetIdentity
不会把不同 program 的 lineage facts 合并掉。SQLite reference adapter 继续保存
现有 `source_table` / `target_table` 字段，不创建 Dataset registry 表，也不改变
现有 batch、active/history 或 evidence schema。

## Non-goals

本 V1 不实现：

- logical dataset / logical asset；
- platform 或 catalog namespace；
- default schema inference；
- ProgramIdentity / Job semantics（Issue #38）；
- DWS Materialization DDL 或 writer（Issue #39）；
- `lineage_closure`（Issue #40）；
- `program_name` target contract（Issue #44）；
- Python AST fallback（Issue #50）；
- serialization 或 performance 优化。
