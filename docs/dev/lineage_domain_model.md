# Lineage Phase 1：领域模型与语义边界

本文件冻结后续 Provider、Parser、Physical DAG Builder、Audit 和 Materialization
之间的最小协作契约。模型位于 `shared/lineage/domain.py`，只依赖 Python 标准库，
不连接数据库、不读取 metadata、不访问文件系统，也不负责解析 SQL。

## 领域对象

| 对象 | 作用 | 关键 optional 语义 |
| --- | --- | --- |
| `ProgramSource` | Parser 的统一程序输入 | `expected_target=None` 表示 Provider 无法提供预期结果表；`source_hash=None` 表示尚未提供 hash。 |
| `PhysicalNode` | 程序内部 DAG 的节点 | `kind` 可显式指定；省略时按可替换 TMP 名称规则推导。 |
| `PhysicalEdge` | 程序内部有向边 | `source` 是上游，`target` 是下游；允许指向 TMP，也不在此阶段吞掉自引用。 |
| `LineageEdge` | 正式业务血缘的 direct fact | `program_name`/`job_key`、`source_hash`、`batch_id`、时间字段和不含源码的结构化 `evidence` 可由后续采集/发布阶段补齐。 |
| `LineageIssue` | Physical DAG 审计事实 | `node_key`、`branch_sink` 与 `stable_key` 可按 issue 类型选择；生命周期时间字段可在首次发现时补齐。 |

### `ProgramSource`

必填文本字段为：

```text
environment
source_profile
program_name
script_code
```

其中 `source_profile` 是数据源/profile 标识，不等价于 `environment`；Parser 不应
通过它推断连接方式。`script_code` 在领域层是 `str`；MySQL 大字段的 `bytes`
解码属于后续 Provider 边界。Phase 1 不计算 `source_hash`，只保证 provider 提供
的 hash 原样保留。

### Physical DAG

`PhysicalNodeKind` 只有两个 Phase 1 分类：

```text
FORMAL_ASSET
TEMPORARY_ASSET
```

Physical 层必须保留 TMP，例如：

```text
ODS.DEMO_A → TMP1 → DWM.DEMO_B
```

`PhysicalEdge` 不把 TMP 折叠，也不把 `A → B` 递归扩展成祖先关系。后续 Audit
需要 Physical 图中的可达性、孤儿分支、多 sink、cycle 和 self-reference 信息。

### 正式业务血缘

`LineageEdge` 的定义是：

> 一条 `LineageEdge` 表示某环境下，一个正式上游资产到一个正式下游资产的直接业务血缘事实。

因此：

```text
A → DWM.DEMO_B → TMP1 → DWA.DEMO_C
```

在正式 direct lineage 中最多表达为：

```text
A → DWM.DEMO_B
DWM.DEMO_B → DWA.DEMO_C
```

禁止在 Phase 1 以方便查询为由提前存成 `A → DWA.DEMO_C`。`LineageEdge` 默认
拒绝名称规则识别出的 TMP endpoint；可选 `evidence` 只保存轻量 provenance，完整
collapse 留给 Phase 5。

### `LineageIssue`

`LineageIssue.stable_key` 是可选的、跨进程稳定的 issue identity；Audit 阶段按
环境、来源 profile、程序、issue 类型和对应 node/branch/cycle 语义填充它，不依赖
Python `hash()`、时间或 message。`issue_key` 与 `fingerprint` 是兼容别名。

首批 `IssueType` 固定为：

```text
ORPHAN_BRANCH
MULTI_SINK_CANDIDATE
TARGET_NOT_FOUND
TARGET_MISMATCH
CYCLE_DETECTED
SELF_REFERENCE
```

一个程序通常只有一个正式结果表只是 default assumption，不是模型约束。后续
Audit 必须能够用 `MULTI_SINK_CANDIDATE`、`TARGET_NOT_FOUND` 和
`TARGET_MISMATCH` 表达偏离默认值的情况；孤儿分支应优先以 `branch_sink` 聚合，
而不是为每一个节点静默丢弃或重复报警。

## source / target 方向

全项目新增领域接口永久使用：

```text
source = upstream
target = downstream
```

例如 `DWF.DEMO_A → DWM.DEMO_B` 的 source 是 `DWF.DEMO_A`，target 是
`DWM.DEMO_B`。旧实现中如果使用相反方向的内部 adjacency，不得直接当作新领域
`PhysicalEdge` 或 `LineageEdge`；应在对应迁移 Phase 增加 adapter。

Phase 6 查询继续消费这一定义：`environment` 是 graph boundary，
`source_profile` 只是可选的 provenance/filter dimension；省略 profile 时，同一环境
内不同 profile 的正式 edge 可以连接同一张业务图。查询、depth、Viewer contract 和
Blast Radius 的完整语义见 [`lineage_query.md`](lineage_query.md)。

## TMP 判定边界

仓库已有两类 TMP 逻辑，本模型不覆盖它们：

- `apps/svn_check/core/lakehouse/_sql_parser.py::is_temp_table_statement` 识别
  `CREATE TEMP/TEMPORARY/GLOBAL TEMP/LOCAL TEMP TABLE` 语句；
- `apps/svn_check/core/lakehouse/ddl_rule.py` 使用 `DWS_TEMP_TABLE_PREFIXES`
  （当前为 `TMP_`）执行命名检查，并识别 `TMP` schema。

`domain.py::is_temporary_asset` 只负责 Physical 节点的资产名称分类。默认规则
识别 `TMP`、`TMP_1`、`TMP1`、`TMP_STAGE_X` 及其带 schema 的形式，但不会把
`TMPORARY_BUSINESS` 或 `DEMO_TMP_1` 猜成 TMP。调用方可以传入可测试的规则元组，
并用 `rules=()` 显式关闭默认规则。真实生产 schema、库名和业务表名不写入默认
规则。

名称分类不是 SQL Parser：后续 Parser 仍需结合 `CREATE TEMP TABLE` 语句、表名、
别名和上下文决定 Physical 节点；不能仅凭一个正则删除节点。

## Phase 1 兼容边界

本阶段只新增领域对象、测试和说明，不修改以下旧入口：

- `shared/lineage/lineage_builder.py` 的现有 pipeline 图查询；
- `shared/lineage/schedule_table_lineage.py` 的作业/表双向漫游；
- `shared/lineage/mapping_sqlite.py` 的 Excel 字段映射 SQLite 导入和查询；
- `tools/search`、`tools/integrations`、`jobs`、`apps` 的现有生产/演示入口。

这些入口的实际调用和方向兼容点见 [`lineage_call_graph.md`](lineage_call_graph.md)。
