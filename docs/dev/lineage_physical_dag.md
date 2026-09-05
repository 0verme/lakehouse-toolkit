# Lineage Phase 3：Program Physical DAG Builder

Phase 3 将一个 `ProgramSource` 的 `script_code` 转换为程序内部的
Physical 图。这里记录的是程序可以静态确认的事实，不判断结果是否符合
`expected_target`，也不把 TMP 折叠成正式资产之间的直连关系。

## API

实现位于 `shared/lineage/physical_dag.py`：

```python
from shared.lineage import ProgramSource, build_program_physical_dag

physical_dag = build_program_physical_dag(program_source)
```

`ProgramPhysicalDAG` 包含：

- `program_source`：原始 `ProgramSource`；
- `steps`：按程序中提取顺序排列的 `SQLStep`；
- `nodes`：去重后的 `PhysicalNode` tuple；
- `edges`：去重后的 `PhysicalEdge` tuple；方向固定为
  `source=upstream`、`target=downstream`；
- `sinks`：实际写入 target 中出度为零的标准化资产名；
- `expected_target`：从 `ProgramSource.expected_target` 标准化得到的值，未知时
  为 `None`。

`node_map` 和 `edge_pairs` 是只读访问便利属性。图对象不要求严格无环，
因此可以表达自引用和 cycle。

## SQL step

`SQLStep` 是构图的最小单位，至少保存：

- `statement_index`（从零开始）；
- `statement_type`；
- 标准化后的 `target` 和 `sources`；
- `raw_target` 和 `raw_sources`；
- Python execution call 的行列位置；
- `is_temporary`、`insert_mode` 和轻量 `evidence`。

当前支持仓库常见的写入形式：

```sql
INSERT INTO target SELECT ... FROM source;
INSERT OVERWRITE TABLE target SELECT ... FROM source;
CREATE TABLE target AS SELECT ... FROM source;
CREATE TEMP TABLE target AS SELECT ... FROM source;
CREATE TEMPORARY TABLE target AS SELECT ... FROM source;
CREATE VIEW target AS SELECT ... FROM source;
MERGE INTO target USING source ON ...;
UPDATE target SET ... FROM source;
```

`CREATE TABLE target (...)` 这类没有来源表的 DDL 仍然会产生 target 节点，
但不会凭空产生 edge。普通 `SELECT` 可以保留 source step/node，但没有写入
target 时不会产生 `source → expected_target`。

同一个 step 的多个来源分别生成边；JOIN 顺序不是血缘层级：

```text
A + B → TMP1
```

只生成 `A → TMP1` 和 `B → TMP1`。

## 程序内 SQL 提取

`script_code` 有两条路径：

1. 如果是 raw SQL（不能被 Python AST 解析且以 SQL 语句关键字开头），使用
   共享 `split_sql_statements` 按顶层分号切分；
2. 如果是 Python 源码，只从已知 SQL execution context 提取 SQL：
   `execute`、`cursor.execute`、`execute_sql`、`run_sql`、`spark.sql`、
   `read_sql` 等调用。SQL 可以是直接字符串、常量拼接、已知变量，或所有
   插值变量都能静态解析的 f-string。

普通 `logger.info("FROM ...")`、普通 message string 和无法静态解析的动态
SQL 不会进入 step。动态 SQL 不会用 `expected_target`、程序名或其它上下文
补猜 target；如果 target/source 变量无法解析，整个动态语句不产生 edge。

原有 `apps/svn_check/core/lakehouse/_sql_parser.py` 仍通过兼容 wrapper 暴露
`split_sql_statements`，实际实现位于 `shared/lineage/sql_parser.py`。

## source / target 与 normalization

所有 edge 都是：

```text
source = upstream
 target = downstream
```

节点身份复用 `shared.lineage.lineage_builder.normalize_table_name`。因此仓库
既有规则会继续生效，例如 `DWM.DEMO_A` 的 canonical 名称是
`DWS_DWM.DEMO_A`，带反引号、双引号、大小写变化的同一名称会合并为一个节点。

`table_name_aliases` 继续服务于 registry/lookup 侧；Physical 节点不会用短名
alias 去合并节点，否则会错误合并 `ODS.A` 和 `DWM.A`。不同 schema 的资产始终
按不同 canonical 名称保留。

SQL comment 使用共享的 `strip_sql_comments`。该 helper 会忽略引号中的
`--`、`/*` 文本；SQL literal 还会在 `FROM/JOIN/USING` 扫描前被遮盖。CTE 名称
只在当前 statement 内作为 SQL relation alias 过滤，不会创建 Physical 节点：

```sql
WITH base AS (SELECT * FROM ODS.A), joined AS (
    SELECT * FROM base JOIN DWF.B ON ...
)
INSERT INTO DWM.C SELECT * FROM joined;
```

得到的来源事实至少为 `ODS.A → DWM.C` 和 `DWF.B → DWM.C`，不会出现 `BASE`
或 `JOINED` 节点。SQL table alias 也不会成为资产节点。

## TMP、sink 与 expected target

节点分类复用 Phase 1 的 `PhysicalNode`、`PhysicalNodeKind` 和
`is_temporary_asset()`。名称符合 TMP 规则的节点自动为
`TEMPORARY_ASSET`；`CREATE TEMP/TEMPORARY TABLE` 即使名称不是 TMP，也会将
该 target 标为临时资产。Phase 3 不删除 TMP、不折叠正式资产中间层，也不做
递归祖先展开。

例如：

```text
ODS.A + DWF.B → TMP1
TMP1 + DWM.C → TMP2
TMP2 + DWA.D → DWA.F
```

Physical 图完整保留：

```text
ODS.A → TMP1
DWF.B → TMP1
TMP1 → TMP2
DWM.C → TMP2
TMP2 → DWA.F
DWA.D → DWA.F
```

`sinks` 只按写入 target 的实际出度计算。孤立分支和多个 sink 会照实返回；
Phase 3 不产生 `LineageIssue`。`expected_target` 只从 Provider 给出的字段
读取并标准化，绝不把它接到没有明确写入关系的 source 上。

自引用和 cycle 同样保留：

```text
DWM.A → DWM.A
A → TMP1 → TMP2 → TMP1
```

它们留给 Phase 4 判断，不在 Builder 中删除或告警。

## Evidence

每条 `PhysicalEdge.evidence` 保存轻量 statement evidence：

- `statement_index` / `statement_indices`；
- `statement_type`；
- `raw_source`、`raw_target`；
- `normalized_source`、`normalized_target`；
- 可用的 Python 行列位置和 `insert_mode`；
- `occurrences` 列表，用于解释同一 edge 被多个 statement 观察到的情况。

不会把完整程序代码写入 edge evidence。

## 阶段边界与限制

本阶段明确不实现：

- `LineageIssue`、orphan/multi-sink/target mismatch/cycle/self-reference 检测；
- TMP collapse 或正式 `LineageEdge` materialization；
- recursive upstream/downstream、Blast Radius、closure；
- batch publish、incremental/history/diff 和 Viewer；
- 所有 SQL 方言和完整 Python 静态执行分析。

尚未可靠静态支持的复杂方言、变量来自运行时的动态 SQL、未列入 execution
context 的自定义封装调用会被跳过，而不是猜测资产关系。后续如果需要支持
新的项目调用模式，应先增加有 fixture 覆盖的 execution adapter。

因此 Phase 3 的输出可以直接交给 Phase 4 消费：Phase 4 再基于完整 Physical
事实判断 orphan-like branch、multiple sinks、expected target mismatch 候选、
self-reference 和 cycle。
