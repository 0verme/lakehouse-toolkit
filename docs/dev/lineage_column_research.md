# Issue #45：Column Lineage 可行性与 MetadataProvider Contract 研究

> 研究结论：**`CORE_ASSET_ONLY`**
>
> 字段级血缘对核心资产有足够的查询和影响分析价值，但不值得在当前阶段对所有
> SQL、所有资产和所有表达式全量承诺。只有在 metadata 有版本、可回放，并且解析
> 结果通过高准确率门槛时，才进入另一个 implementation Issue。本 Issue 不接入
> production pipeline，不替换当前 parser，不创建 DWS column lineage 表。

## 1. 研究问题与边界

本研究只回答两个问题：

1. 当前 table lineage 稳定后，column lineage 是否值得做；
2. 如果做，最小的 `MetadataProvider` contract 是什么。

不在本 Issue 实现：

- 全量 column lineage 或完整 expression AST persistence；
- 修改 `DatasetIdentity`、`SQLStep`、Physical DAG 或当前 `LineageEdge` 的生产语义；
- 接入 SQLLineage / SQLGlot 作为生产 parser；
- 修改 DWS materialization、provider、SQLite reference store 或 parser full suite；
- 使用真实 SQL、真实表名、真实字段名、连接信息或生产 metadata。

验证材料只有公开 synthetic SQL、fake provider 和脱敏式 aggregate estimate。

## 2. 现有模型审计

审计基于最新 `origin/main` 的现有代码和 #37 已完成的 contract。

| 对象 | 当前语义 | Column Lineage 结论 |
| --- | --- | --- |
| `DatasetIdentity` (`shared/lineage/domain.py`) | `(environment, canonical_schema, canonical_table)` 的 immutable physical identity；禁止缺失 schema、额外 namespace 和 TMP 猜测。 | 可作为 provider lookup key；不应为字段级需求偷偷加入 catalog/platform。 |
| `SQLStep` (`shared/lineage/physical_dag.py`) | 保存 statement type、table-level `target`/`sources`、raw token、位置和轻量 evidence。 | 完全是 table-level；不能把 column list 塞入 `sources` 或 `evidence` 冒充字段事实。 |
| `ProgramPhysicalDAG` | `PhysicalNode`/`PhysicalEdge` 保存程序内部 table/asset 图，TMP、cycle、self-reference 仍保留。 | 是 column analysis 的输入边界，但不是 column fact store。 |
| `LineageEdge` (`shared/lineage/domain.py`) | formal asset → formal asset 的 direct table fact；endpoint 必须可解析为 `DatasetIdentity`。 | 保持 table-level，不添加 `source_column`/`target_column` 字段。 |
| materialization/query | 消费和查询正式 table edge；现有 edge identity 还包含 provenance。 | Column result 应是独立的 research/read model，不能混入当前 schema。 |

关键现状：

```text
ProgramSource
  -> SQLStep (table target/sources)
  -> ProgramPhysicalDAG (table/asset nodes and edges)
  -> audit/materialization
  -> LineageEdge (formal table -> formal table)
```

因此，#37 提供了正确的 dataset namespace，但没有提供 column schema，也不应被
解释为 column identity contract。最小新增边界应是：

```text
DatasetIdentity
  -> MetadataProvider.get_columns()
  -> isolated column resolution
  -> explicit confidence/status
```

### 2.1 为什么不修改 `LineageEdge`

当前 `LineageEdge` 的稳定性来自 table-level formal asset boundary、TMP collapse 和
现有 materialization identity。字段血缘还需要：

- output ordinal/target mapping；
- source snapshot/version；
- wildcard 展开证据；
- expression dependencies；
- ambiguity/staleness/error 状态。

把这些 optional 字段加到 `LineageEdge` 会让 table fact 的唯一性、历史和 query 语义
变得不清楚，也会诱导调用方把部分字段结果当成完整事实。未来实现应使用独立的
contract/read model；#45 不创建其正式 DWS 表。

## 3. 最小 MetadataProvider Contract

### 3.1 接口

概念接口如下，当前只在 research fixture 中验证，没有导出到
`shared.lineage`：

```python
class MetadataProvider(Protocol):
    def get_columns(
        self,
        dataset_identity: DatasetIdentity,
    ) -> MetadataSnapshot:
        ...
```

`DatasetIdentity` 是唯一必需输入。不能传入只有 `schema.table` 的裸字符串后再由
provider 猜 environment，也不能由 provider 通过 source profile 猜 catalog。

### 3.2 返回值

`MetadataSnapshot` 至少包含：

| 字段 | 约束 |
| --- | --- |
| `dataset` | 原样对应请求的 `DatasetIdentity`；不允许 provider 返回另一张表。 |
| `status` | 只能是 `RESOLVED`、`NOT_AVAILABLE`、`STALE`、`AMBIGUOUS`、`ERROR`。 |
| `columns` | `ColumnMetadata` tuple；按 `ordinal` 排序使用，不能依赖返回顺序。 |
| `snapshot_version` | catalog/snapshot 的稳定版本或 fingerprint；不能只用当前时间。 |
| `observed_at` | timezone-aware observation time；必须可解释 freshness。 |
| `availability` | 至少区分 available / not available；不能用空 tuple 同时表示空表、权限失败和未找到。 |
| `source` | 例如 `dws-catalog`、`offline-synthetic-snapshot`；用于 provenance 和回放。 |
| `error_code` | `ERROR` 时只返回稳定分类码，不把连接串或原始异常写入 lineage fact。 |

每条 `ColumnMetadata` 至少包含：

```text
name
ordinal
data_type             # 可为 unknown，但不能伪造
snapshot_version
observed_at
availability
source
```

Contract invariants：

1. `RESOLVED` 必须有完整可用 column records、version、`observed_at` 和 source；
2. `STALE` 可以携带旧 columns 供诊断，但默认不能产生 `RESOLVED` column fact；
3. `NOT_AVAILABLE` 不携带 columns，不根据目标表、SQL `*` 或历史猜字段；
4. `AMBIGUOUS` 表示 provider 自己无法确定唯一 schema，例如多 catalog 同名结果；
5. `ERROR` 必须有 stable `error_code`，调用方只能降级，不可 fallback 成 guessed columns；
6. column name 的 lookup normalization 可以 case-insensitive，但必须保留 ordinal、版本和
   source provenance；
7. 同一 evaluation 内用 `(DatasetIdentity, snapshot_version)` 做 cache/invalidation key，
   不能把不同 environment 的同名表合并。

### 3.3 状态语义

| Metadata status | 是否可直接解析 | 对外含义 |
| --- | --- | --- |
| `RESOLVED` | 是 | 该 snapshot 可用于产生字段依赖，但仍需 SQL scope 不歧义。 |
| `NOT_AVAILABLE` | 否 | 没有可用 schema；输出必须 `UNRESOLVED`，不得猜。 |
| `STALE` | 默认否 | 找到旧 schema，但 freshness/version 不满足当前策略。 |
| `AMBIGUOUS` | 否 | metadata 本身有多个候选或不能确定唯一列。 |
| `ERROR` | 否 | provider 失败；不能将异常当成空 schema 或 best-effort 事实。 |

`availability` 和 lineage result status 是两层状态：provider 可以返回
`status=STALE` 且每条旧 column record 的 `availability=AVAILABLE`；这不等于本次
SQL resolution 已经 `RESOLVED`。

### 3.4 没有 metadata 时的硬规则

```text
metadata missing != empty schema
metadata stale != current schema
metadata ambiguous != choose first candidate
best effort != resolved fact
```

所有拒绝都应保留一个可观察的 reason/metric，但不能把原 SQL 或敏感 metadata 写入
公开报告。

## 4. Metadata 来源比较

| 来源 | Freshness | Latency | Permissions | Availability | Maintenance | Offline replay |
| --- | --- | --- | --- | --- | --- | --- |
| DWS catalog | 如果 catalog 在写入/发布时带 version，通常最适合与业务资产对齐；可明确 snapshot boundary。 | 一次批量 snapshot 或缓存 lookup 可控；单表远程查询仍需 cache。 | 需要只读 catalog/service account；权限模型集中。 | 取决于 catalog 覆盖率；缺失必须返回 `NOT_AVAILABLE`。 | 由 catalog owner 维护 schema、版本和失效策略。 | **强**：versioned export 可重放并审计。 |
| `information_schema` | 反映当前数据库 schema，可能比异步 catalog 新；不自动代表历史执行时 schema。 | 依赖数据库连接和 system view；大量表逐表查询成本高。 | 需要访问 system view，跨引擎差异大。 | 受实例、网络、权限和 dialect 限制。 | 每个 engine/dialect 都要维护 adapter。 | **弱**：必须额外导出，不能依赖线上连接重放。 |
| existing asset platform | 可能有 ownership、logical asset 和业务标签；column freshness/完整性未必是其 contract。 | API latency 和 rate limit 不确定；适合按核心资产拉取。 | 需要平台 token/租户权限；字段可见性可能不同。 | 取决于平台是否同步 DWS catalog。 | 依赖外部平台 schema 和 API 版本。 | **中**：只有平台有 immutable snapshot 时才可回放。 |
| offline schema snapshot | 由采集任务生成；freshness 取决于导出周期，可直接暴露 version/observed_at。 | 本地读取快、可批量加载。 | 不需要生产连接；snapshot 本身需要访问控制。 | 只覆盖导出范围，缺失明确为 `NOT_AVAILABLE`。 | 需要维护格式、签名、过期和生成作业。 | **最强**：适合 fixture、controlled replay 和 regression。 |

### 推荐

- **Production path**：`DWS catalog` 的 versioned read adapter。provider 必须把
  catalog 缺失、权限不足、版本过期映射到明确 status；不能静默 fallback 到
  `information_schema` 或猜测。
- **Test/replay path**：offline schema snapshot + `FakeMetadataProvider`。fixture 只用
  `SYNTHETIC` environment、虚构 `ODS.A`/`DWM.TARGET` 等名称，并固定
  `snapshot_version`/`observed_at`。
- `information_schema` 可以作为另一个明确声明的 provider 实现，但不是本 Issue 的
  production fallback；existing asset platform 作为补充 provenance，不作为无条件的
  column truth source。

## 5. SQLLineage / SQLGlot 能力与边界

本节是能力比较，不是依赖引入或 production integration。资料访问日期为
`2026-09-08`。

| 维度 | SQLGlot | SQLLineage | 对本项目的结论 |
| --- | --- | --- | --- |
| column resolution | `sqlglot.lineage.lineage` 可建立单列/多列 lineage graph；optimizer scope 能处理 CTE、subquery、join、set operation 等形态。 | 提供 column-level lineage；有 `MetaDataProvider`、dictionary/SQLAlchemy 等 metadata 入口。 | 两者都证明 column lineage 可行，但 metadata 是准确率前置条件。 |
| `SELECT *` | 需要 schema mapping 才能可靠展开和 qualify；没有 schema 时不能把 wildcard 变成事实。 | metadata 可改善 wildcard；无 metadata 时保留 virtual/不完整结果。 | 采用“source columns 不可用则 `UNRESOLVED`”硬规则。 |
| ambiguity | qualified scope 和 schema 有助于发现不唯一列；调用方仍需定义拒绝策略。 | 可返回不完整/ambiguous 候选，metadata 不全时更容易 best-effort。 | `AMBIGUOUS` 永不落成 dependency。 |
| dialect support | dialect-aware parser/optimizer 较强，但 parser 和 optimizer 版本需要锁定。 | dialect coverage 依赖 SQLAlchemy/引擎和项目 adapter。 | 只做能力参考，不替换当前 parser。 |
| metadata dependency | schema/source query mapping 直接影响 qualification、wildcard 和 intermediate query。 | metadata optional，但准确率会随 metadata 缺失下降。 | 独立 `MetadataProvider` 是必须的治理边界。 |
| maintenance cost | 引入后要锁定版本、dialect、optimizer behavior 和回归 corpus。 | 需要维护 provider、dialect 行为和 best-effort 解释。 | #45 不增加依赖；若另开 Issue，先做 shadow comparison。 |

参考：

- [SQLGlot lineage API](https://sqlglot.com/sqlglot/lineage.html)
- [SQLGlot schema / qualification](https://sqlglot.com/sqlglot/schema.html)
- [SQLGlot lineage implementation](https://github.com/tobymao/sqlglot/blob/main/sqlglot/lineage.py)
- [SQLLineage metadata](https://sqllineage.readthedocs.io/en/latest/gear_up/metadata.html)
- [SQLLineage metadata provider concepts](https://sqllineage.readthedocs.io/en/latest/basic_concepts/metadata_provider.html)
- [SQLLineage design limitations](https://sqllineage.readthedocs.io/en/latest/behind_the_scene/dos_and_donts.html)

## 6. Synthetic SQL coverage matrix

下表中的 status 是 column lineage result status；metadata provider status 另按第 3 节
处理。`RESOLVED` 只表示当前声明范围内的依赖可解释，不表示 parser 对所有 SQL
方言都正确。

| Synthetic SQL shape | Metadata prerequisite | Expected status | 可保留的 dependency | 拒绝/降级条件 |
| --- | --- | --- | --- | --- |
| `SELECT * FROM ODS.A` | source columns `RESOLVED`、fresh | `RESOLVED` | 按 ordinal 展开每个 output → `ODS.A` 同名 source column。 | source `NOT_AVAILABLE`/`STALE`/`AMBIGUOUS` 时 `UNRESOLVED`，不把 `*` 展成“所有字段”。 |
| `SELECT * FROM ODS.UNKNOWN` | source columns unavailable | `UNRESOLVED` | 无 | 不根据 target、历史 schema 或命名规则猜。 |
| explicit columns | 引用 scope 可解析；unqualified column 必须唯一 | `RESOLVED` | 每个 output column 到明确 source column。 | 缺 metadata 为 `UNRESOLVED`；同名候选为 `AMBIGUOUS`。 |
| alias：`A.ID AS IDENTIFIER` | source columns and alias scope | `RESOLVED` | output `IDENTIFIER` → `A.ID`。 | alias 只改 output label，不改变 source identity。 |
| expression：`A + B AS C` | A/B metadata resolved | `RESOLVED` | `C` → `{A, B}`。 | 无 explicit output alias 或有未知引用为 `UNRESOLVED`。 |
| `CASE WHEN X THEN Y ELSE Z END AS RESULT` | X/Y/Z resolved | `RESOLVED` | `RESULT` → `{X, Y, Z}`；不持久化完整 AST。 | 任一引用不可解析则不伪造完整 expression fact。 |
| qualified JOIN | 两边 metadata resolved，引用带 qualifier | `RESOLVED` | 只连接明确的 `A.ID`/`B.ID` 等列。 | qualifier 不存在或 metadata 不可用则 unresolved。 |
| ambiguous JOIN：`SELECT id FROM A JOIN B ...` | A/B 都有 `ID` | `AMBIGUOUS` | 无 `ID` fact。 | 禁止选择第一张表或按 join condition 猜。 |
| aligned `UNION` | 各 branch arity、ordinal 和 refs 可解析 | `RESOLVED` | output ordinal 合并到各 branch 的 source columns。 | branch arity mismatch 为 `PARTIALLY_RESOLVED`；未知 branch 不补猜。 |
| CTE | CTE output 到 source origin 可追溯 | `RESOLVED` | CTE column 继续追溯到 physical source column。 | recursive/unsupported scope 超出研究范围则 unresolved/partial。 |
| subquery | derived alias 和 projection 可解析 | `RESOLVED` | outer reference 追溯到 inner source origin。 | derived output duplicate/unknown 为 ambiguous/unresolved。 |
| `INSERT SELECT` with target column list | source projection resolved；target list arity 相同 | `RESOLVED` | output target column → source dependencies。 | arity mismatch 为 `PARTIALLY_RESOLVED`，不按位置硬塞。 |
| `INSERT SELECT *` without target metadata | source 可展开，target positional mapping unknown | `PARTIALLY_RESOLVED` | prototype 不发 target fact；仅记录可观察的降级原因。 | 没有 target schema 时不得假定 source/output 顺序相同。 |
| `MERGE` explicit assignments | source refs 可解析，但 action/condition semantics 未建模 | `PARTIALLY_RESOLVED` | 仅用于 research observation 的显式 assignment dependency。 | 不把 `ON`、matched/not matched 分支和 target row semantics 当完整事实。 |

原型测试覆盖上述十类关键形态；它的 purpose 是验证 contract 和拒绝语义，不是新
建一个生产 SQL parser。

## 7. `SELECT *` 策略

`SELECT *` 是 pilot 的硬 gate：

1. 先解析 relation scope；
2. 对每个 source dataset 调用 `get_columns(dataset_identity)`；
3. 只有每个所需 source snapshot 为 `RESOLVED` 且 freshness policy 通过时，才按
   ordinal 展开；
4. 展开结果中的每一条 dependency 都带 source dataset、source column、snapshot
   version 和 observed/source provenance；
5. 任一 source metadata 不可用时，整个受影响 projection 不得降级成 guessed
   one-to-one mapping。

对 `INSERT INTO target SELECT * FROM source`，还需要 target schema 才能确认 positional
mapping。source columns 可展开不等于 target columns 已知；target metadata 缺失时结果
最多 `PARTIALLY_RESOLVED`，不发布 target column facts。

多源 `SELECT *` 如果展开后出现重复 output column name，也必须 `AMBIGUOUS`，除非
SQL 明确提供 target projection/alias 语义。不能使用 table join 顺序隐式解决。

## 8. Ambiguity 与 expression semantics

### 8.1 JOIN ambiguity

```sql
SELECT id
FROM ODS.A A
JOIN DWF.B B ON A.KEY = B.KEY
```

如果 A、B 都有 `ID`：

```text
result.status = AMBIGUOUS
dependencies = ()
```

只有 `A.ID` 或 `B.ID` 这样的 qualified reference 才能进入 `RESOLVED`。`ON` 条件
不能反向证明 SELECT 中未限定的 `id` 属于哪一边。

### 8.2 Expression lineage

本 Issue 只需要回答：

```text
output column -> depends_on source columns
```

例如：

```text
TOTAL  -> A, B
RESULT -> X, Y, Z
```

不设计完整 expression AST persistence、rewrite、constant folding、UDF semantics 或
row-level condition lineage。没有 source column 的 constant/unsupported function 不能
被标成已解析字段事实；最安全的结果是 `UNRESOLVED` 或 `PARTIALLY_RESOLVED`。

## 9. 分层路线与 go/no-go 门槛

| Tier | 范围 | 允许的事实 |
| --- | --- | --- |
| Tier 1 | table lineage，全量 | 继续使用当前稳定的 `ProgramSource → Physical DAG → LineageEdge`。 |
| Tier 2 | column lineage，核心资产 | 只处理 explicit/qualified/metadata-backed projection；只发布 `RESOLVED`。 |
| Tier 3 | expression lineage，按需 | 对用户请求或指定资产计算 dependency set；不默认全量 materialize AST。 |

### Tier 2 核心资产试点边界

试点资产必须同时满足：

- `DatasetIdentity` 稳定且属于显式 core-asset allowlist；
- DWS catalog snapshot 可取得并有 version/observed_at；
- SQL source 可静态定位到当前 table lineage step；
- SQL result status 为 `RESOLVED`，`AMBIGUOUS`/`UNRESOLVED` 只进入 metrics；
- 试点数据使用 synthetic 或脱敏 aggregate，不把源码和字段名带入公开报告。

### 建议门槛（未在本 Issue 声称已达成）

进入 implementation Issue 前，建议用人工标注的 synthetic + 脱敏 golden corpus 验证：

- `RESOLVED` precision **≥ 99.5%**；
- ambiguous column 的 false-resolve **= 0**；
- core asset SQL step 覆盖率 **≥ 85%**；
- 所需 metadata snapshot availability **≥ 99%**；
- cache warm 的单 statement p95 **≤ 50 ms**；cold metadata lookup 的单 statement p95
  **≤ 250 ms**（不含超时重试）；
- stale snapshot 不得静默进入 resolved result；
- 每个 status 都能在 replay 中重现，不能只留一个成功计数。

任何门槛未达成时，保留 table lineage，column lineage 回到 `DEFER`，而不是扩大
best-effort 范围。

## 10. 性能、metadata cache 与存储估算

以下是 synthetic planning estimate，不是真实环境 benchmark，也不包含真实字段名。
假设：平均每张表 80 columns、每个 SQL step 2.2 个 source dataset、每个 output 平均
1.3 个 source dependencies、cache hit ratio 90%。

| 场景 | Synthetic aggregate | Metadata lookup | Column dependency expansion | 估算存储 |
| --- | --- | ---: | ---: | ---: |
| Core pilot | 1,000 datasets、10,000 SQL steps、60 outputs/step | 22,000 logical lookups；约 2,200 次 cold provider fetch | 约 780,000 raw dependencies；去重后约 0.47–0.62M | 80,000 column records 约 13–26 MB；dependency facts 约 90–200 MB（不含 DB index/overhead） |
| Wider replay | 10,000 datasets、50,000 SQL steps、60 outputs/step | 110,000 logical lookups；约 11,000 次 cold provider fetch | 约 3.9M raw dependencies；去重后约 2.3–3.1M | 800,000 column records 约 130–260 MB；dependency facts 约 0.4–1.0 GB（不含 DB index/overhead） |

估算依据和边界：

- `ColumnMetadata` serialized size 按 160–320 bytes/record 估算；Python object overhead、
  compression、索引和重复 provenance 未计入；
- explicit pass-through 通常约 1 dependency/output；expression 约 1.5–3；join/union
  约 2–4；这些是 planning ranges，不是准确率承诺；
- wildcard 在 fresh source metadata 下接近 one-to-one expansion；metadata missing 时
  expansion 是 **0 个事实**，不是 source × target 的笛卡尔积；
- cache 以 DatasetIdentity + snapshot version 失效。snapshot version 改变时只重算受
  影响的 SQL steps；不能用固定 TTL 把 stale 当 current；
- 原型只在 memory 中保留 dependency，不增加正式 DWS table，不把估算文件提交到
  `runtime/` 或 `logs/`。

### Cache/latency budget 的实际含义

- cold path 只允许一次受控 provider lookup per distinct dataset per evaluation；
- warm path 必须命中 versioned snapshot cache；
- provider timeout、permission error 和 stale 都应迅速返回 status，不在 parser 线程中
  无限 retry；
- cache 命中率、lookup count、stale count、status distribution 应是 aggregate metric，
  不能只报告“成功条数”。

## 11. 结论：`CORE_ASSET_ONLY`

字段级血缘是有价值的，但价值主要集中在：

- 核心资产的 impact analysis；
- 受控的 downstream column search；
- 已有 table lineage path 上的可解释 column dependency；
- 能够拿到 versioned schema 的资产。

全量实现目前不值得承诺，因为 wildcard、动态 SQL、方言差异、MERGE action semantics、
metadata permissions 和 stale replay 会把 best-effort 结果混入事实层。结论不是
`NO_VALUE`，因为在 metadata 完整的核心资产范围内，synthetic prototype 已显示
explicit/alias/expression/CASE/qualified JOIN/UNION/CTE/subquery/INSERT mapping
可以在保守状态机下得到可解释结果；但也不是全量 `FEASIBLE_PILOT`。

## 12. Recommended follow-up

另开一个 implementation Issue，范围限定为 **Tier 2 core assets**，至少包含：

1. 独立的 production `MetadataProvider` adapter（首选 DWS catalog）；
2. offline snapshot replay 和 contract versioning；
3. synthetic + 脱敏 golden corpus，以及 SQLGlot/SQLLineage shadow comparison；
4. `RESOLVED` precision、coverage、latency、cache 和 stale metrics；
5. 独立 column dependency read model，明确不改写当前 table-level `LineageEdge`；
6. 失败/退出条件：metadata availability、accuracy、ambiguity 或 performance 任一
   低于第 9 节门槛即停止扩大范围。

本 Issue 的 prototype 和 tests 只验证研究结论。**Production pipeline changed: NO。**
