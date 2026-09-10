# SQL Lineage Business Asset Boundary

## Contract

Issue #95 冻结 SQL Lineage 的双轨语义：

- **Physical Lineage** 保留 SQL/程序真实产生的全部节点和边，包括 TMP、DLO、DWO、cycle
  和 orphan branch。
- **Business Lineage** 只投影 Business Asset 之间的 direct edge。
- DLO、DWO 是 technical / pre-business ingestion、staging 层；它们可以作为
  collapse traversal 的中间节点，但不能成为 Business Lineage 的 `source_table` 或
  `target_table`。
- DWF 是最低内部业务数仓层；DWF 及以上正式资产仍可作为 Business Lineage endpoint。
- 本版本复用 `DatasetIdentity` 的 qualified `schema.table` 校验和现有 explicit
  legacy/DWS namespace registry。`DWS_DLO`、`DWS_DWO`、`DLK_DLO` 在 boundary
  classification 中分别按 DLO/DWO 处理；Physical DatasetIdentity 仍保留 SQL 中观察到的
  原始 schema，不做静默改写。

Boundary policy 位于 `shared/lineage/domain.py` 的
`is_business_asset()` / `is_technical_asset()`，不会改写 SQL extraction、parser 或
`ProgramPhysicalDAG`。`LineageEdge` 和 DWS business projection 都会再次拒绝 DLO/DWO
endpoint，避免绕过 materialization policy 的调用方写入违规 business fact。

## Projection examples

### Technical-only path

```text
DLO.A -> DWO.B -> DWF.C
```

Physical projection 保留两条边；Business projection 不伪造 `DLO.A -> DWO.B`、
`DWO.B -> DWF.C`，因为该程序没有 Business Asset source。结果是零 Business edge，
而不是 materialization failure。

### Business assets through technical nodes

```text
DWF.A -> DLO.B -> DWO.C -> DWM.D
```

Business projection 只产生：

```text
DWF.A -> DWM.D
```

edge evidence 同时保留完整的 physical edge pair/path summary，并单独记录
`collapsed_technical_nodes`；DLO/DWO 的 Physical rows 不会被删除或改写。

### Formal boundary and ambiguity

已有 Business Asset 之间的 direct edge 继续保留；到达第一个 Business Asset 后停止
transitive collapse，不生成跨该 boundary 的 N-hop edge。两个正式 Business sink 仍生成
`MULTI_SINK_CANDIDATE` 并 fail closed。若所有候选 sink 都是注册的 DLO/DWO technical
sink，则不生成面向 Business Lineage 的 `MULTI_SINK_CANDIDATE`，但 audit 仍可读取同一
份 Physical DAG evidence。

`SELF_REFERENCE` 仍由 Physical DAG audit 产生，self edge 的 Business Asset 语义和
原有 evidence 不变。DLO/DWO self-reference 也不会被提升为 Business edge。

## Coverage interpretation

Coverage 同时报告 Physical 和 Business 两条 funnel：

- `programs_with_physical_edges` / `physical_edge_count` 继续反映 Physical coverage；
- `programs_with_lineage_edges` / `lineage_edge_count` 只反映 Business edge；
- `programs_with_business_boundary_only` 统计“有 Physical edge、没有 Business edge、且
  结果是干净 boundary-only projection”的程序；
- `lineage_failure_reasons.NO_LINEAGE_EDGE` 排除 boundary-only 程序，只保留真实
  materialization 缺口。

因此，DLO/DWO 技术链路规模变化不会被误报为 parser 或 Physical Lineage coverage
下降；原始 physical evidence 仍由 `lineage_edge` projection 保存。

## Persistence and scope

Business boundary 只改变 Phase 5 Business projection、Audit sink classification 和
coverage interpretation：

- SQLite/DWS 的 Physical `lineage_edge` schema 与 lifecycle 不变；
- DWS `lineage_business_edge` 继续只接收已 materialized 的 Business `LineageEdge`，不在
  writer 内重新执行 collapse；旧 snapshot 中若有 boundary 之前写入的 DLO/DWO business
  row，SQLite/DWS read projection 会将其隐藏，下一次 publish 负责切换并 retire 旧 active
  row；
- schedule lineage、column lineage、OpenLineage exporter、source providers 以及
  SQL parser 不属于本 policy 的修改范围。

本 contract 的 synthetic tests 覆盖 technical-only、technical-to-DWF、
business-through-technical、DWS/legacy normalization、formal ambiguity、self-reference、
DWS persistence 和 coverage regression。
