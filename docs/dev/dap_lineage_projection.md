# DAP Lineage Adapter：Task Graph Projection

## 语义边界

`dwp.lineage_business_edge` 中每条 business edge 是 toolkit 的业务事实，身份由 source、program 和 target 决定。DAP Contract V1 的 edge 则是 task graph 中某个局部关系。两者不是一一对应：`toolkit business fact != DAP graph edge`。

本 adapter 只改变 DAP snapshot projection，不对 business facts 去重，不修改 materialization 或 `dwp.lineage_business_edge`。table/task node identity 与 normalization 保持不变。

## Semantic edge identity

每条 business fact 仍先按 `business_edge_key` 去重；同 key 的冲突 row 会失败。随后分别聚合两种有向语义边：

- `table_to_task`：`environment + source_profile + source_table + program_key`。target table 不参与 identity；一个 program 读取同一 source table 并写多个 target 时只产生一条 input edge。
- `task_to_table`：`environment + source_profile + program_key + target_table`。source table 不参与 identity；一个 program 从多个 source 读取并写同一 target 时只产生一条 output edge。

`program_key` 是现有 `ProgramIdentity(environment, source_profile, program_name)` 的稳定 identity。DAP edge `externalId` 从完整语义 tuple 确定性生成；超长 readable ID 使用该 tuple 的 SHA-256 fallback。`evidence.sourceRecordId` 使用同一 semantic edge ID。生成不依赖输入顺序、运行时 UUID 或 Python `hash()`。

例如 `A/B/C → JOB → T` 保留三条不同 `A/B/C → JOB` input edges，并将 `JOB → T` 聚合为一条 output edge。共享 source、多个 target 以及多个 task 同样按上述独立 identity 处理。

## Bounded provenance

每条聚合 edge 只输出固定大小的 provenance summary：`businessFactCount`、`collapseDepthMin` / `collapseDepthMax`、`pipelineVersion` 及其 distinct/present/missing count。单一 pipeline version 保留经过长度和内容检查的标签；多版本输出 `pipelineVersion: "multiple"`，不输出版本数组；缺失时输出 `unavailable`。描述为简短的 toolkit business fact count。

不导出 business-edge key 列表、SQL、raw evidence JSON、source hash、连接信息或 secret。所有 fact-level lineage 仍留在 toolkit business materialization 中。

## Dry-run 统计

- `business_edges`：去重后的 toolkit business fact 数。
- `raw_projected_edges`：修正前每条 business fact 两条 DAP edges 的计数（`business_edges × 2`）。
- `semantic_dap_edges` 与向后兼容的 `dap_edges`：最终 semantic edges 数。
- `deduplicated_edges` / `dedup_reduction_pct`：从 raw projection 移除的边数和比例。

DAP Contract V1 (`contractVersion=1.0`, `snapshot.mode=replace`)、single complete FULL snapshot 输入及当前 capacity preflight 保持不变。preflight 超限只报告容量状态；不会切 chunk，也不会 publish。DAP large-graph 容量扩展属于 DAP Phase 2，不由此 adapter 修正实现。
