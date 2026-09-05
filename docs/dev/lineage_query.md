# Lineage Phase 6：Query、Viewer 合约与 Blast Radius

Phase 6 只消费 Phase 5 已发布的 active `lineage_edge`。查询层不调用
Provider、不读取 `script_code`、不重新解析 SQL，也不构建 Physical DAG：

```text
active lineage_edge
        ↓
LineageQueryService / BFS
        ↓
Viewer JSON 或 BlastRadiusResult
```

SQLite 只是当前 reference adapter；BFS 依赖的最小 reader 合约可以由未来的
PostgreSQL、Oracle、DWS 等 repository 实现。

## 方向与 scope

领域方向固定为：

```text
source = upstream
target = downstream
```

例如 `ODS.DEMO_A → DWM.DEMO_B → DWA.DEMO_C`：

- 查询 `DWM.DEMO_B` 的 upstream 返回 `ODS.DEMO_A`；
- 查询 `DWM.DEMO_B` 的 downstream 返回 `DWA.DEMO_C`。

查询必须显式提供 `environment`。它是 graph boundary，因此 `DEV` 查询不会
看到 `PROD` edge。`source_profile` 是 optional provenance/filter dimension：
省略时，environment 内不同 profile 的正式 edge 可以连接同一张业务图；提供时，
只投影该 profile 的 edge。它不是默认的业务图 boundary。

```python
from shared.lineage import LineageQueryService

service = LineageQueryService(store)
result = service.query_downstream(
    "DWM.DEMO_B",
    "DEV",
    source_profile=None,
    depth=7,
    max_nodes=300,
)
```

`SQLiteMaterializationStore.read_outgoing_edges()` 和
`read_incoming_edges()` 是窄读取 API，只查询 `is_active = 1`、environment、
当前邻接表和可选 profile，并使用 `lineage_edge` 的 source/target 索引范围；
不会先把完整 active 图加载到 Python。

## BFS 语义

`LineageQueryService` 和函数式 facade `query_upstream()`、`query_downstream()`、
`query_lineage()` 共用同一个 iterative BFS core：

- root `depth = 0`；
- direct neighbor `depth = 1`；
- `depth` 是 graph edge distance，不是节点数、递归调用数或 SQL 层数；
- 默认 `depth = 7`；
- 默认 `max_nodes = 300`，root 计入限制；
- `max_nodes` 必须大于 0，`depth` 可以为 0；
- 使用 queue 和 visited node，cycle/self-reference 不会无限入队；
- 已经返回的节点之间的 edge（包括 cycle edge）仍可展示；
- 每个 scope 内的 Viewer graph edge identity 是
  `source_table + target_table`，所以不同 program、job 或 profile 产生的相同
  business edge 只输出一条逻辑边；Phase 5 持久化事实不被修改。

输出只包含已返回节点两端都存在的 edge。若 depth 或 max_nodes 之外仍存在
可达的新节点，则 `truncated = true`；图自然结束，或限制边界外只有已访问的
cycle 节点时，`truncated = false`。因此 depth 边界恰好是自然终点时不会误报
truncation。

如果 root 在当前 active environment/profile projection 中完全不存在，返回空
contract，不凭空声明该资产已知。如果 root 存在但查询方向没有邻居，保留 root
节点，返回空 edges。这同样覆盖没有 active batch、active empty batch、方向叶子
节点和 unknown table。

## Viewer JSON contract

`LineageQueryResult.to_viewer_dict()` / `to_json()` 生成稳定、可序列化的最小
contract：

```json
{
  "nodes": [
    {
      "id": "DWM.DEMO_B",
      "table": "DWM.DEMO_B",
      "depth": 0
    }
  ],
  "edges": [
    {
      "source": "ODS.DEMO_A",
      "target": "DWM.DEMO_B"
    }
  ],
  "truncated": false
}
```

约定如下：

- node id 使用正式资产名，当前 projection 中 `id == table`；
- edge 永远保持原始 `source → target` 方向，即使查询是 upstream；
- nodes 按 `(depth, id, table)` 排序，edges 按 `(source, target)` 排序；
- 不暴露 Python dataclass repr、SQLite row、完整 SQL 或 `script_code`；
- 同一 active `lineage_edge` 的输入行顺序、program 顺序和 fixture 顺序变化时，
  JSON 内容不变。

## Blast Radius

`analyze_blast_radius()` 只消费同一个 downstream BFS result，绝不把 upstream
算入影响范围。返回字段：

```text
direct_impact   = depth 1 的唯一节点数
indirect_impact = depth >= 2 的唯一节点数
total_impact    = direct_impact + indirect_impact
max_depth       = 返回结果中最大的 downstream graph distance
```

root 本身不计入 impact。Diamond 图：

```text
A → B
A → C
B → D
C → D
```

结果为 `direct_impact=2`、`indirect_impact=1`、`total_impact=3`、
`max_depth=2`；`D` 只计算一次。Blast result 也携带 `truncated`，用于说明
impact 是否受安全限制裁剪。unknown root 返回零 impact，`root=None`。

## SQLite 与安全边界

查询 adapter 的 SQL 只使用固定的 source/target 分支，environment、table 和
source_profile 都是参数；不会把用户提供的表名拼接为 SQL 标识符。查询只读
active snapshot，不查询历史 candidate，也不创建 `lineage_closure`。

本阶段不新增 closure/transitive materialization，不实现历史 diff、incremental
rebuild、DEV/PROD diff 或 lineage-viewer 前端集成。
