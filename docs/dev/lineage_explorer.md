# Lineage Explorer MVP

Lineage Explorer 是一个只消费正式 DWS lineage fact 的 PyWebIO 工具，不重新读取
`SCRIPT_CODE`，不调用 Provider，也不重新解析 SQL。

```text
configs/lineage_providers.local.yaml
        ↓
LineageEnvironmentScopeResolver
        ↓
DWSLineageEdgeReader（active batch）
        ↓
LineageQueryService（bounded traversal）
        ↓
LineageQueryResult / Viewer projection
        ↓
PyWebIO SVG renderer
```

## 查询 contract

`LineageQueryService` 复用既有 iterative BFS，并新增兼容的显式 view contract：

```python
service.query(
    root,
    environment,
    direction="upstream|downstream|both",
    view="business|physical",
    depth=1,
    max_nodes=100,
    source_profile=scope.sql_source_profile,
)
```

- root `depth=0`，root 计入 `max_nodes`；
- `both` 在同一个 request scope 内复用 upstream/downstream traversal，合并时 root
  只保留一次，edge 去重且始终保持 `source → target`；
- `truncated=true` 表示深度或节点上限阻止了仍可达的新节点；
- `to_viewer_dict()` 仍保持 #8 的最小 `{nodes, edges, truncated}` contract；
  `to_explorer_dict()` 只在 Explorer 需要时附加 environment、root、view、batch 和
  timing metadata；
- DWS reader 提供 `contains_node()`，因此 root 不存在和 root 在当前方向没有邻居
  可以分开处理。

## Business / Physical 语义

```text
business  → dwp.lineage_business_edge
physical  → dwp.lineage_edge
```

这是正式 DWS projection 的选择，不是 UI 或 Python 根据表名重新推断。尤其是：

- `TMP`、`TEMP`、`STG`、`TEST` 命名本身没有 lineage semantics；
- `DWP.TMP_FORMAL_RESULT` 如果在 active fact 中存在，必须正常出现在 graph；
- temporary 只能来自已有显式 asset fact，例如 `CREATE TEMP/TEMPORARY TABLE`；
- physical reader 保留 direct evidence endpoint，business reader 只消费正式 business
  projection；
- DLO/DWO 的 formal business boundary 继续由现有 domain/materialization contract
  决定。

## DWS active reader 与性能

`DWSLineageEdgeReader` 在一次 request scope 中：

1. 打开一个配置 scope 对应的 DWS connection；
2. 解析唯一的 `PUBLISHED + is_active` batch；
3. 将 batch、environment、source/target table、source profile 条件下推到 DWS；
4. BFS 扩展的所有邻居查询复用同一连接；
5. 退出 scope 时关闭本次新建的 connection。

active batch 不存在或不唯一时 fail closed。inactive/history edge 通过 active batch join
和 `e.is_active` 同时排除。查询 timing 至少记录 connection、active batch resolve、
edge query、traversal、projection、render preparation 和 total。

## PyWebIO 页面

正式 entrypoint：

```text
tools/lineage/lineage_explorer_web.py
```

WebAdmin registry：

```yaml
name: lineage_explorer
title: 血缘探索
port: 8615
```

页面只让用户选择 environment、root、方向、视图、depth 和 max_nodes；profiles 由
`LineageEnvironmentScopeResolver` 从 `configs/lineage_providers.local.yaml` 的
`scopes` 解析。每次提交都会清空上一张图的结果 scope，避免切换 environment 后旧图
继续显示。

渲染器是 PyWebIO 内嵌的轻量 SVG adapter，支持 root 高亮、完整表名 tooltip/detail、
点击节点、zoom、pan、fit 和截断提示；核心 query/domain model 不携带 `x`、`y`、
`symbolSize`、`itemStyle` 等 renderer 私有字段。

## 内网启动与验收

在仓库根目录执行：

```bat
"C:\path\to\pywebio\Scripts\python.exe" -m tools.lineage.lineage_explorer_web
```

验收时至少检查：

1. 选择 `DEV214`，确认加载其 provider config scope；
2. 用一个有明确上下游的 golden root 检查 upstream、downstream、both；
3. 分别检查 business / physical、depth 1 / 2，并记录 nodes、edges、truncated、
   DWS query ms、traversal ms、total ms；
4. 用已有正式 authority 的 `DWP.TMP_FORMAL_RESULT` 或内网中同类 TMP 命名资产，确认
   不会因名字被隐藏、过滤或标记 temporary；
5. 切换到另一个 enabled environment，确认旧图、详情和 highlight 清空，查询不会
   串环境；
6. 用不存在的 root、disabled environment、无 active snapshot 分别确认错误语义。

本工具不依赖 #40 的 `lineage_closure`。后续是否需要 closure 由真实 DEV214 benchmark
和 SLO evidence 决定。
