# SQL parser backend 最小边界

Issue #41 只建立 parser experiment 的隔离边界，不重新设计 SQL compiler，也不替换已经在真实 corpus 上验证过的 legacy parser。

```text
ProgramSource.script_code
        ↓
   SQL extraction
        ↓
   ParserBackend / SqlAnalyzer
        ↓
   LegacyParserBackend（production default）
        ↓
   现有 SQLStep
        ↓
   Physical DAG → Audit → LineageEdge → Materialization
```

## Contract

实现位于 `shared/lineage/parser_backend.py`：

```python
from shared.lineage import analyze_sql

analysis = analyze_sql(script_code)  # 默认 legacy
```

`ParserBackend` 是唯一的 structural `Protocol`；`SqlAnalyzer` 是同一个
Protocol 的语义化别名，不维护第二套接口。backend 必须提供：

- `backend`：稳定的 backend 名称，例如 `legacy`；
- `backend_version`：backend 自己的 parser contract 版本；
- `analyze(script_code) -> SqlAnalysis`。

`SqlAnalysis` 的最小字段为：

- `steps`：现有 `SQLStep` tuple；因此 `statement_type`、`sources`、`target`、
  raw token、位置和 edge evidence 继续沿用当前 parser 输出；
- `ctes`：与 `steps` 一一对应的 CTE 名称 tuple。CTE 仅是 parser evidence，
  不创建 Physical 节点或边；
- `candidate_count`：保持现有 coverage 语义，不等同于 step 数量；
- `parse_status`：`success`、`unresolved` 或 `failed`；
- `extraction_reason`：保留现有 `SQLExtractionReason` value；
- `evidence`：不包含完整源码或 raw SQL 的轻量 backend evidence；
- `confidence`：`high`、`conservative` 或 `none`；恢复路径不会伪装成普通静态成功；
- `backend` / `backend_version`：用于 future shadow compare 的 provenance。

`SqlAnalysis.to_compare_dict()` 只提供 deterministic 的最小快照：

```json
{
  "backend": "legacy",
  "backend_version": "legacy-parser-v2-relation-context",
  "parse_status": "success",
  "confidence": "high",
  "extraction_reason": "CANDIDATE_FOUND",
  "candidate_count": 1,
  "steps": [
    {
      "statement_index": 0,
      "statement_type": "insert",
      "sources": ["ODS.DEMO_A"],
      "target": "DWA.DEMO_RESULT",
      "ctes": ["BASE"]
    }
  ]
}
```

这里不实现 compare runner。future runner 可在外层补充 `elapsed_ms`、Physical
edge count、issue count 和差异分类。

## 生命周期与失败语义

1. `ProgramSource.script_code` 进入 `ParserBackend.analyze`；
2. backend 返回不可变的 `SqlAnalysis`；
3. Physical DAG 只消费 `analysis.steps`、`candidate_count` 和
   `extraction_reason`，继续使用原有 `SQLStep` / graph logic；
4. Audit、Materialization 和持久化层不读取 backend metadata。

Legacy parser 的状态映射：

| 现有 extraction reason | status | confidence |
| --- | --- | --- |
| `CANDIDATE_FOUND`、`RAW_SQL` | `success` | `high` |
| `PYTHON_PARSE_RECOVERED` | `success` | `conservative` |
| `PYTHON_PARSE_FAILED` | `failed` | `none` |
| empty、dynamic、unknown wrapper、non-SQL 等无安全 candidate 情况 | `unresolved` | `none` |

`unresolved` 和 `failed` 都返回空 `steps`，不会用 `expected_target`、程序名或 sink
反推关系。backend 预期的解析失败必须显式返回 `SqlAnalysis`；unexpected
exception 不做静默 fallback，避免把错误误报成“无血缘”。

## Legacy adapter 与 production default

`LegacyParserBackend` 延迟调用 `shared.lineage.physical_dag` 中已有的：

- `_extract_python_candidates_with_reason`；
- `_parse_sql_candidates_with_ctes`，内部复用原有 SQL 分割、comment/literal 处理、
  statement 分类和 asset normalization。

没有复制或重写 production parser。`DEFAULT_PARSER_BACKEND` 始终是
`LegacyParserBackend()`；production job `jobs/crontab/imp_lineage_edge.py` 不需要
配置变更。`extract_sql_steps`、`build_program_physical_dag` 和兼容 facade 只新增
可选 keyword `backend=`，省略时继续走 legacy。

## 行为与 cache 兼容性

本 PR 不修改：

- `ProgramSource`、`SQLStep`、Physical DAG、Audit、`LineageEdge`、Materialization；
- target authority、TMP collapse、DWS、DatasetIdentity、column/runtime lineage；
- legacy parser 的 candidate、statement、source/target、failure reason 和 edge evidence。

Fake backend contract test 对同一个 `SqlAnalysis` 走完整的 SQLStep → Physical DAG →
Audit → Materialization 链路，比较 sources、target、statement type、parse reason、
edge 和 issue 结果。

`LINEAGE_PIPELINE_VERSION` 当前为
`lineage-pipeline-v11-asset-naming-semantics`。本次（Issue #121）修正的是 core lineage
的资产语义：`TMP` / `TEMP` / `STG` / `TEST` 命名不再产生 temporary classification，非
`005` program_name 不再进入 Program Inventory，`DLO`/`DWO` 仍不进入正式 lineage。
因此同一 source hash 的 v10 facts 必须 coherent rebuild，不能复用旧的 Physical/Business
projection。`ParserBackend` abstraction、production default 和 downstream adapter 边界
保持不变；`legacy-parser-v2-relation-context` 只标识该 backend 的修正后 parser
contract；真正控制持久化事实 cache invalidation 的仍是 pipeline version。未知但语法
有效的 relation 不因 schema 未登记而删除。shadow backend 的结果不应写入 production
facts 或 cache。

adapter 只增加一次轻量 `SqlAnalysis` 对象和 metadata，不进行第二次 SQL parse，也
不保存源码。默认 backend 不引入 SQLGlot/SQLLineage；因此预期性能影响为微小的
Python object/metadata allocation，未改变 parser 算法或下游复杂度。

## Future #42 接入边界

#42 可以实现 `SqlGlotShadowBackend`，在边界外调用 `analyze_sql(..., backend=...)`
并把 `SqlAnalysis.to_compare_dict()` 与 legacy 快照比较。它不应直接构造或修改
`SQLStep`、Physical DAG、Audit 或 Materialization，也不进入 production default。
本 PR 不依赖、不修改 #42 的 research artifact，也不等待 #42。
