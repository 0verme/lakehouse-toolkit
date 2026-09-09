# Issue #42：SQLGlot 作为 lineage parser shadow / oracle 的收益评估

## Research Conclusion

```text
KEEP_AS_SHADOW
```

这个结论只表示：SQLGlot 在**不改变 production parser 结果**的前提下，作为
shadow comparison 有可观测性价值；不表示它是 ground truth，也不表示本 PR
接入 production、fallback 或 selected-dialect routing。

在本次固定 corpus 中，SQLGlot：

- 发现了 legacy parser 在 MySQL `INSERT IGNORE` 上丢失 target / statement type
  的 blind spot；
- 对非法 SQL 的显式 parse failure 比 legacy 静默产出 source/target 更安全；
- 在 24 个可比较的 SQL semantics sample 上，truth exact rate 为 `91.67%`，legacy
  为 `87.50%`，增量为 `+4.17 percentage points`；
- 没有证明可以安全接管 fallback：它对合法 MySQL `INSERT SET` parse failure，
  对合法 MySQL `REPLACE` 与 legacy 一样没有 bounded mapping；
- 需要额外依赖、per-dialect 配置、AST-to-lineage 映射和版本升级复核。

因此本 PR 交付的是可重复 research artifact，不改 production parser，也不把
SQLGlot 结果自动写入 Physical DAG、Audit、Materialization、DatasetIdentity、
DWS 或 pipeline version。

## Scope 与背景

研究基线来自：

- Epic `#33`：production 主路径继续由自研 parser 提供事实；SQLGlot 只作为
  future shadow/oracle/fallback candidate；
- `#26`：coverage funnel 将 candidate → SQL step → write target → Physical DAG
  → LineageEdge 分阶段观察，并要求动态 SQL / 未知 wrapper 保守 unresolved；
- `#34`：四 profile replay 已完成，但公开 evidence 只有聚合 replay 数字和
  parser robustness case，没有可提交的真实 SQL；
- `#41`：本研究不依赖尚未 merge 的 backend abstraction，只在 research module
  中使用最小隔离 adapter。

本轮没有读取或提交真实内网 SQL、Python source、program name、schema/table
identity、SVN path、host、credential 或 connection string。

## Reproducibility

Corpus 与 runner：

```text
tests/fixtures/research/sqlglot_corpus.jsonl
tools/research/sqlglot_compare.py
requirements-research.txt
```

安装 research-only dependency：

```bash
python -m pip install -r requirements-research.txt
```

只运行比较（不运行耗时 benchmark）：

```bash
python -B -m tools.research.sqlglot_compare \
  --corpus tests/fixtures/research/sqlglot_corpus.jsonl \
  --no-benchmark \
  --report artifacts/lineage_coverage/issue-42-compare.json
```

运行带 quantile 的 benchmark：

```bash
python -B -m tools.research.sqlglot_compare \
  --corpus tests/fixtures/research/sqlglot_corpus.jsonl \
  --benchmark-repeats 100 \
  --benchmark-warmup 5 \
  --report artifacts/lineage_coverage/issue-42-compare.json
```

本次 runner 使用的基线为：

| Item | Value |
| --- | --- |
| Git revision | `365287e71d4bcfe0f4a0646cfb8312c75e570437`（origin/main） |
| SQLGlot | `30.12.0` |
| Runtime | CPython `3.13.2` on Windows |
| SQLGlot configuration | `read=<sample dialect>`, `error_level=RAISE` |
| Unsupported AST root | `failed_not_guessed` |
| Relation mapping | AST `Table` nodes，排除 target 和 CTE；table-valued function 不提升为 asset |
| Corpus canonical SHA-256 | `beacd7dd20e45b059cc2268f1e05a59e127ffddaf55caed5da1b10fa3f409c4d` |
| Benchmark repeats / warmup | `100 / 5` |
| Wall-clock hard threshold | 无 |

`requirements-research.txt` 是 optional research dependency，SQLGlot 没有加入
production `requirements.txt`。

## Sanitized Corpus

### 数量与来源

- Sample count：`29`。
- `26` 个 synthetic reconstruction；`3` 个 sanitized shape。
- SQL semantics 可比较样本：`24`。
- `4` 个 extraction boundary sample 不用于 backend accuracy 结论。
- `2` 个 production scope 外的样本（`DELETE` 与 `REPLACE`）单独保留，用于
  防止把 parser feature breadth 误当作 lineage value。
- 所有 sample 都有 bounded truth note；truth basis 为：
  `synthetic_expected_result`、`manual_fixture_truth` 或
  `sanitized_human_review`。

### Dialect distribution

| Dialect | Samples |
| --- | ---: |
| mysql | 18 |
| postgres | 5 |
| snowflake | 2 |
| bigquery | 2 |
| hive | 1 |
| spark | 1 |

### Covered shapes

Corpus 覆盖：

- normal SQL、complex join、schema.table、quoted identifier、alias；
- CTE、nested subquery、set operation、multi statement；
- `INSERT SELECT`、temporary table/view、`MERGE`、`UPDATE`；
- MySQL `INSERT IGNORE`、`INSERT SET`、`REPLACE`；
- Hive partition、Spark temporary view、BigQuery `QUALIFY`；
- dynamic SQL 的静态重建、Python AST parse recovery、unknown wrapper；
- invalid SQL、literal/comment masking、table-valued function；
- target-only DML、parse failure、statement type mismatch。

所有 fixture 中的 relation identity 都是 `DEMO_*`、`TMP_*`、`SESSION_*` 或
`CATALOG_*` synthetic namespace。runner 的 privacy validation 会拒绝明显的
credential、URL、绝对路径和非 synthetic qualified identifier。

## Adapter 与分类 contract

### Legacy

直接复用当前 production parser 的：

```text
_extract_python_candidates_with_reason
_parse_sql_candidates
```

只观察 SQL extraction / SQL step 结果，不调用 DAG、Audit、Materialization 或
DWS。legacy 的 confidence 标记为 `production_semantics`，但这不等于每个结果
都是真实 truth。

### SQLGlot

SQLGlot 只接收 corpus 提供的静态 SQL text。对于 Python wrapper，静态 SQL 是
synthetic reconstruction；SQLGlot 不负责 Python AST extraction，也不能因为
它能 parse SQL 就声称恢复了动态 Python。

AST mapping 只输出：

```text
statement_type
sources
target
cte
parse_status
evidence
confidence
```

`confidence=parser_result_only`。unsupported statement 或 parse exception 不猜
source/target，统一保留失败原因类别。

### 两层分类

runner 分开输出：

1. `availability_class`：`MATCH`、`LEGACY_ONLY`、`SQLGLOT_ONLY`、
   `BOTH_UNRESOLVED`、`BOTH_FAILED`、`ONE_FAILED_ONE_UNRESOLVED`、
   `BOTH_RESOLVED_DISAGREEMENT`；
2. `failure_classes`：`LEGACY_FAILED`、`SQLGLOT_FAILED`；
3. `truth_class`：`MATCH`、`LEGACY_MORE_CONSERVATIVE`、
   `SQLGLOT_MORE_CONSERVATIVE`、`LEGACY_MORE_AGGRESSIVE`、
   `SQLGLOT_MORE_AGGRESSIVE`、`BOTH_UNRESOLVED`、`LEGACY_FAILED`、
   `SQLGLOT_FAILED`、`BOTH_FAILED` 或 `BOUNDARY_NOT_COMPARABLE`。

mismatch dimension 至少区分：`source`、`target`、`cte`、`statement_type`、
`parse_status`。

## Accuracy 与 disagreement evidence

### SQL semantics accuracy

| Backend | Eligible | Truth exact | Exact rate | FP events | FN events | Silent wrong target | Silent wrong source | Parse failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy | 24 | 21 | 87.50% | 1 | 3 | 2 | 1 | 1 |
| SQLGlot | 24 | 22 | 91.67% | 0 | 2 | 0 | 0 | 3 |

这里的 FP/FN 是相对 fixture truth 的 bounded event，不是四套真实 profile 的
生产 precision 估计。`truth exact` 也不等于 SQLGlot 是 oracle。

### Comparison counts

| Dimension / class | Count |
| --- | ---: |
| `MATCH` availability | 20 |
| `BOTH_RESOLVED_DISAGREEMENT` | 1 |
| `LEGACY_ONLY` | 2 |
| `SQLGLOT_ONLY` | 2 |
| `BOTH_UNRESOLVED` | 2 |
| `ONE_FAILED_ONE_UNRESOLVED` | 1 |
| `BOTH_FAILED` | 1 |
| `LEGACY_FAILED` failure class | 3 |
| `SQLGLOT_FAILED` failure class | 3 |
| SQL semantics disagreements | 4 |
| extraction boundary / outside-scope findings | 5 |

### Reviewed disagreements

| Sample shape | Truth class | Truth review | Evidence / implication |
| --- | --- | --- | --- |
| MySQL `INSERT IGNORE` | `LEGACY_MORE_CONSERVATIVE` | `sanitized_human_review` | legacy 的 regex 先看到 `SELECT`，结果丢失 target 且 statement type 变成 `select`；SQLGlot 保留 insert target。属于真实的 source/target contract blind spot。 |
| MySQL `INSERT SET` | `SQLGLOT_MORE_CONSERVATIVE` | `sanitized_human_review` | legacy 至少保留 target；SQLGlot parse failure。说明 SQLGlot 不能作为无条件 fallback。 |
| unterminated block comment | `LEGACY_MORE_AGGRESSIVE` | `sanitized_human_review` | legacy 对 invalid SQL 静默产出 target/source；SQLGlot 显式失败。silent wrong target/source 比 parse failure 更危险。 |
| MySQL `REPLACE` | `BOTH_FAILED` | `sanitized_human_review` | 合法 DML 形态在本 bounded mapping 中双方都失败；SQLGlot 没有自动解决 parser coverage。该样本也在 production scope 外。 |

`DELETE` 的 SQLGlot-only 结果不计入上表 SQL semantics accuracy：它证明了
feature breadth，不证明当前 write-through lineage contract 应该把 delete 当作
source → target derivation。

### CTE / subquery / alias / MERGE / UPDATE

normal SQL、complex join、CTE、nested subquery、alias、multi statement、quoted
identifier、schema/table、MERGE、UPDATE、Hive partition、Spark view、BigQuery
QUALIFY 和 table-valued function sample 均通过 synthetic expected result 或
manual fixture truth 复核。没有观察到 SQLGlot 在这些已覆盖 shape 上比 legacy
更可信的系统性优势；`INSERT IGNORE` 是本轮最明确的 legacy blind spot。

### Parse failure 与 dynamic boundary

- unknown wrapper 与 runtime dynamic SQL：两边均 unresolved；这是安全结果，
  不是 SQLGlot 的缺失。
- malformed Python：SQLGlot 不能代替 Python extraction。静态重建 sample 只能
  证明 SQL AST 层可比较，不能证明 SQLGlot 能恢复 Python。
- invalid SQL：legacy 的“有结果”是 false positive 风险；SQLGlot 的失败是更
  合理的 evidence provenance。
- SQLGlot 对合法 `INSERT SET` 的失败，以及双方对 `REPLACE` 的失败，保留为
  fallback 风险证据。

## Performance

benchmark 不是 CI wall-clock gate。所有数字来自本地 Windows/Python 运行，应用
于比较相对倍数、分位数和 sample scale；不要把单次毫秒数当作跨机器 SLA。

benchmark sample selection 为 `sqlglot_input is not null`，共 `26` 个 sample，
每个 backend `2,600` operations。`legacy` 是 production boundary（包含
Python extraction）；`legacy_sql_surface` 与 SQLGlot 接收相同的静态 SQL text，
用于公平 SQL parser surface 对比。

| Backend | Total ms | Mean ms/sample | P50 ms | P95 ms | Max ms | Failure ops | Unresolved ops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy production boundary | 482.0913 | 0.185420 | 0.181100 | 0.294415 | 0.8223 | 200 | 100 |
| legacy same SQL surface | 479.0637 | 0.184255 | 0.176150 | 0.297705 | 0.6279 | 100 | 100 |
| SQLGlot | 738.8010 | 0.284154 | 0.259000 | 0.519290 | 1.1074 | 300 | 0 |

Relative cost：

- SQLGlot / legacy production boundary：mean `1.5325x`，P95 `1.7638x`；
- SQLGlot / legacy same SQL surface：mean `1.5422x`，P95 `1.7443x`。

这不是脆弱的 hard threshold。它只说明 shadow 需要采样、异步或离线 replay
预算；若每条 production program 都同步跑两套 parser，overhead 不能忽略。

## Integration Assessment

### 方案 A：Shadow

**结论：有价值，但本 PR 不接入。**

收益：

- 可发现 `INSERT IGNORE` 这种 legacy target/statement mismatch；
- 可把 legacy 的 silent wrong result 与 SQLGlot parse failure 分开观测；
- evidence provenance 能保留 backend、版本、dialect、parse status、confidence；
- 不改变 production fact source。

成本：

- 每条样本需要记录双 backend 的 result digest 和 bounded mismatch；
- 需要固定 SQLGlot version、dialect registry 和 upgrade replay；
- 需要为 disagreement 做人工/synthetic truth review；
- 同步执行约 `1.55x` SQL parser surface 成本，P95 约 `1.76x`。

建议未来若实施，只从 offline/controlled replay 或低比例异步 shadow 开始，
不得把 disagreement 自动升级为 production edge。

### 方案 B：Fallback

**结论：不采用。**

`legacy failed → SQLGlot` 会遇到：

- SQLGlot 对合法 MySQL `INSERT SET` failure；
- 双方对合法 MySQL `REPLACE` 都 failure；
- dialect 配置错误时的 parse/semantic drift；
- fallback 可能把 legacy bug 隐藏在“另一套 parser 成功”后面；
- provenance、confidence 和 downstream acceptance 需要新增语义，而不能伪装成
  legacy fact。

本研究没有证据证明 SQLGlot 的 fallback precision 足以承担这些风险。

### 方案 C：Selected Dialect

**结论：本轮不路由。**

MySQL edge case 确实发现了增量差异，但 corpus 是 synthetic，不能推导真实
profile 的频率、业务语义或收益密度。为单一 dialect 增加 routing、配置和
maintenance surface 的证据不足。后续只有在真实 sanitized feature counts
确认某个 dialect gap 具有规模价值时，才另开 implementation Issue。

## Dependency / Maintenance Cost

- SQLGlot `30.12.0` 需要额外 dependency；本轮通过
  `requirements-research.txt` 隔离，没有污染 production requirements。
- 每次 SQLGlot upgrade 都可能改变 parser AST、dialect support、warning 和
  unsupported fallback 行为，必须重跑 corpus 与 truth review。
- AST-to-lineage mapping 不是简单的 `parse success`：target、CTE、subquery、
  table-valued function、statement type 和 source authority 都要单独定义。
- 版本、dialect 和 mapping configuration 必须进入 evidence；否则 shadow 差异
  无法解释。
- 依赖体积、供应链、启动/运行 overhead 和 parser semantic drift 都是持续维护
  成本，而不是一次性 demo 成本。

## Production Boundary

```text
Production Code Changes: NONE
```

本 PR 只新增 research runner、synthetic/sanitized corpus、optional research
requirement、runner tests 和本报告。没有修改：

- `shared/lineage/physical_dag.py` production parser interface；
- Materialization、Audit、DatasetIdentity、DWS、pipeline version；
- #41 未 merge 的 backend abstraction。

## Verification

已执行：

```text
ruff check tools/research/sqlglot_compare.py tests/tools/test_sqlglot_compare.py
ruff format --check tools/research/sqlglot_compare.py tests/tools/test_sqlglot_compare.py
python -B -m unittest tests.tools.test_sqlglot_compare -v
python -B -m tools.research.sqlglot_compare --corpus tests/fixtures/research/sqlglot_corpus.jsonl --benchmark-repeats 100 --benchmark-warmup 5
```

research tests 覆盖 corpus privacy、required shapes、classification、truth
review、determinism、public report 不泄露 SQL/asset text 和 benchmark quantiles。

## Recommended Follow-up

如果维护者接受 `KEEP_AS_SHADOW`，另开 implementation Issue，限定为：

1. 设计不会改变 production fact source 的 shadow evidence sink；
2. 先接 controlled replay / offline report，不接 synchronous fallback；
3. 明确 backend/version/dialect/config/provenance contract；
4. 以新的真实 sanitized feature counts 复核 `INSERT IGNORE`、`INSERT SET`、
   invalid SQL 等 disagreement；
5. 不在该 implementation Issue 中自动采纳 SQLGlot source/target，也不扩展为
   column lineage。

该 follow-up 不属于本 PR 的实现范围。
