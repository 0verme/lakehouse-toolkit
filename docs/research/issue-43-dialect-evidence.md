# Issue #43：四套 profile 的 SQL dialect evidence review

## 结论

```text
INSUFFICIENT_EVIDENCE
```

当前公开的四 profile replay evidence 证明了 pipeline 可以完成 full replay，**没有证明真实 corpus 的每类 SQL syntax 已被逐项观察并支持**。因此：

- 没有确认的 `DIALECT_SYNTAX` gap；
- 也不能把缺少 feature-level evidence 当作 `NOT_NEEDED`；
- 本 Issue 不实现 dialect framework、parser hook 或 SQLGlot integration；
- 如果后续 replay 仍找不到 dialect gap，可合法关闭为 `CLOSED AS NOT NEEDED`。

## Scope / Privacy

- profile 使用 `profile_A`、`profile_B`、`profile_C`、`profile_D` 作为公开标签；不输出真实 profile 名称。
- 只保留聚合 count、固定 category、固定 syntax feature 和 replay status。
- 不包含 SQL、表名、列名、`program_name`、源码、连接配置、内网路径或凭据。
- 表中的 `UNKNOWN` 表示当前 sanitized evidence 没有提供该字段，不表示 count 为零。

## 已核对的 evidence

| Evidence | Sanitized facts | 能证明什么 | 不能证明什么 |
| --- | --- | --- | --- |
| #34 final full replay | 4 个 profile；`ProgramSource=20,498`；`LineageEdge=111,323`；`Issues=11,853`；build/publish/job/coverage PASS | 四 profile 的 replay pipeline 完成，聚合 coverage 有分母 | 没有 syntax feature、statement type、dialect failure 的 profile-level 计数 |
| #34 profile totals | `5,387 + 8,329 + 3,651 + 3,131 = 20,498` | 四个 profile 的 source total 可对账 | 无法推导任一 profile 使用了哪些 SQL feature |
| #34 earlier failure note / #50 | 1 个历史 Python AST parse robustness case，已拆分并修复路径 | 存在非 dialect 的 parser robustness evidence | 不能把 Python parse failure 叫作 dialect gap |
| #26/#27 coverage contract | 现有 report 只输出 funnel count 和固定 failure reason | 可以复用 sanitized replay/coverage 入口 | 现有 schema 没有 CTE/MERGE/function/partition 等 feature 标签 |

`replay PASS` 仅代表本次流程成功结束，不会自动转换为 `SUPPORTED`。

## 四 profile dialect evidence matrix

状态定义：

- `OBSERVED`：真实 corpus 中观察到该 feature，但 support 结果未确认；
- `SUPPORTED`：观察到且 parser 结果被 evidence 确认；
- `FAILED`：观察到且 failure 已被确认是 dialect-specific；
- `UNKNOWN`：没有相应的 sanitized feature-level evidence。

| Profile | Replay | Programs | CTE | MERGE | UPDATE | DELETE | INSERT | Identifier quoting | Function syntax | Date functions | Partition syntax | Warehouse hints | Subquery | Alias |
| --- | --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| profile_A | PASS | 5,387 | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
| profile_B | PASS | 8,329 | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
| profile_C | PASS | 3,651 | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
| profile_D | PASS | 3,131 | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |

### Matrix review

- `CTE`、`MERGE`、`UPDATE`、`DELETE`、`INSERT`：没有公开的真实 profile feature count。
- identifier quoting、function/date syntax、partition syntax、warehouse-specific hints：没有公开的真实观察记录。
- subquery、alias：现有 synthetic fixture / parser unit test 不是四套真实 profile evidence，不能填入 `SUPPORTED`。
- 因此当前 `Confirmed Dialect Gaps` 为空，但 `DIALECT_SYNTAX` 不是 `0`，而是 `UNKNOWN`。

## Failure 分类

| Scope | PYTHON_WRAPPER | DYNAMIC_SQL | TARGET_AUTHORITY | PROGRAM_NAME | PARSER_BUG | DIALECT_SYNTAX | UNKNOWN |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 四 profile aggregate | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 1* | UNKNOWN | UNKNOWN |

`1*` 是 #34 公开评论中提到的历史 Python AST parse robustness case，按 `PARSER_BUG` 记录为**非 dialect** evidence；它已经由 #50 拆分处理。其余 category 没有公开的真实 profile count，不能擅自写成 `0`。

明确排除：

- Python wrapper / 未识别调用：应归 `PYTHON_WRAPPER`，不能归 `DIALECT_SYNTAX`；本次没有 profile-level count。
- dynamic SQL / binding：应归 `DYNAMIC_SQL`，不能归 `DIALECT_SYNTAX`；本次没有 profile-level count。
- target/source inference：应归 `TARGET_AUTHORITY`；本次没有 profile-level count。
- `program_name` shape/target review：应归 `PROGRAM_NAME`；本次 follow-up 没有 failure count。
- generic `UNSUPPORTED_STATEMENT`、`PYTHON_PARSE_FAILED`、`NO_SQL_STEP`：原因不足时保留 `UNKNOWN`，不能自动升级为 dialect。
- replay issue total、性能长尾和 publish 状态不是 SQL dialect failure。

分类与报告 helper：`tools/research/dialect_evidence.py`。它只接受固定枚举和 aggregate count；对 generic parse failure 默认返回 `UNKNOWN`，只有明确的 `DIALECT_SYNTAX_GAP_CONFIRMED` 才能生成 dialect category。

## Decision framework 对照

| Decision | 需要的 evidence | 本次结果 |
| --- | --- | --- |
| `NOT_NEEDED` | 四 profile 的实际 syntax 已逐项覆盖且没有 confirmed dialect gap | 未满足：feature evidence 缺失 |
| `MINIMAL_HOOK_ONLY` | 少量、明确、局部的 dialect gap，且不形成跨 profile 系统性问题 | 未满足：没有 confirmed gap |
| `ABSTRACTION_REQUIRED` | 多 profile / 多 syntax 的重复 dialect failure，需共享边界 | 未满足：没有 confirmed gap |
| `INSUFFICIENT_EVIDENCE` | replay 有聚合结果，但缺少逐 feature、逐 profile 的 sanitized evidence | **满足** |

## SQLGlot 边界

本次没有运行或接入 SQLGlot。若后续拿到明确的 sanitized dialect failure，可参考 #42 做 shadow comparison，比较 feature detection、source/target、parse failure 和性能；在没有 gap evidence 前，不把 SQLGlot 变成默认答案，也不接入 production。

## Recommended follow-up

1. 在可访问真实 provider 的环境重新执行四 profile replay，沿用 #26/#27 的 aggregate-only report。
2. 仅额外输出上表固定 feature 的 `observed/supported/failed/unknown` 和 category/count；禁止输出 SQL、源码、资产名和 `program_name`。
3. 由 parser owner 与 data-domain owner 对每个 `FAILED` 样本确认根因；generic parser failure 必须先排除 wrapper、dynamic SQL、target authority、program identity 和 parser bug。
4. 若四 profile feature matrix 全部为 `SUPPORTED` 且无 `DIALECT_SYNTAX` failure，可将本研究结论更新为 `NOT_NEEDED` 并 `CLOSED AS NOT NEEDED`。
5. 若出现真实 dialect gap，另开 implementation Issue，只列出受影响 syntax/profile、最小 hook 边界、fixture 和性能预算；本 Issue 不实现 #41 或任何 dialect framework。

## Parser change guard

```text
Parser Changed: MUST BE NO
```
