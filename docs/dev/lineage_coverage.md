# Lineage Coverage Funnel

本页描述真实脚本解析的脱敏复测入口，不引入新的 lineage 架构阶段，也不改变
Physical DAG、audit 或 atomic publish 的事实语义。

## 覆盖漏斗

定时任务在同一遍 `ProgramSource` → Physical DAG → materialization 流程中观察以下阶段：

```text
candidate → SQL step → write target → Physical DAG → LineageEdge
```

报告按 `(environment, source_profile)` 聚合，只保留计数、比例和固定枚举原因。不会写入
program name、源码、SQL、表名、连接配置或异常文本。报告字段包括：

- `sql_candidate_count` / `programs_with_sql_candidates`
- `sql_step_count` / `programs_with_sql_steps`
- `programs_with_write_target`
- `physical_node_count` / `physical_edge_count`
- `lineage_edge_count` / `programs_with_lineage_edges`
- `failure_reasons` 与 `lineage_failure_reasons`

`coverage_scope=ALL_PROGRAMS` 表示本次 provider snapshot 中的程序都完成了 DAG 观察；
增量运行只观察被 rebuild 的程序时报告为 `REBUILT_PROGRAMS`，不会把 source count 和
DAG count 重复相加。

## 复测入口

在已配置本地 provider 的环境中，使用 `--force-rebuild` 让当前 snapshot 中的每个
程序重新进入既有 parser：

```bash
python -B -m jobs.crontab.imp_lineage_edge \
  --force-rebuild \
  --coverage-report artifacts/lineage_coverage/replay.json
```

也可以直接执行 `jobs/crontab/imp_lineage_edge.py`。provider 配置仍由现有
`PYTOOLS_LINEAGE_PROVIDER_CONFIG` 和本地配置约定控制；公开仓库不提供真实凭据。
`--progress-every N` 可调整 rebuild 进度日志频率。JSON 输出路径必须位于
`artifacts/lineage_coverage/` 下，运行态报告不会提交到 Git。

测试复测入口：

```bash
python -B -m unittest tests.shared.test_lineage_physical_dag tests.shared.test_lineage_coverage tests.shared.test_lineage_job_observability -v
```

## 兼容性边界

本轮按仓库证据做了以下兼容性修复：

| 证据 | 保守处理 |
| --- | --- |
| `shared/db/gaussdb.py` 与 `shared/db/postgres.py` 的 `fetch_all`、`execute_sql` | 识别 profile-first 与 SQL-first 的已知参数位置 |
| `shared/db/gaussdb.py` 的 `select_sql_with_profile`、`run_sql_with_profile` | 识别 `sql_str` 及第二个位置参数 |
| `apps/svn_check`、`tools/integrations` 的 `select_sql`、`select_mysql_sql` | 仅解析静态第一个 SQL 参数 |
| `tests/fixtures/demo_workspace/.../demo_job.py` | 仅当模块只有一个静态可解析 `return SQL_TEXT` 时进入 parser |
| `apps/svn_check` 旧 splitter、`shared/text/regex.py` legacy 抽取 | 只作为审计证据，不重新启用 regex 表抽取，也不直接写入正式 `LineageEdge` |

动态 f-string、未知 wrapper、多个互斥 return、无法建立静态绑定和无法解析的 Python
不会猜测，报告其安全失败原因。

## 与 progress logging PR 的协调

阶段日志复用了未合并的 progress logging PR #25（`ccb5e60`）的脱敏日志约定：
`source_load`、`incremental_plan`、`build`、`publish` 和 `job` 阶段只输出 count、耗时、
受限 batch ID 与异常 class；coverage 行另行输出聚合计数。该 PR 仍可独立审阅/合并，
本分支不依赖其先合并后才能执行 coverage。
