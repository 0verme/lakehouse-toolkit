# 本地 demo metadata 与宽表血缘

这份文档说明 `docs/dev/local_pg_audit_meta.sql` 如何为本地演示提供元数据。模型完全独立于任何生产系统，只使用 `demo_meta` schema 和虚构资产。

## 模型概览

| 表 | 用途 |
| --- | --- |
| `demo_meta.plans` | 计划目录及其描述 |
| `demo_meta.jobs` | 作业、作业流、程序、依赖和状态 |
| `demo_meta.programs` | 程序文件与目标表 |
| `demo_meta.result_receipts` | 结果表、接入计划和来源系统的演示映射 |
| `demo_meta.job_outputs` | 作业输出路径 |
| `demo_meta.processes` | 用于 SQL 血缘解析的脚本文本 |
| `demo_meta.relations` | 表到表的虚构血缘 |
| `demo_meta.runtimes` | 虚构运行时间 |
| `demo_meta.term_roots` | 命名词根 |
| `demo_meta.reference_tables` | 字典/参考表登记 |

## 宽表演示链路

SQL 脚本创建的链路为：

```text
DWF.F_DEMO_EVENT
    ↓
DWD.R_DEMO_EVENT
    ↓
DWM.M_DEMO_SUMMARY
    ↓
DWP.DEMO_DWP_EXPORT
```

- `demo_meta.programs.file_path` 指向 `examples` 风格的虚构路径。
- `demo_meta.programs.target_table` 保存程序的目标表。
- `demo_meta.jobs.program_name` 把作业与程序连接起来。
- `demo_meta.jobs.dependency_text` 使用 `33:DEMO_JOB_EXPORT` 形式演示依赖解析。
- `demo_meta.result_receipts` 把 `DWP.DEMO_DWP_EXPORT` 与 `DEMO_PLAN_EXPORT_DAY` 关联。
- `demo_meta.job_outputs` 提供 `/demo/output/demo_export.csv` 这样的非真实输出路径。

## 运行本地 demo

1. 准备 PostgreSQL 数据库，例如 `pytools_demo`。
2. 使用 `configs/audit_datasource.example.yaml` 创建本地配置，或复制为 `configs/audit_datasource.local.yaml` 并按需修改。
3. 设置 `PYTOOLS_DEMO_DB_PASSWORD`，再执行：

   ```bash
   psql -h 127.0.0.1 -U demo_user -d pytools_demo -f docs/dev/local_pg_audit_meta.sql
   ```

4. 使用本地 workspace 启动审查页：

   ```bash
   streamlit run apps/svn_check/app.py --server.address 127.0.0.1 --server.port 8888
   ```

   运行模式选择“本地目录审计”，目录可使用 `tests/fixtures/demo_workspace`。

## 安全边界

- 该 SQL 只用于演示，不要把生产元数据导入公开仓库。
- 真实 schema、表名、字段名、路径和连接凭据放在被忽略的 `.local` 配置中。
- `runtime/`、`logs/`、SQLite 和导出文件属于运行态，不应提交。
