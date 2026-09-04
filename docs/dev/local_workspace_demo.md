# Local Workspace + demo PostgreSQL 开发指南

本指南说明如何用完全虚构的本地 workspace 和 `demo_meta` metadata model 演示代码审查、调度校验与字段血缘。公开仓库不要求连接任何外部服务。

## 1. 准备环境

- Python 3.10+
- PostgreSQL（仅用于本地 demo，可不启动外部服务时运行纯函数测试）
- `requirements.txt` 中的依赖

创建本地配置：

```bash
copy configs\audit_datasource.example.yaml configs\audit_datasource.local.yaml
```

将 `configs/audit_datasource.local.yaml` 中的本地数据库参数按需填写，并设置密码环境变量：

```powershell
$env:PYTOOLS_DEMO_DB_PASSWORD="your-local-password"
$env:AUDIT_DB_PROFILE="demo_local"
```

不要把 `.local` 配置或密码写入 Git。

## 2. 初始化 demo metadata

```bash
psql -h 127.0.0.1 -U demo_user -d pytools_demo -f docs/dev/local_pg_audit_meta.sql
```

脚本只创建 `demo_meta` 下的虚构表，例如 `demo_meta.jobs`、`demo_meta.programs` 和 `demo_meta.relations`。

## 3. 使用本地 workspace

启动：

```bash
streamlit run apps/svn_check/app.py --server.address 127.0.0.1 --server.port 8888
```

选择“本地目录审计”，输入：

```text
tests/fixtures/demo_workspace
```

该 fixture 仅包含 `DEMO_*` 任务、SQL 和 Excel 样本，适合测试以下能力：

- SQL/DDL 解析和表名规则；
- 计划、作业流、作业依赖检查；
- 程序路径到目标表的推导；
- 结果表与虚构来源系统的展示；
- 本地文件血缘索引和导出。

## 4. 运行测试

```bash
python -B -m unittest discover -s tests -t . -v
```

测试应使用 mock 或 fixture，不应连接真实 PostgreSQL、SVN、FTP、HTTP API 或 JDBC 集群。

## 5. 常见问题

### 缺少 `PYTOOLS_DEMO_DB_PASSWORD`

PostgreSQL 连接会明确报错 `Missing required environment variable PYTOOLS_DEMO_DB_PASSWORD`。请在当前 shell 配置变量，不要在代码或 YAML 中填入密码。

### demo metadata 表不存在

重新执行 `docs/dev/local_pg_audit_meta.sql`，确认数据库为本地 `pytools_demo`。

### SQLite 或导出文件出现在工作区

这是运行态文件。`runtime/`、`logs/`、`*.db`、`*.sqlite`、`*.html` 和导出压缩包已加入 `.gitignore`，发布前只提交源代码和示例配置。

### 审查页无法拉取 SVN

公开版默认只提供占位 SVN 地址。若确需使用 SVN，请在本地 `.local` 配置中设置地址和凭据，并确保不把该文件复制到 Public 仓库。
