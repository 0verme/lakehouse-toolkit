# pytools

`pytools` 是一个面向数据开发团队的 Python 工具平台，提供 Streamlit 应用、PyWebIO 小工具以及可选的批处理入口。项目聚焦通用的数据资产治理和开发效率场景，不附带任何生产环境数据或连接凭据。

## 核心功能

- SQL 表名解析、SQL 审计和 DDL 命名检查
- 数据资产、字段词根和指标目录管理
- 表级/字段级血缘分析与可视化导出
- 作业依赖、调度清单和循环依赖检查
- 本地 workspace 与报表文件搜索
- Excel / CSV 导入导出
- 可选的 PostgreSQL、JDBC、MySQL、SVN、FTP 和任务编排服务适配

所有外部服务都必须由使用者通过本地配置或环境变量显式启用；默认示例只使用 `localhost`、`127.0.0.1` 和 `.example.invalid` 占位地址。

## 项目结构

```text
pytools/
├─ apps/          # Streamlit 应用
├─ tools/         # PyWebIO / Python 工具
├─ jobs/          # 可选导入、同步和迁移入口
├─ shared/        # 数据库、搜索、图、血缘、导出和配置公共模块
├─ configs/       # 通用配置、example 模板和本地配置说明
├─ docs/dev/      # 独立的 demo metadata model 与开发指南
├─ resources/     # CSS 和不随仓库分发的外部驱动说明
└─ tests/         # fixture + mock 单元测试
```

`runtime/` 和 `logs/` 只用于本地运行，SQLite、HTML、CSV、压缩包和日志不会作为公开资源提交。

## 快速启动

### 安装依赖

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 启动工具管理台

```bash
streamlit run apps/webadmin/app.py --server.address 127.0.0.1 --server.port 8500
```

管理台读取 `configs/tools.yaml`。本机需要自定义路径或端口时，复制 `configs/tools.local.example.yaml` 为 `configs/tools.local.yaml`；该文件已被忽略，不要提交。

### 启动常用应用

```bash
streamlit run apps/svn_check/app.py --server.address 127.0.0.1 --server.port 8501
streamlit run apps/metric_portal/metric_portal_streamlit_mvp.py --server.address 127.0.0.1 --server.port 8502
streamlit run apps/interface_manager/app.py --server.address 127.0.0.1 --server.port 8504
```

`svn_check` 可在“本地目录审计”模式下直接分析 `tests/fixtures/demo_workspace`，无需 SVN 或数据库。

## 配置方式

公开模板：

| 文件 | 用途 |
| --- | --- |
| `configs/database.example.yaml` | 可选 JDBC profile 模板 |
| `configs/audit_datasource.example.yaml` | 本地 PostgreSQL metadata profile |
| `configs/migrate/clusters.example.json` | 两个 demo 集群的迁移模板 |
| `configs/svn.example.yaml` | SVN 项目地址和环境变量引用模板 |
| `configs/tools.yaml` | 通用工具注册表 |
| `configs/tools.local.example.yaml` | 本机覆盖示例 |
| `docs/dev/local_pg_audit_meta.sql` | 独立虚构 metadata model |

需要本地化的配置使用同名 `.local.yaml` / `.local.json`。真实地址、账号、密码、Token 和 Secret 不应写入源码、example 文件或 README。

### 本地 metadata demo

1. 准备本地 PostgreSQL 数据库 `pytools_demo`。
2. 根据 `configs/audit_datasource.example.yaml` 创建 `configs/audit_datasource.local.yaml`。
3. 设置 `PYTOOLS_DEMO_DB_PASSWORD`。
4. 执行：

```bash
psql -h 127.0.0.1 -U demo_user -d pytools_demo -f docs/dev/local_pg_audit_meta.sql
```

完整说明见 [`docs/dev/local_workspace_demo.md`](docs/dev/local_workspace_demo.md) 和 [`docs/dev/local_pg_wide_table_lineage.md`](docs/dev/local_pg_wide_table_lineage.md)。

### 指标门户管理员初始化

指标门户不会创建 `admin` 或任何固定密码账号。首次启动且本地数据库没有用户时，必须显式设置：

```bash
# Linux/macOS
export ADMIN_USERNAME="choose-a-local-admin"
export ADMIN_PASSWORD="choose-a-strong-local-password"
# Windows PowerShell
$env:ADMIN_USERNAME="choose-a-local-admin"
$env:ADMIN_PASSWORD="choose-a-strong-local-password"
```

初始化完成后可以移除这两个变量；系统只读取已有用户记录。若变量缺失，应用会提示配置，不会使用弱默认值。

## 环境变量清单

| 环境变量 | 用途 | 必填 | 默认值 |
| --- | --- | --- | --- |
| `ADMIN_USERNAME` | 首次初始化指标门户管理员用户名 | 仅首次初始化时 | 无 |
| `ADMIN_PASSWORD` | 首次初始化指标门户管理员密码 | 仅首次初始化时 | 无 |
| `AUDIT_DB_PROFILE` | 审计 metadata profile | 否 | `demo_local` |
| `APP_ENV` | 选择审计环境映射 | 否 | 无 |
| `PYTOOLS_DEMO_DB_PASSWORD` | 本地 PostgreSQL/JDBC demo 密码 | 使用对应数据库时 | 无 |
| `PYTOOLS_DB_PROFILE` | 通用 JDBC profile | 否 | `demo` |
| `PYTOOLS_METADATA_SCHEMA` | metadata schema | 否 | `demo_meta` |
| `PYTOOLS_METADATA_*_TABLE` | 覆盖某个 metadata 表名 | 否 | 对应 `demo_meta` 表 |
| `PYTOOLS_MYSQL_HOST` | 可选 MySQL 主机 | 否 | `localhost` |
| `PYTOOLS_MYSQL_USER` | 可选 MySQL 用户 | 使用 MySQL 工具时 | 无 |
| `PYTOOLS_MYSQL_PASSWORD` | 可选 MySQL 密码 | 使用 MySQL 工具时 | 无 |
| `PYTOOLS_MYSQL_DATABASE` | 可选 MySQL 数据库 | 否 | `pytools_demo` |
| `PYTOOLS_SVN_USERNAME` | SVN 用户名 | 使用需认证的 SVN 时 | 无 |
| `PYTOOLS_SVN_PASSWORD` | SVN 密码 | 使用需认证的 SVN 时 | 无 |
| `PYTOOLS_SVN_BIN` | SVN 可执行文件 | 否 | `svn` |
| `PYTOOLS_SVN_SAMPLE_URL` | SVN 本地调试地址 | 否 | `svn://svn.example.invalid/lakehouse/trunk` |
| `SVN_CHECK_EXPORT_BASE` | SVN 导出目录 | 否 | `runtime/export` |
| `SVN_CHECK_DOWNLOAD_ROOT` | 导出文件 HTTP 根地址 | 否 | `http://localhost:8500/exports` |
| `ASSET_PORTAL_BASE_URL` | 资产链接基地址 | 否 | `http://localhost:5099` |
| `REPORT_PREVIEW_BASE_URL` | 报表预览基地址 | 否 | `http://localhost:8500/reports` |
| `PYTOOLS_WORKSPACE_ROOT` | workspace 搜索根目录 | 否 | `examples/workspace` |
| `PYTOOLS_REPORT_ROOT` | 报表搜索根目录 | 否 | `examples/reports` |
| `PYTOOLS_UPSTREAM_ROOT` | 外部来源搜索根目录 | 否 | `examples/upstream` |
| `PYTOOLS_DIRECTORY_INDEX_PATH` | 目录索引文件 | 否 | `runtime/cache/directories.txt` |
| `PYTOOLS_WORKSPACE_HTTP_ROOT` | workspace 下载根地址 | 否 | `http://localhost:8500/workspace` |
| `PYTOOLS_REPORT_HTTP_ROOT` | 报表下载根地址 | 否 | `http://localhost:8500/reports` |
| `PYTOOLS_UPSTREAM_HTTP_ROOT` | 外部来源下载根地址 | 否 | `http://localhost:8500/upstream` |
| `PYTOOLS_FTP_HOST` | 可选 FTP 主机 | 使用 FTP 时 | `ftp.example.invalid` |
| `PYTOOLS_FTP_PORT` | 可选 FTP 端口 | 否 | `21` |
| `PYTOOLS_FTP_USER` | FTP 用户名 | 使用 FTP 时 | 无 |
| `PYTOOLS_FTP_PASSWORD` | FTP 密码 | 使用 FTP 时 | 无 |
| `CMS_API_BASE_URL` | 可选 CMS API 基地址 | 使用 CMS 适配器时 | 无 |
| `CMS_USERNAME` / `CMS_PASSWORD` | CMS API 用户凭据 | 使用 CMS 适配器时 | 无 |
| `CMS_CLIENT_ID` | CMS OAuth client id | 否 | `demo-client` |
| `CMS_CLIENT_SECRET` | CMS OAuth client secret | 使用 CMS OAuth 时 | 无 |
| `CMS_MYSQL_HOST` / `CMS_MYSQL_USER` | CMS metadata 数据库连接 | 使用 CMS 数据库时 | `localhost` / 无 |
| `CMS_MYSQL_PASSWORD` | CMS metadata 数据库密码 | 使用 CMS 数据库时 | 无 |
| `ORCHESTRATOR_API_BASE_URL` | 可选任务编排 API 基地址 | 使用编排适配器时 | 无 |
| `ORCHESTRATOR_USERNAME` / `ORCHESTRATOR_PASSWORD` | 编排 API 用户凭据 | 使用编排适配器时 | 无 |
| `ORCHESTRATOR_CLIENT_SECRET` | 编排 API OAuth Secret | 使用编排 OAuth 时 | 无 |
| `PYTOOLS_CONTROL_DB_URL` | 迁移队列控制库 JDBC URL | 使用迁移队列时 | 无 |
| `PYTOOLS_CONTROL_DB_USER` / `PYTOOLS_CONTROL_DB_PASSWORD` | 迁移队列控制库凭据 | 使用迁移队列时 | 无 |

带 `PASSWORD`、`SECRET` 的变量只在运行环境中设置；本表不展示任何 Secret 值。

## 安全说明

- 本仓库只公开源代码、虚构 fixture 和独立 demo metadata model。
- 所有凭据必须通过环境变量或外部 Secret 管理器注入；缺失必填变量会以 `Missing required environment variable XXX` 明确失败。
- 默认网络地址仅用于本地或 `.example.invalid` 文档示例，应用不会自动连接生产服务。
- `runtime/`、`logs/`、SQLite、HTML、CSV、SQL 压缩包、密钥文件和外部 JAR 已加入 `.gitignore`。
- `resources/jars/` 不包含 JDBC 驱动。需要 JDBC 的使用者必须依据目标数据库官方文档和适用许可证自行获取，并放到本地配置指定的位置。
- 运行前应检查上传的 SQL、workspace、报表和导出文件，避免把真实业务数据带入公开仓库。

## 开发说明

运行单元测试：

```bash
python -B -m unittest discover -s tests -t . -v
```

测试使用 `tests/fixtures/demo_workspace`、mock 和本地临时目录，不应访问真实数据库、SVN、FTP、HTTP API 或 JDBC 集群。新增外部服务测试时，请使用 mock 或显式 skip。

主要入口：

- `apps/webadmin/app.py`：工具管理台
- `apps/metric_portal/metric_portal_streamlit_mvp.py`：指标目录 demo
- `apps/interface_manager/app.py`：接口资产 demo
- `apps/svn_check/app.py`：本地 workspace 代码审查
- `shared/lineage/`：通用血缘和映射逻辑
- `tools/misc/xlsx_sql_tables.py`：Excel SQL 表名解析

## License

This project is licensed under the MIT License.
See [LICENSE](LICENSE) for details.
