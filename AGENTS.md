# AGENTS.md

本文件是公开版 Lakehouse Toolkit 的开发约定，帮助贡献者在不接触真实环境的情况下运行和修改项目。

## 项目结构

- `apps/`：Streamlit 应用
- `tools/`：手动触发的 Python / PyWebIO 工具
- `jobs/`：可选的批处理与导入脚本
- `shared/`：数据库、搜索、血缘、导出和配置公共能力
- `configs/`：公开配置模板与工具注册表
- `docs/`：开发文档和 demo metadata model
- `tests/`：使用 fixture 和 mock 的单元测试
- `runtime/`、`logs/`：本地运行态目录，不提交

## 配置原则

1. `configs/*.example.yaml`、`configs/*.example.json` 只能包含占位值。
2. 本地配置使用 `*.local.yaml`、`*.local.json`，这些文件已被 `.gitignore` 忽略。
3. 密码、Token、OAuth Secret、私钥和真实连接串只能通过环境变量或本地密钥管理器提供。
4. 缺少必填凭据时必须显式失败，禁止弱默认值或静默 fallback。
5. SQL 标识符来自配置时，先通过安全校验；不要把未验证的用户输入直接拼进 SQL。

## 开发约定

- 优先复用 `shared/` 中的公共能力，修改前检查调用方。
- 新增可启动工具时同步更新 `configs/tools.yaml`，并保持 `workdir`、`script`、`port` 等字段可跨平台使用。
- 默认以 `127.0.0.1` 或 `localhost` 运行示例；不要在源码中写入真实 IP、域名、服务器路径或生产资产名。
- 演示数据使用 `DEMO_*`、`demo_meta` 等虚构命名，不能由样例推断真实业务关系。
- 不要删除或提交用户本地的 `runtime/`、`logs/`、数据库文件、导出文件或外部驱动。

## 验证命令

```bash
python -B -m unittest discover -s tests -t . -v
```

涉及配置或启动逻辑时，至少运行对应单元测试；外部数据库、SVN、FTP、HTTP API 和 JDBC 测试必须 mock 或 skip。

## 提交前检查

```bash
git status --short
git diff --stat
git diff
```

提交前确认没有 `.env`、密钥、Token、私钥、生产凭据、运行产物或未经许可的二进制文件。许可证选择由项目维护者另行决定，本仓库不预设许可证。
