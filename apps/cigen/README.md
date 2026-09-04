# cigen

`cigen` 是一个面向数据命名治理的 Streamlit 工具，用于维护词根、检查表/字段命名并导入 CSV 或 Excel。公开版默认只指向可选的本地 demo JDBC profile，不包含任何生产 metadata。

## 功能

- 未登录用户只读查看词根
- 管理员可以新增、修改和导入词根
- 支持 CSV / Excel 导入
- 根据字段类型给出命名建议

## 运行

```bash
streamlit run apps/cigen/app.py --server.address 127.0.0.1 --server.port 8503
```

应用使用 `PYTOOLS_DB_PROFILE` 选择 JDBC profile，默认值为 `demo`。JDBC 驱动需要使用者依据目标数据库官方文档和许可证自行准备；仓库不携带驱动。

## 管理员与凭据

本应用不会创建默认账号或固定密码。请在本地 metadata 数据库中预先创建用户，并通过 `configs/database.local.yaml` 和对应的 `password_env` 提供连接密码。不要把密码写入代码、YAML 或文档。

## demo metadata

可使用仓库根目录的 `docs/dev/local_pg_audit_meta.sql` 初始化独立的 `demo_meta` model。默认表名包括：

- `demo_meta.app_users`
- `demo_meta.term_roots`

表名可以通过 `PYTOOLS_METADATA_APP_USERS_TABLE` 和 `PYTOOLS_METADATA_TERM_ROOTS_TABLE` 覆盖。公开示例只使用虚构命名。

## 导入格式

必填列：

- `root_code`
- `root_cn`

可选列：

- `category`
- `status`
- `remark`
