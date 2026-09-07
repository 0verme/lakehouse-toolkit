# Lineage Phase 2：ProgramSource Providers

Phase 2 把不同 metadata 来源统一成 Phase 1 冻结的 `ProgramSource`。Provider 只
负责“获取程序”，不负责解释程序内容：

```text
MySQL / legacy metadata
          ↓
       Provider
          ↓
    ProgramSource
          ↓
 [Phase 3 Parser]
```

## Contract

`shared.lineage.providers.ProgramSourceProvider` 是轻量 `Protocol`：

```python
class ProgramSourceProvider(Protocol):
    def iter_program_sources(self) -> Iterable[ProgramSource]: ...
```

DEV 和 PROD 的调用方都可以只依赖 `Iterable[ProgramSource]`，不需要知道 MySQL、
metadata row、cursor 或文件细节。`iter_program_sources(providers)` 是一个薄的
streaming 聚合器，不会把所有程序先转换成 `list`。

Provider 不做 SQL parsing、表名提取、TMP 判断、sink/audit、Physical DAG、TMP
collapse 或 lineage materialization；`script_code` 到达 `ProgramSource` 后即停止。

## DEV MySQL profiles

`MySQLProcessProfile` 将两个维度分开：

- `environment`：运行环境，例如 `DEV`；
- `name`：来源逻辑身份，例如 `mysql_dev_a`，输出为 `source_profile`。

profile 数量没有固定上限。`configs/lineage_providers.example.yaml` 展示了 `1..N`
个 profile 的配置形状；本地使用时复制为被忽略的
`configs/lineage_providers.local.yaml`，不要把真实值提交到仓库。

每个 profile 选择一种连接来源，三种来源最终都归一为同一个
`MySQLConnectionSettings`：

1. `connection`：适合被忽略的 `lineage_providers.local.yaml`，直接填写
   `host`、`port`、`user`、`password`、`database`；
2. `connection_env`：填写上述五个字段对应的环境变量名，适合 CI / Docker；
3. legacy 顶层 `host_env`、`port_env`、`user_env`、`password_env`、
   `database_env`：保持已有配置不变。

例如本地模式为：

```yaml
connection:
  host: 127.0.0.1
  port: 3306
  user: DEMO_USER
  password: DEMO_PASSWORD_VALUE
  database: demo_meta
```

环境变量模式为：

```yaml
connection_env:
  host: LAKEHOUSE_DEV_A_MYSQL_HOST
  port: LAKEHOUSE_DEV_A_MYSQL_PORT
  user: LAKEHOUSE_DEV_A_MYSQL_USER
  password: LAKEHOUSE_DEV_A_MYSQL_PASSWORD
  database: LAKEHOUSE_DEV_A_MYSQL_DATABASE
```

同一 profile 不应同时配置多种来源；这样可以避免迁移期间出现不明确的凭据
优先级。连接值在 Provider 开始读取时解析，缺少任一必填值会带着
`environment` 和 `source_profile` 显式失败，不会回退到 `localhost`、demo 数据库
或空密码。直接值不会进入 profile 的 `repr`，但仍只能放在被忽略的本地配置中。

`table`、`program_name_column`、`script_code_column` 和可选的
`expected_target_column` 都会通过 `shared.config.env.safe_identifier` 校验后才
进入查询模板。运行时数据仍由 cursor 返回，不把用户值拼接进 SQL。

## Batch / streaming

`MySQLProcessProvider` 使用：

```text
execute
  → fetchmany(batch_size)
  → 逐行映射并 yield ProgramSource
  → 空 batch 时结束
  → finally close cursor / connection
```

它不会全量读取程序代码。即使映射或消费过程中出现异常，Provider 也会执行资源
清理；调用方提前关闭 iterator 时同样会触发 `finally`。

## Decode 与 expected target

`shared.lineage.domain.decode_code()` 统一处理 `str`、`bytes`、
`bytearray` 和 `None`，最终 `script_code` 永远是 `str`。默认使用 UTF-8，历史脏
数据无法严格解码时沿用 `errors="ignore"`。空的 `expected_target` 会变成
`None`，不会变成字符串 `"None"`。

Provider 只有在 profile 或 legacy row 明确提供结果表字段时才填写
`expected_target`。没有可靠字段时保持 `None`；不会从文件名、SQL 最后一个表、
所有非 TMP 表或程序名猜测 target。

## source_hash

Provider 统一生成小写十六进制 SHA-256。canonical 输入是固定 JSON（`sort_keys`
和紧凑分隔符固定）中的三个字段：

```json
{"expected_target": null, "program_name": "...", "script_code": "..."}
```

只包含 `program_name`、`script_code` 和 `expected_target`；连接 host、密码、连接
ID、读取时间、batch ID 都不会进入 hash。因此相同语义输入得到相同 hash，代码或
明确的 expected target 改变会得到不同 hash；`bytes` 与等价的 UTF-8 `str` 也会
得到相同 hash。

## PROD adapter

`ProductionProvider` 默认惰性调用现有
`shared.lineage.lineage_builder.load_process_infos()`，把 legacy row 的
`process_name` / `program_name` 和 `script_code` 转换为 `ProgramSource`，并使用
默认 `environment="PROD"`、`source_profile="production_metadata"`。旧
`ProcessInfo` 没有独立 target 字段时，`expected_target` 保持 `None`；需要明确
metadata 字段时可以注入 `expected_target_getter`。

这是 adapter，不是 production metadata 查询重写：没有删除 `ProcessInfo`、没有
复制一套 legacy SQL，也没有修改旧工具入口。旧调用方继续使用原来的 loader；新
调用方可以单独使用 Provider contract。

## 安全与边界

仓库只提交 example 配置，其中使用 `demo_meta`、demo 占位值和占位环境变量名。
真实密码、Token、私钥和连接串不得提交；内网人工执行可将 `connection` 写入被
忽略的 `*.local.yaml`，CI / Docker 应使用 `connection_env` 或外部 secret manager。
Phase 2 不实现 Parser、Physical DAG、Audit、TMP collapse、materialization、
query/viewer、incremental rebuild 或 history/diff。
