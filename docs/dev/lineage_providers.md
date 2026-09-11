# Lineage Phase 2：ProgramSource Providers

Phase 2 把不同 metadata 来源统一成 Phase 1 冻结的 `ProgramSource`。Provider 只
负责“获取程序”，不负责解释程序内容：

```text
MySQL / legacy metadata       local SVN working copy
          ↓                              ↓
       Provider ← validated inventory ───┘
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

## Production SVN profiles

`SVNProgramSourceProvider`（别名 `ProductionSVNProvider`）只消费已经 checkout
的 local working copy。它不执行 `svn checkout`、`svn update`，不访问 SVN URL，也
不读取 credentials。`load_program_source_providers()` 会在保留 MySQL profiles 的
同时加载 `svn_profiles`；旧的 `ProductionProvider` 仍是独立的 legacy metadata
adapter，不会被替换成 SVN backend。

公开配置保留两个独立的 production profile，即使它们共享同一个 local root：

```yaml
svn_profiles:
  - name: prod_svn_processing
    environment: PROD
    root_path: "E:/demo/svn/production"
    layout: processing
  - name: prod_svn_dwf
    environment: PROD
    root_path: "E:/demo/svn/production"
    layout: dwf
```

inventory 先按 `processing` / `dwf` 的 directory contract 验证
`matched_program_file` 和 `declared_primary_target`。Provider 只读取 matched 文件，
复用 `read_python_source()` 的 coding-cookie / decode 规则，并将
`declared_primary_target` 原样作为 `ProgramSource.expected_target`；不从文件名、
SQL 最后一个写入或 fuzzy match 猜 target。源码不会写回 inventory report。

SVN 的兼容 `program_name` 是 `SVN/<case-folded repository-relative locator>`，
locator 同时包含 directory layout 和 filename，并统一为 `/` 分隔符。它不包含
absolute root、drive letter 或 SVN URL，因此不同机器的同一相对文件得到相同
`ProgramIdentity` 和 `source_hash`。不同相对文件如果因规范化发生 collision，
provider 会拒绝这些文件并记录 `IDENTITY_COLLISION`，不会覆盖。

每次 provider 迭代都保留安全 accounting：`OUT_OF_SCOPE` 只计数、不进入
`ProgramSource`；malformed candidate、read/decode failure 和 identity collision
会记录固定 reason 与 anonymous `program_id`，不输出 path、filename、program_name、
SQL 或源码。只要 profile 不是成功完整扫描，job 就把 snapshot 降级为 partial，
不会获得 disappearance / `DELETED` authority。

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

### Unified lineage deployment config

`configs/lineage_providers.local.yaml` 是唯一的 lineage deployment config，同时承载
`mysql_process_profiles`、`svn_profiles`、`production` 和可选的 `scopes`。公开模板
`configs/lineage_providers.example.yaml` 保持同样的根结构。`scopes` 供
reconciliation Web、lineage daily job 和 `LineageEnvironmentScopeResolver` 使用，例如：

```yaml
scopes:
  - name: dev
    environment: DEV
    sql_source_profile: mysql_dev_a
    schedule_source_profile: mysql_dev_a
    dws_profile: czcb
    label: DEV
    enabled: true
```

`dws_profile` 仍引用 `configs/database.local.yaml`（或公开 database example）中的
数据库 profile，不把 JDBC 配置放入 lineage provider config。旧 provider 配置没有
`scopes` 时仍可用于 ingestion、verification 和单任务 materialization；reconciliation
scope resolver 与 lineage daily job 会对缺失或非法的 `scopes` 返回配置错误。不要创建
独立的 `configs/lineage_scopes.local.yaml` 或其它 daily 专用配置文件。

`table`、`program_name_column`、`script_code_column` 和可选的
`expected_target_column` 都会通过 `shared.config.env.safe_identifier` 校验后才
进入查询模板。运行时数据仍由 cursor 返回，不把用户值拼接进 SQL。

`primary_target_strategy` 作为兼容字段保留，但不再配置 program-name prefix。
lineage target authority 固定为 explicit/provider target 优先，缺失时仅按严格四段的
canonical `005:<logical_target>:<step_seq>:<opaque_suffix>` grammar 使用第二段
logical target。三段及其它非 canonical legacy name 保持 unknown。不提供 multi-prefix
abstraction；`005` 是唯一合法 legacy marker。

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

Provider 优先使用 profile/legacy row 明确提供的结果表字段；缺失时仅按
`parse_program_name()` 的 conservative 规则恢复 canonical 四段的第二段
logical target。例如 `005:DEMO_DWM.RESULT_A:1:ABCD` 得到 `DEMO_DWM.RESULT_A`，suffix
`ABCD` 只产生 informational diagnostic，不会阻断 lineage。

`005:DEMO_DWM.RESULT_A` 与 `005:DEMO_DWM.RESULT_A:00` 都是非 canonical 的
ambiguous shape，不恢复 target 或 step。target 无法安全识别时返回 `None`，不猜测
其它字段。该值只是 `expected_target` 的 declared logical target hint；它不会替换
Physical DAG 中的其它 formal sink，也不会把多个 sink 变成唯一结果。完整字段与
grouping 语义见 [`lineage_program_name.md`](lineage_program_name.md)。

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
`ProcessInfo` 没有独立 target 字段时，Provider 仍会按固定 `005` grammar 尝试恢复
canonical logical target；三段及其它非 canonical name 保持 unknown。需要明确 metadata
字段时可以注入 `expected_target_getter`，且 explicit/provider 值优先。
`program_name_target_prefix` 不再支持，避免引入 multi-prefix abstraction。

这是 adapter，不是 production metadata 查询重写：没有删除 `ProcessInfo`、没有
复制一套 legacy SQL，也没有修改旧工具入口。旧调用方继续使用原来的 loader；新
调用方可以单独使用 Provider contract。每个 raw `ProgramSource` 仍保留独立
Program Step provenance，logical grouping 不会合并原始 source。

## 内网验收命令

以下命令只适用于已配置 local root 和凭据的内网机器；本地开发环境不要伪造
full replay 结果。`--limit` 始终是 partial replay；不带 `--limit` 的单 profile
命令只有在 provider 完整成功时才允许该 profile scope 内的 disappearance 判断。

```bash
# processing sample
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_processing --limit 20 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_processing --limit 100 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_processing --limit 500 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_processing --force-rebuild

# DWF sample
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_dwf --limit 20 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_dwf --limit 100 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_dwf --limit 500 --force-rebuild
"C:\Users\czcb.CZCB-20220214FO\pywebio\Scripts\python.exe" -m jobs.crontab.imp_lineage_edge --profile prod_svn_dwf --force-rebuild
```

最后两条是 profile full 命令，不代表本仓库已经执行过真实内网扫描。验收时应
核对脱敏 accounting，目标规模仅作外部 evidence 对照：processing `3851`、DWF
`4155`、合计 `8006`。

## 安全与边界

仓库只提交 example 配置，其中使用 `demo_meta`、demo 占位值和占位环境变量名。
真实密码、Token、私钥和连接串不得提交；内网人工执行可将 `connection` 写入被
忽略的 `*.local.yaml`，CI / Docker 应使用 `connection_env` 或外部 secret manager。
Phase 2 不实现 SQL/AST extraction、Physical DAG、Audit、TMP collapse 或另一套
materialization；SVN provider 只把 source 接入现有 pipeline，后续仍复用既有
Physical DAG、Audit、incremental rebuild、materialization、query/viewer 和
history/diff。本页的 program-name target recovery 只属于 metadata semantic
boundary，不替代 SQL parser。
