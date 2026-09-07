# Lineage Productionization：内网数据源核验脚手架

## Purpose

Lineage Materialization V1 和 Legacy Source Inventory 已完成。本工具的目标不是在公网猜测真实内网结构，而是提供一个可重复、可审计、默认脱敏的核验 harness：

```text
MySQLProcessProvider / ProductionProvider
                    ↓
          verification harness
                    ↓
       sanitized JSON / Markdown report
```

它只负责回答“当前配置和真实 driver 是否可工作”，不改变 `ProgramSource`、Parser、Physical DAG、Audit、Materialization、Query、History、Diff 或 Viewer 的语义。

SVN 继续保持 development/audit source（Role B）。本次没有新增 `SVNProgramSourceProvider`，也没有修改 `svn_service` 或 `svn_check`。

## Threat Model

默认假设数据库、SVN、脚本正文和 production metadata 都是敏感信息。报告和日志只允许包含：

- logical profile alias，例如 `dev_a`；
- environment、状态枚举、计数、类型标签、长度和耗时；
- target evidence 的分类和比较计数；
- duplicate identity 的不可逆短 hash。

报告不会包含：

- password、Token、连接串或用户名；
- host、IP、port、database、schema、table、column；
- SQL 文本；
- `script_code` 正文；
- `program_name`、真实 target 名称；
- SVN URL；
- 异常对象的 `repr` 或原始错误文本。

异常只会被归一化为 `CONFIG_ERROR`、`CONNECT_ERROR`、`QUERY_ERROR`、`ROW_MAPPING_ERROR` 或 `DECODE_ERROR` 等安全错误分类。

## Installation

在仓库根目录安装运行依赖（包括 YAML 配置解析所需的 `PyYAML`）：

```bash
python -m pip install -r requirements.txt
```

建议使用执行命令对应的同一个 Python 环境安装依赖；不要只在其他 virtualenv
中安装。缺少 `PyYAML` 时 CLI 会输出 `stage=dependency`，不会把依赖问题误报为
配置错误。

## Local Config

公开模板为 [`configs/lineage_providers.example.yaml`](../../configs/lineage_providers.example.yaml)。内网人工执行前复制为：

```text
configs/lineage_providers.local.yaml
```

`*.local.yaml` 已在 `.gitignore` 中。内网 Windows 人工执行可在 local config
中直接填写 `connection`，减少多套 DEV 连接所需的环境变量；共享配置、CI / Docker
则应使用 `connection_env` 或 legacy 顶层 `*_env`，由运行环境注入真实值。

每个 profile 可配置：

- `name`、`environment`；
- 三选一的 `connection`、`connection_env`，或 legacy 的
  `host_env`、`port_env`、`user_env`、`password_env`、`database_env`；
- `table`、`program_name_column`、`script_code_column`；
- 可选 `expected_target_column`；
- `batch_size`。

`connection` 中的直接值只能保存在被忽略的本地配置，不得提交真实密码、Token、
私钥或连接串；缺少必填连接字段时命令会显式失败，不会使用弱默认值。

缺少显式 `--config`、文件不存在或 profile 配置不合法时，命令会显式失败，不会 fallback 到真实默认地址或公开 demo 数据库。

## Verification Command

默认 JSON 会写入被 Git 忽略的 `artifacts/lineage_verification/report.json`：

```bash
python -m tools.lineage.verify_sources \
  --config configs/lineage_providers.local.yaml \
  --output artifacts/lineage_verification/report.json
```

可选参数：

```bash
# 只读 bounded sample；结果必须为 PARTIAL，不能用于 DELETED 判断
python -m tools.lineage.verify_sources \
  --config configs/lineage_providers.local.yaml \
  --sample-only \
  --sample-limit 20

# 额外显式调用 legacy production metadata loader
python -m tools.lineage.verify_sources \
  --config configs/lineage_providers.local.yaml \
  --include-production

# 额外生成脱敏 Markdown 摘要
python -m tools.lineage.verify_sources \
  --config configs/lineage_providers.local.yaml \
  --markdown-output artifacts/lineage_verification/report.md
```

`--include-production` 是唯一允许命令尝试真实 legacy production loader 的开关。不提供时 `ProductionProvider` 为 `NOT_RUN`，命令不会连接 production metadata。

退出码：

- `0`：所有执行的 full snapshot 成功；
- `1`：连接/查询/映射/解码失败，或使用了 `--sample-only`；
- `2`：配置或报告路径错误。

命令日志只输出 `profile`、`environment`、stage、状态、行数和耗时，不输出 settings、raw row、SQL 或脚本内容。

## Startup Diagnostics

启动或配置错误统一返回 exit code `2`，并使用安全的错误分类：

- `stage=dependency ... dependency=PyYAML`：当前 Python 环境缺少 PyYAML；运行
  `python -m pip install -r requirements.txt` 后重试；
- `error=CONFIG_NOT_FOUND`：`--config` 文件不存在；
- `error=CONFIG_READ_ERROR`：文件无法读取或不是普通文件；
- `error=YAML_INVALID line=<n> column=<n>`：YAML 语法错误；只显示位置，不显示原始 YAML 行；
- `error=CONFIG_INVALID reason=...`：根节点、profile 列表或字段结构不合法；reason 只显示安全的字段/结构信息；
- `error=UNEXPECTED exception=<ExceptionClass>`：未预期的启动异常；默认不输出异常对象内容。

诊断日志不会打印密码、`password_env` 对应的环境变量值、完整连接信息、完整 YAML
或异常原文。不要为了获取更多信息直接上传 local YAML 或 raw log。

## Report Schema

JSON 顶层包含：

```json
{
  "generated_at": "2026-09-05T00:00:00+00:00",
  "duplicate_identity_count": 0,
  "duplicate_identity_samples": [],
  "profiles": [],
  "production_provider": {}
}
```

每个 profile 的公开字段包括：

- `profile`、`environment`；
- `connection_status`、`query_status`、`error_code`、`error_message_safe`；
- `schema_checks`：只表示配置查询是否证明 table/column/row shape 可用，不输出标识符；
- `row_count`、`sample_count`；
- `script_type_counts`，例如 `str`、`bytes`、`bytearray`、`memoryview`、`driver_specific_object`、`None`、`other`；
- `script_null_count`、`script_decode_successes`、`script_decode_failures`、`script_thin_adapter_required`；
- `script_length_min/max` 和 `script_raw_length_min/max`；
- `explicit_target_available`、`explicit_target_present_count`、`explicit_target_null_count`；
- `target_match_count`、`target_conflict_count`、`derived_only_count`、`target_missing_count`、`target_unknown_count`；
- `duplicate_identity_count` 和最多 20 个不可逆短 hash；
- `snapshot_status`。

`ProductionProvider` 诊断字段为：

- `import_status`；
- `legacy_loader_callable`；
- `loader_invocation_attempted`；
- `rows_readable`；
- `row_count`、`sample_count`、`snapshot_status`、`error_code`。

## Script / CLOB Runtime Shape

harness 通过现有 `MySQLProcessProvider` 的 `fetchmany` 边界观察 raw value，但不会保存或输出正文：

- `str`：统计字符长度；
- `bytes`、`bytearray`、`memoryview`：统计 raw length，并进行严格 UTF-8 decode 诊断；
- 提供 `read()` 的 driver object：只做 thin unwrap，统计为 `driver_specific_object`；
- `None`：计入 `script_null_count`；
- 未知对象或 decode 异常：计入 `script_decode_failures`，必要时计入 `script_thin_adapter_required`，报告 `DECODE_ERROR`，不能成为 `COMPLETE`。

driver-specific object 的 wrapper 只在 verification 边界使用，不改变正式 `ProgramSource` semantics；没有安全 unwrap 能力时只报告失败，不猜测对象内容。

## Target Evidence

`shared.lineage.verification.TargetEvidenceProbe` 通过 dependency injection 接收两个可选回调：

- `metadata_join`：历史 metadata join 的诊断结果；
- `derived`：历史 naming/path inference 的对照结果。

回调只接收 `environment`、`source_profile`、`program_name` identity，不接收 `script_code`。回调不得记录或输出真实 identity/target。

每行只输出分类和比较计数，不输出 target 值：

```text
EXPLICIT_FIELD
METADATA_JOIN
DERIVED_ONLY
MISSING
CONFLICT
UNKNOWN
```

多个来源还会有 `MATCH`、`SINGLE_SOURCE`、`CONFLICT`、`MISSING`、`UNKNOWN` 比较结果。如果 explicit field 与 metadata join 不一致，结果为 `CONFLICT`；不会自动覆盖任一来源。本 Issue 不决定最终 target authority，也不会把 derived target 升级为 `ProgramSource.expected_target` contract。

复杂历史 SQL 不应塞回 `MySQLProcessProvider`；应通过独立 adapter/injectable callable 接入。若当前证据不足，返回 `UNKNOWN` 或记录 `THIN_ADAPTER_REQUIRED`，不要猜测。

## Snapshot Status

`COMPLETE` 只在以下条件同时满足时出现：

- full streaming scan 已结束；
- connection 和 query 成功；
- 所有 required rows 可映射；
- 没有 decode failure；
- 没有 mid-stream failure。

含义：

- `COMPLETE`：可以作为后续人工判断完整性的输入；
- `PARTIAL`：只执行 bounded sample，不能用于 `DELETED` 判断；
- `FAILED`：连接、查询、映射、解码或中途失败，绝不能当作完整 snapshot；
- `NOT_RUN`：该检查没有执行，例如默认不启用 production loader。

profile 失败时，报告绝不会输出 `snapshot_status=COMPLETE`。

## How To Run Intranet

人工在批准的内网环境执行：

1. checkout approved commit；
2. 创建本地、未跟踪的 `configs/lineage_providers.local.yaml`；
3. 在进程环境中设置各 profile 的 credentials；
4. 运行 verification command；
5. 检查 JSON/Markdown 中的状态、计数、CLOB type、target coverage 和 snapshot status；
6. 确认报告不含敏感值；
7. 只复制 sanitized report 到公网或 issue/PR 讨论。

不要把 local config、`.env` 或 raw logs 作为诊断附件。

## What Must Never Be Shared

明确禁止导出：

- local YAML；
- `.env`；
- raw logs；
- raw `script_code`；
- SQL dump；
- real program names；
- real target names；
- SVN URL；
- DB host/database/schema；
- credentials、Token、私钥或完整连接串。

## Safe Outputs To Bring Back

内网带回的结果只应类似：

```text
DEV_A
rows=...
script_types={str: ..., bytes: ..., driver_specific_object: ...}
decode_failures=...
target_explicit=yes/no
target_conflicts=...
snapshot=COMPLETE/PARTIAL/FAILED

PROD loader:
PASS / FAIL
```

如需重复 identity 定位，只带回报告中的短 hash，不带真实 `program_name`。

## Boundaries and Follow-ups

本次不做：

- 真实内网接入；
- production 默认入口切换；
- Parser、DAG、Audit、Query、Incremental、History、Diff 或 Viewer 修改；
- production migration；
- SVN Provider；
- 最终 target authority 决策。

上一轮发现的 SVN credential argv/runtime URL logging 仍属于独立 `SECURITY_REVIEW_RECOMMENDED`，本次不顺手重构 SVN。若 verification 结果显示真实 driver 需要专用 CLOB adapter，应另行评审；本 harness 只保留 thin adapter 或 `THIN_ADAPTER_REQUIRED` 诊断。
