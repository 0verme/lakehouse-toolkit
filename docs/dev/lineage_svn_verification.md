# Production SVN Python 清单与内网验证

## 范围

本工具只读取已经 checkout 好的本地 SVN working copy：

```text
SVN working copy
  -> Python 文件扫描
  -> processing / dwf 路径校验
  -> parent/grandparent primary target 推导
  -> 脱敏 coverage / verification report
```

它不会执行 `svn checkout`、`svn update`，不会访问 `svn://`，不会执行或导入
Python 程序，也不会写入 MySQL、SQLite、`lineage_edge` 或 materialization。
本轮没有修改 `ProductionProvider`，也没有把 SVN 接入 lineage materialization。

## 现有 SVN / Production 审计

### ProductionProvider

当前 `shared.lineage.providers.ProductionProvider` 不是 SVN provider。它默认惰性调用
`_default_legacy_process_loader()`，再调用
`shared.lineage.lineage_builder.load_process_infos()`；后者执行
`PROCESS_SQL`，通过 `select_mysql_sql()` 使用 legacy MySQL 连接读取
`process_name` 和 `script_code`。因此：

- 当前 ProductionProvider 的数据来源是 legacy production metadata / MySQL
  process registry adapter；
- 它不扫描本地 SVN working copy；
- 它没有读取 SVN URL，也没有从本地 Python parent directory 推导 target。

### 旧 SVN 审计工具

- `apps/svn_check/services/svn_service.svn_main()` 连接 SVN，做 branch/trunk
  diff，然后 `svn export` 到本地 export 目录；它不是本地 working-copy inventory
  scanner。
- `load_svn_workspace()` 消费 export 后的本地绝对路径。
- `load_local_workspace()` 使用 `os.walk()` 递归读取调用方给的本地目录，排除
  `.git`、`.svn`、`__pycache__`、`.idea`、`.vscode`，但本身不验证生产程序布局。
- `is_dws_py()` 仍使用 `**/WORKSPACE/{DWM,DWA,DM,DWP,DWE,DWD}/*.py` 这类
  `fnmatch` 规则；它没有要求 `DIDP_PROJECT_WORKSPACE`、`1.0`、`DWS_<LAYER>`
  和 table 目录，所以不能覆盖本轮确认的更深目录结构。旧规则只能作为历史证据。
- `match_any()` 先把 `Path(path)` 转成 POSIX 形式，再调用 `fnmatch.fnmatch()`。
- 旧流程中的 `py_url` 实际上是导出或本地发现的文件系统路径（通常是本地
  absolute path），不是 SVN URL，也不是目录路径；`programs.file_path` 是另一
  个 metadata path value。旧代码没有把 `py_url` 当 SVN 网络地址使用。
- 旧的 `get_program_table_name()` 和
  `re_service._table_name_from_program_path_value()` 会从 parent folder 的点号
  直接拆分并删除 `DWS_`，没有同时验证 grandparent，因此不是本 verifier 的安全
  parser。它们是已有的 path -> result table 历史 helper，但不能作为新扫描的权威
  规则。

## 新模块与配置

实现位于：

- `shared/lineage/svn_inventory.py`
- `tools/lineage/verify_svn_sources.py`

完整主配置模板 `configs/lineage_providers.example.yaml` 已包含虚构的
`svn_profiles`。内网执行时直接复制为被忽略的
`configs/lineage_providers.local.yaml`，然后修改 MySQL connection 和
`svn_profiles.root_path`，无需从两个 example 手工拼接：

```yaml
production:
  environment: PROD
  source_profile: production_metadata

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

processing 与 dwf 可以复用同一个 working copy 根目录。`root_path` 必须是当前
运行 verifier 的机器可访问的本地目录，不是 `svn://` 或 HTTP SVN URL。Windows
按实际情况填写类似 `E:/svn/production`；Linux 填写类似
`/data/svn/production`。代码使用 `pathlib`，纯路径 parser 同时接受 Windows 和
Linux 形式。

`configs/svn_inventory.example.yaml` 保留为 SVN 专项最小示例，便于单独查看
`svn_profiles` 字段，但不是配置 local.yaml 的必需步骤。

## SVN layouts

### Processing

只匹配以下完整结构，文件必须直接位于 table directory 下：

```text
DIDP_PROJECT_WORKSPACE/
  <LAYER>/1.0/
    DWS_<LAYER>/
      DWS_<LAYER>.<TABLE>/*.py
```

支持的 processing layers 包括 `DWM`、`DWA`、`DM`、`DWP`、`DWUPRR`、`DWD`；
由于旧 `is_dws_py()` 明确包含 `DWE`，新 scanner 也保留 `DWE` 兼容统计。报告会
按 layer 聚合，不输出真实 table 名称。

### DWF

DWF 使用独立 discovery path：

```text
DIDP_PROJECT_WORKSPACE/
  DW_PROJECT/1.0/
    DWS_DWF/
      DWS_DWF.<TABLE>/*.py
```

DWF profile 与 processing profile 分开扫描、分开统计。DWF 特殊的是 discovery
path；primary target 的 normalization 仍然相同。

## Primary target 规则

`derive_primary_target_from_program_path(path)` 只在以下条件同时成立时返回结果：

1. 文件后缀是 `.py`；
2. parent 正好匹配 `DWS_<LAYER>.<TABLE>`；
3. grandparent 正好是 `DWS_<LAYER>`；
4. parent 的 schema prefix 与 grandparent 一致。

例如：

```text
DWS_DWM/ DWS_DWM.RESULT_A/ program.py -> DWM.RESULT_A
DWS_DWP/ DWS_DWP.RESULT_B/ program.py -> DWP.RESULT_B
DWS_DWF/ DWS_DWF.RESULT_C/ program.py -> DWF.RESULT_C
```

只删除 schema 上的 `DWS_`；table 名中的 `DWS_` 不会被删除。文件名仅保存为
inventory locator，不能覆盖目录推导，也不读取 SQL 猜 target。parent/grandparent
不一致、目录格式错误或布局不支持时返回 `None` / unresolved；不会相似度猜测。

## 读取与报告安全边界

匹配的 Python 文件使用 `tokenize.open()` 读取，因此遵循 Python coding cookie，
而不是假设全部为 UTF-8。单个文件的 `PermissionError` / `OSError` 计为
`READ_ERROR`，编码声明或解码失败计为 `DECODE_ERROR`，扫描继续进行。

JSON report 只包含 profile alias、environment、layout、状态、计数、耗时、layer
计数、原因计数和脱敏 sample shape，例如：

```json
{
  "layout": "processing",
  "layer": "DWM",
  "directory_pattern_valid": true,
  "primary_target_resolved": true
}
```

默认不会写入 root path、absolute path、relative path、filename、table name、
SVN URL、script code 或 SQL。报告指标包括：

- `scanned_python_files`
- `matched_program_files`
- `unmatched_python_files`
- `primary_target_resolved`
- `primary_target_unresolved`
- `primary_resolved_rate`
- `readable_files`、`read_errors`、`decode_errors`
- `layer_counts`
- `unresolved_reasons`

## Verification CLI

Sample（每个 profile 最多扫描 20 个 Python 文件）：

```bash
python -m tools.lineage.verify_svn_sources \
  --config configs/lineage_providers.local.yaml \
  --sample-only \
  --sample-limit 20
```

Windows PowerShell：

```powershell
python -m tools.lineage.verify_svn_sources `
  --config configs/lineage_providers.local.yaml `
  --sample-only `
  --sample-limit 20
```

Full scan：

```bash
python -m tools.lineage.verify_svn_sources \
  --config configs/lineage_providers.local.yaml \
  --output artifacts/lineage_verification/svn_report.json
```

每个 profile 会立即输出 `stage=svn_scan`，大扫描每 500 个 Python 文件输出一次
`stage=svn_scan_progress`。错误状态会区分：

- `CONFIG_ERROR`
- `ROOT_NOT_FOUND`
- `ROOT_NOT_DIRECTORY`
- `NO_MATCHED_FILES`
- `PATH_LAYOUT_ERROR`
- `READ_ERROR`
- `DECODE_ERROR`

## Windows 内网执行顺序

### STEP 1：确认 root

```powershell
$root = "E:/svn/xxx"
Test-Path -LiteralPath $root
Get-Item -LiteralPath $root | Select-Object FullName, Attributes
```

预期 `Test-Path` 为 `True`，并且 `Attributes` 表明它是目录。不要把真实 root
写入公开 example 或提交到 Git。

### STEP 2：sample scan

先在 `configs/lineage_providers.local.yaml` 填入上面的两个 profile，然后运行：

```powershell
python -m tools.lineage.verify_svn_sources `
  --config configs/lineage_providers.local.yaml `
  --sample-only `
  --sample-limit 20 `
  --output artifacts/lineage_verification/svn_sample_report.json
```

### STEP 3：确认两个 profile

检查 console 中分别出现：

```text
profile=prod_svn_processing ... status=SUCCESS
profile=prod_svn_dwf ... status=SUCCESS
```

如果失败，按输出的 `status` 处理，不要只看 `FAILED`：配置问题是
`CONFIG_ERROR`；根目录问题是 `ROOT_NOT_FOUND` / `ROOT_NOT_DIRECTORY`；路径没有
命中是 `NO_MATCHED_FILES` 或 `PATH_LAYOUT_ERROR`；文件读取问题是
`READ_ERROR` / `DECODE_ERROR`。

### STEP 4：确认 primary resolved rate

在脱敏 JSON 中分别检查两个 profile 的：

```text
matched_program_files
primary_target_resolved
primary_target_unresolved
primary_resolved_rate
readable_files
read_errors / decode_errors
```

同时确认 processing 的 `layer_counts` 中 `DWM`、`DWP` 等层有预期计数，DWF
profile 的 `layer_counts` 使用 `DWF` 单独统计。

### STEP 5：full scan

sample 通过后再执行 full scan，并保留本地被忽略的 JSON：

```powershell
python -m tools.lineage.verify_svn_sources `
  --config configs/lineage_providers.local.yaml `
  --output artifacts/lineage_verification/svn_report.json
```

本轮 full scan 仍然只生成 inventory / verification report；不要运行
`imp_lineage_edge`，也不要把结果写入 lineage materialization。
