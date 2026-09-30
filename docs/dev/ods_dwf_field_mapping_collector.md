# ODS / 上游源系统 → DWF 字段映射 Collector

## Purpose

独立采集 ODS / 上游源系统 → DWF 标准层 ETL 的表、字段映射事实，并通过 DAP Field Mapping Import API 批量 upsert。

## Ownership

- **lakehouse-toolkit**：扫描程序、解析可确认的映射、构造 payload、报告采集与同步结果。
- **DAP**：校验 contract、授权、幂等 upsert、持久化与治理。

程序使用 DAP 当前 `backend/app/contracts/field_mapping.py` 所定义的语义。API 为 `POST /api/field-mappings/import`，要求已认证的签名 session cookie 和 `field_mapping:write` 权限；请求使用 `mode: "upsert"`，响应含 summary 及逐项 `created` / `updated` / `unchanged` / `failed`。API 限制为最多 500 items / request、最多 1000 fields / item。Collector 默认每批 100 张表，最大配置值为 500。

DAP 对表映射采用 `sourceSystemId + sourceTable` 身份，字段采用 `sourceField + targetField` 身份。当前 toolkit 的 program `source_profile` / workspace 元数据不能可靠换算成 DAP 上游系统主键，因此每次运行必须显式配置 DAP 的 `p_upstream_system.system_pk`：`--source-system-id` 或 `PYTOOLS_DAP_SOURCE_SYSTEM_ID`。一次运行应只扫描属于该上游系统的程序目录；多个系统请使用各自隔离的程序目录和 ID 分别执行。缺少 ID、源表无法唯一归属或一张源表写入多个 DWF 目标时会 skip 并报告，不从表名猜系统、不取候选、不模糊匹配。`dataSourceId` 当前不从 toolkit 配置推导，因此不发送。

`.py` 程序 SQL 候选提取复用现有静态 Python/SQL candidate extractor；SQL 表达式通过 SQLGlot AST 解析，不使用旧脚本的 regex / comma-split 字段解析，也不进入 lineage 产物或持久化。`.sql` / `.py` 文件只支持能明确解析的 INSERT…SELECT，目标字段列表须显式给出。仅将 schema/layer 标识为 `DWF` / `DWF_*` 或目标表名以 `DWF_` 开头的 INSERT 视为 DWF 写入；其他命名布局明确不纳入第一版。默认 SQL dialect 为 `mysql`，可通过 `--sql-dialect` / `PYTOOLS_DAP_MAPPING_SQL_DIALECT` 设置。直接字段及重命名分别标记为 `DIRECT` / `RENAME`；单一来源字段的转换表达式记为 `待补充`。CTE、嵌套查询、无法唯一归属的字段表达式（例如多字段 `COALESCE(a, b)` / `CASE WHEN`）明确 skip，不猜测来源。解析后的字段顺序取 INSERT/SELECT 对应位置。

## Non-goals

- DWF→DWM→DWD→DWA→DM 通用 SQL lineage 或 full column lineage。
- 删除、replace-all、truncate 或根据扫描缺失结果失效资产。
- 直接写 DAP 数据库。
- 替换、调用或改造 `jobs/crontab/imp_dws_comments.py`；旧脚本和 cron 保持独立不动。

第一阶段只发送 upsert；API 未收到的旧字段不会被删除。

## Execution

目录和上游系统 ID 没有弱默认值，示例使用占位路径 / ID。认证通过外部注入现有 DAP session cookie；不要把真实 URL / cookie 写入仓库或命令历史。

本地 dry-run 扫描、解析、校验和输出统计，不调用 HTTP API。可选 `--payload-output` 写出 `dryRun=true` 的批次预览，便于检查 / 送 DAP server dry-run：

```sh
export PYTOOLS_DAP_SOURCE_SYSTEM_ID='<DAP_UPSTREAM_SYSTEM_ID>'
python -B jobs/crontab/sync_ods_dwf_field_mappings.py \
  --directory '<ODS_TO_DWF_PROGRAM_WORKSPACE>' \
  --dry-run \
  --payload-output /tmp/ods-dwf-field-mapping-preview.json
```

DAP server dry-run 会实际调用 API，但 DAP 的 `dryRun=true` 只预览、不持久化；它与 local dry-run 不同：

```sh
export DAP_API_BASE_URL='<DAP_BASE_URL>'
export DAP_SESSION_COOKIE='<signed-session-cookie-header>'
python -B jobs/crontab/sync_ods_dwf_field_mappings.py \
  --directory '<ODS_TO_DWF_PROGRAM_WORKSPACE>' \
  --server-dry-run
```

真实 upsert 示例（确认上游 ID、workspace 归属和 server dry-run 结果后再执行）：

```sh
python -B jobs/crontab/sync_ods_dwf_field_mappings.py \
  --directory '<ODS_TO_DWF_PROGRAM_WORKSPACE>' \
  --batch-size 50
```

`--batch-size` 可设为 1..500，默认 100，也可用 `PYTOOLS_DAP_MAPPING_BATCH_SIZE` 覆盖。失败表可复制日志中的 canonical `sourceTable` 精确单表重扫、重试；多表可重复指定参数：

```sh
python -B jobs/crontab/sync_ods_dwf_field_mappings.py \
  --directory '<ODS_TO_DWF_PROGRAM_WORKSPACE>' \
  --source-table 'ODS.CUSTOMER'
```

网络设置：`PYTOOLS_DAP_MAPPING_CONNECT_TIMEOUT`（默认 5 秒）、`PYTOOLS_DAP_MAPPING_READ_TIMEOUT`（默认 30 秒）、`PYTOOLS_DAP_MAPPING_MAX_RETRIES`（默认 2 次 retry，最多 5 次）。仅连接超时、临时连接错误和 HTTP 502/503/504 有界指数退避；400/401/403、contract 错误不重试。DAP item-level failure 会逐表输出 table、error code / message；failed table 可单独重跑，无自动无限重试。

日志输出 scanned/candidate/parsed/skipped/batch 统计及每张 failed table；不输出 API cookie。`--dry-run` 输出 `dry_run=true`。若没有有效 mapping，会打印原因并以非零状态结束。
