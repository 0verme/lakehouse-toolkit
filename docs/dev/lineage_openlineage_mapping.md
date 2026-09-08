# OpenLineage Dataset / Job 静态映射兼容性评估（Issue #46）

**研究结论：`DOCUMENT_ONLY`**

本研究确认：当前 `DatasetIdentity` / `ProgramIdentity` 可以在不改变
`lakehouse-toolkit` 主 domain 的前提下，稳定投影为 OpenLineage `Dataset` / `Job`，
并且当前 OpenLineage schema 已提供不带 `Run` 的 `JobEvent` / `DatasetEvent` 静态事件。

但本 Issue 不实现 exporter、HTTP client、runtime emitter 或 RunEvent producer。仓库当前
没有指定的 OpenLineage consumer、scheduler runtime contract 或生产导出需求；此外，消费端
对静态事件、rename 生命周期和 custom facet 的支持仍需针对目标产品做 acceptance test。因此
本次只冻结 mapping boundary 和未来兼容层形状，保留实现为独立 Issue。

## Scope

本文件只回答两个问题：

1. 现有 identity 是否能稳定映射为 OpenLineage `Dataset` / `Job`；
2. 未来需要静态 lineage 导出时，最小兼容层应如何表达，以及哪些信息不能表达。

明确不做：

- 不接入 runtime OpenLineage；
- 不修改 `DatasetIdentity`、`ProgramIdentity`、`ProgramState`、`LineageEdge`、
  `ProgramSource`、`ProgramPhysicalDAG` 或 `MaterializationBatch`；
- 不把 `MaterializationBatch` 伪装成 `Run`；
- 不新增 `openlineage-python` production dependency；
- 不连接 Marquez、DataHub、OpenMetadata 或任何 HTTP/Kafka endpoint；
- 不导出 SQL、Python source、真实 table/schema、credentials、database URL、SVN path
  或 local path。

## 当前 domain contract

研究以 `origin/main` 当前实现为准，而不是 Issue #46 创建时的旧假设。

### DatasetIdentity（Issue #37）

```text
DatasetIdentity.key = (environment, canonical_schema, canonical_table)
```

- `environment` 是 hard graph boundary，保留 surrounding whitespace 以外的原有大小写；
- `schema` / `table` 在 identity boundary 做 trim + upper，并组成明确的
  `canonical_schema.canonical_table`；
- `source_profile` 是 collection provenance / filter，不是 Dataset identity；
- 没有 platform、catalog、database、instance 等额外 namespace；
- 缺 schema、多于两段的 `catalog.schema.table` 和 TMP 均不构成正式
  `DatasetIdentity`。

### ProgramIdentity（Issue #38）

```text
ProgramIdentity.key = (environment, source_profile, program_name)
```

三个字段只 trim surrounding whitespace，并保留大小写。

- `environment` 和 `source_profile` 都是 Program identity scope；
- `program_name` 是 static program name，不是 scheduler runtime id；
- `source_hash`、`pipeline_version`、`batch_id`、`job_key` 不属于 identity；
- rename 不做 alias inference：旧 identity 可以在 complete snapshot 中 `DELETED`，
  新 identity 为 `NEW`；
- `ProgramState` 的时间字段、`batch_id`、active flag 是观察和 materialization history，
  不是执行历史。

### 静态分析边界

当前系统证明的是：

```text
source code observed at time T
static DAG / audit / materialization facts
```

当前系统没有证明：

```text
job started / completed / failed
runtime duration
execution success or failure
scheduler run id
actual execution timestamp
```

`Program ≈ static Job definition`，但 `MaterializationBatch != OpenLineage Run`。

## OpenLineage 规范基线

本研究核对了 OpenLineage 当前公开 schema 和 static lineage proposal：

- [OpenLineage JSON schema 2-0-2](https://openlineage.io/spec/2-0-2/OpenLineage.json)
- [OpenLineage spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md)
- [Object Model](https://openlineage.io/docs/spec/object-model)
- [Static Lineage proposal](https://github.com/OpenLineage/OpenLineage/blob/main/proposals/1837/static_lineage.md)

### 三个核心对象必须分开

| OpenLineage 对象 | 语义 | 本仓库的关系 |
| --- | --- | --- |
| `Dataset` | namespace 内唯一的数据实体 | 可由 `DatasetIdentity` 投影 |
| `Job` | 消费/生产 Dataset 的 process definition | 可由 `ProgramIdentity` 投影 |
| `Run` | 某个 Job 的一次 execution instance，必须有 client-generated `runId` | 当前 domain 没有对应对象 |

当前 schema 的 `RunEvent` 是 run lifecycle event，包含 `run`、`job`、可选的
`inputs` / `outputs`，并由 `eventType` 表达 `START`、`COMPLETE`、`FAIL` 等状态。

当前 schema 同时定义了静态事件：

- `JobEvent`：需要 `job`，可带 `inputs` / `outputs`，明确不需要 `run`；
- `DatasetEvent`：需要 `dataset`，用于独立发布静态 Dataset metadata，不需要 `job` 或
  `run`；
- 两类静态事件仍需要 `BaseEvent` 的 `eventTime`、`producer` 和 `schemaURL`。

所以“OpenLineage 围绕 RunEvent”准确描述了最常见的 runtime ingestion path，但不应
忽略当前规范已经为 static metadata 提供的 `JobEvent` / `DatasetEvent`。本仓库可以
使用静态事件，不需要 fake `RunEvent`。

## DatasetIdentity → OpenLineage Dataset

### 候选 namespace 策略

| 候选 | `Dataset.namespace` | `Dataset.name` | 评价 |
| --- | --- | --- | --- |
| A | `environment` | `schema.table` | 相对 #37 是 lossless；但 bare environment 可能与其它 producer 的 namespace 冲突，且未定义 URI serialization。 |
| B | `environment + source_profile` | `schema.table` | 不接受。`source_profile` 不是 Dataset identity；同一 Dataset 从两个 profile 采集会错误地产生两个 OpenLineage Dataset。 |
| C（推荐） | `lakehouse-toolkit://dataset/<encoded-environment>` | `canonical_schema.canonical_table` | 是 #37 tuple 的稳定 transport encoding；只增加 producer-owned namespace 前缀，不增加 domain identity 字段。 |

推荐的兼容层公式为：

```text
Dataset.namespace = lakehouse-toolkit://dataset/<percent-encoded environment>
Dataset.name      = <canonical_schema>.<canonical_table>
```

`<encoded environment>` 必须按 UTF-8、固定的 percent-encoding 规则生成，不做
lowercase、authority normalization 或其它会改变 #37 大小写语义的处理。实现时应使用
明确的 canonical serializer；不能直接拼接未经验证的用户输入。

这相当于候选 A 的 identity 语义加候选 C 的 producer namespace 隔离：

- `DEV / DEMO_DWF.RESULT_A` 和 `PROD / DEMO_DWF.RESULT_A` 必须是两个 Dataset；
- `DEV / profile_a / DEMO_DWF.RESULT_A` 与 `DEV / profile_b / DEMO_DWF.RESULT_A`
  仍然是同一个 Dataset；profile provenance 不能改变 Dataset identity；
- `schema.table` 已是 #37 的 canonical name，不再从 raw SQL 或 source token 猜测
  catalog/database。

### Dataset mapping matrix

| Toolkit value | OpenLineage | 结果 | Confidence | Semantic loss | Risk / recommendation |
| --- | --- | --- | --- | --- | --- |
| `DatasetIdentity.environment` | `Dataset.namespace` 的 encoded segment | `YES` | HIGH | 无；解码可还原 environment | 使用固定 producer-owned URI namespace，保留大小写。 |
| `canonical_schema` + `canonical_table` | `Dataset.name` | `YES` | HIGH | 无；canonical name 直接保留 | 只允许已经通过 #37 校验的两段名称。 |
| `DatasetIdentity.key` | `namespace/name` pair | `YES` | HIGH | 无；`source_profile` 按 #37 有意不在 key 中 | 不要把 tuple 塞入一个未定义的字符串或引入 catalog。 |
| `source_profile` | Dataset id | `NO` | HIGH | 有意不导出为 identity；profile provenance 不是 Dataset identity | 禁止候选 B；若未来确需保留，使用独立 custom provenance，不能改变 Dataset id。 |
| 缺失 schema、额外 namespace、TMP | Dataset | `UNSUPPORTED` | HIGH | 无法安全构成正式 Dataset | 保留现有 unresolved / Physical / audit 事实，不猜 namespace。 |

## ProgramIdentity → OpenLineage Job

### 推荐 mapping

```text
Job.namespace = lakehouse-toolkit://job/<percent-encoded environment>/<percent-encoded source_profile>
Job.name      = program_name
```

这与 #38 中“environment + source_profile 组成 Job namespace、program name 作为
Job name”的 contract boundary 一致。`lakehouse-toolkit://job/` 是 transport prefix，
不是新增的 domain 维度。

不能把动态 identity segment 放入 URI authority，也不能将它们 lowercase；否则
`ProgramIdentity` 当前保留大小写的语义可能在 URI 或 consumer normalization 中碰撞。
`program_name` 必须使用 trim 后的原值，不把 `source_hash`、`pipeline_version` 或
`batch_id` 拼入 Job name。

### Program 变化的语义

| 变化 | `ProgramIdentity` | OpenLineage Job | 结论 |
| --- | --- | --- | --- |
| `DEV / profile_a / DEMO_JOB` → 同值 | 相同 | 同一个 namespace/name | 同一个 static Job，可更新 Job facet。 |
| environment 改变 | 不同 | namespace 不同 | 不得合并。 |
| source profile 改变 | 不同 | namespace 不同 | 不得因 program name 相同而合并。 |
| 仅 `source_hash` 改变 | 相同 | Job identity 不变 | 作为 source/content evidence；不能改 Job name。 |
| 仅 `pipeline_version` 改变 | 相同 | Job identity 不变 | 作为 analyzer/pipeline version evidence；不能当 runtime job version。 |
| 仅 `batch_id` / observation 改变 | 相同 | Job identity 不变 | 是 static observation/materialization history，不是新 Run。 |
| `DEMO_OLD_JOB` → `DEMO_NEW_JOB` | 不同 | 两个 Job identity | 不自动 alias；旧 Job 的删除/retire 需要 consumer-specific lifecycle contract。 |
| `job_key` 改变 | 相同 | Job identity 不变 | `job_key` 当前只是 edge provenance，不能升级为 Job identity。 |

### Job mapping matrix

| Toolkit value | OpenLineage | 结果 | Confidence | Semantic loss | Risk / recommendation |
| --- | --- | --- | --- | --- | --- |
| `ProgramIdentity.environment` + `source_profile` | `Job.namespace` | `YES` | HIGH | 无；两个 scope 维度都保留 | 使用固定 encoding；不要依赖未验证 `job_key`。 |
| `ProgramIdentity.program_name` | `Job.name` | `YES` | HIGH | 无；当前大小写语义保留 | 不加 hash、pipeline version、batch id。 |
| `ProgramIdentity.key` | `Job.namespace/name` pair | `YES` | HIGH | 无（在推荐 namespace contract 内） | 只允许从稳定 identity 构造 Job。 |
| `LineageEdge.program_name is None` | Job | `UNSUPPORTED` | HIGH | 无法推导稳定 Job identity | 不要使用 `job_key` 或 sentinel name 猜测 Job；未来 adapter 应显式报告 skipped fact。 |
| rename 的 old/new 生命周期 | Job identity history | `PARTIAL` | HIGH | OpenLineage static event 没有本仓库的 `DELETED → NEW` snapshot lifecycle | 不实现 alias；需要独立的 consumer cleanup / retirement contract。 |

## source_hash / pipeline_version / observed_at / batch_id

| 字段 | 当前语义 | Identity? | 推荐 OpenLineage 表达 | 分类 |
| --- | --- | --- | --- | --- |
| `source_hash` | Provider 对 program/source/expected target 的 content hash | 否 | 可选的 custom Job facet candidate；不是当前标准 `sourceCodeLocation.version` | `CUSTOM_FACET_CANDIDATE` |
| `pipeline_version` | parser、Physical DAG、audit、TMP collapse/materialization 的 semantic version | 否 | 可选的 custom static-analysis Job facet | `CUSTOM_FACET_CANDIDATE` |
| `observed_at` | 静态 source/fact 被观察或静态 event 被生成的时间 | 否 | `JobEvent` / `DatasetEvent` 的标准 `BaseEvent.eventTime`，仅表示 metadata observation | `STANDARD_EVENT_FIELD`；禁止解释为 execution time |
| `batch_id` | candidate/replay/publish snapshot identity | 否 | 默认 internal-only；如未来确有审计需求，只能放 sanitized custom facet | `SHOULD_NOT_EXPORT` by default |
| `first_seen_at` / `last_seen_at` / `last_changed_at` | static program state history | 否 | 没有当前对应的 runtime facet | `CUSTOM_FACET_CANDIDATE` 或 internal-only |
| `job_key` | edge provenance，且没有 provider-scoped authority | 否 | 不进入 Job identity；默认不导出 | `SHOULD_NOT_EXPORT` |

### `source_hash` 不能直接当标准 Job version

OpenLineage 标准 `sourceCodeLocation` Job facet 的 `version` 语义是 source control
或 deployed source 的实际唯一版本，例如 Git SHA 或 SVN revision，并且该 facet 还
需要 `type`、`url` 等 source location 信息。当前 `source_hash` 是本仓库对静态输入
内容的 SHA-256，不携带 source-control URL，也没有被 contract 定义为 deployed version。

因此：

```text
source_hash != Job.name
source_hash != Run.runId
source_hash != automatically SourceCodeLocationJobFacet.version
```

如果未来 consumer 需要它，建议使用项目命名的 custom Job facet，例如概念上的
`lakehouseToolkit_staticAnalysis`，并让该 facet schema 使用 immutable、versioned
`_schemaURL`。custom facet 必须遵守 OpenLineage 的 prefix 和 schema URL 约束，不应
把 hash 无声地伪装成标准 VCS version。

同理，`pipeline_version` 是 analyzer pipeline 的语义版本，不是被分析程序的
runtime version；只能作为 custom static-analysis evidence。

## Static Lineage 表达方式

### 概念映射

对每个有稳定 `ProgramIdentity` 的 static program，可以把当前 formal edges 分组为：

```text
Job(DEMO_JOB)
  inputs  = {Dataset A, Dataset B}
  outputs = {Dataset C}
```

其中：

- `LineageEdge.source_dataset_identity` → `JobEvent.inputs[]`；
- `LineageEdge.target_dataset_identity` → `JobEvent.outputs[]`；
- 同一 Job 的多个 edge 应按 Dataset `namespace/name` deterministic deduplicate；
- edge 的 `evidence` 不应默认复制进 Dataset 或 Job facet，除非经过 privacy review；
- `program_name is None` 的 edge 没有可安全生成的 Job，必须显式跳过或进入 unsupported
  report，不得用 `job_key` 猜测。

### Protocol-level emission

当前 OpenLineage schema 允许如下最小静态事件形状（仅示意，值为 placeholder 或
`DEMO_*` synthetic identity）：

```json
{
  "eventTime": "<static-observed-at>",
  "producer": "<configured-static-exporter-uri>",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
  "job": {
    "namespace": "lakehouse-toolkit://job/DEV/profile_a",
    "name": "DEMO_JOB"
  },
  "inputs": [
    {
      "namespace": "lakehouse-toolkit://dataset/DEV",
      "name": "DEMO_DWF.RESULT_A"
    }
  ],
  "outputs": [
    {
      "namespace": "lakehouse-toolkit://dataset/DEV",
      "name": "DEMO_DWF.RESULT_B"
    }
  ]
}
```

关键边界：

- 这是 `JobEvent`，不是 `RunEvent`；
- 没有 `eventType`、`run`、`runId`、`START`、`COMPLETE` 或 `FAIL`；
- `eventTime` 是静态 metadata event 的 observation time，不是“Job 在此时执行”；
- `producer` / `schemaURL` 是协议必需字段，未来 adapter 必须显式提供；它们不应从
  当前 domain 猜测；
- `DatasetEvent` 可在未来独立发布 Dataset facet，但不用于虚构一次 transform run。

因此：

```text
Conceptual mapping: YES
Protocol-level static Job/Dataset event: YES
Runtime RunEvent producer: NOT RECOMMENDED
```

OpenLineage static lineage proposal 的目标本身就是 bootstrap 尚未运行的 Jobs、表达
可能的 DAG paths 以及在没有 Run 的情况下更新 Dataset/Job metadata。目标 consumer
仍需要单独验证：Marquez、DataHub、OpenMetadata 的文档和实现路径不应被假设为完全
等价，尤其是静态 JobEvent 的 facet、rename 和 unresolved Dataset 行为。

## Facet mapping matrix

| 信息 | Standard facet / field | 分类 | 处理建议 |
| --- | --- | --- | --- |
| static observation timestamp | `BaseEvent.eventTime` | `STANDARD_FACET` 不准确；它是 `STANDARD_EVENT_FIELD` | 仅用于 `JobEvent` / `DatasetEvent` 的 metadata observation；绝不写成 Run execution time。 |
| source-control URL + actual Git/SVN deployed revision（当前 domain 没有） | `sourceCodeLocation` Job facet | `STANDARD_FACET`（条件成立时） | 当前不生成；没有真实 URL/path/revision 时不得填充。 |
| `source_hash` | 无直接 standard facet | `CUSTOM_FACET_CANDIDATE` | 可放 sanitized custom Job facet；不塞 Job name，不冒充 VCS version。 |
| `pipeline_version` | 无直接 standard facet | `CUSTOM_FACET_CANDIDATE` | 表示 analyzer semantic version，不表示 runtime job version。 |
| parser backend/version | 无直接 standard facet | `CUSTOM_FACET_CANDIDATE` | 只在未来 consumer 明确需要时增加；schema 必须 immutable。 |
| static-analysis confidence / bounded evidence summary | 无直接 standard facet | `CUSTOM_FACET_CANDIDATE` | 只导出 sanitized、无源码/SQL/凭据的 summary；默认不导出原始 evidence。 |
| Dataset column/schema definition（当前仅有 schema name） | `schema` Dataset facet | `NOT_EXPORTABLE` | `canonical_schema` 是 Dataset name 的一部分，不是列 schema；不能伪造字段定义。 |
| Dataset datastore snapshot/version（当前没有） | `version` Dataset facet | `NOT_EXPORTABLE` | `source_hash` / `pipeline_version` 都不是 datastore snapshot ID。 |
| `batch_id` / materialization history | 无对应 standard facet | `SHOULD_NOT_EXPORT` | 默认留在内部 history；永远不能映射为 `Run.runId`。 |
| `nominal time` | Run `nominalTime` facet | `SHOULD_NOT_EXPORT` | 当前没有 scheduler 计划语义；`observed_at` 不能冒充 nominal time。 |
| parent execution | Run `parent` facet | `SHOULD_NOT_EXPORT` | 当前没有 parent Job/Run contract。 |
| runtime status / duration / error | Run event / Run facets | `SHOULD_NOT_EXPORT` | static analysis 不证明执行状态。 |
| Python source / SQL / local path / SVN path / credentials | `sourceCode` 或其它 metadata | `SHOULD_NOT_EXPORT` | 本研究与未来默认 adapter 都不复制敏感原文。 |
| `source_profile` 作为 Dataset identity | 无 | `NOT_EXPORTABLE` | #37 明确它是 Dataset provenance/filter；不要制造跨 profile Dataset duplication。 |

## Mapping matrix 总结

| lakehouse-toolkit | OpenLineage | Mapping | Confidence | Semantic loss | Risk / recommendation |
| --- | --- | --- | --- | --- | --- |
| `DatasetIdentity` | `Dataset` | `YES` | HIGH | 推荐 namespace encoding 下无 identity loss | 采用候选 C；`source_profile` 不进 Dataset id。 |
| Dataset canonical key | `Dataset.namespace/name` | `YES` | HIGH | 无 | `environment` 用固定 encoded namespace，`schema.table` 用 canonical name。 |
| `ProgramIdentity` | `Job` | `YES` | HIGH | identity 可保留；rename lifecycle 不自动保留 | 用 environment/profile namespace + program name；不添加 hash/version。 |
| `source_hash` | Job version/facet | `CUSTOM_FACET_CANDIDATE` | HIGH | 标准 VCS version 语义不适用 | 仅在 custom facet contract 中表达。 |
| `pipeline_version` | Job version/facet | `CUSTOM_FACET_CANDIDATE` | HIGH | 不是 runtime Job version | 标记为 analyzer static evidence。 |
| `MaterializationBatch` | `Run` | **`NO`** | HIGH | 全部 runtime execution semantics 都是伪造 | 不生成 `runId`、state event 或 run facet。 |
| `observed_at` | static event `eventTime` | `YES`（静态限定） | HIGH | 不能表达 execution time | 只在 JobEvent/DatasetEvent 上作为 metadata observation。 |
| `LineageEdge` | JobEvent `inputs/outputs` | `CONCEPTUAL YES` / `PROTOCOL YES` | MEDIUM-HIGH | 需要 stable program；edge evidence 不自动迁移 | 按 ProgramIdentity 分组；缺 program name 的 edge 显式 unsupported。 |
| `LineageIssue` / Audit Issue | Job/Dataset/Run | `NO` | HIGH | OpenLineage core graph 不表达本仓库 audit issue lifecycle | 留在内部 audit API；不要用 custom facet 默认外发。 |
| `job_key` | Job identity | `NO` | HIGH | 当前无权威 identity 语义 | 仅 edge provenance；不作为 fallback。 |

## Round-trip / collision analysis

以下全部是 synthetic identity，不代表真实资产。

### Dataset collision

推荐映射：

| Toolkit identity | OpenLineage namespace | OpenLineage name |
| --- | --- | --- |
| `DEV / DEMO_DWF.RESULT_A` | `lakehouse-toolkit://dataset/DEV` | `DEMO_DWF.RESULT_A` |
| `PROD / DEMO_DWF.RESULT_A` | `lakehouse-toolkit://dataset/PROD` | `DEMO_DWF.RESULT_A` |

两个 pair 不相等，因此不会意外合并。若使用候选 B：

```text
DEV / profile_a / DEMO_DWF.RESULT_A
DEV / profile_b / DEMO_DWF.RESULT_A
```

会产生两个 Dataset identity，这与 #37“profile 是 provenance，不是 Dataset identity”
冲突，因此 B 明确拒绝。

### Program collision

| Toolkit identity | OpenLineage namespace | OpenLineage name |
| --- | --- | --- |
| `DEV / profile_a / DEMO_JOB` | `lakehouse-toolkit://job/DEV/profile_a` | `DEMO_JOB` |
| `DEV / profile_b / DEMO_JOB` | `lakehouse-toolkit://job/DEV/profile_b` | `DEMO_JOB` |

两个 Job pair 不相等，符合 #38。`DEV` / `PROD` 也会因 namespace 不同而隔离。

### Version change

```text
same ProgramIdentity
source_hash:      A → B
pipeline_version: v1 → v2
```

映射后的 Job namespace/name 保持不变。未来可以发布新的 static `JobEvent`，更新
custom static-analysis facet；不能创建新的 Job name，也不能创建 Run。

### Rename

```text
DEV / profile_a / DEMO_OLD_JOB
DEV / profile_a / DEMO_NEW_JOB
```

映射后是两个不同 Job。adapter 不应生成 alias，也不应因 hash 相似而合并。当前
OpenLineage static event contract 没有直接等价于本仓库 complete snapshot 的
`DELETED → NEW` 生命周期；如果 consumer 需要清理 old Job，必须另立 consumer-specific
retirement/cleanup contract。

### Round-trip 边界

- Dataset 的 recommended `namespace/name` 可以由本 adapter 解码回
  `(environment, canonical_schema, canonical_table)`；不能也不应解码出
  `source_profile`，因为它本来不在 #37 identity 中；
- Job 的 recommended `namespace/name` 可以解码回
  `(environment, source_profile, program_name)`；
- 任意没有 adapter namespace prefix、encoding version 或字段完整性的外部
  OpenLineage object，不应反向猜成当前 domain identity；
- round-trip 只证明 identity serialization，不证明 lineage history、runtime
  execution 或 consumer database lifecycle 可逆。

## 最小未来兼容层（不在本 Issue 实现）

若未来出现明确 consumer 和导出需求，最小层应是纯静态 serializer/DTO，而不是 runtime
integration：

1. `to_openlineage_dataset(DatasetIdentity) -> dict`：只生成确定性的 namespace/name；
2. `to_openlineage_job(ProgramIdentity) -> dict`：只生成确定性的 namespace/name；
3. `to_static_job_event(program, edges, observed_at, producer) -> dict`：按 Job 分组
   inputs/outputs，生成 `JobEvent`，不创建 Run；
4. 可选 `to_dataset_event(dataset, facets, observed_at, producer)`：仅当真正需要
   Dataset static facet 时使用；
5. 用 pinned OpenLineage JSON schema 做本地 serialization/contract validation；
6. 对缺失 `program_name`、未解析 Dataset、敏感 evidence 返回显式 unsupported/report，
   不静默 fallback。

实现策略：

- 优先直接生成 JSON DTO，不引入 `openlineage-python` 长期 production dependency；
- 不提供 HTTP client、Kafka producer、Marquez/DataHub/OpenMetadata connector；
- 不提供 scheduler hook、runtime listener、RunEvent state machine；
- future implementation 需要至少有一个明确 consumer 的 acceptance test，覆盖静态
  `JobEvent`、无 Run、inputs/outputs、facet replacement 和 rename/retirement 语义；
- 若 consumer 只能可靠接收 `RunEvent`，本仓库仍不应伪造 Run；应改为记录
  `DEFER` 或选择 consumer 原生静态 metadata API。

## Privacy / security review

本次文档和 synthetic examples 只使用：

```text
DEMO_*
DEV / PROD
profile_a / profile_b
```

没有导出或提交真实 program name、真实 table/schema、SQL、Python source、credentials、
database URL、SVN path 或 local path。

未来 custom facet 需要额外 review：

- source hash 虽不可直接还原 source，但可以作为跨系统 join/fingerprint；
- raw evidence、error message、parser trace 可能包含 SQL、路径、连接信息；
- `sourceCode` Job facet 会直接复制程序内容，本项目默认禁止；
- `sourceCodeLocation` 需要 URL/path，当前 provider contract 没有安全、公开且允许
  外发的 source location；
- dataset names 本身可能是业务敏感 metadata，生产导出必须由目标环境的 allowlist/
  privacy policy 授权，不能把本研究 synthetic contract 当作授权。

## Research decision

```text
Protocol mapping: FEASIBLE
Repository decision: DOCUMENT_ONLY
```

理由：

1. Dataset / Job identity mapping 清晰，且可以保留 #37 / #38 的 hard boundary；
2. 当前 OpenLineage schema 已支持无 Run 的 `JobEvent` / `DatasetEvent`，无需伪造
   runtime event；
3. static JobEvent 对 bootstrap 未运行 Job、静态 DAG 和 prospective lineage 有明确
   价值；
4. 但当前仓库没有指定 consumer 或生产 exporter 需求，JobEvent 的跨产品 lifecycle
   行为尚未由本项目 acceptance test 证明；
5. 因此先保留这份 mapping 文档，不引入 SDK、生产依赖或 exporter。未来若有具体
   consumer demand，再另开 implementation issue，并先验证 static event ingestion。

## References

- [OpenLineage JSON schema 2-0-2](https://openlineage.io/spec/2-0-2/OpenLineage.json)
- [OpenLineage spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md)
- [OpenLineage Object Model](https://openlineage.io/docs/spec/object-model)
- [OpenLineage Static Lineage proposal](https://github.com/OpenLineage/OpenLineage/blob/main/proposals/1837/static_lineage.md)
- [OpenLineage Job source code location facet](https://openlineage.io/docs/spec/facets/job-facets/source-code-location)
- [OpenLineage Run nominal time facet](https://openlineage.io/docs/spec/facets/run-facets/nominal_time)
- [OpenLineage Run parent facet](https://openlineage.io/docs/spec/facets/run-facets/parent_run/)
- [DataHub OpenLineage integration](https://docs.datahub.com/docs/lineage/openlineage)
- [OpenMetadata OpenLineage connector](https://docs.open-metadata.org/v2.0.x/connectors/pipeline/openlineage)
