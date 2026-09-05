"""安全的内网 lineage source verification harness。

这个模块只做诊断，不改变 ``ProgramSource``、Parser、DAG、Audit 或
materialization 的语义。它复用现有 Provider 的连接、safe identifier 和
streaming query 边界，在内网运行时只生成脱敏统计结果。
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from shared.lineage.domain import ProgramSource, normalize_expected_target
from shared.lineage.providers import (
    ConnectionFactory,
    ExpectedTargetGetter,
    MySQLProcessProfile,
    MySQLProcessProvider,
    ProductionProvider,
    ProviderError,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_SAMPLE_LIMIT = 20
DEFAULT_REPORT_PATH = Path("artifacts/lineage_verification/report.json")


class VerificationErrorCode(str, Enum):
    """可供自动化和人工诊断使用的错误分类。"""

    SUCCESS = "SUCCESS"
    NOT_RUN = "NOT_RUN"
    CONFIG_ERROR = "CONFIG_ERROR"
    CONNECT_ERROR = "CONNECT_ERROR"
    QUERY_ERROR = "QUERY_ERROR"
    ROW_MAPPING_ERROR = "ROW_MAPPING_ERROR"
    DECODE_ERROR = "DECODE_ERROR"


class SnapshotStatus(str, Enum):
    """snapshot 是否具备支持 ``DELETED`` 判断的完整性。"""

    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    NOT_RUN = "NOT_RUN"


class TargetEvidenceSource(str, Enum):
    """诊断用 target evidence 分类，不表示最终 authority。"""

    EXPLICIT_FIELD = "EXPLICIT_FIELD"
    METADATA_JOIN = "METADATA_JOIN"
    DERIVED_ONLY = "DERIVED_ONLY"
    MISSING = "MISSING"
    CONFLICT = "CONFLICT"
    UNKNOWN = "UNKNOWN"


class TargetEvidenceComparison(str, Enum):
    """多个 target evidence 来源之间的比较结果。"""

    MATCH = "MATCH"
    CONFLICT = "CONFLICT"
    SINGLE_SOURCE = "SINGLE_SOURCE"
    MISSING = "MISSING"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class TargetProbeInput:
    """传给 target probe 的最小 identity 输入，不包含 script code。"""

    environment: str
    source_profile: str
    program_name: str = field(repr=False)


TargetGetter = Callable[[TargetProbeInput], object | None]


@dataclass(frozen=True, slots=True)
class TargetEvidenceProbe:
    """可注入的历史 metadata join / derived target 诊断回调。"""

    metadata_join: TargetGetter | None = None
    derived: TargetGetter | None = None


@dataclass(frozen=True, slots=True)
class TargetEvidence:
    """只保存分类和比较结果，不保存真实 target 值。"""

    source: TargetEvidenceSource
    comparison: TargetEvidenceComparison
    explicit_present: bool
    metadata_join_present: bool
    derived_present: bool

    @classmethod
    def from_values(
        cls,
        *,
        explicit_value: object | None,
        explicit_configured: bool,
        metadata_value: object | None,
        metadata_configured: bool,
        derived_value: object | None,
        derived_configured: bool,
        probe_error: bool = False,
    ) -> TargetEvidence:
        """比较来源值并丢弃原始 target，只返回安全分类。"""

        if probe_error:
            return cls(
                source=TargetEvidenceSource.UNKNOWN,
                comparison=TargetEvidenceComparison.UNKNOWN,
                explicit_present=False,
                metadata_join_present=False,
                derived_present=False,
            )

        explicit = _canonical_target(explicit_value) if explicit_configured else None
        metadata = _canonical_target(metadata_value) if metadata_configured else None
        derived = _canonical_target(derived_value) if derived_configured else None
        present_values = [value for value in (explicit, metadata, derived) if value]
        explicit_present = explicit is not None
        metadata_present = metadata is not None
        derived_present = derived is not None

        if not present_values:
            source = TargetEvidenceSource.MISSING
            comparison = TargetEvidenceComparison.MISSING
        elif len(set(present_values)) > 1:
            source = TargetEvidenceSource.CONFLICT
            comparison = TargetEvidenceComparison.CONFLICT
        else:
            if explicit_present:
                source = TargetEvidenceSource.EXPLICIT_FIELD
            elif metadata_present:
                source = TargetEvidenceSource.METADATA_JOIN
            else:
                source = TargetEvidenceSource.DERIVED_ONLY
            comparison = (
                TargetEvidenceComparison.MATCH
                if len(present_values) >= 2
                else TargetEvidenceComparison.SINGLE_SOURCE
            )

        return cls(
            source=source,
            comparison=comparison,
            explicit_present=explicit_present,
            metadata_join_present=metadata_present,
            derived_present=derived_present,
        )


@dataclass
class _ScriptInspection:
    runtime_type: str
    is_null: bool
    raw_length: int | None
    decoded_length: int | None
    decode_success: bool
    thin_adapter_required: bool = False
    replacement: object = field(default_factory=lambda: _NO_REPLACEMENT)


@dataclass
class _ObservationState:
    """单个 profile 的内存诊断状态；不会被序列化为原始行。"""

    script_type_counts: Counter[str] = field(default_factory=Counter)
    script_null_count: int = 0
    script_decode_successes: int = 0
    script_decode_failures: int = 0
    script_thin_adapter_required: int = 0
    script_raw_lengths: list[int] = field(default_factory=list)
    script_decoded_lengths: list[int] = field(default_factory=list)
    row_count: int = 0
    explicit_target_null_count: int = 0
    explicit_target_present_count: int = 0
    connection_status: str = "NOT_RUN"
    query_status: str = "NOT_RUN"
    mapping_error: bool = False
    identity_hashes: set[str] = field(default_factory=set)
    duplicate_identity_count: int = 0
    duplicate_identity_samples: list[str] = field(default_factory=list)
    target_evidence_counts: Counter[str] = field(default_factory=Counter)
    target_comparison_counts: Counter[str] = field(default_factory=Counter)
    target_match_count: int = 0
    target_conflict_count: int = 0
    derived_only_count: int = 0
    target_missing_count: int = 0
    target_unknown_count: int = 0

    def observe_row(
        self,
        row: object,
        profile: MySQLProcessProfile,
    ) -> object:
        """统计一个 raw row，并在必要时只替换 driver-specific script 值。"""

        self.row_count += 1
        try:
            _program_value = _raw_row_value(row, 0, profile.program_name_column)
            script_value = _raw_row_value(row, 1, profile.script_code_column)
            target_value = None
            if profile.expected_target_column is not None:
                target_value = _raw_row_value(row, 2, profile.expected_target_column)
        except _RawRowError:
            self.mapping_error = True
            return row

        if profile.expected_target_column is not None:
            try:
                normalized_target = normalize_expected_target(target_value)
            except Exception:
                normalized_target = None
            if normalized_target is None:
                self.explicit_target_null_count += 1
            else:
                self.explicit_target_present_count += 1

        inspection = _inspect_script_value(script_value)
        self.script_type_counts[inspection.runtime_type] += 1
        if inspection.is_null:
            self.script_null_count += 1
        elif inspection.decode_success:
            self.script_decode_successes += 1
        else:
            self.script_decode_failures += 1
        if inspection.thin_adapter_required:
            self.script_thin_adapter_required += 1
        if inspection.raw_length is not None and not inspection.is_null:
            self.script_raw_lengths.append(inspection.raw_length)
        if inspection.decoded_length is not None and not inspection.is_null:
            self.script_decoded_lengths.append(inspection.decoded_length)
        return _replace_script_value(
            row,
            profile.script_code_column,
            inspection.replacement,
        )

    def record_source(
        self,
        source: ProgramSource,
        profile: MySQLProcessProfile,
        target_probe: TargetEvidenceProbe | None,
    ) -> None:
        identity_hash = _identity_hash(
            source.environment,
            source.source_profile,
            source.program_name,
        )
        if identity_hash in self.identity_hashes:
            self.duplicate_identity_count += 1
            if (
                identity_hash not in self.duplicate_identity_samples
                and len(self.duplicate_identity_samples) < DEFAULT_SAMPLE_LIMIT
            ):
                self.duplicate_identity_samples.append(identity_hash)
        self.identity_hashes.add(identity_hash)

        metadata_configured = target_probe is not None and (
            target_probe.metadata_join is not None
        )
        derived_configured = target_probe is not None and (
            target_probe.derived is not None
        )
        metadata_value: object | None = None
        derived_value: object | None = None
        probe_error = False
        probe_input = TargetProbeInput(
            environment=source.environment,
            source_profile=source.source_profile,
            program_name=source.program_name,
        )
        if metadata_configured:
            try:
                metadata_value = target_probe.metadata_join(probe_input)  # type: ignore[misc]
            except Exception:
                probe_error = True
        if derived_configured:
            try:
                derived_value = target_probe.derived(probe_input)  # type: ignore[misc]
            except Exception:
                probe_error = True

        evidence = TargetEvidence.from_values(
            explicit_value=source.expected_target,
            explicit_configured=profile.expected_target_column is not None,
            metadata_value=metadata_value,
            metadata_configured=metadata_configured,
            derived_value=derived_value,
            derived_configured=derived_configured,
            probe_error=probe_error,
        )
        self.target_evidence_counts[evidence.source.value] += 1
        self.target_comparison_counts[evidence.comparison.value] += 1
        if evidence.comparison is TargetEvidenceComparison.MATCH:
            self.target_match_count += 1
        if evidence.source is TargetEvidenceSource.CONFLICT:
            self.target_conflict_count += 1
        if evidence.source is TargetEvidenceSource.DERIVED_ONLY:
            self.derived_only_count += 1
        if evidence.source is TargetEvidenceSource.MISSING:
            self.target_missing_count += 1
        if evidence.source is TargetEvidenceSource.UNKNOWN:
            self.target_unknown_count += 1


@dataclass(frozen=True, slots=True)
class ProfileVerificationResult:
    """一个 MySQL profile 的公开、安全诊断结果。"""

    profile_name: str
    environment: str
    connection_status: str
    query_status: str
    row_count: int
    sample_count: int
    script_null_count: int
    script_type_counts: dict[str, int]
    script_decode_successes: int
    script_decode_failures: int
    script_thin_adapter_required: int
    script_length_min: int | None
    script_length_max: int | None
    script_raw_length_min: int | None
    script_raw_length_max: int | None
    explicit_target_available: bool | None
    explicit_target_null_count: int
    explicit_target_present_count: int
    explicit_target_missing_count: int
    target_match_count: int
    target_conflict_count: int
    derived_only_count: int
    target_missing_count: int
    target_unknown_count: int
    target_evidence_counts: dict[str, int]
    target_comparison_counts: dict[str, int]
    duplicate_identity_count: int
    duplicate_identity_samples: tuple[str, ...]
    snapshot_status: str
    schema_checks: dict[str, bool | None]
    error_code: str
    error_message_safe: str | None
    elapsed_ms: int

    def to_dict(self) -> dict[str, object]:
        """只返回白名单字段；不包含 profile 配置、row 或异常 repr。"""

        return {
            "profile": self.profile_name,
            "environment": self.environment,
            "connection_status": self.connection_status,
            "query_status": self.query_status,
            "snapshot_status": self.snapshot_status,
            "row_count": self.row_count,
            "sample_count": self.sample_count,
            "schema_checks": dict(self.schema_checks),
            "script_type_counts": dict(self.script_type_counts),
            "script_null_count": self.script_null_count,
            "script_decode_successes": self.script_decode_successes,
            "script_decode_failures": self.script_decode_failures,
            "script_thin_adapter_required": self.script_thin_adapter_required,
            "script_length_min": self.script_length_min,
            "script_length_max": self.script_length_max,
            "script_raw_length_min": self.script_raw_length_min,
            "script_raw_length_max": self.script_raw_length_max,
            "explicit_target_available": self.explicit_target_available,
            "explicit_target_null_count": self.explicit_target_null_count,
            "explicit_target_present_count": self.explicit_target_present_count,
            "explicit_target_missing_count": self.explicit_target_missing_count,
            "target_match_count": self.target_match_count,
            "target_conflict_count": self.target_conflict_count,
            "derived_only_count": self.derived_only_count,
            "target_missing_count": self.target_missing_count,
            "target_unknown_count": self.target_unknown_count,
            "target_evidence_counts": dict(self.target_evidence_counts),
            "target_comparison_counts": dict(self.target_comparison_counts),
            "duplicate_identity_count": self.duplicate_identity_count,
            "duplicate_identity_samples": list(self.duplicate_identity_samples),
            "error_code": self.error_code,
            "error_message_safe": self.error_message_safe,
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass(frozen=True, slots=True)
class ProductionProviderVerificationResult:
    """ProductionProvider backend 的安全诊断结果。"""

    import_status: str
    legacy_loader_callable: bool
    loader_invocation_attempted: bool
    rows_readable: bool | None
    row_count: int
    sample_count: int
    snapshot_status: str
    error_code: str
    error_message_safe: str | None
    elapsed_ms: int

    @classmethod
    def not_run(cls) -> ProductionProviderVerificationResult:
        return cls(
            import_status="NOT_RUN",
            legacy_loader_callable=False,
            loader_invocation_attempted=False,
            rows_readable=None,
            row_count=0,
            sample_count=0,
            snapshot_status=SnapshotStatus.NOT_RUN.value,
            error_code=VerificationErrorCode.NOT_RUN.value,
            error_message_safe=None,
            elapsed_ms=0,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "import_status": self.import_status,
            "legacy_loader_callable": self.legacy_loader_callable,
            "loader_invocation_attempted": self.loader_invocation_attempted,
            "rows_readable": self.rows_readable,
            "row_count": self.row_count,
            "sample_count": self.sample_count,
            "snapshot_status": self.snapshot_status,
            "error_code": self.error_code,
            "error_message_safe": self.error_message_safe,
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass(frozen=True, slots=True)
class VerificationReport:
    """JSON/Markdown 报告的安全白名单模型。"""

    generated_at: str
    profiles: tuple[ProfileVerificationResult, ...]
    production_provider: ProductionProviderVerificationResult
    duplicate_identity_count: int
    duplicate_identity_samples: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "duplicate_identity_count": self.duplicate_identity_count,
            "duplicate_identity_samples": list(self.duplicate_identity_samples),
            "profiles": [profile.to_dict() for profile in self.profiles],
            "production_provider": self.production_provider.to_dict(),
        }


class _StageFailure(RuntimeError):
    def __init__(self, code: VerificationErrorCode) -> None:
        super().__init__()
        self.code = code


class _RawRowError(ValueError):
    pass


_NO_REPLACEMENT = object()


def verify_mysql_profile(
    profile: MySQLProcessProfile,
    *,
    connection_factory: ConnectionFactory | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    sample_only: bool = False,
    target_probe: TargetEvidenceProbe | None = None,
    _identity_hashes: set[str] | None = None,
) -> ProfileVerificationResult:
    """以现有 ``MySQLProcessProvider`` 流式核验一个 profile。"""

    _validate_sample_limit(sample_limit)
    started = _monotonic_ms()
    state = _ObservationState()
    if _identity_hashes is not None:
        state.identity_hashes = _identity_hashes
    observed_factory = _ObservedConnectionFactory(
        profile=profile,
        state=state,
        actual_factory=connection_factory,
        sample_limit=sample_limit,
        sample_only=sample_only,
    )
    error_code = VerificationErrorCode.SUCCESS
    iterator = None

    try:
        provider = MySQLProcessProvider(
            profile,
            connection_factory=observed_factory,
        )
        try:
            iterator = provider.iter_program_sources()
        except ProviderError as exc:
            error_code = _classify_profile_error(exc, state)
        else:
            try:
                for source in iterator:
                    state.record_source(source, profile, target_probe)
                    if sample_only and state.row_count >= sample_limit:
                        break
            except ProviderError as exc:
                error_code = _classify_profile_error(exc, state)
            finally:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
    except Exception:
        error_code = VerificationErrorCode.CONFIG_ERROR

    if error_code is VerificationErrorCode.SUCCESS:
        if state.mapping_error:
            error_code = VerificationErrorCode.ROW_MAPPING_ERROR
        elif state.script_decode_failures:
            error_code = VerificationErrorCode.DECODE_ERROR

    snapshot_status = (
        SnapshotStatus.PARTIAL
        if sample_only
        else (
            SnapshotStatus.COMPLETE
            if error_code is VerificationErrorCode.SUCCESS
            else SnapshotStatus.FAILED
        )
    )
    query_succeeded = state.query_status == "SUCCESS"
    if profile.expected_target_column is None:
        explicit_available = False
    elif query_succeeded:
        explicit_available = True
    else:
        explicit_available = None
    schema_checks = {
        "configured_table_accessible": query_succeeded,
        "program_column_readable": query_succeeded,
        "script_column_readable": query_succeeded,
        "target_column_readable": (
            None if profile.expected_target_column is None else query_succeeded
        ),
        "row_shape_valid": (None if state.row_count == 0 else not state.mapping_error),
    }
    error_message = _safe_error_message(error_code)
    return ProfileVerificationResult(
        profile_name=profile.name,
        environment=profile.environment,
        connection_status=state.connection_status,
        query_status=state.query_status,
        row_count=state.row_count,
        sample_count=min(state.row_count, sample_limit),
        script_null_count=state.script_null_count,
        script_type_counts={
            key: state.script_type_counts[key]
            for key in sorted(state.script_type_counts)
        },
        script_decode_successes=state.script_decode_successes,
        script_decode_failures=state.script_decode_failures,
        script_thin_adapter_required=state.script_thin_adapter_required,
        script_length_min=_minimum(state.script_decoded_lengths),
        script_length_max=_maximum(state.script_decoded_lengths),
        script_raw_length_min=_minimum(state.script_raw_lengths),
        script_raw_length_max=_maximum(state.script_raw_lengths),
        explicit_target_available=explicit_available,
        explicit_target_null_count=state.explicit_target_null_count,
        explicit_target_present_count=state.explicit_target_present_count,
        explicit_target_missing_count=(
            state.explicit_target_null_count
            if profile.expected_target_column is not None
            else 0
        ),
        target_match_count=state.target_match_count,
        target_conflict_count=state.target_conflict_count,
        derived_only_count=state.derived_only_count,
        target_missing_count=state.target_missing_count,
        target_unknown_count=state.target_unknown_count,
        target_evidence_counts=_enum_counter(
            state.target_evidence_counts, TargetEvidenceSource
        ),
        target_comparison_counts=_enum_counter(
            state.target_comparison_counts, TargetEvidenceComparison
        ),
        duplicate_identity_count=state.duplicate_identity_count,
        duplicate_identity_samples=tuple(state.duplicate_identity_samples),
        snapshot_status=snapshot_status.value,
        schema_checks=schema_checks,
        error_code=error_code.value,
        error_message_safe=error_message,
        elapsed_ms=max(0, _monotonic_ms() - started),
    )


def verify_mysql_profiles(
    profiles: Iterable[MySQLProcessProfile],
    *,
    connection_factory: ConnectionFactory | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    sample_only: bool = False,
    target_probe: TargetEvidenceProbe | None = None,
) -> tuple[ProfileVerificationResult, ...]:
    """按配置顺序验证 1..N 个 MySQL profile。"""

    _validate_sample_limit(sample_limit)
    identity_hashes: set[str] = set()
    results: list[ProfileVerificationResult] = []
    for profile in profiles:
        results.append(
            verify_mysql_profile(
                profile,
                connection_factory=connection_factory,
                sample_limit=sample_limit,
                sample_only=sample_only,
                target_probe=target_probe,
                _identity_hashes=identity_hashes,
            )
        )
    return tuple(results)


def verify_production_provider(
    loader: Callable[[], Iterable[object]] | None = None,
    *,
    environment: str = "PROD",
    source_profile: str = "production_metadata",
    expected_target_getter: ExpectedTargetGetter | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    sample_only: bool = False,
) -> ProductionProviderVerificationResult:
    """验证 ProductionProvider backend；不传 ``loader`` 时使用真实 legacy loader。"""

    _validate_sample_limit(sample_limit)
    started = _monotonic_ms()
    import_status = "PASS"
    resolved_loader = loader
    if resolved_loader is None:
        resolved_loader = _resolve_legacy_loader()
    loader_callable = callable(resolved_loader)
    invocation_attempted = False
    row_count = 0
    error_code = VerificationErrorCode.SUCCESS
    rows_readable: bool | None = None
    iterator = None

    if not loader_callable:
        error_code = VerificationErrorCode.CONFIG_ERROR
    else:
        try:
            provider = ProductionProvider(
                process_loader=resolved_loader,
                environment=environment,
                source_profile=source_profile,
                expected_target_getter=expected_target_getter,
            )
            invocation_attempted = True
            iterator = provider.iter_program_sources()
            for _source in iterator:
                row_count += 1
                if sample_only and row_count >= sample_limit:
                    break
            rows_readable = True
        except ProviderError as exc:
            rows_readable = False
            cause = exc.__cause__
            error_code = (
                VerificationErrorCode.ROW_MAPPING_ERROR
                if isinstance(cause, ValueError)
                else VerificationErrorCode.QUERY_ERROR
            )
        except Exception as exc:
            rows_readable = False
            error_code = (
                VerificationErrorCode.CONFIG_ERROR
                if isinstance(exc, (TypeError, ValueError))
                else VerificationErrorCode.QUERY_ERROR
            )
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    snapshot_status = (
        SnapshotStatus.PARTIAL
        if sample_only and error_code is VerificationErrorCode.SUCCESS
        else (
            SnapshotStatus.COMPLETE
            if error_code is VerificationErrorCode.SUCCESS
            else SnapshotStatus.FAILED
        )
    )
    return ProductionProviderVerificationResult(
        import_status=import_status,
        legacy_loader_callable=loader_callable,
        loader_invocation_attempted=invocation_attempted,
        rows_readable=rows_readable,
        row_count=row_count,
        sample_count=min(row_count, sample_limit),
        snapshot_status=snapshot_status.value,
        error_code=error_code.value,
        error_message_safe=_safe_error_message(error_code),
        elapsed_ms=max(0, _monotonic_ms() - started),
    )


def build_verification_report(
    profiles: Iterable[ProfileVerificationResult],
    *,
    production_provider: ProductionProviderVerificationResult | None = None,
    generated_at: str | None = None,
) -> VerificationReport:
    """组装只含脱敏字段的 JSON 报告。"""

    profile_results = tuple(profiles)
    duplicate_count = sum(result.duplicate_identity_count for result in profile_results)
    duplicate_samples: list[str] = []
    for result in profile_results:
        for identity_hash in result.duplicate_identity_samples:
            if identity_hash not in duplicate_samples:
                duplicate_samples.append(identity_hash)
            if len(duplicate_samples) >= DEFAULT_SAMPLE_LIMIT:
                break
        if len(duplicate_samples) >= DEFAULT_SAMPLE_LIMIT:
            break
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    return VerificationReport(
        generated_at=timestamp,
        profiles=profile_results,
        production_provider=production_provider
        or ProductionProviderVerificationResult.not_run(),
        duplicate_identity_count=duplicate_count,
        duplicate_identity_samples=tuple(duplicate_samples),
    )


def write_json_report(report: VerificationReport, output_path: str | Path) -> Path:
    """写入 artifacts/lineage_verification 下的 JSON 报告。"""

    path = _safe_report_path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # pi-lens-ignore: python-path-traversal (path is resolved under the report root)
    path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def render_markdown_report(report: VerificationReport) -> str:
    """生成只引用 JSON 白名单字段的人工摘要。"""

    lines = [
        "# Lineage Intranet Verification Report",
        "",
        f"- Generated at: `{report.generated_at}`",
        f"- Duplicate identity count: `{report.duplicate_identity_count}`",
        "",
        "## MySQL Profiles",
        "",
        "| Profile | Environment | Connection | Query | Snapshot | Rows | Error |",
        "| --- | --- | --- | --- | --- | ---: | --- |",
    ]
    for profile in report.profiles:
        lines.append(
            f"| {profile.profile_name} | {profile.environment} | "
            f"{profile.connection_status} | {profile.query_status} | "
            f"{profile.snapshot_status} | {profile.row_count} | "
            f"{profile.error_code} |"
        )
    production = report.production_provider
    lines.extend(
        [
            "",
            "## ProductionProvider",
            "",
            f"- Import: `{production.import_status}`",
            f"- Legacy loader callable: `{production.legacy_loader_callable}`",
            f"- Loader invocation attempted: `"
            f"{production.loader_invocation_attempted}`",
            f"- Rows readable: `{production.rows_readable}`",
            f"- Snapshot: `{production.snapshot_status}`",
            f"- Error: `{production.error_code}`",
            "",
            "This report intentionally omits credentials, hosts, schema/table/column "
            "names, SQL, raw script content, real program/target names and SVN URLs.",
            "",
        ]
    )
    return "\n".join(lines)


def write_markdown_report(report: VerificationReport, output_path: str | Path) -> Path:
    """写入 artifacts/lineage_verification 下的 Markdown 摘要。"""

    path = _safe_report_path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # pi-lens-ignore: python-path-traversal (path is resolved under the report root)
    path.write_text(render_markdown_report(report), encoding="utf-8")
    return path


def _resolve_legacy_loader() -> Callable[[], Iterable[object]] | None:
    try:
        from shared.lineage.lineage_builder import load_process_infos
    except Exception:
        return None
    return load_process_infos if callable(load_process_infos) else None


class _ObservedConnectionFactory:
    def __init__(
        self,
        *,
        profile: MySQLProcessProfile,
        state: _ObservationState,
        actual_factory: ConnectionFactory | None,
        sample_limit: int,
        sample_only: bool,
    ) -> None:
        self.profile = profile
        self.state = state
        self.actual_factory = actual_factory
        self.sample_limit = sample_limit
        self.sample_only = sample_only

    def __call__(self, settings: Any) -> _ObservedConnection:
        factory = self.actual_factory
        if factory is None:
            from shared.lineage.providers import default_mysql_connection_factory

            factory = default_mysql_connection_factory
        try:
            connection = factory(settings)
        except Exception as exc:
            self.state.connection_status = "FAILED"
            raise _StageFailure(VerificationErrorCode.CONNECT_ERROR) from exc
        self.state.connection_status = "SUCCESS"
        return _ObservedConnection(
            connection,
            profile=self.profile,
            state=self.state,
            sample_limit=self.sample_limit,
            sample_only=self.sample_only,
        )


class _ObservedConnection:
    def __init__(
        self,
        connection: Any,
        *,
        profile: MySQLProcessProfile,
        state: _ObservationState,
        sample_limit: int,
        sample_only: bool,
    ) -> None:
        self.connection = connection
        self.profile = profile
        self.state = state
        self.sample_limit = sample_limit
        self.sample_only = sample_only

    def cursor(self) -> _ObservedCursor:
        try:
            cursor = self.connection.cursor()
        except Exception as exc:
            self.state.connection_status = "FAILED"
            raise _StageFailure(VerificationErrorCode.CONNECT_ERROR) from exc
        return _ObservedCursor(
            cursor,
            profile=self.profile,
            state=self.state,
            sample_limit=self.sample_limit,
            sample_only=self.sample_only,
        )

    def close(self) -> None:
        self.connection.close()


class _ObservedCursor:
    def __init__(
        self,
        cursor: Any,
        *,
        profile: MySQLProcessProfile,
        state: _ObservationState,
        sample_limit: int,
        sample_only: bool,
    ) -> None:
        self.cursor = cursor
        self.profile = profile
        self.state = state
        self.sample_limit = sample_limit
        self.sample_only = sample_only

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        try:
            result = self.cursor.execute(*args, **kwargs)
        except Exception as exc:
            self.state.query_status = "FAILED"
            raise _StageFailure(VerificationErrorCode.QUERY_ERROR) from exc
        self.state.query_status = "SUCCESS"
        return result

    def fetchmany(self, size: int) -> Any:
        fetch_size = size
        if self.sample_only:
            remaining = self.sample_limit - self.state.row_count
            if remaining <= 0:
                return []
            fetch_size = min(size, remaining)
        try:
            batch = self.cursor.fetchmany(fetch_size)
        except Exception as exc:
            self.state.query_status = "FAILED"
            raise _StageFailure(VerificationErrorCode.QUERY_ERROR) from exc
        if not batch:
            return batch
        return [self.state.observe_row(row, self.profile) for row in batch]

    def close(self) -> None:
        self.cursor.close()


def _raw_row_value(row: object, index: int, key: str) -> object:
    if isinstance(row, Mapping):
        if key not in row:
            raise _RawRowError()
        return row[key]
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, KeyError, TypeError):
        raise _RawRowError() from None


def _replace_script_value(row: object, key: str, replacement: object) -> object:
    if replacement is _NO_REPLACEMENT:
        return row
    if isinstance(row, Mapping):
        copied = dict(row)
        copied[key] = replacement
        return copied
    if isinstance(row, tuple):
        values = list(row)
        if len(values) > 1:
            values[1] = replacement
            return tuple(values)
        return row
    if isinstance(row, list):
        values = list(row)
        if len(values) > 1:
            values[1] = replacement
        return values
    return row


def _inspect_script_value(value: object) -> _ScriptInspection:
    if value is None:
        return _ScriptInspection(
            runtime_type="None",
            is_null=True,
            raw_length=None,
            decoded_length=0,
            decode_success=True,
        )
    if isinstance(value, str):
        return _ScriptInspection(
            runtime_type="str",
            is_null=False,
            raw_length=len(value),
            decoded_length=len(value),
            decode_success=True,
            replacement=value,
        )
    if isinstance(value, bytes):
        return _decode_bytes_like("bytes", bytes(value))
    if isinstance(value, bytearray):
        return _decode_bytes_like("bytearray", bytes(value))
    if isinstance(value, memoryview):
        try:
            raw = value.tobytes()
        except Exception:
            return _ScriptInspection(
                runtime_type="memoryview",
                is_null=False,
                raw_length=None,
                decoded_length=None,
                decode_success=False,
            )
        return _decode_bytes_like("memoryview", raw)

    try:
        reader = getattr(value, "read", None)
    except Exception:
        reader = None
    if callable(reader):
        try:
            unwrapped = reader()
        except Exception:
            return _ScriptInspection(
                runtime_type="driver_specific_object",
                is_null=False,
                raw_length=None,
                decoded_length=None,
                decode_success=False,
                thin_adapter_required=True,
            )
        if unwrapped is value:
            return _ScriptInspection(
                runtime_type="driver_specific_object",
                is_null=False,
                raw_length=None,
                decoded_length=None,
                decode_success=False,
                thin_adapter_required=True,
            )
        decoded = _inspect_script_value(unwrapped)
        decoded.runtime_type = "driver_specific_object"
        return decoded

    return _ScriptInspection(
        runtime_type="other",
        is_null=False,
        raw_length=None,
        decoded_length=None,
        decode_success=False,
        thin_adapter_required=True,
    )


def _decode_bytes_like(runtime_type: str, raw: bytes) -> _ScriptInspection:
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError:
        return _ScriptInspection(
            runtime_type=runtime_type,
            is_null=False,
            raw_length=len(raw),
            decoded_length=None,
            decode_success=False,
        )
    return _ScriptInspection(
        runtime_type=runtime_type,
        is_null=False,
        raw_length=len(raw),
        decoded_length=len(decoded),
        decode_success=True,
        replacement=decoded,
    )


def _canonical_target(value: object | None) -> str | None:
    try:
        normalized = normalize_expected_target(value)
    except Exception:
        return None
    return normalized.upper() if normalized is not None else None


def _identity_hash(environment: str, source_profile: str, program_name: str) -> str:
    payload = "\x1f".join((environment, source_profile, program_name)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def _classify_profile_error(
    error: ProviderError,
    state: _ObservationState,
) -> VerificationErrorCode:
    stage = _find_stage_failure(error)
    if stage is not None:
        return stage
    if state.mapping_error:
        return VerificationErrorCode.ROW_MAPPING_ERROR
    if state.script_decode_failures:
        return VerificationErrorCode.DECODE_ERROR
    if state.query_status == "SUCCESS":
        return VerificationErrorCode.ROW_MAPPING_ERROR
    return VerificationErrorCode.CONFIG_ERROR


def _find_stage_failure(error: BaseException) -> VerificationErrorCode | None:
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, _StageFailure):
            return current.code
        current = current.__cause__ or current.__context__
    return None


def _safe_error_message(code: VerificationErrorCode) -> str | None:
    messages = {
        VerificationErrorCode.CONFIG_ERROR: "profile configuration is invalid",
        VerificationErrorCode.CONNECT_ERROR: "connection could not be established",
        VerificationErrorCode.QUERY_ERROR: "metadata query failed",
        VerificationErrorCode.ROW_MAPPING_ERROR: "metadata row shape is invalid",
        VerificationErrorCode.DECODE_ERROR: "script value could not be decoded",
    }
    return messages.get(code)


def _enum_counter(counter: Counter[str], enum_type: type[Enum]) -> dict[str, int]:
    return {member.value: counter.get(member.value, 0) for member in enum_type}


def _minimum(values: list[int]) -> int | None:
    return min(values) if values else None


def _maximum(values: list[int]) -> int | None:
    return max(values) if values else None


def _validate_sample_limit(sample_limit: int) -> None:
    if isinstance(sample_limit, bool) or not isinstance(sample_limit, int):
        raise ValueError("sample_limit must be a non-negative integer")
    if sample_limit < 0:
        raise ValueError("sample_limit must be a non-negative integer")


def _safe_report_path(output_path: str | Path) -> Path:
    root = (Path.cwd() / "artifacts" / "lineage_verification").resolve()
    candidate = Path(output_path)
    resolved = (
        candidate if candidate.is_absolute() else Path.cwd() / candidate
    ).resolve()
    if root not in resolved.parents:
        raise ValueError("report output must stay under artifacts/lineage_verification")
    return resolved


def _monotonic_ms() -> int:
    import time

    try:
        current = time.monotonic()
        return int(current * 1000)
    except (OverflowError, TypeError, ValueError):
        return 0


__all__ = [
    "DEFAULT_REPORT_PATH",
    "DEFAULT_SAMPLE_LIMIT",
    "ProfileVerificationResult",
    "ProductionProviderVerificationResult",
    "SnapshotStatus",
    "TargetEvidence",
    "TargetEvidenceComparison",
    "TargetEvidenceProbe",
    "TargetEvidenceSource",
    "TargetProbeInput",
    "VerificationErrorCode",
    "VerificationReport",
    "build_verification_report",
    "render_markdown_report",
    "verify_mysql_profile",
    "verify_mysql_profiles",
    "verify_production_provider",
    "write_json_report",
    "write_markdown_report",
]
