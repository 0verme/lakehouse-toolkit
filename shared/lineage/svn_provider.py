"""将已 checkout 的本地 SVN inventory 映射为 ``ProgramSource``。

这个 adapter 只消费 local working copy：不执行 ``svn checkout``、``svn update``，
不访问 SVN 网络，也不持有凭据。inventory 负责目录 contract，provider 只对已经
validated 的 program file 做安全、惰性的源码读取，然后复用现有 Parser、Physical
DAG、Audit 和 materialization pipeline。
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Generator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath

from . import svn_inventory
from .domain import (
    ProgramSource,
    compute_source_hash,
    normalize_expected_target,
)

# These reasons are deliberately fixed, low-cardinality values.  They are safe to
# expose in logs and diagnostics; no path or source text is attached to them.
IDENTITY_COLLISION = "IDENTITY_COLLISION"
DUPLICATE_IDENTITY = "DUPLICATE_IDENTITY"
INVALID_PROGRAM_IDENTITY = "INVALID_PROGRAM_IDENTITY"
TARGET_AUTHORITY_MISSING = "TARGET_AUTHORITY_MISSING"
ITERATION_INCOMPLETE = "ITERATION_INCOMPLETE"

SNAPSHOT_COMPLETE = "COMPLETE"
SNAPSHOT_PARTIAL = "PARTIAL"
SNAPSHOT_FAILED = "FAILED"

_PROVIDER_STAGE_INVENTORY = "inventory"
_PROVIDER_STAGE_IDENTITY = "identity"
_PROVIDER_STAGE_SOURCE_READ = "source_read"


class SVNProviderError(RuntimeError):
    """Fatal local working-copy provider error without sensitive context."""


@dataclass(frozen=True, slots=True)
class SVNProviderDiagnostic:
    """A sanitized provider diagnostic.

    Only a short anonymous ``program_id`` may identify one rejected file.  The
    relative path, filename, target, source code and root path are intentionally
    absent from this value and its serialized form.
    """

    profile_name: str
    environment: str
    stage: str
    reason: str
    program_id: str | None = None
    count: int = 1

    def __post_init__(self) -> None:
        for field_name in (
            "profile_name",
            "environment",
            "stage",
            "reason",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if self.program_id is not None:
            if not isinstance(self.program_id, str) or not self.program_id.strip():
                raise ValueError("program_id must be a non-empty string or None")
        if isinstance(self.count, bool) or not isinstance(self.count, int):
            raise ValueError("count must be a positive integer")
        if self.count <= 0:
            raise ValueError("count must be a positive integer")

    def to_dict(self) -> dict[str, object]:
        """Return a path/source-free JSON-ready diagnostic."""

        return {
            "profile_name": self.profile_name,
            "environment": self.environment,
            "stage": self.stage,
            "reason": self.reason,
            "program_id": self.program_id,
            "count": self.count,
        }


@dataclass(frozen=True, slots=True)
class SVNProviderAccounting:
    """Sanitized counters for one provider inventory/read attempt."""

    profile_name: str
    environment: str
    layout: str
    scanned_python_files: int
    candidate_program_files: int
    matched_program_files: int
    yielded_program_sources: int
    rejected_program_files: int
    out_of_scope_python_files: int
    primary_target_resolved: int
    primary_target_unresolved: int
    readable_files: int
    read_errors: int
    decode_errors: int
    layer_counts: dict[str, int] = field(default_factory=dict)
    unresolved_reasons: dict[str, int] = field(default_factory=dict)
    diagnostics: tuple[SVNProviderDiagnostic, ...] = ()
    snapshot_status: str = SNAPSHOT_FAILED

    @property
    def unmatched_python_files(self) -> int:
        return max(0, self.scanned_python_files - self.matched_program_files)

    @property
    def snapshot_complete(self) -> bool:
        return self.snapshot_status == SNAPSHOT_COMPLETE

    @property
    def diagnostic_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for diagnostic in self.diagnostics:
            counts[diagnostic.reason] = (
                counts.get(diagnostic.reason, 0) + diagnostic.count
            )
        return dict(sorted(counts.items()))

    def to_dict(self) -> dict[str, object]:
        """Return only aggregate and anonymous diagnostic fields."""

        return {
            "profile_name": self.profile_name,
            "environment": self.environment,
            "layout": self.layout,
            "scanned_python_files": self.scanned_python_files,
            "candidate_program_files": self.candidate_program_files,
            "matched_program_files": self.matched_program_files,
            "unmatched_python_files": self.unmatched_python_files,
            "yielded_program_sources": self.yielded_program_sources,
            "rejected_program_files": self.rejected_program_files,
            "out_of_scope_python_files": self.out_of_scope_python_files,
            "primary_target_resolved": self.primary_target_resolved,
            "primary_target_unresolved": self.primary_target_unresolved,
            "layer_counts": dict(self.layer_counts),
            "readable_files": self.readable_files,
            "read_errors": self.read_errors,
            "decode_errors": self.decode_errors,
            "unresolved_reasons": dict(self.unresolved_reasons),
            "diagnostic_counts": self.diagnostic_counts,
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "snapshot_status": self.snapshot_status,
        }


def _normalize_relative_locator(value: str | os.PathLike[str]) -> str:
    """Normalize a root-relative locator without accepting machine paths."""

    raw_value = os.fspath(value)
    if isinstance(raw_value, bytes):
        raw_value = os.fsdecode(raw_value)
    text = str(raw_value).replace("\\", "/")
    if not text:
        raise ValueError("relative locator must be non-empty")

    posix_path = PurePosixPath(text)
    windows_path = PureWindowsPath(text)
    if (
        posix_path.is_absolute()
        or bool(windows_path.drive)
        or bool(windows_path.root)
        or "://" in text
    ):
        raise ValueError("relative locator must not contain a machine path")

    parts = posix_path.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("relative locator contains an invalid component")
    if PurePosixPath(parts[-1]).suffix.casefold() != ".py":
        raise ValueError("relative locator must identify a Python file")
    return "/".join(parts)


def canonicalize_svn_relative_locator(value: str | os.PathLike[str]) -> str:
    """Return the machine-independent, case-folded relative program locator."""

    return _normalize_relative_locator(value).casefold()


def svn_program_name_from_relative_path(value: str | os.PathLike[str]) -> str:
    """Build the compatibility ``program_name`` for one SVN file.

    The complete relative locator (directory layout plus filename) is retained
    under an explicit SVN namespace.  It is never derived from the primary
    target alone, and it contains no absolute root, drive letter or URL.
    """

    return f"SVN/{canonicalize_svn_relative_locator(value)}"


def _anonymous_program_id(
    environment: str,
    source_profile: str,
    program_name: str,
) -> str:
    identity = "\x1f".join((environment, source_profile, program_name))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]


def _safe_source_path(
    profile: svn_inventory.SVNProfile, relative_path: str
) -> Path | None:
    """Resolve a scanner-produced relative path while preventing traversal."""

    try:
        root = profile.root_path.expanduser().resolve()
        relative_locator = _normalize_relative_locator(relative_path)
        candidate = (root / Path(*PurePosixPath(relative_locator).parts)).resolve()
        candidate.relative_to(root)
    except (OSError, TypeError, ValueError):
        return None
    return candidate if candidate.is_file() else None


def _diagnostic(
    profile: svn_inventory.SVNProfile,
    stage: str,
    reason: str,
    *,
    program_name: str | None = None,
    count: int = 1,
) -> SVNProviderDiagnostic:
    program_id = (
        _anonymous_program_id(profile.environment, profile.name, program_name)
        if program_name is not None
        else None
    )
    return SVNProviderDiagnostic(
        profile_name=profile.name,
        environment=profile.environment,
        stage=stage,
        reason=reason,
        program_id=program_id,
        count=count,
    )


def _record_program_name(
    record: svn_inventory.SVNFileInventory,
) -> tuple[str, str] | None:
    try:
        locator = _normalize_relative_locator(record.relative_path)
        return locator, svn_program_name_from_relative_path(locator)
    except (TypeError, ValueError):
        return None


class SVNProgramSourceProvider:
    """Read validated programs from one already checked-out local SVN profile."""

    def __init__(self, profile: svn_inventory.SVNProfile) -> None:
        if not isinstance(profile, svn_inventory.SVNProfile):
            raise TypeError("profile must be an SVNProfile")
        if profile.environment.upper() != "PROD":
            raise ValueError("SVN production provider requires environment=PROD")
        self.profile = profile
        self.environment = "PROD"
        self.source_profile = profile.name
        self._accounting: SVNProviderAccounting | None = None
        self._diagnostics: tuple[SVNProviderDiagnostic, ...] = ()
        self._snapshot_complete: bool | None = None

    @property
    def diagnostics(self) -> tuple[SVNProviderDiagnostic, ...]:
        """Diagnostics from the latest completed or attempted iteration."""

        return self._diagnostics

    @property
    def accounting(self) -> SVNProviderAccounting | None:
        """Sanitized counters from the latest completed or attempted iteration."""

        return self._accounting

    @property
    def snapshot_complete(self) -> bool | None:
        """Whether the latest iteration is safe to use as deletion authority."""

        return self._snapshot_complete

    @property
    def snapshot_status(self) -> str | None:
        accounting = self._accounting
        return None if accounting is None else accounting.snapshot_status

    def iter_program_sources(self) -> Generator[ProgramSource, None, None]:
        """Yield only readable, validated target programs in stable order."""

        return self._iter_program_sources()

    def _iter_program_sources(self) -> Generator[ProgramSource, None, None]:
        profile = self.profile
        diagnostics: list[SVNProviderDiagnostic] = []
        unresolved_reasons: dict[str, int] = {}
        scan_result: svn_inventory.SVNScanResult | None = None
        scanned = 0
        candidate_count = 0
        matched_count = 0
        out_of_scope_count = 0
        readable_count = 0
        read_error_count = 0
        decode_error_count = 0
        yielded_count = 0
        completed = False
        fatal_root_error = False

        self._accounting = None
        self._diagnostics = ()
        self._snapshot_complete = None

        try:
            # Inventory classification is metadata-only here.  Source code is
            # read below, one validated record at a time.
            scan_result = svn_inventory.scan_svn_profile(
                profile,
                read_sources=False,
            )
            scanned = scan_result.scanned_python_files
            candidate_count = scan_result.candidate_program_files
            matched_count = scan_result.matched_program_files
            out_of_scope_count = scan_result.out_of_scope_python_files
            read_error_count = scan_result.read_errors
            unresolved_reasons = dict(scan_result.unresolved_reasons)

            if out_of_scope_count:
                diagnostics.append(
                    _diagnostic(
                        profile,
                        _PROVIDER_STAGE_INVENTORY,
                        svn_inventory.OUT_OF_SCOPE,
                        count=out_of_scope_count,
                    )
                )
            if scan_result.read_errors:
                diagnostics.append(
                    _diagnostic(
                        profile,
                        _PROVIDER_STAGE_INVENTORY,
                        svn_inventory.READ_ERROR,
                        count=scan_result.read_errors,
                    )
                )

            entries: list[tuple[svn_inventory.SVNFileInventory, str, str]] = []
            identity_locators: dict[str, set[str]] = {}
            for record in scan_result.records:
                if not record.candidate:
                    continue
                if not record.matched_program_file:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_INVENTORY,
                            record.unresolved_reason or svn_inventory.INVALID_LAYOUT,
                            program_name=(_record_program_name(record) or (None, None))[
                                1
                            ],
                        )
                    )
                    continue

                program_data = _record_program_name(record)
                if program_data is None:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_IDENTITY,
                            INVALID_PROGRAM_IDENTITY,
                        )
                    )
                    continue
                locator, program_name = program_data
                entries.append((record, locator, program_name))
                identity_locators.setdefault(program_name, set()).add(locator)

            colliding_identities = {
                program_name
                for program_name, locators in identity_locators.items()
                if len(locators) > 1
            }
            identity_counts: dict[str, int] = {}
            for _record, _locator, program_name in entries:
                identity_counts[program_name] = identity_counts.get(program_name, 0) + 1
            duplicate_identities = {
                program_name
                for program_name, locators in identity_locators.items()
                if len(locators) == 1 and identity_counts.get(program_name, 0) > 1
            }
            for record, _locator, program_name in entries:
                if program_name in colliding_identities:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_IDENTITY,
                            IDENTITY_COLLISION,
                            program_name=program_name,
                        )
                    )
                    continue
                if program_name in duplicate_identities:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_IDENTITY,
                            DUPLICATE_IDENTITY,
                            program_name=program_name,
                        )
                    )
                    continue

                if record.declared_primary_target is None:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_INVENTORY,
                            TARGET_AUTHORITY_MISSING,
                            program_name=program_name,
                        )
                    )
                    continue

                source_path = _safe_source_path(profile, record.relative_path)
                if source_path is None:
                    read_error_count += 1
                    unresolved_reasons[svn_inventory.READ_ERROR] = (
                        unresolved_reasons.get(svn_inventory.READ_ERROR, 0) + 1
                    )
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_SOURCE_READ,
                            svn_inventory.READ_ERROR,
                            program_name=program_name,
                        )
                    )
                    continue

                try:
                    read_result = svn_inventory.read_python_source(source_path)
                except Exception:
                    # A custom reader must not turn a path or driver detail
                    # into a public diagnostic.  Treat every reader exception
                    # as a read failure.
                    read_result = svn_inventory.SourceReadResult(
                        None,
                        svn_inventory.READ_ERROR,
                        svn_inventory.READ_ERROR,
                    )

                if read_result.read_status == svn_inventory.READ_ERROR:
                    read_error_count += 1
                    unresolved_reasons[svn_inventory.READ_ERROR] = (
                        unresolved_reasons.get(svn_inventory.READ_ERROR, 0) + 1
                    )
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_SOURCE_READ,
                            svn_inventory.READ_ERROR,
                            program_name=program_name,
                        )
                    )
                    continue
                if read_result.read_status == svn_inventory.DECODE_ERROR:
                    decode_error_count += 1
                    unresolved_reasons[svn_inventory.DECODE_ERROR] = (
                        unresolved_reasons.get(svn_inventory.DECODE_ERROR, 0) + 1
                    )
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_SOURCE_READ,
                            svn_inventory.DECODE_ERROR,
                            program_name=program_name,
                        )
                    )
                    continue
                if read_result.read_status != svn_inventory.READABLE or not isinstance(
                    read_result.script_code, str
                ):
                    read_error_count += 1
                    unresolved_reasons[svn_inventory.READ_ERROR] = (
                        unresolved_reasons.get(svn_inventory.READ_ERROR, 0) + 1
                    )
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_SOURCE_READ,
                            svn_inventory.READ_ERROR,
                            program_name=program_name,
                        )
                    )
                    continue

                readable_count += 1
                expected_target = normalize_expected_target(
                    record.declared_primary_target
                )
                if expected_target is None:
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_INVENTORY,
                            TARGET_AUTHORITY_MISSING,
                            program_name=program_name,
                        )
                    )
                    continue
                try:
                    source = ProgramSource(
                        environment="PROD",
                        source_profile=profile.name,
                        program_name=program_name,
                        script_code=read_result.script_code,
                        expected_target=expected_target,
                        source_hash=compute_source_hash(
                            program_name,
                            read_result.script_code,
                            expected_target,
                        ),
                    )
                except (TypeError, ValueError):
                    diagnostics.append(
                        _diagnostic(
                            profile,
                            _PROVIDER_STAGE_IDENTITY,
                            INVALID_PROGRAM_IDENTITY,
                            program_name=program_name,
                        )
                    )
                    continue
                yielded_count += 1
                yield source

            completed = True
        except OSError as error:
            fatal_root_error = True
            if isinstance(error, FileNotFoundError):
                reason = svn_inventory.ROOT_NOT_FOUND
            elif isinstance(error, NotADirectoryError):
                reason = svn_inventory.ROOT_NOT_DIRECTORY
            else:
                reason = svn_inventory.READ_ERROR
            diagnostics.append(_diagnostic(profile, _PROVIDER_STAGE_INVENTORY, reason))
            raise
        finally:
            if not completed and not fatal_root_error and scan_result is not None:
                diagnostics.append(
                    _diagnostic(
                        profile,
                        _PROVIDER_STAGE_INVENTORY,
                        ITERATION_INCOMPLETE,
                    )
                )
            has_fatal_diagnostic = any(
                item.reason != svn_inventory.OUT_OF_SCOPE for item in diagnostics
            )
            if fatal_root_error or scan_result is None:
                snapshot_status = SNAPSHOT_FAILED
            elif not completed or has_fatal_diagnostic:
                snapshot_status = SNAPSHOT_PARTIAL
            elif scan_result.status != svn_inventory.SUCCESS:
                snapshot_status = SNAPSHOT_PARTIAL
            else:
                snapshot_status = SNAPSHOT_COMPLETE

            self._diagnostics = tuple(diagnostics)
            self._snapshot_complete = snapshot_status == SNAPSHOT_COMPLETE
            primary_target_resolved = (
                scan_result.primary_target_resolved if scan_result is not None else 0
            )
            primary_target_unresolved = (
                scan_result.primary_target_unresolved if scan_result is not None else 0
            )
            layer_counts = (
                dict(scan_result.layer_counts) if scan_result is not None else {}
            )
            self._accounting = SVNProviderAccounting(
                profile_name=profile.name,
                environment="PROD",
                layout=profile.layout,
                scanned_python_files=scanned,
                candidate_program_files=candidate_count,
                matched_program_files=matched_count,
                yielded_program_sources=yielded_count,
                rejected_program_files=max(0, candidate_count - yielded_count),
                out_of_scope_python_files=out_of_scope_count,
                primary_target_resolved=primary_target_resolved,
                primary_target_unresolved=primary_target_unresolved,
                layer_counts=layer_counts,
                readable_files=readable_count,
                read_errors=read_error_count,
                decode_errors=decode_error_count,
                unresolved_reasons=dict(sorted(unresolved_reasons.items())),
                diagnostics=self._diagnostics,
                snapshot_status=snapshot_status,
            )


# Descriptive alias for callers that name the backend first.  This is an alias,
# not a change to the legacy metadata ``ProductionProvider``.
ProductionSVNProvider = SVNProgramSourceProvider


def load_svn_program_source_profiles(
    config_path: str | Path | None = None,
) -> list[svn_inventory.SVNProfile]:
    """Load SVN profiles from the combined provider config.

    The SVN section is optional for backward-compatible MySQL-only local
    configs; if it is present, the strict inventory loader validates it.
    """

    path = (
        Path(config_path).expanduser()
        if config_path is not None
        else (
            svn_inventory.DEFAULT_CONFIG_PATH
            if svn_inventory.DEFAULT_CONFIG_PATH.exists()
            else svn_inventory.EXAMPLE_CONFIG_PATH
        )
    )
    return svn_inventory.load_svn_profiles(path, allow_missing=True)


def load_svn_program_source_providers(
    config_path: str | Path | None = None,
) -> tuple[SVNProgramSourceProvider, ...]:
    """Create independent providers for each configured SVN profile."""

    return tuple(
        SVNProgramSourceProvider(profile)
        for profile in load_svn_program_source_profiles(config_path)
    )


__all__ = [
    "DUPLICATE_IDENTITY",
    "IDENTITY_COLLISION",
    "INVALID_PROGRAM_IDENTITY",
    "ITERATION_INCOMPLETE",
    "ProductionSVNProvider",
    "SNAPSHOT_COMPLETE",
    "SNAPSHOT_FAILED",
    "SNAPSHOT_PARTIAL",
    "SVNProgramSourceProvider",
    "SVNProviderAccounting",
    "SVNProviderDiagnostic",
    "SVNProviderError",
    "TARGET_AUTHORITY_MISSING",
    "canonicalize_svn_relative_locator",
    "load_svn_program_source_profiles",
    "load_svn_program_source_providers",
    "svn_program_name_from_relative_path",
]
