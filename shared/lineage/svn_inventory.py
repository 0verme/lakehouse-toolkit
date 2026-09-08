"""Inventory and verification helpers for local production SVN workspaces.

This module deliberately does not know how to contact SVN.  It only walks a
working copy that has already been checked out, validates the repository's
known directory layouts, and reads matching Python files with
:func:`tokenize.open`.
"""

from __future__ import annotations

import json
import os
import re
import time
import tokenize
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

WORKSPACE_DIR = "DIDP_PROJECT_WORKSPACE"
PROCESSING_LAYOUT = "processing"
DWF_LAYOUT = "dwf"
SUPPORTED_LAYOUTS = frozenset({PROCESSING_LAYOUT, DWF_LAYOUT})

# DWE is retained because it is present in the legacy is_dws_py matcher.  The
# six layers below are the required production-processing layers documented by
# the current verification contract.
PROCESSING_LAYERS = (
    "DWM",
    "DWA",
    "DM",
    "DWP",
    "DWUPRR",
    "DWD",
    "DWE",
)
REQUIRED_PROCESSING_LAYERS = PROCESSING_LAYERS[:-1]
DWF_LAYER = "DWF"

IGNORED_DIRECTORY_NAMES = frozenset({".svn", "__pycache__", ".git", ".venv", "venv"})

READABLE = "READABLE"
READ_ERROR = "READ_ERROR"
DECODE_ERROR = "DECODE_ERROR"
NOT_ATTEMPTED = "NOT_ATTEMPTED"

SUCCESS = "SUCCESS"
NO_MATCHED_FILES = "NO_MATCHED_FILES"
PATH_LAYOUT_ERROR = "PATH_LAYOUT_ERROR"
ROOT_NOT_FOUND = "ROOT_NOT_FOUND"
ROOT_NOT_DIRECTORY = "ROOT_NOT_DIRECTORY"
CONFIG_ERROR = "CONFIG_ERROR"

INVALID_LAYOUT = "INVALID_LAYOUT"
GRANDPARENT_MISMATCH = "GRANDPARENT_MISMATCH"
INVALID_PROGRAM_DIRECTORY = "INVALID_PROGRAM_DIRECTORY"
# Retained for compatibility with report consumers that know the legacy key.
# A non-target top-level subtree is now classified as OUT_OF_SCOPE instead.
UNSUPPORTED_LAYER = "UNSUPPORTED_LAYER"
OUT_OF_SCOPE = "OUT_OF_SCOPE"
NOT_PYTHON = "NOT_PYTHON"

SVN_REPORT_VERSION = 2

UNRESOLVED_REASON_ORDER = (
    INVALID_LAYOUT,
    GRANDPARENT_MISMATCH,
    INVALID_PROGRAM_DIRECTORY,
    UNSUPPORTED_LAYER,
    NOT_PYTHON,
    READ_ERROR,
    DECODE_ERROR,
)

_PROGRAM_DIRECTORY_RE = re.compile(
    r"^DWS_(?P<layer>[A-Z][A-Z0-9_]*)\."
    r"(?P<table>[A-Z0-9][A-Z0-9_]*)$",
    re.IGNORECASE,
)

ProgressCallback = Callable[[str, int, int, int], None]


class SVNInventoryConfigError(ValueError):
    """A safe, field-oriented local SVN profile configuration error."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class SVNProfile:
    """Configuration for one local working-copy scan."""

    name: str
    environment: str
    root_path: Path = field(repr=False)
    layout: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _is_safe_profile_name(self.name):
            raise SVNInventoryConfigError("profile name must be a safe alias")
        if not isinstance(self.environment, str) or not _is_safe_profile_name(
            self.environment
        ):
            raise SVNInventoryConfigError("environment must be a safe alias")
        if self.layout not in SUPPORTED_LAYOUTS:
            raise SVNInventoryConfigError("layout must be one of: processing, dwf")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object], index: int = 0) -> SVNProfile:
        if not isinstance(raw, Mapping):
            raise SVNInventoryConfigError(f"profile[{index}] must be a mapping")

        def required_text(key: str) -> str:
            value = raw.get(key)
            if not isinstance(value, str) or not value.strip():
                raise SVNInventoryConfigError(
                    f"profile[{index}] missing required field: {key}"
                )
            return value.strip()

        name = required_text("name")
        environment = required_text("environment").upper()
        root_value = raw.get("root_path")
        if (
            not isinstance(root_value, (str, os.PathLike))
            or not str(root_value).strip()
        ):
            raise SVNInventoryConfigError(
                f"profile[{index}] missing required field: root_path"
            )
        layout = required_text("layout").lower()
        try:
            root_path = Path(root_value).expanduser()
        except (TypeError, ValueError, OSError) as exc:
            raise SVNInventoryConfigError(
                f"profile[{index}] root_path is invalid"
            ) from exc
        try:
            return cls(
                name=name,
                environment=environment,
                root_path=root_path,
                layout=layout,
            )
        except SVNInventoryConfigError as exc:
            raise SVNInventoryConfigError(f"profile[{index}] {exc.reason}") from exc


def load_svn_profiles(config_path: str | Path) -> list[SVNProfile]:
    """Load local-only ``svn_profiles`` from a YAML file.

    The parser intentionally accepts no URL, credential, checkout, or update
    settings.  A profile only identifies a local root and a known layout.
    """

    path = Path(config_path).expanduser()
    try:
        if not path.exists():
            raise SVNInventoryConfigError("config file was not found")
        if not path.is_file():
            raise SVNInventoryConfigError("config path is not a file")
    except OSError as exc:
        raise SVNInventoryConfigError("config file could not be read") from exc

    try:
        import yaml  # pyright: ignore[reportMissingModuleSource]
    except ModuleNotFoundError as exc:
        raise SVNInventoryConfigError("PyYAML is required to read the config") from exc

    try:
        with path.open("r", encoding="utf-8") as stream:
            raw_config = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise SVNInventoryConfigError("config file was not found") from exc
    except (OSError, UnicodeError) as exc:
        raise SVNInventoryConfigError("config file could not be read") from exc
    except yaml.YAMLError as exc:
        raise SVNInventoryConfigError("config YAML is invalid") from exc

    if not isinstance(raw_config, Mapping):
        raise SVNInventoryConfigError("root must be a mapping")
    raw_profiles = raw_config.get("svn_profiles")
    if not isinstance(raw_profiles, list) or not raw_profiles:
        raise SVNInventoryConfigError("svn_profiles must be a non-empty list")

    profiles = [
        SVNProfile.from_mapping(item, index=index)
        for index, item in enumerate(raw_profiles)
    ]
    names = [profile.name for profile in profiles]
    if len(names) != len(set(names)):
        raise SVNInventoryConfigError("profile names must be unique")
    return profiles


def _is_safe_profile_name(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value.strip()))


def _component(value: object) -> str:
    return str(value).casefold()


def _path_parts(path: str | os.PathLike[str]) -> tuple[str, ...]:
    """Return path components for both native and foreign Windows paths.

    ``Path`` is used for real filesystem access.  The two ``Pure*Path``
    variants additionally make the pure path parser deterministic when a
    Windows path is supplied to tests running on Linux (and vice versa).
    """

    value = os.fspath(path)
    if isinstance(value, bytes):
        value = os.fsdecode(value)

    candidates: list[tuple[str, ...]] = []
    for path_type in (Path, PurePosixPath, PureWindowsPath):
        try:
            parts = tuple(path_type(value).parts)
        except (TypeError, ValueError):
            continue
        if parts and parts not in candidates:
            candidates.append(parts)

    for parts in candidates:
        if any(_component(part) == _component(WORKSPACE_DIR) for part in parts):
            return parts
    return candidates[0] if candidates else ()


def _is_python_path(path: str | os.PathLike[str]) -> bool:
    parts = _path_parts(path)
    return bool(parts and PurePath(parts[-1]).suffix.casefold() == ".py")


def _workspace_indexes(parts: tuple[str, ...]) -> list[int]:
    return [
        index
        for index, part in enumerate(parts)
        if _component(part) == _component(WORKSPACE_DIR)
    ]


def _parse_program_directory(
    path: str | os.PathLike[str],
) -> tuple[str, str] | None:
    parts = _path_parts(path)
    if len(parts) < 3:
        return None

    parent = parts[-2]
    grandparent = parts[-3]
    match = _PROGRAM_DIRECTORY_RE.fullmatch(parent)
    if match is None:
        return None

    layer = match.group("layer").upper()
    table = match.group("table").upper()
    if grandparent.upper() != f"DWS_{layer}":
        return None
    return layer, table


def derive_primary_target_from_program_path(
    path: str | os.PathLike[str],
) -> str | None:
    """Derive a target only from a validated parent/grandparent directory pair.

    The filename and its task-number tokens are deliberately ignored.  The
    function also does not infer a target from SQL text or from a fuzzy path
    match.  A structurally valid, but unsupported, layer can still be parsed;
    layout-specific scanning decides whether it is a supported program.
    """

    if not _is_python_path(path):
        return None
    parsed = _parse_program_directory(path)
    if parsed is None:
        return None
    layer, table = parsed
    return f"{layer}.{table}"


@dataclass(frozen=True)
class PathClassification:
    """Safe classification metadata for one filesystem path.

    ``candidate`` marks a path inside the selected layout's domain, including
    malformed candidates.  ``out_of_scope`` is deliberately separate so a
    sibling layout or unrelated subtree cannot become a verification failure.
    """

    layout: str
    layer: str | None
    declared_primary_target: str | None
    matched_program_file: bool
    directory_pattern_valid: bool
    candidate: bool
    unresolved_reason: str | None = None
    out_of_scope: bool = False

    @property
    def primary_target_resolved(self) -> bool:
        return self.declared_primary_target is not None


def _classification(
    layout: str,
    *,
    layer: str | None = None,
    target: str | None = None,
    matched: bool = False,
    directory_valid: bool = False,
    candidate: bool = False,
    reason: str | None = None,
    out_of_scope: bool = False,
) -> PathClassification:
    return PathClassification(
        layout=layout,
        layer=layer,
        declared_primary_target=target,
        matched_program_file=matched,
        directory_pattern_valid=directory_valid,
        candidate=candidate,
        unresolved_reason=reason,
        out_of_scope=out_of_scope,
    )


def _classify_processing_at(
    parts: tuple[str, ...], workspace_index: int
) -> PathClassification:
    layout = PROCESSING_LAYOUT
    first_after_workspace_raw = (
        parts[workspace_index + 1] if len(parts) > workspace_index + 1 else None
    )
    first_after_workspace = (
        first_after_workspace_raw.upper()
        if first_after_workspace_raw is not None
        else None
    )
    if first_after_workspace not in PROCESSING_LAYERS:
        return _classification(
            layout,
            reason=OUT_OF_SCOPE,
            out_of_scope=True,
        )

    layer = first_after_workspace
    if len(parts) != workspace_index + 6:
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_LAYOUT,
        )

    expected_dws = f"DWS_{layer}"
    if (
        parts[workspace_index + 2] != "1.0"
        or parts[workspace_index + 3].upper() != expected_dws
    ):
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_LAYOUT,
        )

    parent = parts[workspace_index + 4]
    grandparent = parts[workspace_index + 3]
    parent_match = _PROGRAM_DIRECTORY_RE.fullmatch(parent)
    if parent_match is None:
        if "." in parent and parent.upper().split(".", 1)[0] != grandparent.upper():
            reason = GRANDPARENT_MISMATCH
        else:
            reason = INVALID_PROGRAM_DIRECTORY
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=reason,
        )

    parent_layer = parent_match.group("layer").upper()
    if parent_layer != layer or grandparent.upper() != f"DWS_{parent_layer}":
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=GRANDPARENT_MISMATCH,
        )

    target = derive_primary_target_from_program_path(
        PurePosixPath(*parts),
    )
    if target is None:
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_PROGRAM_DIRECTORY,
        )
    return _classification(
        layout,
        layer=layer,
        target=target,
        matched=True,
        directory_valid=True,
        candidate=True,
    )


def _classify_dwf_at(
    parts: tuple[str, ...], workspace_index: int
) -> PathClassification:
    layout = DWF_LAYOUT
    first_after_workspace_raw = (
        parts[workspace_index + 1] if len(parts) > workspace_index + 1 else None
    )
    first_after_workspace = (
        first_after_workspace_raw.upper()
        if first_after_workspace_raw is not None
        else None
    )
    if first_after_workspace != "DW_PROJECT":
        return _classification(
            layout,
            reason=OUT_OF_SCOPE,
            out_of_scope=True,
        )

    layer = DWF_LAYER
    if len(parts) != workspace_index + 6:
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_LAYOUT,
        )

    if (
        parts[workspace_index + 2] != "1.0"
        or parts[workspace_index + 3].upper() != "DWS_DWF"
    ):
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_LAYOUT,
        )

    parent = parts[workspace_index + 4]
    grandparent = parts[workspace_index + 3]
    parent_match = _PROGRAM_DIRECTORY_RE.fullmatch(parent)
    if parent_match is None:
        if "." in parent and parent.upper().split(".", 1)[0] != grandparent.upper():
            reason = GRANDPARENT_MISMATCH
        else:
            reason = INVALID_PROGRAM_DIRECTORY
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=reason,
        )

    parent_layer = parent_match.group("layer").upper()
    if parent_layer != layer or grandparent.upper() != f"DWS_{parent_layer}":
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=GRANDPARENT_MISMATCH,
        )

    target = derive_primary_target_from_program_path(PurePosixPath(*parts))
    if target is None:
        return _classification(
            layout,
            layer=layer,
            candidate=True,
            reason=INVALID_PROGRAM_DIRECTORY,
        )
    return _classification(
        layout,
        layer=layer,
        target=target,
        matched=True,
        directory_valid=True,
        candidate=True,
    )


def classify_svn_program_path(
    path: str | os.PathLike[str], layout: str
) -> PathClassification:
    """Validate one path against either the processing or DWF layout."""

    normalized_layout = str(layout).strip().lower()
    if normalized_layout not in SUPPORTED_LAYOUTS:
        raise ValueError("layout must be one of: processing, dwf")
    if not _is_python_path(path):
        return _classification(
            normalized_layout,
            candidate=False,
            reason=NOT_PYTHON,
        )

    parts = _path_parts(path)
    indexes = _workspace_indexes(parts)
    if not indexes:
        return _classification(
            normalized_layout,
            candidate=False,
            reason=OUT_OF_SCOPE,
            out_of_scope=True,
        )

    classifications = []
    for workspace_index in indexes:
        if normalized_layout == PROCESSING_LAYOUT:
            classifications.append(_classify_processing_at(parts, workspace_index))
        else:
            classifications.append(_classify_dwf_at(parts, workspace_index))

    for result in classifications:
        if result.matched_program_file:
            return result

    # Prefer a layout candidate's specific reason over an out-of-scope Python
    # path, while keeping the return deterministic for unusual nested
    # workspaces.
    return sorted(
        classifications,
        key=lambda result: (
            not result.candidate,
            result.out_of_scope,
            UNRESOLVED_REASON_ORDER.index(result.unresolved_reason)
            if result.unresolved_reason in UNRESOLVED_REASON_ORDER
            else len(UNRESOLVED_REASON_ORDER),
        ),
    )[0]


# Short alias for callers that do not need the SVN-specific spelling.
classify_program_path = classify_svn_program_path


@dataclass(frozen=True)
class SourceReadResult:
    """Result of safely reading one Python source file."""

    script_code: str | None = field(repr=False)
    read_status: str
    error_code: str | None = None


def _resolve_read_path(path: str | os.PathLike[str]) -> Path | None:
    try:
        candidate = Path(path).expanduser().resolve()
    except (OSError, TypeError, ValueError):
        return None
    return candidate if candidate.is_file() else None


def read_python_source(path: str | os.PathLike[str]) -> SourceReadResult:
    """Read source with the PEP 263 coding-cookie rules used by Python."""

    safe_path = _resolve_read_path(path)
    if safe_path is None:
        return SourceReadResult(None, READ_ERROR, READ_ERROR)

    try:
        with tokenize.open(safe_path) as stream:
            return SourceReadResult(stream.read(), READABLE)
    except (UnicodeDecodeError, LookupError, SyntaxError):
        return SourceReadResult(None, DECODE_ERROR, DECODE_ERROR)
    except (PermissionError, OSError):
        return SourceReadResult(None, READ_ERROR, READ_ERROR)


@dataclass(frozen=True)
class SVNFileInventory:
    """In-memory inventory row; report serialization is intentionally separate."""

    profile_name: str
    environment: str
    relative_path: str
    filename: str
    layout: str
    layer: str | None
    declared_primary_target: str | None
    file_size: int | None
    read_status: str
    matched_program_file: bool
    directory_pattern_valid: bool
    unresolved_reason: str | None = None
    candidate: bool = False
    out_of_scope: bool = False

    @property
    def primary_target_resolved(self) -> bool:
        return self.declared_primary_target is not None


@dataclass(frozen=True)
class SVNScanResult:
    """Coverage and verification result for one profile."""

    profile_name: str
    environment: str
    layout: str
    status: str
    scanned_python_files: int
    matched_program_files: int
    unmatched_python_files: int
    primary_target_resolved: int
    primary_target_unresolved: int
    readable_files: int
    read_errors: int
    decode_errors: int
    layer_counts: dict[str, int]
    unresolved_reasons: dict[str, int]
    elapsed_ms: float
    records: tuple[SVNFileInventory, ...] = field(repr=False, default_factory=tuple)
    # Appended with defaults so existing report/result constructors remain
    # source-compatible while consumers adopt the v2 accounting fields.
    candidate_program_files: int = 0
    out_of_scope_python_files: int = 0

    @property
    def primary_resolved_rate(self) -> float:
        denominator = self.primary_target_resolved + self.primary_target_unresolved
        if denominator == 0:
            return 0.0
        return round(100.0 * self.primary_target_resolved / denominator, 2)

    @property
    def sample_records(self) -> tuple[SVNFileInventory, ...]:
        return self.records

    @classmethod
    def empty(
        cls,
        profile: SVNProfile,
        status: str,
        *,
        elapsed_ms: float = 0.0,
    ) -> SVNScanResult:
        return cls(
            profile_name=profile.name,
            environment=profile.environment,
            layout=profile.layout,
            status=status,
            scanned_python_files=0,
            candidate_program_files=0,
            matched_program_files=0,
            out_of_scope_python_files=0,
            unmatched_python_files=0,
            primary_target_resolved=0,
            primary_target_unresolved=0,
            readable_files=0,
            read_errors=0,
            decode_errors=0,
            layer_counts=_initial_layer_counts(profile.layout),
            unresolved_reasons=_initial_reason_counts(),
            elapsed_ms=elapsed_ms,
        )


def _initial_layer_counts(layout: str) -> dict[str, int]:
    if layout == DWF_LAYOUT:
        return {DWF_LAYER: 0}
    return dict.fromkeys(PROCESSING_LAYERS, 0)


def _initial_reason_counts() -> dict[str, int]:
    return dict.fromkeys(UNRESOLVED_REASON_ORDER, 0)


def _iter_python_files(root: Path) -> tuple[list[Path], int]:
    paths: list[Path] = []
    walk_errors = 0

    def onerror(_error: OSError) -> None:
        nonlocal walk_errors
        walk_errors += 1

    for current_root, directory_names, file_names in os.walk(
        root, topdown=True, onerror=onerror
    ):
        directory_names[:] = sorted(
            name
            for name in directory_names
            if name.casefold() not in IGNORED_DIRECTORY_NAMES
        )
        for file_name in sorted(file_names, key=str.casefold):
            file_path = Path(current_root) / file_name
            if _is_python_path(file_path):
                paths.append(file_path)
    paths.sort(key=lambda item: item.relative_to(root).as_posix().casefold())
    return paths, walk_errors


def _scan_status(
    scanned: int,
    matched: int,
    read_errors: int,
    decode_errors: int,
    *,
    candidate: int | None = None,
) -> str:
    if read_errors and decode_errors:
        return READ_ERROR
    if read_errors:
        return READ_ERROR
    if decode_errors:
        return DECODE_ERROR
    if matched:
        return SUCCESS
    # Only a scanned candidate can prove that the target layout is malformed.
    # A root containing only sibling or unrelated Python files is not a path
    # layout error for this profile.
    candidate_count = scanned if candidate is None else candidate
    if candidate_count:
        return PATH_LAYOUT_ERROR
    return NO_MATCHED_FILES


def scan_svn_profile(
    profile: SVNProfile,
    *,
    sample_only: bool = False,
    sample_limit: int = 20,
    progress_callback: ProgressCallback | None = None,
    progress_interval: int = 500,
) -> SVNScanResult:
    """Scan one existing local working-copy root sequentially."""

    if sample_limit < 0:
        raise ValueError("sample_limit must be non-negative")
    if progress_interval <= 0:
        raise ValueError("progress_interval must be positive")

    root = profile.root_path.expanduser()
    if not root.exists():
        raise FileNotFoundError
    if not root.is_dir():
        raise NotADirectoryError
    root = root.resolve()

    started_at = time.perf_counter()
    all_python_paths, walk_errors = _iter_python_files(root)
    classified_paths = [
        (path, classify_svn_program_path(path, profile.layout))
        for path in all_python_paths
    ]
    if sample_only:
        # Walking and classifying paths is cheap metadata work.  Source reads
        # still happen only for the selected, profile-relevant sample below.
        classified_paths = [item for item in classified_paths if item[1].candidate][
            :sample_limit
        ]

    records: list[SVNFileInventory] = []
    layer_counts = _initial_layer_counts(profile.layout)
    reason_counts = _initial_reason_counts()
    candidate_count = 0
    matched_count = 0
    out_of_scope_count = 0
    primary_resolved_count = 0
    primary_unresolved_count = 0
    readable_count = 0
    read_error_count = 0
    decode_error_count = 0

    if walk_errors:
        reason_counts[READ_ERROR] += walk_errors
        read_error_count += walk_errors

    for processed, (file_path, classification) in enumerate(classified_paths, start=1):
        relative_path = file_path.relative_to(root).as_posix()
        file_size: int | None = None
        read_status = NOT_ATTEMPTED
        unresolved_reason = classification.unresolved_reason
        if classification.candidate:
            candidate_count += 1
        elif classification.out_of_scope:
            out_of_scope_count += 1

        if classification.matched_program_file:
            matched_count += 1
            if classification.layer is not None:
                layer_counts[classification.layer] = (
                    layer_counts.get(classification.layer, 0) + 1
                )
            if classification.primary_target_resolved:
                primary_resolved_count += 1
            else:
                primary_unresolved_count += 1

            try:
                file_size = file_path.stat().st_size
            except OSError:
                file_size = None

            read_result = read_python_source(file_path)
            read_status = read_result.read_status
            if read_status == READABLE:
                readable_count += 1
            elif read_status == READ_ERROR:
                read_error_count += 1
                reason_counts[READ_ERROR] += 1
                unresolved_reason = READ_ERROR
            elif read_status == DECODE_ERROR:
                decode_error_count += 1
                reason_counts[DECODE_ERROR] += 1
                unresolved_reason = DECODE_ERROR
        elif classification.candidate:
            if unresolved_reason is not None:
                reason_counts[unresolved_reason] += 1
            if not classification.primary_target_resolved:
                primary_unresolved_count += 1

        records.append(
            SVNFileInventory(
                profile_name=profile.name,
                environment=profile.environment,
                relative_path=relative_path,
                filename=file_path.name,
                layout=profile.layout,
                layer=classification.layer,
                declared_primary_target=classification.declared_primary_target
                if classification.matched_program_file
                else None,
                file_size=file_size,
                read_status=read_status,
                matched_program_file=classification.matched_program_file,
                directory_pattern_valid=classification.directory_pattern_valid,
                unresolved_reason=unresolved_reason,
                candidate=classification.candidate,
                out_of_scope=classification.out_of_scope,
            )
        )

        if progress_callback is not None and processed % progress_interval == 0:
            progress_callback(
                profile.name,
                processed,
                primary_resolved_count,
                read_error_count + decode_error_count,
            )

    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    return SVNScanResult(
        profile_name=profile.name,
        environment=profile.environment,
        layout=profile.layout,
        status=_scan_status(
            len(classified_paths),
            matched_count,
            read_error_count,
            decode_error_count,
            candidate=candidate_count,
        ),
        scanned_python_files=len(classified_paths),
        candidate_program_files=candidate_count,
        matched_program_files=matched_count,
        out_of_scope_python_files=out_of_scope_count,
        unmatched_python_files=len(classified_paths) - matched_count,
        primary_target_resolved=primary_resolved_count,
        primary_target_unresolved=primary_unresolved_count,
        readable_files=readable_count,
        read_errors=read_error_count,
        decode_errors=decode_error_count,
        layer_counts=layer_counts,
        unresolved_reasons=reason_counts,
        elapsed_ms=elapsed_ms,
        records=tuple(records),
    )


def _safe_sample(record: SVNFileInventory) -> dict[str, object]:
    """Return shape evidence without paths, filenames, targets, or source."""

    return {
        "layout": record.layout,
        "layer": record.layer,
        "candidate": record.candidate,
        "out_of_scope": record.out_of_scope,
        "directory_pattern_valid": record.directory_pattern_valid,
        "primary_target_resolved": record.primary_target_resolved,
        "read_status": record.read_status,
        "unresolved_reason": record.unresolved_reason,
    }


def build_svn_verification_report(
    results: Iterable[SVNScanResult],
    *,
    sample_only: bool = False,
    sample_limit: int = 20,
) -> dict[str, object]:
    """Build a sanitized JSON-ready report."""

    if sample_limit < 0:
        raise ValueError("sample_limit must be non-negative")

    profiles: list[dict[str, object]] = []
    for result in results:
        sample = [_safe_sample(record) for record in result.records[:sample_limit]]
        candidate_count = result.candidate_program_files
        if candidate_count == 0:
            # Results constructed by pre-v2 callers do not have the appended
            # field; their primary counters still provide the candidate total.
            candidate_count = (
                result.primary_target_resolved + result.primary_target_unresolved
            )
        profiles.append(
            {
                "profile_name": result.profile_name,
                "environment": result.environment,
                "layout": result.layout,
                "status": result.status,
                "scanned_python_files": result.scanned_python_files,
                "candidate_program_files": candidate_count,
                "matched_program_files": result.matched_program_files,
                "out_of_scope_python_files": result.out_of_scope_python_files,
                "unmatched_python_files": result.unmatched_python_files,
                "primary_target_resolved": result.primary_target_resolved,
                "primary_target_unresolved": result.primary_target_unresolved,
                "primary_resolved_rate": result.primary_resolved_rate,
                "readable_files": result.readable_files,
                "read_errors": result.read_errors,
                "decode_errors": result.decode_errors,
                "layer_counts": dict(result.layer_counts),
                "unresolved_reasons": dict(result.unresolved_reasons),
                "sample": sample,
                "elapsed_ms": result.elapsed_ms,
            }
        )
    return {
        "report_type": "svn_source_verification",
        "report_version": SVN_REPORT_VERSION,
        "sample_only": sample_only,
        "profiles": profiles,
    }


# Descriptive alias for callers that prefer the shorter name.
build_verification_report = build_svn_verification_report


def write_json_report(report: Mapping[str, object], output_path: str | Path) -> None:
    """Write an already-sanitized report, creating its parent directory."""

    path = Path(output_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")


__all__ = [
    "CONFIG_ERROR",
    "DECODE_ERROR",
    "DWF_LAYOUT",
    "DWF_LAYER",
    "GRANDPARENT_MISMATCH",
    "IGNORED_DIRECTORY_NAMES",
    "INVALID_LAYOUT",
    "INVALID_PROGRAM_DIRECTORY",
    "NO_MATCHED_FILES",
    "NOT_ATTEMPTED",
    "NOT_PYTHON",
    "OUT_OF_SCOPE",
    "PATH_LAYOUT_ERROR",
    "PROCESSING_LAYERS",
    "PROCESSING_LAYOUT",
    "READABLE",
    "READ_ERROR",
    "REQUIRED_PROCESSING_LAYERS",
    "ROOT_NOT_DIRECTORY",
    "ROOT_NOT_FOUND",
    "SUCCESS",
    "SVN_REPORT_VERSION",
    "PathClassification",
    "SVNFileInventory",
    "SVNInventoryConfigError",
    "SVNProfile",
    "SVNScanResult",
    "SourceReadResult",
    "UNSUPPORTED_LAYER",
    "build_svn_verification_report",
    "build_verification_report",
    "classify_program_path",
    "classify_svn_program_path",
    "derive_primary_target_from_program_path",
    "load_svn_profiles",
    "read_python_source",
    "scan_svn_profile",
    "write_json_report",
]
