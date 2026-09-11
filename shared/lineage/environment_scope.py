"""Resolve a user-facing environment to its formal lineage read scope.

The web UI deliberately exposes only ``environment``.  SQL and schedule
source profiles, as well as the DWS connection profile, remain deployment
configuration and are resolved here rather than in the presentation layer.
Public example configuration contains placeholders only; real deployments
may provide ``configs/lineage_providers.local.yaml``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from .providers import (
    CONFIG_PATH as LINEAGE_PROVIDER_CONFIG_PATH,
    EXAMPLE_CONFIG_PATH as LINEAGE_PROVIDER_EXAMPLE_CONFIG_PATH,
)

CONFIG_PATH = LINEAGE_PROVIDER_CONFIG_PATH
EXAMPLE_CONFIG_PATH = LINEAGE_PROVIDER_EXAMPLE_CONFIG_PATH

UNKNOWN_LINEAGE_ENVIRONMENT = "UNKNOWN_LINEAGE_ENVIRONMENT"
DISABLED_LINEAGE_ENVIRONMENT = "DISABLED_LINEAGE_ENVIRONMENT"
LINEAGE_SCOPE_CONFIG_NOT_FOUND = "LINEAGE_SCOPE_CONFIG_NOT_FOUND"
LINEAGE_SCOPE_CONFIG_INVALID = "LINEAGE_SCOPE_CONFIG_INVALID"


class LineageEnvironmentScopeError(ValueError):
    """Safe, coded error raised by the environment scope contract."""

    def __init__(self, code: str, *, reason: str | None = None) -> None:
        self.code = code
        self.reason = reason
        message = code if not reason else f"{code}: {reason}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class LineageEnvironmentScope:
    """The complete backend scope for one selectable environment."""

    name: str
    environment: str
    sql_source_profile: str
    schedule_source_profile: str
    label: str
    dws_profile: str
    enabled: bool = True

    _TEXT_FIELDS: ClassVar[tuple[str, ...]] = (
        "name",
        "environment",
        "sql_source_profile",
        "schedule_source_profile",
        "label",
        "dws_profile",
    )

    def __post_init__(self) -> None:
        for field_name in self._TEXT_FIELDS:
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be a boolean")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "LineageEnvironmentScope":
        """Build one scope from the public/local YAML contract."""

        if not isinstance(raw, Mapping):
            raise ValueError("scope must be a mapping")

        def required_text(field_name: str) -> str:
            value = raw.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"scope field {field_name} must be a non-empty string")
            return value

        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("scope field enabled must be a boolean")

        return cls(
            name=required_text("name"),
            environment=required_text("environment"),
            sql_source_profile=required_text("sql_source_profile"),
            schedule_source_profile=required_text("schedule_source_profile"),
            label=required_text("label"),
            dws_profile=required_text("dws_profile"),
            enabled=enabled,
        )


class LineageEnvironmentScopeResolver:
    """Resolve configured environments without exposing profile selection to callers."""

    def __init__(self, scopes: Iterable[LineageEnvironmentScope]) -> None:
        values = tuple(scopes)
        if any(not isinstance(scope, LineageEnvironmentScope) for scope in values):
            raise TypeError("scopes must contain LineageEnvironmentScope values")

        by_environment: dict[str, LineageEnvironmentScope] = {}
        names: set[str] = set()
        for scope in values:
            if scope.environment in by_environment:
                raise ValueError("scope environments must be unique")
            if scope.name in names:
                raise ValueError("scope names must be unique")
            by_environment[scope.environment] = scope
            names.add(scope.name)

        self._scopes = values
        self._by_environment = by_environment

    @property
    def scopes(self) -> tuple[LineageEnvironmentScope, ...]:
        """Return all configured scopes, including disabled entries."""

        return self._scopes

    def enabled_scopes(self) -> tuple[LineageEnvironmentScope, ...]:
        """Return only scopes enabled for UI and production batch consumers."""

        return tuple(scope for scope in self._scopes if scope.enabled)

    def resolve(self, environment: str) -> LineageEnvironmentScope:
        """Resolve an exact environment value and reject unknown/disabled ones."""

        if not isinstance(environment, str) or not environment.strip():
            raise LineageEnvironmentScopeError(UNKNOWN_LINEAGE_ENVIRONMENT)
        normalized = environment.strip()
        scope = self._by_environment.get(normalized)
        if scope is None:
            raise LineageEnvironmentScopeError(UNKNOWN_LINEAGE_ENVIRONMENT)
        if not scope.enabled:
            raise LineageEnvironmentScopeError(DISABLED_LINEAGE_ENVIRONMENT)
        return scope


def load_lineage_environment_scopes(
    config_path: str | Path | None = None,
) -> tuple[LineageEnvironmentScope, ...]:
    """Load scopes from the provider config, falling back to its public example."""

    try:
        import yaml  # pyright: ignore[reportMissingModuleSource]
    except ModuleNotFoundError as exc:
        if exc.name == "yaml":
            raise LineageEnvironmentScopeError(
                LINEAGE_SCOPE_CONFIG_INVALID,
                reason="PyYAML dependency is unavailable",
            ) from exc
        raise

    path = (
        Path(config_path).expanduser()
        if config_path is not None
        else (CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH)
    )
    try:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise LineageEnvironmentScopeError(LINEAGE_SCOPE_CONFIG_NOT_FOUND) from exc
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise LineageEnvironmentScopeError(LINEAGE_SCOPE_CONFIG_INVALID) from exc

    if not isinstance(data, Mapping):
        raise LineageEnvironmentScopeError(
            LINEAGE_SCOPE_CONFIG_INVALID,
            reason="root must be a mapping",
        )
    raw_scopes = data.get("scopes")
    if not isinstance(raw_scopes, list):
        raise LineageEnvironmentScopeError(
            LINEAGE_SCOPE_CONFIG_INVALID,
            reason="scopes must be a list",
        )

    scopes: list[LineageEnvironmentScope] = []
    for index, raw_scope in enumerate(raw_scopes):
        try:
            scopes.append(LineageEnvironmentScope.from_mapping(raw_scope))
        except (TypeError, ValueError) as exc:
            raise LineageEnvironmentScopeError(
                LINEAGE_SCOPE_CONFIG_INVALID,
                reason=f"scope[{index}] is invalid",
            ) from exc

    try:
        return tuple(LineageEnvironmentScopeResolver(scopes).scopes)
    except (TypeError, ValueError) as exc:
        raise LineageEnvironmentScopeError(
            LINEAGE_SCOPE_CONFIG_INVALID,
            reason="scope names and environments must be unique",
        ) from exc


def load_lineage_environment_scope_resolver(
    config_path: str | Path | None = None,
) -> LineageEnvironmentScopeResolver:
    """Load the configured environment resolver."""

    return LineageEnvironmentScopeResolver(
        load_lineage_environment_scopes(config_path=config_path)
    )


__all__ = [
    "CONFIG_PATH",
    "DISABLED_LINEAGE_ENVIRONMENT",
    "EXAMPLE_CONFIG_PATH",
    "LINEAGE_SCOPE_CONFIG_INVALID",
    "LINEAGE_SCOPE_CONFIG_NOT_FOUND",
    "LineageEnvironmentScope",
    "LineageEnvironmentScopeError",
    "LineageEnvironmentScopeResolver",
    "UNKNOWN_LINEAGE_ENVIRONMENT",
    "load_lineage_environment_scope_resolver",
    "load_lineage_environment_scopes",
]
