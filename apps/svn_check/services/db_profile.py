from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parents[3]
CONFIG_PATH = ROOT_DIR / "configs" / "audit_datasource.local.yaml"
EXAMPLE_CONFIG_PATH = ROOT_DIR / "configs" / "audit_datasource.example.yaml"


def load_audit_datasource_config() -> dict:
    config_path = CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_active_audit_profile_name() -> str:
    config = load_audit_datasource_config()
    resolver = config.get("resolver", {})
    profiles = config.get("profiles", {})

    env_var = resolver.get("env_var", "AUDIT_DB_PROFILE")
    env_profile_name = str(os.getenv(env_var, "") or "").strip()
    if env_profile_name:
        if env_profile_name not in profiles:
            raise KeyError(f"audit datasource profile not found: {env_profile_name}")
        return env_profile_name

    app_env_var = resolver.get("app_env_var", "APP_ENV")
    app_env_value = str(os.getenv(app_env_var, "") or "").strip().lower()
    default_by_env = resolver.get("default_by_env", {})
    if app_env_value:
        profile_name = default_by_env.get(app_env_value)
        if profile_name:
            if profile_name not in profiles:
                raise KeyError(f"audit datasource profile not found: {profile_name}")
            return profile_name

    fallback = resolver.get("fallback", "")
    if fallback not in profiles:
        raise KeyError(f"audit datasource fallback profile not found: {fallback}")
    return fallback


def get_active_audit_profile() -> dict:
    config = load_audit_datasource_config()
    profiles = config.get("profiles", {})
    profile_name = get_active_audit_profile_name()
    if profile_name not in profiles:
        raise KeyError(f"audit datasource profile not found: {profile_name}")
    profile = dict(profiles[profile_name] or {})
    profile["name"] = profile_name
    return profile


def get_active_backend() -> str:
    profile = get_active_audit_profile()
    backend = str(profile.get("backend", "") or "").strip()
    if not backend:
        raise KeyError(f"audit datasource backend not configured: {profile['name']}")
    return backend


def is_postgres_profile() -> bool:
    return get_active_backend() == "postgres_native"


def is_gauss_jdbc_profile() -> bool:
    return get_active_backend() == "gauss_jdbc"
