from __future__ import annotations

import os
import re
from pathlib import Path

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def required_env(name: str) -> str:
    """读取必填环境变量；缺失时以不含 Secret 的明确错误失败。"""
    value = os.getenv(name, "")
    if not value.strip():
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


def safe_identifier(value: str, label: str = "identifier") -> str:
    """校验 SQL 标识符，避免把未经验证的配置拼接进 SQL。"""
    text = str(value or "").strip()
    parts = text.split(".")
    if not text or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid SQL {label}: {value!r}")
    return ".".join(parts)


def metadata_table(key: str, default_table: str) -> str:
    """返回元数据表名，默认只指向公开 demo model。"""
    env_name = f"PYTOOLS_METADATA_{key.upper()}_TABLE"
    configured = os.getenv(env_name, "").strip()
    if configured:
        return safe_identifier(configured, env_name)

    schema = os.getenv("PYTOOLS_METADATA_SCHEMA", "demo_meta").strip() or "demo_meta"
    return safe_identifier(f"{schema}.{default_table}", "metadata table")


def configured_path(env_name: str, default: str | Path) -> Path:
    """读取路径配置；默认路径始终位于项目的公开 runtime/demo 范围内。"""
    return Path(os.getenv(env_name, str(default))).expanduser()
