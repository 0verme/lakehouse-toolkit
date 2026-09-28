from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as stream:
        data = yaml.safe_load(stream) or {}
    return data if isinstance(data, dict) else {}


def load_tool_configuration(
    config_path: str | Path,
    local_config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Load the public tool registry with local overrides and additions.

    A local tool with a matching name overrides its public entry. A local tool
    with a new name is appended, allowing private deployment-only tools to be
    registered without copying machine-specific settings into the public YAML.
    """

    config = _read_yaml_mapping(Path(config_path))
    if local_config_path is None:
        return config

    local_config = _read_yaml_mapping(Path(local_config_path))
    merged_config = dict(config)
    merged_config.update(
        {key: value for key, value in local_config.items() if key != "tools"}
    )

    base_tools = config.get("tools", [])
    local_tools = local_config.get("tools", [])
    if not isinstance(base_tools, list):
        base_tools = []
    if not isinstance(local_tools, list):
        local_tools = []

    local_by_name = {
        str(tool["name"]): tool
        for tool in local_tools
        if isinstance(tool, dict) and tool.get("name")
    }
    merged_tools: list[dict[str, Any]] = []
    base_names: set[str] = set()
    for tool in base_tools:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name", ""))
        if name:
            base_names.add(name)
        merged_tools.append({**tool, **local_by_name.get(name, {})})

    merged_tools.extend(
        dict(tool)
        for tool in local_tools
        if isinstance(tool, dict)
        and tool.get("name")
        and str(tool["name"]) not in base_names
    )
    merged_config["tools"] = merged_tools
    return merged_config
