from __future__ import annotations

import os
from urllib.parse import urlencode

DEFAULT_PORTAL_BASE_URL = "http://localhost:5099"


def get_portal_base_url():
    configured_base_url = os.getenv("ASSET_PORTAL_BASE_URL", "").strip()
    return configured_base_url or DEFAULT_PORTAL_BASE_URL


def _build_url(path, params=None):
    query = urlencode({key: value for key, value in (params or {}).items() if value})
    base_url = get_portal_base_url().rstrip("/")
    if query:
        return f"{base_url}{path}?{query}"
    return f"{base_url}{path}"


def build_root_management_link(root_word):
    return _build_url("/root-management", {"q": root_word})


def build_data_warehouse_link(table_name):
    return _build_url("/data-warehouse", {"q": table_name})


def build_portal_link(issue):
    if issue.issue_type == "ROOT_MISSING":
        return build_root_management_link(issue.root_word)
    if issue.issue_type == "ASSET_TABLE_REVIEW":
        qualified_table_name = (
            ".".join(
                [value for value in (issue.schema_name, issue.table_name) if value]
            )
            or issue.table_name
        )
        return build_data_warehouse_link(qualified_table_name)
    return get_portal_base_url().rstrip("/")
