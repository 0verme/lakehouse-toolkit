from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


@dataclass(frozen=True)
class AuditAssetIssue:
    issue_key: str
    issue_hash_key: str
    issue_type: str
    issue_title: str
    issue_desc: str
    asset_type: str
    source_module: str
    source_file: str
    severity: str
    suggestion: str
    portal_module: str
    action_label: str
    schema_name: str = ""
    table_name: str = ""
    field_name: str = ""
    root_word: str = ""
    portal_url: str = ""


def _clean_text(value, upper=False):
    if value is None:
        return ""
    text = str(value).strip()
    return text.upper() if upper else text


def build_issue_key(
    issue_type,
    source_module,
    source_file,
    schema_name="",
    table_name="",
    field_name="",
    root_word="",
):
    parts = [
        _clean_text(issue_type, upper=True),
        _clean_text(source_module, upper=True),
        _clean_text(source_file),
        _clean_text(schema_name, upper=True),
        _clean_text(table_name, upper=True),
        _clean_text(field_name, upper=True),
        _clean_text(root_word, upper=True),
    ]
    return "|".join(parts)


def build_issue_hash_key(issue_key):
    return sha256(issue_key.encode("utf-8")).hexdigest()[:12]


def create_audit_asset_issue(
    *,
    issue_type,
    issue_title,
    issue_desc,
    asset_type,
    source_module,
    source_file,
    severity,
    suggestion,
    portal_module,
    action_label,
    schema_name="",
    table_name="",
    field_name="",
    root_word="",
    portal_url="",
):
    issue_key = build_issue_key(
        issue_type=issue_type,
        source_module=source_module,
        source_file=source_file,
        schema_name=schema_name,
        table_name=table_name,
        field_name=field_name,
        root_word=root_word,
    )
    return AuditAssetIssue(
        issue_key=issue_key,
        issue_hash_key=build_issue_hash_key(issue_key),
        issue_type=_clean_text(issue_type, upper=True),
        issue_title=_clean_text(issue_title),
        issue_desc=_clean_text(issue_desc),
        asset_type=_clean_text(asset_type),
        source_module=_clean_text(source_module),
        source_file=_clean_text(source_file),
        severity=_clean_text(severity),
        suggestion=_clean_text(suggestion),
        portal_module=_clean_text(portal_module),
        action_label=_clean_text(action_label),
        schema_name=_clean_text(schema_name, upper=True),
        table_name=_clean_text(table_name, upper=True),
        field_name=_clean_text(field_name, upper=True),
        root_word=_clean_text(root_word, upper=True),
        portal_url=_clean_text(portal_url),
    )


def dedupe_issues(issues):
    deduped = []
    seen = set()
    for issue in issues or []:
        if issue.issue_key in seen:
            continue
        seen.add(issue.issue_key)
        deduped.append(issue)
    return deduped
