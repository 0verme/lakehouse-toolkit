"""Shared DWS ``TIMESTAMP WITH TIME ZONE`` boundary contract.

Huawei Gauss200 JDBC can bind Python strings as ``VARCHAR`` and can expose a
DWS timestamptz as a JVM-local wall-clock value when the driver returns the raw
JDBC timestamp object.  Keep both boundaries explicit and parse the textual
representation without inventing a timezone for naive values.
"""

from __future__ import annotations

import re
from datetime import datetime

from shared.config.env import safe_identifier

TIMESTAMPTZ_PARAM_SQL = "CAST(? AS TIMESTAMP WITH TIME ZONE)"
_TIMESTAMP_OFFSET_SUFFIX_RE = re.compile(
    r"(?P<sign>[+-])(?P<hours>\d{2})"
    r"(?:(?::(?P<colon_minutes>\d{2}))|(?P<compact_minutes>\d{2}))?$"
)
_TIMESTAMP_TIME_PREFIX_RE = re.compile(r"(?:T| )\d{2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?$")
_TIMESTAMP_FRACTION_SUFFIX_RE = re.compile(
    r"(?:T| )\d{2}:\d{2}:\d{2}(?P<separator>[.,])"
    r"(?P<fraction>\d+)$"
)


def dws_timestamp_projection(column: str, alias: str | None = None) -> str:
    """Return a validated text projection for one DWS timestamptz column."""

    safe_column = safe_identifier(column, "DWS timestamp column")
    default_alias = safe_column.rsplit(".", 1)[-1]
    safe_alias = safe_identifier(alias or default_alias, "DWS timestamp alias")
    return f"CAST({safe_column} AS VARCHAR(128)) AS {safe_alias}"


def _require_aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return value


def dws_timestamp_param(
    value: datetime | None, field_name: str = "timestamp"
) -> str | None:
    """Serialize an aware datetime as ISO text for an explicit DWS SQL cast."""

    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None")
    return _require_aware(value, field_name).isoformat()


def _normalize_fractional_seconds(value: str) -> str:
    match = _TIMESTAMP_FRACTION_SUFFIX_RE.search(value)
    if match is None:
        return value
    fraction = match.group("fraction")
    if not 1 <= len(fraction) <= 6:
        raise ValueError("timestamp fractional seconds must contain 1 to 6 digits")
    return f"{value[: match.start('separator')]}.{fraction.ljust(6, '0')}"


def normalize_dws_timestamp_text(value: str) -> str:
    """Normalize DWS offset and fractional-second spellings for Python 3.10."""

    if not isinstance(value, str):
        raise TypeError("DWS timestamp text must be a string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    offset_match = _TIMESTAMP_OFFSET_SUFFIX_RE.search(text)
    if offset_match is None:
        return _normalize_fractional_seconds(text)

    base = text[: offset_match.start()]
    if _TIMESTAMP_TIME_PREFIX_RE.search(base) is None:
        return _normalize_fractional_seconds(text)

    minutes = (
        offset_match.group("colon_minutes")
        or offset_match.group("compact_minutes")
        or "00"
    )
    offset = f"{offset_match.group('sign')}{offset_match.group('hours')}:{minutes}"
    return _normalize_fractional_seconds(base) + offset


def parse_dws_timestamp(value: object, field_name: str = "timestamp") -> datetime:
    """Parse DWS timestamp text and reject values without an explicit offset."""

    if isinstance(value, datetime):
        return _require_aware(value, field_name)
    if value is None:
        raise ValueError(f"{field_name} must not be NULL")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} is not a valid timestamp")
    try:
        parsed = datetime.fromisoformat(normalize_dws_timestamp_text(text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid timestamp") from exc
    return _require_aware(parsed, field_name)


__all__ = [
    "TIMESTAMPTZ_PARAM_SQL",
    "dws_timestamp_param",
    "dws_timestamp_projection",
    "normalize_dws_timestamp_text",
    "parse_dws_timestamp",
]
