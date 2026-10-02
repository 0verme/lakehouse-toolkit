from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlsplit

import requests

from .models import MappingItem

DAP_IMPORT_PATH = "/api/field-mappings/import"
DAP_SYSTEMS_PATH = "/api/upstreams/systems"
DAP_MAX_ITEMS = 500
DAP_MAX_FIELDS_PER_ITEM = 1_000
DEFAULT_BATCH_SIZE = 100
DEFAULT_TIMEOUT = (5.0, 30.0)


class FieldMappingApiError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


def validate_mapping_item(item: dict[str, object]) -> None:
    source_system_id = item.get("sourceSystemId")
    if (
        not isinstance(source_system_id, int)
        or isinstance(source_system_id, bool)
        or source_system_id <= 0
    ):
        raise ValueError("sourceSystemId must be a positive integer")
    for key, maximum in (("sourceTable", 128), ("targetTable", 128)):
        value = item.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > maximum:
            raise ValueError(f"{key} must contain 1..{maximum} characters")
    if item.get("targetLayer") != "DWF":
        raise ValueError("targetLayer must be DWF")
    fields = item.get("fields")
    if not isinstance(fields, list) or not 1 <= len(fields) <= DAP_MAX_FIELDS_PER_ITEM:
        raise ValueError("fields must contain 1..1000 items")
    seen: set[tuple[str, str]] = set()
    for index, mapping in enumerate(fields):
        if not isinstance(mapping, dict):
            raise TypeError(f"fields[{index}] must be an object")
        for key, maximum in (
            ("sourceField", 128),
            ("targetField", 128),
            ("mappingRule", 64),
        ):
            value = mapping.get(key)
            if not isinstance(value, str) or not value.strip() or len(value) > maximum:
                raise ValueError(
                    f"fields[{index}].{key} must contain 1..{maximum} characters"
                )
        order = mapping.get("fieldOrder")
        if not isinstance(order, int) or isinstance(order, bool) or order < 1:
            raise ValueError(f"fields[{index}].fieldOrder must be a positive integer")
        for key, maximum in (("sourceType", 128), ("sourceComment", 1_000)):
            value = mapping.get(key)
            if value is not None and (
                not isinstance(value, str) or len(value) > maximum
            ):
                raise ValueError(f"fields[{index}].{key} exceeds contract length")
        identity = (
            str(mapping["sourceField"]).casefold(),
            str(mapping["targetField"]).casefold(),
        )
        if identity in seen:
            raise ValueError(f"fields[{index}] duplicates sourceField/targetField")
        seen.add(identity)


def build_import_payload(
    items: Sequence[MappingItem], *, dry_run: bool = False
) -> dict[str, object]:
    if not items:
        raise ValueError("at least one mapping item is required")
    if len(items) > DAP_MAX_ITEMS:
        raise ValueError("DAP accepts at most 500 mapping items per request")
    identities: set[tuple[int, str, str]] = set()
    payload_items: list[dict[str, object]] = []
    for item in items:
        if item.dap_identity in identities:
            raise ValueError(
                "duplicate sourceSystemId/sourceTable/targetTable identity in request"
            )
        identities.add(item.dap_identity)
        payload = item.to_contract()
        validate_mapping_item(payload)
        payload_items.append(payload)
    return {"mode": "upsert", "dryRun": dry_run, "items": payload_items}


def split_batches(
    items: Sequence[MappingItem], batch_size: int = DEFAULT_BATCH_SIZE
) -> list[list[MappingItem]]:
    if not 1 <= batch_size <= DAP_MAX_ITEMS:
        raise ValueError("batch size must be between 1 and 500")
    return [
        list(items[index : index + batch_size])
        for index in range(0, len(items), batch_size)
    ]


def is_local_write_url(base_url: str) -> bool:
    try:
        parsed = urlsplit((base_url or "").strip())
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.hostname.casefold() in {"localhost", "127.0.0.1"}
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment
    )


class FieldMappingApiClient:
    """Authenticated DAP client for read-only discovery and upsert-only import."""

    def __init__(
        self,
        base_url: str,
        *,
        session_cookie: str = "",
        timeout: tuple[float, float] = DEFAULT_TIMEOUT,
        max_retries: int = 2,
        retry_backoff: float = 0.5,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        parsed = urlsplit((base_url or "").strip().rstrip("/"))
        if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
            raise ValueError("DAP base URL must be an HTTP(S) URL with a host")
        if (
            parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(
                "DAP base URL must not contain credentials, path, query or fragment"
            )
        if min(timeout) <= 0 or max_retries < 0 or retry_backoff < 0:
            raise ValueError("invalid DAP timeout or retry settings")
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = session if session is not None else requests.Session()
        self._owns_session = session is None
        if parsed.hostname.casefold() in {"localhost", "127.0.0.1"} and hasattr(
            self.session, "trust_env"
        ):
            self.session.trust_env = False
        self._cookie = session_cookie.strip()
        self._sleep = sleep
        if self._cookie:
            self.session.headers.update({"Cookie": self._cookie})
        self.session.headers.update({"Accept": "application/json"})
        self._authenticated = bool(self._cookie)

    def close(self) -> None:
        if self._owns_session:
            self.session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def redact(self, text: str) -> str:
        secrets = {self._cookie} if self._cookie else set()
        for cookie in getattr(self.session, "cookies", ()):
            value = getattr(cookie, "value", "")
            if value:
                secrets.add(str(value))
        for segment in self._cookie.split(";"):
            if "=" in segment:
                value = segment.split("=", 1)[1].strip()
                if value:
                    secrets.add(value)
        redacted = text
        for secret in secrets:
            redacted = redacted.replace(secret, "[REDACTED]")
        return redacted

    def login(self, username: str, password: str) -> dict[str, Any]:
        if not username.strip() or not password:
            raise ValueError("DAP login username and password are required")
        try:
            response = self.session.post(
                f"{self.base_url}/api/auth/login",
                json={"username": username.strip(), "password": password},
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.exceptions.RequestException as cause:
            raise FieldMappingApiError(
                "LOGIN_NETWORK_ERROR", "DAP login request failed"
            ) from cause
        if response.status_code >= 400:
            # Never relay a login response body: authentication handlers may echo
            # request details in deployment-specific errors.
            raise FieldMappingApiError(
                f"HTTP_{response.status_code}", "DAP login was rejected"
            )
        self._authenticated = True
        return self.current_user()

    def current_user(self) -> dict[str, Any]:
        response = self._get(f"{self.base_url}/api/auth/me")
        if response.status_code >= 400:
            raise self._http_error(response)
        return self._json_object(response, "DAP auth/me")

    def get_upstream_systems(self) -> dict[str, Any]:
        page = 1
        all_items: list[dict[str, Any]] = []
        total: int | None = None
        page_size = 500
        while total is None or len(all_items) < total:
            response = self._get(
                f"{self.base_url}{DAP_SYSTEMS_PATH}",
                params={"page": page, "pageSize": page_size},
            )
            if response.status_code >= 400:
                raise self._http_error(response)
            body = self._json_object(response, "DAP upstream systems")
            items = body.get("items")
            if not isinstance(items, list) or any(
                not isinstance(row, dict) for row in items
            ):
                raise FieldMappingApiError(
                    "INVALID_RESPONSE_CONTRACT",
                    "DAP systems response has no items array",
                )
            all_items.extend(items)
            raw_total = body.get("total")
            if isinstance(raw_total, int) and not isinstance(raw_total, bool):
                total = raw_total
            effective_size = body.get("pageSize")
            if (
                not isinstance(effective_size, int)
                or isinstance(effective_size, bool)
                or effective_size < 1
            ):
                effective_size = page_size
            if not items or (total is not None and len(all_items) >= total):
                break
            if total is None and len(items) < effective_size:
                break
            page += 1
            if page > 10_000:
                raise FieldMappingApiError(
                    "INVALID_RESPONSE_CONTRACT",
                    "DAP systems pagination did not terminate",
                )
        return {
            "items": all_items,
            "total": total if total is not None else len(all_items),
        }

    def import_mappings(self, payload: dict[str, object]) -> dict[str, Any]:
        if payload.get("mode") != "upsert":
            raise ValueError("DAP import mode must be upsert")
        if payload.get("dryRun") is not True:
            if not is_local_write_url(self.base_url):
                raise ValueError(
                    "real DAP writes are restricted to localhost/127.0.0.1"
                )
            # Environment proxy variables could otherwise send a loopback URL to
            # a remote proxy; real writes must use a direct local connection.
            if hasattr(self.session, "trust_env"):
                self.session.trust_env = False
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(
                    f"{self.base_url}{DAP_IMPORT_PATH}",
                    json=payload,
                    timeout=self.timeout,
                    allow_redirects=False,
                )
            except requests.exceptions.Timeout:
                error = FieldMappingApiError(
                    "CONNECTION_TIMEOUT", "DAP request timed out", retryable=True
                )
            except requests.exceptions.SSLError as cause:
                raise FieldMappingApiError(
                    "TLS_ERROR", "DAP TLS connection failed"
                ) from cause
            except requests.exceptions.ConnectionError:
                error = FieldMappingApiError(
                    "NETWORK_ERROR", "DAP connection failed", retryable=True
                )
            except requests.exceptions.RequestException as cause:
                raise FieldMappingApiError(
                    "REQUEST_ERROR", "DAP request could not be completed"
                ) from cause
            else:
                if response.status_code in {502, 503, 504}:
                    error = FieldMappingApiError(
                        f"HTTP_{response.status_code}",
                        "temporary DAP server error",
                        retryable=True,
                    )
                elif response.status_code >= 400:
                    raise self._http_error(response)
                else:
                    result = self._json_object(response, "DAP import")
                    self._validate_import_response(result, payload)
                    return result
            if attempt >= self.max_retries or not error.retryable:
                raise error
            self._sleep(self.retry_backoff * (2**attempt))
        raise FieldMappingApiError("NETWORK_ERROR", "DAP request failed")

    def get_mapping_stats(self) -> dict[str, Any]:
        response = self._get(f"{self.base_url}/api/field-mappings/stats")
        if response.status_code >= 400:
            raise self._http_error(response)
        return self._json_object(response, "DAP field mapping stats")

    def get_mapping_tables(self, *, page_size: int = 500) -> list[dict[str, Any]]:
        return self._get_mapping_list("tables", page_size=page_size)

    def get_mapping_fields(self, *, page_size: int = 500) -> list[dict[str, Any]]:
        return self._get_mapping_list("fields", page_size=page_size)

    def _get_mapping_list(
        self, resource: str, *, page_size: int
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        page = 1
        total: int | None = None
        while total is None or len(result) < total:
            response = self._get(
                f"{self.base_url}/api/field-mappings/{resource}",
                params={"page": page, "pageSize": page_size},
            )
            if response.status_code >= 400:
                raise self._http_error(response)
            body = self._json_object(response, f"DAP field mappings {resource}")
            items = body.get("items")
            if not isinstance(items, list) or any(
                not isinstance(row, dict) for row in items
            ):
                raise FieldMappingApiError(
                    "INVALID_RESPONSE_CONTRACT",
                    "DAP mapping list response has no items array",
                )
            result.extend(items)
            raw_total = body.get("total")
            if isinstance(raw_total, int) and not isinstance(raw_total, bool):
                total = raw_total
            effective_size = body.get("pageSize")
            if (
                not isinstance(effective_size, int)
                or isinstance(effective_size, bool)
                or effective_size < 1
            ):
                effective_size = page_size
            if not items or (total is not None and len(result) >= total):
                break
            if total is None and len(items) < effective_size:
                break
            page += 1
            if page > 100_000:
                raise FieldMappingApiError(
                    "INVALID_RESPONSE_CONTRACT",
                    "DAP mapping pagination did not terminate",
                )
        return result

    def _get(self, url: str, *, params: dict[str, int] | None = None) -> Any:
        try:
            return self.session.get(
                url, params=params, timeout=self.timeout, allow_redirects=False
            )
        except requests.exceptions.SSLError as cause:
            raise FieldMappingApiError(
                "TLS_ERROR", "DAP TLS connection failed"
            ) from cause
        except requests.exceptions.Timeout as cause:
            raise FieldMappingApiError(
                "CONNECTION_TIMEOUT", "DAP GET request timed out"
            ) from cause
        except requests.exceptions.ConnectionError as cause:
            raise FieldMappingApiError(
                "NETWORK_ERROR", "DAP connection failed"
            ) from cause
        except requests.exceptions.RequestException as cause:
            raise FieldMappingApiError(
                "REQUEST_ERROR", "DAP GET request failed"
            ) from cause

    def _http_error(self, response: Any) -> FieldMappingApiError:
        code = f"HTTP_{response.status_code}"
        message = "DAP rejected the request"
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            code = self.redact(str(body["error"].get("code") or code))
            message = self.redact(str(body["error"].get("message") or message))
        return FieldMappingApiError(code, message)

    def _json_object(self, response: Any, label: str) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError as error:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_JSON", f"{label} returned invalid JSON"
            ) from error
        if not isinstance(data, dict):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", f"{label} response must be an object"
            )
        return data

    @staticmethod
    def _validate_import_response(
        data: dict[str, Any], payload: dict[str, object]
    ) -> None:
        if data.get("mode") != "upsert" or data.get("dryRun") is not bool(
            payload.get("dryRun", False)
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP mode/dryRun response did not match request",
            )
        summary = data.get("summary")
        items = data.get("items")
        required = (
            "received",
            "created",
            "updated",
            "unchanged",
            "failed",
            "fieldCount",
        )
        if not isinstance(summary, dict) or any(
            not isinstance(summary.get(name), int)
            or isinstance(summary.get(name), bool)
            for name in required
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response summary is incomplete"
            )
        if summary["received"] != len(payload.get("items", [])) or not isinstance(
            items, list
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response item count did not match request",
            )


def response_items(
    response: dict[str, Any], expected: Sequence[MappingItem]
) -> list[dict[str, Any]]:
    raw_items = response.get("items")
    if not isinstance(raw_items, list) or len(raw_items) != len(expected):
        raise FieldMappingApiError(
            "INVALID_RESPONSE_CONTRACT", "DAP response item count did not match request"
        )
    by_index: dict[int, dict[str, Any]] = {}
    valid_actions = {"created", "updated", "unchanged", "failed"}
    for item in raw_items:
        if not isinstance(item, dict):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item is invalid"
            )
        index = item.get("index")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or not 0 <= index < len(expected)
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item index is invalid"
            )
        if index in by_index or item.get("action") not in valid_actions:
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT", "DAP response item identity is invalid"
            )
        identity = item.get("identity")
        source_system_id = (
            identity.get("sourceSystemId", identity.get("upstreamSystemId"))
            if isinstance(identity, dict)
            else None
        )
        source_table = (
            identity.get("sourceTable") if isinstance(identity, dict) else None
        )
        target_table = (
            identity.get("targetTable") if isinstance(identity, dict) else None
        )
        wanted = expected[index]
        if (
            source_system_id != wanted.source_system_id
            or not isinstance(source_table, str)
            or source_table.casefold() != wanted.source_table.casefold()
            or (
                target_table is not None
                and str(target_table).casefold() != wanted.target_table.casefold()
            )
        ):
            raise FieldMappingApiError(
                "INVALID_RESPONSE_CONTRACT",
                "DAP response identity did not match request",
            )
        by_index[index] = item
    if len(by_index) != len(expected):
        raise FieldMappingApiError(
            "INVALID_RESPONSE_CONTRACT", "DAP response item indexes are incomplete"
        )
    return [by_index[index] for index in range(len(expected))]


def submit_batches(
    items: Sequence[MappingItem],
    client: FieldMappingApiClient,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool,
) -> dict[str, Any]:
    totals: dict[str, Any] = {
        "batches": 0,
        "received": 0,
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "failed": 0,
        "fieldCount": 0,
        "failedItems": [],
    }
    for batch in split_batches(items, batch_size):
        payload = build_import_payload(batch, dry_run=dry_run)
        totals["batches"] += 1
        totals["received"] += len(batch)
        totals["fieldCount"] += sum(len(item.fields) for item in batch)
        try:
            response = client.import_mappings(payload)
            rows = response_items(response, batch)
        except FieldMappingApiError as error:
            totals["failed"] += len(batch)
            totals["failedItems"].extend(
                {
                    "sourceSystemIdentity": item.source_system_identity,
                    "sourceSystemId": item.source_system_id,
                    "sourceTable": item.source_table,
                    "targetTable": item.target_table,
                    "errorCode": client.redact(error.code),
                    "message": client.redact(error.message),
                }
                for item in batch
            )
            continue
        for item, result in zip(batch, rows, strict=True):
            totals[result["action"]] += 1
            if result["action"] == "failed":
                error = result.get("error")
                totals["failedItems"].append(
                    {
                        "sourceSystemIdentity": item.source_system_identity,
                        "sourceSystemId": item.source_system_id,
                        "sourceTable": item.source_table,
                        "targetTable": item.target_table,
                        "errorCode": client.redact(
                            str(error.get("code", "ITEM_FAILED"))
                        )
                        if isinstance(error, dict)
                        else "ITEM_FAILED",
                        "message": client.redact(
                            str(error.get("message", "DAP marked item failed"))
                        )
                        if isinstance(error, dict)
                        else "DAP marked item failed",
                    }
                )
    return totals


__all__ = [
    "DAP_MAX_FIELDS_PER_ITEM",
    "DAP_MAX_ITEMS",
    "DEFAULT_BATCH_SIZE",
    "FieldMappingApiClient",
    "FieldMappingApiError",
    "build_import_payload",
    "is_local_write_url",
    "response_items",
    "split_batches",
    "submit_batches",
    "validate_mapping_item",
]
