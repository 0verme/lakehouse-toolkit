from __future__ import annotations

import json
import logging
import os
import platform
import re
import sys
import time
import traceback
import uuid
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import streamlit as st

APP_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = Path(__file__).resolve().parents[3]
LOG_DIR = ROOT_DIR / "logs"
LOG_FILE = LOG_DIR / "svn_check.diagnostic.log"
RERUN_WINDOW_SECONDS = 10
RERUN_WARNING_THRESHOLD = 5

_LOGGER: logging.Logger | None = None
_RISK_SCAN_DONE = False
_REDACTED_FIELD_MARKERS = (
    "password",
    "secret",
    "token",
    "authorization",
    "cookie",
    "url",
    "path",
    "directory",
    "filename",
)


def _sanitize_value(field_name: str, value: Any):
    name = str(field_name).lower()
    if any(marker in name for marker in _REDACTED_FIELD_MARKERS):
        return f"<redacted {type(value).__name__}>"
    if isinstance(value, dict):
        return {key: _sanitize_value(str(key), item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(field_name, item) for item in value]
    return value


def _get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("svn_check_diag")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter("%(message)s")
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=2 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    _LOGGER = logger
    return logger


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _emit(level: str, event: str, **fields: Any) -> None:
    logger = _get_logger()
    payload = {
        "ts": _now_iso(),
        "level": level.upper(),
        "event": event,
        "pid": os.getpid(),
        **{key: _sanitize_value(key, value) for key, value in fields.items()},
    }
    logger.log(getattr(logging, level.upper(), logging.INFO), _safe_json(payload))


def _ensure_session_meta() -> None:
    if "_diag_session_id" not in st.session_state:
        st.session_state["_diag_session_id"] = str(uuid.uuid4())
    if "_diag_rerun_seq" not in st.session_state:
        st.session_state["_diag_rerun_seq"] = 0
    if "_diag_rerun_history" not in st.session_state:
        st.session_state["_diag_rerun_history"] = []
    if "_diag_last_rerun_ts" not in st.session_state:
        st.session_state["_diag_last_rerun_ts"] = None
    if "_diag_current_run_id" not in st.session_state:
        st.session_state["_diag_current_run_id"] = None


def _base_context() -> dict[str, Any]:
    _ensure_session_meta()
    return {
        "session_id": st.session_state.get("_diag_session_id"),
        "run_id": st.session_state.get("_diag_current_run_id"),
        "rerun_seq": st.session_state.get("_diag_rerun_seq"),
    }


def describe_value(value: Any) -> str:
    type_name = type(value).__name__
    if isinstance(value, str):
        return f"{type_name}(len={len(value)})"
    if isinstance(value, (list, tuple, set, frozenset, dict, deque)):
        return f"{type_name}(len={len(value)})"
    if isinstance(value, bytes):
        return f"{type_name}(len={len(value)})"
    return type_name


def dump_session_state_summary() -> dict[str, str]:
    _ensure_session_meta()
    summary: dict[str, str] = {}
    for key in sorted(st.session_state.keys()):
        try:
            summary[key] = describe_value(st.session_state[key])
        except Exception as exc:
            summary[key] = f"unavailable({type(exc).__name__})"
    return summary


def log_session_state_snapshot(label: str, **extra: Any) -> None:
    _emit(
        "info",
        "SESSION_STATE_SUMMARY",
        label=label,
        session_state_summary=dump_session_state_summary(),
        **_base_context(),
        **extra,
    )


def begin_app_rerun() -> None:
    _ensure_session_meta()
    now_ts = time.time()
    last_ts = st.session_state.get("_diag_last_rerun_ts")
    interval_seconds = None if last_ts is None else round(now_ts - float(last_ts), 3)

    st.session_state["_diag_rerun_seq"] += 1
    st.session_state["_diag_current_run_id"] = str(uuid.uuid4())
    st.session_state["_diag_last_rerun_ts"] = now_ts

    history = deque(st.session_state.get("_diag_rerun_history", []), maxlen=64)
    history.append(now_ts)
    st.session_state["_diag_rerun_history"] = list(history)

    recent_count = sum(1 for ts in history if now_ts - ts <= RERUN_WINDOW_SECONDS)
    context = _base_context()
    _emit(
        "info",
        "APP_RERUN_START",
        interval_since_last_rerun_seconds=interval_seconds,
        session_state_keys=sorted(st.session_state.keys()),
        python_version=platform.python_version(),
        streamlit_version=st.__version__,
        current_time=_now_iso(),
        **context,
    )
    if recent_count > RERUN_WARNING_THRESHOLD:
        _emit(
            "warning",
            "RERUN_TOO_FREQUENT",
            recent_rerun_count=recent_count,
            window_seconds=RERUN_WINDOW_SECONDS,
            **context,
        )


def end_app_rerun(render_completed: bool, **extra: Any) -> None:
    event = "APP_RERUN_END" if render_completed else "APP_RERUN_ABORTED"
    _emit(
        "info",
        event,
        session_state_keys=sorted(st.session_state.keys()),
        session_state_summary=dump_session_state_summary(),
        python_version=platform.python_version(),
        streamlit_version=st.__version__,
        current_time=_now_iso(),
        **_base_context(),
        **extra,
    )


def log_warning_event(event: str, **extra: Any) -> None:
    _emit("warning", event, **_base_context(), **extra)


def log_info_event(event: str, **extra: Any) -> None:
    _emit("info", event, **_base_context(), **extra)


def log_exception_event(event: str, exc: BaseException, **extra: Any) -> None:
    _emit(
        "error",
        event,
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
        **_base_context(),
        **extra,
    )


def log_button_event(button_name: str, phase: str, **extra: Any) -> None:
    _emit(
        "info",
        "BUTTON_EVENT",
        button_name=button_name,
        phase=phase,
        **_base_context(),
        **extra,
    )


def log_task_event(task_name: str, phase: str, **extra: Any) -> None:
    _emit(
        "info",
        "TASK_EVENT",
        task_name=task_name,
        phase=phase,
        **_base_context(),
        **extra,
    )


def log_widget_snapshot(widget_values: dict[str, Any], label: str) -> None:
    summary = {key: describe_value(value) for key, value in widget_values.items()}
    _emit(
        "info",
        "WIDGET_SNAPSHOT",
        label=label,
        widget_values=widget_values,
        widget_summary=summary,
        **_base_context(),
    )


def log_widget_changes(widget_values: dict[str, Any], label: str) -> dict[str, Any]:
    previous_values = st.session_state.get("_diag_widget_values", {})
    changed: dict[str, Any] = {}
    for key, value in widget_values.items():
        previous_value = previous_values.get(key, None)
        if previous_value != value:
            changed[key] = {
                "previous": previous_value,
                "current": value,
            }

    if changed:
        _emit(
            "info",
            "WIDGET_VALUES_CHANGED",
            label=label,
            changed=changed,
            **_base_context(),
        )
    else:
        _emit(
            "info",
            "WIDGET_VALUES_UNCHANGED",
            label=label,
            widget_keys=sorted(widget_values.keys()),
            **_base_context(),
        )

    st.session_state["_diag_widget_values"] = dict(widget_values)
    return changed


def record_idle_rerun_signal(
    *,
    submitted: bool,
    widget_changed: bool,
    window_seconds: int = 10,
    threshold: int = 5,
) -> None:
    _ensure_session_meta()
    now_ts = time.time()
    history = st.session_state.get("_diag_rerun_history", [])
    recent_count = sum(1 for ts in history if now_ts - float(ts) <= window_seconds)
    if submitted or widget_changed or recent_count <= threshold:
        return

    _emit(
        "warning",
        "IDLE_RERUN_TOO_FREQUENT",
        recent_rerun_count=recent_count,
        window_seconds=window_seconds,
        submitted=bool(submitted),
        widget_changed=bool(widget_changed),
        **_base_context(),
    )


def run_static_risk_scan() -> None:
    global _RISK_SCAN_DONE
    if _RISK_SCAN_DONE:
        return

    py_files = sorted(APP_DIR.rglob("*.py"))
    findings: list[dict[str, str]] = []
    patterns = [
        (
            "ST_RERUN_FOUND",
            re.compile(r"\bst\.rerun\s*\("),
            "发现 st.rerun()，需确认不是无条件触发",
        ),
        (
            "WHILE_TRUE_FOUND",
            re.compile(r"\bwhile\s+True\s*:"),
            "发现 while True，需确认有 sleep 或退出条件",
        ),
        (
            "SESSION_STATE_CLEAR_FOUND",
            re.compile(r"session_state\.(?:clear|pop)\s*\("),
            "发现 session_state 清理逻辑，需确认不是误重置",
        ),
        (
            "SESSION_STATE_DELETE_FOUND",
            re.compile(r"del\s+st\.session_state\["),
            "发现 session_state 删除逻辑，需确认不是误重置",
        ),
        (
            "SUBPROCESS_RUN_NO_TIMEOUT",
            re.compile(r"subprocess\.run\("),
            "发现 subprocess.run()，当前未见 timeout 参数，外部命令卡住时需要重点排查",
        ),
    ]

    for path in py_files:
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for event, pattern, message in patterns:
            if not pattern.search(content):
                continue
            if event == "SUBPROCESS_RUN_NO_TIMEOUT" and "timeout=" in content:
                continue
            findings.append(
                {
                    "event": event,
                    "file": str(path),
                    "message": message,
                }
            )

    if findings:
        for finding in findings:
            log_warning_event(
                finding["event"],
                file=finding["file"],
                detail=finding["message"],
            )
    else:
        log_info_event("RISK_SCAN_OK", scanned_files=len(py_files))
    _RISK_SCAN_DONE = True
