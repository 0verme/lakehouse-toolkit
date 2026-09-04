"""可选的任务编排服务状态与重跑示例。"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from pywebio.input import input
from pywebio.output import put_text
from pywebio.session import info as session_info

from shared.config.env import required_env
from shared.ui.pywebio_helper import put_red_text, put_table_plus, start_pywebio_app

TOKEN_PATH = os.getenv("ORCHESTRATOR_TOKEN_PATH", "/auth/oauth/token")
STATUS_PATH = os.getenv("ORCHESTRATOR_STATUS_PATH", "/api/jobs/status")
RESET_PATH = os.getenv("ORCHESTRATOR_RESET_PATH", "/api/jobs/reset")


def _api_base_url() -> str:
    return required_env("ORCHESTRATOR_API_BASE_URL").rstrip("/")


def _request_timeout() -> float:
    return float(os.getenv("ORCHESTRATOR_API_TIMEOUT_SECONDS", "15"))


def get_auth_token() -> tuple[int, str | None]:
    response = requests.post(
        f"{_api_base_url()}{TOKEN_PATH}",
        data={
            "username": required_env("ORCHESTRATOR_USERNAME"),
            "password": required_env("ORCHESTRATOR_PASSWORD"),
            "client_id": os.getenv("ORCHESTRATOR_CLIENT_ID", "demo-client"),
            "client_secret": required_env("ORCHESTRATOR_CLIENT_SECRET"),
            "grant_type": "password",
        },
        timeout=_request_timeout(),
    )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token") if isinstance(payload, dict) else None
    return (0, str(token)) if token else (-1, None)


def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def resets(names: list[str], run_date: str, object_type: str, token: str):
    payload = [
        {"object_name": name, "object_type": object_type, "run_date": run_date}
        for name in names
    ]
    response = requests.post(
        f"{_api_base_url()}{RESET_PATH}",
        data=json.dumps(payload),
        headers=_auth_headers(token),
        timeout=_request_timeout(),
    )
    response.raise_for_status()
    return response.json()


def plan_stat(token: str) -> dict:
    response = requests.get(
        f"{_api_base_url()}{STATUS_PATH}",
        headers=_auth_headers(token),
        params={"page": 1, "size": 10000},
        timeout=_request_timeout(),
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {"data": {}}


def categorize_plan(plan_name: str) -> str:
    upper_name = str(plan_name or "").upper()
    for marker in ("INGEST", "TRANSFORM", "DWF", "DWM", "DWA", "DWP", "EXPORT"):
        if marker in upper_name:
            return marker
    return "OTHER"


def build_status_table(records: list[dict]) -> pd.DataFrame:
    categories = ("INGEST", "TRANSFORM", "DWF", "DWM", "DWA", "DWP", "EXPORT", "OTHER")
    counts = {
        category: {"total": 0, "success": 0, "running": 0, "failed": 0}
        for category in categories
    }
    for record in records:
        category = categorize_plan(record.get("name"))
        status = str(record.get("status") or "")
        bucket = counts[category]
        bucket["total"] += 1
        if "成功" in status or status.upper() in {"SUCCESS", "SUCCEEDED"}:
            bucket["success"] += 1
        elif "处理中" in status or status.upper() in {"RUNNING", "PROCESSING"}:
            bucket["running"] += 1
        elif "失败" in status or status.upper() in {"FAILED", "ERROR"}:
            bucket["failed"] += 1

    rows = []
    for category in categories:
        bucket = counts[category]
        total = bucket["total"]
        rows.append(
            [
                category,
                total,
                bucket["success"],
                bucket["running"],
                bucket["failed"],
                f"{bucket['success'] / total:.2%}" if total else "0.00%",
            ]
        )
    return pd.DataFrame(
        rows, columns=["分类", "总计", "成功", "处理中", "失败", "完成率"]
    )


def main():
    status, token = get_auth_token()
    if status != 0 or not token:
        raise RuntimeError("OAuth token was not returned by the configured service")

    payload = plan_stat(token)
    records = (payload.get("data") or {}).get("records") or []
    records = [record for record in records if isinstance(record, dict)]
    put_text(f"当前访问来源: {session_info.user_ip}")
    put_table_plus(
        [
            build_status_table(records).columns.tolist(),
            *build_status_table(records).values.tolist(),
        ]
    )

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_date = input("重跑日期", type="date", value=yesterday, required=True)
    names = [str(record.get("name") or "") for record in records if record.get("name")]
    if names:
        resets(names, run_date, "plan", token)
        put_red_text(f"已提交 {len(names)} 个任务的重跑请求。")


if __name__ == "__main__":
    start_pywebio_app("任务编排状态演示", main)
