"""可选的元数据注释同步示例。

该模块只在显式调用时访问外部服务；地址、账号和 OAuth Secret 全部来自环境变量。
"""

from __future__ import annotations

import os
from typing import Any

import requests

from shared.config.env import required_env, safe_identifier
from shared.config.metadata import table as metadata_table

TOKEN_PATH = os.getenv("CMS_TOKEN_PATH", "/auth/oauth/token")
PROCESS_CONFIG_PATH = os.getenv(
    "CMS_PROCESS_CONFIG_PATH", "/api/process/config/{process_name}"
)


def _api_base_url() -> str:
    return required_env("CMS_API_BASE_URL").rstrip("/")


def _request_timeout() -> float:
    return float(os.getenv("CMS_API_TIMEOUT_SECONDS", "15"))


def get_db():
    """按需创建元数据数据库连接，不在 import 时连接网络。"""
    import pymysql

    return pymysql.connect(
        host=os.getenv("CMS_MYSQL_HOST", "localhost"),
        user=required_env("CMS_MYSQL_USER"),
        password=required_env("CMS_MYSQL_PASSWORD"),
        database=os.getenv("CMS_MYSQL_DATABASE", "pytools_demo"),
        charset="utf8mb4",
        autocommit=True,
    )


def select_mysql_sql(sql: str, params: tuple[Any, ...] = ()):
    db = get_db()
    try:
        with db.cursor() as cursor:
            # pi-lens-ignore: python-sql-injection
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        db.close()


def get_auth_token() -> tuple[int, str | None]:
    data = {
        "username": required_env("CMS_USERNAME"),
        "password": required_env("CMS_PASSWORD"),
        "client_id": os.getenv("CMS_CLIENT_ID", "demo-client"),
        "client_secret": required_env("CMS_CLIENT_SECRET"),
        "grant_type": "password",
    }
    response = requests.post(
        f"{_api_base_url()}{TOKEN_PATH}",
        data=data,
        timeout=_request_timeout(),
    )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        return -1, None
    return 0, str(token)


def plan_stat(process_name: str, token: str) -> dict[str, Any]:
    path = PROCESS_CONFIG_PATH.format(process_name=process_name)
    response = requests.get(
        f"{_api_base_url()}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=_request_timeout(),
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {"data": payload}


def _sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def build_mapping_sql(target_name: str, column_configs: list[dict[str, Any]]) -> str:
    """把通用字段映射转换为参数化前的 demo SQL 文本。"""
    target_table = safe_identifier(
        os.getenv(
            "PYTOOLS_CMS_MAPPING_TABLE",
            metadata_table("result_receipts", "source_target_mapping"),
        ),
        "PYTOOLS_CMS_MAPPING_TABLE",
    )
    statement_template = (
        "INSERT INTO __TARGET_TABLE__ "
        "(target_name, source_table, source_column, target_column, column_order) "
        "VALUES (__TARGET_NAME__, __SOURCE_TABLE__, __SOURCE_COLUMN__, "
        "__TARGET_COLUMN__, __COLUMN_ORDER__);"
    )
    statements = []
    for index, item in enumerate(column_configs, start=1):
        statement = (
            statement_template.replace("__TARGET_NAME__", _sql_literal(target_name))
            .replace("__SOURCE_TABLE__", _sql_literal(item.get("source_table", "")))
            .replace("__SOURCE_COLUMN__", _sql_literal(item.get("source_column", "")))
            .replace("__TARGET_COLUMN__", _sql_literal(item.get("target_column", "")))
            .replace("__COLUMN_ORDER__", str(index))
            .replace("__TARGET_TABLE__", target_table)
        )
        statements.append(statement)
    return "\n".join(statements)


def main(process_name: str, target_name: str) -> str:
    status, token = get_auth_token()
    if status != 0 or not token:
        raise RuntimeError("OAuth token was not returned by the configured service")
    payload = plan_stat(process_name, token)
    configs = payload.get("data", {}).get("columnConfigs", [])
    if not isinstance(configs, list):
        raise ValueError("Configured service returned invalid columnConfigs")
    return build_mapping_sql(
        target_name, [item for item in configs if isinstance(item, dict)]
    )


if __name__ == "__main__":
    process_name = os.getenv("CMS_PROCESS_NAME") or required_env("CMS_PROCESS_NAME")
    target_name = os.getenv("CMS_TARGET_NAME") or required_env("CMS_TARGET_NAME")
    print(main(process_name, target_name))
