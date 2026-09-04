"""可选的 JDBC 表迁移示例。

集群连接信息只从 configs/migrate/*.local.json 或环境变量读取；公开仓库不携带实际集群配置。
"""

from __future__ import annotations

import argparse
import json
import os
import re
from importlib import import_module
from pathlib import Path

from shared.config.env import required_env, safe_identifier

ROOT_DIR = Path(__file__).resolve().parents[2]
CLUSTERS_FILE = ROOT_DIR / "configs" / "migrate" / "clusters.local.json"
EXAMPLE_CLUSTERS_FILE = ROOT_DIR / "configs" / "migrate" / "clusters.example.json"
DEFAULT_JAR_FILE = ROOT_DIR / "resources" / "jars" / "jdbc-driver.jar"
_DATA_TYPE_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]*(?:\s+[A-Za-z][A-Za-z0-9_]*)*"
    r"(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\[\])?$"
)


def _column_definition(column) -> str:
    if not isinstance(column, (tuple, list)) or len(column) < 2:
        raise ValueError(f"无效的列结构: {column!r}")
    name = safe_identifier(str(column[0]), "column")
    data_type = str(column[1] or "").strip()
    if not _DATA_TYPE_RE.fullmatch(data_type):
        raise ValueError(f"不支持的列类型: {data_type!r}")
    return f"{name} {data_type}"


def _render_identifier_sql(template: str, replacements: dict[str, str]) -> str:
    rendered = template
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def load_clusters(config_file: str | Path | None = None) -> dict:
    path = (
        Path(config_file)
        if config_file
        else (CLUSTERS_FILE if CLUSTERS_FILE.exists() else EXAMPLE_CLUSTERS_FILE)
    )
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file) or {}
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"无法读取 clusters 配置: {path}") from exc
    return data.get("clusters", {})


def _cluster_password(name: str, config: dict) -> str:
    env_name = str(config.get("password_env") or "").strip()
    if not env_name:
        env_name = f"PYTOOLS_{name.upper()}_PASSWORD"
    return required_env(env_name)


def _cluster_jar(config: dict) -> Path:
    return Path(config.get("jar_path") or DEFAULT_JAR_FILE).expanduser()


def connect_cluster(name: str, config: dict):
    jar_path = _cluster_jar(config)
    if not jar_path.exists():
        raise FileNotFoundError(
            f"JDBC driver not found: {jar_path}. Obtain it separately and configure jar_path."
        )
    jaydebeapi = import_module("jaydebeapi")
    return jaydebeapi.connect(
        config["driver"],
        config["jdbc_url"],
        [config["user"], _cluster_password(name, config)],
        str(jar_path),
    )


def get_control_connection():
    """创建用于迁移队列状态更新的可选连接。"""
    return connect_cluster(
        "CONTROL_DB",
        {
            "driver": os.getenv("PYTOOLS_CONTROL_DB_DRIVER", "org.postgresql.Driver"),
            "jdbc_url": required_env("PYTOOLS_CONTROL_DB_URL"),
            "user": required_env("PYTOOLS_CONTROL_DB_USER"),
            "password_env": "PYTOOLS_CONTROL_DB_PASSWORD",
            "jar_path": os.getenv("PYTOOLS_CONTROL_DB_JAR", str(DEFAULT_JAR_FILE)),
        },
    )


def select_sql(sql: str, connection=None):
    own_connection = connection is None
    connection = connection or get_control_connection()
    cursor = connection.cursor()
    try:
        # pi-lens-ignore: python-sql-injection
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()
        if own_connection:
            connection.close()


def run_sql(sql: str, connection=None) -> None:
    own_connection = connection is None
    connection = connection or get_control_connection()
    cursor = connection.cursor()
    try:
        # pi-lens-ignore: python-sql-injection
        cursor.execute(sql)
        connection.jconn.commit()
    finally:
        cursor.close()
        if own_connection:
            connection.close()


def get_table_structure(connection, schema: str, table: str):
    schema = safe_identifier(schema, "schema").split(".")[-1]
    table = safe_identifier(table, "table").split(".")[-1]
    query = """
        SELECT a.attname AS column_name,
               format_type(a.atttypid, a.atttypmod) AS data_type
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE c.relkind IN ('r', 'v', 'm')
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND n.nspname = ?
          AND c.relname = ?
        ORDER BY a.attnum
    """
    cursor = connection.cursor()
    try:
        # pi-lens-ignore: python-sql-injection
        cursor.execute(query, (schema, table))
        return cursor.fetchall()
    finally:
        cursor.close()


def create_foreign_table(
    cursor,
    foreign_server: str,
    local_schema: str,
    local_table: str,
    source_table: str,
    structure,
):
    foreign_server = safe_identifier(foreign_server, "foreign server")
    local_schema = safe_identifier(local_schema, "schema")
    local_table = safe_identifier(local_table, "table")
    source_table = safe_identifier(source_table, "source table")
    columns_sql = ", ".join(_column_definition(column) for column in structure)
    sql = _render_identifier_sql(
        """CREATE FOREIGN TABLE IF NOT EXISTS __LOCAL_SCHEMA__.__LOCAL_TABLE__ (__COLUMNS__)
        SERVER __FOREIGN_SERVER__
        OPTIONS (schema_name '__LOCAL_SCHEMA__', table_name '__SOURCE_TABLE__', encoding 'utf8')""",
        {
            "__LOCAL_SCHEMA__": local_schema,
            "__LOCAL_TABLE__": local_table,
            "__COLUMNS__": columns_sql,
            "__FOREIGN_SERVER__": foreign_server,
            "__SOURCE_TABLE__": source_table,
        },
    )
    # pi-lens-ignore: python-sql-injection
    cursor.execute(sql)


def migrate_table(
    source_name: str,
    target_name: str,
    schema: str,
    table: str,
    config_file: str | Path | None = None,
) -> None:
    clusters = load_clusters(config_file)
    if source_name not in clusters or target_name not in clusters:
        raise KeyError("指定的源或目标集群不存在于本地 clusters 配置")
    if source_name == target_name:
        raise ValueError("源集群和目标集群不能相同")

    schema = safe_identifier(schema, "schema")
    table = safe_identifier(table, "table")
    source = connect_cluster(source_name, clusters[source_name])
    target = connect_cluster(target_name, clusters[target_name])
    try:
        source_structure = get_table_structure(source, schema, table)
        target_structure = get_table_structure(target, schema, table)
        if not source_structure:
            raise RuntimeError(f"源表不存在: {schema}.{table}")
        if target_structure and source_structure != target_structure:
            raise RuntimeError(f"源表与目标表结构不一致: {schema}.{table}")

        if not target_structure:
            create_cursor = target.cursor()
            try:
                columns_sql = ", ".join(
                    _column_definition(column) for column in source_structure
                )
                create_sql = _render_identifier_sql(
                    "CREATE TABLE IF NOT EXISTS __SCHEMA__.__TABLE__ (__COLUMNS__)",
                    {
                        "__SCHEMA__": schema,
                        "__TABLE__": table,
                        "__COLUMNS__": columns_sql,
                    },
                )
                # pi-lens-ignore: python-sql-injection
                create_cursor.execute(create_sql)
                target.jconn.commit()
            finally:
                create_cursor.close()

        external_table = safe_identifier(
            f"ext_{table.split('.')[-1]}", "temporary table"
        )
        cursor = target.cursor()
        try:
            create_foreign_table(
                cursor,
                clusters[source_name]["server_remote"],
                schema,
                external_table,
                table,
                source_structure,
            )
            # pi-lens-ignore: python-sql-injection
            cursor.execute(
                _render_identifier_sql(
                    "TRUNCATE TABLE __SCHEMA__.__TABLE__",
                    {"__SCHEMA__": schema, "__TABLE__": table},
                )
            )
            # pi-lens-ignore: python-sql-injection
            cursor.execute(
                _render_identifier_sql(
                    "INSERT INTO __SCHEMA__.__TABLE__ SELECT * FROM __SCHEMA__.__EXTERNAL_TABLE__",
                    {
                        "__SCHEMA__": schema,
                        "__TABLE__": table,
                        "__EXTERNAL_TABLE__": external_table,
                    },
                )
            )
            # pi-lens-ignore: python-sql-injection
            cursor.execute(
                _render_identifier_sql(
                    "DROP FOREIGN TABLE IF EXISTS __SCHEMA__.__EXTERNAL_TABLE__",
                    {"__SCHEMA__": schema, "__EXTERNAL_TABLE__": external_table},
                )
            )
            target.jconn.commit()
        finally:
            cursor.close()
    finally:
        source.close()
        target.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="迁移两个已配置集群之间的示例表")
    parser.add_argument(
        "--source", required=True, help="本地 clusters 配置中的源集群名"
    )
    parser.add_argument(
        "--target", required=True, help="本地 clusters 配置中的目标集群名"
    )
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--config")
    args = parser.parse_args()
    migrate_table(args.source, args.target, args.schema, args.table, args.config)
    print(f"数据迁移完成: {args.schema}.{args.table}")


if __name__ == "__main__":
    main()
