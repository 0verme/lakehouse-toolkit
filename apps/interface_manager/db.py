import os
import sqlite3
from importlib import import_module
from pathlib import Path

pd = import_module("pandas")


ROOT_DIR = Path(__file__).resolve().parents[2]
DB_PATH = Path(
    os.getenv(
        "PYTOOLS_INTERFACE_DB_PATH",
        str(ROOT_DIR / "runtime" / "interface" / "interface_assets.db"),
    )
).expanduser()


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS downstream_system (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_code TEXT NOT NULL UNIQUE,
                system_name TEXT NOT NULL,
                ip TEXT,
                username TEXT,
                password_enc TEXT,
                ftp_type TEXT,
                port TEXT,
                remark TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS push_job (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_code TEXT NOT NULL,
                job_name TEXT NOT NULL,
                file_desc TEXT,
                lake_path TEXT,
                target_path TEXT,
                push_frequency TEXT,
                enabled_flag TEXT DEFAULT 'Y',
                remark TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_push_job_unique
            ON push_job(system_code, job_name);

            CREATE TABLE IF NOT EXISTS file_spec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_code TEXT NOT NULL,
                job_name TEXT NOT NULL,
                file_name TEXT,
                file_comment TEXT,
                biz_desc TEXT,
                file_remark TEXT,
                delimiter TEXT,
                push_frequency TEXT,
                field_seq INTEGER,
                field_name TEXT,
                field_cn_name TEXT,
                field_meaning TEXT,
                source_system TEXT,
                field_remark TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_spec_unique
            ON file_spec(system_code, job_name, field_seq);
            """
        )


def query_df(sql, params=None):
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params or [])


def execute_sql(sql, params=None):
    with get_conn() as conn:
        # pi-lens-ignore: python-sql-injection
        conn.execute(sql, params or [])
        conn.commit()
