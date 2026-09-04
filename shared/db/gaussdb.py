# !/bin/python
from __future__ import annotations

import traceback
from pathlib import Path

import jaydebeapi
import yaml

from shared.config.env import required_env

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "configs" / "database.local.yaml"
EXAMPLE_CONFIG_PATH = ROOT_DIR / "configs" / "database.example.yaml"
DEFAULT_DRIVER = "org.postgresql.Driver"
DEFAULT_JAR = ROOT_DIR / "resources" / "jars" / "jdbc-driver.jar"


def load_db_profiles() -> dict:
    config_path = CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    defaults = data.get("defaults", {})
    profiles = data.get("profiles", {})
    merged = {}
    for name, profile in profiles.items():
        config = dict(defaults)
        config.update(profile)
        merged[name] = config
    return merged


def get_db_profile(profile: str) -> dict:
    profiles = load_db_profiles()
    if profile not in profiles:
        raise KeyError(f"database profile not found: {profile}")

    config = dict(profiles[profile])
    config.setdefault("driver", DEFAULT_DRIVER)
    config["jar_path"] = str(Path(config.get("jar_path") or DEFAULT_JAR).expanduser())
    return config


def _get_profile_password(config: dict) -> str:
    password_env = str(config.get("password_env", "") or "").strip()
    if not password_env:
        raise KeyError("database profile missing required field: password_env")
    return required_env(password_env)


def connect_with_profile(profile: str):
    config = get_db_profile(profile)
    jar_path = Path(config["jar_path"])
    if not jar_path.exists():
        raise FileNotFoundError(
            f"JDBC driver not found: {jar_path}. Obtain the driver separately and configure jar_path."
        )
    return jaydebeapi.connect(
        config["driver"],
        config["jdbc_url"],
        [config["user"], _get_profile_password(config)],
        str(jar_path),
    )


def _is_autocommit_enabled(conn) -> bool | None:
    jconn = getattr(conn, "jconn", None)
    if jconn is None:
        return None
    try:
        return bool(jconn.getAutoCommit())
    except Exception:
        return None


def _commit_if_needed(conn):
    auto_commit_enabled = _is_autocommit_enabled(conn)
    if auto_commit_enabled is True:
        return

    try:
        conn.commit()
        return
    except Exception as e:
        if "autoCommit is enabled" in str(e):
            return

        jconn = getattr(conn, "jconn", None)
        if jconn is None:
            raise

        try:
            jconn.commit()
        except Exception as inner_e:
            if "autoCommit is enabled" in str(inner_e):
                return
            raise inner_e from e


def _close_quietly(resource) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        return


def fetch_all(profile: str, sql: str, params: tuple | list | None = None):
    conn = None
    curs = None
    try:
        conn = connect_with_profile(profile)
        curs = conn.cursor()
        # pi-lens-ignore: python-sql-injection
        curs.execute(sql, params or ())
        return curs.fetchall()
    except Exception as exc:
        print(f"select_sql exception [{profile}]:", exc)
        print(traceback.format_exc())
        return None
    finally:
        _close_quietly(curs)
        _close_quietly(conn)


def execute_sql(
    profile: str, sql: str, autocommit: bool = True, params: tuple | list | None = None
):
    conn = None
    curs = None
    try:
        conn = connect_with_profile(profile)
        curs = conn.cursor()
        # pi-lens-ignore: python-sql-injection
        curs.execute(sql, params or ())
        if autocommit:
            _commit_if_needed(conn)
        return True
    except Exception as exc:
        print(f"run_sql exception [{profile}]:", exc)
        print(traceback.format_exc())
        return False
    finally:
        _close_quietly(curs)
        _close_quietly(conn)


def select_sql_with_profile(
    profile: str, sql_str: str, params: tuple | list | None = None
):
    return fetch_all(profile, sql_str, params=params)


def run_sql_with_profile(
    profile: str, sql_str: str, params: tuple | list | None = None
):
    return execute_sql(profile, sql_str, params=params)
