from __future__ import annotations

from importlib import import_module

from shared.config.env import required_env, safe_identifier

psycopg = import_module("psycopg")


def _get_required_profile_value(profile: dict, key: str) -> str:
    value = str(profile.get(key, "") or "").strip()
    if not value:
        raise KeyError(f"postgres profile missing required field: {key}")
    return value


def _get_password(profile: dict) -> str:
    password_env = _get_required_profile_value(profile, "password_env")
    return required_env(password_env)


def fetch_all(
    sql: str, params: tuple | list | None = None, profile: dict | None = None
) -> list[tuple]:
    if profile is None:
        raise ValueError("postgres profile is required")

    raw_port = profile.get("port", 5432) or 5432
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as exc:
        raise ValueError("postgres profile port must be an integer") from exc

    connection_kwargs = {
        "host": _get_required_profile_value(profile, "host"),
        "port": port,
        "dbname": _get_required_profile_value(profile, "database"),
        "user": _get_required_profile_value(profile, "user"),
        "password": _get_password(profile),
    }
    search_path = str(profile.get("search_path", "") or "").strip()
    if search_path:
        search_path = safe_identifier(search_path, "search_path")

    with psycopg.connect(**connection_kwargs) as conn:
        if search_path:
            with conn.cursor() as cur:
                search_path_sql = psycopg.sql.SQL("SET search_path TO {}").format(
                    psycopg.sql.Identifier(search_path)
                )
                # psycopg.sql.Identifier quotes the validated search path as an identifier.
                # pi-lens-ignore: python-sql-injection
                cur.execute(search_path_sql)
        with conn.cursor() as cur:
            # This adapter receives a complete query plus separately bound values.
            bound_params = params if params is not None else ()
            # pi-lens-ignore: python-sql-injection
            cur.execute(sql, bound_params)
            return [tuple(row) for row in cur.fetchall()]
