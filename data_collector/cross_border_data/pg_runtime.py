"""PostgreSQL runtime connection helpers for collector services."""

from __future__ import annotations

import os


def env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def pg_connection_config() -> dict[str, object | None]:
    if env_truthy("PG_TUNNEL_ENABLED"):
        host = os.environ.get("PG_SYNC_TUNNEL_LOCAL_HOST") or os.environ.get("PG_TUNNEL_LOCAL_HOST") or "127.0.0.1"
        port = os.environ.get("PG_SYNC_TUNNEL_LOCAL_PORT") or os.environ.get("PG_TUNNEL_LOCAL_PORT") or "15432"
    else:
        host = os.environ.get("PG_HOST")
        port = os.environ.get("PG_PORT", "5432")

    return {
        "host": host,
        "port": int(str(port or "5432")),
        "dbname": os.environ.get("PG_DB"),
        "user": os.environ.get("PG_USER"),
        "password": os.environ.get("PG_PASSWORD", ""),
        "connect_timeout": 3,
    }


def pg_connection_configured() -> bool:
    config = pg_connection_config()
    return bool(config.get("host") and config.get("dbname") and config.get("user"))
