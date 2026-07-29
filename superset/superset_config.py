"""Local-first Superset configuration for the Crypto Lakehouse."""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required Superset environment variable is missing: {name}")
    return value


SECRET_KEY = _required("SUPERSET_SECRET_KEY")

_metadata_user = os.getenv("SUPERSET_DB_USER", "superset")
_metadata_password = quote_plus(_required("SUPERSET_DB_PASSWORD"))
_metadata_host = os.getenv("SUPERSET_DB_HOST", "superset-db")
_metadata_port = os.getenv("SUPERSET_DB_PORT", "5432")
_metadata_database = os.getenv("SUPERSET_DB_NAME", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_metadata_user}:{_metadata_password}"
    f"@{_metadata_host}:{_metadata_port}/{_metadata_database}"
)

# This is an HTTP-only, localhost development deployment. TLS termination and
# TALISMAN should be configured before exposing Superset beyond the local host.
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True

ROW_LIMIT = 10_000
SQL_MAX_ROW = 10_000
SUPERSET_WEBSERVER_TIMEOUT = 60

# Keep the MVP synchronous and small. Redis/Celery can be added later for alerts,
# reports, or long-running asynchronous SQL Lab queries.
GLOBAL_ASYNC_QUERIES = False
