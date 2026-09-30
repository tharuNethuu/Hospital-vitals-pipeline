"""Thin psycopg2 helpers. All sessions are pinned to UTC so that the naive
TIMESTAMP columns of the original tables are always written/read as UTC."""
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from common.config import Settings, get_settings


def connect(settings: Settings = None):
    s = settings or get_settings()
    return psycopg2.connect(
        host=s.pg_host,
        port=s.pg_port,
        dbname=s.pg_db,
        user=s.pg_user,
        password=s.pg_password,
        sslmode=s.pg_sslmode,
        connect_timeout=10,
        options="-c timezone=UTC",
        application_name="hospital-vitals-pipeline",
    )


@contextmanager
def transaction(settings: Settings = None):
    """Yield a cursor inside a single transaction (commit or rollback)."""
    conn = connect(settings)
    try:
        with conn:  # commits on success, rolls back on exception
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


def execute_values(cur, sql: str, rows, template=None, page_size: int = 500):
    psycopg2.extras.execute_values(cur, sql, rows, template=template, page_size=page_size)


def write_heartbeat(cur, component: str, details: dict) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_heartbeat (component, last_seen, details)
        VALUES (%s, now(), %s)
        ON CONFLICT (component) DO UPDATE
            SET last_seen = EXCLUDED.last_seen, details = EXCLUDED.details
        """,
        (component, psycopg2.extras.Json(details)),
    )
