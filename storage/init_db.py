"""Apply storage/schema.sql to the configured Postgres (idempotent).

Usage:  python -m storage.init_db
"""
import logging
import sys
from pathlib import Path

from common.db import connect
from common.logging_utils import get_logger, log_event

logger = get_logger("storage_init")
SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def main() -> int:
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    try:
        conn = connect()
        with conn, conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
    except Exception as exc:
        log_event(logger, logging.ERROR, "Schema migration failed", error=str(exc))
        return 1
    log_event(logger, logging.INFO, "Schema applied", schema_file=str(SCHEMA_FILE))
    return 0


if __name__ == "__main__":
    sys.exit(main())
