"""Structured JSON logging shared by every pipeline stage.

Emits one JSON object per line with the same core keys as Member A's
ingestion scripts (timestamp, level, component, message) plus optional
structured fields (run_id, batch_id, patient_id, row counts ...), so that logs
from ingestion -> processing -> storage -> orchestration can be grepped and
correlated on the same keys.

Unlike a format-string template, the formatter uses json.dumps, so messages
containing quotes or newlines still produce valid JSON.
"""
import json
import logging
import sys
from datetime import datetime, timezone

ALERT_PREFIX = "ALERT:"


class JsonFormatter(logging.Formatter):
    def __init__(self, component: str):
        super().__init__()
        self.component = component

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "component": self.component,
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class _CurrentStdoutHandler(logging.StreamHandler):
    """Writes to whatever sys.stdout is at emit time. Airflow swaps sys.stdout
    while a task runs (and logs it at INFO, whereas stderr is tagged WARNING);
    binding lazily makes our lines land in the task log with the right level."""

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, _value):
        pass


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(f"hospital.{component}")
    if not logger.handlers:
        handler = _CurrentStdoutHandler()
        handler.setFormatter(JsonFormatter(component))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields) -> None:
    """Log `message` with extra structured key/value fields."""
    logger.log(level, message, extra={"fields": fields})
