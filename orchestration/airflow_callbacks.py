"""Airflow task callbacks that feed the pipeline alerting system.

A failed task logs a structured "ALERT:" line (visible in the task log) and
opens a `pipeline_alerts` row (visible on the dashboard, /api/pipeline/alerts
and /metrics). The next successful run of the same task resolves it.
"""
import logging

from common.db import transaction
from common.logging_utils import ALERT_PREFIX, get_logger, log_event

logger = get_logger("airflow")


def _rule_name(context) -> str:
    ti = context["task_instance"]
    return f"airflow_task_failed.{ti.dag_id}.{ti.task_id}"


def on_task_failure(context) -> None:
    ti = context["task_instance"]
    exc = context.get("exception")
    message = f"Airflow task {ti.dag_id}.{ti.task_id} failed (run {context['run_id']}): {exc}"
    log_event(logger, logging.CRITICAL, f"{ALERT_PREFIX} {message}", alert_type="pipeline",
              dag_id=ti.dag_id, task_id=ti.task_id, run_id=context["run_id"],
              try_number=ti.try_number, error=str(exc))
    try:
        with transaction() as cur:
            cur.execute(
                "INSERT INTO pipeline_alerts (rule_name, severity, message) VALUES (%s, 'critical', %s) "
                "ON CONFLICT (rule_name) WHERE status = 'open' "
                "DO UPDATE SET last_seen = now(), message = EXCLUDED.message",
                (_rule_name(context), message[:2000]))
    except Exception as db_exc:  # alerting must never mask the original failure
        log_event(logger, logging.ERROR, "Could not record pipeline alert", error=str(db_exc))


def on_task_success(context) -> None:
    try:
        with transaction() as cur:
            cur.execute("UPDATE pipeline_alerts SET status = 'resolved', resolved_at = now(), "
                        "last_seen = now() WHERE rule_name = %s AND status = 'open' RETURNING id",
                        (_rule_name(context),))
            if cur.fetchone():
                log_event(logger, logging.INFO, f"RESOLVED: {_rule_name(context)} succeeded again",
                          run_id=context["run_id"])
    except Exception as db_exc:
        log_event(logger, logging.ERROR, "Could not resolve pipeline alert", error=str(db_exc))
