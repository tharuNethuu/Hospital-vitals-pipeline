"""DAG pipeline_health_checks - pipeline-wide alert rules, every minute.

Evaluates observability/alert_rules.json (stream staleness, speed-layer
heartbeat, live-view freshness, late lab feed, batch backlog / failure rate,
data-quality reject rate, ward abnormal rate) and manages `pipeline_alerts`.
The task FAILS (red in the Airflow UI) while any critical rule is breached, so
the grid view doubles as an at-a-glance health history.
"""
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

PIPELINE_HOME = os.environ.get("PIPELINE_HOME", str(Path(__file__).resolve().parents[2]))
if PIPELINE_HOME not in sys.path:
    sys.path.insert(0, PIPELINE_HOME)

from observability.health_check import has_critical, run_health_checks  # noqa: E402


def evaluate_health_rules():
    results = run_health_checks()
    critical = [r.name for r in results if r.breached and r.severity == "critical"]
    if has_critical(results):
        raise AirflowException(f"ALERT: critical pipeline rules breached: {critical}")
    return {r.name: r.breached for r in results}


with DAG(
    dag_id="pipeline_health_checks",
    description="Evaluate pipeline health/alert rules every minute",
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "member_c", "retries": 0},
    tags=["hospital", "observability"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="evaluate_health_rules", python_callable=evaluate_health_rules,
                   execution_timeout=timedelta(seconds=50))
