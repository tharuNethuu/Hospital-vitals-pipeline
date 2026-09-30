"""DAG hospital_daily_risk_batch - the Lambda BATCH path, once per simulated day.

    wait_for_lab_files  -> run_spark_batch_job -> validate_report_output -> publish_report
    (PythonSensor)         (BashOperator:         (PythonOperator:          (PythonOperator:
                            PySpark job)           row / audit checks)       .md + .csv files)

* Schedule = one simulated day (SIMULATED_DAY_SECONDS, default 120s).
* The sensor looks in LAB_RESULTS_DIR for lab files without a successful
  `batch_runs` entry, older than BATCH_GRACE_SECONDS (avoids half-written
  CSVs), then applies a completeness gate: wait until the master dataset holds
  readings past the file's window end (e.g. the speed layer is still replaying
  Kafka after a restart), for at most BATCH_COMPLETENESS_MAX_WAIT_SECONDS;
  after that it runs anyway and the batch job logs the window as incomplete. All pending files are handed to ONE Spark run
  (up to BATCH_MAX_FILES_PER_RUN) so a backlog after downtime is caught up
  instead of trailing forever.
* Airflow's run_id is passed to Spark as the correlation id: it appears in
  every log line, in batch_runs and in daily_risk_report.run_id.
* Failures -> "ALERT:" log line + pipeline_alerts row (airflow_callbacks).
"""
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.python import PythonSensor

PIPELINE_HOME = os.environ.get("PIPELINE_HOME", str(Path(__file__).resolve().parents[2]))
if PIPELINE_HOME not in sys.path:
    sys.path.insert(0, PIPELINE_HOME)

from common.config import get_settings  # noqa: E402
from common.db import transaction  # noqa: E402
from common.logging_utils import get_logger, log_event  # noqa: E402
from orchestration.airflow_callbacks import on_task_failure, on_task_success  # noqa: E402
from processing.lab_files import (fetch_latest_event_epoch,  # noqa: E402
                                  fetch_processed_file_names, list_lab_files,
                                  pending_lab_files, ready_for_batch)

settings = get_settings()
logger = get_logger("airflow_batch_dag")
SENSOR_TASK = "wait_for_lab_files"


def find_pending_lab_files() -> PokeReturnValue:
    with transaction(settings) as cur:
        processed = fetch_processed_file_names(cur)
        latest_event = fetch_latest_event_epoch(cur)
    pending = pending_lab_files(list_lab_files(settings.lab_results_dir), processed,
                                grace_seconds=settings.batch_grace_seconds,
                                max_age_seconds=settings.lab_file_max_age_seconds,
                                limit=settings.batch_max_files_per_run)
    if not pending:
        log_event(logger, logging.INFO, "No unprocessed lab file yet", lab_results_dir=str(settings.lab_results_dir))
        return PokeReturnValue(is_done=False)
    ready = ready_for_batch(pending, latest_event,
                            grace_seconds=settings.batch_grace_seconds,
                            max_wait_seconds=settings.batch_completeness_max_wait_seconds)
    if not ready:
        log_event(logger, logging.INFO, "Waiting for master dataset to cover the vitals window",
                  file=pending[0].name, latest_event_epoch=latest_event,
                  window_end_epoch=pending[0].dropped_at_epoch)
        return PokeReturnValue(is_done=False)
    log_event(logger, logging.INFO, "Lab files ready for batch layer", files=[f.name for f in ready])
    return PokeReturnValue(is_done=True, xcom_value=[f.as_dict() for f in ready])


def validate_report_output(ti, run_id, **_):
    files = [f["name"] for f in ti.xcom_pull(task_ids=SENSOR_TASK)]
    with transaction(settings) as cur:
        cur.execute("""
            SELECT f.name AS source_file, b.status, b.rows_written, b.patients, b.high_risk_patients,
                   (SELECT count(*) FROM daily_risk_report d WHERE d.source_file = f.name) AS stored_rows
            FROM unnest(%s::text[]) AS f(name)
            LEFT JOIN LATERAL (SELECT * FROM batch_runs WHERE source_file = f.name AND run_id = %s
                               ORDER BY started_at DESC LIMIT 1) b ON true
        """, (files, run_id))
        results = cur.fetchall()
    problems = [r for r in results
                if r["status"] != "success" or not r["stored_rows"] or r["stored_rows"] != r["rows_written"]]
    for r in results:
        log_event(logger, logging.INFO, "Report validation", run_id=run_id, **dict(r))
    if problems:
        raise AirflowException(f"Report validation failed for {[p['source_file'] for p in problems]}")


def publish_report(ti, run_id, **_):
    from storage.report_export import export_report

    written = []
    for f in ti.xcom_pull(task_ids=SENSOR_TASK):
        written += [str(p) for p in export_report(f["name"])]
    log_event(logger, logging.INFO, "Daily report files published", run_id=run_id, files=written)
    return written


default_args = {
    "owner": "member_c",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
}

with DAG(
    dag_id="hospital_daily_risk_batch",
    description="Daily batch layer: lab file + day's vitals -> daily_risk_report -> report files",
    schedule=timedelta(seconds=settings.simulated_day_seconds),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(seconds=max(4 * settings.simulated_day_seconds, 600)),
    default_args=default_args,
    tags=["hospital", "batch-layer", "lambda"],
    doc_md=__doc__,
) as dag:
    wait_for_lab_files = PythonSensor(
        task_id=SENSOR_TASK,
        python_callable=find_pending_lab_files,
        poke_interval=10,
        timeout=int(settings.simulated_day_seconds * 1.5),
        mode="poke",
        # No file this cycle is not a pipeline failure: skip the run. A late
        # daily feed is caught by the lab_feed_stale health rule instead.
        soft_fail=True,
        retries=0,
    )

    run_spark_batch_job = BashOperator(
        task_id="run_spark_batch_job",
        bash_command=(
            'cd "$PIPELINE_HOME" && python -m processing.batch_layer_daily_report '
            "--run-id '{{ run_id }}'"
            "{% for f in ti.xcom_pull(task_ids='" + SENSOR_TASK + "') %}"
            " --lab-file '{{ f.path }}'{% endfor %}"
        ),
        env={"PIPELINE_HOME": PIPELINE_HOME},
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    validate = PythonOperator(task_id="validate_report_output",
                              python_callable=validate_report_output)

    publish = PythonOperator(task_id="publish_report", python_callable=publish_report)

    wait_for_lab_files >> run_spark_batch_job >> validate >> publish
