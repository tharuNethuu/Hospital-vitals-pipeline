"""Collects pipeline health metrics from every stage in one place.

The same numbers feed three consumers, so they can never disagree:
  * observability/health_check.py  - evaluates alert_rules.json against them;
  * storage/api.py  GET /metrics   - Prometheus text exposition;
  * storage/api.py  GET /health    - JSON health summary.

Stage -> signal:
  ingestion   newest reading / lab file age, readings & abnormal ratio (5 min)
  processing  speed-layer heartbeat age + rows/s + rejected ratio,
              live-summary freshness, batch success/failure, pending lab files
  storage     DB reachability, open patient / pipeline alerts
"""
import time
from typing import Dict, Optional

from common.config import Settings, get_settings
from processing.lab_files import fetch_processed_file_names, list_lab_files, pending_lab_files

# (metric name, help text) - documents every metric exposed by /metrics.
METRIC_HELP = {
    "db_up": "1 if Postgres is reachable",
    "vitals_last_ingest_age_seconds": "Seconds since the newest raw reading was stored",
    "vitals_last_event_lag_seconds": "Ingest time minus event time of the newest reading (end-to-end latency)",
    "vitals_readings_5m": "Raw readings stored in the last 5 minutes",
    "vitals_abnormal_ratio_5m": "Share of abnormal readings in the last 5 minutes",
    "active_patients_2m": "Patients with at least one reading in the last 2 minutes",
    "live_summary_last_update_age_seconds": "Seconds since vitals_live_summary was last upserted",
    "speed_layer_heartbeat_age_seconds": "Seconds since the speed-layer driver last reported",
    "speed_layer_rejected_ratio": "Share of consumed readings rejected by data-quality rules",
    "speed_layer_input_rows_per_second": "Kafka input rate of the raw query (last micro-batch)",
    "patient_alerts_active": "Patient alert episodes updated in the last 2 minutes",
    "patient_alerts_24h": "Patient alert episodes opened in the last 24 hours",
    "batch_runs_success_30m": "Successful batch-layer runs in the last 30 minutes",
    "batch_runs_failed_30m": "Failed batch-layer runs in the last 30 minutes",
    "batch_failure_ratio_30m": "Failed / finished batch runs in the last 30 minutes",
    "batch_last_success_age_seconds": "Seconds since the last successful batch run",
    "batch_last_duration_seconds": "Duration of the last finished batch run",
    "lab_feed_last_file_age_seconds": "Seconds since the newest lab results file was dropped",
    "lab_files_pending": "Lab files older than the grace period without a successful batch run",
    "pipeline_alerts_open": "Open pipeline-health alerts",
}

_SQL = {
    "raw": """
        SELECT EXTRACT(EPOCH FROM now() - max(ingested_at))                       AS ingest_age,
               EXTRACT(EPOCH FROM (SELECT ingested_at - event_time FROM vitals_readings
                                   ORDER BY ingested_at DESC LIMIT 1))            AS event_lag,
               count(*) FILTER (WHERE ingested_at > now() - interval '5 minutes') AS readings_5m,
               avg(is_abnormal::int) FILTER (WHERE ingested_at > now() - interval '5 minutes')
                                                                                  AS abnormal_ratio_5m,
               count(DISTINCT patient_id) FILTER (WHERE ingested_at > now() - interval '2 minutes')
                                                                                  AS active_patients
        FROM vitals_readings
        WHERE ingested_at > now() - interval '1 day'
    """,
    "raw_any": "SELECT EXTRACT(EPOCH FROM now() - max(ingested_at)) AS age FROM vitals_readings",
    "summary": """
        SELECT EXTRACT(EPOCH FROM timezone('UTC', now()) - max(computed_at)) AS age
        FROM vitals_live_summary
    """,
    "heartbeat": """
        SELECT EXTRACT(EPOCH FROM now() - last_seen) AS age, details
        FROM pipeline_heartbeat WHERE component = 'speed_layer'
    """,
    "patient_alerts": """
        SELECT count(*) FILTER (WHERE updated_at > now() - interval '2 minutes')  AS active,
               count(*) FILTER (WHERE created_at > now() - interval '24 hours')   AS last_24h
        FROM patient_alerts
    """,
    "batch": """
        SELECT count(*) FILTER (WHERE status = 'success' AND started_at > now() - interval '30 minutes') AS ok,
               count(*) FILTER (WHERE status = 'failed'  AND started_at > now() - interval '30 minutes') AS failed,
               EXTRACT(EPOCH FROM now() - max(finished_at) FILTER (WHERE status = 'success'))  AS last_ok_age,
               (SELECT EXTRACT(EPOCH FROM finished_at - started_at) FROM batch_runs
                 WHERE finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1)             AS last_duration
        FROM batch_runs
    """,
    "pipeline_alerts": "SELECT count(*) AS open FROM pipeline_alerts WHERE status = 'open'",
}


def _f(value) -> Optional[float]:
    return None if value is None else float(value)


def collect_metrics(cur, settings: Settings = None) -> Dict[str, Optional[float]]:
    """Return {metric_name: value}. None means 'no data yet' (e.g. empty table)."""
    s = settings or get_settings()
    m: Dict[str, Optional[float]] = {"db_up": 1.0}

    cur.execute(_SQL["raw"])
    row = cur.fetchone()
    m["vitals_last_ingest_age_seconds"] = _f(row["ingest_age"])
    if m["vitals_last_ingest_age_seconds"] is None:  # nothing in the last day
        cur.execute(_SQL["raw_any"])
        m["vitals_last_ingest_age_seconds"] = _f(cur.fetchone()["age"])
    m["vitals_last_event_lag_seconds"] = _f(row["event_lag"])
    m["vitals_readings_5m"] = _f(row["readings_5m"])
    m["vitals_abnormal_ratio_5m"] = _f(row["abnormal_ratio_5m"])
    m["active_patients_2m"] = _f(row["active_patients"])

    cur.execute(_SQL["summary"])
    m["live_summary_last_update_age_seconds"] = _f(cur.fetchone()["age"])

    cur.execute(_SQL["heartbeat"])
    hb = cur.fetchone()
    m["speed_layer_heartbeat_age_seconds"] = _f(hb["age"]) if hb else None
    counters = ((hb or {}).get("details") or {}).get("counters", {})
    seen = counters.get("raw_rows_seen") or 0
    m["speed_layer_rejected_ratio"] = (counters.get("raw_rows_rejected", 0) / seen) if seen else None
    raw_q = ((hb or {}).get("details") or {}).get("queries", {}).get("vitals_raw_to_master", {})
    m["speed_layer_input_rows_per_second"] = _f(raw_q.get("input_rows_per_sec"))

    cur.execute(_SQL["patient_alerts"])
    row = cur.fetchone()
    m["patient_alerts_active"] = _f(row["active"])
    m["patient_alerts_24h"] = _f(row["last_24h"])

    cur.execute(_SQL["batch"])
    row = cur.fetchone()
    ok, failed = row["ok"] or 0, row["failed"] or 0
    m["batch_runs_success_30m"] = float(ok)
    m["batch_runs_failed_30m"] = float(failed)
    m["batch_failure_ratio_30m"] = (failed / (ok + failed)) if (ok + failed) else 0.0
    m["batch_last_success_age_seconds"] = _f(row["last_ok_age"])
    m["batch_last_duration_seconds"] = _f(row["last_duration"])

    files = list_lab_files(s.lab_results_dir)
    now = time.time()
    m["lab_feed_last_file_age_seconds"] = (now - files[-1].dropped_at_epoch) if files else None
    processed = fetch_processed_file_names(cur)
    m["lab_files_pending"] = float(len(pending_lab_files(
        files, processed, grace_seconds=s.batch_grace_seconds,
        max_age_seconds=s.lab_file_max_age_seconds, limit=10_000, now=now)))

    cur.execute(_SQL["pipeline_alerts"])
    m["pipeline_alerts_open"] = float(cur.fetchone()["open"])
    return m


def to_prometheus(metrics: Dict[str, Optional[float]], rule_states: Dict[str, bool] = None,
                  prefix: str = "hospital_") -> str:
    """Render metrics in Prometheus text exposition format (NaN = no data)."""
    lines = []
    for name, value in metrics.items():
        full = prefix + name
        lines.append(f"# HELP {full} {METRIC_HELP.get(name, name)}")
        lines.append(f"# TYPE {full} gauge")
        lines.append(f"{full} {'NaN' if value is None else repr(float(value))}")
    if rule_states:
        full = prefix + "health_rule_breached"
        lines.append(f"# HELP {full} 1 if the health-check rule is currently breached")
        lines.append(f"# TYPE {full} gauge")
        for rule, breached in sorted(rule_states.items()):
            lines.append(f'{full}{{rule="{rule}"}} {1 if breached else 0}')
    return "\n".join(lines) + "\n"
