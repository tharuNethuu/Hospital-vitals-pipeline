"""SERVING LAYER - REST API + live ward dashboard (Member C).

Merges the two Lambda views for clients:
  speed view  vitals_live_summary / vitals_readings / patient_alerts  (seconds old)
  batch view  daily_risk_report                                       (once per simulated day)

Endpoints
  GET /                              live ward dashboard (HTML, polls the API)
  GET /api/ward/live                 real-time ward monitoring figures + per-patient status
  GET /api/patients/{patient_id}     recent readings, windows, alerts and latest risk report
  GET /api/alerts                    per-patient threshold alert episodes
  GET /api/pipeline/alerts           pipeline-health alerts (observability)
  GET /api/reports/daily             list of generated daily reports
  GET /api/reports/daily/latest      latest consolidated risk report
  GET /api/reports/daily/{file}      a specific day's consolidated risk report
  GET /health                        liveness + health-rule evaluation (JSON)
  GET /metrics                       Prometheus metrics export

Run:  uvicorn storage.api:app --host 0.0.0.0 --port 8000
"""
import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import psycopg2.extras
import psycopg2.pool
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from common.config import get_settings
from common.logging_utils import get_logger, log_event
from observability.health_check import evaluate_rules, load_rules
from observability.metrics import collect_metrics, to_prometheus
from storage.report_export import consolidate, fetch_report_rows, latest_source_file

logger = get_logger("serving_api")
settings = get_settings()
app = FastAPI(title="Hospital Vitals Pipeline - Serving API", version="1.0")
DASHBOARD = Path(__file__).with_name("static") / "dashboard.html"

ACTIVE_WINDOW = "2 minutes"   # a patient counts as "currently monitored" within this
ALERT_ACTIVE_WINDOW = "60 seconds"  # an alert episode is "active" if updated within this

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    with _pool_lock:
        if _pool is None:
            s = settings
            _pool = psycopg2.pool.ThreadedConnectionPool(
                1, 8, host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user=s.pg_user,
                password=s.pg_password, sslmode=s.pg_sslmode, connect_timeout=5,
                options="-c timezone=UTC", application_name="hospital-serving-api")
        return _pool


@contextmanager
def db_cursor():
    try:
        pool = _get_pool()
        conn = pool.getconn()
    except psycopg2.Error as exc:
        log_event(logger, logging.ERROR, "Database unavailable", error=str(exc))
        raise HTTPException(status_code=503, detail="database unavailable")
    broken = False
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
    except psycopg2.OperationalError:
        broken = True
        raise
    finally:
        pool.putconn(conn, close=broken)


@app.middleware("http")
async def access_log(request: Request, call_next):
    started = time.monotonic()
    try:
        response = await call_next(request)
    except Exception as exc:
        log_event(logger, logging.ERROR, "Request failed", method=request.method,
                  path=request.url.path, error=str(exc))
        return JSONResponse({"detail": "internal error"}, status_code=500)
    if request.url.path not in ("/metrics",):  # keep scrape noise out of the logs
        log_event(logger, logging.INFO, "HTTP request", method=request.method,
                  path=request.url.path, status=response.status_code,
                  duration_ms=round((time.monotonic() - started) * 1000, 1))
    return response


# ---------------------------------------------------------------------------
# Real-time (speed view)
# ---------------------------------------------------------------------------
WARD_SQL = f"""
    SELECT count(DISTINCT patient_id) FILTER (WHERE ingested_at > now() - interval '{ACTIVE_WINDOW}')
                                                                      AS active_patients,
           count(*)                                                   AS readings_5m,
           avg(is_abnormal::int)::float                               AS abnormal_ratio_5m,
           avg(heart_rate)::float                                     AS avg_heart_rate_5m,
           avg(spo2)::float                                           AS avg_spo2_5m,
           avg(temperature)::float                                    AS avg_temperature_5m
    FROM vitals_readings WHERE ingested_at > now() - interval '5 minutes'
"""

PATIENTS_SQL = f"""
    WITH last_reading AS (
        SELECT DISTINCT ON (patient_id) patient_id, event_time, heart_rate, spo2, systolic_bp,
               diastolic_bp, temperature, is_abnormal
        FROM vitals_readings WHERE event_time > now() - interval '1 day'
        ORDER BY patient_id, event_time DESC
    ), last_window AS (
        SELECT DISTINCT ON (patient_id) patient_id, window_start, window_end, avg_heart_rate,
               avg_spo2, avg_temperature, abnormal_count, total_readings
        FROM vitals_live_summary WHERE window_end > timezone('UTC', now()) - interval '1 day'
        ORDER BY patient_id, window_end DESC
    ), active_alert AS (
        SELECT DISTINCT ON (patient_id) patient_id, alert_id, severity, reasons, updated_at
        FROM patient_alerts WHERE updated_at > now() - interval '{ALERT_ACTIVE_WINDOW}'
        ORDER BY patient_id, (severity = 'critical') DESC, updated_at DESC
    ), latest_risk AS (
        SELECT DISTINCT ON (patient_id) patient_id, risk_flag, report_day, risk_reasons
        FROM daily_risk_report ORDER BY patient_id, generated_at DESC
    )
    SELECT r.patient_id, r.event_time AS last_reading_at, r.heart_rate, r.spo2, r.systolic_bp,
           r.diastolic_bp, r.temperature, r.is_abnormal AS last_reading_abnormal,
           w.window_start, w.window_end, w.avg_heart_rate, w.avg_spo2, w.avg_temperature,
           w.abnormal_count AS window_abnormal_count, w.total_readings AS window_readings,
           a.alert_id, a.severity AS alert_severity, a.reasons AS alert_reasons,
           k.risk_flag AS latest_daily_risk, k.report_day AS latest_report_day,
           GREATEST(0, EXTRACT(EPOCH FROM now() - r.event_time))::float AS seconds_since_last_reading
    FROM last_reading r
    LEFT JOIN last_window w USING (patient_id)
    LEFT JOIN active_alert a USING (patient_id)
    LEFT JOIN latest_risk k USING (patient_id)
    ORDER BY r.patient_id
"""

_STATUS_ORDER = {"critical": 0, "warning": 1, "no_signal": 2, "normal": 3}


@app.get("/api/ward/live")
def ward_live():
    with db_cursor() as cur:
        cur.execute(WARD_SQL)
        ward = cur.fetchone()
        cur.execute(PATIENTS_SQL)
        patients = cur.fetchall()
    for p in patients:
        if p["alert_severity"]:
            p["status"] = p["alert_severity"]
        elif p["seconds_since_last_reading"] > 120:
            p["status"] = "no_signal"
        else:
            p["status"] = "normal"
    patients.sort(key=lambda p: (_STATUS_ORDER[p["status"]], p["patient_id"]))
    ward = dict(ward)
    ward["patients_monitored"] = len(patients)
    ward["patients_critical"] = sum(p["status"] == "critical" for p in patients)
    ward["patients_warning"] = sum(p["status"] == "warning" for p in patients)
    return {"generated_at": time.time(), "ward": ward, "patients": patients}


@app.get("/api/patients/{patient_id}")
def patient_detail(patient_id: str, minutes: int = Query(10, ge=1, le=1440)):
    pid = patient_id.upper()
    with db_cursor() as cur:
        cur.execute("SELECT event_time, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, "
                    "is_abnormal FROM vitals_readings WHERE patient_id = %s AND event_time > "
                    "now() - make_interval(mins => %s) ORDER BY event_time DESC LIMIT 200",
                    (pid, minutes))
        readings = cur.fetchall()
        cur.execute("SELECT window_start, window_end, avg_heart_rate, avg_spo2, avg_temperature, "
                    "abnormal_count, total_readings FROM vitals_live_summary WHERE patient_id = %s "
                    "ORDER BY window_end DESC LIMIT 60", (pid,))
        windows = cur.fetchall()
        cur.execute("SELECT * FROM patient_alerts WHERE patient_id = %s "
                    "ORDER BY updated_at DESC LIMIT 20", (pid,))
        alerts = cur.fetchall()
        cur.execute("SELECT * FROM daily_risk_report WHERE patient_id = %s AND source_file = "
                    "(SELECT source_file FROM daily_risk_report WHERE patient_id = %s "
                    " ORDER BY generated_at DESC LIMIT 1)", (pid, pid))
        risk_rows = cur.fetchall()
    if not (readings or windows or alerts or risk_rows):
        raise HTTPException(status_code=404, detail=f"no data for patient {pid}")
    latest_risk = consolidate([dict(r) for r in risk_rows])["patients"] if risk_rows else []
    return {"patient_id": pid, "recent_readings": readings, "recent_windows": windows,
            "alerts": alerts, "latest_daily_risk": latest_risk[0] if latest_risk else None}


@app.get("/api/alerts")
def patient_alerts(active_only: bool = False, limit: int = Query(50, ge=1, le=500)):
    where = f"WHERE updated_at > now() - interval '{ALERT_ACTIVE_WINDOW}'" if active_only else ""
    with db_cursor() as cur:
        cur.execute(f"SELECT *, updated_at > now() - interval '{ALERT_ACTIVE_WINDOW}' AS active "
                    f"FROM patient_alerts {where} ORDER BY updated_at DESC LIMIT %s", (limit,))
        return {"alerts": cur.fetchall()}


# ---------------------------------------------------------------------------
# Daily reports (batch view)
# ---------------------------------------------------------------------------
@app.get("/api/reports/daily")
def list_reports(limit: int = Query(20, ge=1, le=200)):
    with db_cursor() as cur:
        cur.execute("SELECT DISTINCT ON (source_file) source_file, report_day, run_id, finished_at, "
                    "rows_written, patients, high_risk_patients FROM batch_runs "
                    "WHERE status = 'success' ORDER BY source_file, finished_at DESC")
        runs = sorted(cur.fetchall(), key=lambda r: r["finished_at"], reverse=True)[:limit]
    return {"reports": runs}


def _report(source_file: str):
    with db_cursor() as cur:
        rows = fetch_report_rows(cur, source_file)
    if not rows:
        raise HTTPException(status_code=404, detail=f"no report for {source_file}")
    return consolidate(rows)


@app.get("/api/reports/daily/latest")
def latest_report():
    with db_cursor() as cur:
        source_file = latest_source_file(cur)
    if not source_file:
        raise HTTPException(status_code=404, detail="no daily report generated yet")
    return _report(source_file)


@app.get("/api/reports/daily/{source_file}")
def report_by_file(source_file: str):
    return _report(source_file)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------
@app.get("/api/pipeline/alerts")
def pipeline_alerts(status: str = Query("open", pattern="^(open|resolved|all)$"),
                    limit: int = Query(50, ge=1, le=500)):
    where = "" if status == "all" else "WHERE status = %s"
    params = (limit,) if status == "all" else (status, limit)
    with db_cursor() as cur:
        cur.execute(f"SELECT * FROM pipeline_alerts {where} ORDER BY last_seen DESC LIMIT %s", params)
        return {"alerts": cur.fetchall()}


def _metrics_and_rules():
    with db_cursor() as cur:
        metrics = collect_metrics(cur, settings)
    results = evaluate_rules(metrics, load_rules(), settings.simulated_day_seconds)
    return metrics, results


@app.get("/health")
def health():
    try:
        metrics, results = _metrics_and_rules()
    except HTTPException:
        return JSONResponse({"status": "down", "database": "unreachable"}, status_code=503)
    breached = [r for r in results if r.breached]
    status = ("critical" if any(r.severity == "critical" for r in breached)
              else "degraded" if breached else "ok")
    return {"status": status, "database": "ok",
            "rules": [{"name": r.name, "severity": r.severity, "breached": r.breached,
                       "value": r.value, "threshold": r.threshold, "description": r.description}
                      for r in results],
            "metrics": metrics}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    try:
        values, results = _metrics_and_rules()
    except HTTPException:
        return PlainTextResponse("# HELP hospital_db_up 1 if Postgres is reachable\n"
                                 "# TYPE hospital_db_up gauge\nhospital_db_up 0\n")
    return PlainTextResponse(to_prometheus(values, {r.name: r.breached for r in results}),
                             media_type="text/plain; version=0.0.4")


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(DASHBOARD, media_type="text/html")
