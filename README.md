# Hospital Patient Vitals Monitoring — Lambda Pipeline

Use case 2 of the EC8203 mini-project. Bedside monitors stream vital signs; the pathology lab
uploads one lab-results file per simulated day. The platform answers:

> **Which patients show concerning vital-sign trends right now, and how do yesterday's lab
> results change the risk picture for those patients going forward?**

| Question part | Answered by | Where to see it |
|---|---|---|
| "right now" | **Speed layer**: Spark Structured Streaming, 30s windows sliding every 10s, per-patient threshold alerts | `GET /api/ward/live`, dashboard `http://localhost:8000/` |
| "how do labs change the picture" | **Batch layer**: daily Spark job recomputes the day from raw readings and joins it with the lab file | `daily_risk_report` table, `reports/*.md`, `GET /api/reports/daily/latest` |

**Simulated clock:** 1 simulated day = **120 s** real time (`SIMULATED_DAY_SECONDS`, must match
`lab_results_simulator.py`). Vitals arrive every 2 s for 15 patients (`P100`–`P114`).

---

## Architecture

```
 INGESTION (Member A)            PROCESSING (Member B)                     STORAGE / SERVING (Member C)
 ─────────────────────           ──────────────────────────────────────    ─────────────────────────────
 vitals_producer.py ──Avro──▶ Kafka "vitals-stream" ──▶ SPEED LAYER  (processing/speed_layer_stream.py)
  (every 2 s)                  (Confluent Cloud)         decode ▸ clean ▸ flag abnormal
                                                         ├─ query 1: raw readings ──────────▶ vitals_readings   (MASTER DATASET)
                                                         └─ query 2: 30s/10s windows ───────▶ vitals_live_summary (speed view)
                                                                     + threshold alerts ────▶ patient_alerts
                                                                                                  │
 lab_results_simulator.py ──CSV──▶ lab_results_drops/ ──▶ BATCH LAYER (processing/batch_layer_daily_report.py)
  (1 file / simulated day)                               lab CSV ▸ clean ▸ flag  ┐
                                                         vitals_readings (day) ──┴▶ join ▸ risk ─▶ daily_risk_report (batch view)
                                                              ▲                                     │
 ORCHESTRATION (Member C) ── Airflow DAG hospital_daily_risk_batch (every simulated day):          ▼
   wait_for_lab_files ▸ run_spark_batch_job ▸ validate_report_output ▸ publish_report ──▶ reports/*.md, *.csv

 SERVING: storage/api.py (FastAPI) merges speed + batch views ─▶ REST API + live dashboard + /metrics
 OBSERVABILITY: JSON logs in every stage · heartbeat · alert_rules.json evaluated every minute by the
                Airflow DAG pipeline_health_checks ─▶ pipeline_alerts · Prometheus rules (optional)
```

### Why Lambda (not Kappa)

* The two sources have **different natures**: an unbounded stream and a bounded daily file. The
  daily join with lab results is naturally a batch computation over a *closed* day.
* **Correctness over the day matters more than latency for the risk report.** The speed layer
  uses overlapping sliding windows (each reading is counted in 3 windows) and drops data later
  than the watermark; that is fine for "right now" but wrong for daily totals. The batch layer
  therefore **recomputes from the immutable master dataset** (`vitals_readings`, one row per Kafka
  offset), not from the speed view. Re-running a day (e.g. after late data) simply replaces its
  report — we demonstrated this after a speed-layer outage.
* Kappa would push the lab files through Kafka and compute everything as one stream-stream/
  stream-static join with long-lived state; replay would need Kafka retention of every raw
  reading, and correcting a day means replaying the topic. For a once-a-day reference feed this
  adds cost/complexity without a latency benefit. **Trade-off accepted:** two code paths (stream
  + batch) that must agree on rules — mitigated by sharing `processing/vitals_rules.py` and
  `common/config.py` thresholds between both paths.

### Technology choices (and why for *this* use case)

| Layer | Choice | Justification |
|---|---|---|
| Ingestion | Kafka (Confluent Cloud), Avro | Durable, replayable, partitioned by `patient_id` (per-patient ordering); Avro gives a compact typed contract (`vitals.avsc`). |
| Stream processing | Spark Structured Streaming | Event-time windows + watermarks for out-of-order bedside data; checkpoints give exactly-once *effect* with our idempotent sinks; the same engine/API serves the batch layer (one skill set, shared rules). Runs locally, in Docker, or on Databricks. |
| Batch processing | Spark (PySpark) batch | Same DataFrame code style as the stream; JDBC pushdown of the day's time range; scales to many wards without rewrite. |
| Orchestration | Apache Airflow | File sensor + dependency chain + retries + callbacks for the daily feed; visible run history for the demo. |
| Storage / serving | PostgreSQL (Supabase or local) | Small, relational, heavily joined data; `ON CONFLICT` upserts give idempotency for stream replays and batch retries; one store serves both views to the API. |
| Serving API | FastAPI | Lightweight JSON API + static dashboard; Prometheus `/metrics` export. |
| Observability | JSON logs, heartbeat table, rule engine, Prometheus (optional) | Rules are data (`alert_rules.json`), evaluated by Airflow every minute; alert lifecycle (open → resolved) is stored and shown on the dashboard. |

---

## Repository layout

```
vitals_producer.py, lab_results_simulator.py, vitals.avsc   Member A - ingestion (unchanged)
common/            config (env-driven), JSON logging, Postgres helpers
processing/        Member B - speed_layer_stream.py, batch_layer_daily_report.py,
                   vitals_rules.py (shared rules), lab_files.py (file discovery + completeness gate)
storage/           Member C - schema.sql, init_db.py, api.py (serving), report_export.py, static/dashboard.html
orchestration/     Member C - dags/hospital_daily_risk_batch.py, dags/pipeline_health_checks.py, airflow_callbacks.py
observability/     Member C - metrics.py, health_check.py, alert_rules.json, prometheus/
tests/             pytest (pure-Python + Spark transformation tests)
docker/, docker-compose.yml, requirements.txt, .env.example
```

---

## Running it

### 1. Configure

```bash
cp .env.example .env      # fill BOOTSTRAP_SERVER / API_KEY / API_SECRET (Confluent Cloud)
                          # optional: PG_* for Supabase; leave unset to use the bundled Postgres
```

### 2. Start processing + storage + orchestration + serving (Docker)

```bash
docker compose build
docker compose up -d                        # add  --profile monitoring  for Prometheus on :9090
```

| Service | URL |
|---|---|
| Ward dashboard | http://localhost:8000/ |
| API docs (Swagger) | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (admin / admin) |
| Prometheus (optional) | http://localhost:9090/alerts |
| Postgres (from host) | `localhost:5433`, db/user/password `hospital` |

`db-migrate` applies `storage/schema.sql` automatically (idempotent — also safe on Supabase,
where it only adds the new tables/columns/indexes).

### 3. Start the simulators on the host (from the repo root)

```bash
python vitals_producer.py            # terminal 1
python lab_results_simulator.py      # terminal 2 - writes lab_results_drops/, visible to Airflow
```

Within ~30 s the dashboard fills; after each lab file (+~10 s grace) Airflow runs the batch DAG and
the new report appears on the dashboard and in `reports/`.

### Running pieces without Docker

```bash
pip install -r requirements.txt                       # Java 17 needed for PySpark 3.5
python -m storage.init_db                             # apply schema
python -m processing.speed_layer_stream               # speed layer
python -m processing.batch_layer_daily_report --pending            # batch layer, all ready files
python -m processing.batch_layer_daily_report --lab-file lab_results_drops/<file>.csv   # recompute one day
python -m storage.report_export --latest              # write reports/*.md + *.csv
uvicorn storage.api:app --port 8000                   # serving API + dashboard
python -m observability.health_check --loop 30        # alert rules without Airflow
```
(Airflow does not run natively on Windows — use Docker or WSL.)

### Running the speed layer on Databricks

The job code is Databricks-aware (existing `spark` session, `kafkashaded.` JAAS prefix, credentials
via env vars or the `hospital-pipeline` secret scope). In a notebook attached to the repo (Repos):

```python
%pip install psycopg2-binary
```
```python
import os, sys
sys.path.append("/Workspace/Repos/<you>/Hospital-vitals-pipeline")
# Community Edition has no secret scopes: set credentials in a cell you clear before exporting.
os.environ.update({"BOOTSTRAP_SERVER": "...", "API_KEY": "...", "API_SECRET": "...",
                   "PG_HOST": "...", "PG_PORT": "5432", "PG_DB": "postgres",
                   "PG_USER": "...", "PG_PASSWORD": "...", "PG_SSLMODE": "require"})
from processing.speed_layer_stream import run_speed_layer
run_speed_layer(spark)
```

Only run the speed layer in **one** place at a time (Databricks *or* Docker). Running both would not
corrupt data — every sink is idempotent — but it wastes work. The **batch layer runs locally via
Airflow**: Databricks Community Edition has no REST API/Jobs, so Airflow cannot trigger it there,
and the lab files live on the machine running the simulator.

### Tests

```bash
docker compose run --rm --no-deps --entrypoint python api -m pytest -q     # 18 tests incl. Spark
```

---

## Data model (Postgres)

| Table | Written by | Purpose |
|---|---|---|
| `vitals_readings` | speed layer (query 1) | **Master dataset**: every valid reading, PK `(kafka_partition, kafka_offset)` ⇒ replay-safe |
| `vitals_live_summary` | speed layer (query 2) | 30s/10s sliding windows per patient, upserted on `(patient_id, window_start, window_end)` |
| `patient_alerts` | speed layer | Per-patient threshold alert **episodes** (overlapping breaching windows extend one alert) |
| `daily_risk_report` | batch layer | One row per (patient, lab test); original columns + trend/lab context (see `schema.sql`) |
| `batch_runs` | batch layer | Audit trail: run_id, file, status, rows, duration, error |
| `pipeline_heartbeat` | speed layer driver | Liveness + query progress + counters |
| `pipeline_alerts` | health checks, Airflow callbacks | Pipeline alert lifecycle (open → resolved) |

### Processing rules (`processing/vitals_rules.py`, thresholds in `common/config.py`)

* **Cleaning (speed):** undecodable Avro, bad patient id, missing/impossible values (HR 20–250,
  SpO2 50–100, temp 30–45 °C, BP 30–260, systolic > diastolic) are rejected and counted.
* **Abnormal reading** (same as the producer): HR < 60 or > 100, SpO2 < 95, temp > 37.5.
* **Patient alert (per window):** *critical* if window average SpO2 < 90, HR < 50 or > 130, temp ≥ 39;
  otherwise *warning* if the window contains an abnormal reading.
* **Batch "concerning vitals":** ≥ 20 % of the day's readings abnormal, or day-average HR/SpO2/temp
  outside the normal range. Also reports HR/SpO2 linear trends (`regr_slope`) per patient.
* **Lab abnormal:** result outside the parsed `reference_range`.
* **Risk:** `high` (vitals + labs) · `elevated_vitals` · `elevated_labs` (labs changed the picture) ·
  `normal`. `vitals_only_risk` and `risk_changed_by_labs` show the effect of the labs explicitly.

---

## Observability

* **Structured logs everywhere** — one JSON object per line with `timestamp, level, component,
  message` (+ `run_id`, `batch_id`, `patient_id`, row counts, durations). Airflow's `run_id` is
  passed into the Spark batch job and stored in `batch_runs`/`daily_risk_report`, so one run can be
  traced from the DAG through Spark to the stored rows. Alert lines start with **`ALERT:`**.
* **Metrics** (`GET /metrics`, Prometheus format): ingest freshness & end-to-end lag, readings and
  abnormal ratio (5 min), active patients, speed-layer heartbeat age / input rate / reject ratio,
  live-view freshness, batch success/failure/duration, lab-feed age, pending lab files, open alerts,
  and `hospital_health_rule_breached{rule=...}`.
* **Alert rules** (`observability/alert_rules.json`, evaluated every minute by the Airflow DAG
  `pipeline_health_checks`; mirrored in `observability/prometheus/alert_rules.yml`):

| Rule | Condition | Severity | Diagnoses |
|---|---|---|---|
| `vitals_stream_stale` | no reading stored for 60 s | critical | producer/Kafka/speed layer down |
| `speed_layer_down` | no heartbeat for 60 s | critical | Spark job crashed (vs. producer silent) |
| `live_summary_stale` | windows not updated for 90 s | warning | streaming aggregation stuck |
| `lab_feed_stale` | no lab file for 2.5 simulated days | warning | daily source late |
| `batch_backlog` | > 2 lab files waiting | warning | Airflow/batch falling behind |
| `batch_job_failing` | > 20 % batch runs failed (30 min) | critical | batch job broken |
| `data_quality_rejects_high` | > 5 % readings rejected | warning | producer/schema problem |
| `ward_abnormal_rate_high` | > 30 % abnormal readings (5 min) | warning | ward event or faulty sensors |

  Any failed Airflow task also raises an `ALERT:` and a `pipeline_alerts` row, which is resolved
  automatically by the next successful run of that task.

### Demo script (failure drill, ~3 min)

1. Everything running → dashboard `health: ok` (or `degraded` for warnings), Airflow DAGs green.
2. `docker compose stop speed-layer` → after ~60 s `vitals_stream_stale` + `speed_layer_down`
   open (dashboard "Pipeline health alerts", `ALERT:` lines in the `pipeline_health_checks` task log,
   task turns red, `/metrics` shows `hospital_health_rule_breached{...} 1`).
3. `docker compose start speed-layer` → the stream resumes from its checkpoint, **replays the missed
   Kafka offsets** (no gap in `vitals_readings`) and the alerts move to *resolved*.
4. The batch DAG's completeness gate waits until the replay has covered the day before building
   the report; a day computed too early can be recomputed with
   `python -m processing.batch_layer_daily_report --lab-file <file>`.

---

## Design notes / deviations from the handoff docs

* **Master dataset added (`vitals_readings`)** and the batch layer reads it instead of
  `vitals_live_summary`: with 30s windows sliding every 10s each reading appears in 3 windows, so
  summing window counts would triple-count, and recomputing from raw data is what makes this a
  Lambda batch layer.
* **Schema changes are additive only** (`storage/schema.sql`): unique index on
  `vitals_live_summary(patient_id, window_start, window_end)` for upserts (TRUNCATE the table first
  if earlier tests left duplicate windows), extra `daily_risk_report` columns, new tables above.
* **Batch job runs as local PySpark from Airflow** (BashOperator), not on Databricks — CE has no
  Jobs API and cannot see the local lab files (open items B-10.2 / C-6.2, C-6.3).
* **Report key is the lab file name**, not the day number: the simulator restarts at day 1, so day
  numbers repeat between sessions.
* **Credentials:** env vars / `.env` (git-ignored) or Databricks secret scope `hospital-pipeline`;
  nothing is hard-coded.
* **Density of the simulation:** 15 patients × one reading every 2 s ⇒ ~1 reading per patient per
  30 s window and ~8 readings per patient per simulated day. Window averages are therefore often a
  single reading; a longer day or a faster producer gives smoother trends.

## Limitations & what we'd change at production scale

* Micro-batches are collected to the driver and written with psycopg2 (fine for one ward; at scale
  use `foreachPartition` writers, or a Delta/Iceberg lake as master dataset instead of Postgres).
* Master dataset in Postgres keeps growing; production would partition by day and archive to
  Parquet/object storage with retention policies.
* Single-node Spark `local[2]`, single-broker test setups, LocalExecutor Airflow; production would
  use a cluster (Databricks Jobs / EMR), CeleryExecutor/Kubernetes and managed secrets.
* Alerts are stored/logged/exposed but not paged (no email/Slack/PagerDuty integration).
* Clinical thresholds are simplified population ranges, not patient-specific baselines (e.g. NEWS2).
