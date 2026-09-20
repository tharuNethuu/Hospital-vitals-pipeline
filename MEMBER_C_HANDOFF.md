# Handoff to Member C — Orchestration, Storage & Observability

**Project:** Hospital Patient Vitals Monitoring (Lambda Architecture)
**Your role:** Airflow orchestration (batch scheduling), Postgres storage/serving layer support, and pipeline-wide observability (logging, metrics, alerting)
**Status as of handoff:** Ingestion layer complete. Spark speed-layer streaming job drafted and tested (Kafka→Databricks confirmed working); batch join job and Postgres writes not yet done by Member B. Postgres tables already created.

---

## 1. What's already done

### Ingestion (Member A)
- `vitals_producer.py` — streams simulated bedside vitals to Kafka topic `vitals-stream` every 2 seconds, for a fixed pool of 15 patients (`P100`–`P114`). ~10% of readings are intentionally abnormal.
- `lab_results_simulator.py` — drops a CSV file every 120 seconds (1 simulated day = 120s) into a local `lab_results_drops/` folder, containing lab results for a random subset of the same 15 patients.
- Both scripts have structured JSON logging (INFO/WARNING/ERROR levels).
- Kafka topics on shared Confluent Cloud cluster: `vitals-stream` (in use), `lab-results-batch` (created, unused — lab results are local files, not Kafka), `vitals-alerts` (created, unused so far).

### Processing (Member B — in progress)
- Databricks Community Edition set up, Kafka↔Databricks connectivity confirmed working (with a Databricks-specific JAAS `kafkashaded.` prefix fix — already solved, documented in her handoff).
- Speed layer: Avro deserialization + abnormal-reading flagging + 30-second windowed aggregation per patient — drafted and tested against live data in a temporary in-memory sink.
- **Not yet done by her:** writing the stream to the `vitals_live_summary` Postgres table (next immediate step for her), and the full batch join job.

Full schema/topic reference: `docs/SCHEMA_AND_TOPICS.md`. Member B's full handoff: `docs/MEMBER_B_HANDOFF.md` (worth reading for context on what her batch job will output).

---

## 2. Your task, broken into three parts

### Part A — Storage/Serving layer support
Postgres tables are already created (Supabase). Your job here is mostly to:
- Confirm the schema fits what's actually needed as Member B's job matures (may need small adjustments — coordinate with her)
- Set up any **serving-layer access** the brief expects: the rubric mentions "consolidated report/dashboard" and the use case's suggested output is an **API endpoint to return real-time ward monitoring figures**. Simplest approach: a small script (Flask/FastAPI) that queries `vitals_live_summary` and `daily_risk_report` and returns JSON — doesn't need to be fancy, just functional and demoable.

### Part B — Orchestration (Airflow)
This is your core, most substantial task. You need an Airflow DAG that:
1. **Triggers once per simulated day** (matching the 120-second simulated day used elsewhere — coordinate with the team if this changes)
2. **Waits for/detects** that day's lab results CSV has been dropped into `lab_results_drops/` (a `FileSensor` or simple existence check)
3. **Triggers Member B's batch join job** (the Spark batch script that joins vitals summary + lab results and writes `daily_risk_report`)
4. **Logs success/failure** of each step (ties into Part C below)

### Part C — Observability (pipeline-wide)
The brief requires, at minimum:
- Structured logging across ingestion, processing, and storage stages — **ingestion is done** (see Member A's pattern below, reuse it for consistency). Processing/storage logging is your responsibility to add if Member B's job doesn't already have it — check with her.
- **At least one basic alert/health-check rule.** Suggested options (pick one, or do more if time allows):
  - "No vitals data received in the last N minutes" (staleness check — query `vitals_live_summary` for latest `computed_at`, alert if too old)
  - "Error rate above threshold" (e.g., count of `abnormal_count` spikes, or Kafka delivery failures logged as ERROR, exceeding a threshold in a time window)
  - Simplest to implement: a small scheduled Airflow task that checks the staleness condition and logs a `CRITICAL`/alert-level log line (or even just prints a clear "ALERT:" message) if triggered — doesn't need email/Slack integration unless you want to add that for extra polish

---

## 3. Environment setup

### Airflow — recommended approach given no Docker
Given the team's storage constraint, avoid a full local Airflow install (it pulls in a lot of dependencies and a metadata DB). Two options:

**Option A (recommended): pip install directly**
```bash
pip install apache-airflow
```
This can be heavier than expected (many dependencies). If your machine has room (or you're using a teammate's machine as discussed), this is the standard approach and matches what most courses expect.

**Option B: If storage is still tight — a free-tier hosted Airflow (e.g., Astronomer Cloud trial)**
Slower to set up initially but avoids local footprint entirely. Only worth it if Option A genuinely doesn't fit on your machine.

Given your team already leaned on a teammate's machine for Docker-related needs earlier, the same approach probably works fine here — try Option A first.

### Postgres (Supabase) — connection details
Get the host/port/db name/user/password from Member A or Member B directly (not stored in this doc). You'll need `psycopg2` or `apache-airflow-providers-postgres` installed to connect from Airflow:
```bash
pip install apache-airflow-providers-postgres
```

---

## 4. Postgres tables you'll be working with

```sql
CREATE TABLE vitals_live_summary (
    patient_id TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_heart_rate FLOAT,
    avg_spo2 FLOAT,
    avg_temperature FLOAT,
    abnormal_count INT,
    total_readings INT,
    computed_at TIMESTAMP DEFAULT now()
);

CREATE TABLE daily_risk_report (
    report_day INT,
    patient_id TEXT,
    avg_heart_rate FLOAT,
    avg_spo2 FLOAT,
    vitals_abnormal_count INT,
    lab_test_type TEXT,
    lab_result_value FLOAT,
    lab_abnormal BOOLEAN,
    risk_flag TEXT,
    generated_at TIMESTAMP DEFAULT now()
);
```

`vitals_live_summary` is written continuously by Member B's speed-layer job (once she finishes that step). `daily_risk_report` is written by her batch job, triggered by your Airflow DAG.

---

## 5. Structured logging pattern already in use (reuse this format for consistency)

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","component":"YOUR_COMPONENT_NAME","message":"%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)
logger = logging.getLogger("YOUR_COMPONENT_NAME")
```

Use `logger.info()` for normal flow, `logger.warning()` for concerning-but-expected events, `logger.error()` for actual failures. For your alert rule specifically, consider a distinct marker (e.g., prefixing the message with `"ALERT:"`) so it's easy to point to in your report/demo as satisfying the alerting requirement.

---

## 6. Key open items / decisions still needed from the team

1. **Databricks Secrets not yet set up** — Member B's notebook currently uses hardcoded credentials during dev. If your Airflow DAG needs to trigger her Databricks job (e.g., via Databricks Jobs API), you'll need those credentials too — coordinate securely, and consider whether proper secrets management is worth implementing vs. documenting as a limitation in the report.
2. **How Airflow triggers the Spark batch job** — this needs a team decision:
   - If Member B's batch job runs as a Databricks notebook, Airflow can trigger it via the **Databricks Provider** (`apache-airflow-providers-databricks`) using a Databricks Jobs API call — this is the "proper" way and worth doing if time allows
   - Simpler fallback: if the batch job can also run as a local PySpark script, Airflow can just run it via `BashOperator`/`PythonOperator` — less impressive architecturally but faster to get working
   Recommend raising this with Member B once her batch job exists.
3. **Local file access for lab results** — `lab_results_drops/` is generated locally by whoever runs the ingestion script. Your Airflow FileSensor needs to run on a machine that can see this folder. If Airflow runs on a different machine than the ingestion simulator, this breaks — likely means running the full demo from one shared machine, or all team members' pieces co-located at demo time. Worth confirming with the team before demo day.
4. **Simulated day length (120 seconds)** — your DAG's schedule interval needs to match whatever the team settles on.

---

## 7. Where to find things in the repo

```
hospital-vitals-pipeline/
├── ingestion/               # Member A's completed work
├── processing/              # Member B's work (in progress)
├── orchestration/           # <-- YOUR FOLDER, put your Airflow DAGs here
├── storage/                 # <-- Postgres schema scripts, and any API/serving layer code
├── observability/           # <-- Alert rule scripts/config, shared logging setup notes
├── docs/
│   ├── SCHEMA_AND_TOPICS.md
│   └── MEMBER_B_HANDOFF.md
└── README.md
```

---

## 8. Rubric items your work directly covers (for context on what matters most)

| Rubric item | Marks | Covered by |
|---|---|---|
| Storage & Serving Layer | 10 | Your Part A |
| Observability | 10 | Your Part C |
| Data Ingestion / Processing (partial — orchestration ties these together) | — | Your Part B connects the pieces |

Observability and the serving layer are explicit, standalone rubric line items — make sure both are clearly demoable (visible logs, a working alert trigger, a working API/query endpoint) since graders will look for concrete evidence, not just code that theoretically does it.

---

**Questions?** Ping Member A for ingestion details/credentials, Member B for processing/Databricks details.
