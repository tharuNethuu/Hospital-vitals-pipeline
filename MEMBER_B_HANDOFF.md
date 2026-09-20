# Handoff to Member B — Spark Processing Layer

**Project:** Hospital Patient Vitals Monitoring (Lambda Architecture)
**Your role:** Stream + Batch processing layers using Spark (Databricks Community Edition)
**Status as of handoff:** Ingestion layer complete and pushed to repo. Kafka↔Databricks connectivity confirmed working. Windowed aggregation logic drafted and tested against live data.

---

## 1. What's already done (by Member A — Ingestion)

- `vitals_producer.py` — streams simulated bedside monitor readings to Kafka topic `vitals-stream` every 2 seconds, for a fixed pool of 15 patients (`P100`–`P114`). ~10% of readings are intentionally abnormal.
- `lab_results_simulator.py` — drops a CSV file every 120 seconds (1 simulated day = 120s) into `lab_results_drops/`, containing lab results for a random subset of the same 15 patients.
- Both scripts have structured JSON logging (INFO/WARNING/ERROR).
- Kafka topics created on our shared Confluent Cloud cluster:
  - `vitals-stream` — real-time vitals (**this is what you'll consume**)
  - `lab-results-batch` — created but unused (we decided to keep lab results as local file drops instead, not Kafka-based)
  - `vitals-alerts` — created but unused so far (optional, for future alerting)

Full schema/topic reference: see `docs/SCHEMA_AND_TOPICS.md` in the repo.

---

## 2. Your task: Processing Layer (Speed + Batch)

### Speed layer (real-time)
Spark Structured Streaming job that:
1. Reads `vitals-stream` from Kafka (Avro-encoded)
2. Deserializes using the vitals schema (below)
3. Flags abnormal readings (same thresholds as the producer — see below)
4. Computes a 30-second sliding window aggregation per patient (avg heart rate, avg SpO2, avg temperature, abnormal count, total readings)
5. Writes results continuously to a Postgres table: `vitals_live_summary`

### Batch layer (daily)
A separate Spark batch job (runs once per simulated day, triggered by Member C's Airflow DAG later) that:
1. Reads that day's `vitals_live_summary` data from Postgres
2. Reads that day's lab results CSV from `lab_results_drops/` (**note:** this folder is local, not cloud — see open issue below)
3. Joins both on `patient_id`
4. Computes a risk flag (e.g., "elevated" if both abnormal vitals trend AND abnormal lab result present)
5. Writes to Postgres table: `daily_risk_report`

---

## 3. Environment setup (what's already confirmed working)

- **Platform:** Databricks Community Edition — https://community.cloud.databricks.com
- **Cluster:** created, Spark 3.5.x (check `spark.version` in a notebook cell to confirm exact version)
- **Library installed on cluster:** `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` (Maven, under Cluster → Libraries) — match the version number to whatever `spark.version` shows

### ⚠️ Known Databricks-specific gotcha (already solved, don't re-debug this)
Databricks shades the Kafka client internally. Your JAAS config **must** use the `kafkashaded.` prefix, not the standard one:

```python
.option("kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";')
```

Using the unprefixed `org.apache.kafka...` version throws:
`KafkaIllegalStateException: No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule`

---

## 4. Credentials you'll need (get these from Member A directly — NOT stored in this doc or the repo)

- Confluent Cloud `BOOTSTRAP_SERVER`, `API_KEY`, `API_SECRET` (same cluster as ingestion — ask for these securely, e.g., via a private message, not Git)
- Supabase/Neon Postgres connection details (host, port, db name, user, password) — **team still needs to finalize this (see open items below)**

**⚠️ Important — not yet done:** We have NOT yet set up Databricks Secrets for these credentials. Right now they'd need to be hardcoded in your notebook to test. Please:
- Do NOT commit any notebook containing real credentials to the GitHub repo
- Either use Databricks Secrets (via CLI) if you have time, or keep credentials in a cell you clear before exporting/sharing the notebook
- We'll mention this as a known limitation in the report either way

---

## 5. Vitals Avro Schema (paste directly as a Python dict — no file upload needed in Databricks)

```python
avro_schema_json = json.dumps({
    "type": "record",
    "name": "VitalSign",
    "namespace": "com.miniproject.hospital",
    "fields": [
        {"name": "patient_id", "type": "string"},
        {"name": "heart_rate", "type": "int"},
        {"name": "spo2", "type": "float"},
        {"name": "systolic_bp", "type": "int"},
        {"name": "diastolic_bp", "type": "int"},
        {"name": "temperature", "type": "float"},
        {"name": "timestamp", "type": "string"}
    ]
})
```

## 6. Lab Results CSV columns (for the batch join later)
`patient_id, test_type, result_value, reference_range, collected_at`

Test types used: `Hemoglobin, WBC_Count, Creatinine, Glucose, CRP`

## 7. Abnormal reading thresholds (keep consistent across ingestion + processing)
- Heart rate: normal 60–100 bpm
- SpO2: normal ≥95%
- Temperature: abnormal if >37.5°C

---

## 8. Progress so far on your part (starting point — don't redo from scratch)

- Kafka → Databricks connectivity: **confirmed working**
- Avro deserialization + abnormal flagging + 30-second windowed aggregation: **drafted and tested**, results displayed successfully in a temporary in-memory sink (`writeStream.format("memory")`)
- **Not yet done:** writing the stream to Postgres (`vitals_live_summary` table) instead of the in-memory test sink — this is the next immediate step
- **Not yet done:** the batch layer join job entirely

## 9. Postgres tables already created (Supabase)

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

You'll need `psycopg2-binary` installed in your Databricks notebook (`%pip install psycopg2-binary`) to write to these.

---

## 10. Open items / decisions the team still needs to make

1. **Databricks Secrets** — not yet set up; credentials currently would be hardcoded during dev. Decide before final submission whether to implement properly or just document as a limitation.
2. **Local file access for lab results** — `lab_results_drops/` is generated locally by whoever runs `lab_results_simulator.py`. Since your batch job runs on Databricks (cloud), it can't see this folder directly. Options to discuss with the team:
   - Upload CSVs manually/periodically to Databricks (DBFS or a shared location) — simplest for a demo
   - Have Member A's script also push to a shared cloud location (adds scope)
   - Run the whole demo from one laptop where everything (simulators + notebook triggers) is co-located
3. **Simulated day length** — currently 120 seconds. If your windowing/batch logic needs a different cadence to demo well, raise it with the team before final demo recording.

---

## 11. Where to find things in the repo

```
hospital-vitals-pipeline/
├── ingestion/              # Member A's completed work
│   ├── vitals_producer.py
│   ├── lab_results_simulator.py
│   ├── vitals.avsc
│   └── lab_results.avsc
├── processing/              # <-- YOUR FOLDER, currently empty, put your notebooks/scripts here
├── orchestration/           # Member C's folder
├── storage/                 # DB schema scripts
├── observability/
├── docs/
│   └── SCHEMA_AND_TOPICS.md   # full schema + topic reference
└── README.md
```

Export your Databricks notebook as `.py` or `.ipynb` (with credentials cleared) into `processing/` when ready to commit.

---

**Questions?** Ping Member A directly for credentials and any context not covered here.
