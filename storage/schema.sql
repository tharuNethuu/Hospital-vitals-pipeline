-- =============================================================================
-- Hospital vitals pipeline - storage / serving schema (PostgreSQL)
--
-- Idempotent: safe to run on the Supabase database where the two original
-- tables already exist (CREATE ... IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
-- Apply with:  python -m storage.init_db
--
-- Timestamp convention: every writer session runs with timezone=UTC, so the
-- naive TIMESTAMP columns of the two original tables hold UTC; new tables use
-- TIMESTAMPTZ.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. MASTER DATASET (Lambda "all data" layer): every valid raw reading, append
--    only, keyed by its Kafka coordinates so replays are idempotent.
--    Written by the speed layer, read by the batch layer.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vitals_readings (
    kafka_partition  INT          NOT NULL,
    kafka_offset     BIGINT       NOT NULL,
    patient_id       TEXT         NOT NULL,
    heart_rate       INT          NOT NULL,
    spo2             REAL         NOT NULL,
    systolic_bp      INT          NOT NULL,
    diastolic_bp     INT          NOT NULL,
    temperature      REAL         NOT NULL,
    is_abnormal      BOOLEAN      NOT NULL,
    event_time       TIMESTAMPTZ  NOT NULL,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_vitals_readings_event_time  ON vitals_readings (event_time);
CREATE INDEX IF NOT EXISTS ix_vitals_readings_patient     ON vitals_readings (patient_id, event_time);
CREATE INDEX IF NOT EXISTS ix_vitals_readings_ingested_at ON vitals_readings (ingested_at);

-- ---------------------------------------------------------------------------
-- 2. SPEED VIEW: 30s sliding-window aggregates per patient (original table).
--    Unique key added so the stream can UPSERT windows (update output mode).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vitals_live_summary (
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
-- NOTE: fails if the table already holds duplicate windows from earlier
-- testing; in that case TRUNCATE vitals_live_summary first.
CREATE UNIQUE INDEX IF NOT EXISTS ux_vitals_live_summary_window
    ON vitals_live_summary (patient_id, window_start, window_end);
CREATE INDEX IF NOT EXISTS ix_vitals_live_summary_window_end
    ON vitals_live_summary (window_end DESC);

-- ---------------------------------------------------------------------------
-- 3. Per-patient threshold alerts (speed layer). One row per alert *episode*:
--    consecutive/overlapping breaching windows extend the same episode instead
--    of raising a new alert for every sliding window.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patient_alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    patient_id        TEXT        NOT NULL,
    severity          TEXT        NOT NULL CHECK (severity IN ('warning', 'critical')),
    reasons           TEXT        NOT NULL,
    first_window_start TIMESTAMPTZ NOT NULL,
    last_window_end   TIMESTAMPTZ NOT NULL,
    max_heart_rate    FLOAT,
    min_heart_rate    FLOAT,
    min_spo2          FLOAT,
    max_temperature   FLOAT,
    breaching_windows INT         NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_patient_alerts_patient ON patient_alerts (patient_id, last_window_end DESC);
CREATE INDEX IF NOT EXISTS ix_patient_alerts_updated ON patient_alerts (updated_at DESC);

-- ---------------------------------------------------------------------------
-- 4. BATCH VIEW: daily consolidated risk report (original table + extra
--    columns for the trend/lab context the business question asks for).
--    One row per (patient, lab test); patients without labs get one row.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_risk_report (
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
ALTER TABLE daily_risk_report
    ADD COLUMN IF NOT EXISTS source_file                TEXT,
    ADD COLUMN IF NOT EXISTS run_id                     TEXT,
    ADD COLUMN IF NOT EXISTS window_start               TIMESTAMP,
    ADD COLUMN IF NOT EXISTS window_end                 TIMESTAMP,
    ADD COLUMN IF NOT EXISTS total_readings             INT,
    ADD COLUMN IF NOT EXISTS abnormal_ratio             FLOAT,
    ADD COLUMN IF NOT EXISTS avg_temperature            FLOAT,
    ADD COLUMN IF NOT EXISTS min_spo2                   FLOAT,
    ADD COLUMN IF NOT EXISTS max_heart_rate             INT,
    ADD COLUMN IF NOT EXISTS hr_trend_bpm_per_min       FLOAT,
    ADD COLUMN IF NOT EXISTS spo2_trend_pct_per_min     FLOAT,
    ADD COLUMN IF NOT EXISTS lab_reference_range        TEXT,
    ADD COLUMN IF NOT EXISTS lab_collected_at           TIMESTAMP,
    ADD COLUMN IF NOT EXISTS patient_abnormal_lab_count INT,
    ADD COLUMN IF NOT EXISTS vitals_concerning          BOOLEAN,
    ADD COLUMN IF NOT EXISTS vitals_only_risk           TEXT,
    ADD COLUMN IF NOT EXISTS risk_changed_by_labs       BOOLEAN,
    ADD COLUMN IF NOT EXISTS risk_reasons               TEXT;
CREATE INDEX IF NOT EXISTS ix_daily_risk_report_source ON daily_risk_report (source_file);
CREATE INDEX IF NOT EXISTS ix_daily_risk_report_day    ON daily_risk_report (report_day, patient_id);

-- ---------------------------------------------------------------------------
-- 5. OBSERVABILITY tables
-- ---------------------------------------------------------------------------
-- Liveness + progress of long-running components (speed layer driver).
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component  TEXT PRIMARY KEY,
    last_seen  TIMESTAMPTZ NOT NULL,
    details    JSONB
);

-- One row per batch-layer execution per lab file (audit trail + metrics).
CREATE TABLE IF NOT EXISTS batch_runs (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    source_file    TEXT        NOT NULL,
    report_day     INT,
    status         TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    rows_written   INT,
    patients       INT,
    high_risk_patients INT,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS ix_batch_runs_source ON batch_runs (source_file, status);
CREATE INDEX IF NOT EXISTS ix_batch_runs_started ON batch_runs (started_at DESC);

-- Pipeline-health alerts raised by observability/health_check.py.
-- At most one OPEN alert per rule; it is resolved when the rule recovers.
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id           BIGSERIAL PRIMARY KEY,
    rule_name    TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    message      TEXT        NOT NULL,
    metric_value FLOAT,
    status       TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_pipeline_alerts_open_rule
    ON pipeline_alerts (rule_name) WHERE status = 'open';
