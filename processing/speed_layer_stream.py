"""SPEED LAYER - Spark Structured Streaming job (Member B).

Kafka `vitals-stream` (Avro, schemaless, one record per reading)
  -> decode with vitals.avsc (Member A's schema, single source of truth)
  -> clean: drop undecodable / incomplete / physically impossible readings
  -> flag abnormal readings (same thresholds as the producer)
  -> two streaming queries sharing the same parsed input:

  1. `vitals_raw_to_master`  (append, every 5s)
     valid readings -> Postgres `vitals_readings` (the Lambda master dataset
     the batch layer recomputes from). Keyed by (partition, offset) and written
     with ON CONFLICT DO NOTHING, so a replay after a crash is idempotent.

  2. `vitals_window_summary` (update mode, every 10s)
     30-second windows sliding every 10s per patient, 30s watermark ->
     UPSERT into `vitals_live_summary` (real-time serving view) and
     per-patient threshold alerts -> `patient_alerts`.

The driver loop publishes a heartbeat (query progress + counters) to
`pipeline_heartbeat` so the health checks can tell "producer silent" apart
from "speed layer dead".

Run locally / in Docker:   python -m processing.speed_layer_stream
Run on Databricks:         see README ("Running the speed layer on Databricks")
"""
import logging
import sys
import time
from datetime import datetime, timezone
from functools import partial

from common.config import REPO_ROOT, Settings, get_settings, is_databricks
from common.db import execute_values, transaction, write_heartbeat
from common.logging_utils import ALERT_PREFIX, get_logger, log_event
from processing import vitals_rules
from processing.spark_session import get_spark, spark_packages

COMPONENT = "speed_layer"
logger = get_logger(COMPONENT)

AVRO_SCHEMA_PATH = REPO_ROOT / "vitals.avsc"

# Cumulative counters since the job started (reported in the heartbeat).
COUNTERS = {"raw_rows_seen": 0, "raw_rows_written": 0, "raw_rows_rejected": 0,
            "windows_upserted": 0, "alerts_opened": 0, "alerts_extended": 0}


# ---------------------------------------------------------------------------
# Source + transformations (pure DataFrame functions - unit-testable)
# ---------------------------------------------------------------------------
def load_avro_schema() -> str:
    return AVRO_SCHEMA_PATH.read_text(encoding="utf-8")


def kafka_options(s: Settings) -> dict:
    opts = {
        "kafka.bootstrap.servers": s.kafka_bootstrap,
        "subscribe": s.kafka_topic,
        "startingOffsets": s.kafka_starting_offsets,
        "maxOffsetsPerTrigger": str(s.kafka_max_offsets_per_trigger),
        # Confluent Cloud retention may delete old segments while we are down;
        # continue from what is available instead of failing the query.
        "failOnDataLoss": "false",
        "kafka.security.protocol": s.kafka_security_protocol,
    }
    if s.kafka_security_protocol.upper().startswith("SASL"):
        # Databricks shades the Kafka client -> the login module lives under
        # the `kafkashaded.` package there (see MEMBER_B_HANDOFF.md section 3).
        prefix = "kafkashaded." if is_databricks() else ""
        opts.update({
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                f"{prefix}org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{s.kafka_api_key}" password="{s.kafka_api_secret}";'),
        })
    return opts


def parse_vitals(kafka_df, avro_schema_json: str):
    """Decode Avro payloads and attach lineage + quality columns."""
    from pyspark.sql import functions as F
    from pyspark.sql.avro.functions import from_avro

    decoded = kafka_df.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        # PERMISSIVE: a corrupt payload becomes a NULL struct instead of
        # killing the stream; it is then rejected by the plausibility rule.
        from_avro(F.col("value"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("v"),
    )
    parsed = decoded.select(
        "kafka_partition", "kafka_offset",
        F.upper(F.trim(F.col("v.patient_id"))).alias("patient_id"),
        F.col("v.heart_rate").alias("heart_rate"),
        F.col("v.spo2").cast("double").alias("spo2"),
        F.col("v.systolic_bp").alias("systolic_bp"),
        F.col("v.diastolic_bp").alias("diastolic_bp"),
        F.col("v.temperature").cast("double").alias("temperature"),
        # Event time from the device; fall back to broker time if unparseable.
        F.coalesce(F.to_timestamp(F.col("v.timestamp")), F.col("kafka_timestamp"))
         .alias("event_time"),
    )
    return add_quality_columns(parsed)


def add_quality_columns(df):
    from pyspark.sql import functions as F

    return (df
            .withColumn("is_valid", F.coalesce(vitals_rules.plausible_reading_col(), F.lit(False)))
            .withColumn("is_abnormal", F.coalesce(
                vitals_rules.abnormal_col(F.col("heart_rate"), F.col("spo2"), F.col("temperature")),
                F.lit(False))))


def windowed_summary(valid_df, window_duration: str, window_slide: str, watermark: str):
    """Per-patient sliding-window aggregation (event time, watermarked)."""
    from pyspark.sql import functions as F

    return (valid_df
            .withWatermark("event_time", watermark)
            .groupBy(F.window("event_time", window_duration, window_slide), "patient_id")
            .agg(F.avg("heart_rate").alias("avg_heart_rate"),
                 F.avg("spo2").alias("avg_spo2"),
                 F.avg("temperature").alias("avg_temperature"),
                 F.sum(F.col("is_abnormal").cast("int")).alias("abnormal_count"),
                 F.count(F.lit(1)).alias("total_readings"),
                 F.max("heart_rate").alias("max_heart_rate"),
                 F.min("heart_rate").alias("min_heart_rate"),
                 F.min("spo2").alias("min_spo2"),
                 F.max("temperature").alias("max_temperature"))
            .select("patient_id",
                    F.col("window.start").cast("double").alias("window_start_epoch"),
                    F.col("window.end").cast("double").alias("window_end_epoch"),
                    "avg_heart_rate", "avg_spo2", "avg_temperature", "abnormal_count",
                    "total_readings", "max_heart_rate", "min_heart_rate", "min_spo2",
                    "max_temperature"))


def _utc(epoch_seconds: float) -> datetime:
    # Timestamps cross the Spark->Python boundary as epoch seconds to avoid
    # PySpark's conversion to the *local* timezone of the driver process.
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Sinks (foreachBatch). Micro-batches here are tiny (15 patients, one reading
# every 2s), so rows are collected to the driver and written with psycopg2,
# which gives us UPSERT semantics that Spark's JDBC sink cannot.
# ---------------------------------------------------------------------------
RAW_INSERT_SQL = """
    INSERT INTO vitals_readings (kafka_partition, kafka_offset, patient_id, heart_rate, spo2,
                                 systolic_bp, diastolic_bp, temperature, is_abnormal, event_time)
    VALUES %s
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""

WINDOW_UPSERT_SQL = """
    INSERT INTO vitals_live_summary (patient_id, window_start, window_end, avg_heart_rate, avg_spo2,
                                     avg_temperature, abnormal_count, total_readings, computed_at)
    VALUES %s
    ON CONFLICT (patient_id, window_start, window_end) DO UPDATE SET
        avg_heart_rate  = EXCLUDED.avg_heart_rate,
        avg_spo2        = EXCLUDED.avg_spo2,
        avg_temperature = EXCLUDED.avg_temperature,
        abnormal_count  = EXCLUDED.abnormal_count,
        total_readings  = EXCLUDED.total_readings,
        computed_at     = EXCLUDED.computed_at
"""
WINDOW_UPSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, timezone('UTC', now()))"

ALERT_EXTEND_SQL = """
    UPDATE patient_alerts SET
        breaching_windows = breaching_windows
                            + CASE WHEN %(window_end)s > last_window_end THEN 1 ELSE 0 END,
        last_window_end   = GREATEST(last_window_end, %(window_end)s),
        reasons  = CASE WHEN %(severity)s = 'critical' OR severity = 'warning'
                        THEN %(reasons)s ELSE reasons END,
        severity = CASE WHEN severity = 'critical' OR %(severity)s = 'critical'
                        THEN 'critical' ELSE 'warning' END,
        max_heart_rate  = GREATEST(max_heart_rate, %(max_heart_rate)s),
        min_heart_rate  = LEAST(min_heart_rate, %(min_heart_rate)s),
        min_spo2        = LEAST(min_spo2, %(min_spo2)s),
        max_temperature = GREATEST(max_temperature, %(max_temperature)s),
        updated_at = now()
    WHERE alert_id = (SELECT alert_id FROM patient_alerts
                      WHERE patient_id = %(patient_id)s AND last_window_end >= %(window_start)s
                      ORDER BY last_window_end DESC LIMIT 1)
    RETURNING alert_id
"""

ALERT_INSERT_SQL = """
    INSERT INTO patient_alerts (patient_id, severity, reasons, first_window_start, last_window_end,
                                max_heart_rate, min_heart_rate, min_spo2, max_temperature)
    VALUES (%(patient_id)s, %(severity)s, %(reasons)s, %(window_start)s, %(window_end)s,
            %(max_heart_rate)s, %(min_heart_rate)s, %(min_spo2)s, %(max_temperature)s)
    RETURNING alert_id
"""


def write_raw_batch(batch_df, batch_id: int, settings: Settings) -> None:
    started = time.monotonic()
    rows = batch_df.select(
        "kafka_partition", "kafka_offset", "patient_id", "heart_rate", "spo2", "systolic_bp",
        "diastolic_bp", "temperature", "is_abnormal", "is_valid",
        batch_df["event_time"].cast("double").alias("event_epoch")).collect()
    if not rows:
        return
    valid = [(r.kafka_partition, r.kafka_offset, r.patient_id, r.heart_rate, r.spo2,
              r.systolic_bp, r.diastolic_bp, r.temperature, r.is_abnormal, _utc(r.event_epoch))
             for r in rows if r.is_valid]
    rejected = [r for r in rows if not r.is_valid]
    if valid:
        with transaction(settings) as cur:
            execute_values(cur, RAW_INSERT_SQL, valid)
    COUNTERS["raw_rows_seen"] += len(rows)
    COUNTERS["raw_rows_written"] += len(valid)
    COUNTERS["raw_rows_rejected"] += len(rejected)
    for r in rejected[:5]:  # sample of rejects for diagnosis, not the full list
        log_event(logger, logging.WARNING, "Rejected invalid vitals reading", batch_id=batch_id,
                  kafka_partition=r.kafka_partition, kafka_offset=r.kafka_offset,
                  patient_id=r.patient_id, heart_rate=r.heart_rate, spo2=r.spo2,
                  temperature=r.temperature)
    log_event(logger, logging.INFO, "Raw micro-batch stored in master dataset",
              query="vitals_raw_to_master", batch_id=batch_id, rows_in=len(rows),
              rows_written=len(valid), rows_rejected=len(rejected),
              abnormal=sum(1 for v in valid if v[8]),
              duration_ms=int((time.monotonic() - started) * 1000))


def write_window_batch(batch_df, batch_id: int, settings: Settings) -> None:
    started = time.monotonic()
    rows = batch_df.orderBy("window_start_epoch").collect()
    if not rows:
        return
    summary_rows = [(r.patient_id, _utc(r.window_start_epoch), _utc(r.window_end_epoch),
                     r.avg_heart_rate, r.avg_spo2, r.avg_temperature, r.abnormal_count,
                     r.total_readings) for r in rows]
    opened = extended = 0
    with transaction(settings) as cur:
        execute_values(cur, WINDOW_UPSERT_SQL, summary_rows, template=WINDOW_UPSERT_TEMPLATE)
        for r in rows:
            severity, reasons = vitals_rules.classify_window_alert(
                r.avg_heart_rate, r.avg_spo2, r.avg_temperature, r.abnormal_count)
            if severity is None:
                continue
            params = {"patient_id": r.patient_id, "severity": severity,
                      "reasons": "; ".join(reasons),
                      "window_start": _utc(r.window_start_epoch),
                      "window_end": _utc(r.window_end_epoch),
                      "max_heart_rate": r.max_heart_rate, "min_heart_rate": r.min_heart_rate,
                      "min_spo2": r.min_spo2, "max_temperature": r.max_temperature}
            # Overlapping/adjacent breaching windows extend the open episode.
            cur.execute(ALERT_EXTEND_SQL, params)
            if cur.fetchone():
                extended += 1
                continue
            cur.execute(ALERT_INSERT_SQL, params)
            alert_id = cur.fetchone()["alert_id"]
            opened += 1
            log_event(logger, logging.CRITICAL if severity == "critical" else logging.WARNING,
                      f"{ALERT_PREFIX} patient {r.patient_id} {severity} - {params['reasons']}",
                      alert_type="patient_vitals", alert_id=alert_id, patient_id=r.patient_id,
                      severity=severity, window_start=params["window_start"],
                      window_end=params["window_end"])
    COUNTERS["windows_upserted"] += len(summary_rows)
    COUNTERS["alerts_opened"] += opened
    COUNTERS["alerts_extended"] += extended
    log_event(logger, logging.INFO, "Window micro-batch upserted to vitals_live_summary",
              query="vitals_window_summary", batch_id=batch_id, windows=len(summary_rows),
              patients=len({r.patient_id for r in rows}), alerts_opened=opened,
              alerts_extended=extended, duration_ms=int((time.monotonic() - started) * 1000))


# ---------------------------------------------------------------------------
# Driver: start queries, heartbeat, fail fast
# ---------------------------------------------------------------------------
def _progress(query) -> dict:
    p = query.lastProgress
    if not p:
        return {"active": query.isActive, "status": query.status.get("message")}
    return {
        "active": query.isActive,
        "status": query.status.get("message"),
        "batch_id": p.get("batchId"),
        "input_rows": p.get("numInputRows"),
        "input_rows_per_sec": p.get("inputRowsPerSecond"),
        "processed_rows_per_sec": p.get("processedRowsPerSecond"),
        "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"),
        "watermark": (p.get("eventTime") or {}).get("watermark"),
        "state_rows": sum(op.get("numRowsTotal", 0) for op in p.get("stateOperators", [])),
    }


def _heartbeat(queries, settings: Settings, started_at: str) -> None:
    details = {"started_at": started_at, "counters": dict(COUNTERS),
               "queries": {q.name: _progress(q) for q in queries}}
    try:
        with transaction(settings) as cur:
            write_heartbeat(cur, COMPONENT, details)
    except Exception as exc:  # never let monitoring kill the stream
        log_event(logger, logging.ERROR, "Heartbeat write failed", error=str(exc))
        return
    log_event(logger, logging.INFO, "Speed layer heartbeat", **details)


def start_queries(spark, settings: Settings):
    avro_schema = load_avro_schema()
    kafka_df = spark.readStream.format("kafka").options(**kafka_options(settings)).load()
    parsed = parse_vitals(kafka_df, avro_schema)
    ckpt = settings.checkpoint_dir.rstrip("/")

    raw_query = (parsed.writeStream
                 .queryName("vitals_raw_to_master")
                 .foreachBatch(partial(write_raw_batch, settings=settings))
                 .option("checkpointLocation", f"{ckpt}/vitals_raw_to_master")
                 .trigger(processingTime=settings.raw_trigger)
                 .start())

    summary = windowed_summary(parsed.filter("is_valid"), settings.window_duration,
                               settings.window_slide, settings.watermark_delay)
    window_query = (summary.writeStream
                    .queryName("vitals_window_summary")
                    .outputMode("update")
                    .foreachBatch(partial(write_window_batch, settings=settings))
                    .option("checkpointLocation", f"{ckpt}/vitals_window_summary")
                    .trigger(processingTime=settings.agg_trigger)
                    .start())
    return [raw_query, window_query]


def run_speed_layer(spark=None, settings: Settings = None) -> None:
    """Start both queries and block until one of them stops (raises on failure)."""
    settings = settings or get_settings()
    if not settings.kafka_bootstrap:
        raise RuntimeError("BOOTSTRAP_SERVER is not configured (see .env.example)")
    spark = spark or get_spark("hospital-speed-layer",
                               spark_packages(kafka=True, avro=True))
    started_at = datetime.now(timezone.utc).isoformat()
    queries = start_queries(spark, settings)
    log_event(logger, logging.INFO, "Speed layer started", topic=settings.kafka_topic,
              starting_offsets=settings.kafka_starting_offsets,
              window=settings.window_duration, slide=settings.window_slide,
              watermark=settings.watermark_delay, checkpoint_dir=settings.checkpoint_dir,
              queries=[q.name for q in queries])
    try:
        while not spark.streams.awaitAnyTermination(settings.heartbeat_interval_seconds):
            _heartbeat(queries, settings, started_at)
    except KeyboardInterrupt:
        log_event(logger, logging.INFO, "Speed layer stopping (interrupted)")
    finally:
        failed = [(q.name, q.exception()) for q in queries if q.exception() is not None]
        for q in queries:
            if q.isActive:
                q.stop()
        _heartbeat(queries, settings, started_at)
    if failed:
        for name, exc in failed:
            log_event(logger, logging.ERROR, f"{ALERT_PREFIX} streaming query failed",
                      alert_type="pipeline", query=name, error=str(exc))
        raise RuntimeError(f"Streaming query failed: {failed[0][0]}")


def main() -> int:
    try:
        run_speed_layer()
    except Exception as exc:
        log_event(logger, logging.ERROR, "Speed layer terminated with error", error=str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
