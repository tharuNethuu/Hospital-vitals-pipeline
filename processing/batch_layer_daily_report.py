"""BATCH LAYER - daily consolidated patient risk report (Member B).

Triggered once per simulated day by the Airflow DAG `hospital_daily_risk_batch`
(or manually). For every lab-results file it:

  1. loads + cleans the lab CSV (types, reference-range parsing, de-duplication)
     and flags out-of-range results;
  2. recomputes the day's vitals from the MASTER DATASET (`vitals_readings`,
     raw readings) over the simulated day that closed when the file dropped:
     counts, abnormal ratio, averages, extremes and linear trends (regr_slope);
  3. joins both on patient_id (full outer: patients with vitals but no labs and
     vice-versa are both reported);
  4. derives risk before labs (vitals only) and after labs, with reasons:
        high            concerning vitals AND abnormal labs
        elevated_vitals concerning vitals only
        elevated_labs   abnormal labs only (labs changed the picture)
        normal
  5. replaces that file's rows in `daily_risk_report` in one transaction
     (idempotent - safe for Airflow retries) and records the run in
     `batch_runs` (audit + observability).

Recomputing from raw readings (not from the speed layer's sliding windows) is
the Lambda property: the batch view is exact even if the speed layer
double-counted overlapping windows, dropped late data or had a bug.

Usage:
    python -m processing.batch_layer_daily_report --lab-file lab_results_drops/lab_results_day3_1789136839.csv
    python -m processing.batch_layer_daily_report --pending      # all unprocessed files
"""
import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from common import config as cfg
from common.config import PATIENT_ID_PATTERN, Settings, get_settings, is_databricks
from common.db import execute_values, transaction
from common.logging_utils import ALERT_PREFIX, get_logger, log_event
from processing import vitals_rules
from processing.lab_files import (LabFile, fetch_latest_event_epoch,
                                  fetch_processed_file_names, list_lab_files, parse_lab_file,
                                  pending_lab_files, ready_for_batch)
from processing.spark_session import get_spark, spark_packages

COMPONENT = "batch_layer"
logger = get_logger(COMPONENT)

REFERENCE_RANGE_RE = r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*(.*)$"


# ---------------------------------------------------------------------------
# Transformations (pure DataFrame -> DataFrame, unit-tested in tests/)
# ---------------------------------------------------------------------------
def clean_lab_results(raw_df):
    """Type, validate and de-duplicate lab rows; flag abnormal results."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    typed = raw_df.select(
        F.upper(F.trim("patient_id")).alias("patient_id"),
        F.trim("test_type").alias("test_type"),
        F.col("result_value").cast("double").alias("result_value"),
        F.trim("reference_range").alias("reference_range"),
        F.to_timestamp("collected_at").alias("collected_at"),
    )
    with_range = (typed
                  .withColumn("ref_low", F.regexp_extract("reference_range", REFERENCE_RANGE_RE, 1))
                  .withColumn("ref_high", F.regexp_extract("reference_range", REFERENCE_RANGE_RE, 2))
                  .withColumn("ref_low", F.when(F.col("ref_low") != "", F.col("ref_low").cast("double")))
                  .withColumn("ref_high", F.when(F.col("ref_high") != "", F.col("ref_high").cast("double"))))
    valid = with_range.filter(
        F.col("patient_id").rlike(PATIENT_ID_PATTERN)
        & F.col("test_type").isNotNull() & (F.col("test_type") != "")
        & F.col("result_value").isNotNull()
        & F.col("ref_low").isNotNull() & F.col("ref_high").isNotNull())
    # Keep the latest result if a test was reported twice for a patient.
    latest_first = Window.partitionBy("patient_id", "test_type").orderBy(
        F.col("collected_at").desc_nulls_last(), F.col("result_value").desc())
    deduped = (valid.withColumn("_rn", F.row_number().over(latest_first))
               .filter("_rn = 1").drop("_rn"))
    return deduped.withColumn(
        "lab_abnormal",
        (F.col("result_value") < F.col("ref_low")) | (F.col("result_value") > F.col("ref_high")))


def summarise_vitals(readings_df):
    """Per-patient day summary from raw readings, including linear trends."""
    from pyspark.sql import functions as F

    minutes = F.col("event_time").cast("double") / 60.0
    return (readings_df
            .withColumn("t_min", minutes)
            .groupBy("patient_id")
            .agg(F.count(F.lit(1)).alias("total_readings"),
                 F.sum(F.col("is_abnormal").cast("int")).alias("vitals_abnormal_count"),
                 F.avg("heart_rate").alias("avg_heart_rate"),
                 F.avg("spo2").alias("avg_spo2"),
                 F.avg("temperature").alias("avg_temperature"),
                 F.min("spo2").alias("min_spo2"),
                 F.max("heart_rate").alias("max_heart_rate"),
                 # Least-squares slope of the vital over time (NULL with <2 points).
                 F.expr("regr_slope(heart_rate, t_min)").alias("hr_trend_bpm_per_min"),
                 F.expr("regr_slope(spo2, t_min)").alias("spo2_trend_pct_per_min"))
            .withColumn("abnormal_ratio",
                        F.col("vitals_abnormal_count") / F.col("total_readings")))


def build_risk_report(vitals_summary_df, labs_df):
    """Join vitals summary with labs and derive the risk picture.

    Output: one row per (patient, lab test); one row with NULL lab columns for
    patients that had no lab test that day.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    by_patient = Window.partitionBy("patient_id")
    labs = (labs_df
            .withColumn("patient_abnormal_lab_count",
                        F.sum(F.col("lab_abnormal").cast("int")).over(by_patient))
            .withColumn("abnormal_lab_tests", F.array_sort(F.collect_set(
                F.when(F.col("lab_abnormal"), F.col("test_type"))).over(by_patient))))

    joined = (vitals_summary_df.join(labs, on="patient_id", how="full_outer")
              .withColumn("total_readings", F.coalesce("total_readings", F.lit(0)))
              .withColumn("vitals_abnormal_count", F.coalesce("vitals_abnormal_count", F.lit(0)))
              .withColumn("patient_abnormal_lab_count",
                          F.coalesce("patient_abnormal_lab_count", F.lit(0))))

    vitals_concerning = F.coalesce(vitals_rules.vitals_concerning_col(), F.lit(False))
    labs_abnormal = F.col("patient_abnormal_lab_count") > 0
    reasons = F.concat_ws("; ",
        F.when(F.col("total_readings") == 0, F.lit("no vitals received in window")),
        F.when(F.col("abnormal_ratio") >= cfg.VITALS_ABNORMAL_RATIO_THRESHOLD,
               F.format_string("%.0f%% of %d readings abnormal",
                               F.col("abnormal_ratio") * 100, F.col("total_readings"))),
        F.when(F.col("avg_spo2") < cfg.SPO2_NORMAL_MIN,
               F.format_string("avg SpO2 %.1f%%", F.col("avg_spo2"))),
        F.when((F.col("avg_heart_rate") > cfg.HR_NORMAL_MAX) | (F.col("avg_heart_rate") < cfg.HR_NORMAL_MIN),
               F.format_string("avg HR %.0f bpm", F.col("avg_heart_rate"))),
        F.when(F.col("avg_temperature") > cfg.TEMP_NORMAL_MAX,
               F.format_string("avg temp %.1fC", F.col("avg_temperature"))),
        F.when(labs_abnormal, F.concat(F.lit("abnormal labs: "),
                                       F.array_join("abnormal_lab_tests", ", "))),
    )
    return (joined
            .withColumn("vitals_concerning", vitals_concerning)
            .withColumn("vitals_only_risk",
                        F.when(F.col("vitals_concerning"), F.lit("elevated")).otherwise(F.lit("normal")))
            .withColumn("risk_flag", vitals_rules.risk_flag_col(F.col("vitals_concerning"), labs_abnormal))
            .withColumn("risk_changed_by_labs", labs_abnormal)
            .withColumn("risk_reasons", reasons)
            .select("patient_id", "total_readings", "vitals_abnormal_count", "abnormal_ratio",
                    "avg_heart_rate", "avg_spo2", "avg_temperature", "min_spo2", "max_heart_rate",
                    "hr_trend_bpm_per_min", "spo2_trend_pct_per_min",
                    F.col("test_type").alias("lab_test_type"),
                    F.col("result_value").alias("lab_result_value"),
                    F.col("reference_range").alias("lab_reference_range"),
                    F.col("collected_at").cast("double").alias("lab_collected_epoch"),
                    "lab_abnormal", "patient_abnormal_lab_count", "vitals_concerning",
                    "vitals_only_risk", "risk_flag", "risk_changed_by_labs", "risk_reasons"))


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------
LAB_CSV_SCHEMA = ("patient_id STRING, test_type STRING, result_value STRING, "
                  "reference_range STRING, collected_at STRING")


def read_lab_csv(spark, path: str):
    return (spark.read.option("header", True).option("mode", "PERMISSIVE")
            .schema(LAB_CSV_SCHEMA).csv(path))


def read_vitals_window(spark, s: Settings, start: datetime, end: datetime):
    """Raw readings of the simulated day, via Spark JDBC with pushed-down range."""
    query = ("(SELECT patient_id, heart_rate, spo2, temperature, is_abnormal, event_time "
             "FROM vitals_readings "
             f"WHERE event_time >= '{start.isoformat()}' AND event_time < '{end.isoformat()}') "
             "AS day_readings")
    return (spark.read.format("jdbc")
            .option("url", s.jdbc_url)
            .option("dbtable", query)
            .option("user", s.pg_user)
            .option("password", s.pg_password)
            .option("driver", "org.postgresql.Driver")
            .load())


REPORT_COLUMNS = [
    "report_day", "patient_id", "avg_heart_rate", "avg_spo2", "vitals_abnormal_count",
    "lab_test_type", "lab_result_value", "lab_abnormal", "risk_flag", "generated_at",
    "source_file", "run_id", "window_start", "window_end", "total_readings", "abnormal_ratio",
    "avg_temperature", "min_spo2", "max_heart_rate", "hr_trend_bpm_per_min",
    "spo2_trend_pct_per_min", "lab_reference_range", "lab_collected_at",
    "patient_abnormal_lab_count", "vitals_concerning", "vitals_only_risk",
    "risk_changed_by_labs", "risk_reasons",
]


def _severity_rank(flag: str) -> int:
    return vitals_rules.RISK_SEVERITY_ORDER.index(flag)


def to_report_rows(report_rows, lab_file: LabFile, run_id: str, window_start, window_end):
    generated_at = datetime.now(timezone.utc)
    out = []
    for r in sorted(report_rows, key=lambda r: (_severity_rank(r.risk_flag), r.patient_id,
                                                 r.lab_test_type or "")):
        lab_collected = (datetime.fromtimestamp(r.lab_collected_epoch, tz=timezone.utc)
                         if r.lab_collected_epoch is not None else None)
        out.append((
            lab_file.day, r.patient_id, r.avg_heart_rate, r.avg_spo2, r.vitals_abnormal_count,
            r.lab_test_type, r.lab_result_value, r.lab_abnormal, r.risk_flag, generated_at,
            lab_file.name, run_id, window_start, window_end, r.total_readings, r.abnormal_ratio,
            r.avg_temperature, r.min_spo2, r.max_heart_rate, r.hr_trend_bpm_per_min,
            r.spo2_trend_pct_per_min, r.lab_reference_range, lab_collected,
            r.patient_abnormal_lab_count, r.vitals_concerning, r.vitals_only_risk,
            r.risk_changed_by_labs, r.risk_reasons,
        ))
    return out


def write_report(cur, rows, source_file: str) -> None:
    cur.execute("DELETE FROM daily_risk_report WHERE source_file = %s", (source_file,))
    if rows:
        execute_values(cur, f"INSERT INTO daily_risk_report ({', '.join(REPORT_COLUMNS)}) VALUES %s",
                       rows)


# ---------------------------------------------------------------------------
# Orchestration of one file
# ---------------------------------------------------------------------------
def process_lab_file(spark, s: Settings, lab_file: LabFile, run_id: str) -> dict:
    started = time.monotonic()
    window_start, window_end = lab_file.vitals_window(s.simulated_day_seconds)
    ctx = {"run_id": run_id, "source_file": lab_file.name, "report_day": lab_file.day}
    with transaction(s) as cur:
        cur.execute("INSERT INTO batch_runs (run_id, source_file, report_day, status) "
                    "VALUES (%s, %s, %s, 'running') RETURNING id",
                    (run_id, lab_file.name, lab_file.day))
        batch_run_pk = cur.fetchone()["id"]
        latest_event = fetch_latest_event_epoch(cur)
    log_event(logger, logging.INFO, "Batch run started", window_start=window_start,
              window_end=window_end, **ctx)
    if latest_event is None or latest_event < window_end.timestamp():
        # The speed layer has not stored readings up to the window end (yet):
        # the report may undercount. Re-running this file later recomputes it.
        log_event(logger, logging.WARNING, "Master dataset may be incomplete for this window",
                  latest_event_epoch=latest_event, window_end=window_end, **ctx)
    try:
        raw_labs = read_lab_csv(spark, str(lab_file.path)).cache()
        labs = clean_lab_results(raw_labs).cache()
        raw_count, lab_count = raw_labs.count(), labs.count()
        log_event(logger, logging.INFO, "Lab results loaded and cleaned", rows_raw=raw_count,
                  rows_clean=lab_count, rows_dropped=raw_count - lab_count,
                  abnormal_labs=labs.filter("lab_abnormal").count(), **ctx)

        readings = read_vitals_window(spark, s, window_start, window_end).cache()
        reading_count = readings.count()
        level = logging.INFO if reading_count else logging.WARNING
        log_event(logger, level, "Vitals loaded from master dataset",
                  readings=reading_count, window_start=window_start, window_end=window_end, **ctx)

        report = build_risk_report(summarise_vitals(readings), labs)
        rows = to_report_rows(report.collect(), lab_file, run_id, window_start, window_end)
        patients = {r[1] for r in rows}
        by_flag = {}
        for r in rows:
            by_flag.setdefault(r[8], set()).add(r[1])
        high = len(by_flag.get(vitals_rules.RISK_HIGH, ()))

        with transaction(s) as cur:
            write_report(cur, rows, lab_file.name)
            cur.execute("UPDATE batch_runs SET status='success', finished_at=now(), rows_written=%s, "
                        "patients=%s, high_risk_patients=%s WHERE id=%s",
                        (len(rows), len(patients), high, batch_run_pk))
        for cached in (raw_labs, labs, readings):
            cached.unpersist()

        stats = {"rows_written": len(rows), "patients": len(patients),
                 "patients_by_risk": {k: len(v) for k, v in by_flag.items()},
                 "duration_ms": int((time.monotonic() - started) * 1000)}
        log_event(logger, logging.INFO, "Daily risk report written", **stats, **ctx)
        for pid in sorted(by_flag.get(vitals_rules.RISK_HIGH, ())):
            log_event(logger, logging.WARNING, f"High-risk patient in daily report: {pid}",
                      patient_id=pid, **ctx)
        return stats
    except Exception as exc:
        with transaction(s) as cur:
            cur.execute("UPDATE batch_runs SET status='failed', finished_at=now(), error=%s "
                        "WHERE id=%s", (str(exc)[:2000], batch_run_pk))
        log_event(logger, logging.ERROR, f"{ALERT_PREFIX} batch run failed", alert_type="pipeline",
                  error=str(exc), **ctx)
        raise


def resolve_files(args, s: Settings):
    if args.lab_file:
        files = []
        for p in args.lab_file:
            lab_file = parse_lab_file(p)
            if lab_file is None:
                raise ValueError(f"Not a lab results file name: {p}")
            files.append(lab_file)
        return files
    with transaction(s) as cur:
        processed = fetch_processed_file_names(cur)
        latest_event = fetch_latest_event_epoch(cur)
    pending = pending_lab_files(list_lab_files(s.lab_results_dir), processed,
                                grace_seconds=s.batch_grace_seconds,
                                max_age_seconds=s.lab_file_max_age_seconds,
                                limit=s.batch_max_files_per_run)
    return ready_for_batch(pending, latest_event, grace_seconds=s.batch_grace_seconds,
                           max_wait_seconds=s.batch_completeness_max_wait_seconds)


def main(argv=None, spark=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--lab-file", action="append", help="lab CSV to process (repeatable)")
    source.add_argument("--pending", action="store_true",
                        help="process all unprocessed files in LAB_RESULTS_DIR")
    parser.add_argument("--run-id", default=None, help="correlation id (Airflow run_id)")
    args = parser.parse_args(argv)

    s = get_settings()
    run_id = args.run_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    files = resolve_files(args, s)
    if not files:
        log_event(logger, logging.INFO, "No lab files to process", run_id=run_id)
        return 0

    spark = spark or get_spark("hospital-batch-layer", [] if is_databricks()
                               else spark_packages(postgres=True))
    failures = 0
    for lab_file in files:
        try:
            process_lab_file(spark, s, lab_file, run_id)
        except Exception:
            failures += 1
    log_event(logger, logging.INFO if not failures else logging.ERROR, "Batch layer finished",
              run_id=run_id, files=[f.name for f in files], failed=failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
