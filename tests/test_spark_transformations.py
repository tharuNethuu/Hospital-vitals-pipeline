"""Spark transformation tests (need pyspark + Java 17; run inside the Docker image:
docker compose run --rm --no-deps --entrypoint python api -m pytest -q)."""
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from processing.batch_layer_daily_report import (build_risk_report, clean_lab_results,
                                                 read_lab_csv, summarise_vitals)

pytest.importorskip("pyspark")
REPO = Path(__file__).resolve().parents[1]


def _labs(spark, rows):
    from processing.batch_layer_daily_report import LAB_CSV_SCHEMA

    return spark.createDataFrame(rows, LAB_CSV_SCHEMA)


def test_clean_lab_results_types_flags_and_dedups(spark):
    raw = _labs(spark, [
        ("P100", "CRP", "15.2", "0-10 mg/L", "2026-09-11T14:26:59+00:00"),       # abnormal high
        ("p101 ", "Hemoglobin", "7.5", "12.0-17.0 g/dL", "2026-09-11T14:26:59+00:00"),  # low, trimmed
        ("P102", "Glucose", "100", "70-140 mg/dL", "2026-09-11T14:00:00+00:00"),  # older duplicate
        ("P102", "Glucose", "150", "70-140 mg/dL", "2026-09-11T14:30:00+00:00"),  # latest wins
        ("P103", "WBC_Count", "n/a", "4.0-11.0 x10^9/L", None),                   # bad value
        ("BAD", "CRP", "1", "0-10 mg/L", None),                                   # bad patient id
        ("P104", "CRP", "1", "unknown", None),                                    # bad range
    ])
    out = {(r.patient_id, r.test_type): r for r in clean_lab_results(raw).collect()}
    assert set(out) == {("P100", "CRP"), ("P101", "Hemoglobin"), ("P102", "Glucose")}
    assert out[("P100", "CRP")].lab_abnormal and out[("P100", "CRP")].ref_high == 10.0
    assert out[("P101", "Hemoglobin")].lab_abnormal
    assert out[("P102", "Glucose")].result_value == 150.0 and out[("P102", "Glucose")].lab_abnormal


def test_simulator_csv_files_parse(spark):
    files = sorted((REPO / "lab_results_drops").glob("lab_results_day*.csv"))
    if not files:
        pytest.skip("no sample lab files")
    raw = read_lab_csv(spark, str(files[0]))
    assert clean_lab_results(raw).count() == raw.count()   # simulator output is clean


def _readings(spark, pid_values):
    from pyspark.sql import Row

    t0 = datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc)
    rows = []
    for pid, values in pid_values.items():
        for i, (hr, spo2, temp) in enumerate(values):
            rows.append(Row(patient_id=pid, heart_rate=hr, spo2=spo2, temperature=temp,
                            is_abnormal=hr < 60 or hr > 100 or spo2 < 95 or temp > 37.5,
                            event_time=t0 + timedelta(seconds=30 * i)))
    return spark.createDataFrame(rows)


def test_summary_and_risk_report(spark):
    readings = _readings(spark, {
        "P100": [(80, 98.0, 36.8), (140, 85.0, 39.0), (130, 88.0, 38.8), (82, 97.0, 36.9)],  # 50% abn
        "P101": [(70, 98.0, 36.6), (72, 98.5, 36.7), (74, 99.0, 36.6)],                      # normal
        "P102": [(90, 97.0, 36.9), (95, 96.0, 37.0)],                                         # normal
        "P104": [(75, 96.0, 36.5)] * 2 + [(150, 82.0, 39.5)],                                 # 33% abn
    })
    summary = {r.patient_id: r for r in summarise_vitals(readings).collect()}
    assert summary["P100"].total_readings == 4 and summary["P100"].vitals_abnormal_count == 2
    assert summary["P100"].abnormal_ratio == 0.5
    assert summary["P101"].hr_trend_bpm_per_min == pytest.approx(4.0)   # +2 bpm per 30s

    labs = clean_lab_results(_labs(spark, [
        ("P100", "CRP", "25", "0-10 mg/L", None),          # abnormal -> high
        ("P100", "Glucose", "100", "70-140 mg/dL", None),
        ("P101", "Creatinine", "2.0", "0.6-1.3 mg/dL", None),  # abnormal -> elevated_labs
        ("P102", "Hemoglobin", "14", "12.0-17.0 g/dL", None),  # normal
        ("P103", "WBC_Count", "20", "4.0-11.0 x10^9/L", None), # labs only, no vitals
    ]))
    rows = build_risk_report(summarise_vitals(readings), labs).collect()
    by_patient = {}
    for row in rows:
        by_patient.setdefault(row.patient_id, []).append(row)

    assert {p: rs[0].risk_flag for p, rs in by_patient.items()} == {
        "P100": "high", "P101": "elevated_labs", "P102": "normal",
        "P103": "elevated_labs", "P104": "elevated_vitals"}
    assert len(by_patient["P100"]) == 2                      # one row per lab test
    assert by_patient["P104"][0].lab_test_type is None       # vitals-only patient kept
    assert by_patient["P100"][0].vitals_only_risk == "elevated"
    assert by_patient["P100"][0].risk_changed_by_labs
    assert "abnormal labs: CRP" in by_patient["P100"][0].risk_reasons
    assert "50% of 4 readings abnormal" in by_patient["P100"][0].risk_reasons
    p103 = by_patient["P103"][0]
    assert p103.total_readings == 0 and "no vitals received" in p103.risk_reasons


def _avro_bytes(reading):
    fastavro = pytest.importorskip("fastavro")
    schema = fastavro.parse_schema(json.loads((REPO / "vitals.avsc").read_text()))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, reading)   # exactly what vitals_producer.py sends
    return buf.getvalue()


def test_speed_layer_parsing_and_windows(spark):
    from processing.speed_layer_stream import load_avro_schema, parse_vitals, windowed_summary

    def reading(pid, hr, spo2, temp, ts):
        return {"patient_id": pid, "heart_rate": hr, "spo2": spo2, "systolic_bp": 120,
                "diastolic_bp": 80, "temperature": temp, "timestamp": ts}

    kafka_ts = datetime(2026, 9, 11, 14, 0, 30, tzinfo=timezone.utc)
    payloads = [
        _avro_bytes(reading("P100", 80, 98.0, 36.8, "2026-09-11T14:00:01.500000+00:00")),
        _avro_bytes(reading("P100", 140, 85.0, 39.1, "2026-09-11T14:00:05+00:00")),
        _avro_bytes(reading("P101", 999, 98.0, 36.8, "2026-09-11T14:00:05+00:00")),  # impossible HR
        _avro_bytes(reading("P102", 70, 97.0, 36.5, "not-a-timestamp")),              # -> kafka time
        b"\x00garbage",                                                               # corrupt
    ]
    kafka_df = spark.createDataFrame(
        [(0, i, kafka_ts, bytearray(p)) for i, p in enumerate(payloads)],
        "partition INT, offset LONG, timestamp TIMESTAMP, value BINARY")
    parsed_df = parse_vitals(kafka_df, load_avro_schema())
    parsed = {r.kafka_offset: r for r in parsed_df.withColumn(
        "event_epoch", parsed_df["event_time"].cast("double")).collect()}

    assert parsed[0].is_valid and not parsed[0].is_abnormal
    assert parsed[0].event_epoch == datetime(2026, 9, 11, 14, 0, 1, 500000,
                                             tzinfo=timezone.utc).timestamp()   # ISO +00:00 parsed
    assert parsed[1].is_valid and parsed[1].is_abnormal
    assert not parsed[2].is_valid                        # rejected by plausibility rule
    assert parsed[3].is_valid and parsed[3].event_epoch == kafka_ts.timestamp()  # broker time
    assert not parsed[4].is_valid                        # corrupt payload does not crash

    valid = parse_vitals(kafka_df, load_avro_schema()).filter("is_valid")
    windows = windowed_summary(valid, "30 seconds", "10 seconds", "30 seconds").collect()
    p100 = [w for w in windows if w.patient_id == "P100"]
    # 2 readings at :01.5 and :05 fall into the 3 overlapping windows starting -20s, -10s, 0s
    assert len(p100) == 3
    assert all(w.total_readings == 2 and w.abnormal_count == 1 for w in p100)
    assert {w.window_end_epoch - w.window_start_epoch for w in p100} == {30.0}
    start = datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc).timestamp()
    assert max(w.window_start_epoch for w in p100) == start
