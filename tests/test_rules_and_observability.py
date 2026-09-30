import json
import logging

from common.logging_utils import JsonFormatter
from observability.health_check import evaluate_rules, load_rules
from observability.metrics import to_prometheus
from processing import vitals_rules as r


def test_abnormal_thresholds_match_producer():
    assert not r.is_abnormal(60, 95.0, 37.5)       # boundaries are normal
    assert r.is_abnormal(59, 98.0, 36.8)
    assert r.is_abnormal(101, 98.0, 36.8)
    assert r.is_abnormal(80, 94.9, 36.8)
    assert r.is_abnormal(80, 98.0, 37.6)


def test_window_alert_classification():
    assert r.classify_window_alert(80, 98, 36.8, 0) == (None, [])
    sev, reasons = r.classify_window_alert(80, 97, 37.0, 1)
    assert sev == "warning" and "1 abnormal" in reasons[0]
    sev, reasons = r.classify_window_alert(140, 85, 39.2, 1)
    assert sev == "critical" and len(reasons) == 3


def test_patient_risk_matrix():
    assert r.classify_patient_risk(True, True) == r.RISK_HIGH
    assert r.classify_patient_risk(True, False) == r.RISK_ELEVATED_VITALS
    assert r.classify_patient_risk(False, True) == r.RISK_ELEVATED_LABS
    assert r.classify_patient_risk(False, False) == r.RISK_NORMAL


def _rules():
    return {rule["name"]: rule for rule in load_rules()}


def test_alert_rules_file_is_valid():
    rules = load_rules()
    assert len({x["name"] for x in rules}) == len(rules)
    for rule in rules:
        assert rule["op"] in (">", ">=", "<", "<=")
        assert rule["severity"] in ("warning", "critical")
        assert ("threshold" in rule) != ("threshold_simulated_days" in rule)


def test_evaluate_rules_breach_ok_and_missing():
    rules = [_rules()["vitals_stream_stale"], _rules()["batch_job_failing"],
             _rules()["lab_feed_stale"]]
    results = {x.name: x for x in evaluate_rules(
        {"vitals_last_ingest_age_seconds": 75.0, "batch_failure_ratio_30m": None,
         "lab_feed_last_file_age_seconds": 250.0}, rules, simulated_day_seconds=120)}
    assert results["vitals_stream_stale"].breached               # 75 > 60
    assert not results["batch_job_failing"].breached             # missing -> ok
    assert not results["lab_feed_stale"].breached                # 250 < 2.5 * 120
    assert results["lab_feed_stale"].threshold == 300
    missing = evaluate_rules({}, [_rules()["vitals_stream_stale"]], 120)[0]
    assert missing.breached and "no data" in missing.message   # never received data


def test_prometheus_exposition():
    text = to_prometheus({"db_up": 1.0, "vitals_readings_5m": None}, {"x": True})
    assert "hospital_db_up 1.0" in text
    assert "hospital_vitals_readings_5m NaN" in text
    assert 'hospital_health_rule_breached{rule="x"} 1' in text


def test_json_log_lines_stay_valid_with_quotes():
    record = logging.LogRecord("t", logging.WARNING, __file__, 1, 'bad "value"\nnext', None, None)
    record.fields = {"patient_id": "P101", "rows": 3}
    payload = json.loads(JsonFormatter("unit").format(record))
    assert payload["message"] == 'bad "value"\nnext'
    assert payload["component"] == "unit" and payload["patient_id"] == "P101"
    assert payload["level"] == "WARNING"
