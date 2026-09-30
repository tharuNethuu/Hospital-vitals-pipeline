"""Consolidated daily patient-risk report: patient-level view + file export.

Used by
  * the Airflow DAG (task `publish_report`) to write reports/<file>.md/.csv
    after every batch run - the scheduled "report file" deliverable;
  * the serving API (GET /api/reports/daily/...) for the same data as JSON.

CLI:  python -m storage.report_export --latest
      python -m storage.report_export --source-file lab_results_day3_1789136839.csv
"""
import argparse
import csv
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import List, Optional

from common.config import get_settings
from common.db import transaction
from common.logging_utils import get_logger, log_event
from processing.vitals_rules import RISK_SEVERITY_ORDER

logger = get_logger("report_export")

PATIENT_FIELDS = ["patient_id", "risk_flag", "vitals_only_risk", "risk_changed_by_labs",
                  "vitals_concerning", "total_readings", "vitals_abnormal_count",
                  "abnormal_ratio", "avg_heart_rate", "avg_spo2", "avg_temperature", "min_spo2",
                  "max_heart_rate", "hr_trend_bpm_per_min", "spo2_trend_pct_per_min",
                  "patient_abnormal_lab_count", "risk_reasons"]


def latest_source_file(cur) -> Optional[str]:
    cur.execute("SELECT source_file FROM batch_runs WHERE status = 'success' "
                "ORDER BY finished_at DESC LIMIT 1")
    row = cur.fetchone()
    return row["source_file"] if row else None


def fetch_report_rows(cur, source_file: str) -> List[dict]:
    cur.execute("SELECT * FROM daily_risk_report WHERE source_file = %s "
                "ORDER BY patient_id, lab_test_type", (source_file,))
    return [dict(r) for r in cur.fetchall()]


def consolidate(rows: List[dict]) -> dict:
    """(patient x lab-test) rows -> report metadata + one entry per patient."""
    patients = {}
    for r in rows:
        p = patients.setdefault(r["patient_id"], {k: r.get(k) for k in PATIENT_FIELDS} | {"labs": []})
        if r.get("lab_test_type"):
            p["labs"].append({"test_type": r["lab_test_type"], "result_value": r["lab_result_value"],
                              "reference_range": r.get("lab_reference_range"),
                              "abnormal": r["lab_abnormal"]})
    ordered = sorted(patients.values(),
                     key=lambda p: (RISK_SEVERITY_ORDER.index(p["risk_flag"]), p["patient_id"]))
    first = rows[0] if rows else {}
    counts = Counter(p["risk_flag"] for p in ordered)
    return {
        "report_day": first.get("report_day"),
        "source_file": first.get("source_file"),
        "run_id": first.get("run_id"),
        "window_start": first.get("window_start"),
        "window_end": first.get("window_end"),
        "generated_at": first.get("generated_at"),
        "patients_by_risk": {flag: counts.get(flag, 0) for flag in RISK_SEVERITY_ORDER},
        "escalated_by_labs": [p["patient_id"] for p in ordered
                              if p["vitals_only_risk"] == "elevated" and p["risk_flag"] == "high"],
        "newly_flagged_by_labs": [p["patient_id"] for p in ordered
                                  if p["risk_flag"] == "elevated_labs"],
        "patients": ordered,
    }


def _fmt(value, spec: str = ".1f", empty: str = "-") -> str:
    return empty if value is None else format(value, spec)


def render_markdown(report: dict) -> str:
    lines = [
        f"# Daily Patient Risk Report - simulated day {report['report_day']}",
        "",
        f"- **Lab file:** `{report['source_file']}`",
        f"- **Vitals window (UTC):** {report['window_start']} -> {report['window_end']}",
        f"- **Generated:** {report['generated_at']} (run `{report['run_id']}`)",
        "",
        "## Risk summary",
        "",
        "| Risk flag | Patients |",
        "|---|---|",
    ]
    lines += [f"| {flag} | {n} |" for flag, n in report["patients_by_risk"].items()]
    lines += [
        "",
        "**How yesterday's labs change the risk picture**",
        "",
        f"- Escalated to *high* (concerning vitals confirmed by abnormal labs): "
        f"{', '.join(report['escalated_by_labs']) or 'none'}",
        f"- Newly flagged by labs alone (vitals looked normal): "
        f"{', '.join(report['newly_flagged_by_labs']) or 'none'}",
        "",
        "## Patients (most severe first)",
        "",
        "| Patient | Risk | Vitals-only | Readings | Abnormal % | Avg HR | Avg SpO2 | Avg Temp "
        "| HR trend (bpm/min) | SpO2 trend (%/min) | Reasons |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for p in report["patients"]:
        ratio = None if p["abnormal_ratio"] is None else p["abnormal_ratio"] * 100
        lines.append(
            f"| {p['patient_id']} | **{p['risk_flag']}** | {p['vitals_only_risk']} "
            f"| {p['total_readings']} | {_fmt(ratio, '.0f')} | {_fmt(p['avg_heart_rate'], '.0f')} "
            f"| {_fmt(p['avg_spo2'])} | {_fmt(p['avg_temperature'])} "
            f"| {_fmt(p['hr_trend_bpm_per_min'], '+.1f')} | {_fmt(p['spo2_trend_pct_per_min'], '+.2f')} "
            f"| {p['risk_reasons'] or ''} |")
    lines += ["", "## Lab results", "", "| Patient | Test | Result | Reference range | Abnormal |",
              "|---|---|---|---|---|"]
    for p in report["patients"]:
        for lab in p["labs"]:
            flag = "**YES**" if lab["abnormal"] else "no"
            lines.append(f"| {p['patient_id']} | {lab['test_type']} | {lab['result_value']} "
                         f"| {lab['reference_range']} | {flag} |")
    return "\n".join(lines) + "\n"


def export_report(source_file: Optional[str] = None, out_dir: Path = None) -> List[Path]:
    s = get_settings()
    out_dir = Path(out_dir or s.reports_dir)
    with transaction(s) as cur:
        source_file = source_file or latest_source_file(cur)
        if not source_file:
            raise LookupError("No successful batch run yet - nothing to export")
        rows = fetch_report_rows(cur, source_file)
    if not rows:
        raise LookupError(f"No daily_risk_report rows for {source_file}")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"daily_risk_report_{Path(source_file).stem.replace('lab_results_', '')}"
    md_path, csv_path = out_dir / f"{stem}.md", out_dir / f"{stem}.csv"
    report = consolidate(rows)
    md_path.write_text(render_markdown(report), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log_event(logger, logging.INFO, "Report files exported", source_file=source_file,
              markdown=str(md_path), csv=str(csv_path), rows=len(rows),
              patients_by_risk=report["patients_by_risk"])
    return [md_path, csv_path]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Export the daily risk report to Markdown + CSV")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--source-file")
    group.add_argument("--latest", action="store_true")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args(argv)
    try:
        for path in export_report(args.source_file, args.out_dir):
            print(path)
    except LookupError as exc:
        log_event(logger, logging.WARNING, str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
