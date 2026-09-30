from storage.report_export import consolidate, render_markdown


def _row(pid, flag, vitals_only, test=None, value=None, abnormal=None, **kw):
    base = {"report_day": 4, "source_file": "lab_results_day4_1.csv", "run_id": "r1",
            "window_start": "s", "window_end": "e", "generated_at": "g", "patient_id": pid,
            "risk_flag": flag, "vitals_only_risk": vitals_only, "risk_changed_by_labs": False,
            "vitals_concerning": vitals_only == "elevated", "total_readings": 4,
            "vitals_abnormal_count": 1, "abnormal_ratio": 0.25, "avg_heart_rate": 90.0,
            "avg_spo2": 96.0, "avg_temperature": 37.0, "min_spo2": 94.0, "max_heart_rate": 101,
            "hr_trend_bpm_per_min": 1.5, "spo2_trend_pct_per_min": -0.2,
            "patient_abnormal_lab_count": 0, "risk_reasons": "", "lab_test_type": test,
            "lab_result_value": value, "lab_reference_range": "1-2", "lab_abnormal": abnormal}
    base.update(kw)
    return base


def test_consolidate_groups_labs_and_orders_by_severity():
    rows = [
        _row("P101", "normal", "normal"),
        _row("P100", "elevated_labs", "normal", "CRP", 15.0, True),
        _row("P102", "high", "elevated", "WBC_Count", 17.0, True),
        _row("P102", "high", "elevated", "Glucose", 90.0, False),
    ]
    report = consolidate(rows)
    assert [p["patient_id"] for p in report["patients"]] == ["P102", "P100", "P101"]
    assert len(report["patients"][0]["labs"]) == 2
    assert report["patients"][2]["labs"] == []
    assert report["patients_by_risk"] == {"high": 1, "elevated_vitals": 0, "elevated_labs": 1,
                                          "normal": 1}
    assert report["escalated_by_labs"] == ["P102"]
    assert report["newly_flagged_by_labs"] == ["P100"]

    md = render_markdown(report)
    assert "simulated day 4" in md
    assert "| P102 | **high** |" in md
    assert "| P102 | WBC_Count | 17.0 | 1-2 | **YES** |" in md
