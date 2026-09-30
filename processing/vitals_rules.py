"""Clinical/data-quality rules shared by the speed and batch layers.

Each rule exists in two forms that are kept side by side on purpose:
  * a Spark Column expression, used inside the streaming / batch jobs;
  * a plain-Python function with identical semantics, used for per-row logic
    on the driver (alert episodes) and by the unit tests.
Thresholds come from common.config so ingestion and processing agree.
"""
from typing import List, Optional, Tuple

from common import config as c

RISK_HIGH = "high"                        # concerning vitals AND abnormal labs
RISK_ELEVATED_VITALS = "elevated_vitals"  # concerning vitals only
RISK_ELEVATED_LABS = "elevated_labs"      # abnormal labs only
RISK_NORMAL = "normal"
RISK_SEVERITY_ORDER = [RISK_HIGH, RISK_ELEVATED_VITALS, RISK_ELEVATED_LABS, RISK_NORMAL]


# --------------------------------------------------------------------------
# Plain-Python versions
# --------------------------------------------------------------------------
def is_abnormal(heart_rate, spo2, temperature) -> bool:
    return (heart_rate < c.HR_NORMAL_MIN or heart_rate > c.HR_NORMAL_MAX
            or spo2 < c.SPO2_NORMAL_MIN or temperature > c.TEMP_NORMAL_MAX)


def classify_window_alert(avg_heart_rate: float, avg_spo2: float, avg_temperature: float,
                          abnormal_count: int) -> Tuple[Optional[str], List[str]]:
    """Per-patient threshold alert for one speed-layer window.

    critical: window averages cross the critical limits;
    warning:  at least one reading in the window was outside the normal range;
    None:     nothing to alert on.
    """
    critical = []
    if avg_spo2 < c.ALERT_CRITICAL_SPO2_BELOW:
        critical.append(f"SpO2 {avg_spo2:.1f}% < {c.ALERT_CRITICAL_SPO2_BELOW:g}%")
    if avg_heart_rate < c.ALERT_CRITICAL_HR_BELOW:
        critical.append(f"HR {avg_heart_rate:.0f} bpm < {c.ALERT_CRITICAL_HR_BELOW}")
    if avg_heart_rate > c.ALERT_CRITICAL_HR_ABOVE:
        critical.append(f"HR {avg_heart_rate:.0f} bpm > {c.ALERT_CRITICAL_HR_ABOVE}")
    if avg_temperature >= c.ALERT_CRITICAL_TEMP_AT_OR_ABOVE:
        critical.append(f"Temp {avg_temperature:.1f}C >= {c.ALERT_CRITICAL_TEMP_AT_OR_ABOVE:g}C")
    if critical:
        return "critical", critical
    if abnormal_count and abnormal_count > 0:
        return "warning", [f"{abnormal_count} abnormal reading(s) in window"]
    return None, []


def classify_patient_risk(vitals_concerning: bool, labs_abnormal: bool) -> str:
    if vitals_concerning and labs_abnormal:
        return RISK_HIGH
    if vitals_concerning:
        return RISK_ELEVATED_VITALS
    if labs_abnormal:
        return RISK_ELEVATED_LABS
    return RISK_NORMAL


# --------------------------------------------------------------------------
# Spark Column versions (pyspark imported lazily so the plain-Python helpers
# above can be used by Airflow/the API without a Spark install)
# --------------------------------------------------------------------------
def abnormal_col(hr, spo2, temp):
    return ((hr < c.HR_NORMAL_MIN) | (hr > c.HR_NORMAL_MAX)
            | (spo2 < c.SPO2_NORMAL_MIN) | (temp > c.TEMP_NORMAL_MAX))


def plausible_reading_col():
    """True when the reading is complete and physically possible."""
    from pyspark.sql import functions as F

    col = F.col
    return (
        col("patient_id").isNotNull() & col("patient_id").rlike(c.PATIENT_ID_PATTERN)
        & col("event_time").isNotNull()
        & col("heart_rate").between(*c.HR_PLAUSIBLE)
        & col("spo2").between(*c.SPO2_PLAUSIBLE)
        & col("temperature").between(*c.TEMP_PLAUSIBLE)
        & col("systolic_bp").between(*c.BP_PLAUSIBLE)
        & col("diastolic_bp").between(*c.BP_PLAUSIBLE)
        & (col("systolic_bp") > col("diastolic_bp"))
    )


def vitals_concerning_col():
    """Day-level 'concerning vital-sign trend' rule on summarised vitals."""
    from pyspark.sql import functions as F

    col = F.col
    return (
        (col("total_readings") > 0)
        & ((col("abnormal_ratio") >= c.VITALS_ABNORMAL_RATIO_THRESHOLD)
           | (col("avg_heart_rate") > c.HR_NORMAL_MAX)
           | (col("avg_heart_rate") < c.HR_NORMAL_MIN)
           | (col("avg_spo2") < c.SPO2_NORMAL_MIN)
           | (col("avg_temperature") > c.TEMP_NORMAL_MAX))
    )


def risk_flag_col(vitals_concerning, labs_abnormal):
    from pyspark.sql import functions as F

    return (F.when(vitals_concerning & labs_abnormal, RISK_HIGH)
            .when(vitals_concerning, RISK_ELEVATED_VITALS)
            .when(labs_abnormal, RISK_ELEVATED_LABS)
            .otherwise(RISK_NORMAL))
