"""Pipeline health checks + alerting.

Evaluates the data-driven rules in alert_rules.json against the metrics from
observability/metrics.py and manages the alert lifecycle in `pipeline_alerts`:

  rule newly breached   -> INSERT open alert  + log  "ALERT: ..."    (CRITICAL/WARNING)
  rule still breached   -> UPDATE last_seen/value (no log spam)
  rule recovered        -> mark resolved      + log  "RESOLVED: ..." (INFO)
  database unreachable  -> log "ALERT: ..." (CRITICAL) - the check itself fails

Run:
  python -m observability.health_check            # one evaluation (exit 2 if critical)
  python -m observability.health_check --loop 30  # every 30s, without Airflow
Airflow runs `run_health_checks()` every minute (DAG pipeline_health_checks).
"""
import argparse
import json
import logging
import operator
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from common.config import Settings, get_settings
from common.db import transaction
from common.logging_utils import ALERT_PREFIX, get_logger, log_event
from observability.metrics import collect_metrics

COMPONENT = "health_check"
logger = get_logger(COMPONENT)
RULES_FILE = Path(__file__).with_name("alert_rules.json")
_OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}


@dataclass
class RuleResult:
    name: str
    severity: str
    breached: bool
    value: Optional[float]
    threshold: float
    description: str

    @property
    def message(self) -> str:
        shown = "no data" if self.value is None else f"{self.value:.2f}"
        return f"{self.description} (value={shown}, threshold={self.threshold:g})"


def load_rules(path: Path = RULES_FILE) -> List[dict]:
    return json.loads(path.read_text(encoding="utf-8"))["rules"]


def rule_threshold(rule: dict, simulated_day_seconds: int) -> float:
    if "threshold_simulated_days" in rule:
        return float(rule["threshold_simulated_days"]) * simulated_day_seconds
    return float(rule["threshold"])


def evaluate_rules(metrics: Dict[str, Optional[float]], rules: List[dict],
                   simulated_day_seconds: int) -> List[RuleResult]:
    """Pure function: metrics + rules -> results (unit-tested)."""
    results = []
    for rule in rules:
        value = metrics.get(rule["metric"])
        threshold = rule_threshold(rule, simulated_day_seconds)
        if value is None:
            breached = rule.get("on_missing", "ok") == "breach"
        else:
            breached = _OPS[rule["op"]](value, threshold)
        results.append(RuleResult(rule["name"], rule["severity"], breached, value, threshold,
                                  rule["description"]))
    return results


def _sync_alert(cur, result: RuleResult) -> None:
    cur.execute("SELECT id FROM pipeline_alerts WHERE rule_name = %s AND status = 'open'",
                (result.name,))
    open_alert = cur.fetchone()
    fields = dict(alert_type="pipeline", rule=result.name, severity=result.severity,
                  metric_value=result.value, threshold=result.threshold)
    if result.breached and open_alert is None:
        cur.execute("INSERT INTO pipeline_alerts (rule_name, severity, message, metric_value) "
                    "VALUES (%s, %s, %s, %s)",
                    (result.name, result.severity, result.message, result.value))
        level = logging.CRITICAL if result.severity == "critical" else logging.WARNING
        log_event(logger, level, f"{ALERT_PREFIX} [{result.name}] {result.message}", **fields)
    elif result.breached:
        cur.execute("UPDATE pipeline_alerts SET last_seen = now(), metric_value = %s, message = %s "
                    "WHERE id = %s", (result.value, result.message, open_alert["id"]))
    elif open_alert is not None:
        cur.execute("UPDATE pipeline_alerts SET status = 'resolved', resolved_at = now(), "
                    "last_seen = now(), metric_value = %s WHERE id = %s",
                    (result.value, open_alert["id"]))
        log_event(logger, logging.INFO, f"RESOLVED: [{result.name}] back within threshold", **fields)


def run_health_checks(settings: Settings = None, record: bool = True) -> List[RuleResult]:
    """Collect metrics, evaluate rules and (optionally) record alert state."""
    s = settings or get_settings()
    rules = load_rules()
    try:
        with transaction(s) as cur:
            metrics = collect_metrics(cur, s)
            results = evaluate_rules(metrics, rules, s.simulated_day_seconds)
            if record:
                for result in results:
                    _sync_alert(cur, result)
    except Exception as exc:
        log_event(logger, logging.CRITICAL, f"{ALERT_PREFIX} [storage_unreachable] "
                  "health check could not query Postgres", alert_type="pipeline",
                  rule="storage_unreachable", severity="critical", error=str(exc))
        raise
    breached = [r for r in results if r.breached]
    log_event(logger, logging.WARNING if breached else logging.INFO, "Health check completed",
              rules_evaluated=len(results), rules_breached=[r.name for r in breached],
              metrics={k: (round(v, 3) if v is not None else None) for k, v in metrics.items()})
    return results


def has_critical(results: List[RuleResult]) -> bool:
    return any(r.breached and r.severity == "critical" for r in results)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate pipeline health rules")
    parser.add_argument("--loop", type=int, default=0, metavar="SECONDS",
                        help="re-run every N seconds instead of once")
    args = parser.parse_args(argv)
    while True:
        try:
            results = run_health_checks()
            code = 2 if has_critical(results) else 0
        except Exception:
            code = 1
        if not args.loop:
            return code
        time.sleep(args.loop)


if __name__ == "__main__":
    sys.exit(main())
