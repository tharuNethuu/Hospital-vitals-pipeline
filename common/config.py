"""Central, environment-driven configuration.

Every tunable of the pipeline lives here so that the same code runs locally,
inside Docker Compose, from Airflow and inside a Databricks notebook. Values
come from (in order): real environment variables, a `.env` file at the repo
root, Databricks Secrets (scope `hospital-pipeline`, only for credentials),
then the defaults below.

Credentials are NEVER hard-coded or committed - see `.env.example`.
"""
import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

try:  # python-dotenv is optional (not installed on Databricks)
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
except ImportError:  # pragma: no cover
    pass

DATABRICKS_SECRET_SCOPE = "hospital-pipeline"

# Patient pool shared with the simulators (P100 ... P114). Used for validation.
PATIENT_ID_PATTERN = r"^P\d{3,}$"

# ---------------------------------------------------------------------------
# Clinical thresholds. MUST stay consistent with vitals_producer.py, which
# uses exactly these limits to decide what it logs as "ABNORMAL".
# ---------------------------------------------------------------------------
HR_NORMAL_MIN = 60          # bpm
HR_NORMAL_MAX = 100         # bpm
SPO2_NORMAL_MIN = 95.0      # %
TEMP_NORMAL_MAX = 37.5      # degC

# Physiologically impossible values are treated as sensor faults and dropped
# by the speed layer (data cleaning), not flagged as clinical events.
HR_PLAUSIBLE = (20, 250)
SPO2_PLAUSIBLE = (50.0, 100.0)
TEMP_PLAUSIBLE = (30.0, 45.0)
BP_PLAUSIBLE = (30, 260)

# Per-patient real-time alert thresholds (speed layer, per window).
ALERT_CRITICAL_SPO2_BELOW = 90.0
ALERT_CRITICAL_HR_BELOW = 50
ALERT_CRITICAL_HR_ABOVE = 130
ALERT_CRITICAL_TEMP_AT_OR_ABOVE = 39.0

# Daily risk rules (batch layer).
VITALS_ABNORMAL_RATIO_THRESHOLD = 0.20   # >=20% of the day's readings abnormal


def _get_databricks_secret(name: str):
    """Return a Databricks secret if running on Databricks, else None."""
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return None
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.secrets.get(scope=DATABRICKS_SECRET_SCOPE, key=name)
    except Exception:  # secret/scope not configured
        return None


def get_value(name: str, default=None, secret: bool = False):
    value = os.environ.get(name)
    if value in (None, "") and secret:
        value = _get_databricks_secret(name)
    return default if value in (None, "") else value


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class Settings:
    # Kafka (Confluent Cloud) - same variable names as Member A's .env
    kafka_bootstrap: str
    kafka_api_key: str
    kafka_api_secret: str
    kafka_security_protocol: str
    kafka_topic: str
    kafka_starting_offsets: str
    kafka_max_offsets_per_trigger: int

    # Postgres (Supabase in the cloud setup, local container in Docker Compose)
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    pg_sslmode: str

    # Simulated clock + file drop location (must match lab_results_simulator.py)
    simulated_day_seconds: int
    lab_results_dir: Path
    reports_dir: Path

    # Speed layer
    checkpoint_dir: str
    window_duration: str
    window_slide: str
    watermark_delay: str
    raw_trigger: str
    agg_trigger: str
    heartbeat_interval_seconds: int

    # Batch / orchestration
    batch_grace_seconds: int
    batch_completeness_max_wait_seconds: int
    batch_max_files_per_run: int
    lab_file_max_age_seconds: int
    spark_master: str

    @property
    def jdbc_url(self) -> str:
        return (f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"
                f"?sslmode={self.pg_sslmode}")


def get_settings() -> Settings:
    default_ckpt = ("/tmp/hospital_checkpoints" if is_databricks()
                    else str(REPO_ROOT / "checkpoints"))
    return Settings(
        kafka_bootstrap=get_value("BOOTSTRAP_SERVER", "", secret=True),
        kafka_api_key=get_value("API_KEY", "", secret=True),
        kafka_api_secret=get_value("API_SECRET", "", secret=True),
        kafka_security_protocol=get_value("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        kafka_topic=get_value("KAFKA_TOPIC", "vitals-stream"),
        kafka_starting_offsets=get_value("KAFKA_STARTING_OFFSETS", "latest"),
        kafka_max_offsets_per_trigger=int(get_value("KAFKA_MAX_OFFSETS_PER_TRIGGER", 5000)),
        pg_host=get_value("PG_HOST", "localhost", secret=True),
        pg_port=int(get_value("PG_PORT", 5432, secret=True)),
        pg_db=get_value("PG_DB", "postgres", secret=True),
        pg_user=get_value("PG_USER", "postgres", secret=True),
        pg_password=get_value("PG_PASSWORD", "", secret=True),
        pg_sslmode=get_value("PG_SSLMODE", "prefer"),
        simulated_day_seconds=int(get_value("SIMULATED_DAY_SECONDS", 120)),
        lab_results_dir=Path(get_value("LAB_RESULTS_DIR", str(REPO_ROOT / "lab_results_drops"))),
        reports_dir=Path(get_value("REPORTS_DIR", str(REPO_ROOT / "reports"))),
        checkpoint_dir=get_value("CHECKPOINT_DIR", default_ckpt),
        window_duration=get_value("WINDOW_DURATION", "30 seconds"),
        window_slide=get_value("WINDOW_SLIDE", "10 seconds"),
        watermark_delay=get_value("WATERMARK_DELAY", "30 seconds"),
        raw_trigger=get_value("RAW_TRIGGER", "5 seconds"),
        agg_trigger=get_value("AGG_TRIGGER", "10 seconds"),
        heartbeat_interval_seconds=int(get_value("HEARTBEAT_INTERVAL_SECONDS", 15)),
        batch_grace_seconds=int(get_value("BATCH_GRACE_SECONDS", 10)),
        batch_completeness_max_wait_seconds=int(get_value(
            "BATCH_COMPLETENESS_MAX_WAIT_SECONDS", get_value("SIMULATED_DAY_SECONDS", 120))),
        batch_max_files_per_run=int(get_value("BATCH_MAX_FILES_PER_RUN", 5)),
        lab_file_max_age_seconds=int(get_value("LAB_FILE_MAX_AGE_SECONDS", 6 * 3600)),
        spark_master=get_value("SPARK_MASTER", "local[2]"),
    )
