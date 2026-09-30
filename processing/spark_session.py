"""SparkSession factory that works both locally (pip pyspark / Docker) and on
Databricks, where a session already exists and the Kafka/Avro/JDBC connectors
are pre-installed on the cluster."""
from typing import Iterable

from common.config import get_settings, is_databricks

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def spark_packages(kafka: bool = False, avro: bool = False, postgres: bool = False) -> list:
    import pyspark

    version = pyspark.__version__
    packages = []
    if kafka:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_2.12:{version}")
    if avro:
        packages.append(f"org.apache.spark:spark-avro_2.12:{version}")
    if postgres:
        packages.append(POSTGRES_JDBC_PACKAGE)
    return packages


def get_spark(app_name: str, packages: Iterable[str] = ()):
    from pyspark.sql import SparkSession

    if is_databricks():
        spark = SparkSession.builder.getOrCreate()
    else:
        settings = get_settings()
        builder = (SparkSession.builder
                   .appName(app_name)
                   .master(settings.spark_master)
                   .config("spark.sql.shuffle.partitions", "4")
                   .config("spark.ui.showConsoleProgress", "false")
                   .config("spark.sql.streaming.stopGracefullyOnShutdown", "true"))
        packages = list(packages)
        if packages:
            builder = builder.config("spark.jars.packages", ",".join(packages))
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
    # All event-time logic and all stored timestamps are UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
