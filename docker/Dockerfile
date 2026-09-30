# One image for every pipeline service (Airflow, speed layer, serving API):
# Airflow 2.10 + Java 17 + PySpark 3.5 + the Kafka/Avro/Postgres Spark connectors.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*  && mkdir -p /opt/checkpoints && chown airflow:root /opt/checkpoints

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Pin airflow so pip cannot up/downgrade it while resolving our requirements.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# Pre-download the Spark connector jars into ~/.ivy2 at build time so jobs start
# fast and work without Maven access at runtime.
COPY --chown=airflow:root common /tmp/prefetch/common
COPY --chown=airflow:root processing /tmp/prefetch/processing
RUN cd /tmp/prefetch && python -c "\
from processing.spark_session import get_spark, spark_packages; \
s = get_spark('prefetch', spark_packages(kafka=True, avro=True, postgres=True)); s.stop()" \
 && rm -rf /tmp/prefetch

ENV PIPELINE_HOME=/opt/pipeline \
    PYTHONPATH=/opt/pipeline \
    PYTHONUNBUFFERED=1 \
    TZ=UTC
WORKDIR /opt/pipeline
