import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from processing.spark_session import get_spark, spark_packages

    session = get_spark("pipeline-tests", spark_packages(avro=True))
    yield session
    session.stop()
