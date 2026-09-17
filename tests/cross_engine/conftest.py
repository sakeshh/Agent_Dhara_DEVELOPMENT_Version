import time
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Shared session-scoped SparkSession for all cross-engine parity tests."""
    spark = None
    for attempt in range(5):
        try:
            spark = (
                SparkSession.builder
                .master("local[2]")
                .appName("AgentDhara_CrossEngine_Session")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "2")
                .getOrCreate()
            )
            break
        except (PermissionError, Exception) as e:
            if attempt < 4 and "Permission denied" in str(e):
                time.sleep(1.5)
            else:
                raise
    yield spark
    if spark:
        try:
            spark.stop()
        except Exception:
            pass

