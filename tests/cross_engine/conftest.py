import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Shared session-scoped SparkSession for all cross-engine parity tests."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("AgentDhara_CrossEngine_Session")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()
