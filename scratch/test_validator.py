import sys
sys.path.insert(0, '.')
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source

bad_code = """
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

DATASETS = ['data_quality_issues.json']

def transform(df: DataFrame) -> DataFrame:
    df = df.fillna({'email': F.lit(None)})
    parsed = F.expr("try_to_timestamp(raw, 'yyyy-MM-dd')")
    return df
"""

ok, errs = validate_pyspark_source(bad_code)
print('OK:', ok)
print('ERRORS:', errs)
