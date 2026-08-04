import unittest
from agent.etl_pipeline.codegen_validator import validate_pyspark

class TestCodegenPhase1Invariants(unittest.TestCase):

    def test_codegen_validator_rejects_inner_join_in_cleanse(self):
        code = """
# PySpark ETL -- plan_id=test_plan_cleanse
# Generation mode: cleanse_only
def transform_marketing(df, all_dfs):
    # Step: Join marketing and sales
    df = df.join(parent, "campaign_id", "inner")
    return df
"""
        ok, errors, warnings = validate_pyspark(code, never_drop_rows=True)
        self.assertFalse(ok)
        self.assertTrue(any("Inner join is forbidden" in e for e in errors))

    def test_codegen_validator_rejects_nondeterministic_row_number(self):
        code = """
# PySpark ETL -- plan_id=test_plan_cleanse
# Generation mode: cleanse_only
def transform_sales(df, all_dfs):
    w = Window.partitionBy("sale_id").orderBy(F.lit(1))
    df = df.withColumn("row_num", F.row_number().over(w))
    return df
"""
        ok, errors, warnings = validate_pyspark(code, never_drop_rows=True)
        self.assertFalse(ok)
        self.assertTrue(any("Nondeterministic duplicate survivorship selection" in e for e in errors))

    def test_codegen_validator_accepts_clean_cleanse_code(self):
        code = """
# PySpark ETL -- plan_id=test_plan_cleanse
# Generation mode: cleanse_only
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def transform_sales(df, all_dfs):
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    return df
"""
        ok, errors, warnings = validate_pyspark(code, never_drop_rows=True)
        self.assertTrue(ok)

if __name__ == "__main__":
    unittest.main()
