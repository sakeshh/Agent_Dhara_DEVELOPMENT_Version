import pytest
import ast
from agent.fabric_notebook_deployer import _customize_code_for_dataset, _extract_datasets_from_code
from agent.profiling.dq_checks import make_json_serializable
from agent.service_layer import build_dq_scorecard

def test_ast_safe_customization():
    pyspark_code = """
import pyspark.sql.functions as F

dfs = {}
dfs["orders"] = (df.withColumn("a", F.trim("a"))
                 .withColumn("b", F.trim("b")))
dfs["customers"] = df.withColumn("x", F.trim("x"))
"""
    # Customize for 'orders', so 'customers' should be wrapped in 'if False:'
    custom_code = _customize_code_for_dataset(pyspark_code, "orders")
    
    # Assert it compiles successfully (valid Python syntax)
    tree = ast.parse(custom_code)
    assert tree is not None
    
    # Assert 'orders' assignment is active, but 'customers' is inside an 'if False:' block
    assert 'dfs[\'orders\'] = ' in custom_code
    assert 'if False:' in custom_code
    assert 'dfs[\'customers\']' in custom_code

def test_extract_datasets_from_code():
    code_with_datasets = """
DATASETS = ["orders", "customers"]
def transform_orders(df):
    return df
"""
    assert _extract_datasets_from_code(code_with_datasets) == ["orders", "customers"]

    code_without_datasets = """
def transform_orders(df):
    return df
def transform_customers(df):
    return df
"""
    assert _extract_datasets_from_code(code_without_datasets) == ["orders", "customers"]

    empty_code = "print('hello')"
    assert _extract_datasets_from_code(empty_code) == ["default"]

def test_dq_scoring_fallback():
    # Test dataset assessment result structure with issues but no row_indexes
    assessment_result = {
        "datasets": {
            "blob_dataset": {
                "row_count": 100,
                "columns": {
                    "email": {"dtype": "string"},
                    "id": {"dtype": "double"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "blob_dataset": {
                    "summary": {
                        "issue_count": 2,
                        "high_severity": 1,
                        "medium_severity": 1,
                        "low_severity": 0,
                        "dq_score_0_100": None  # Trigger fallback estimation
                    },
                    "issues": [
                        {
                            "column": "email",
                            "type": "invalid_email",
                            "severity": "high",
                            "count": 50,
                            "row_indexes": None,  # Missing row_indexes
                            "message": "10 invalid emails"
                        },
                        {
                            "column": "id",
                            "type": "outlier",
                            "severity": "medium",
                            "count": 5,
                            "row_indexes": [],  # Empty row_indexes
                            "message": "5 outliers"
                        }
                    ]
                }
            }
        }
    }

    scorecard = build_dq_scorecard(assessment_result)
    
    # Assert scorecard verdict and computed overall score
    assert scorecard["verdict"] == "NEEDS_WORK"
    assert scorecard["overall_dq_score"] != "N/A"
    assert scorecard["overall_dq_score"] < 100.0  # Must have computed a penalty!
    
    # Assert N/A behavior when all metrics are absent
    empty_result = {
        "datasets": {
            "empty_ds": {
                "row_count": 0,
                "columns": {}
            }
        },
        "data_quality_issues": {
            "datasets": {
                "empty_ds": {
                    "summary": {
                        "issue_count": 0,
                        "dq_score_0_100": None
                    },
                    "issues": []
                }
            }
        }
    }
    
    empty_scorecard = build_dq_scorecard(empty_result)
    assert empty_scorecard["verdict"] == "UNKNOWN"
    assert empty_scorecard["overall_dq_score"] == "N/A"


def test_html_unescape_on_generated_code():
    from agent.etl_pipeline.llm_codegen import _strip_markdown_fences
    escaped_code = """
def transform_data(df):
    # check condition
    df = df.filter(F.col("id") &lt; 100)
    return df
    """
    unescaped = _strip_markdown_fences(escaped_code)
    assert "df.filter(F.col(\"id\") < 100)" in unescaped


def test_approx_quantile_validation_error():
    from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
    bad_code = """
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
import logging
DATASETS = ["data"]

def transform_data(df: DataFrame) -> DataFrame:
    q = df.approxQuantile("val", [0.5], 0.01)[0]
    return df
    """
    ok, errs = validate_pyspark_source(bad_code)
    assert not ok
    assert any("approxQuantile" in err for err in errs)


def test_safe_cast_on_id_column():
    from agent.etl_pipeline.pyspark_codegen import _emit_spark
    # customer_id has "id" (key) and "long" is integer target
    lines = _emit_spark("cast_type", "customer_id", "df", {"target_type": "long"})
    full_line = "".join(lines)
    assert "rlike" in full_line
    assert "F.lit(None).cast('long')" in full_line

    # normal column shouldn't use rlike try-cast
    normal_lines = _emit_spark("cast_type", "amount", "df", {"target_type": "long"})
    normal_line = "".join(normal_lines)
    assert "rlike" not in normal_line
    assert "df.withColumn('amount', F.col('amount').cast('long'))" in normal_line


def test_no_invented_fill_for_email():
    from agent.etl_pipeline.pyspark_codegen import _emit_fill_spark
    # email column is not text safe; fill strategy value with None fill_value should keep original values as-is
    lines = _emit_fill_spark("email", "df", {"fill_strategy": "value", "fill_value": None})
    full_line = "".join(lines)
    assert "keeping original values as-is" in full_line
    assert 'F.lit("")' not in full_line

    # name/comment column is text-safe (no key indicators or categorical rules)
    name_lines = _emit_fill_spark("comment", "df", {"fill_strategy": "value", "fill_value": None})
    name_line = "".join(name_lines)
    assert 'F.lit("")' in name_line




def test_iqr_bounds_unpacking_validation():
    from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
    bad_code = """
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
import logging
DATASETS = ["data"]

def transform_data(df: DataFrame) -> DataFrame:
    bounds = _iqr_bounds(df, "id")
    df = df.withColumn("id_outlier_flagged", F.when((F.col("id") < bounds[0]), 1).otherwise(0))
    return df
    """
    ok, errs = validate_pyspark_source(bad_code)
    assert not ok
    assert any("outlier bounds: always unpack all 4 variables" in err for err in errs)


def test_inject_pyspark_helpers():
    from agent.etl_pipeline.llm_codegen import _inject_pyspark_helpers
    code_using_iqr = """
def transform_data(df):
    _stats, _iqr, _lower, _upper = _iqr_bounds(df, "id")
    return df
    """
    injected = _inject_pyspark_helpers(code_using_iqr)
    assert "def _iqr_bounds" in injected


def test_clean_invalid_fillna_pyspark():
    from agent.etl_pipeline.llm_codegen import _inject_pyspark_helpers
    code_with_bad_fillna = """
def transform_data(df):
    df = df.fillna({"email": None})
    out = out.fillna(None)
    return df
    """
    cleaned = _inject_pyspark_helpers(code_with_bad_fillna)
    assert 'df = df.fillna({"email": None})' not in cleaned
    assert 'out = out.fillna(None)' not in cleaned
    assert 'skipped empty fillna' in cleaned


def test_safe_coerce_numeric_on_id_column():
    from agent.etl_pipeline.pyspark_codegen import _emit_spark
    lines = _emit_spark("coerce_numeric", "customer_id", "df")
    full_line = "".join(lines)
    assert "double" not in full_line
    assert "F.trim(F.col('customer_id').cast('string'))" in full_line


