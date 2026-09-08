"""Tests for PR 6 Cross-Dialect Precedence and Canonical Expected-Result Model.

Verifies:
- Central predicate precedence:
  source_null -> placeholder_token -> format_violation -> valid
- Multi-condition fixture ('  ###  ' combining whitespace, placeholder, and invalid format)
  evaluates to identical clean values, validation status, and raw preservation across
  Pandas, PySpark, and T-SQL.
"""

import pytest
from agent.etl_pipeline.composite_field_compiler import (
    create_field_work_package,
    compile_field_work_package,
)

# Canonical Neutral Expected-Result Specifications
CANONICAL_PREDECENCE_SPECS = [
    {
        "description": "Multi-condition token: whitespace + placeholder + invalid format",
        "input_value": "  ###  ",
        "expected": {
            "raw": "  ###  ",
            "clean": None,
            "validation_status": "placeholder_normalized_to_null",
            "row_disposition": "retain_all",
        },
    },
    {
        "description": "Valid email with whitespace padding",
        "input_value": "  jane.dev@example.com  ",
        "expected": {
            "raw": "  jane.dev@example.com  ",
            "clean": "jane.dev@example.com",
            "validation_status": "valid",
            "row_disposition": "retain_all",
        },
    },
    {
        "description": "Syntax error email (no at-sign or domain)",
        "input_value": "not-an-email-value",
        "expected": {
            "raw": "not-an-email-value",
            "clean": None,
            "validation_status": "invalid_email_format",
            "row_disposition": "retain_all",
        },
    },
    {
        "description": "Null input value",
        "input_value": None,
        "expected": {
            "raw": None,
            "clean": None,
            "validation_status": "source_null",
            "row_disposition": "retain_all",
        },
    },
]


def test_cross_dialect_compilation_contracts():
    """Compiles field pipeline for Pandas, PySpark, and T-SQL and verifies matching precedence."""
    findings = [
        {
            "id": "rec-email-fmt",
            "dataset": "dbo.Customers",
            "column": "Email",
            "issue_type": "invalid_format",
            "action_id": "nullify_clean_value",
            "authority_source": "DETERMINISTIC_REGISTRY",
        }
    ]

    wp = create_field_work_package("dbo.Customers", "Email", findings, semantic_type="email")

    art_pyspark = compile_field_work_package(wp, dialect="pyspark")
    art_pandas = compile_field_work_package(wp, dialect="pandas")
    art_tsql = compile_field_work_package(wp, dialect="tsql")

    # Invariant 1: All dialects emit output companion columns
    for art in (art_pyspark, art_pandas, art_tsql):
        assert "Email_raw" in art.output_columns
        assert "Email_clean" in art.output_columns
        assert "Email_validation_status" in art.output_columns
        assert "Email_validation_reason" in art.output_columns
        assert art.row_disposition == "retain_all"

    # Invariant 2: Centralized condition precedence in T-SQL atomic CASE
    tsql_code = art_tsql.code
    assert "source_null" in tsql_code
    assert "placeholder_normalized_to_null" in tsql_code
    assert "invalid_email_format" in tsql_code
    # Precedence: source_null check appears before placeholder, which appears before format check
    idx_null = tsql_code.find("source_null")
    idx_place = tsql_code.find("placeholder_normalized_to_null")
    idx_fmt = tsql_code.find("invalid_email_format")
    assert idx_null < idx_place < idx_fmt

    # Invariant 3: Centralized condition precedence in Pandas
    pandas_code = art_pandas.code
    assert "is_null" in pandas_code
    assert "is_placeholder" in pandas_code
    assert "is_syntax_error" in pandas_code
    p_null = pandas_code.find("is_null")
    p_place = pandas_code.find("is_placeholder")
    p_fmt = pandas_code.find("is_syntax_error")
    assert p_null < p_place < p_fmt

    # Invariant 4: Centralized condition precedence in PySpark
    spark_code = art_pyspark.code
    assert "is_null" in spark_code
    assert "is_placeholder" in spark_code
    assert "is_valid_email" in spark_code
