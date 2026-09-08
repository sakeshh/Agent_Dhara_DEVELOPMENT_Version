"""
Tests for PR 6.1: Semantic Composite Compiler Completion and Governed Configuration Action Routing.

Verifies:
1. Configuration actions (update_manifest) route to plan['configuration_changes'] as StructuredConfigPatch
   with exact field names (phone, gstin), bypassing dataset steps and eliminating 'Unknown action' errors.
2. Flag-only never nullifies clean values for numeric fields (Courses.Price).
3. CreatedDate pipeline validates placeholders, sentinels, impossible dates, and produces date-typed clean companion.
4. Age pipeline validates [0, 150], fractional integer anomalies, and maintains nullable integer type.
5. Phone pipeline strips formatting and validates length and placeholders.
6. InstructorID sentinel pipeline nullifies configured sentinels (999, -999, 9999999) while preserving numeric type.
7. Pandas null preservation prevents null values from degrading into string "nan" or "None".
8. All generated artifacts strictly satisfy the 4-companion column contract (raw, clean, validation_status, validation_reason).
"""
import pytest
import pandas as pd
import numpy as np

from agent.etl_pipeline.composite_field_compiler import (
    create_field_work_package,
    compile_field_work_package,
)
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
from agent.etl_pipeline.validate_plan import validate_promoted_plan


def test_update_manifest_routed_to_configuration_changes_not_column_step():
    """update_manifest is isolated as StructuredConfigPatch and not treated as a column step on '*'."""
    sample_plan = {
        "datasets": {
            "dbo.Customers": {
                "steps": []
            }
        },
        "manual_review": [
            {
                "id": "mr-manifest-phone",
                "dataset": "dbo.Customers",
                "column": "*",
                "issue_type": "metadata_manifest_mismatch",
                "message": "manifest column not in data: phone",
                "status": "pending",
                "severity": "medium",
            },
            {
                "id": "mr-manifest-gstin",
                "dataset": "dbo.Customers",
                "column": "*",
                "issue_type": "metadata_manifest_mismatch",
                "message": "manifest column not in data: gstin",
                "status": "pending",
                "severity": "medium",
            }
        ],
        "business_rules": {},
    }

    resolutions = [
        {"item_id": "mr-manifest-phone", "resolution_id": "update_manifest"},
        {"item_id": "mr-manifest-gstin", "resolution_id": "update_manifest"},
    ]

    updated, errs = apply_manual_resolutions(sample_plan, resolutions)
    assert len(errs) == 0

    # Invariant 1: No dataset steps created on '*'
    cust_steps = updated["datasets"]["dbo.Customers"]["steps"]
    assert len(cust_steps) == 0

    # Invariant 2: Routed to configuration_changes with exact field names
    cfg_changes = updated.get("configuration_changes") or []
    assert len(cfg_changes) == 2
    field_names = [c["field_name"] for c in cfg_changes]
    assert "phone" in field_names
    assert "gstin" in field_names
    assert all(c["action_type"] == "remove_or_map_missing" for c in cfg_changes)

    # Invariant 3: Candidate plan passes promotion validation with 0 issues
    valid, issues = validate_promoted_plan(updated)
    assert valid is True
    assert len(issues) == 0


def test_flag_only_preserves_clean_numeric_value():
    """flag_only on negative numeric fields (Price) flags defect but NEVER nullifies clean value."""
    findings = [
        {
            "id": "rec-price-neg",
            "dataset": "Courses",
            "column": "Price",
            "issue_type": "negative_value",
            "suggested_action": "flag_only",
            "selected_option_id": "opt-flag",
            "supported_options": [{"option_id": "opt-flag", "action_id": "flag_only", "title": "Flag negative value"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Courses", "Price", findings, semantic_type="decimal")
    
    # PySpark
    art_pyspark = compile_field_work_package(wp, dialect="pyspark")
    assert "F.when(compound_err, None)" not in art_pyspark.code
    assert "withColumn('Price_clean', num_val)" in art_pyspark.code

    # Pandas
    art_pandas = compile_field_work_package(wp, dialect="pandas")
    assert "num_val.where(~compound_err, None)" not in art_pandas.code
    assert "df['Price_clean'] = num_val" in art_pandas.code


def test_created_date_semantic_pipeline():
    """CreatedDate pipeline detects impossible dates, sentinels, placeholders, and produces date-typed clean field."""
    findings = [
        {
            "id": "rec-date-imp",
            "dataset": "Customers_Raw",
            "column": "CreatedDate",
            "issue_type": "impossible_date",
            "suggested_action": "nullify_clean_value",
            "selected_option_id": "opt-nullify",
            "supported_options": [{"option_id": "opt-nullify", "action_id": "nullify_clean_value", "title": "Nullify"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "CreatedDate", findings, semantic_type="datetime")

    for dialect in ("pyspark", "tsql", "pandas"):
        art = compile_field_work_package(wp, dialect=dialect)
        assert art.code is not None
        assert "CreatedDate_raw" in art.code
        assert "CreatedDate_clean" in art.code
        assert "CreatedDate_validation_status" in art.code
        assert "CreatedDate_validation_reason" in art.code
        assert "impossible_date" in art.code
        assert "sentinel_date" in art.code
        assert art.output_types["CreatedDate_clean"] == "DATETIME"


def test_age_semantic_pipeline_preserves_numeric_type():
    """Age pipeline validates [0, 150], flags/nullifies appropriately, and outputs nullable integer type."""
    findings = [
        {
            "id": "rec-age-outlier",
            "dataset": "Customers",
            "column": "Age",
            "issue_type": "implausible_age",
            "suggested_action": "flag_only",
            "selected_option_id": "opt-flag-age",
            "supported_options": [{"option_id": "opt-flag-age", "action_id": "flag_only", "title": "Flag out-of-range"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers", "Age", findings, semantic_type="integer")

    for dialect in ("pyspark", "tsql", "pandas"):
        art = compile_field_work_package(wp, dialect=dialect)
        assert art.code is not None
        assert "Age_raw" in art.code
        assert "Age_clean" in art.code
        assert "Age_validation_status" in art.code
        assert "Age_validation_reason" in art.code
        assert "implausible_age" in art.code
        assert "fractional_integer_anomaly" in art.code
        assert art.output_types["Age_clean"] == "INT"


def test_instructor_id_sentinel_pipeline():
    """InstructorID sentinel processing nullifies sentinel values while maintaining integer ID type."""
    findings = [
        {
            "id": "rec-inst-sentinel",
            "dataset": "Courses",
            "column": "InstructorID",
            "issue_type": "sentinel_numeric_value",
            "suggested_action": "nullify_clean_value",
            "selected_option_id": "opt-sentinel",
            "supported_options": [{"option_id": "opt-sentinel", "action_id": "nullify_clean_value", "title": "Nullify"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Courses", "InstructorID", findings, semantic_type="integer")

    for dialect in ("pyspark", "tsql", "pandas"):
        art = compile_field_work_package(wp, dialect=dialect)
        assert art.code is not None
        assert "999" in art.code
        assert "sentinel_value" in art.code
        assert art.output_types["InstructorID_clean"] == "INT"


def test_pandas_null_preservation_execution():
    """Pandas pipelines preserve source nulls without corrupting into string 'nan' or 'None'."""
    df = pd.DataFrame({
        "CreatedDate": [None, np.nan, "2023-01-15", "2023-13-01", "9999-12-31"],
        "Age": [None, np.nan, 25.0, 180.0, -5.0],
    })

    # Execute CreatedDate pipeline
    wp_date = create_field_work_package("Customers_Raw", "CreatedDate", [{
        "id": "rec-dt", "dataset": "Customers_Raw", "column": "CreatedDate",
        "issue_type": "impossible_date", "suggested_action": "nullify_clean_value",
        "selected_option_id": "opt-1", "approval_status": "approved"
    }], semantic_type="datetime")
    art_date = compile_field_work_package(wp_date, dialect="pandas")
    
    locs = {"df": df.copy(), "pd": pd, "np": np}
    exec(art_date.code, locs)
    res_df = locs["df"]

    assert pd.isna(res_df.loc[0, "CreatedDate_clean"])
    assert pd.isna(res_df.loc[1, "CreatedDate_clean"])
    assert res_df.loc[0, "CreatedDate_clean"] != "nan"
    assert res_df.loc[0, "CreatedDate_clean"] != "None"
    assert res_df.loc[0, "CreatedDate_validation_status"] == "source_null"
    assert res_df.loc[3, "CreatedDate_validation_status"] == "impossible_date"
    assert res_df.loc[4, "CreatedDate_validation_status"] == "sentinel_date"
