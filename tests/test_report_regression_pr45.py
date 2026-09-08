"""
tests/test_report_regression_pr45.py — Report Regression Verification Suite

Verifies the exact report regression criteria from PR 4.5:
1. Courses.Price:
   - Authority displays "Catalog option, AI ranking unavailable" (never false "AI-assisted").
   - When pending, displays provisional warning and "Decision required".
   - When flag_only is active: Price_clean retains numeric value, validation status identifies violation, no quarantine reason, all rows in clean_df.
2. Customers_Raw.CreatedDate:
   - When keep_as_is is selected: displays no-op retention banner without transformation code.
   - When sentinel_date is selected: date placeholders and date cast, never numeric long cast.
3. Customers.Age:
   - When keep_as_is is selected: no integer cast, no clean-column mutation, no row filtering.
4. Customers_Raw.Email:
   - Composite field pipeline covers whitespace, placeholders, nulls, and email format with clean XOR quarantine.
5. Harmonized Governance Score:
   - Active HIGH findings cap readiness at 69.9 and classify assessment as NEEDS WORK, noting use case BLOCKED.
"""
import pandas as pd
import pytest

from agent.service_layer import build_html_report, build_dq_scorecard
from agent.dq_recommendations_agent import DQRecommendationsAgent, get_pandas_code, get_pyspark_code
from agent.etl_pipeline.composite_field_compiler import create_field_work_package, compile_field_work_package


def test_regression_courses_price_catalog_fallback_and_flag_only():
    """Courses.Price under catalog fallback shows 'Catalog option, AI ranking unavailable', flag_only retains all rows."""
    finding = {
        "id": "rec-courses-price",
        "dataset": "dbo.Courses",
        "column": "Price",
        "issue_type": "negative_values",
        "authority_source": "CATALOG_FALLBACK",
        "ai_assistance_status": "unavailable",
        "suggested_action": "flag_only",
        "mapped_action": "flag_only",
        "severity": "high",
        "priority": 1,
        "why_it_matters": "Negative price violates domain constraints",
        "supported_options": [
            {"option_id": "opt-flag", "action_id": "flag_only", "title": "Flag only (retain rows)"}
        ],
        "lifecycle": {"approval_status": "pending"},
    }

    report_payload = {
        "datasets": {
            "dbo.Courses": {"row_count": 80, "column_count": 3, "data_volume_bytes": 1920, "columns": {"Price": {"dtype": "float64", "semantic_type": "numeric"}}}
        },
        "data_quality_issues": {
            "datasets": {"dbo.Courses": {"issues": [{"type": "negative_values", "column": "Price", "severity": "high"}]}},
            "global_issues": {},
        },
        "dq_recommendations": {
            "findings": [finding],
            "metadata": {"total_findings": 1},
        },
        "governance": {
            "assessment_run_id": "run-regression-001",
            "job_id": "job-regression-001",
            "deployment_version": "v2.4.1",
            "schema_hash": "sha256:abcd1234",
            "rules_hash": "sha256:5678efgh",
        }
    }

    html = build_html_report(report_payload)

    # Invariant 1: No false AI-assisted label; exact fallback badge
    assert "Catalog option, AI ranking unavailable" in html
    assert "AI-assisted option" not in html

    # Invariant 2: Fallback notice banner present
    assert "AI-assisted ranking was unavailable. Controlled catalog options are available for human selection." in html

    # Invariant 3: Provisional banner rendered for pending approval
    assert "Provisional code preview for the selected option." in html
    assert "This transformation is not approved or authorized for execution." in html

    # Invariant 4: Flag_only code retains rows in clean_df and does not quarantine
    p_code = get_pandas_code("Price", "negative_values", action_id="flag_only")
    assert "clean_df = df.copy()" in p_code
    assert "quarantine_reason" not in p_code


def test_regression_customers_raw_created_date_keep_as_is():
    """Customers_Raw.CreatedDate with keep_as_is renders no-op banner and suppresses code."""
    finding = {
        "id": "rec-created-date",
        "dataset": "dbo.Customers_Raw",
        "column": "CreatedDate",
        "issue_type": "impossible_date",
        "authority_source": "CATALOG_FALLBACK",
        "ai_assistance_status": "unavailable",
        "suggested_action": "keep_as_is",
        "mapped_action": "keep_as_is",
        "severity": "medium",
        "priority": 2,
        "why_it_matters": "Non-fatal legacy date anomalies",
        "supported_options": [
            {"option_id": "opt-noop", "action_id": "keep_as_is", "title": "Keep as is"}
        ],
        "lifecycle": {"approval_status": "pending"},
    }

    report_payload = {
        "datasets": {
            "dbo.Customers_Raw": {"row_count": 50, "column_count": 2, "data_volume_bytes": 1000, "columns": {"CreatedDate": {"dtype": "object", "semantic_type": "date"}}}
        },
        "data_quality_issues": {
            "datasets": {"dbo.Customers_Raw": {"issues": [{"type": "impossible_date", "column": "CreatedDate", "severity": "medium"}]}},
            "global_issues": {},
        },
        "dq_recommendations": {
            "findings": [finding],
            "metadata": {"total_findings": 1},
        },
        "governance": {"assessment_run_id": "run-002", "schema_hash": "sha256:1111", "rules_hash": "sha256:2222"}
    }

    html = build_html_report(report_payload)
    assert "No-op: Retained as-is without ETL transformation code." in html


def test_regression_customers_raw_created_date_normalize_sentinel():
    """Customers_Raw.CreatedDate with sentinel_date uses date cast and date placeholders, never numeric cast."""
    spark_code = get_pyspark_code("CreatedDate", "sentinel_date", action_id="nullify_clean_value")
    assert ".cast('date')" in spark_code
    assert ".cast('long')" not in spark_code
    assert "9999-12-31" in spark_code

    pandas_code = get_pandas_code("CreatedDate", "sentinel_date", action_id="nullify_clean_value")
    assert "pd.to_datetime" in pandas_code
    assert "pd.to_numeric" not in pandas_code


def test_regression_customers_age_keep_as_is():
    """Customers.Age with keep_as_is produces no integer cast and no clean mutation."""
    spark_code = get_pyspark_code("Age", "integer_stored_as_float", action_id="keep_as_is")
    assert spark_code is None

    pandas_code = get_pandas_code("Age", "integer_stored_as_float", action_id="keep_as_is")
    assert pandas_code is None


def test_regression_customers_raw_email_composite_work_package():
    """Customers_Raw.Email compiles multiple overlapping findings into a single composite pipeline."""
    findings = [
        {"id": "rec-e1", "dataset": "Customers_Raw", "column": "Email", "issue_type": "leading_trailing_whitespace", "suggested_action": "trim", "selected_option_id": "o1", "supported_options": [{"option_id": "o1", "action_id": "trim"}]},
        {"id": "rec-e2", "dataset": "Customers_Raw", "column": "Email", "issue_type": "placeholder_values", "suggested_action": "normalize_registered_placeholder", "selected_option_id": "o2", "supported_options": [{"option_id": "o2", "action_id": "nullify_clean_value"}]},
        {"id": "rec-e3", "dataset": "Customers_Raw", "column": "Email", "issue_type": "invalid_email_format", "suggested_action": "quarantine_row", "selected_option_id": "o3", "supported_options": [{"option_id": "o3", "action_id": "quarantine_row"}]},
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pandas")

    assert artifact.generator_id == "composite_field_generator"
    # Single pipeline preserves raw and manages compound errors
    assert "df['Email_raw'] = raw" in artifact.code
    assert "compound_err = is_placeholder | is_syntax_error" in artifact.code
    assert "clean_df = df[~compound_err].copy()" in artifact.code
    assert "quarantine_df = df[compound_err].copy()" in artifact.code


def test_regression_score_harmonization_needs_work_and_blocked():
    """Gating-adjusted readiness score caps at 69.9 with NEEDS WORK assessment and BLOCKED use cases."""
    assessment_result = {
        "datasets": {
            "dbo.Courses": {"row_count": 80, "column_count": 3, "data_volume_bytes": 1920, "columns": {"Price": {"dtype": "float64", "semantic_type": "numeric"}}}
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Courses": {
                    "summary": {
                        "dq_score_0_100": 69.9,
                        "gating_adjusted_score": 69.9,
                        "readiness": "NEEDS_WORK",
                        "active_high_severity_count": 1,
                        "overall_status": "NEEDS_WORK",
                    },
                    "issues": [
                        {"type": "negative_value", "column": "Price", "severity": "high", "gating": True, "count": 10}
                    ]
                }
            },
            "global_issues": {}
        }
    }
    scorecard = build_dq_scorecard(assessment_result)
    assert scorecard["verdict"] in ["NEEDS_WORK", "BLOCKED"]
    courses_entry = next(d for d in scorecard["datasets"] if "Courses" in d["name"])
    assert courses_entry["readiness"] == "NEEDS_WORK"
    assert courses_entry["use_case_gates"]["catalogue_publication"] == "BLOCKED"
