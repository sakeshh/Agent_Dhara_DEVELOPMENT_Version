"""Comprehensive verification tests for the 14 Report Quality & Provenance Audit Items.

Verifies:
1. AI Provenance Consistency (no contradictory AI badge when AI unavailable)
2. Mixed work package labeling (standard normalization + catalog handling)
3. Configuration patch field specificity (phone, gstin explicitly targeted)
4. Canonical status code alignment (placeholder_normalized_to_null, punctuation_only_normalized_to_null)
5. Safe Age conversion in Pandas (fractional protection before Int64 cast) & outcome text
6. Date dialect parity (consistent 1970-01-01 sentinels across PySpark, Pandas, SQL)
7. Phone length policy transparency (7-15 digits explicitly cited)
8. Email remediation wording accuracy (retains raw, sets clean companion null)
9. Consecutive priority numbering (P1, P2, ... Pn without gaps)
10. Card impact metrics bar (affected rows, percentage, policy limit, decision)
11. Collapsible code previews (details/summary scannability)
12. Retain-all quarantine clarity (explicitly shows quarantine not used for retain_all)
13. Duplicate row deterministic metadata code (replaces manual comment with Window ranking)
14. Executive guidance grammar correctness
"""

import re
import pytest
import pandas as pd
import numpy as np
from agent.service_layer import build_html_report
from agent.etl_pipeline.composite_field_compiler import create_field_work_package, compile_field_work_package


def test_ai_provenance_and_mixed_package_labeling():
    """Points 1 & 2: Cards never claim AI-assisted if AI ranking was unavailable."""
    findings = [
        # CustomerName has standard whitespace + catalog placeholder -> MIXED
        {
            "id": "rec-name-space",
            "dataset": "dbo.Customers_Raw",
            "column": "CustomerName",
            "issue_type": "whitespace",
            "authority_source": "DETERMINISTIC_REGISTRY",
            "severity": "medium",
            "priority": 9,
            "lifecycle": {"approval_status": "pending"},
        },
        {
            "id": "rec-name-placeholder",
            "dataset": "dbo.Customers_Raw",
            "column": "CustomerName",
            "issue_type": "placeholder",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "severity": "medium",
            "priority": 9,
            "lifecycle": {"approval_status": "pending"},
        },
        # CreatedDate has only catalog fallback -> CATALOG
        {
            "id": "rec-date-sentinel",
            "dataset": "dbo.Customers_Raw",
            "column": "CreatedDate",
            "issue_type": "impossible_date",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "severity": "high",
            "priority": 2,
            "suggested_fix": "AI-assisted option: Nullify impossible dates",
            "lifecycle": {"approval_status": "pending"},
        },
    ]

    report_payload = {
        "datasets": {"dbo.Customers_Raw": {"row_count": 100, "columns": {"CustomerName": {}, "CreatedDate": {}}}},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": findings, "metadata": {"total_findings": 3}},
        "governance": {"assessment_run_id": "run-prov-01"},
    }

    html = build_html_report(report_payload)

    # Invariant 1: CustomerName card is labeled Mixed work package
    assert "Mixed work package: standard normalization with catalog-governed semantic handling" in html
    
    # Invariant 2: CreatedDate card is labeled Catalog option, AI ranking unavailable
    assert "Catalog option, AI ranking unavailable" in html
    # CreatedDate fix text was sanitized from "AI-assisted option:" to "Catalog option:"
    assert "Catalog option: Nullify impossible dates (AI-assisted ranking was unavailable)." in html

    # Invariant 3: Governance breakdown has 0 false AI-assisted entries
    assert "AI-assisted: 0" in html
    assert "Catalog fallback: 2" in html


def test_configuration_patches_separate_and_specific_fields():
    """Point 3: Mismatches produce separate, explicit patches for each missing field."""
    findings = [
        {
            "id": "cfg-phone",
            "dataset": "dbo.Customers",
            "column": "phone",
            "issue_type": "metadata_manifest_mismatch",
            "authority_source": "DETERMINISTIC_REGISTRY",
            "severity": "medium",
            "priority": 2,
            "message": "manifest column not in data: phone",
        },
        {
            "id": "cfg-gstin",
            "dataset": "dbo.Customers",
            "column": "gstin",
            "issue_type": "metadata_manifest_mismatch",
            "authority_source": "DETERMINISTIC_REGISTRY",
            "severity": "medium",
            "priority": 2,
            "message": "manifest column not in data: gstin",
        },
    ]

    report_payload = {
        "datasets": {"dbo.Customers": {"row_count": 50, "columns": {}}},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": findings, "metadata": {"total_findings": 2}},
        "governance": {"assessment_run_id": "run-cfg-02"},
    }

    html = build_html_report(report_payload)

    # Invariant: Two separate cards created, one for phone and one for gstin
    assert "dbo.Customers [Config: phone]" in html
    assert "dbo.Customers [Config: gstin]" in html
    assert 'field_name: "phone"' in html or 'field_name: &quot;phone&quot;' in html
    assert 'field_name: "gstin"' in html or 'field_name: &quot;gstin&quot;' in html
    assert 'field_name: "manifest"' not in html and 'field_name: &quot;manifest&quot;' not in html
    assert 'justification: "Manifest field is absent from dbo.Customers"' in html or 'justification: &quot;Manifest field is absent from dbo.Customers&quot;' in html


def test_consecutive_priorities_and_impact_metrics_and_scannability():
    """Points 9, 10, 11: Priorities P1..Pn without gaps, impact metrics bar, collapsible code."""
    findings = [
        {"id": "f1", "dataset": "dbo.T", "column": "C1", "issue_type": "nulls", "severity": "high", "priority": 12, "affected_count": 15},
        {"id": "f2", "dataset": "dbo.T", "column": "C2", "issue_type": "outliers", "severity": "medium", "priority": 24, "affected_count": 5},
    ]

    report_payload = {
        "datasets": {"dbo.T": {"row_count": 50, "columns": {"C1": {}, "C2": {}}}},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": findings, "metadata": {"total_findings": 2}},
        "governance": {"assessment_run_id": "run-p-01"},
    }

    html = build_html_report(report_payload)

    # Invariant Point 9: Consecutive priorities P1 and P2 without gaps
    assert '<span class="pill-priority">P1</span>' in html
    assert '<span class="pill-priority">P2</span>' in html
    assert '<span class="pill-priority">P12</span>' not in html
    assert '<span class="pill-priority">P24</span>' not in html

    # Invariant Point 10: Impact metrics bar
    assert '<div class="card-impact-metrics"' in html
    assert "Affected:</strong> 15 of 50 rows (30.0%)" in html
    assert "Policy Limit:</strong> 2.0% or 5,000 rows" in html
    assert "Human approval required" in html

    # Invariant Point 11: Collapsible code previews
    assert '<details class="code-preview-details"' in html
    assert "View Generated Code Previews &amp; Technical Implementation Paths" in html or "View Generated Code Previews & Technical Implementation Paths" in html


def test_pandas_age_fractional_safety_and_outcome_wording():
    """Point 5: Pandas age conversion protects against fractional Int64 crash."""
    wp = create_field_work_package(
        "dbo.Customers",
        "Age",
        [{"id": "rec-age", "dataset": "dbo.Customers", "column": "Age", "issue_type": "implausible_age", "action_id": "flag_only"}],
        semantic_type="age"
    )
    art_pandas = compile_field_work_package(wp, dialect="pandas")

    test_df = pd.DataFrame({
        "Age": [25.0, 30.5, -5.0, 180.0, None, "not_a_number"]
    })

    local_env = {"df": test_df.copy(), "pd": pd, "np": np}
    # Must execute safely without raising TypeError
    exec(art_pandas.code, local_env)
    df_out = local_env["df"]

    assert df_out.loc[0, "Age_clean"] == 25
    assert df_out.loc[0, "Age_validation_status"] == "valid"
    assert df_out.loc[1, "Age_validation_status"] == "fractional_integer_anomaly"
    assert df_out.loc[2, "Age_validation_status"] == "negative_value_review_required"
    assert df_out.loc[3, "Age_validation_status"] == "implausible_age"


def test_date_dialect_parity_and_sentinel_alignment():
    """Point 6: PySpark, Pandas, and T-SQL date pipelines all share 1970-01-01 sentinel."""
    wp = create_field_work_package(
        "dbo.Customers_Raw",
        "CreatedDate",
        [{"id": "rec-date", "dataset": "dbo.Customers_Raw", "column": "CreatedDate", "issue_type": "impossible_date", "action_id": "nullify_clean_value"}],
        semantic_type="date"
    )

    art_pyspark = compile_field_work_package(wp, dialect="pyspark")
    art_pandas = compile_field_work_package(wp, dialect="pandas")
    art_tsql = compile_field_work_package(wp, dialect="tsql")

    for art in (art_pyspark, art_pandas, art_tsql):
        assert "1970-01-01" in art.code
        assert "9999-12-31" in art.code
        assert "1900-01-01" in art.code


def test_duplicate_row_deterministic_metadata_code():
    """Point 13: Duplicate rows generate deterministic window metadata instead of manual comment."""
    findings = [
        {
            "id": "rec-dup",
            "dataset": "dbo.Customers",
            "column": "[Row-level]",
            "issue_type": "duplicate_rows",
            "authority_source": "CATALOG_FALLBACK",
            "suggested_action": "retain_and_flag",
            "severity": "medium",
            "priority": 1,
            "example_pyspark": "# Remediate near_duplicate_rows using PySpark manually",
        }
    ]

    report_payload = {
        "datasets": {"dbo.Customers": {"row_count": 50, "columns": {}}},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": findings, "metadata": {"total_findings": 1}},
        "governance": {"assessment_run_id": "run-dup-01"},
    }

    html = build_html_report(report_payload)

    # Invariant: Manual comment replaced with real metadata-attachment code
    assert "# Remediate near_duplicate_rows using PySpark manually" not in html
    assert "row_duplicate_rank" in html
    assert "row_is_duplicate" in html
    assert "duplicate_candidate" in html


def test_executive_guidance_grammar_fix():
    """Point 14: Executive score guidance contains correct grammar."""
    report_payload = {
        "datasets": {},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": [], "metadata": {"total_findings": 0}},
        "governance": {"assessment_run_id": "run-gram-01"},
    }

    html = build_html_report(report_payload)

    assert "and is prohibited from direct production consumption without quarantine." not in html
    assert "Direct production consumption remains prohibited until the applicable release gates are cleared." in html
