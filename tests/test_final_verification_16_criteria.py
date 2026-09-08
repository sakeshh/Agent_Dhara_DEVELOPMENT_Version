import pandas as pd
import numpy as np
from agent.profiling.gx_capability_registry import RULE_OWNERSHIP
from agent.profiling.dq_checks import analyze_dataset_quality
from agent.profiling.engine3_pipeline import run_engine3_pipeline
from agent.service_layer import build_dq_scorecard, build_html_report, build_markdown_report
from agent.dq_recommendations_agent import get_pandas_code, get_pyspark_code


def test_16_production_criteria():
    # 1. Semantic & Conformity
    rule_info = RULE_OWNERSHIP.get("column.type_cast")
    assert rule_info["dimension"] == "conformity"

    # Test analyze_dataset_quality on integer stored as float
    df_float_age = pd.DataFrame({"Age": [25.0] * 12})
    profile_float_age = {"columns": {"Age": {"semantic_type": "numeric"}}}
    dq_out = analyze_dataset_quality("dbo.Customers", df_float_age, profile_float_age)
    issues = dq_out.get("issues", [])
    float_issues = [i for i in issues if i.get("type") == "integer_stored_as_float"]
    assert len(float_issues) == 1
    assert float_issues[0]["severity"] == "low"

    # 2. Pipeline Execution
    df_customers = pd.DataFrame({
        "CustomerID": [1, 2, 3, 4],
        "Age": [25.0, 30.0, -5.0, 180.0],
        "Phone": ["+1 (555) 123-4567", "555-987-6543", "1111111111", "abcd"],
        "Email": ["valid@example.com", "user@domain.com", "invalid-email", "another@site.org"]
    })

    raw_issues_customers = [
        {"type": "integer_stored_as_float", "column": "Age", "count": 2, "row_indexes": [0, 1], "severity": "low"},
        {"type": "implausible_age", "column": "Age", "count": 2, "row_indexes": [2, 3], "severity": "high", "gating": True},
        {"type": "invalid_phone", "column": "Phone", "count": 2, "row_indexes": [2, 3], "severity": "medium"},
        {"type": "invalid_email", "column": "Email", "count": 1, "row_indexes": [2], "severity": "medium"}
    ]

    res_cust = run_engine3_pipeline(
        name="dbo.Customers",
        df=df_customers,
        raw_issues=raw_issues_customers,
        gx_results_raw=[],
        profile={"columns": {"CustomerID": {"semantic_type": "identifier"}, "Age": {"semantic_type": "numeric"}, "Phone": {"semantic_type": "phone"}, "Email": {"semantic_type": "email"}}}
    )

    assert res_cust.get("gating_adjusted_score") <= 69.9
    assert res_cust.get("readiness") == "NEEDS_WORK"

    # Courses
    df_courses = pd.DataFrame({
        "CourseID": [101, 102, 103],
        "Price": [50.0, -20.0, 99.99],
        "InstructorID": [10, 999, 12]
    })
    raw_issues_courses = [
        {"type": "negative_value", "column": "Price", "count": 10, "row_indexes": list(range(10)), "severity": "high", "gating": True},
        {"type": "sentinel_numeric_value", "column": "InstructorID", "count": 3, "row_indexes": [1], "severity": "medium"}
    ]
    res_courses = run_engine3_pipeline(
        name="dbo.Courses",
        df=df_courses,
        raw_issues=raw_issues_courses,
        gx_results_raw=[],
        profile={"columns": {"CourseID": {"semantic_type": "identifier"}, "Price": {"semantic_type": "numeric"}, "InstructorID": {"semantic_type": "identifier"}}}
    )

    assessment_result = {
        "datasets": {
            "dbo.Customers": {"row_count": 60, "column_count": 4, "size_in_bytes": 2400, "columns": {
                "CustomerID": {"dtype": "int64", "semantic_type": "identifier"},
                "Age": {"dtype": "float64", "semantic_type": "numeric"},
                "Phone": {"dtype": "object", "semantic_type": "phone"},
                "Email": {"dtype": "object", "semantic_type": "email"}
            }},
            "dbo.Courses": {"row_count": 80, "column_count": 3, "size_in_bytes": 1920, "columns": {
                "CourseID": {"dtype": "int64", "semantic_type": "identifier"},
                "Price": {"dtype": "float64", "semantic_type": "numeric"},
                "InstructorID": {"dtype": "int64", "semantic_type": "identifier"}
            }},
            "dbo.Customers_Raw": {"row_count": 50, "column_count": 2, "size_in_bytes": 1000, "columns": {
                "RawID": {"dtype": "int64", "semantic_type": "identifier"},
                "CreatedDate": {"dtype": "object", "semantic_type": "date"}
            }}
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Customers": {
                    "summary": res_cust,
                    "issues": raw_issues_customers
                },
                "dbo.Courses": {
                    "summary": res_courses,
                    "issues": raw_issues_courses
                },
                "dbo.Customers_Raw": {
                    "summary": {"dq_score_0_100": 65.0, "estimated_clean_rows_after_high": 45},
                    "issues": [
                        {"type": "impossible_date", "column": "CreatedDate", "count": 5, "row_indexes": [0, 1, 2, 3, 4], "severity": "high", "gating": True}
                    ]
                }
            },
            "global_issues": {}
        },
        "governance": {
            "assessment_run_id": "run-prod-001",
            "job_id": "job-prod-001",
            "deployment_version": "v2.4.0-prod",
            "schema_hash": "sha256:abcd1234ef56",
            "rules_hash": "sha256:7890fedcba"
        }
    }

    # 3. Scorecard & Gates
    scorecard = build_dq_scorecard(assessment_result)
    assert scorecard.get("verdict") in ["NEEDS_WORK", "BLOCKED"]
    assert "Courses (Price: 10)" in scorecard.get("high_breakdown_str")
    assert "Customers (Age: 2)" in scorecard.get("high_breakdown_str")
    assert "Customers_Raw (CreatedDate: 5)" in scorecard.get("high_breakdown_str")
    assert "Total: 17" in scorecard.get("high_breakdown_str")

    courses_entry = next(d for d in scorecard.get("datasets", []) if "Courses" in d.get("name"))
    courses_gates = courses_entry.get("use_case_gates", {})
    assert courses_gates.get("catalogue_publication") == "BLOCKED"
    assert courses_gates.get("pricing_consumption") == "BLOCKED"
    assert courses_gates.get("customer_contact") == "NOT_APPLICABLE"

    # 4. HTML Report No Escaped Tags
    html_rep = build_html_report(assessment_result)
    assert "&lt;strong&gt;" not in html_rep
    assert 'id="score-methodology"' in html_rep
    assert 'id="technical-appendix"' in html_rep
    assert "run-prod-001" in html_rep
    assert "sha256:abcd1234ef56" in html_rep
    assert "NOT APPLICABLE" in html_rep

    # 5. Remediation Snippets 4-Column Contract
    p_sentinel = get_pandas_code("InstructorID", "sentinel_numeric_value")
    assert "999" in p_sentinel
    assert "quarantine_reason" in p_sentinel
    assert "quarantine_df" in p_sentinel
    assert "clean_df" in p_sentinel

    p_phone = get_pandas_code("Phone", "invalid_phone")
    assert "digits" in p_phone
    assert "quarantine_reason" in p_phone
    assert "quarantine_df" in p_phone
    assert "clean_df" in p_phone

    p_email = get_pandas_code("Email", "invalid_email")
    assert "is_valid" in p_email
    assert "quarantine_reason" in p_email

    p_date = get_pandas_code("CreatedDate", "impossible_date")
    assert "is_impossible" in p_date
    assert "errors='coerce'" in p_date

    p_float = get_pandas_code("Age", "integer_stored_as_float")
    assert "Int64" in p_float

    # 6. Contextual Negative Value Remediation (Age vs Price)
    p_age_neg = get_pandas_code("Age", "negative_value")
    assert "Age cannot be negative" in p_age_neg
    assert "Catalog price cannot be negative" not in p_age_neg

    spark_age_neg = get_pyspark_code("Age", "negative_value")
    assert "Age cannot be negative" in spark_age_neg

    p_price_neg = get_pandas_code("Price", "negative_value")
    assert "Catalog price cannot be negative" in p_price_neg

    # 7. PII Diagnostic Masking
    from agent.service_layer import mask_pii_sample
    assert mask_pii_sample("user@example.com", "Email") == "u****@example.com"
    assert mask_pii_sample("+1 (555) 123-4567", "Phone") == "+1 (555) ***-4567"
    assert mask_pii_sample("555-123-4567", "phone_number") == "555-***-4567"
    assert mask_pii_sample("-25", "Age") == "-25"
    assert mask_pii_sample("2099-01-01", "CreatedDate") == "2099-01-01"

    # 8. Dual Scores & Guidance Notes
    md_rep = build_markdown_report(assessment_result)
    assert "Intrinsic Score" in md_rep
    assert "Gating-Adjusted Score" in md_rep
    assert "Executive Score Interpretation Guidance" in md_rep
    assert "Accuracy Plausibility Proxy Notice" in md_rep

    assert "Executive Score Interpretation Guidance" in html_rep
    assert "Accuracy Plausibility Proxy Notice" in html_rep
    assert "Git Commit SHA" in html_rep
    assert "fuzzy_token_sort (base threshold: 0.85, effective threshold: 0.95" in html_rep

    # 9. Recommendation Prioritization & Provenance
    from agent.dq_recommendations_agent import DQRecommendationsAgent
    agent = DQRecommendationsAgent()
    rec_out, _ = agent.recommend(merged_dq=assessment_result["data_quality_issues"])
    recs = rec_out.recommendations
    assert len(recs) > 0
    # Confirm provenance tag
    assert all("recommendation_source" in r for r in recs)
    # Confirm high severity comes before medium
    high_seen = False
    med_after_high = False
    for r in recs:
        sev = (r.get("severity") or "").upper()
        if sev == "HIGH":
            high_seen = True
            assert not med_after_high, "HIGH issue appeared after MEDIUM issue!"
        elif sev == "MEDIUM":
            med_after_high = True

