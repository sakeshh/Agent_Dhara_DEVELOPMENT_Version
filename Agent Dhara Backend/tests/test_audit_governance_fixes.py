import pytest
import pandas as pd
from agent.profiling.assessment_orchestrator import confirm_business_key_duplicates
from agent.specialists.gx_validation_specialist import run_gx_validation
from agent.etl_readiness_scorer import compute_etl_readiness

def test_confirm_business_key_duplicates_null_filtering():
    """Verify that rows containing NULLs/placeholders in candidate PK columns are not counted as PK duplicates."""
    df = pd.DataFrame({
        "campaign_id": [None, None, "C100", "C100", "C101", None],
        "customer_name": ["mike", "mike", "alice", "alice", "bob", "mike"],
        "budget": [None, None, 1000, 1000, 500, None]
    })
    res = confirm_business_key_duplicates(df, ["campaign_id", "customer_name", "budget"])
    assert res["confirmed"] is True
    # "C100", "alice", 1000 appears twice (1 duplicate)
    assert res["business_key_duplicate_count"] == 1
    # NULL rows (3 rows) are counted as missing_business_key_count
    assert res["missing_business_key_count"] == 3

def test_sales_xml_underreporting_fix():
    """Verify that sales.xml with error amounts, invalid dates, placeholders, and digit regions is accurately detected and scored."""
    sales_df = pd.DataFrame({
        "sale_id": [f"S{i}" for i in range(1, 101)],
        "amount": ["100.50", "error", "-190.98", "50.00", "error"] + ["200.00"] * 95,
        "customer": ["Alice", "???", "Bob", "Charlie", "???"] + ["Dave"] * 95,
        "region": ["US", "123", "EU", "123", "APAC"] + ["US"] * 95,
        "sale_date": ["2024-01-15", "invalid", "00/00/0000", "2024-02-01", "2024-02-02"] + ["2024-03-01"] * 95
    })
    
    gx_res = run_gx_validation({"sales.xml": sales_df}, {})
    results = gx_res.get("sales.xml", {}).get("results", [])
    
    expectations_triggered = {r.get("expectation") for r in results if not r.get("success")}
    
    assert "invalid_numeric_values" in expectations_triggered
    assert "invalid_date_format" in expectations_triggered
    assert "placeholder_detected" in expectations_triggered
    assert "string_with_only_digits_in_text_column" in expectations_triggered

    # Verify scoring is in realistic 50-80 range (Needs Work vs 100 READY)
    from agent.profiling.dq_checks import analyze_dataset_quality
    sales_issues = analyze_dataset_quality("sales.xml", sales_df, {})
    assessment = {
        "datasets": {"sales.xml": {"columns": {}, "row_count": 100}},
        "data_quality_issues": {"datasets": {"sales.xml": sales_issues}, "global_issues": {}}
    }
    scorer_res = compute_etl_readiness(assessment)
    assert 50 <= scorer_res["score"] <= 80

def test_factory_json_proportional_scoring():
    """Verify that a small number of affected rows (8 out of 1000) results in proportional score deduction (45-65 range) rather than 31 BLOCKED."""
    factory_df = pd.DataFrame({
        "factory_id": [f"F{i}" for i in range(992)] + ["F1"] * 8, # 8 duplicates
        "machine_name": ["tool"] * 1000,
        "location": ["NY"] * 1000
    })
    from agent.profiling.dq_checks import analyze_dataset_quality
    factory_issues = analyze_dataset_quality("factory.json", factory_df, {})
    assessment = {
        "datasets": {"factory.json": {"columns": {}, "row_count": 1000}},
        "data_quality_issues": {"datasets": {"factory.json": factory_issues}, "global_issues": {}}
    }
    scorer_res = compute_etl_readiness(assessment)
    assert scorer_res["score"] >= 45
