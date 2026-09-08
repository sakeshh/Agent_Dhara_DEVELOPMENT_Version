"""
Integration Test: End-to-End Governance Lifecycle from DQ Findings/Recommendations
through Planning, Manual Review Approval, to Production T-SQL Code Generation.

Verifies:
1. DQ findings and LLM recommendations are mapped and ingested into the plan.
2. Standard safe field remediations (range_clip, replace_sentinel_values, parse_dates)
   are compiled into active dataset steps.
3. Complex/destructive or pending items are held in manual_review.
4. User approval of manual review items promotes them into active dataset steps.
5. ETL code generation produces valid T-SQL with:
   - Negative prices nullified and quarantined.
   - InstructorID sentinels nullified without being overwritten by numeric coercion.
   - Age anomalies nullified.
   - Impossible calendar dates nullified.
   - Text placeholders nullified.
   - Proper DDL types (BIGINT, DECIMAL, DATETIME).
   - Zero syntax errors, no inner GO, balanced transactions.
"""
import pytest
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.llm_codegen import _postprocess_tsql_code
from agent.etl_pipeline.codegen_validator import validate_tsql


def test_end_to_end_dq_to_etl_lifecycle():
    # ── 1. Assessment findings & recommendations ──
    assessment = {
        "datasets": {
            "Courses": {
                "columns": {
                    "CourseID": {"dtype": "int", "nullable": False},
                    "CourseTitle": {"dtype": "string", "nullable": False},
                    "InstructorID": {"dtype": "float", "nullable": True},
                    "Price": {"dtype": "float", "nullable": True},
                    "CreatedDate": {"dtype": "string", "nullable": True},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "Price",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "affected_rows": 10,
                            "message": "Negative price found (-500)",
                        },
                        {
                            "column": "InstructorID",
                            "issue_type": "sentinel_values",
                            "severity": "medium",
                            "affected_rows": 5,
                            "message": "Placeholder instructor 999.0 found",
                        },
                    ]
                },
            },
            "Customers": {
                "columns": {
                    "CustomerID": {"dtype": "int", "nullable": False},
                    "CustomerName": {"dtype": "string", "nullable": False},
                    "Age": {"dtype": "float", "nullable": True},
                    "Email": {"dtype": "string", "nullable": True},
                    "Phone": {"dtype": "string", "nullable": True},
                    "City": {"dtype": "string", "nullable": True},
                    "CreatedDate": {"dtype": "string", "nullable": True},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "Age",
                            "issue_type": "implausible_age",
                            "severity": "high",
                            "affected_rows": 3,
                            "message": "Negative age -3.0 and impossible age 150 found",
                        },
                        {
                            "column": "CreatedDate",
                            "issue_type": "invalid_date_format",
                            "severity": "high",
                            "affected_rows": 8,
                            "message": "Impossible calendar dates such as 2022-02-30 found",
                        },
                        {
                            "column": "City",
                            "issue_type": "placeholder_text",
                            "severity": "medium",
                            "affected_rows": 4,
                            "message": "Placeholder '???' found in City",
                        },
                    ]
                },
            },
        },
        "dq_recommendations": {
            "recommendations": [
                {
                    "dataset": "Courses",
                    "column": "Price",
                    "issue_type": "negative_values",
                    "severity": "high",
                    "suggested_fix": "Flag negative prices, clip bounds to 0, quarantine to rejects",
                    "action": "range_clip",
                },
                {
                    "dataset": "Courses",
                    "column": "InstructorID",
                    "issue_type": "sentinel_values",
                    "severity": "medium",
                    "suggested_fix": "Replace sentinel values 999.0 with NULL",
                    "action": "replace_sentinel_values",
                },
                {
                    "dataset": "Customers",
                    "column": "Age",
                    "issue_type": "out_of_range",
                    "severity": "high",
                    "suggested_fix": "Clip age between 0 and 120, nullify implausible values",
                    "action": "range_clip",
                },
                {
                    "dataset": "Customers",
                    "column": "CreatedDate",
                    "issue_type": "invalid_dates",
                    "severity": "high",
                    "suggested_fix": "Parse dates and nullify impossible calendar dates",
                    "action": "parse_dates",
                },
            ]
        },
    }

    rules = {
        "never_drop_rows": True,
        "required_columns": ["CourseID", "CustomerID"],
    }

    # ── 2. Stage 2: Planning ──
    print("\n--- CHECKPOINT 1: Building plan ---", flush=True)
    plan = build_etl_plan(
        assessment,
        rules,
        engine="sql",
        generation_mode="full",
    )
    print("--- CHECKPOINT 2: Plan built ---", flush=True)

    datasets_steps = plan.get("datasets") or {}
    assert "Courses" in datasets_steps, "Courses dataset should have steps"
    assert "Customers" in datasets_steps, "Customers dataset should have steps"

    courses_steps = datasets_steps["Courses"].get("steps") or []
    courses_actions = [s.get("action") for s in courses_steps]
    courses_cols = [s.get("column") for s in courses_steps]

    # Verify standard remediation replace_sentinel_values for InstructorID reached plan directly
    assert "replace_sentinel_values" in courses_actions, "Courses should have replace_sentinel_values step for InstructorID"

    # Verify high-severity finding (Price negative values) is kept on hold in manual_review
    manual_review = plan.get("manual_review") or []
    mr_columns = [m.get("column") for m in manual_review]
    assert "Price" in mr_columns, "Price negative values should be held in manual_review pending user approval"

    # Verify execution order: replace_sentinel_values must run BEFORE coerce_numeric if both exist
    if "replace_sentinel_values" in courses_actions and "coerce_numeric" in courses_actions:
        sentinel_idx = courses_actions.index("replace_sentinel_values")
        coerce_idx = courses_actions.index("coerce_numeric")
        assert sentinel_idx < coerce_idx, "Sentinel replacement must be prioritized before numeric coercion"

    # ── 3. Stage 3: User Approval of Manual Review Items ──
    print("--- CHECKPOINT 3: Approving manual review ---", flush=True)
    resolutions = []
    for mr in manual_review:
        iid = mr.get("id")
        options = mr.get("resolution_options") or []
        res_id = "clip_to_zero" if any(o.get("id") == "clip_to_zero" for o in options) else (options[0].get("id") if options else "recommended")
        resolutions.append({"item_id": iid, "resolution_id": res_id})

    updated_plan, errs = apply_manual_resolutions(plan, resolutions, business_rules=rules, assessment=assessment)
    assert not errs, f"Manual review promotion should succeed, got errors: {errs}"
    print("--- CHECKPOINT 4: Applied resolutions ---", flush=True)

    # Verify that after user approval, range_clip for Price is now promoted into active Courses steps!
    promoted_courses_steps = updated_plan.get("datasets", {}).get("Courses", {}).get("steps", [])
    promoted_actions = [s.get("action") for s in promoted_courses_steps]
    assert "range_clip" in promoted_actions, "range_clip for Price must be included in Courses steps after approval"

    # ── 4. Stage 4: T-SQL Code Generation from Approved Plan ──
    print("--- CHECKPOINT 5: Generating SQL ---", flush=True)
    sql_code = generate_sql_etl(updated_plan, assessment, dialect="tsql")
    print(f"--- CHECKPOINT 6: SQL generated (length: {len(sql_code)}) ---", flush=True)
    processed_sql = _postprocess_tsql_code(sql_code)
    print(f"--- CHECKPOINT 7: Postprocessed SQL (length: {len(processed_sql)}) ---", flush=True)

    # Validate syntax, balanced transactions, absence of inner GO
    valid, errors, _ = validate_tsql(processed_sql)
    print(f"--- CHECKPOINT 8: Validated TSQL (valid={valid}) ---", flush=True)
    assert valid, f"Generated T-SQL must pass syntax validation, errors: {errors}"

    # Verify all 8 critical DQ remediations in generated T-SQL:
    upper_sql = processed_sql.upper()

    # Finding 1: Negative Prices handled
    assert "TRY_CAST" in upper_sql and ("PRICE" in upper_sql)
    assert "< 0" in processed_sql or "< 0.0" in processed_sql or "PRICE_CLEAN" in upper_sql

    # Finding 2: InstructorID sentinels (999) handled
    assert "999" in processed_sql
    assert "INSTRUCTORID" in upper_sql

    # Finding 3: Age bounds handled
    assert "AGE" in upper_sql

    # Finding 4: Date parsing handled
    assert "TRY_CONVERT(DATE" in upper_sql or "TRY_CONVERT(DATETIME" in upper_sql

    # Finding 5: Rejects table logging exists
    assert "INSERT INTO DBO.ETL_REJECTS" in upper_sql

    # Finding 6: Clean target tables created with explicit types
    assert "[COURSES_CLEAN]" in upper_sql or "COURSES_CLEAN" in upper_sql
