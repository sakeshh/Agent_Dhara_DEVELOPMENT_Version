import pandas as pd
import pytest
from agent.specialists.gx_validation_specialist import run_gx_validation
from agent.transformation_suggester import suggest_transformations
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item
from agent.etl_pipeline.manual_review_promote import count_pending_manual_review
from agent.etl_handlers import etl_confirm_plan, etl_plan_start
from agent.session_store import load_session, save_session

def test_new_dq_detection():
    # Construct a dataframe with at least 25 rows to satisfy length guards on checks
    df = pd.DataFrame({
        "mixed_scalars": [1, 2, "three", "four", 5] * 5, 
        "names": ["Smith, John", "John Smith", "Doe, Jane", "Jane Doe", "Smith, John"] * 5, 
        "phones": ["+919876543210", "9876543210", "+919876543210", "09876543210", "9876543210"] * 5, 
        "placeholders": ["test", "test", "test", "test", "valid_value"] * 5, 
        "latitudes": [45.0, 105.0, -120.0, 30.0, 40.0] * 5, 
        "impossible_dates": ["2023-02-29", "2023-02-31", "2023-01-15", "2023-04-31", "2023-05-15"] * 5, 
        "zeros_id": ["001", "002", "3", "4", "5"] * 5, 
        "ints_stored_as_floats": [1.0, 2.0, 3.0, 4.0, 5.0] * 5, 
        "ambiguous_bools": ["yes", "no", "true", "false", "yes"] * 5, 
        "constant_col": ["always_same"] * 25, 
        "empty_strings": ["", "", "non_empty", "non_empty", "non_empty"] * 5,
        "bad_numeric": ["100", "200", "abc", "300", "400"] * 5
    })

    profile_results = {
        "datasets": {
            "test_ds": {
                "columns": {
                    "mixed_scalars": {"semantic_type": "text"},
                    "names": {"semantic_type": "name"},
                    "phones": {"semantic_type": "phone"},
                    "placeholders": {"semantic_type": "text"},
                    "latitudes": {"semantic_type": "numeric"},
                    "impossible_dates": {"semantic_type": "date"},
                    "zeros_id": {"semantic_type": "id"},
                    "ints_stored_as_floats": {"semantic_type": "numeric"},
                    "ambiguous_bools": {"semantic_type": "categorical"},
                    "constant_col": {"semantic_type": "text"},
                    "empty_strings": {"semantic_type": "text"},
                    "bad_numeric": {"semantic_type": "numeric"}
                }
            }
        }
    }

    # Run validation
    res = run_gx_validation(
        datasets={"test_ds": df},
        profile_results=profile_results
    )
    
    assert "test_ds" in res
    results = res["test_ds"].get("results") or []
    
    expectations = [r["expectation"] for r in results]
    
    # Assert each is detected
    assert "mixed_scalar_types" in expectations
    assert "name_format_inconsistency" in expectations
    assert "mixed_phone_formats" in expectations
    assert "placeholder_detected" in expectations
    assert "custom_range" in expectations
    assert "invalid_date_format" in expectations
    assert "case_inconsistency" in expectations # For leading zeros
    assert "integer_stored_as_float" in expectations
    assert "ambiguous_boolean" in expectations
    assert "constant_column" in expectations
    assert "empty_string_values" in expectations
    assert "invalid_numeric_values" in expectations


def test_suggestion_mappings():
    assessment = {
        "datasets": {
            "test_ds": {
                "columns": {
                    "col1_id": {"semantic_type": "numeric_id"},
                    "col2": {"semantic_type": "name"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "test_ds": {
                    "issues": [
                        {"type": "mixed_scalar_types", "column": "col1_id", "severity": "medium", "count": 10},
                        {"type": "name_format_inconsistency", "column": "col2", "severity": "medium", "count": 5}
                    ]
                }
            }
        }
    }
    
    sugs = suggest_transformations(assessment)
    sug_list = sugs["suggested_transformations"]
    
    # We look up the action from the specific issue types in sug_list to avoid overwrite by proactive suggestions
    mixed_sug = next(s for s in sug_list if s["issue_type"] == "mixed_scalar_types")
    name_sug = next(s for s in sug_list if s["issue_type"] == "name_format_inconsistency")
    
    assert mixed_sug["suggested_action"] == "coerce_numeric"
    assert name_sug["suggested_action"] == "review_manually"


def test_planner_deduplication():
    # If same column has two different issue types that map to different actions, they should not overwrite each other.
    assessment = {
        "datasets": {
            "test_ds": {
                "columns": {
                    "col1": {"semantic_type": "text"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "test_ds": {
                    "issues": [
                        {"type": "whitespace", "column": "col1", "severity": "medium", "count": 10},
                        {"type": "all_caps_values", "column": "col1", "severity": "medium", "count": 5}
                    ]
                }
            }
        }
    }
    
    sugs = suggest_transformations(assessment)
    plan = build_etl_plan(assessment, {}, engine="python", dq_recommendations=sugs["suggested_transformations"])
    
    steps = plan["datasets"]["test_ds"]["steps"]
    actions = [s["action"] for s in steps]
    
    # We should have BOTH trim and lowercase in the plan!
    assert "trim" in actions
    assert "lowercase" in actions


def test_auto_resolve_safe_defaults():
    # Setup a mock session
    sid = "test_session_safe_defaults"
    sess = {
        "session_id": sid,
        "context": {
            "last_assessment_result": {
                "datasets": {
                    "test_ds": {
                        "columns": {"col1": {"dtype": "object"}}
                    }
                }
            },
            "etl_flow": {
                "plan": {
                    "plan_id": "mock_plan",
                    "datasets": {
                        "test_ds": {"steps": []}
                    },
                    "manual_review": [
                        {
                            "id": "test_ds|col1|mixed_scalar_types",
                            "dataset": "test_ds",
                            "column": "col1",
                            "issue_type": "mixed_scalar_types",
                            "severity": "medium",
                            "message": "Mixed scalar types",
                            "status": "pending"
                        }
                    ]
                },
                "business_rules": {
                    "auto_resolve_safe_defaults": True
                }
            }
        }
    }
    
    save_session(sess)
    
    # Confirming the plan should auto-resolve the pending item because auto_resolve_safe_defaults is True!
    res = etl_confirm_plan(session_id=sid)
    
    assert res["ok"] is True
    
    # Verify the step was promoted
    approved_plan = res.get("approved_plan") or {}
    assert count_pending_manual_review(approved_plan) == 0
    
    steps = approved_plan.get("datasets", {}).get("test_ds", {}).get("steps", [])
    assert len(steps) == 1
    assert steps[0]["action"] == "coerce_numeric"


def test_phase4_sql_upgrades():
    from agent.etl_pipeline.sql_codegen import generate_sql_etl
    
    plan = {
        "plan_id": "test_phase4_plan",
        "business_rules": {
            "never_drop_rows": False,
            "non_nullable": ["Phone", "Email"],
        },
        "datasets": {
            "dbo.Orders_Raw": {
                "steps": [
                    {"order": 1, "column": "Phone", "action": "normalize_phone"},
                    {"order": 2, "column": "Email", "action": "sanitize_email"},
                    {"order": 3, "column": "OrderDate", "action": "parse_dates"},
                    {"order": 4, "column": "ClumpedDate", "action": "nullify_dummy_dates"},
                    {"order": 5, "column": "PunctCol", "action": "nullify_punctuation"},
                    {"order": 6, "column": "row-level", "action": "deduplicate"}
                ]
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.Orders_Raw": {
                "columns": {
                    "OrderID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerID": {"dtype": "int"},
                    "Phone": {"dtype": "varchar"},
                    "Email": {"dtype": "varchar"},
                    "OrderDate": {"dtype": "varchar"},
                    "ClumpedDate": {"dtype": "varchar"},
                    "PunctCol": {"dtype": "varchar"}
                }
            }
        }
    }
    
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    
    # 1. Phone Normalization checks
    assert "LEN(" in sql and "10" in sql
    assert "NOT LIKE '%[^0-9]%'" in sql
    assert "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE" in sql
    
    # 2. Email Validation checks
    assert "LIKE '%@%@%'" in sql
    assert "LIKE '%.@%'" in sql
    assert "LIKE '%..%'" in sql
    
    # 3. Dummy Date Nullification checks
    assert "CASE WHEN" in sql and "1900-01-01" in sql
    assert "FORMAT(TRY_CAST(" in sql and "01-01" in sql
    
    # 4. Punctuation-Only Nullification checks
    assert "NOT LIKE '%[a-zA-Z0-9]%'" in sql
    
    # 5. Composite Key Deduplication checks
    # For row-level dedup, it should partition by columns other than OrderID
    assert "PARTITION BY" in sql
    partition_clause = sql.split("PARTITION BY")[1].split("ORDER BY")[0]
    assert "OrderID" not in partition_clause
    assert "CustomerID" in partition_clause

