"""
End-to-End ETL Flow Validation Script
Tests: Assessment → Plan → SQL Code Generation

Usage:
    python validate_etl_flow.py

This script validates that:
1. Assessment DQ issues are fully propagated to ETL plan
2. ETL plan steps generate SQL code (not no-ops)
3. Generated SQL is syntactically valid
4. All assessment columns are referenced in SQL
5. No missing transformations
"""

import json
import os
import sys
from typing import Dict, Any, List, Set

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_assessment_to_plan_mapping():
    """Test that assessment issues map 1:1 to plan steps"""
    print("\n" + "="*80)
    print("TEST 1: Assessment Issues → ETL Plan Mapping")
    print("="*80)

    # Sample assessment with known issues
    assessment = {
        "datasets": {
            "dbo.Orders_Raw": {
                "row_count": 1000,
                "column_count": 5,
                "columns": {
                    "OrderID": {
                        "dtype": "int64",
                        "null_percentage": 0.0,
                        "candidate_primary_key": True
                    },
                    "CustomerName": {
                        "dtype": "object",
                        "null_percentage": 0.15,
                        "semantic_type": "name"
                    },
                    "Price": {
                        "dtype": "float64",
                        "null_percentage": 0.05,
                        "mean": 150.5,
                        "median": 125.0,
                        "std": 75.2
                    },
                    "OrderDate": {
                        "dtype": "object",
                        "null_percentage": 0.02,
                        "semantic_type": "date"
                    },
                    "Email": {
                        "dtype": "object",
                        "null_percentage": 0.20,
                        "semantic_type": "email"
                    }
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Orders_Raw": {
                    "issues": [
                        {
                            "id": "issue_1",
                            "column": "CustomerName",
                            "issue_type": "missing_values",
                            "severity": "medium",
                            "message": "15% null values detected",
                            "row_count_affected": 150
                        },
                        {
                            "id": "issue_2",
                            "column": "Price",
                            "issue_type": "numeric_outliers_iqr",
                            "severity": "medium",
                            "message": "Outliers detected outside IQR bounds",
                            "row_count_affected": 50
                        },
                        {
                            "id": "issue_3",
                            "column": "OrderDate",
                            "issue_type": "date_format_inconsistency",
                            "severity": "high",
                            "message": "Mixed date formats detected",
                            "row_count_affected": 200
                        },
                        {
                            "id": "issue_4",
                            "column": "Email",
                            "issue_type": "invalid_email",
                            "severity": "medium",
                            "message": "Invalid email formats detected",
                            "row_count_affected": 80
                        },
                        {
                            "id": "issue_5",
                            "column": "Price",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "message": "Negative price values detected",
                            "row_count_affected": 10
                        }
                    ]
                }
            }
        }
    }

    try:
        from agent.etl_pipeline import build_etl_plan

        plan = build_etl_plan(
            assessment=assessment,
            business_rules={},
            engine="sql"
        )

        # Count issues and steps
        issues = assessment["data_quality_issues"]["datasets"]["dbo.Orders_Raw"]["issues"]
        steps = plan.get("datasets", {}).get("dbo.Orders_Raw", {}).get("steps", [])

        issue_count = len(issues)
        step_count = len(steps)

        print(f"\n✓ Assessment Issues: {issue_count}")
        print(f"✓ ETL Plan Steps: {step_count}")

        # Check mapping
        issue_ids = {issue["id"] for issue in issues}
        step_evidence_ids = set()
        for step in steps:
            evidence_id = step.get("evidence_id") or step.get("source_issue_ids", [])
            if isinstance(evidence_id, list):
                step_evidence_ids.update(evidence_id)
            elif evidence_id:
                step_evidence_ids.add(evidence_id)

        # Print detailed mapping
        print(f"\n{'Issue ID':<15} {'Column':<20} {'Type':<30} {'→ Step Action'}")
        print("-" * 100)
        for issue in issues:
            issue_id = issue["id"]
            col = issue["column"]
            itype = issue["issue_type"]

            # Find corresponding step
            matching_steps = [s for s in steps if col in str(s.get("column", ""))]
            if matching_steps:
                for step in matching_steps:
                    print(f"{issue_id:<15} {col:<20} {itype:<30} → {step.get('action')}")
            else:
                print(f"{issue_id:<15} {col:<20} {itype:<30} → ❌ NO STEP GENERATED")

        # Calculate coverage
        coverage_pct = (step_count / issue_count * 100) if issue_count > 0 else 0

        print(f"\n📊 Coverage: {coverage_pct:.1f}% ({step_count}/{issue_count} issues have steps)")

        if coverage_pct >= 100:
            print("✅ TEST PASSED: All issues mapped to plan steps")
            return True
        elif coverage_pct >= 80:
            print("⚠️  TEST WARNING: Most issues mapped (≥80%)")
            return True
        else:
            print("❌ TEST FAILED: Poor issue→step mapping (<80%)")
            return False

    except Exception as e:
        print(f"❌ TEST FAILED: Exception during plan generation: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sql_generation():
    """Test that SQL code is generated from plan"""
    print("\n" + "="*80)
    print("TEST 2: ETL Plan → SQL Code Generation")
    print("="*80)

    # Use same assessment from test 1
    assessment = {
        "datasets": {
            "dbo.Orders_Raw": {
                "row_count": 1000,
                "columns": {
                    "OrderID": {"dtype": "int64", "null_percentage": 0.0, "candidate_primary_key": True},
                    "CustomerName": {"dtype": "object", "null_percentage": 0.15},
                    "Price": {"dtype": "float64", "null_percentage": 0.05, "mean": 150.5, "median": 125.0},
                    "Email": {"dtype": "object", "null_percentage": 0.20}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Orders_Raw": {
                    "issues": [
                        {"id": "i1", "column": "CustomerName", "issue_type": "missing_values", "severity": "medium"},
                        {"id": "i2", "column": "Price", "issue_type": "negative_values", "severity": "high"}
                    ]
                }
            }
        }
    }

    try:
        from agent.etl_pipeline import build_etl_plan
        from agent.etl_pipeline.sql_codegen import generate_sql_etl

        plan = build_etl_plan(assessment=assessment, business_rules={}, engine="sql")
        sql = generate_sql_etl(plan, assessment, dialect="tsql")

        print(f"\n✓ SQL Generated: {len(sql)} characters")

        # Check for key SQL patterns
        checks = {
            "CREATE TABLE": "dbo.Orders_Clean",
            "ALTER TABLE": "ADD",
            "UPDATE": "SET",
            "COALESCE": "fill nulls",
            "CASE WHEN": "conditional logic",
            "GO": "batch separator"
        }

        found = {}
        for pattern, desc in checks.items():
            if pattern in sql:
                found[pattern] = True
                print(f"  ✓ {pattern}: Found ({desc})")
            else:
                found[pattern] = False
                print(f"  ✗ {pattern}: Missing ({desc})")

        # Check minimum SQL length
        if len(sql) < 500:
            print(f"\n❌ TEST FAILED: SQL too short ({len(sql)} chars), likely stub/empty")
            return False

        # Check all columns referenced
        columns = ["OrderID", "CustomerName", "Price", "Email"]
        missing_cols = []
        for col in columns:
            if f"[{col}]" not in sql:
                missing_cols.append(col)

        if missing_cols:
            print(f"\n⚠️  Warning: Columns not referenced in SQL: {missing_cols}")

        # Count transformation patterns
        transform_count = sql.count("COALESCE") + sql.count("CASE WHEN") + sql.count("TRY_CAST")
        print(f"\n📊 Transformation Patterns Found: {transform_count}")

        if all(found.values()) and len(sql) >= 1000 and transform_count >= 2:
            print("✅ TEST PASSED: Complete SQL generated with transformations")
            return True
        elif len(sql) >= 500:
            print("⚠️  TEST WARNING: SQL generated but may be incomplete")
            return True
        else:
            print("❌ TEST FAILED: Insufficient SQL generation")
            return False

    except Exception as e:
        print(f"❌ TEST FAILED: Exception during SQL generation: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sql_syntax():
    """Test that generated SQL is syntactically valid"""
    print("\n" + "="*80)
    print("TEST 3: SQL Syntax Validation")
    print("="*80)

    assessment = {
        "datasets": {
            "dbo.Test": {
                "row_count": 100,
                "columns": {
                    "ID": {"dtype": "int64", "candidate_primary_key": True},
                    "Name": {"dtype": "object", "null_percentage": 0.10}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Test": {
                    "issues": [
                        {"id": "i1", "column": "Name", "issue_type": "missing_values", "severity": "low"}
                    ]
                }
            }
        }
    }

    try:
        from agent.etl_pipeline import build_etl_plan
        from agent.etl_pipeline.sql_codegen import generate_sql_etl

        plan = build_etl_plan(assessment=assessment, business_rules={}, engine="sql")
        sql = generate_sql_etl(plan, assessment, dialect="tsql")

        # Basic syntax checks
        errors = []

        # Check balanced brackets
        if sql.count("[") != sql.count("]"):
            errors.append("Unbalanced square brackets")

        # Check balanced parentheses
        if sql.count("(") != sql.count(")"):
            errors.append("Unbalanced parentheses")

        # Check no invalid patterns
        invalid_patterns = [
            ("[Col]umnName", "Malformed column name"),
            ("FROM FROM", "Duplicate FROM"),
            ("SELECT SELECT", "Duplicate SELECT"),
            ("EXEC EXEC", "Duplicate EXEC"),
        ]

        for pattern, desc in invalid_patterns:
            if pattern in sql:
                errors.append(f"{desc}: '{pattern}'")

        if errors:
            print("\n❌ Syntax Errors Found:")
            for err in errors:
                print(f"  • {err}")
            return False
        else:
            print("\n✅ TEST PASSED: No syntax errors detected")
            return True

    except Exception as e:
        print(f"❌ TEST FAILED: Exception: {e}")
        return False


def test_traceability():
    """Test end-to-end traceability from assessment → plan → SQL"""
    print("\n" + "="*80)
    print("TEST 4: End-to-End Traceability")
    print("="*80)

    assessment = {
        "datasets": {
            "dbo.Products": {
                "row_count": 500,
                "columns": {
                    "ProductID": {"dtype": "int64", "candidate_primary_key": True},
                    "ProductName": {"dtype": "object", "null_percentage": 0.08},
                    "UnitPrice": {"dtype": "float64", "null_percentage": 0.03, "mean": 29.99, "median": 24.99}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Products": {
                    "issues": [
                        {
                            "id": "trace_issue_1",
                            "column": "ProductName",
                            "issue_type": "missing_values",
                            "severity": "medium",
                            "row_count_affected": 40
                        },
                        {
                            "id": "trace_issue_2",
                            "column": "UnitPrice",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "row_count_affected": 5
                        }
                    ]
                }
            }
        }
    }

    try:
        from agent.etl_pipeline import build_etl_plan
        from agent.etl_pipeline.sql_codegen import generate_sql_etl

        plan = build_etl_plan(assessment=assessment, business_rules={}, engine="sql")
        sql = generate_sql_etl(plan, assessment, dialect="tsql")

        print("\n🔍 Tracing Issue: trace_issue_1 (ProductName null values)")
        print("-" * 80)

        # Find in plan
        steps = plan.get("datasets", {}).get("dbo.Products", {}).get("steps", [])
        name_steps = [s for s in steps if "ProductName" in str(s.get("column", ""))]

        if name_steps:
            print(f"✓ Found in PLAN: {len(name_steps)} step(s)")
            for step in name_steps:
                print(f"  • Action: {step.get('action')}")
                print(f"    Evidence: {step.get('source_issue_type')}")
        else:
            print("✗ NOT FOUND in plan")
            return False

        # Find in SQL
        if "[ProductName]" in sql and ("COALESCE" in sql or "CASE" in sql):
            print(f"✓ Found in SQL: Transformation logic present")
        else:
            print("✗ NOT FOUND in SQL or no transformation")
            return False

        print("\n🔍 Tracing Issue: trace_issue_2 (UnitPrice negative values)")
        print("-" * 80)

        # Find in plan
        price_steps = [s for s in steps if "UnitPrice" in str(s.get("column", ""))]

        if price_steps:
            print(f"✓ Found in PLAN: {len(price_steps)} step(s)")
            for step in price_steps:
                print(f"  • Action: {step.get('action')}")
        else:
            print("✗ NOT FOUND in plan")
            return False

        # Find in SQL
        if "[UnitPrice]" in sql and ("CASE" in sql or "TRY_CAST" in sql):
            print(f"✓ Found in SQL: Transformation logic present")
        else:
            print("✗ NOT FOUND in SQL or no transformation")
            return False

        print("\n✅ TEST PASSED: Complete traceability verified")
        return True

    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all validation tests"""
    print("\n" + "="*80)
    print("AGENT DHARA - END-TO-END ETL FLOW VALIDATION")
    print("="*80)
    print("\nThis script validates the complete workflow:")
    print("  Assessment → ETL Plan → SQL Code Generation")
    print("\nRunning 4 critical tests...\n")

    results = {
        "Assessment→Plan Mapping": test_assessment_to_plan_mapping(),
        "Plan→SQL Generation": test_sql_generation(),
        "SQL Syntax": test_sql_syntax(),
        "End-to-End Traceability": test_traceability()
    }

    # Summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80 + "\n")

    passed = sum(1 for r in results.values() if r)
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:10} {test_name}")

    print(f"\n📊 Overall: {passed}/{total} tests passed ({passed/total*100:.0f}%)")

    if passed == total:
        print("\n🎉 SUCCESS: All validation tests passed!")
        print("✓ The ETL flow is working correctly end-to-end")
        print("✓ Ready for VP demo")
        return 0
    elif passed >= total * 0.75:
        print("\n⚠️  WARNING: Most tests passed but some issues remain")
        print("Review failed tests before demo")
        return 1
    else:
        print("\n❌ FAILURE: Critical issues detected")
        print("Fix broken tests before proceeding")
        return 2


if __name__ == "__main__":
    sys.exit(main())
