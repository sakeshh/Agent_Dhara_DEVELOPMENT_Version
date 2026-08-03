import unittest
import pandas as pd
from agent.profiling.sql_dialect_adapter import get_remediation_sql
from agent.profiling.issue_contract import validate_issue, InvalidIssueContract, DQIssue
from agent.profiling.dq_checks import analyze_dataset_quality
from agent.service_layer import build_markdown_report, build_html_report

class TestReportGenerationV2(unittest.TestCase):

    def test_sql_dialect_adapter(self):
        # Test fabric_spark_sql (RLIKE syntax)
        sql_spark = get_remediation_sql("fabric_spark_sql", "customers", "age", "invalid_numeric")
        self.assertIn("ALTER TABLE staging.customers ADD age_raw VARCHAR(255);", sql_spark)
        self.assertIn("RLIKE '^[+-]?[0-9]+([.][0-9]+)?$'", sql_spark)
        self.assertIn("fabric_spark_sql", sql_spark)

        # Test fabric_warehouse_tsql (TRY_CAST syntax)
        sql_tsql = get_remediation_sql("fabric_warehouse_tsql", "customers", "age", "invalid_numeric")
        self.assertIn("TRY_CAST(age AS FLOAT) IS NULL", sql_tsql)

        # Test postgres (~ syntax)
        sql_postgres = get_remediation_sql("postgres", "customers", "age", "invalid_numeric")
        self.assertIn("age !~ '^[+-]?[0-9]+([.][0-9]+)?$'", sql_postgres)

    def test_issue_contract(self):
        # Empty issue dict should fail contract validation
        with self.assertRaises(InvalidIssueContract):
            validate_issue({})

        # Partially valid issue dict missing rule_source should fail
        with self.assertRaises(InvalidIssueContract):
            validate_issue({
                "issue_type": "nulls",
                "severity": "high",
                "gating": True,
                "fixability_details": {}
            })

        # Complete issue dict should succeed contract validation
        valid_issue = {
            "issue_type": "nulls",
            "severity": "high",
            "gating": True,
            "fixability_details": {
                "fixability": "HUMAN_REVIEW_REQUIRED"
            },
            "rule_source": "declared"
        }
        # Should not raise exception
        validate_issue(valid_issue)

    def test_root_cause_grouping(self):
        # Construct a simple dataframe and run DQ profiling to verify root cause grouping counts
        df = pd.DataFrame({
            "id": [1, 2, 2, 4],
            "name": ["Alice", "Bob", None, "Dave"]
        })
        profile = {
            "row_count": 4,
            "column_count": 2,
            "columns": {
                "id": {"null_count": 0},
                "name": {"null_count": 1}
            }
        }
        res = analyze_dataset_quality(
            name="test_dataset",
            df=df,
            profile=profile,
            thresholds={
                "relationships": {
                    "declared": [],
                    "readiness": {"ready_threshold": 70, "blocked_threshold": 35}
                }
            }
        )
        self.assertIn("root_cause_summary", res["summary"])
        rc_sum = res["summary"]["root_cause_summary"]
        self.assertTrue(rc_sum["family_counts_are_non_additive"])
        self.assertGreaterEqual(rc_sum["rule_hit_count"], 1)

    def test_report_restructuring(self):
        # Mock a complete assessment result structure to test Markdown & HTML generation
        mock_result = {
            "run_id": "test-run-123",
            "datasets": {
                "customers": {
                    "row_count": 100,
                    "column_count": 3,
                    "data_volume_bytes": 5000,
                    "columns": {
                        "id": {"null_percentage": 0.0, "unique_count": 100},
                        "email": {"null_percentage": 0.05, "unique_count": 95}
                    }
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers": {
                        "issues": [
                            {
                                "type": "nulls",
                                "column": "email",
                                "severity": "medium",
                                "message": "Nulls in email",
                                "row_indexes": [5, 10],
                                "gating": True,
                                "fixability": "HUMAN_REVIEW_REQUIRED"
                            },
                            {
                                "type": "whitespace",
                                "column": "email",
                                "severity": "low",
                                "message": "Whitespace in email",
                                "row_indexes": [12],
                                "gating": False,
                                "fixability": "AUTO_FIXABLE"
                            }
                        ],
                        "summary": {
                            "dq_score_0_100": 85.0,
                            "estimated_clean_rows_after_high": 100,
                            "estimated_clean_rows_after_high_and_medium": 98,
                            "dimension_breakdown": {
                                "completeness": {"score": 95.0, "passed_cells": 298, "evaluated_cells": 300}
                            },
                            "root_cause_summary": {
                                "rule_hit_count": 2,
                                "distinct_issue_types": 2,
                                "distinct_rows_affected": 3,
                                "distinct_cells_affected": 3,
                                "root_cause_family_count": 2,
                                "family_counts_are_non_additive": True,
                                "root_cause_groups": [
                                    {
                                        "family": "missing_or_null",
                                        "issue_types_triggered": ["nulls"],
                                        "distinct_cells_affected": 2,
                                        "distinct_rows_affected": 2
                                    },
                                    {
                                        "family": "casing_style",
                                        "issue_types_triggered": ["whitespace"],
                                        "distinct_cells_affected": 1,
                                        "distinct_rows_affected": 1
                                    }
                                ]
                            }
                        }
                    }
                },
                "global_issues": {}
            },
            "governance": {
                "agent_version": "3.4.0",
                "manifest_schema_version": "2.0.0",
                "schema_hash": "abc123hash",
                "rules_hash": "xyz789hash"
            }
        }

        # Render reports
        md_report = build_markdown_report(mock_result)
        html_report = build_html_report(mock_result)

        # Assert Markdown report features
        self.assertIn("## DQ Score Methodology", md_report)
        self.assertIn("## Technical Appendix", md_report)
        self.assertIn("### Reproducibility Metadata", md_report)
        self.assertIn("### Assessment Limitations", md_report)
        self.assertIn("#### Root-Cause Groups (Non-Additive)", md_report)

        # Assert HTML report features
        self.assertIn('id="score-methodology"', html_report)
        self.assertIn('id="technical-appendix"', html_report)
        self.assertIn('class="low-severity-details"', html_report)
        self.assertIn('class="root-cause-summary-wrap"', html_report)
