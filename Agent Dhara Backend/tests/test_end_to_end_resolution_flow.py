"""
End-to-end regression tests verifying policy resolutions promotion, composition/canonicalization,
readiness certificate scope, and engine recommendation logic.
"""
from __future__ import annotations

import unittest
from typing import Any, Dict

from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
from agent.etl_pipeline.readiness_engine import build_projected_column_certificates, evaluate_all_operation_gates
from agent.etl_pipeline.validate_plan import validate_etl_plan
from agent.etl_pipeline.source_context import build_source_context


class TestEndToEndResolutionFlow(unittest.TestCase):
    def setUp(self) -> None:
        # 1. Setup multi-dataset assessment containing joins, required columns, and anomalies
        self.assessment = {
            "datasets": {
                "marketing_relational_test.csv": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {
                            "dtype": "string",
                            "null_count": 10, # nulls on parent key!
                            "distinct_values": 500,
                        },
                        "customer_name": {
                            "dtype": "string",
                            "null_count": 50,
                            "distinct_values": 450,
                        }
                    }
                },
                "sales_relational_test.xml": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {
                            "dtype": "string",
                            "null_count": 100,  # nulls on key!
                            "distinct_values": 400,
                        },
                        "product_id": {
                            "dtype": "string",
                            "null_count": 0,
                            "distinct_values": 300,
                        }
                    }
                }
            },
            "relationships": [
                {
                    "relationship_id": "rel_marketing_sales",
                    "dataset_a": "marketing_relational_test.csv",
                    "column_a": "campaign_id",
                    "dataset_b": "sales_relational_test.xml",
                    "column_b": "campaign_id",
                    "cardinality": "one_to_many",
                    "max_rows_per_key_a": 1,
                    "max_rows_per_key_b": 9999,
                    "required": True
                }
            ],
            "dq_recommendations": [],
            "data_quality_issues": {
                "datasets": {
                    "marketing_relational_test.csv": {
                        "issues": [
                            {
                                "column": "campaign_id",
                                "type": "nulls",
                                "severity": "high",
                                "message": "Nulls in campaign_id"
                            },
                            {
                                "column": "customer_name",
                                "type": "nulls",
                                "severity": "medium",
                                "message": "Nulls in customer_name"
                            },
                            {
                                "column": "customer_name",
                                "type": "duplicate_insensitive_values",
                                "severity": "medium",
                                "message": "Duplicate casing in customer_name"
                            }
                        ]
                    },
                    "sales_relational_test.xml": {
                        "issues": [
                            {
                                "column": "campaign_id",
                                "type": "nulls",
                                "severity": "medium",
                                "message": "Nulls in campaign_id"
                            }
                        ]
                    }
                }
            }
        }

        self.dq_recommendations = {
            "recommendations": [
                {
                    "dataset": "marketing_relational_test.csv",
                    "column": "campaign_id",
                    "issue_type": "nulls",
                    "severity": "high",
                    "message": "Null values found in campaign_id key column",
                    "suggested_fix": "Flag missing campaign_id parent keys",
                    "mapped_action": "fill_nulls_simple",
                },
                {
                    "dataset": "sales_relational_test.xml",
                    "column": "campaign_id",
                    "issue_type": "nulls",
                    "severity": "medium",
                    "message": "Null values found in campaign_id child column",
                    "suggested_fix": "Flag missing campaign_id child keys",
                    "mapped_action": "fill_nulls_simple",
                },
                {
                    "dataset": "marketing_relational_test.csv",
                    "column": "customer_name",
                    "issue_type": "nulls",
                    "severity": "medium",
                    "message": "Null values found in customer_name required column",
                    "suggested_fix": "Fill customer_name nulls with constant",
                    "mapped_action": "fill_nulls_simple",
                },
                {
                    "dataset": "marketing_relational_test.csv",
                    "column": "customer_name",
                    "issue_type": "duplicate_insensitive_values",
                    "severity": "medium",
                    "message": "Inconsistent duplicate values in customer_name due to casing",
                    "suggested_fix": "Lowercase customer name to standardize duplicates",
                    "mapped_action": "lowercase",
                }
            ]
        }

        self.business_rules = {
            "required_columns": ["customer_name"],
            "never_drop_rows": True,
            "approved_waivers": [
                {
                    "waiver_id": "waiver_norm_mismatch",
                    "scope": {
                        "operation_id": "rel_marketing_relational_test.csv_sales_relational_test.xml",
                    },
                    "effective_at": "2020-01-01",
                    "expires_at": "2030-01-01",
                    "affected_requirement": "NORMALIZATION_MISMATCH_ALLOWED",
                    "reason": "Allow normalization mismatch in test"
                }
            ]
        }

    def test_end_to_end_resolution_workflow(self) -> None:
        # 1. Build source context with Fabric indicators
        src_ctx = build_source_context(
            session_context={"workspace_id": "fab-123", "lakehouse_id": "lh-456"},
            assessment=self.assessment
        )

        self.assertEqual(src_ctx["target_platform"], "fabric_lakehouse")
        self.assertEqual(src_ctx["target_format"], "delta")
        self.assertEqual(src_ctx["dataset_count"], 2)
        self.assertEqual(src_ctx["relationship_count"], 1)

        # 2. Build initial ETL plan
        plan = build_etl_plan(
            self.assessment,
            self.business_rules,
            source_context=src_ctx,
            dq_recommendations=self.dq_recommendations
        )

        # Recommendation should be PySpark for Fabric
        self.assertEqual(plan["engine_recommendation"]["engine"], "pyspark")
        # Column Certificates should exist for:
        # - joins: marketing_relational_test.csv:campaign_id, sales_relational_test.xml:campaign_id
        # - required columns: marketing_relational_test.csv:customer_name
        certs = plan.get("projected_column_readiness") or []

        cert_columns = {(c["dataset_id"], c["source_column"]) for c in certs}
        
        self.assertIn(("marketing_relational_test.csv", "campaign_id"), cert_columns)
        self.assertIn(("sales_relational_test.xml", "campaign_id"), cert_columns)
        self.assertIn(("marketing_relational_test.csv", "customer_name"), cert_columns)
        # Should NOT contain other random columns
        self.assertNotIn(("sales_relational_test.xml", "product_id"), cert_columns)

        # Since we have unhandled nulls on parent key, it is BLOCKED
        blocked_certs = {c["source_column"] for c in certs if c["readiness_status"].startswith("BLOCKED")}
        self.assertIn("campaign_id", blocked_certs)

        # 3. Simulate resolving manual cards
        # Dynamically build resolutions list from generated manual review items
        resolutions_list = []
        for item in plan.get("manual_review") or []:
            iid = item["id"]
            col = item["column"]
            it = item["issue_type"]
            ds = item["dataset"]
            
            # Select target option based on dataset and column
            if ds == "marketing_relational_test.csv" and col == "campaign_id" and it == "nulls":
                opt = next(o for o in item["resolution_options"] if o["action"] == "flag_missing_key")
                resolutions_list.append({"item_id": iid, "resolution_id": opt["id"]})
            elif ds == "sales_relational_test.xml" and col == "campaign_id" and it == "nulls":
                opt = next(o for o in item["resolution_options"] if o["action"] == "flag_missing_key")
                resolutions_list.append({"item_id": iid, "resolution_id": opt["id"]})
            elif ds == "marketing_relational_test.csv" and col == "customer_name" and it == "nulls":
                opt = next(o for o in item["resolution_options"] if o["action"] == "fill_constant")
                resolutions_list.append({"item_id": iid, "resolution_id": opt["id"]})
            elif ds == "marketing_relational_test.csv" and col == "customer_name" and it == "duplicate_insensitive_values":
                opt = next(o for o in item["resolution_options"] if o["action"] == "lowercase")
                resolutions_list.append({"item_id": iid, "resolution_id": opt["id"]})
            elif ds == "marketing_relational_test.csv" and col == "customer_name" and it == "proactive_trim":
                opt = next(o for o in item["resolution_options"] if o["id"] == "llm_suggested")
                resolutions_list.append({"item_id": iid, "resolution_id": opt["id"]})

        # Apply resolutions and promote to steps
        updated_plan, errs = apply_manual_resolutions(plan, resolutions_list, business_rules=self.business_rules)

        self.assertEqual(len(errs), 0)

        # Verify steps promoted as APPROVED_MANUAL execution mode
        promoted_steps = []
        for block in updated_plan["datasets"].values():
            for step in block.get("steps") or []:
                if step.get("execution_mode") == "APPROVED_MANUAL":
                    promoted_steps.append(step)

        self.assertGreater(len(promoted_steps), 0)
        for st in promoted_steps:
            self.assertFalse(st.get("evidence", {}).get("confidence_advisory", True))

        # Run Plan Canonicalization checks
        # customer_name had both fill_constant and lowercase + trim. They must be composed in logical order.
        m_steps = updated_plan["datasets"]["marketing_relational_test.csv"]["steps"]
        actions = [s["action"] for s in m_steps if s.get("column") == "customer_name"]
        
        # Verify no duplicate 'trim' exists, actions are sorted logically: trim -> lowercase -> fill
        self.assertIn("lowercase", actions)

        # 4. Plan Validation check must pass cleanly
        ok, val_errs = validate_etl_plan(updated_plan, self.assessment, self.business_rules)
        self.assertTrue(ok, f"Validation failed: {val_errs}")

        # 5. Verify readiness engine certificates and gates are updated post-resolutions
        updated_certs = build_projected_column_certificates(updated_plan, self.assessment)
        joins_list = updated_plan.get("relationships", {}).get("joins") or []
        updated_gates = evaluate_all_operation_gates(updated_certs, joins_list, self.business_rules.get("approved_waivers") or [])

        # Post-resolution: Projected null count for campaign_id should be 0 (handled)
        campaign_cert = next(c for c in updated_certs if c["source_column"] == "campaign_id" and c["dataset_id"] == "marketing_relational_test.csv")
        self.assertEqual(campaign_cert["metrics"]["null_count"], 0)
        self.assertEqual(campaign_cert["readiness_status"], "READY")

        # Relationship gate should now be ready (with warnings from the authorized waiver)
        campaign_gate = next(g for g in updated_gates if g["operation_id"] == "rel_marketing_relational_test.csv_sales_relational_test.xml")
        self.assertEqual(campaign_gate["gate_status"], "READY_WITH_WARNINGS")
