import unittest
import re
import pandas as pd
from typing import Any, Dict

from agent.intelligent_data_assessment import detect_semantic_type
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item


class TestSemanticEtlUpgrades(unittest.TestCase):
    def test_date_hint_word_boundaries(self) -> None:
        # Regex used for date hints: r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)'
        date_hint_pattern = r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)'
        
        # Valid date hints
        self.assertTrue(bool(re.search(date_hint_pattern, "created_at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "updated_at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "order_date")))
        self.assertTrue(bool(re.search(date_hint_pattern, "dob")))
        self.assertTrue(bool(re.search(date_hint_pattern, "birth_date")))
        self.assertTrue(bool(re.search(date_hint_pattern, "updated at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "dt_created")))

        # Non-date columns containing "at" or similar substrings
        self.assertFalse(bool(re.search(date_hint_pattern, "attendance")))
        self.assertFalse(bool(re.search(date_hint_pattern, "category")))
        self.assertFalse(bool(re.search(date_hint_pattern, "rate")))
        self.assertFalse(bool(re.search(date_hint_pattern, "latitude")))
        self.assertFalse(bool(re.search(date_hint_pattern, "status")))

    def test_auto_resolve_pending_manual_review(self) -> None:
        # Construct assessment result
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "customer_email": {"dtype": "object"},
                    }
                }
            }
        }
        
        # Enable auto_resolve_pending in business rules
        business_rules = {
            "never_drop_rows": False,
            "auto_resolve_pending": True,
        }
        
        # Force a suggestion that triggers manual review
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw=business_rules,
            source_context={
                "suggestions": [
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "whitespace",
                        "severity": "high",
                        "message": "Whitespace detected",
                        "auto_fixable": False,
                    }
                ]
            }
        )
        
        # Since auto_resolve_pending is True, it should have resolved the manual review
        # with the default/recommended action (which is trim)
        self.assertEqual(len(plan["manual_review"]), 0)
        self.assertEqual(len(plan["resolved_manual_review"]), 1)
        resolved = plan["resolved_manual_review"][0]
        self.assertEqual(resolved["status"], "resolved")
        self.assertEqual(resolved["selected_resolution"], "trim")
        
        # And the trim step should be added to steps
        steps = plan["datasets"]["dbo.Orders_Raw"]["steps"]
        self.assertTrue(any(s["action"] == "trim" for s in steps))

    def test_sql_codegen_quarantine_rejects(self) -> None:
        # Construct a plan with parse_dates and sanitize_email steps
        plan = {
            "plan_id": "test_quarantine_plan",
            "business_rules": {
                "never_drop_rows": False,
                "non_nullable": ["Email"],
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "OrderDate",
                            "action": "parse_dates",
                        },
                        {
                            "order": 2,
                            "column": "Email",
                            "action": "sanitize_email",
                        }
                    ]
                }
            }
        }
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "OrderDate": {"dtype": "varchar"},
                        "Email": {"dtype": "varchar"},
                    }
                }
            }
        }
        
        # Dialect: tsql, never_drop: False
        sql_with_drop = generate_sql_etl(plan, assessment, dialect="tsql")
        
        self.assertIn("dbo.etl_rejects", sql_with_drop)
        self.assertIn("Log unparseable dates", sql_with_drop)
        self.assertIn("Quarantine invalid emails", sql_with_drop)
        self.assertIn("DELETE FROM [dbo].[Orders_Clean]", sql_with_drop)
        self.assertIn("FOR JSON PATH", sql_with_drop)
        
        # Dialect: tsql, never_drop: True
        plan_never_drop = {
            "plan_id": "test_quarantine_plan_nd",
            "business_rules": {
                "never_drop_rows": True,
                "non_nullable": ["Email"],
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "OrderDate",
                            "action": "parse_dates",
                        },
                        {
                            "order": 2,
                            "column": "Email",
                            "action": "sanitize_email",
                        }
                    ]
                }
            }
        }
        sql_no_drop = generate_sql_etl(plan_never_drop, assessment, dialect="tsql")
        self.assertNotIn("Quarantine invalid dates", sql_no_drop)
        self.assertNotIn("Quarantine invalid emails", sql_no_drop)
        self.assertNotIn("DELETE FROM dbo.Orders_Clean", sql_no_drop)

    def test_sql_codegen_exclude_column_action(self) -> None:
        plan = {
            "plan_id": "test_exclude_column_plan",
            "business_rules": {
                "never_drop_rows": False,
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "CollidingCol",
                            "action": "exclude_column",
                        }
                    ]
                }
            }
        }
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "CollidingCol": {"dtype": "varchar"},
                        "GoodCol": {"dtype": "varchar"},
                    }
                }
            }
        }
        
        sql = generate_sql_etl(plan, assessment, dialect="tsql")
        # CollidingCol should be skipped in insert and comment should mark it excluded
        self.assertIn("GoodCol", sql)
        self.assertNotIn("[CollidingCol]", sql)
        self.assertIn("skipped via exclude_column transform step", sql)


    def test_unified_manual_review_risk_tiers(self) -> None:
        from agent.etl_pipeline.planner import build_etl_plan
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "customer_email": {"dtype": "varchar"},
                    }
                }
            }
        }
        business_rules = {
            "auto_resolve_pending": False,
        }
        # 1. Non-fixable type: orphan_foreign_keys
        # 2. Complex type: business_key_duplicate
        # 3. Standard type: whitespace
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw=business_rules,
            source_context={
                "suggestions": [
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "orphan_foreign_keys",
                        "severity": "high",
                        "message": "Orphan foreign keys",
                        "auto_fixable": False,
                    },
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "OrderID",
                        "suggested_action": "review_manually",
                        "issue_type": "business_key_duplicate",
                        "severity": "medium",
                        "message": "Duplicate business key",
                        "auto_fixable": False,
                    },
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "whitespace",
                        "severity": "low",
                        "message": "Whitespace detected",
                        "auto_fixable": False,
                    }
                ]
            }
        )
        self.assertNotIn("non_fixable", plan)
        self.assertEqual(len(plan["manual_review"]), 3)
        
        tiers = {m["issue_type"]: m["risk_tier"] for m in plan["manual_review"]}
        self.assertEqual(tiers["orphan_foreign_keys"], "non_fixable")
        self.assertEqual(tiers["business_key_duplicate"], "complex")
        self.assertEqual(tiers["whitespace"], "standard")

        # Verify coverage by tier
        cov = plan["coverage"]
        self.assertEqual(cov["manual_review_by_tier"]["non_fixable"], 1)
        self.assertEqual(cov["manual_review_by_tier"]["complex"], 1)
        self.assertEqual(cov["manual_review_by_tier"]["standard"], 1)

    def test_codegen_gate_unacknowledged_blockers(self) -> None:
        from agent.session_store import load_session, save_session
        from agent.etl_handlers import etl_generate_code, etl_confirm_plan
        from agent.etl_pipeline.planner import build_etl_plan
        
        # Setup session with plan containing blocker (pending complex risk tier)
        sid = "test_gate_session"
        sess = load_session(sid)
        ctx = sess.setdefault("context", {})
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "customer_email": {"dtype": "varchar"},
                    }
                }
            }
        }
        ctx["last_assessment_result"] = assessment
        
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw={"auto_resolve_pending": False},
            source_context={
                "suggestions": [
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "business_key_duplicate",
                        "severity": "high",
                        "message": "Blocker business key dup",
                        "auto_fixable": False,
                    }
                ]
            }
        )
        
        flow = {
            "phase": "approved",
            "plan": plan,
            "approved_plan": plan,
            "assessment_schema_signature": "sig_dummy"
        }
        ctx["etl_flow"] = flow
        # Set matching signatures so signature mismatch is bypassed
        from agent.etl_handlers import _assessment_schema_signature
        flow["assessment_schema_signature"] = _assessment_schema_signature(assessment)
        save_session(sess)
        
        # Verify etl_generate_code returns blocked
        res = etl_generate_code(sid, engine="python")
        self.assertEqual(res.get("status"), "blocked")
        self.assertEqual(res.get("reason"), "unacknowledged_complex_or_non_fixable_issues")
        self.assertEqual(len(res.get("blockers", [])), 1)
        self.assertEqual(res["blockers"][0]["issue_type"], "business_key_duplicate")

    def test_enrich_review_options_route(self) -> None:
        from agent.session_store import load_session, save_session
        from agent.api_routes import api_etl_enrich_review_options, EtlEnrichReviewOptionsPayload
        
        sid = "test_enrich_session"
        sess = load_session(sid)
        ctx = sess.setdefault("context", {})
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "customer_email": {"dtype": "varchar"},
                    }
                }
            }
        }
        ctx["last_assessment_result"] = assessment
        
        # Start with an unknown issue type having standard fallback options (allow_llm_call=False)
        from agent.etl_pipeline.planner import build_etl_plan
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw={"auto_resolve_pending": False},
            source_context={
                "suggestions": [
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "unknown_random_anomalous_issue",
                        "severity": "medium",
                        "message": "some msg",
                        "auto_fixable": False,
                    }
                ]
            }
        )
        
        flow = {"plan": plan}
        ctx["etl_flow"] = flow
        save_session(sess)
        
        # Trigger enrichment POST route
        payload = EtlEnrichReviewOptionsPayload(
            session_id=sid,
            issue_type="unknown_random_anomalous_issue",
            item={"dataset": "dbo.Orders_Raw", "column": "customer_email"}
        )
        res = api_etl_enrich_review_options(payload)
        self.assertTrue(res.get("ok"))
        self.assertTrue(len(res.get("options", [])) > 0)
        
        # Verify the session plan's manual review options got updated
        sess_updated = load_session(sid)
        plan_updated = sess_updated["context"]["etl_flow"]["plan"]
        mr_item = plan_updated["manual_review"][0]
        self.assertTrue("resolution_options" in mr_item)


if __name__ == "__main__":
    unittest.main()
