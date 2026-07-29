"""ETL generation mode tests: cleanse_only vs full, and EXEC auto-call fix."""
from __future__ import annotations

import unittest
from tests.fixtures.blob_pair_assessment import make_blob_pair_assessment
from agent.etl_pipeline.classify_steps import classify_step_bucket
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.business_rules import normalize_business_rules


def _make_minimal_plan(generation_mode: str = "full") -> tuple:
    assess = make_blob_pair_assessment()
    rules = normalize_business_rules({})
    plan = build_etl_plan(assess, rules)
    if plan:
        plan["generation_mode"] = generation_mode
    return plan, assess, rules


class TestPhaseClassifier(unittest.TestCase):
    def test_cleanse_actions_map_to_cleanse(self):
        for action in ("trim", "lowercase", "fill_nulls_simple", "sanitize_email", "deduplicate", "flag_outliers"):
            result = classify_step_bucket(action)
            self.assertEqual(result["phase"], "cleanse", f"Expected cleanse for {action}, got {result}")

    def test_unknown_action_maps_to_transform(self):
        result = classify_step_bucket("join_aggregate_scd_view_magic")
        self.assertEqual(result["phase"], "transform")


class TestSqlCodegenExecCall(unittest.TestCase):
    """
    Verifies that the generated T-SQL script ends with an EXEC dbo.etl_main call.
    Without this the stored procs are defined but never run → Clean tables stay empty.
    """

    def _get_sql(self, mode: str) -> str:
        plan, assess, _ = _make_minimal_plan(generation_mode=mode)
        if not plan:
            self.skipTest("build_etl_plan returned None – no datasets")
        sql = generate_sql_etl(
            plan,
            assessment=assess,
            dialect="tsql",
        )
        return sql or ""

    def test_full_mode_contains_exec_etl_main(self):
        sql = self._get_sql("full")
        self.assertIn("EXEC dbo.etl_main", sql,
                      "Generated SQL must call EXEC dbo.etl_main so data flows to Clean tables")

    def test_cleanse_only_mode_contains_exec_etl_main(self):
        sql = self._get_sql("cleanse_only")
        self.assertIn("EXEC dbo.etl_main", sql,
                      "cleanse_only mode must also call EXEC dbo.etl_main")

    def test_full_mode_has_create_procedure(self):
        sql = self._get_sql("full")
        self.assertIn("CREATE PROCEDURE", sql)

    def test_cleanse_only_mode_no_views(self):
        sql = self._get_sql("cleanse_only")
        # cleanse_only should not create denormalized views
        self.assertNotIn("CREATE VIEW", sql.upper().replace("--", ""))

    def test_full_mode_has_clean_table_ddl(self):
        sql = self._get_sql("full")
        self.assertIn("_Clean", sql, "Script-level clean table DDL should be present")

    def test_exec_appears_after_go(self):
        """Verify EXEC dbo.etl_main is in its own GO-separated batch (not inside a proc definition)."""
        sql = self._get_sql("full")
        batches = [b.strip() for b in sql.split("\nGO\n") if b.strip()]
        exec_batches = [b for b in batches if "EXEC dbo.etl_main" in b and "CREATE PROCEDURE" not in b]
        self.assertTrue(
            len(exec_batches) >= 1,
            "EXEC dbo.etl_main must appear in a standalone GO batch, not inside a CREATE PROCEDURE block"
        )


if __name__ == "__main__":
    unittest.main()
