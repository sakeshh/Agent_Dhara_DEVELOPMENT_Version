"""
Codegen integration tests per blob file format.
"""
from __future__ import annotations

import unittest
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source

class TestPysparkFormatCodegen(unittest.TestCase):
    def _make_plan(self, filename: str, fmt: str, options: dict | None = None):
        return {
            "plan_id": f"test_{fmt}",
            "datasets": {
                filename: {
                    "steps": [
                        {"order": 1, "column": "id", "action": "trim"}
                    ]
                }
            },
            "connector_manifest": {
                "datasets": {
                    filename: {
                        "location": f"Files/raw/{filename}",
                        "format": fmt,
                        "options": options or {}
                    }
                }
            },
            "business_rules": {}
        }

    def test_codegen_xml_format(self):
        plan = self._make_plan("orders.xml", "xml", {"row_tag": "order"})
        code = generate_pyspark_etl(plan, {})
        self.assertIn('com.databricks.spark.xml', code)
        self.assertIn('option("rowTag", "order")', code)
        self.assertNotIn('read.csv', code)
        ok, errs = validate_pyspark_source(code, plan)
        self.assertTrue(ok, errs)

    def test_codegen_xlsx_format(self):
        plan = self._make_plan("sales.xlsx", "xlsx", {"sheet_name": "Sheet1"})
        code = generate_pyspark_etl(plan, {})
        self.assertIn('com.crealytics.spark.excel', code)
        self.assertIn('option("sheetName", "Sheet1")', code)
        ok, errs = validate_pyspark_source(code, plan)
        self.assertTrue(ok, errs)

    def test_codegen_json_multiline_format(self):
        plan = self._make_plan("events.json", "json", {"multiline": True})
        code = generate_pyspark_etl(plan, {})
        # Spark requires camelCase "multiLine" (capital L) — lowercase "multiline" is silently ignored.
        self.assertIn('option("multiLine", "true")', code)
        ok, errs = validate_pyspark_source(code, plan)
        self.assertTrue(ok, errs)

    def test_codegen_unsupported_format_raises_in_pipeline(self):
        plan = self._make_plan("data.raw", "unsupported_xyz")
        with self.assertRaises(ValueError):
            generate_pyspark_etl(plan, {})

if __name__ == "__main__":
    unittest.main()
