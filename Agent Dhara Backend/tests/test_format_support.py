"""
test_format_support.py - Unit tests for format capabilities registry, codegen snippets, options validation, and Fabric pre-flight package checks.
"""
from __future__ import annotations

import os
import unittest
from agent.etl_pipeline.format_readers import get_pyspark_read_snippet
from agent.etl_pipeline.format_validators import validate_dataset_format_entry, validate_plan_formats
from agent.etl_pipeline.io_snippets import pyspark_read_snippet
from agent.fabric_notebook_deployer import required_packages_for_plan, deploy_and_run_notebook

class TestFormatSupport(unittest.TestCase):
    def test_native_spark_readers_codegen(self):
        # CSV
        snippet_csv = get_pyspark_read_snippet("Files/raw/data.csv", "csv")
        self.assertIn("csv(Files/raw/data.csv)", snippet_csv)
        self.assertIn("header", snippet_csv)

        # TSV
        snippet_tsv = get_pyspark_read_snippet("Files/raw/data.tsv", "tsv")
        self.assertIn("csv(Files/raw/data.tsv)", snippet_tsv)
        self.assertIn("delimiter", snippet_tsv)
        self.assertIn("\\t", snippet_tsv)

        # JSON
        snippet_json = get_pyspark_read_snippet("Files/raw/data.json", "json")
        self.assertIn("json(Files/raw/data.json)", snippet_json)

        # JSON Multiline
        snippet_json_multi = get_pyspark_read_snippet("Files/raw/data.json", "json", {"multiline": True})
        self.assertIn('option("multiLine", "true")', snippet_json_multi)

        # Parquet
        snippet_parquet = get_pyspark_read_snippet("Files/raw/data.parquet", "parquet")
        self.assertIn("parquet(Files/raw/data.parquet)", snippet_parquet)

    def test_xml_spark_reader_codegen(self):
        snippet_xml = get_pyspark_read_snippet("Files/raw/data.xml", "xml", {"row_tag": "customTag"})
        self.assertIn('format("com.databricks.spark.xml")', snippet_xml)
        self.assertIn('option("rowTag", "customTag")', snippet_xml)

    def test_excel_spark_reader_codegen(self):
        snippet_xlsx = get_pyspark_read_snippet("Files/raw/data.xlsx", "xlsx", {"sheet_name": "Sheet1"})
        self.assertIn('format("com.crealytics.spark.excel")', snippet_xlsx)
        self.assertIn('option("sheetName", "Sheet1")', snippet_xlsx)

    def test_unsupported_format_raises_value_error(self):
        # Verify unknown format throws ValueError instead of silently falling back to CSV
        with self.assertRaises(ValueError):
            get_pyspark_read_snippet("Files/raw/data.unsupported", "unsupported")

        with self.assertRaises(ValueError):
            pyspark_read_snippet({
                "location": "Files/raw/data.unsupported",
                "format": "unsupported"
            })

    def test_pre_flight_package_requirements_calculation(self):
        plan = {
            "connector_manifest": {
                "datasets": {
                    "ds_xml": {"format": "xml"},
                    "ds_xlsx": {"format": "xlsx"},
                    "ds_csv": {"format": "csv"},
                }
            }
        }
        pkgs = required_packages_for_plan(plan)
        self.assertEqual(len(pkgs), 2)
        self.assertIn("com.databricks:spark-xml_2.12:0.18.0", pkgs)
        self.assertIn("com.crealytics:spark-excel_2.12:3.5.0_0.20.3", pkgs)

    def test_mock_pre_flight_deployment_preconditions(self):
        plan_xml = {
            "connector_manifest": {
                "datasets": {
                    "ds_xml": {"format": "xml"}
                }
            }
        }
        # XML package missing -> blocks deployment
        os.environ["FABRIC_STRICT_PACKAGE_CHECK"] = "1"
        os.environ["FABRIC_ATTACHED_PACKAGES"] = "some-other-pkg"
        try:
            res = deploy_and_run_notebook(
                session_id="test_xml_deploy",
                pyspark_code="# dummy PySpark code\ndef transform_ds_xml(): pass",
                plan=plan_xml
            )
            self.assertFalse(res["ok"])
            self.assertEqual(res["error"], "MISSING_SPARK_PACKAGES")
        finally:
            os.environ.pop("FABRIC_STRICT_PACKAGE_CHECK", None)
            os.environ.pop("FABRIC_ATTACHED_PACKAGES", None)

        # XML package attached -> allowed to deploy (subject to other setup configs)
        os.environ["FABRIC_WORKSPACE_ID"] = "00000000-0000-0000-0000-000000000000"
        os.environ["FABRIC_LAKEHOUSE_NAME"] = "00000000-0000-0000-0000-000000000000"
        os.environ["FABRIC_STRICT_PACKAGE_CHECK"] = "1"
        os.environ["FABRIC_ATTACHED_PACKAGES"] = "com.databricks:spark-xml_2.12:0.18.0"
        try:
            # We mock the FabricAPIClient calls so we only test pre-flight check routing
            from unittest.mock import patch
            with patch("agent.fabric_api_client.FabricAPIClient.create_or_update_notebook") as mock_create:
                mock_create.return_value = {"ok": True, "id": "nb_id"}
                with patch("agent.fabric_api_client.FabricAPIClient.trigger_notebook_run") as mock_run:
                    mock_run.return_value = "run_id"
                    res = deploy_and_run_notebook(
                        session_id="test_xml_deploy",
                        pyspark_code="def transform_ds_xml(): pass",
                        plan=plan_xml
                    )
                    self.assertTrue(res["ok"])
        finally:
            os.environ.pop("FABRIC_STRICT_PACKAGE_CHECK", None)
            os.environ.pop("FABRIC_ATTACHED_PACKAGES", None)
            os.environ.pop("FABRIC_WORKSPACE_ID", None)
            os.environ.pop("FABRIC_LAKEHOUSE_NAME", None)

if __name__ == "__main__":
    unittest.main()
