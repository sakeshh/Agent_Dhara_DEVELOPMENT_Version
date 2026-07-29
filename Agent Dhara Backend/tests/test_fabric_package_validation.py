"""
Tests for Fabric deployment package pre-flight checks and failure hints.
"""
from __future__ import annotations

import os
import unittest
from agent.fabric_notebook_deployer import required_packages_for_plan, deploy_and_run_notebook

class TestFabricPackageValidation(unittest.TestCase):
    def test_required_packages_for_plan(self):
        plan_csv = {
            "connector_manifest": {
                "datasets": {
                    "data.csv": {"format": "csv"}
                }
            }
        }
        self.assertEqual(required_packages_for_plan(plan_csv), [])

        plan_xml_xlsx = {
            "connector_manifest": {
                "datasets": {
                    "data.xml": {"format": "xml"},
                    "data.xlsx": {"format": "xlsx"},
                }
            }
        }
        pkgs = required_packages_for_plan(plan_xml_xlsx)
        self.assertIn("com.databricks:spark-xml_2.12:0.18.0", pkgs)
        self.assertIn("com.crealytics:spark-excel_2.12:3.5.0_0.20.3", pkgs)

    def test_deploy_blocks_when_package_missing_in_strict_mode(self):
        plan_xml = {
            "connector_manifest": {
                "datasets": {
                    "data.xml": {"format": "xml"}
                }
            }
        }
        os.environ["FABRIC_STRICT_PACKAGE_CHECK"] = "1"
        os.environ["FABRIC_ATTACHED_PACKAGES"] = ""
        try:
            res = deploy_and_run_notebook(
                session_id="test_sess",
                pyspark_code="# dummy code",
                plan=plan_xml
            )
            self.assertFalse(res["ok"])
            self.assertEqual(res["error"], "MISSING_SPARK_PACKAGES")
            self.assertIn("spark-xml", res["message"])
        finally:
            os.environ.pop("FABRIC_STRICT_PACKAGE_CHECK", None)
            os.environ.pop("FABRIC_ATTACHED_PACKAGES", None)

if __name__ == "__main__":
    unittest.main()
