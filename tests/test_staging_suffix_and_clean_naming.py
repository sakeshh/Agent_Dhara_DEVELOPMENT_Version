import unittest
import re
from agent.etl_pipeline.sql_codegen import _get_clean_table_name, _get_transformed_table_name, generate_sql_etl
from agent.etl_pipeline.execution_orchestrator import ensure_idempotent_sql
from agent.etl_pipeline.connector_manifest import build_connector_manifest

class TestStagingSuffixAndCleanNaming(unittest.TestCase):
    def test_clean_table_name_from_staging(self):
        # Table ending with _Staging should become _Clean without duplicate _Staging_Clean
        res = _get_clean_table_name("dbo.EmployeeMaster_Staging")
        self.assertEqual(res, "dbo.EmployeeMaster_Clean")

        res_trans = _get_transformed_table_name("dbo.EmployeeMaster_Staging")
        self.assertEqual(res_trans, "dbo.EmployeeMaster_Transformed")

    def test_clean_table_name_from_raw(self):
        res = _get_clean_table_name("dbo.EmployeeMaster_Raw")
        self.assertEqual(res, "dbo.EmployeeMaster_Clean")

    def test_ensure_idempotent_sql_cleans_double_staging(self):
        bad_sql = "UPDATE #EmployeeMaster_Staging_Staging SET [Name_clean] = 'John';"
        fixed_sql = ensure_idempotent_sql(bad_sql)
        self.assertNotIn("#EmployeeMaster_Staging_Staging", fixed_sql)
        self.assertIn("#EmployeeMaster_Staging", fixed_sql)

    def test_sql_codegen_staging_name_no_double_suffix(self):
        plan = {
            "plan_id": "test_plan_staging",
            "generation_mode": "full",
            "datasets": {
                "dbo.EmployeeMaster_Staging": {
                    "steps": [
                        {"action": "trim", "column": "Name"}
                    ]
                }
            }
        }
        assessment = {
            "datasets": {
                "dbo.EmployeeMaster_Staging": {
                    "columns": {
                        "EmpID": {"dtype": "int"},
                        "Name": {"dtype": "varchar"}
                    }
                }
            }
        }
        sql = generate_sql_etl(plan, assessment=assessment, dialect="tsql")
        self.assertNotIn("#EmployeeMaster_Staging_Staging", sql)
        self.assertIn("#EmployeeMaster_Staging", sql)
        self.assertIn("dbo.EmployeeMaster_Clean", sql)

    def test_connector_manifest_uses_clean_time_naming(self):
        ctx = {
            "selected_source": "azure_blob",
            "selected_blob_files": ["EmployeeMaster.csv"]
        }
        assess = {
            "datasets": {
                "EmployeeMaster.csv": {"row_count": 100}
            }
        }
        manifest = build_connector_manifest(ctx, assess)
        out_path = manifest["datasets"]["EmployeeMaster.csv"]["output_path"]
        self.assertTrue(re.match(r"^cleaned/EmployeeMaster_clean_\d{8}_\d{6}\.csv$", out_path.replace("\\", "/")), f"Path did not match timestamp pattern: {out_path}")

if __name__ == "__main__":
    unittest.main()
