"""
Tests for format_readers.py and format_capabilities.py
"""
from __future__ import annotations

import unittest
from agent.etl_pipeline.format_readers import get_pyspark_read_snippet, DISPATCH
from agent.etl_pipeline.format_capabilities import (
    PYSPARK_FORMAT_CAPABILITIES,
    get_capability,
    required_package_for_format,
    get_fabric_hint,
)
from agent.etl_pipeline.format_validators import validate_dataset_format_entry

class TestPysparkFormatReaders(unittest.TestCase):
    def test_csv_reader(self):
        snippet = get_pyspark_read_snippet('_resolve_data_path("data.csv")', "csv")
        self.assertIn("spark.read.option('header', 'true')", snippet)
        self.assertIn(".csv(_resolve_data_path(\"data.csv\"))", snippet)

    def test_tsv_reader(self):
        snippet = get_pyspark_read_snippet('_resolve_data_path("data.tsv")', "tsv")
        self.assertIn("delimiter", snippet)
        self.assertIn("\\t", snippet)

    def test_json_reader_default_and_multiline(self):
        snippet_default = get_pyspark_read_snippet('_resolve_data_path("data.json")', "json")
        self.assertIn('option("multiLine", "true")', snippet_default)
        
        snippet_multi = get_pyspark_read_snippet('_resolve_data_path("data.json")', "json", options={"multiline": True})
        self.assertIn('option("multiLine", "true")', snippet_multi)

    def test_parquet_reader(self):
        snippet = get_pyspark_read_snippet('_resolve_data_path("data.parquet")', "parquet")
        self.assertEqual(snippet, 'spark.read.parquet(_resolve_data_path("data.parquet"))')

    def test_xml_reader_with_options(self):
        snippet = get_pyspark_read_snippet('_resolve_data_path("data.xml")', "xml", options={"row_tag": "record"})
        self.assertIn('com.databricks.spark.xml', snippet)
        self.assertIn('option("rowTag", "record")', snippet)

    def test_excel_reader_with_options(self):
        snippet = get_pyspark_read_snippet('_resolve_data_path("data.xlsx")', "xlsx", options={"sheet_name": "Orders"})
        self.assertIn('com.crealytics.spark.excel', snippet)
        self.assertIn('option("sheetName", "Orders")', snippet)

    def test_unsupported_format_raises(self):
        with self.assertRaises(ValueError) as ctx:
            get_pyspark_read_snippet('_resolve_data_path("data.unknown")', "unknown_fmt")
        self.assertIn("Unsupported PySpark blob format", str(ctx.exception))

    def test_format_capability_registry(self):
        self.assertTrue(get_capability("csv")["fabric_ready"])
        self.assertFalse(get_capability("xml")["fabric_ready"])
        self.assertEqual(required_package_for_format("xml"), "com.databricks:spark-xml_2.12:0.18.0")
        self.assertIn("spark-excel", get_fabric_hint("xlsx"))

    def test_validator_entry(self):
        with self.assertRaises(ValueError):
            validate_dataset_format_entry({"location": "foo.bar", "format": "unsupported_fmt"})
            
        entry_excel = {"location": "foo.xlsx", "format": "xlsx"}
        validate_dataset_format_entry(entry_excel)
        self.assertEqual(entry_excel["options"]["sheet_name"], 0)

if __name__ == "__main__":
    unittest.main()
