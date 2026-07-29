import pytest
from agent.etl_pipeline.llm_codegen import _get_blob_read_template
from agent.etl_pipeline.io_snippets import _get_pandas_blob_read_snippet

def test_pyspark_blob_read_template():
    conn = {"storage_account": "myacct", "container_name": "mycontainer"}
    tmpl = _get_blob_read_template(conn)
    assert "wasbs://mycontainer@myacct.blob.core.windows.net/" in tmpl
    assert ".csv(" in tmpl

def test_pandas_blob_read_snippet():
    conn = {"storage_account": "myacct", "container_name": "mycontainer"}
    snippet = _get_pandas_blob_read_snippet("data.csv", conn)
    assert "_read_blob_pandas" in snippet
    assert "myacct" in snippet
    assert "mycontainer" in snippet
    assert "data.csv" in snippet
