import os
import pytest
from unittest.mock import patch

from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector, _resolve_connection_params


def test_quote_two_part_name_handles_brackets_and_unbracketed():
    conn = AzureSQLPythonNetConnector({"server": "test.database.windows.net", "database": "db"})
    assert conn._quote_two_part_name("dbo.EmployeeMaster") == "[dbo].[EmployeeMaster]"
    assert conn._quote_two_part_name("[dbo].[EmployeeMaster]") == "[dbo].[EmployeeMaster]"
    assert conn._quote_two_part_name(" [dbo] . [EmployeeMaster] ") == "[dbo].[EmployeeMaster]"
    assert conn._quote_two_part_name("EmployeeMaster") == "[EmployeeMaster]"
    assert conn._quote_two_part_name("[EmployeeMaster]") == "[EmployeeMaster]"


def test_resolve_connection_params_prioritizes_env_vars_and_ignores_placeholders(monkeypatch):
    monkeypatch.setenv("AZURE_SQL_SERVER", "prod.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DATABASE", "proddb")
    monkeypatch.setenv("AZURE_SQL_USERNAME", "real_admin")
    monkeypatch.setenv("AZURE_SQL_PASSWORD", "Secret@123")

    # Config has dummy values
    cfg = {
        "server": "agentdharadb.database.windows.net",
        "database": "agentdharadb",
        "username": "user",
        "password": "Pass@123",
    }
    server, db, user, pwd = _resolve_connection_params(cfg)
    assert server == "prod.database.windows.net"
    assert db == "proddb"
    assert user == "real_admin"
    assert pwd == "Secret@123"

    # When env vars are not set, dummy placeholders are discarded
    monkeypatch.delenv("AZURE_SQL_USERNAME", raising=False)
    monkeypatch.delenv("AZURE_SQL_PASSWORD", raising=False)
    server, db, user, pwd = _resolve_connection_params(cfg)
    assert user is None
    assert pwd is None


def test_run_job_fails_fast_when_extractions_empty_and_errors_exist():
    from agent.jobs_worker import _run_job

    job_id = "job-fail-test"
    job = {
        "job_id": job_id,
        "kind": "assess",
        "input": {"session_id": "sess-test", "user_request": "assess selected tables"}
    }

    with patch("agent.langgraph_orchestrator.run_orchestrator") as mock_orch:
        mock_orch.return_value = {
            "extractions": [],
            "extraction_errors": [
                {"source": "database", "type": "database", "error": "Login failed for user 'user'"}
            ]
        }
        with pytest.raises(RuntimeError) as exc_info:
            _run_job(job)

        assert "Data extraction failed" in str(exc_info.value)
        assert "Login failed for user 'user'" in str(exc_info.value)


@pytest.mark.asyncio
async def test_node_extract_async_blob_routing_not_hijacked_by_sql_config():
    from agent.langgraph_orchestrator import _node_extract_async
    import pandas as pd

    state = {
        "user_request": "assess selected files",
        "selected_blob_files": ["customers.csv"],
        "job_id": "job-blob-test",
        "sources_path": "config/sources.yaml",
    }

    fake_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    with patch("agent.mcp_interface.load_selected_blob_datasets") as mock_load_blob, \
         patch("agent.mcp_interface.run_assessment") as mock_run_assess:

        mock_load_blob.return_value = {"customers.csv": fake_df}
        mock_run_assess.return_value = {
            "datasets": {"customers.csv": {"row_count": 2}},
            "relationships": [],
            "data_quality_issues": {"datasets": {}, "global_issues": {}}
        }

        res = await _node_extract_async(state)
        assert "extractions" in res
        assert len(res["extractions"]) == 1
        assert res["extractions"][0]["location_type"] == "azure_blob"
        assert "customers.csv" in res["extractions"][0]["result"]["datasets"]

