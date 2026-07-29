from __future__ import annotations
import pytest

from agent.jobs_store import save_checkpoint, load_checkpoint
from agent.errors import AgentError, AgentErrorCode


def test_checkpoint_save_and_load():
    """
    Contract: Checkpoint data saved under a job_id and stage must be loadable
    exactly as saved, and return None if the checkpoint does not exist.
    """
    job_id = "test_job_123"
    stage = "source_loader"
    stage_output = {"ok": True, "datasets_count": 5}
    
    # Save checkpoint
    save_checkpoint(job_id, stage, stage_output)
    
    # Load checkpoint
    loaded = load_checkpoint(job_id, stage)
    assert loaded == stage_output
    
    # Load non-existent
    assert load_checkpoint(job_id, "non_existent") is None


def test_agent_error_taxonomy():
    """
    Contract: AgentError must wrap AgentErrorCode, message, and recoverable flag correctly,
    and be serializable via to_dict().
    """
    err = AgentError(AgentErrorCode.CONNECTION_FAILED, "Could not connect to database", recoverable=True)
    assert err.code == AgentErrorCode.CONNECTION_FAILED
    assert err.message == "Could not connect to database"
    assert err.recoverable is True
    
    d = err.to_dict()
    assert d["code"] == "CONNECTION_FAILED"
    assert d["message"] == "Could not connect to database"
    assert d["recoverable"] is True


from unittest.mock import patch

@patch("agent.langgraph_orchestrator.run_orchestrator")
def test_run_job_orchestrator_checkpointing(mock_orchestrate):
    """
    Contract: If checkpoint is present for a stage, it must not run the stage again.
    """
    from agent.jobs_worker import _run_job
    from agent.jobs_store import save_checkpoint
    
    job_id = "job-999"
    job = {
        "job_id": job_id,
        "kind": "assess",
        "input": {"session_id": "sess-999", "user_request": "hello"}
    }
    
    # Save a fake orchestrator checkpoint
    fake_state = {"extractions": [{"result": {"datasets": {"dbo.T1": {}}}}]}
    save_checkpoint(job_id, "orchestrator", fake_state)
    
    # Also save a fake reports checkpoint
    fake_reports = {
        "report_markdown": "MD",
        "report_html": "HTML",
        "report_files": []
    }
    save_checkpoint(job_id, "reports", fake_reports)
    
    # Act
    res = _run_job(job)
    
    # Assert
    mock_orchestrate.assert_not_called()
    assert res["report_markdown"] == "MD"

