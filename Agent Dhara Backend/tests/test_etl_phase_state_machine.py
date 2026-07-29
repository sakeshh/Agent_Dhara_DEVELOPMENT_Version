from __future__ import annotations
import pytest
import time
from agent.etl_handlers import (
    _can_transition,
    _transition,
    _migrate_phase,
    rollback_on_failure,
    ETL_PHASES,
    ALLOWED_TRANSITIONS,
)

def test_can_transition_rules():
    """Verify that _can_transition enforces ALLOWED_TRANSITIONS rules correctly."""
    # Self transitions are always valid
    for phase in ETL_PHASES:
        assert _can_transition(phase, phase) is True

    # Valid transitions from planned
    assert _can_transition("planned", "preview_ready") is True
    assert _can_transition("planned", "failed") is True
    assert _can_transition("planned", "approved") is False

    # Valid transitions from failed
    assert _can_transition("failed", "planned") is True
    assert _can_transition("failed", "approved") is False

    # Valid transitions from downloadable
    assert _can_transition("downloadable", "planned") is True
    assert _can_transition("downloadable", "generating") is True
    assert _can_transition("downloadable", "failed") is False


def test_transition_success():
    """Verify that _transition updates the flow phase and appends to phase_history."""
    flow = {"phase": "planned", "phase_history": []}
    
    _transition(flow, "preview_ready", by="test_user", reason="preview loaded")
    assert flow["phase"] == "preview_ready"
    assert len(flow["phase_history"]) == 1
    assert flow["phase_history"][0]["from"] == "planned"
    assert flow["phase_history"][0]["to"] == "preview_ready"
    assert flow["phase_history"][0]["by"] == "test_user"
    assert flow["phase_history"][0]["reason"] == "preview loaded"
    assert isinstance(flow["phase_history"][0]["ts"], float)


def test_transition_invalid_raises_value_error():
    """Verify that invalid transitions raise ValueError."""
    flow = {"phase": "planned"}
    with pytest.raises(ValueError, match="Invalid ETL phase transition"):
        _transition(flow, "approved")

    with pytest.raises(ValueError, match="Unknown phase"):
        _transition(flow, "non_existent_phase")


def test_migrate_phase_legacy_mapping():
    """Verify legacy phase names are correctly migrated to canonical ones."""
    flow = {"phase": "no_plan"}
    _migrate_phase(flow)
    assert flow["phase"] == "planned"

    flow = {"phase": "plan_validated"}
    _migrate_phase(flow)
    assert flow["phase"] == "preview_ready"


def test_rollback_on_failure():
    """Verify rollback resets flow variables and transitions to failed state."""
    flow = {
        "phase": "approved",
        "approved_plan": {"some": "plan"},
        "validation_ok": True,
        "phase_history": [],
    }

    rollback_on_failure(flow, reason="code compilation crashed")
    assert flow["phase"] == "planned"
    assert flow["approved_plan"] is None
    assert flow["validation_ok"] is False
    assert flow["failure_reason"] == "code compilation crashed"
    assert flow["last_failure_reason"] == "code compilation crashed"
    assert len(flow["phase_history"]) == 2
    assert flow["phase_history"][0]["to"] == "failed"
    assert flow["phase_history"][1]["to"] == "planned"
