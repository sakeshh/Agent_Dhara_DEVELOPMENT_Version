import pytest
from agent.etl_pipeline.payload_trimmer import trim_payload, _FLOOR_TRIM_CONFIG, _CODEGEN_TRIM_CONFIG, _estimate_tokens

def test_estimate_tokens():
    assert _estimate_tokens("hello") > 0
    assert _estimate_tokens({"key": "val"}) > 0

def test_trim_payload_no_op():
    payload = {"datasets": {"ds": {"steps": []}}}
    res = trim_payload(payload, _FLOOR_TRIM_CONFIG)
    assert res == payload

def test_trim_payload_standard_cleanups():
    payload = {
        "datasets": {"ds": {"steps": []}},
        "plan_narrator_output": "some narration",
        "policy_block": "some policy",
        "rule_provenance": "some rules"
    }
    from agent.etl_pipeline.payload_trimmer import TrimConfig
    small_config = TrimConfig(mode="codegen", token_budget=5, field_priority=[])
    res = trim_payload(payload, small_config)
    assert "plan_narrator_output" not in res
    assert "policy_block" not in res
    assert "rule_provenance" not in res
