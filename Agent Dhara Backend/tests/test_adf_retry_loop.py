import pytest
from unittest.mock import patch, MagicMock
from agent.etl_pipeline.llm_codegen import generate_adf_with_llm

@pytest.mark.asyncio
async def test_adf_retry_loop_success_on_retry():
    call_mock = MagicMock(side_effect=["{invalid_json}", '{"name": "valid_adf"}'])
    
    plan = {"plan_id": "test-adf", "datasets": {}}
    assessment = {}
    
    with patch("agent.etl_pipeline.llm_codegen._call_llm", call_mock):
        obj, err = await generate_adf_with_llm(plan, assessment)
        assert err is None
        assert obj == {"name": "valid_adf"}
        assert call_mock.call_count == 2

@pytest.mark.asyncio
async def test_adf_retry_loop_fail_after_max_retries():
    call_mock = MagicMock(return_value="{invalid_json}")
    
    plan = {"plan_id": "test-adf", "datasets": {}}
    assessment = {}
    
    with patch("agent.etl_pipeline.llm_codegen._call_llm", call_mock):
        obj, err = await generate_adf_with_llm(plan, assessment)
        assert obj is None
        assert "ADF JSON generation failed" in err
        assert call_mock.call_count == 3
