import pytest
import time
import asyncio
from agent.etl_pipeline.llm_codegen import (
    _write_failure_cache,
    _is_failure_cached,
    _PLAN_FAILURE_COUNTS,
    _MAX_PLAN_FAILURES,
    generate_etl_with_llm,
)

@pytest.mark.asyncio
async def test_failure_cache_ttl():
    cache_key = "test_key"
    _write_failure_cache(cache_key, "some error", ttl_seconds=1)
    assert _is_failure_cached(cache_key) == "some error"
    
    await asyncio.sleep(1.1)
    assert _is_failure_cached(cache_key) is None

@pytest.mark.asyncio
async def test_circuit_breaker():
    plan_id = "test-plan-cb"
    plan = {"plan_id": plan_id, "datasets": {"ds": {"steps": []}}}
    assessment = {}
    
    _PLAN_FAILURE_COUNTS[plan_id] = _MAX_PLAN_FAILURES
    
    code, err = await generate_etl_with_llm(plan, assessment)
    assert code is None
    assert "Circuit open" in err
    
    _PLAN_FAILURE_COUNTS[plan_id] = 0
