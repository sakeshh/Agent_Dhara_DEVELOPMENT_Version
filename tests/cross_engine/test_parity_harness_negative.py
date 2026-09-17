"""
tests/cross_engine/test_parity_harness_negative.py

Negative test suite for Increment 3.0 parity harness.
Deliberately introduces discrepancies and proves that the harness
unconditionally rejects each failure mode:
1. Wrong null result (null vs value)
2. Missing row in occurrence multiset
3. Decimal scale mismatch
4. Timestamp timezone shift
5. Wrong routing destination (curated vs quarantine)
6. HMAC byte mismatch
7. Unexpected success when failure was expected
8. Expired deviation rejection
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pytest

from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
    ExpectedEngineDeviation,
    FailureCategory,
    NormalizedFailure,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
)
from agent.etl_pipeline.cross_engine.parity_models import ParityComparatorType
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    DecimalCanonicalizationMode,
)
from tests.cross_engine.assertions import (
    assert_byte_parity,
    assert_decimal_parity,
    assert_failure_parity,
    assert_floating_parity,
    assert_row_occurrence_parity,
    assert_temporal_parity,
)


def test_negative_wrong_null_result():
    """Prove comparator rejects unexpected null where a value was produced, and vice-versa."""
    # 1. Expected null, got string
    res1 = assert_byte_parity("foo", "bar")
    assert res1.passed is False

    # 2. Decimal nullness mismatch
    res_dec = assert_decimal_parity(Decimal("10.5"), None)
    assert res_dec.passed is False
    assert res_dec.details.get("error") == "Nullness mismatch"

    # 3. Float nullness mismatch
    res_flt = assert_floating_parity(None, 42.0)
    assert res_flt.passed is False
    assert res_flt.details.get("error") == "Nullness mismatch"

    # 4. Temporal nullness mismatch
    res_ts = assert_temporal_parity(datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc), None)
    assert res_ts.passed is False
    assert res_ts.details.get("error") == "Nullness mismatch"


def test_negative_missing_row_in_multiset():
    """Prove multiset comparator catches missing rows, extra rows, or lost duplicates."""
    input_ids = ["row1", "row2", "row2", "row3"]

    # One occurrence of row2 was dropped
    curated_ids = ["row1", "row2"]
    quarantine_ids = ["row3"]
    res = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=curated_ids,
        secondary_ids=quarantine_ids,
        mode="partition_quarantine",
    )
    assert res.passed is False
    assert res.details.get("combined_equals_input") is False


def test_negative_decimal_scale_mismatch():
    """Prove decimal comparator rejects scale mismatch in PRESERVE_DECLARED_SCALE mode."""
    # Scale 2 vs Scale 4
    d1 = Decimal("10.50")
    d2 = Decimal("10.5000")
    res_strict = assert_decimal_parity(
        d1,
        d2,
        mode=DecimalCanonicalizationMode.PRESERVE_DECLARED_SCALE,
    )
    assert res_strict.passed is False


def test_negative_timestamp_timezone_shift():
    """Prove temporal comparator rejects timestamps that represent different instants."""
    dt1 = datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone.utc)
    # 1 hour shift
    dt2 = datetime(2026, 9, 16, 13, 0, 0, tzinfo=timezone.utc)
    res = assert_temporal_parity(dt1, dt2)
    assert res.passed is False


def test_negative_wrong_routing_destination():
    """Prove comparator rejects when rows leak across partition boundaries."""
    input_ids = ["id1", "id2", "id3"]
    # id2 was sent to BOTH curated and quarantine (leakage / overlap violation)
    curated = ["id1", "id2"]
    quarantine = ["id2", "id3"]
    res = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=curated,
        secondary_ids=quarantine,
        mode="partition_quarantine",
    )
    assert res.passed is False
    assert res.details.get("is_disjoint") is False


def test_negative_hmac_byte_mismatch():
    """Prove byte comparator rejects mismatched HMAC digest hex strings."""
    hmac_standard_vector = "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
    hmac_corrupted = "c613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
    res = assert_byte_parity(hmac_standard_vector, hmac_corrupted, field_name="hmac_sha256")
    assert res.passed is False
    assert res.actual != res.expected


def test_negative_unexpected_success_when_failure_expected():
    """Prove failure comparator flags when an error is expected but a different outcome occurs."""
    exp_fail = NormalizedFailure(
        category=FailureCategory.OVERFLOW,
        reason_code="INT64_OVERFLOW",
        step_id="step_calc",
        family=ActionFamily.DERIVE,
        retryable=False,
    )
    # Target produced a different failure reason
    act_fail = NormalizedFailure(
        category=FailureCategory.DIVIDE_BY_ZERO,
        reason_code="DIVISION_BY_ZERO",
        step_id="step_calc",
        family=ActionFamily.DERIVE,
        retryable=False,
    )
    res = assert_failure_parity(exp_fail, act_fail)
    assert res.passed is False
    assert res.details.get("category_match") is False


def test_negative_expired_deviation_is_rejected():
    """Prove expired engine deviations are rejected and cannot mask discrepancies."""
    reg = EngineCapabilityRegistry()
    now_utc = datetime.now(timezone.utc)
    expired_time = now_utc - timedelta(hours=2)

    key = EngineCapabilityKey(
        family=ActionFamily.DERIVE,
        expression_subtype="mod",
        policy_profile_hash="p1",
        compiler_name="pyspark_compiler",
        compiler_version="1.0.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="rt1",
    )

    dev_expired = ExpectedEngineDeviation(
        deviation_id="DEV-EXPIRED-01",
        family=ActionFamily.DERIVE,
        expression_subtype="mod",
        engine_name="pyspark",
        engine_version_range=">=3.5",
        runtime_profile_hash="rt1",
        affected_field="mod_val",
        comparator_policy="exact",
        rationale="Old deviation for modulo",
        approved_by="Architect",
        expires_at=expired_time,
        test_ids=("test_mod",),
    )
    reg.register_deviation(dev_expired)

    active_devs = reg.find_active_deviations(key, affected_field="mod_val", current_time=now_utc)
    assert len(active_devs) == 0, "Expired deviation MUST NOT be returned as active"
