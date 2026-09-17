"""
tests/cross_engine/test_cross_engine_contracts.py

Comprehensive test suite verifying Increment 3.0 shared parity foundation contracts:
- 11-field EngineCapabilityKey and explicit CapabilityDecision eligibility
- EngineCapabilityRegistry default-to-blocked governance and deviation lifecycle
- RuntimeProfile deterministic hashing and semantic mutation sensitivity
- Authoritative CANONICAL_SCALAR_V1 compliance and cryptographic vector hash integrity
- Category-specific semantic comparators and routing mode occurrence conservation
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import pytest

from agent.etl_pipeline.canonical_encoding import (
    CANONICAL_TEST_VECTORS_V1,
    CANONICAL_VECTORS_HASH_V1,
    CANONICAL_TUPLE_TEST_VECTORS_V1,
    CANONICAL_TUPLE_VECTORS_HASH_V1,
    TAG_BOOL_FALSE,
    TAG_BOOL_TRUE,
    TAG_FLOAT_PREFIX,
    TAG_INT_PREFIX,
    TAG_NAT,
    TAG_NULL,
    canonical_encode_scalar_v1,
    canonical_encode_coordinate_tuple_v1,
)
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
from agent.etl_pipeline.cross_engine.runtime_profiles import (
    PandasRuntimeProfile,
    SparkRuntimeProfile,
    SqlRuntimeProfile,
    compute_runtime_profile_hash,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    CanonicalSerializationError,
    DecimalCanonicalizationMode,
)
from tests.cross_engine.assertions import (
    assert_boolean_parity,
    assert_byte_parity,
    assert_decimal_parity,
    assert_failure_parity,
    assert_floating_parity,
    assert_row_occurrence_parity,
    assert_schema_parity,
    assert_temporal_parity,
    assert_validation_metrics_parity,
)


def test_engine_capability_key_11_fields_and_compact_representation():
    """Verify 11-field capability key initialization and deterministic compact string."""
    key = EngineCapabilityKey(
        family=ActionFamily.MAP_VALUE,
        expression_subtype="trim",
        policy_profile_hash="abcdef0123456789",
        ir_version="v1.0",
        catalogue_version="v1.0",
        compiler_name="pyspark_compiler",
        compiler_version="1.0.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        dialect="spark_sql",
        runtime_profile_hash="9876543210fedcba",
    )
    compact = key.to_compact_key()
    assert "pyspark_compiler:1.0.0" in compact
    assert "pyspark:3.5.1:spark_sql" in compact
    assert "map_value:trim" in compact
    assert "ir=v1.0:pol=abcdef01:rt=98765432" in compact


def test_capability_decision_explicit_execution_eligibility():
    """Verify execution eligibility is strictly calculated, never inferred informally."""
    key = EngineCapabilityKey(
        family=ActionFamily.HASH,
        expression_subtype="hmac_sha256",
        policy_profile_hash="p1",
        compiler_name="pyspark_compiler",
        compiler_version="1.0.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="r1",
    )

    # 1. VERIFIED -> execution_eligible = True
    d_ver = CapabilityDecision.create(key=key, status=CapabilityStatus.VERIFIED, reason_code="CERTIFIED")
    assert d_ver.execution_eligible is True
    assert d_ver.waiver_required is False

    # 2. PARTIAL without waiver -> execution_eligible = False, waiver_required = True
    d_part = CapabilityDecision.create(key=key, status=CapabilityStatus.PARTIAL, reason_code="PARTIAL_IMPL")
    assert d_part.execution_eligible is False
    assert d_part.waiver_required is True

    # 3. PARTIAL with waiver -> execution_eligible = True
    d_part_waiver = CapabilityDecision.create(
        key=key, status=CapabilityStatus.PARTIAL, reason_code="PARTIAL_IMPL", waiver_granted=True
    )
    assert d_part_waiver.execution_eligible is True
    assert d_part_waiver.waiver_required is True

    # 4. NOT_TESTED -> execution_eligible = False
    d_nt = CapabilityDecision.create(key=key, status=CapabilityStatus.NOT_TESTED, reason_code="NOT_EVALUATED")
    assert d_nt.execution_eligible is False

    # 5. UNSUPPORTED -> execution_eligible = False
    d_unsup = CapabilityDecision.create(key=key, status=CapabilityStatus.UNSUPPORTED, reason_code="PROHIBITED")
    assert d_unsup.execution_eligible is False

    # 6. ORCHESTRATION_ONLY -> execution_eligible = False
    d_orch = CapabilityDecision.create(key=key, status=CapabilityStatus.ORCHESTRATION_ONLY, reason_code="ORCH_DISPATCH")
    assert d_orch.execution_eligible is False


def test_capability_registry_default_to_blocked():
    """Verify registry defaults any unestablished capability to NOT_TESTED and blocked."""
    reg = EngineCapabilityRegistry()
    unreg_key = EngineCapabilityKey(
        family=ActionFamily.JOIN,
        expression_subtype="cross",
        policy_profile_hash="p_cross",
        compiler_name="pyspark_compiler",
        compiler_version="1.0.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="rt_cross",
    )
    decision = reg.evaluate_capability(unreg_key)
    assert decision.status == CapabilityStatus.NOT_TESTED
    assert decision.execution_eligible is False
    assert decision.reason_code == "CAPABILITY_NOT_REGISTERED"


def test_capability_registry_deviation_expiration():
    """Verify that expected deviations expire deterministically and fail when stale."""
    reg = EngineCapabilityRegistry()
    now_utc = datetime.now(timezone.utc)
    expired_time = now_utc - timedelta(days=1)
    future_time = now_utc + timedelta(days=30)

    key = EngineCapabilityKey(
        family=ActionFamily.DERIVE,
        expression_subtype="float_div",
        policy_profile_hash="p_div",
        compiler_name="pyspark_compiler",
        compiler_version="1.0.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="rt_div",
    )

    dev_active = ExpectedEngineDeviation(
        deviation_id="DEV-001",
        family=ActionFamily.DERIVE,
        expression_subtype="float_div",
        engine_name="pyspark",
        engine_version_range=">=3.5",
        runtime_profile_hash="rt_div",
        affected_field="res_float",
        comparator_policy="floating_tolerance",
        rationale="Minor LSB discrepancy in float division",
        approved_by="Architect",
        expires_at=future_time,
        test_ids=("test_div",),
    )
    reg.register_deviation(dev_active)
    active_matches = reg.find_active_deviations(key, affected_field="res_float", current_time=now_utc)
    assert len(active_matches) == 1
    assert active_matches[0].deviation_id == "DEV-001"

    # Expired deviation
    dev_expired = ExpectedEngineDeviation(
        deviation_id="DEV-002",
        family=ActionFamily.DERIVE,
        expression_subtype="float_div",
        engine_name="pyspark",
        engine_version_range=">=3.5",
        runtime_profile_hash="rt_div",
        affected_field="res_float",
        comparator_policy="floating_tolerance",
        rationale="Old temporary deviation",
        approved_by="Architect",
        expires_at=expired_time,
        test_ids=("test_div_old",),
    )
    reg.register_deviation(dev_expired)
    # DEV-002 must NOT be included in active deviations!
    active_after = reg.find_active_deviations(key, affected_field="res_float", current_time=now_utc)
    active_ids = [d.deviation_id for d in active_after]
    assert "DEV-001" in active_ids
    assert "DEV-002" not in active_ids


def test_runtime_profile_hashing_determinism_and_semantic_sensitivity():
    """Verify runtime profile hashing is deterministic and sensitive to semantic settings."""
    p1 = SparkRuntimeProfile(ansi_enabled=True, session_timezone="UTC", case_sensitive=True)
    p2 = SparkRuntimeProfile(ansi_enabled=True, session_timezone="UTC", case_sensitive=True)
    assert p1.compute_hash() == p2.compute_hash()

    # Altering ANSI mode must alter runtime profile hash
    p3 = SparkRuntimeProfile(ansi_enabled=False, session_timezone="UTC", case_sensitive=True)
    assert p1.compute_hash() != p3.compute_hash()

    # Altering session timezone must alter runtime profile hash
    p4 = SparkRuntimeProfile(ansi_enabled=True, session_timezone="America/New_York", case_sensitive=True)
    assert p1.compute_hash() != p4.compute_hash()


def test_canonical_scalar_v1_vectors_and_integrity_hash():
    """Verify canonical scalar and coordinate tuple encoders pass versioned test vectors with unaltered hashes."""
    # 1. Verify scalar vector hash integrity
    computed_hash = CANONICAL_VECTORS_HASH_V1
    assert computed_hash == "ea0a9a07c8ea220bf6b411dcc2052dc2827a5aa07073ec10e60facd6d372e2be"

    # 2. Verify all scalar test vectors produce exact expected hex bytes
    for vec in CANONICAL_TEST_VECTORS_V1:
        v_type = vec["input_type"]
        expected_hex = vec["expected_hex"]
        if v_type == "null":
            b = canonical_encode_scalar_v1(None)
        elif v_type == "nat":
            import pandas as pd
            b = canonical_encode_scalar_v1(pd.NaT)
        elif v_type == "date":
            from datetime import date
            b = canonical_encode_scalar_v1(date(2026, 9, 16))
        else:
            b = canonical_encode_scalar_v1(vec["value"])

        assert b.hex() == expected_hex, f"Mismatch in vector {vec['vector_id']}: expected {expected_hex}, got {b.hex()}"

    # 3. Verify coordinate tuple vector hash integrity
    tuple_hash = CANONICAL_TUPLE_VECTORS_HASH_V1
    assert tuple_hash == "1bd1c7e8e70fb7ad71db783e353359fd7fce733ffdc0088690a8d25dedbe7f72"

    # 4. Verify all coordinate tuple test vectors produce exact expected hex bytes
    for t_vec in CANONICAL_TUPLE_TEST_VECTORS_V1:
        t_b = canonical_encode_coordinate_tuple_v1(t_vec["coords"])
        assert t_b.hex() == t_vec["expected_hex"], f"Mismatch in tuple vector {t_vec['vector_id']}: expected {t_vec['expected_hex']}, got {t_b.hex()}"


def test_canonical_scalar_v1_rejection_invariants():
    """Verify canonical scalar encoder strict rejection invariants."""
    from datetime import datetime

    # 1. Naive datetime without dataset_tz -> rejects
    with pytest.raises(CanonicalSerializationError, match="Naive timestamp encountered without an explicit dataset timezone"):
        canonical_encode_scalar_v1(datetime(2026, 9, 16, 12, 0, 0))

    # 2. Non-finite Decimal -> rejects
    with pytest.raises(CanonicalSerializationError, match="Non-finite Decimal"):
        canonical_encode_scalar_v1(Decimal("Infinity"))

    # 3. Unsupported object -> rejects explicitly with zero string fallback
    class ArbitraryCustomClass:
        pass
    with pytest.raises(CanonicalSerializationError, match="Unsupported scalar type"):
        canonical_encode_scalar_v1(ArbitraryCustomClass())


def test_category_specific_assertions_byte_decimal_float_temporal():
    """Verify semantic category assertions for byte, decimal, float, and temporal types."""
    # 1. Byte Parity
    res_b_pass = assert_byte_parity(b"exact_bytes", b"exact_bytes")
    assert res_b_pass.passed is True
    res_b_fail = assert_byte_parity(b"bytes_a", b"bytes_b")
    assert res_b_fail.passed is False

    # 2. Decimal Parity (scale normalized)
    res_dec_pass = assert_decimal_parity(Decimal("12.3400"), Decimal("12.34"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    assert res_dec_pass.passed is True
    res_dec_fail = assert_decimal_parity(Decimal("12.34"), Decimal("12.35"))
    assert res_dec_fail.passed is False

    # 3. Floating Parity (tolerance, NaN, -0.0 canonicalization)
    res_flt_pass = assert_floating_parity(1.0000000001, 1.0000000002, abs_tol=1e-8)
    assert res_flt_pass.passed is True
    res_flt_nan = assert_floating_parity(float("nan"), float("nan"))
    assert res_flt_nan.passed is True
    res_flt_zero = assert_floating_parity(-0.0, 0.0)
    assert res_flt_zero.passed is True

    # 4. Temporal Parity (UTC instant matching)
    from datetime import timezone
    dt1 = datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone.utc)
    dt2 = datetime(2026, 9, 16, 8, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
    res_ts_pass = assert_temporal_parity(dt1, dt2)
    assert res_ts_pass.passed is True


def test_mode_specific_row_occurrence_multiset_conservation():
    """Verify mode-specific multiset conservation contracts."""
    input_ids = ["a", "a", "b", "c", "d"]

    # 1. Partition Quarantine: sum equals input AND disjoint
    res_part_pass = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=["a", "a", "b"],
        secondary_ids=["c", "d"],
        mode="partition_quarantine",
    )
    assert res_part_pass.passed is True

    # Overlapping partition -> fails
    res_part_fail = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=["a", "a", "b", "c"],
        secondary_ids=["c", "d"],
        mode="partition_quarantine",
    )
    assert res_part_fail.passed is False

    # 2. Copy Quarantine: primary equals input AND quarantine is submultiset
    res_copy_pass = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=["a", "a", "b", "c", "d"],
        secondary_ids=["a", "c"],
        mode="copy_quarantine",
    )
    assert res_copy_pass.passed is True

    # 3. Filter with dropped: retained + dropped == input
    res_filt_pass = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=["a", "a"],
        secondary_ids=["b", "c", "d"],
        mode="filter_with_dropped",
    )
    assert res_filt_pass.passed is True

    # 4. Deduplicate with removed: survivor + removed == input
    res_dedup_pass = assert_row_occurrence_parity(
        input_ids=input_ids,
        primary_ids=["a", "b", "c", "d"],
        secondary_ids=["a"],
        mode="deduplicate_with_removed",
    )
    assert res_dedup_pass.passed is True


def test_validation_metrics_and_failure_parity():
    """Verify validation metrics and typed failure envelope comparators."""
    exp_metrics = {
        "candidate_count": 100,
        "pass_count": 95,
        "violation_count": 5,
        "raw_unknown_count": 2,
        "effective_violation_count": 7,
    }
    act_metrics = dict(exp_metrics)
    res_vm = assert_validation_metrics_parity(exp_metrics, act_metrics)
    assert res_vm.passed is True

    # Failure parity
    f1 = NormalizedFailure(
        category=FailureCategory.JOIN_CARDINALITY,
        reason_code="ONE_TO_ONE_VIOLATION",
        step_id="step_join",
        family=ActionFamily.JOIN,
        retryable=False,
    )
    f2 = NormalizedFailure(
        category=FailureCategory.JOIN_CARDINALITY,
        reason_code="ONE_TO_ONE_VIOLATION",
        step_id="step_join",
        family=ActionFamily.JOIN,
        retryable=False,
    )
    res_fail = assert_failure_parity(f1, f2)
    assert res_fail.passed is True
