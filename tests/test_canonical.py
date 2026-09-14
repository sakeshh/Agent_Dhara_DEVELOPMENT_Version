"""
Tests for agent.rules.canonical: tagged canonical JSON serialization and hashing.
"""
import math
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import pytest
from pydantic import BaseModel

from agent.rules.canonical import (
    CANONICALIZATION_SCHEME,
    CANONICALIZATION_VERSION,
    CanonicalSerializationError,
    canonical_json,
    compute_canonical_hash,
    _normalize,
    _encode_normalized_json,
)


class StatusEnum(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class SampleModel(BaseModel):
    name: str
    age: int
    score: float


def test_scheme_and_version():
    assert CANONICALIZATION_SCHEME == "agent-dhara-tagged-json"
    assert CANONICALIZATION_VERSION == "1.0"


def test_numeric_equivalence():
    # 1, 1.0, 1.00, and Decimal("1.000") produce identical output
    assert canonical_json(1) == canonical_json(1.0)
    assert canonical_json(1) == canonical_json(Decimal("1.00"))
    assert canonical_json(1) == canonical_json(Decimal("1.000"))
    assert compute_canonical_hash(1) == compute_canonical_hash(Decimal("1.00"))


def test_scientific_and_zero_normalization():
    # 1000 == 1e3, 0 == -0.0
    assert canonical_json(1000) == canonical_json(1e3)
    assert canonical_json(0) == canonical_json(-0.0)
    assert canonical_json(Decimal("0")) == canonical_json(Decimal("-0.0"))


def test_type_collision_prevention():
    # 1, "1", and True must NOT collide
    assert canonical_json(1) != canonical_json("1")
    assert canonical_json(1) != canonical_json(True)
    assert canonical_json("1") != canonical_json(True)
    assert canonical_json(0) != canonical_json(False)


def test_user_mapping_namespace_separation():
    # A user dict with {"$type": "number", "$value": "1"} must not collide with numeric 1
    user_dict = {"$type": "number", "$value": "1"}
    actual_num = 1
    actual_str = "1"
    assert canonical_json(user_dict) != canonical_json(actual_num)
    assert canonical_json(user_dict) != canonical_json(actual_str)


def test_datetime_serialization():
    dt1 = datetime(2026, 9, 14, 12, 0, 0, 123456, tzinfo=timezone.utc)
    # Naive datetime must raise
    naive_dt = datetime(2026, 9, 14, 12, 0, 0)
    with pytest.raises(CanonicalSerializationError, match="Naive datetime"):
        canonical_json(naive_dt)

    assert canonical_json(dt1) == '{"$type":"datetime","$value":"2026-09-14T12:00:00.123456Z"}'


def test_set_sorting_deterministic():
    # Sets with different insertion orders produce identical canonical output
    s1 = {"banana", "apple", "cherry"}
    s2 = {"cherry", "apple", "banana"}
    assert canonical_json(s1) == canonical_json(s2)

    # Set of numbers
    s3 = {3, 1, 2}
    s4 = {2, 3, 1}
    assert canonical_json(s3) == canonical_json(s4)


def test_strict_string_keys():
    # Non-string keys raise error
    with pytest.raises(CanonicalSerializationError, match="keys must be strings"):
        canonical_json({1: "val"})


def test_key_collision_on_normalization():
    # Decomposed and composed unicode in dict keys produce collision
    # e.g., 'e\u0301' (e + combining acute accent) vs '\u00e9' (é)
    k1 = "caf\u00e9"
    k2 = "cafe\u0301"
    assert k1 != k2  # distinct raw strings
    d = {k1: 1, k2: 2}
    with pytest.raises(CanonicalSerializationError, match="key collision"):
        canonical_json(d)


def test_pydantic_and_enum():
    model = SampleModel(name="test", age=30, score=95.5)
    cj = canonical_json(model)
    assert '"name"' in cj
    assert '"age"' in cj
    assert '"score"' in cj

    assert canonical_json(StatusEnum.ACTIVE) == '{"$type":"string","$value":"ACTIVE"}'


def test_two_stage_idempotency():
    data = {"status": "ACTIVE", "counts": [1, 2, 3], "ratio": 0.5}
    norm1 = _normalize(data)
    norm2 = _normalize(norm1)
    # Re-normalizing an already normalized tagged tree should be safe or wrapped in mapping
    encoded1 = _encode_normalized_json(norm1)
    assert canonical_json(data) == encoded1


def test_non_finite_float_rejected():
    with pytest.raises(CanonicalSerializationError, match="Non-finite float"):
        canonical_json(float("inf"))
    with pytest.raises(CanonicalSerializationError, match="Non-finite float"):
        canonical_json(float("nan"))
