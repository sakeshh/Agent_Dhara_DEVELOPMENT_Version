"""
Test suite for Sprint 2: Physical & Semantic Type Separation and Multi-Signal Inference.
"""

import pytest
import pandas as pd

from agent.profiling.semantic_inference import (
    analyze_physical_types,
    infer_semantic_type_with_confidence,
    PhysicalType,
    SemanticType
)


def test_physical_type_distribution():
    s = pd.Series(["100", "200", "300.5", None, "abc"])
    dist = analyze_physical_types(s)

    assert PhysicalType.INTEGER in dist or PhysicalType.FLOAT in dist
    assert PhysicalType.NULL in dist
    assert PhysicalType.STRING in dist


def test_semantic_inference_email_pattern():
    s = pd.Series(["user1@test.com", "user2@test.com", "invalid_email", "user3@domain.org"])
    res = infer_semantic_type_with_confidence("user_email_address", s)

    assert res.semantic_type == SemanticType.EMAIL
    assert res.semantic_confidence >= 0.70


def test_contract_override_precedence():
    s = pd.Series(["00123", "00124", "00125"])
    res = infer_semantic_type_with_confidence("cust_code", s, contract_override="identifier")

    assert res.semantic_type == "identifier"
    assert res.semantic_confidence == 1.0
    assert res.semantic_source == "contract"
