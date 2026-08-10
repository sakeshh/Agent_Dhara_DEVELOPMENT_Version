"""
Test suite for Sprint 5: Context-Aware Placeholder Detection.
"""

import pytest
from agent.profiling.placeholder_detector import (
    classify_placeholder_value,
    SemanticType
)


def test_general_placeholder_detection():
    res = classify_placeholder_value("N/A")
    assert res.is_placeholder is True
    assert res.category == "generic_null"


def test_semantic_date_sentinel():
    res = classify_placeholder_value("1900-01-01", semantic_type=SemanticType.DATE)
    assert res.is_placeholder is True
    assert res.category == "semantic_sentinel"
    assert "date" in res.specific_diagnosis


def test_valid_identifier_lowercase_not_placeholder():
    # Lowercase identifiers like 'usr_abc' should NOT be misclassified as placeholders
    res = classify_placeholder_value("usr_abc", semantic_type=SemanticType.IDENTIFIER)
    assert res.is_placeholder is False
    assert res.category == "valid_value"
