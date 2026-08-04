"""
Test suite for Sprint 7: Privacy Pipeline Separation & Checksum Validation.
"""

import pytest
import pandas as pd

from agent.security.privacy_engine import (
    luhn_checksum_valid,
    iban_checksum_valid,
    analyze_dataset_privacy_risk
)


def test_luhn_checksum():
    # Valid Visa card vs invalid number
    assert luhn_checksum_valid("4532015112830366") is True
    assert luhn_checksum_valid("4532015112830367") is False


def test_privacy_risk_independent_score():
    df = pd.DataFrame({
        "card_number": ["4532015112830366", "4532015112830366"],
        "user_email": ["test1@domain.com", "test2@domain.com"]
    })

    summary = analyze_dataset_privacy_risk(df)

    assert summary.privacy_risk_score_0_100 > 0.0
    assert summary.high_confidence_pii_count == 2
    assert len(summary.findings) == 2
