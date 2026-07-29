from __future__ import annotations
import pytest
import pandas as pd
from agent.pii_masking import (
    is_sensitive_column,
    mask_value,
    mask_rows,
    scan_dataframe_for_pii,
)

def test_is_sensitive_column():
    assert is_sensitive_column("email") is True
    assert is_sensitive_column("customer_email_address") is True
    assert is_sensitive_column("mobile_number") is True
    assert is_sensitive_column("credit_card_no") is True
    assert is_sensitive_column("id") is False
    assert is_sensitive_column("product_name") is False


def test_mask_value():
    assert mask_value("email", "john.doe@example.com") == "jo***@example.com"
    assert mask_value("phone", "+1-555-123-4567") == "***4567"
    assert mask_value("api_key", "secret-token-123") == "***"
    assert mask_value("address", "123 Main St") == "12***St"


def test_mask_rows():
    rows = [
        {"id": 1, "name": "Alice", "email": "alice@gmail.com", "phone": "1234567890"},
        {"id": 2, "name": "Bob", "email": "bob@gmail.com", "phone": "0987654321"},
    ]
    masked = mask_rows(rows)
    assert masked[0]["id"] == 1
    assert masked[0]["name"] == "Alice"
    assert masked[0]["email"] == "al***@gmail.com"
    assert masked[0]["phone"] == "***7890"


def test_scan_dataframe_for_pii():
    df = pd.DataFrame({
        "notes": [
            "Sent email to test@domain.com yesterday",
            "No PII here",
            "Call my phone +1-555-555-1234 for details",
            "Aadhaar number: 3333-4444-5555"
        ],
        "safe_col": [
            "clean note 1",
            "clean note 2",
            "clean note 3",
            "clean note 4"
        ]
    })
    
    issues = scan_dataframe_for_pii(df, sample_size=10)
    
    # We expect issues in column "notes" for email, phone, and/or aadhaar
    assert len(issues) > 0
    note_cols = [x["column"] for x in issues]
    assert all(col == "notes" for col in note_cols)
    
    pii_types = [x["pii_type"] for x in issues]
    assert "email" in pii_types
    assert "phone" in pii_types
    assert "aadhaar" in pii_types
