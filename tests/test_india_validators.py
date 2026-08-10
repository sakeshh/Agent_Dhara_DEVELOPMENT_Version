from __future__ import annotations
import pandas as pd
from agent.validators.india_domain import (
    validate_gstin,
    validate_pan,
    validate_aadhaar,
    validate_ifsc,
    validate_cin
)
from agent.profiling.type_inference import detect_semantic_type
from agent.profiling.dq_checks import analyze_dataset_quality


def test_standalone_validators():
    # GSTIN: 2-digit state + 10 PAN + 1 entity + Z + 1 check
    assert validate_gstin("27AAAAA1111A1Z1") is True
    assert validate_gstin("invalid_gstin") is False
    
    # PAN: 5 letters + 4 digits + 1 letter
    assert validate_pan("ABCDE1234F") is True
    assert validate_pan("ABCD12345F") is False
    
    # Aadhaar: 12 digits, first digit not 0 or 1, validated via Verhoeff
    # Valid Aadhaar with correct Verhoeff checksum
    assert validate_aadhaar("362189456327") is True  # standard Aadhaar-like valid checksum
    assert validate_aadhaar("062189456327") is False  # starts with 0
    assert validate_aadhaar("162189456327") is False  # starts with 1
    assert validate_aadhaar("362189456322") is False  # bad Verhoeff checksum
    
    # IFSC: 4 letters + 0 + 6 alphanumeric
    assert validate_ifsc("SBIN0123456") is True
    assert validate_ifsc("SBIN1123456") is False  # 5th char not 0
    
    # CIN: L/U + 5 digits + 2 letters state + 4 digits year + 3 letters type + 6 digits reg
    assert validate_cin("L01234MH2026PTC123456") is True
    assert validate_cin("invalid_cin") is False


def test_type_inference_integration():
    # Aadhaar series
    aadhaar_series = pd.Series(["362189456327", "362189456327", "362189456327"])
    assert detect_semantic_type(aadhaar_series, "aadhaar_no") == "aadhaar"
    
    # PAN series
    pan_series = pd.Series(["ABCDE1234F", "ABCDE1234F", "ABCDE1234F"])
    assert detect_semantic_type(pan_series, "employee_pan") == "pan"


def test_dq_checks_integration():
    df = pd.DataFrame({
        "gstin": ["27AAAAA1111A1Z1", "invalid_gst", "27AAAAA1111A1Z1"],
        "employee_pan": ["ABCDE1234F", "ABCDE1234F", "bad_pan"]
    })
    
    profile = {
        "columns": {
            "gstin": {"dtype": "object"},
            "employee_pan": {"dtype": "object"}
        }
    }
    
    res = analyze_dataset_quality("test_ds", df, profile)
    issues = res["issues"]
    
    assert len(issues) >= 2
    
    gst_issue = next(i for i in issues if i["type"] == "invalid_gstin")
    assert gst_issue["column"] == "gstin"
    assert gst_issue["count"] == 1
    assert "invalid_gst" in gst_issue["sample_values"]
    
    pan_issue = next(i for i in issues if i["type"] == "invalid_pan")
    assert pan_issue["column"] == "employee_pan"
    assert pan_issue["count"] == 1
    assert "bad_pan" in pan_issue["sample_values"]


def test_disposable_email_and_mojibake_detection():
    df = pd.DataFrame({
        "contact_email": ["test@gmail.com", "fake@mailinator.com", "ok@yopmail.com"],
        "customer_name": ["René", "Renâ€™e", "John Doe"]  # "Renâ€™e" contains common UTF-8 -> ISO-8859-1 mojibake
    })
    
    profile = {
        "columns": {
            "contact_email": {"dtype": "object"},
            "customer_name": {"dtype": "object"}
        }
    }
    
    res = analyze_dataset_quality("test_ds", df, profile)
    issues = res["issues"]
    
    # Verify disposable email detection
    disp_issue = next(i for i in issues if i["type"] == "disposable_email")
    assert disp_issue["column"] == "contact_email"
    assert disp_issue["count"] == 2
    assert "fake@mailinator.com" in disp_issue["sample_values"]
    assert "ok@yopmail.com" in disp_issue["sample_values"]
    
    # Verify mojibake detection
    moji_issue = next(i for i in issues if i["type"] == "encoding_corruption")
    assert moji_issue["column"] == "customer_name"
    assert moji_issue["count"] == 1
    assert "Renâ€™e" in moji_issue["sample_values"]

