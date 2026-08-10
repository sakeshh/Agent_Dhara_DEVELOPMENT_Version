from __future__ import annotations
import pytest
import pandas as pd


@pytest.fixture
def dirty_customer_df() -> pd.DataFrame:
    data = {
        "customer_id": [1, 2, 2, None, 5, 6, 7],  # duplicate key (2), null key (None)
        "name": ["Alice", "Bob", "Bob", "Charlie", "D\xc3\xa9j\xc3\xa0", "Eve", "Frank"],  # Mojibake 'DÃ©jÃ '
        "email": ["alice@gmail.com", "bob@mailinator.com", "bob@mailinator.com", "charlie@gmail", "invalid-email", "eve@tempmail.com", "frank@gmail.com"],  # disposable email, invalid format
        "phone": ["+91 99999 99999", "12345", "+919876543210", None, "abc", "9999999999", "0000000000"],
        "joined_date": ["2023-01-01", "2023/02/02", "2023-02-02", "01-05-2023", "invalid-date", "2023-06-01", "2023-07-01"],  # Date format variants
        "aadhaar": ["362189456327", "123456789012", "062189456327", "362189456328", None, "362189456327", "invalid"],  # valid Aadhaar (362189456327 has valid checksum), invalid Aadhaar (123456789012 has invalid checksum, 062189456327 starts with 0/1)
        "pan": ["ABCDE1234F", "ABCD1234E", "ABCDE12345", None, "ABCDE1234F", "invalid-pan", "XYZPQ9876R"],  # valid PAN (ABCDE1234F), invalid format (ABCD1234E, ABCDE12345)
    }
    return pd.DataFrame(data)
