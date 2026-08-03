from __future__ import annotations
import hashlib
from typing import Any, List, Optional, Union

def hash_value(value: Any, salt: str = "") -> str:
    """Hash a value using SHA-256 with an optional salt."""
    if value is None:
        return ""
    raw = f"{value}:{salt}"
    return f"sha256:{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"

def mask_value(value: Any, col_type: str = "text") -> str:
    """Partially mask sensitive strings (e.g. email, phone, text)."""
    if value is None:
        return ""
    val_str = str(value).strip()
    if not val_str:
        return ""
        
    if "@" in val_str:  # Simple email mask
        parts = val_str.split("@", 1)
        name, domain = parts[0], parts[1]
        masked_name = name[0] + "*" * (len(name) - 1) if len(name) > 1 else "*"
        return f"{masked_name}@{domain}"
        
    if any(char.isdigit() for char in val_str) and len(val_str) >= 7:  # Phone/ID mask
        return val_str[:3] + "*" * (len(val_str) - 5) + val_str[-2:]
        
    # Standard text mask
    return val_str[0] + "*" * (len(val_str) - 2) + val_str[-1] if len(val_str) > 2 else "*"

def protect_samples(
    samples: List[Any],
    mode: str = "masked",
    column_name: Optional[str] = None,
    redact_columns: Optional[List[str]] = None,
    salt: str = "default_dhara_salt"
) -> List[Any]:
    """
    Filter and secure sensitive diagnostic sample values before report inclusion.
    """
    mode = (mode or "masked").strip().lower()
    redact_cols = {str(c).lower() for c in (redact_columns or [])}
    
    # If the column matches redaction list, force redacted/hashed mode
    col_lower = str(column_name or "").lower()
    is_sensitive = col_lower in redact_cols
    
    protected = []
    for s in samples:
        if s is None:
            protected.append(None)
            continue
            
        if is_sensitive or mode == "redacted":
            protected.append("[REDACTED]")
        elif mode == "hashed":
            protected.append(hash_value(s, salt))
        elif mode == "masked":
            protected.append(mask_value(s))
        else:  # clear
            protected.append(s)
            
    return protected
