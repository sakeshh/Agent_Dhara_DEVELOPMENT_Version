from __future__ import annotations
import html
import re

def escape_html_value(value: Any) -> str:
    """Escape string values to prevent HTML injection in report pages."""
    if value is None:
        return ""
    return html.escape(str(value))

def sanitize_csv_cell(value: Any) -> Any:
    """
    Sanitize cell value to prevent CSV Formula Injection (Formula Injection / CSV Injection).
    Prefixes the cell with a single quote if it begins with dangerous characters (=, +, -, @, Tab, CR).
    """
    if value is None:
        return value
    val_str = str(value)
    if val_str and val_str[0] in ('=', '+', '-', '@', '\t', '\r'):
        return f"'{val_str}"
    return value

def redact_paths(path: Optional[str]) -> str:
    """
    Redact local filesystem paths and ABFSS/Azure tokens from path URIs to protect system names.
    """
    if not path:
        return ""
    # Redact local windows/unix users
    path_clean = re.sub(r'([cC]:[\\/]Users[\\/])[^\\/]+', r'\1[REDACTED_USER]', path)
    path_clean = re.sub(r'(/home/)[^/]+', r'\1[REDACTED_USER]', path_clean)
    
    # Redact query string credentials / SAS tokens
    if "?" in path_clean:
        parts = path_clean.split("?", 1)
        path_clean = f"{parts[0]}?[REDACTED_CREDENTIALS]"
        
    return path_clean
