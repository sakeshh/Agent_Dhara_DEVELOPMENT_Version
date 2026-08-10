from dataclasses import dataclass, asdict
from typing import Optional, Literal, Any, Dict

@dataclass
class DQIssue:
    dataset: str
    column: str
    issue_type: str
    severity: Literal["high", "medium", "low"]
    message: str
    source: Literal["gx", "profiler", "pii_scanner", "cross_field"]
    count: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

def normalize_issue_dict(raw: Dict[str, Any], default_dataset: str, default_source: str) -> Dict[str, Any]:
    dataset = str(raw.get("dataset") or default_dataset or "")
    column = str(raw.get("column") or "")
    
    issue_type = str(raw.get("issue_type") or raw.get("type") or raw.get("gx_expectation") or "unknown_issue")
    
    sev = str(raw.get("severity") or "medium").lower()
    severity: Literal["high", "medium", "low"] = "high" if sev == "high" else "low" if sev == "low" else "medium"
    
    message = str(raw.get("message") or "")
    
    source = raw.get("source") or default_source
    if source not in ("gx", "profiler", "pii_scanner", "cross_field"):
        source = "gx"
        
    count = raw.get("count") or raw.get("unexpected_count") or raw.get("business_key_duplicate_count")
    if count is not None:
        try:
            count = int(count)
        except (ValueError, TypeError):
            count = None
            
    canonical = DQIssue(
        dataset=dataset,
        column=column,
        issue_type=issue_type,
        severity=severity,
        message=message,
        source=source,
        count=count
    )
    
    res = dict(raw)
    res.update(canonical.to_dict())
    return res
