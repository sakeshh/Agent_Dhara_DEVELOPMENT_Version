from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

class InvalidIssueContract(Exception):
    """Raised when an issue dictionary does not adhere to the required DQ schema contract."""
    pass

@dataclass(frozen=True)
class DQIssue:
    issue_id: str
    issue_type: str
    dataset: str
    columns: Tuple[str, ...]
    severity: str          # "high" | "medium" | "low"
    gating: bool

    rule_id: str
    rule_source: str          # "declared" | "inferred" | "built_in"
    rule_version: str

    row_count: int
    row_indexes: Tuple[int, ...]
    samples: Tuple[Any, ...]

    fixability: str
    fixability_details: Dict[str, Any]

    score_included: bool
    score_dimension: Optional[str]  # "completeness" | "validity" | etc.

REQUIRED_FIELDS = {
    "issue_type", "severity", "gating",
    "fixability_details", "rule_source"
}

def validate_issue(issue: Dict[str, Any]) -> None:
    """Validate that the issue dictionary contains all required fields of the contract."""
    # Handle both direct dict input and dataclass object
    keys = issue.keys() if isinstance(issue, dict) else set(issue.__dict__.keys())
    missing = REQUIRED_FIELDS - keys
    if missing:
        raise InvalidIssueContract(f"Missing required fields: {sorted(missing)}")
