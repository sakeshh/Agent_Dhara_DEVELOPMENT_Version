from __future__ import annotations
from typing import Any, Dict, List, Optional
from agent.remediation.action_contract import RemediationAction

def plan_remediations(
    dq_result: Dict[str, Any],
    mode: str = "RECOMMEND_ONLY",
    safe_deduplication: bool = False
) -> List[RemediationAction]:
    """
    Plan remediation actions for data quality findings based on issue fixability.
    """
    plans = []
    datasets = dq_result.get("datasets") or {}
    
    for ds_name, block in datasets.items():
        issues = block.get("issues") or []
        for issue in issues:
            issue_type = issue.get("type") or issue.get("issue_type")
            col = issue.get("column")
            fixability_dt = issue.get("fixability_details") or {}
            fixability = fixability_dt.get("fixability")
            
            # Action planning rules
            if not fixability:
                continue
                
            # If conditionally fixable but it's exact_duplicate_rows, we require safe_deduplication flag
            if fixability == "CONDITIONALLY_FIXABLE" and issue_type == "exact_duplicate_rows" and not safe_deduplication:
                continue
                
            # Only planning for AUTO_FIXABLE or approved CONDITIONALLY_FIXABLE
            if fixability == "AUTO_FIXABLE" or (fixability == "CONDITIONALLY_FIXABLE" and safe_deduplication):
                action = RemediationAction(
                    mode=mode,
                    dataset=ds_name,
                    column=col or "",
                    issue_type=issue_type,
                    before_invalid_count=issue.get("count") or len(issue.get("row_indexes") or []),
                    details={
                        "fixability": fixability,
                        "row_indexes": issue.get("row_indexes") or []
                    }
                )
                plans.append(action)
                
    return plans
