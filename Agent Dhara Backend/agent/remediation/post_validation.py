from __future__ import annotations
import pandas as pd
from typing import Any, Dict, List, Tuple
from agent.remediation.action_contract import RemediationAction

def run_post_remediation_validation(
    original_issues: List[Dict[str, Any]],
    remediated_issues: List[Dict[str, Any]],
    actions: List[RemediationAction]
) -> Tuple[bool, Dict[str, Any]]:
    """
    Compare issue sets before and after remediation execution to check for resolution
    and verify no regressions occurred.
    """
    original_types = {i.get("type") or i.get("issue_type") for i in original_issues}
    remediated_types = {i.get("type") or i.get("issue_type") for i in remediated_issues}
    
    # Check if target issues are resolved
    resolved = []
    regressions = []
    
    for act in actions:
        if act.issue_type not in remediated_types:
            resolved.append(act.issue_type)
            
    # Check for new issue types introduced
    new_issues = remediated_types - original_types
    if new_issues:
        for ni in new_issues:
            regressions.append(f"New regression issue introduced: {ni}")
            
    ok = len(regressions) == 0
    
    return ok, {
        "ok": ok,
        "resolved_issue_types": resolved,
        "regressions": regressions,
        "pre_remediation_issues_count": len(original_issues),
        "post_remediation_issues_count": len(remediated_issues)
    }
