from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from agent.governance.waiver_registry import WaiverRegistry

def apply_waivers_to_assessment(
    datasets_dq: Dict[str, Any],
    waiver_registry: WaiverRegistry,
    current_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Evaluate active waivers and apply them to data quality findings.
    If an issue is waived, it suppresses gating (gating=False) and optionally
    suppresses score impact while retaining the issue record as evidence.
    """
    now = current_time or datetime.now(timezone.utc)
    active_waivers = waiver_registry.get_active_waivers(now)
    
    # Track which waivers matched
    applied_waivers = set()
    expired_waivers = []
    
    # Track expired waivers for reporting
    for w in waiver_registry.waivers:
        expiry_str = w.get("expires_at")
        if expiry_str:
            try:
                expiry = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                if now > expiry:
                    expired_waivers.append(w.get("waiver_id"))
            except Exception:
                pass

    datasets = datasets_dq.get("datasets") or {}
    waived_count = 0

    for ds_name, block in datasets.items():
        issues = block.get("issues") or []
        for issue in issues:
            issue_type = issue.get("type") or issue.get("issue_type")
            col = issue.get("column")
            
            # Check active waivers
            for w in active_waivers:
                w_rule = w.get("rule_id")
                w_ds = w.get("dataset")
                
                # Check match criteria
                rule_match = (w_rule == "*" or w_rule == issue_type)
                ds_match = (w_ds == "*" or w_ds == ds_name)
                
                if rule_match and ds_match:
                    # Apply waiver effect
                    issue["waived"] = True
                    issue["waiver_id"] = w.get("waiver_id")
                    
                    effect = w.get("effect") or {}
                    if effect.get("suppress_gating", True):
                        issue["gating"] = False
                    if effect.get("suppress_score_impact", False):
                        issue["score_included"] = False
                        
                    applied_waivers.add(w.get("waiver_id"))
                    waived_count += 1
                    
    # Return pre- and post-waiver meta summary properties
    datasets_dq["waiver_evaluation"] = {
        "active_waivers_applied": sorted(list(applied_waivers)),
        "expired_waivers_detected": sorted(expired_waivers),
        "waived_findings_count": waived_count
    }
    
    return datasets_dq
