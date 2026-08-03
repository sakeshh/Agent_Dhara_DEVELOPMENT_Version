from __future__ import annotations
from typing import Any, Dict, Optional

def validate_approval_policy(
    policy: Dict[str, Any],
    actor: str,
    action_type: str
) -> bool:
    """
    Validate if a proposed action is approved under the current governance policy.
    Checks if the actor has the required roles for action_type (e.g., 'remediate', 'waiver_approve').
    """
    if not policy or not actor:
        return False
        
    allowed_actors = policy.get("allowed_actors") or {}
    actors_list = allowed_actors.get(action_type) or []
    
    return "*" in actors_list or actor in actors_list
