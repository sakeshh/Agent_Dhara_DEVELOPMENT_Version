from typing import Any, Dict, List, Tuple, Optional
from agent.etl_pipeline.rule_provenance import TaggedRule, RuleConflict, RuleProvenance

def _has_potential_conflicts(rules: List[TaggedRule]) -> bool:
    seen = set()
    for r in rules:
        k = (r.dataset or "", r.column or "", r.issue_type or "")
        if k in seen:
            return True
        seen.add(k)
    return False


def detect_conflicts(rules: List[TaggedRule]) -> Tuple[List[TaggedRule], List[RuleConflict]]:
    """
    Groups TaggedRules by (dataset, column, issue_type).
    - If rules have same action across levels, resolve to highest priority rule.
    - If rules suggest different actions, create a RuleConflict, resolve with highest priority,
      and flag it.
    
    Returns:
      (resolved_rules, conflicts)
    """
    if not _has_potential_conflicts(rules):
        return rules, []

    # Group by key: (dataset, column, issue_type)
    groups: Dict[Tuple[str, str, str], List[TaggedRule]] = {}
    for r in rules:
        key = (r.dataset or "", r.column or "", r.issue_type or "")
        groups.setdefault(key, []).append(r)
        
    resolved: List[TaggedRule] = []
    conflicts: List[RuleConflict] = []
    
    for key, group_rules in groups.items():
        dataset, column, issue_type = key
        # Find unique actions in the group
        actions = {r.action for r in group_rules}
        
        # Sort group by provenance priority (lowest IntEnum value has highest priority)
        # So BUSINESS_RULE(1) < SEMANTIC_LAYER(2) < AUTO_DETECTED(3)
        sorted_rules = sorted(group_rules, key=lambda x: x.provenance)
        highest_priority_rule = sorted_rules[0]
        
        if len(actions) == 1:
            # Same action across all layers, no conflict.
            # We just take the highest priority rule as the resolved rule.
            resolved.append(highest_priority_rule)
        else:
            # Different actions suggested -> conflict!
            # Auto-resolved using highest priority rule.
            conflict = RuleConflict(
                dataset=dataset,
                column=column,
                issue_type=issue_type,
                rules=group_rules,
                auto_resolved=True,
                resolution=highest_priority_rule.action
            )
            conflicts.append(conflict)
            resolved.append(highest_priority_rule)
            
    return resolved, conflicts
