"""
manual_review_grouper.py — Collapse individual manual review items into policy-level decisions.
"""
from __future__ import annotations

from typing import Any, Dict, List

POLICY_DEFINITIONS = {
    "POLICY_PLACEHOLDER": {
        "label": "Normalize placeholder values to null",
        "description": "Standardize non-semantic/empty values (e.g. '?', 'N/A', 'UNKNOWN') as database NULLs.",
        "issue_types": {"placeholder_detected", "punctuation_only_value", "sentinel_numeric_value"},
    },
    "POLICY_DECLARED_KEY_NULL": {
        "label": "Disposition of nulls in mandatory business keys",
        "description": "Specify handling for rows where a designated business key (e.g. campaign_id) is NULL.",
        "issue_types": {"declared_key_null_violation"},
    },
    "POLICY_SUSPECTED_KEY_NULL": {
        "label": "Disposition of nulls in suspected identifiers",
        "description": "Null percentage on identifier columns exceeds threshold. Imputation is forbidden.",
        "issue_types": {"high_null_percentage"}, # we refine this by checking semantic_type/column name
    },
    "POLICY_CAMPAIGN_RI": {
        "label": "Referential integrity and orphan partitioning",
        "description": "Resolve child foreign keys (e.g. sale campaign ID) not present in parent reference dataset.",
        "issue_types": {"orphan_foreign_keys", "orphan_foreign_key_rows", "referential_integrity_violation"},
    },
    "POLICY_NUMERIC_PARSE": {
        "label": "Numeric parsing and coercion",
        "description": "Coerce text-encoded numeric representations (e.g. budget, amount) into canonical numeric fields.",
        "issue_types": {"invalid_numeric_values", "numeric_value_serialized_as_string", "mixed_scalar_types"},
    },
    "POLICY_DATE_PARSE": {
        "label": "Date standardisation and parsing",
        "description": "Parse date strings in various formats (e.g. start_date, sale_date) into ISO-compliant dates.",
        "issue_types": {"invalid_date_format", "mixed_date_formats", "date_format_inconsistency"},
    },
    "POLICY_CATEGORICAL_MAP": {
        "label": "Categorical domain value mapping",
        "description": "Resolve categorical invalid categories or enum violations against static domain schemas.",
        "issue_types": {"invalid_category", "enum_violation"},
    },
    "POLICY_EXACT_DEDUP": {
        "label": "Exact duplicates disposition",
        "description": "Identify and flag/deduplicate rows that are byte-for-byte exact matches.",
        "issue_types": {"exact_duplicate_rows", "duplicate_primary_key"},
    },
    "POLICY_NEAR_DEDUP": {
        "label": "Near-duplicates cluster resolution",
        "description": "Review rows sharing high textual/numeric similarity and choose a grouping policy.",
        "issue_types": {"near_duplicate_rows"},
    },
    "POLICY_NEGATIVE_AMOUNT": {
        "label": "Negative financial amount flags",
        "description": "Financial values (amount, budget) fall below zero. Flag for policy compliance review.",
        "issue_types": {"negative_values", "cross_field_non_negative"},
    },
}

def group_manual_review_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group manual review items into policy cards."""
    grouped = []
    
    # Track which items have been collapsed
    categorized_ids = set()
    
    # Initialize groups
    policy_buckets: Dict[str, List[Dict[str, Any]]] = {pid: [] for pid in POLICY_DEFINITIONS.keys()}
    other_items = []
    
    for item in items:
        if not isinstance(item, dict):
            continue
        
        item_id = item.get("id") or str(item)
        it = str(item.get("issue_type") or "").strip()
        col = str(item.get("column") or "").lower()
        
        # 1. Special case: suspected key nulls vs normal description high null percentage
        if it == "high_null_percentage":
            is_key = "id" in col or "key" in col or "code" in col or "ref" in col or "num" in col
            if is_key:
                policy_buckets["POLICY_SUSPECTED_KEY_NULL"].append(item)
                categorized_ids.add(item_id)
                continue
        
        # Find matching policy definitions
        matched = False
        for pid, defn in POLICY_DEFINITIONS.items():
            if it in defn["issue_types"]:
                policy_buckets[pid].append(item)
                categorized_ids.add(item_id)
                matched = True
                break
                
        if not matched:
            other_items.append(item)
            
    # Collapse groups into policy cards
    for pid, collapsed in policy_buckets.items():
        if not collapsed:
            continue
            
        defn = POLICY_DEFINITIONS[pid]
        
        # Use first item's options as default policy resolution options
        # or merge them safely
        options = collapsed[0].get("options") or []
        
        policy_card = {
            "id": pid,
            "policy_id": pid,
            "label": defn["label"],
            "description": defn["description"],
            "options": options,
            "collapsed_items": collapsed,
            "status": "pending" if any(item.get("status", "pending") == "pending" for item in collapsed) else "resolved",
        }
        grouped.append(policy_card)
        
    # Add other non-grouped items as single-item policy cards
    for item in other_items:
        grouped.append({
            "id": item.get("id"),
            "policy_id": "POLICY_INDIVIDUAL",
            "label": f"Individual Item: {item.get('issue_type') or 'General Issue'}",
            "description": item.get("message") or "Manual review required.",
            "options": item.get("options") or [],
            "collapsed_items": [item],
            "status": item.get("status") or "pending",
        })
        
    return grouped
