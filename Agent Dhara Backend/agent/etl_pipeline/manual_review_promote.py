"""
Promote user-selected manual review resolutions into executable plan steps.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from agent.etl_pipeline.classify_steps import classify_step_bucket
from agent.etl_pipeline.manual_review_catalog import (
    action_for_resolution,
    enrich_manual_review_item,
    is_skip_action,
    manual_review_item_id,
)

_ACTION_PRIORITY: Dict[str, int] = {
    "trim": 5,
    "lowercase": 8,
    "uppercase": 8,
    "zero_to_null": 30,
    "replace_sentinel_values": 31,
    "cast_type": 35,
    "coerce_numeric": 40,
    "parse_dates": 45,
    "at_least_one": 46,
    "nullify_future_dates": 48,
    "sanitize_email": 50,
    "normalize_phone": 55,
    "hash_phone": 56,
    "mask_phone": 57,
    "regex_replace": 60,
    "range_clip": 65,
    "clip_or_flag": 65,
    "flag_outliers": 65,
    "clip_outliers": 65,
    "cap_outliers": 65,
    "standardize_boolean": 70,
    "replace_values": 75,
    "fill_or_drop": 80,
    "fill_nulls_simple": 80,
    "drop_column": 85,
    "exclude_column": 86,
    "deduplicate": 200,
    "validate_referential_integrity_or_stage": 300,
    "noop": 999,
}


def _pending_items(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    manual = plan.get("manual_review") or []
    out: List[Dict[str, Any]] = []
    for m in manual:
        if not isinstance(m, dict):
            continue
        st = str(m.get("status") or "pending").lower()
        if st == "pending":
            out.append(m)
    return out


def count_pending_manual_review(plan: Dict[str, Any]) -> int:
    return len(_pending_items(plan))


def apply_manual_resolutions(
    plan: Dict[str, Any],
    resolutions: List[Dict[str, Any]],
    *,
    business_rules: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply user resolution picks. Each resolution: { item_id, resolution_id }.
    Returns (updated_plan, errors).
    """
    rules = business_rules or plan.get("business_rules") or {}
    never_drop = bool(rules.get("never_drop_rows"))
    errs: List[str] = []
    plan = dict(plan)
    manual: List[Dict[str, Any]] = [
        enrich_manual_review_item(m) if isinstance(m, dict) else m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict)
    ]
    by_id = {str(m.get("id")): m for m in manual}

    res_map: Dict[str, str] = {}
    for r in resolutions or []:
        if not isinstance(r, dict):
            continue
        iid = str(r.get("item_id") or r.get("id") or "").strip()
        rid = str(r.get("resolution_id") or r.get("resolution") or "").strip()
        if iid and rid:
            res_map[iid] = rid

    datasets: Dict[str, Any] = dict(plan.get("datasets") or {})
    resolved_log: List[Dict[str, Any]] = list(plan.get("resolved_manual_review") or [])

    for iid, rid in res_map.items():
        item = by_id.get(iid)
        if not item:
            errs.append(f"Unknown manual review item: {iid}")
            continue
        if str(item.get("status") or "pending") != "pending":
            errs.append(f"Item already resolved: {iid}")
            continue

        issue_type = str(item.get("issue_type") or "")
        action = action_for_resolution(issue_type, rid, item.get("resolution_options"))
        if not action:
            errs.append(f"No action for resolution '{rid}' on issue '{issue_type}'")
            continue

        item["selected_resolution"] = rid
        item["resolved_action"] = action
        col = item.get("column")

        if action == "skip_requirement":
            req_cols = rules.get("required_columns") or []
            rules["required_columns"] = [rc for rc in req_cols if str(rc).lower() != str(col).lower()]
            plan["business_rules"] = rules
            item["status"] = "resolved"
            resolved_log.append({**item, "promoted": True})
            continue

        if action == "force_unlock":
            ds = str(item.get("dataset") or "").strip()
            if ds:
                force_list = list(rules.get("force_unlock") or [])
                if ds not in force_list:
                    force_list.append(ds)
                rules["force_unlock"] = force_list
                plan["business_rules"] = rules
            item["status"] = "resolved"
            resolved_log.append({**item, "promoted": True})
            continue

        if is_skip_action(action):
            item["status"] = "skipped"
            resolved_log.append({**item, "promoted": False})
            continue

        ds = str(item.get("dataset") or "").strip()
        col = item.get("column")
        if not ds or ds == "_global":
            errs.append(f"Cannot promote dataset-less item {iid}")
            continue

        block = datasets.setdefault(ds, {"steps": []})
        steps: List[Dict[str, Any]] = list(block.get("steps") or [])
        pri = _ACTION_PRIORITY.get(action, 80)
        res_bucket = classify_step_bucket(
            action,
            severity=str(item.get("severity") or "medium"),
            never_drop_rows=never_drop,
        )
        step = {
            "order": len(steps) + 1,
            "column": col,
            "action": action,
            "bucket": res_bucket["bucket"],
            "phase": res_bucket["phase"],
            "source_issue_type": issue_type,
            "severity": item.get("severity") or "medium",
            "priority": pri,
            "note": f"User-selected resolution: {rid}",
            "evidence": {
                "why_this_action": item.get("guidance") or item.get("message") or "",
                "confidence": 0.95,
                "rule_override": True,
                "user_resolution": rid,
            },
            "message": item.get("message"),
        }
        steps.append(step)
        steps.sort(key=lambda s: (_ACTION_PRIORITY.get(str(s.get("action") or ""), 80), str(s.get("column") or "")))
        for i, st in enumerate(steps, start=1):
            st["order"] = i
        block["steps"] = steps
        datasets[ds] = block

        item["status"] = "resolved"
        resolved_log.append({**item, "promoted": True})

    plan["datasets"] = datasets

    # After processing all resolutions, handle non_fixable tier items
    # that were resolved with quarantine/flag strategy
    nf_resolutions = []
    for iid, rid in res_map.items():
        item = by_id.get(iid)
        if not item:
            continue
        if item.get("risk_tier") == "non_fixable":
            action = action_for_resolution(item.get("issue_type"), rid, item.get("resolution_options"))
            if action and action != "noop":
                nf_resolutions.append({
                    "dataset": item.get("dataset"),
                    "column": item.get("column"),
                    "strategy": rid,
                    "user_note": f"User acknowledged non-fixable: {rid}",
                })
    
    if nf_resolutions:
        plan = promote_non_fixable_resolutions(plan, nf_resolutions)
        datasets = plan.get("datasets") or {}

    # ── AUTO-RESOLVE: safe defaults for any items the user did not act on ──
    import logging as _logging
    _auto_logger = _logging.getLogger("agent.etl_pipeline.manual_review_promote")
    DEFAULT_AUTO_RESOLUTION = {
        "null_heavy":                    "fill_nulls_simple",
        "high_cardinality_possible_key": "deduplicate",
        "type_mismatch":                 "cast_type",
        "invalid_email":                 "sanitize_email",
        "invalid_phone":                 "normalize_phone",
        "outlier_detected":              "flag_outliers",
        "future_date":                   "nullify_future_dates",
        "zero_as_null":                  "zero_to_null",
    }

    for item in manual:
        if str(item.get("status") or "pending") != "pending":
            continue
        issue_type = str(item.get("issue_type") or "")
        default_action = DEFAULT_AUTO_RESOLUTION.get(issue_type)
        if not default_action:
            _auto_logger.warning(
                f"[ManualReview] No auto-default for issue_type={issue_type!r} "
                f"on {item.get('dataset')}.{item.get('column')} — item left pending"
            )
            continue
        ds = str(item.get("dataset") or "").strip()
        if not ds or ds == "_global":
            continue
        block = datasets.setdefault(ds, {"steps": []})
        steps = list(block.get("steps") or [])
        pri = _ACTION_PRIORITY.get(default_action, 80)
        res_bucket = classify_step_bucket(
            default_action,
            severity=str(item.get("severity") or "medium"),
            never_drop_rows=never_drop,
        )
        steps.append({
            "order": len(steps) + 1,
            "column": item.get("column"),
            "action": default_action,
            "bucket": res_bucket["bucket"],
            "phase": res_bucket["phase"],
            "priority": pri,
            "note": f"Auto-resolved (user did not act): default={default_action}",
            "evidence": {
                "why_this_action": "System default — user did not provide resolution",
                "confidence": 0.70,
                "rule_override": False,
                "auto_resolved": True,
            },
            "severity": item.get("severity") or "medium",
            "source_issue_type": issue_type,
        })
        steps.sort(
            key=lambda s: (_ACTION_PRIORITY.get(str(s.get("action") or ""), 80),
                           str(s.get("column") or ""))
        )
        for i, st in enumerate(steps, 1):
            st["order"] = i
        block["steps"] = steps
        datasets[ds] = block
        item["status"] = "auto_resolved"
        item["selected_resolution"] = default_action
        item["resolved_action"] = default_action
        resolved_log.append({**item, "promoted": True, "auto_resolved": True})

    plan["datasets"] = datasets   # ← update datasets dict before pending_only filter

    pending_only = [
        enrich_manual_review_item(m)
        for m in manual
        if str(m.get("status") or "pending") not in ("resolved", "skipped", "auto_resolved")
    ]
    plan["manual_review"] = pending_only
    
    existing_resolved = {x["id"]: x for x in (plan.get("resolved_manual_review") or []) if isinstance(x, dict)}
    for item in resolved_log:
        existing_resolved[item["id"]] = item

    plan["resolved_manual_review"] = list(existing_resolved.values())
    return plan, errs


def enrich_plan_manual_review(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every manual_review entry has catalog options."""
    manual = plan.get("manual_review") or []
    enriched = [enrich_manual_review_item(m) for m in manual if isinstance(m, dict)]
    plan = dict(plan)
    plan["manual_review"] = enriched
    return plan


def promote_non_fixable_resolutions(
    plan: Dict[str, Any],
    non_fixable_resolutions: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Merge user-decided resolutions for non-fixable issues into the ETL plan steps.
    Each resolution becomes a real step in plan.datasets[ds].steps.
    """
    resolution_step_map = {
        "quarantine":          {"action": "validate_referential_integrity_or_stage", "params": {"enforcement_mode": "quarantine"}},
        "quarantine_all_null": {"action": "validate_referential_integrity_or_stage", "params": {"enforcement_mode": "quarantine"}},
        "reject_orphans":      {"action": "validate_referential_integrity_or_stage", "params": {"enforcement_mode": "quarantine"}},
        "validate_referential_integrity_or_stage": {"action": "validate_referential_integrity_or_stage", "params": {"enforcement_mode": "quarantine"}},
        "flag":                {"action": "clip_or_flag",    "params": {"outlier_method": "flag"}},
        "flag_outliers":       {"action": "clip_or_flag",    "params": {"outlier_method": "flag"}},
        "clip_or_flag":        {"action": "clip_or_flag",    "params": {"outlier_method": "flag"}},
        "fill_null":           {"action": "fill_or_drop",    "params": {"fill_strategy": "value", "fill_value": None}},
        "fill_nulls":          {"action": "fill_or_drop",    "params": {"fill_strategy": "value", "fill_value": None}},
        "fill_nulls_simple":   {"action": "fill_or_drop",    "params": {"fill_strategy": "value", "fill_value": None}},
        "fill_or_drop":        {"action": "fill_or_drop",    "params": {"fill_strategy": "value", "fill_value": None}},
        "accept_risk":         None,
        "custom":              None,
    }
    
    updated_plan = dict(plan)
    datasets = dict(plan.get("datasets") or {})
    blocked = list(plan.get("blocked") or [])
    
    for res in (non_fixable_resolutions or []):
        ds = res.get("dataset")
        col = res.get("column")
        strategy = res.get("strategy") or "accept_risk"
        
        if not ds or ds not in datasets:
            continue
        
        step_template = resolution_step_map.get(strategy)
        if step_template is None:
            # accept_risk → add to blocked (documented)
            blocked.append({
                "dataset": ds,
                "column": col,
                "reason": "non_fixable_accepted_risk",
                "user_note": res.get("user_note", ""),
                "acknowledged_at": res.get("timestamp"),
            })
            continue
        
        # Build step and append to dataset steps
        ds_block = dict(datasets[ds])
        steps = list(ds_block.get("steps") or [])
        max_order = max((s.get("order", 0) for s in steps), default=0)
        new_step = {
            "order": max_order + 1,
            "column": col,
            "action": step_template["action"],
            "bucket": "cleanse",
            "phase": "cleanse",
            "priority": 80,
            "params": dict(step_template["params"]),
            "note": f"User resolution: {strategy}. {res.get('user_note', '')}",
            "source": "non_fixable_user_resolution",
        }
        steps.append(new_step)
        ds_block["steps"] = steps
        datasets[ds] = ds_block
    
    updated_plan["datasets"] = datasets
    updated_plan["blocked"] = blocked
    return updated_plan

