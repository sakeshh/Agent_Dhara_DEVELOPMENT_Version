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

# Maps legacy Phase 2 resolution actions to their Phase 1 safe equivalents.
# Used as a last-resort migration when a promoted action violates phase constraints.
PHASE1_LEGACY_MIGRATIONS: Dict[str, str] = {
    "deduplicate":              "flag_duplicate_candidates",
    "fill_nulls_simple":        "fill_nulls_flag",
    "fill_or_drop":             "fill_nulls_flag",
    "clip_or_flag":             "flag_outliers",
    "range_clip":               "flag_range_violation",
    "clip_outliers":            "flag_outliers",
    "cap_outliers":             "flag_outliers",
    "drop_column":              "noop",
    "drop_rows":                "noop",
    "replace_values":           "flag_domain_violation",
    "standardize_case":         "trim_preserve_case",
}

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
    assessment: Optional[Dict[str, Any]] = None,
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

        # P0-C: Phase gate — migrate Phase 2 actions to Phase 1 safe equivalents.
        # This is a last-resort safety net. The RESOLUTION_TO_ACTION_MAP and
        # catalog options should already return Phase 1 actions. If they don't,
        # we migrate rather than silently inserting an invalid action.
        from agent.etl_pipeline.action_registry import ACTION_REGISTRY
        plan_generation_mode = str(plan.get("generation_mode") or "full").lower()
        if plan_generation_mode in ("cleanse", "phase1"):
            act_meta = ACTION_REGISTRY.get(action) or {}
            if act_meta.get("phase") == 2 or act_meta.get("invalid"):
                migrated = PHASE1_LEGACY_MIGRATIONS.get(action)
                if migrated:
                    errs.append(
                        f"[Phase gate] Action '{action}' is Phase 2 — migrated to '{migrated}' "
                        f"for Phase 1 plan (resolution: {rid}, item: {iid})"
                    )
                    action = migrated
                else:
                    errs.append(
                        f"[Phase gate] Action '{action}' is Phase 2 and has no Phase 1 equivalent. "
                        f"Resolution '{rid}' cannot be promoted in Phase 1. Item skipped."
                    )
                    item["status"] = "skipped"
                    resolved_log.append({**item, "promoted": False, "skip_reason": "phase2_blocked"})
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
        # P0-D: Block synthetic/global dataset scopes — these are not real datasets
        # and will cause connector_manifest and assessment lookup failures.
        if not ds or ds in ("_global", "global"):
            errs.append(
                f"Item {iid} has synthetic global scope (dataset='{ds}'). "
                "Use RELATIONSHIP or PORTFOLIO scope for cross-dataset operations. "
                "This item cannot be promoted to a dataset step."
            )
            item["status"] = "skipped"
            resolved_log.append({**item, "promoted": False, "skip_reason": "global_scope"})
            continue

        block = datasets.setdefault(ds, {"steps": []})
        steps: List[Dict[str, Any]] = list(block.get("steps") or [])
        pri = _ACTION_PRIORITY.get(action, 80)
        res_bucket = classify_step_bucket(
            action,
            severity=str(item.get("severity") or "medium"),
            never_drop_rows=never_drop,
        )
        is_llm = (item.get("authority_source") == "LLM_ADVISORY")
        conf = item.get("llm_confidence") if is_llm else None

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
            "resolution_status": "RESOLVED",
            "execution_mode": "APPROVED_MANUAL",
            "auto_fixable": False,
            "selected_by": "user",
            "requires_impact_preview": True,
            "evidence": {
                "why_this_action": item.get("guidance") or item.get("message") or "",
                "confidence": conf if conf is not None else 0.95,
                "confidence_advisory": is_llm,
                "rule_override": True,
                "user_resolution": rid,
            },
            "message": item.get("message"),
        }
        if "llm_recommendation" in item:
            step["llm_recommendation"] = item["llm_recommendation"]
        if "llm_confidence" in item:
            step["llm_confidence"] = item["llm_confidence"]
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
        # Phase 1 safe defaults ONLY.
        # Phase 2 actions (fill_nulls_simple, deduplicate) have been removed.
        # Complex issues (null_heavy, high_cardinality) must be resolved by the user.
        "type_mismatch":    "cast_type",
        "invalid_email":    "sanitize_email",
        "invalid_phone":    "normalize_phone",
        "outlier_detected": "flag_outliers",
        "future_date":      "nullify_future_dates",
        "zero_as_null":     "zero_to_null",
        # NOTE: "null_heavy" and "high_cardinality_possible_key" removed.
        # These are complex decisions requiring explicit user resolution.
        # Auto-resolving them to fill_nulls_simple or deduplicate is Phase 2 and unsafe.
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

    # Run canonicalization on the plan steps
    if assessment:
        plan = canonicalize_promoted_plan(plan, assessment, rules)
        datasets = plan.get("datasets") or {}
    else:
        for ds_name, block in datasets.items():
            if isinstance(block, dict) and "steps" in block:
                block["steps"] = canonicalize_plan_steps(block["steps"], rules, ds_name)

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


def canonicalize_plan_steps(
    steps: List[Dict[str, Any]],
    rules: Dict[str, Any],
    ds_name: str,
) -> List[Dict[str, Any]]:
    """
    Apply canonicalization rules to compose, sort, and normalize step actions:
    1. Removes redundant or duplicate transforms of the exact same type on a column.
    2. Composes trim + trim_preserve_case -> trim_preserve_case.
    3. Maps deduplicate + duplicate_insensitive_values -> create_normalized_match_key.
    4. Removes flag_duplicate_candidates / flag_near_duplicate_cluster if create_normalized_match_key is present.
    5. Orders transforms logically.
    """
    # Group steps by column (ignoring non-column or row-level/wildcard steps)
    by_col: Dict[str, List[Dict[str, Any]]] = {}
    other_steps: List[Dict[str, Any]] = []

    for st in steps:
        if not isinstance(st, dict):
            continue
        col = st.get("column")
        if not col or str(col).lower() in ("*", "[row-level]"):
            other_steps.append(st)
        else:
            by_col.setdefault(col, []).append(st)

    out_steps = list(other_steps)

    for col, col_steps in by_col.items():
        # Remove identical duplicate actions on the same column
        seen_acts = set()
        unique_steps = []
        for st in col_steps:
            act = str(st.get("action") or "").lower()
            if act not in seen_acts:
                seen_acts.add(act)
                unique_steps.append(st)
        col_steps = unique_steps

        # Rule: deduplicate + duplicate_insensitive_values -> create_normalized_match_key
        has_div = any(
            str(st.get("source_issue_type") or "").lower() == "duplicate_insensitive_values"
            for st in col_steps
        )
        has_dedup = any(str(st.get("action") or "").lower() == "deduplicate" for st in col_steps)
        if has_dedup and has_div:
            # Map deduplicate to create_normalized_match_key to avoid row destruction
            for st in col_steps:
                if str(st.get("action") or "").lower() == "deduplicate":
                    st["action"] = "create_normalized_match_key"
                    st["note"] = "Canonicalized: replaced deduplicate with create_normalized_match_key to preserve raw rows"

        # Rule: trim + trim_preserve_case -> keep trim_preserve_case only
        has_trim = any(str(st.get("action") or "").lower() == "trim" for st in col_steps)
        has_tpc = any(str(st.get("action") or "").lower() == "trim_preserve_case" for st in col_steps)
        if has_trim and has_tpc:
            col_steps = [st for st in col_steps if str(st.get("action") or "").lower() != "trim"]

        # Rule: create_normalized_match_key + duplicate flags on text fields -> create_normalized_match_key only
        has_cnmk = any(str(st.get("action") or "").lower() == "create_normalized_match_key" for st in col_steps)
        if has_cnmk:
            col_steps = [
                st for st in col_steps 
                if str(st.get("action") or "").lower() not in ("flag_duplicate_candidates", "flag_near_duplicate_cluster", "deduplicate")
            ]

        # Sort actions logically inside the column pipeline
        order_rank = {
            "trim_preserve_case": 10,
            "trim": 11,
            "nullify_punctuation": 20,
            "replace_sentinel_values": 21,
            "create_normalized_match_key": 30,
            "cast_type": 40,
            "coerce_numeric": 41,
            "standardize_boolean": 42,
            "preserve_null": 50,
            "fill_constant": 51,
            "preserve_null_and_flag": 52,
            "impute_numeric": 53,
            "impute_date": 54,
            "flag_missing_key": 60,
            "flag_missing_declared_key": 61,
            "flag_duplicate_candidates": 62,
            "flag_near_duplicate_cluster": 63,
            "flag_business_key_duplicate": 64,
        }
        col_steps.sort(key=lambda st: order_rank.get(str(st.get("action") or "").lower(), 99))
        out_steps.extend(col_steps)

    # Sort all steps overall by default action priorities
    out_steps.sort(
        key=lambda s: (
            _ACTION_PRIORITY.get(str(s.get("action") or ""), 80),
            str(s.get("column") or ""),
        )
    )

    # Re-number order
    for i, st in enumerate(out_steps, start=1):
        st["order"] = i

    return out_steps


def canonicalize_promoted_plan(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Applies canonicalization rules to compose, sort, and normalize step actions
    across the entire plan and all datasets.
    """
    plan = dict(plan)
    datasets = dict(plan.get("datasets") or {})
    assess_datasets = assessment.get("datasets") or {}

    # Gather relationships info to identify key columns
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    parent_keys = set()
    child_keys = set()
    for j in joins:
        l_ds = j.get("left_dataset")
        r_ds = j.get("right_dataset")
        left_keys = j.get("left_keys") or [j.get("left_key")] if j.get("left_keys") or j.get("left_key") else []
        right_keys = j.get("right_keys") or [j.get("right_key")] if j.get("right_keys") or j.get("right_key") else []
        for lk in left_keys:
            if lk:
                parent_keys.add((l_ds, lk))
        for rk in right_keys:
            if rk:
                child_keys.add((r_ds, rk))

    req_cols = set(rules.get("required_columns") or [])

    for ds_name, block in list(datasets.items()):
        if not isinstance(block, dict) or "steps" not in block:
            continue
        
        steps = list(block.get("steps") or [])
        ds_meta = assess_datasets.get(ds_name) or {}
        columns_meta = ds_meta.get("columns") or {}

        new_steps = []
        for st in steps:
            if not isinstance(st, dict):
                continue
            st = dict(st)
            col = st.get("column")
            act = str(st.get("action") or "").strip().lower()

            # Handle row-level or wildcard steps
            if not col or str(col).lower() in ("*", "[row-level]", "row-level", ""):
                if act == "deduplicate":
                    params = dict(st.get("params") or {})
                    if not params.get("survivorship_policy"):
                        st["action"] = "flag_duplicate_candidates"
                        st["note"] = "Canonicalized: replaced deduplicate with flag_duplicate_candidates to preserve raw rows"
                    else:
                        params.setdefault("duplicate_type", "EXACT_FULL_ROW")
                        params.setdefault("key_columns", list(columns_meta.keys()))
                        params.setdefault("rejected_row_disposition", "QUARANTINE")
                        params.setdefault("requires_impact_preview", True)
                        params.setdefault("approved_manual", True)
                        st["params"] = params
                new_steps.append(st)
                continue

            # Check column role
            is_parent_key = (ds_name, col) in parent_keys
            is_child_key = (ds_name, col) in child_keys
            is_required = col in req_cols
            col_meta = columns_meta.get(col) or {}
            dtype = str(col_meta.get("dtype") or "").lower()

            # 1. Replace legacy/vague fill_nulls_simple or fill_or_drop
            if act in ("fill_nulls_simple", "fill_or_drop"):
                if is_parent_key:
                    st["action"] = "flag_missing_declared_key"
                    st["note"] = f"Canonicalized: flag null parent key for {col}"
                elif is_child_key or is_required:
                    st["action"] = "flag_missing_key"
                    st["note"] = f"Canonicalized: flag null required key for {col}"
                elif any(x in dtype for x in ("int", "decimal", "float", "double", "numeric")):
                    st["action"] = "preserve_null_and_flag"
                    st["note"] = f"Canonicalized: preserve null and flag for measure {col}"
                elif any(x in dtype for x in ("date", "time")):
                    st["action"] = "preserve_null_and_flag"
                    st["note"] = f"Canonicalized: preserve null and flag for date {col}"
                else:
                    st["action"] = "preserve_null"
                    st["note"] = f"Canonicalized: preserve null for optional text {col}"
                act = st["action"]

            # 2. Replace vague cast_type
            if act == "cast_type" and not st.get("params", {}).get("target_type"):
                if any(x in dtype for x in ("int", "decimal", "float", "double", "numeric")):
                    st["action"] = "coerce_numeric"
                    st["params"] = {
                        "target_type": "decimal(18,2)",
                        "preserve_raw": True,
                        "output_column": f"{col}_clean"
                    }
                    st["note"] = f"Canonicalized: coerce {col} to numeric"
                elif any(x in dtype for x in ("date", "time")):
                    st["action"] = "parse_dates"
                    st["params"] = {
                        "preserve_raw": True,
                        "output_column": f"{col}_clean",
                        "accepted_formats": ["yyyy-MM-dd"]
                    }
                    st["note"] = f"Canonicalized: parse {col} as dates"
                elif any(x in col.lower() for x in ("location", "region", "channel")):
                    st["action"] = "flag_domain_violation"
                    st["note"] = f"Canonicalized: flag domain violation for categorical {col}"
                else:
                    st["params"] = {
                        "target_type": "string",
                        "preserve_raw": True,
                        "output_column": f"{col}_clean",
                        "failure_disposition": "FLAG"
                    }
                act = st["action"]

            # 3. Param injections for coerce_numeric, parse_dates, cast_type, create_normalized_match_key
            if act == "coerce_numeric":
                params = dict(st.get("params") or {})
                params.setdefault("target_type", "decimal(18,2)")
                params.setdefault("preserve_raw", True)
                params.setdefault("output_column", f"{col}_clean")
                st["params"] = params
            elif act == "parse_dates":
                params = dict(st.get("params") or {})
                params.setdefault("preserve_raw", True)
                params.setdefault("output_column", f"{col}_clean")
                params.setdefault("accepted_formats", ["yyyy-MM-dd"])
                st["params"] = params
            elif act == "cast_type":
                params = dict(st.get("params") or {})
                params.setdefault("target_type", "string")
                params.setdefault("preserve_raw", True)
                params.setdefault("output_column", f"{col}_clean")
                params.setdefault("failure_disposition", "FLAG")
                st["params"] = params
            elif act == "create_normalized_match_key":
                params = dict(st.get("params") or {})
                params.setdefault("output_column", f"{col}_match_key")
                st["params"] = params

            # 4. Safe defaults on deduplicate for text columns
            if act == "deduplicate":
                if any(x in col.lower() for x in ("name", "customer", "machine")):
                    st["action"] = "create_normalized_match_key"
                    st["params"] = {"output_column": f"{col}_match_key"}
                    st["note"] = f"Canonicalized: map duplicate text values in {col} to match key instead of row deduplication"
                    act = "create_normalized_match_key"
                else:
                    params = dict(st.get("params") or {})
                    if not params.get("survivorship_policy"):
                        st["action"] = "flag_duplicate_candidates"
                        st["note"] = f"Canonicalized: replace deduplicate on {col} with duplicate candidate flagging"
                        act = "flag_duplicate_candidates"
                    else:
                        params.setdefault("duplicate_type", "EXACT_FULL_ROW")
                        params.setdefault("key_columns", [col])
                        params.setdefault("rejected_row_disposition", "QUARANTINE")
                        params.setdefault("requires_impact_preview", True)
                        params.setdefault("approved_manual", True)
                        st["params"] = params

            new_steps.append(st)

        # Apply grouping, pipeline composition, and deduplication of steps
        block["steps"] = canonicalize_plan_steps(new_steps, rules, ds_name)
        datasets[ds_name] = block

    plan["datasets"] = datasets
    return plan


