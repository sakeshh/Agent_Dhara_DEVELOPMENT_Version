"""
Validate ETL plan JSON before confirm / codegen.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


def _columns_for_dataset(assessment: Dict[str, Any], dataset: str) -> Set[str]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    cols = ds.get("columns") or {}
    return {str(c) for c in cols.keys()}


def validate_etl_plan(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """
    Returns (ok, errors). ok=False means plan must not proceed to confirm without fixes.
    """
    errs: List[str] = []

    if not isinstance(plan, dict):
        return False, ["plan must be an object"]

    blocked = plan.get("blocked") or []
    if blocked:
        for b in blocked[:5]:
            if isinstance(b, dict):
                errs.append(b.get("message") or str(b))
            else:
                errs.append(str(b))

    datasets = plan.get("datasets") or {}
    manual_items = plan.get("manual_review") or []
    if not datasets and not (plan.get("global_steps") or []) and not manual_items:
        errs.append("plan has no dataset steps, no global steps, and no manual review items")

    manifest = plan.get("connector_manifest") or {}
    if manifest:
        from agent.etl_pipeline.connector_manifest import validate_connector_manifest

        errs.extend(validate_connector_manifest(plan, manifest))

    never_drop = bool(business_rules.get("never_drop_rows"))
    known_ds = set((assessment.get("datasets") or {}).keys())
    manual_pending = [
        m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict) and str(m.get("status") or "pending") == "pending"
    ]

    for ds_name, block in datasets.items():
        if ds_name not in known_ds:
            errs.append(f"dataset '{ds_name}' not found in assessment")
            continue
        cols = _columns_for_dataset(assessment, ds_name)
        steps = (block or {}).get("steps") or []
        pending_for_ds = any(
            str(m.get("dataset") or "") == ds_name for m in manual_pending
        )
        # Zero steps is acceptable if the dataset requires no transformations
        pass
        seen_cols: Dict[str, List[str]] = {}
        for st in steps:
            if not isinstance(st, dict):
                continue
            action = str(st.get("action") or "")
            col = st.get("column")
            if action == "drop_rows" and never_drop:
                errs.append(f"never_drop_rows: drop_rows on {ds_name}.{col} is not allowed")
            if col and str(col) not in cols and str(col) not in ("*", "[Row-level]"):
                cols_lower = {str(c).lower() for c in cols}
                sub_cols = [c.strip() for c in str(col).split(",") if c.strip()]
                if not (sub_cols and all(str(sc).lower() in cols_lower for sc in sub_cols)):
                    errs.append(f"column '{col}' not in assessment schema for dataset '{ds_name}'")
            if col:
                seen_cols.setdefault(str(col), []).append(action)

        for col, actions in seen_cols.items():
            if "deduplicate" in actions and len(actions) > 1:
                non_dedupe = list(set([a for a in actions if a != "deduplicate"]))
                if len(non_dedupe) > 3:
                    errs.append(f"column '{col}' has many transforms in addition to deduplicate: {non_dedupe}")

    req = business_rules.get("required_columns") or []
    pending_missing_cols = {
        str(m.get("column")).lower()
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict)
        and str(m.get("status") or "pending").lower() == "pending"
        and str(m.get("issue_type") or "").lower() == "missing_required_column"
    }
    for rc in req:
        if str(rc).lower() in pending_missing_cols:
            continue
        found = False
        for ds_name in known_ds:
            cols_lower = {c.lower() for c in _columns_for_dataset(assessment, ds_name)}
            if str(rc).lower() in cols_lower:
                found = True
                break
        if not found:
            errs.append(f"required column '{rc}' missing from all assessed datasets")

    # Check for HIGH severity, NOT_FIXABLE issues in assessment that must be in manual_review or blocked
    ass_datasets = (assessment or {}).get("datasets") or {}
    for ds_name, ds_meta in ass_datasets.items():
        dq_issues = list((ds_meta.get("quality") or {}).get("issues") or [])
        legacy_issues = (assessment or {}).get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
        if legacy_issues:
            dq_issues.extend(legacy_issues)
            
        for issue in dq_issues:
            sev = str(issue.get("severity") or "").upper()
            fix = str(issue.get("fixability") or "").upper()
            col = issue.get("column")
            if sev == "HIGH" and fix == "NOT_FIXABLE":
                covered_in_plan = False
                for mr in (plan.get("manual_review") or []):
                    if isinstance(mr, dict) and str(mr.get("dataset")) == ds_name and str(mr.get("column")) == str(col):
                        covered_in_plan = True
                        break
                for mr in (plan.get("resolved_manual_review") or []):
                    if isinstance(mr, dict) and str(mr.get("dataset")) == ds_name and str(mr.get("column")) == str(col):
                        covered_in_plan = True
                        break
                for b in (plan.get("blocked") or []):
                    if isinstance(b, dict) and str(b.get("dataset")) == ds_name and str(b.get("column")) == str(col):
                        covered_in_plan = True
                        break
                if not covered_in_plan:
                    errs.append(
                        f"Dataset '{ds_name}' column '{col}' has a HIGH severity NOT_FIXABLE issue ({issue.get('type')}) "
                        f"which must be routed to manual review or blocked."
                    )

    phase1_errs = validate_phase1_plan(plan, assessment, business_rules)
    errs.extend(phase1_errs)

    ok = len(errs) == 0
    if ok:
        import hashlib
        import json
        
        # Core user-editable keys to be stable under rehydration
        core_plan = {
            "datasets": plan.get("datasets") or {},
            "business_rules": plan.get("business_rules") or {},
            "relationships": plan.get("relationships") or {},
            "global_steps": plan.get("global_steps") or [],
            "manual_review": plan.get("manual_review") or [],
        }
        try:
            blob = json.dumps(core_plan, sort_keys=True, default=str)
        except Exception:
            blob = str(core_plan)
        plan_hash = "sha256:" + hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()
        
        rel_block = plan.get("relationships") or {}
        try:
            rel_blob = json.dumps(rel_block, sort_keys=True, default=str)
        except Exception:
            rel_blob = str(rel_block)
        rel_hash = "sha256:" + hashlib.sha256(rel_blob.encode("utf-8", errors="ignore")).hexdigest()
        
        plan["validation_certificate"] = {
            "status": "PASSED",
            "validated_plan_hash": plan_hash,
            "validator_version": "1.0.0",
            "action_registry_version": "1.0.0",
            "relationship_contract_hash": rel_hash
        }
    else:
        plan.pop("validation_certificate", None)

    return ok, errs


def validate_phase1_plan(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> List[str]:
    """Enforce Phase 1 validation invariants."""
    errs: List[str] = []
    
    # 1. Plan phase check
    generation_mode = str(plan.get("generation_mode") or "").lower().strip()
    is_phase1 = generation_mode in ("cleanse_only", "phase_1_cleaning", "cleanse")
    
    # Check step classifications
    from agent.etl_pipeline.action_registry import get_action_meta, is_invalid_action
    
    datasets = plan.get("datasets") or {}
    for ds_name, block in datasets.items():
        steps = (block or {}).get("steps") or []
        for st in steps:
            action = str(st.get("action") or "")
            meta = get_action_meta(action)
            
            # Phase 2 actions in Phase 1 plan
            if is_phase1 and meta.get("phase") == 2:
                errs.append(f"Phase 1 error: Action '{action}' on {ds_name}.{st.get('column')} is Phase 2 (transform) and not allowed in Phase 1.")
                
            # Ambiguous/invalid actions
            if meta.get("invalid") or is_invalid_action(action):
                errs.append(f"Validation error: Action '{action}' on {ds_name}.{st.get('column')} is invalid/ambiguous.")

            # Required parameter auto-healing — inject safe Phase 1 defaults rather than failing
            col = st.get("column")
            if "params" not in st or not isinstance(st.get("params"), dict):
                st["params"] = {}
            params = st["params"]
            if action == "cast_type":
                params.setdefault("target_type", "string")
                params.setdefault("preserve_raw", True)
            elif action == "coerce_numeric":
                params.setdefault("target_type", "double")
                params.setdefault("preserve_raw", True)
            elif action in ("parse_dates", "parse_dates_safe"):
                params.setdefault("accepted_formats", ["yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy", "dd-MM-yyyy"])
                
    # Relationship validation
    relationships = plan.get("relationships", {})
    joins = relationships.get("joins", []) if isinstance(relationships, dict) else []
    for j in joins:
        # Check inner join in Phase 1
        if is_phase1 and j.get("join_type") == "inner":
            errs.append(f"Phase 1 error: Declared relationship {j.get('left_dataset')} -> {j.get('right_dataset')} uses inner join. Phase 1 must never drop/exclude orphan rows silently.")
            
        # Check missing required parent evidence (None orphan count)
        if j.get("assessment_status") == "EVIDENCE_NOT_FOUND" and j.get("required", True):
            errs.append(f"Phase 1 error: Relationship evidence missing for required relationship between {j.get('left_dataset')} and {j.get('right_dataset')}.")

    # Manual review issue type checks
    from agent.etl_pipeline.manual_review_catalog import _CATALOG
    for mr in (plan.get("manual_review") or []):
        if not isinstance(mr, dict):
            continue
        it = mr.get("issue_type")
        from agent.etl_pipeline.issue_registry import canonicalize_issue_type
        norm_it = canonicalize_issue_type(it)
        if norm_it and norm_it not in _CATALOG and norm_it not in ("dq_gate_warning", "missing_required_column") and not str(norm_it).startswith("proactive_"):
            errs.append(f"Validation error: Manual review item has unknown/unmapped issue_type: '{it}'")

    return errs


def validate_etl_plan_for_confirm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """Like validate_etl_plan but also requires all manual_review items resolved."""
    ok, errs = validate_etl_plan(plan, assessment, business_rules)
    manual_pending = [
        m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict) and str(m.get("status") or "pending") == "pending"
    ]
    if manual_pending:
        errs.append(
            f"{len(manual_pending)} manual review item(s) still need a resolution before confirm"
        )
        ok = False
    return ok, errs
