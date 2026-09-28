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
            params = st.setdefault("params", {})
            if action == "preserve_null_and_flag" and col:
                params.setdefault("status_column", f"{col}_validation_status")
                params.setdefault("reason_column", f"{col}_validation_reason")
            if action == "drop_rows" and never_drop:
                errs.append(f"never_drop_rows: drop_rows on {ds_name}.{col} is not allowed")
            table_level_exemptions = {"*", "[row-level]", "[table-level]", "[dataset-level]", "dataset", "table", "unknown", "none", "null", "all"}
            if col and str(col).strip().lower() in table_level_exemptions:
                st["column"] = "*"
                col = "*"
            elif col:
                # Case-insensitive column check (Bug 2 fix)
                cols_lower = {str(c).lower() for c in cols}
                col_lower = str(col).strip().lower()
                # Handle composite column references (comma-separated)
                sub_cols = [c.strip() for c in str(col).split(",") if c.strip()]
                all_parts_known = all(sc.lower() in cols_lower for sc in sub_cols) if sub_cols else (col_lower in cols_lower)
                if not all_parts_known:
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
                # Bug 3 fix: use case-insensitive comparison for dataset and column names
                col_norm = str(col).strip().lower() if col is not None else ""
                ds_norm = str(ds_name).strip().lower()
                for mr in (plan.get("manual_review") or []):
                    if not isinstance(mr, dict):
                        continue
                    mr_ds = str(mr.get("dataset") or "").strip().lower()
                    mr_col = str(mr.get("column") or "").strip().lower() if mr.get("column") is not None else ""
                    if mr_ds == ds_norm and mr_col == col_norm:
                        covered_in_plan = True
                        break
                if not covered_in_plan:
                    for mr in (plan.get("resolved_manual_review") or []):
                        if not isinstance(mr, dict):
                            continue
                        mr_ds = str(mr.get("dataset") or "").strip().lower()
                        mr_col = str(mr.get("column") or "").strip().lower() if mr.get("column") is not None else ""
                        if mr_ds == ds_norm and mr_col == col_norm:
                            covered_in_plan = True
                            break
                if not covered_in_plan:
                    for b in (plan.get("blocked") or []):
                        if not isinstance(b, dict):
                            continue
                        b_ds = str(b.get("dataset") or "").strip().lower()
                        b_col = str(b.get("column") or "").strip().lower() if b.get("column") is not None else ""
                        if b_ds == ds_norm and b_col == col_norm:
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
            "structural_status": "VALID",
            "readiness": "BLOCKED_PENDING_MANUAL_REVIEW" if manual_pending else "READY_FOR_EXECUTION",
            "codegen_eligible": (len(manual_pending) == 0),
            "validated_plan_hash": plan_hash,
            "validator_version": "1.0.0",
            "action_registry_version": "1.0.0",
            "relationship_contract_hash": rel_hash
        }
    else:
        plan.pop("validation_certificate", None)

    return ok, errs


def validate_issue_registration(raw_issue_type: str) -> Dict[str, Any]:
    """Validates an issue type against the canonical capability registry and governance catalog."""
    from agent.etl_pipeline.issue_registry import canonicalize_issue_type
    from agent.etl_pipeline.remediation_capability_registry import (
        REMEDIATION_CAPABILITY_REGISTRY,
        get_capability_entry,
    )
    from agent.etl_pipeline.manual_review_catalog import _CATALOG

    canonical_type = canonicalize_issue_type(raw_issue_type)
    is_registered = canonical_type in REMEDIATION_CAPABILITY_REGISTRY
    capability = get_capability_entry(canonical_type)

    scope = getattr(capability, "execution_scope", "field_transform")
    if scope == "configuration_change":
        exec_scope = "configuration"
    elif scope in ("validation_only", "no_op"):
        exec_scope = "validation_only"
    else:
        exec_scope = "data"

    generator_required = bool(capability.generator_ids)
    requires_manual_review = capability.approval_required or (canonical_type in _CATALOG)

    registration_status = "registered" if (
        is_registered
        or canonical_type in _CATALOG
        or canonical_type in ("dq_gate_warning", "missing_required_column")
        or str(canonical_type).startswith("proactive_")
    ) else "unknown"

    return {
        "original_issue_type": raw_issue_type,
        "canonical_issue_type": canonical_type,
        "registration_status": registration_status,
        "execution_scope": exec_scope,
        "generator_required": generator_required,
        "requires_manual_review": requires_manual_review,
    }


def normalize_manual_review_registration(manual_review_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes a manual review record before structural validation.
    Preserves original issue type and routes unregistered issues to universal manual review
    without failing structural plan validation.
    """
    item = dict(manual_review_item)
    issue_type = (
        item.get("issue_type")
        or item.get("detector_issue_type")
        or item.get("rule_type")
        or item.get("finding_name")
        or item.get("type")
        or "unspecified_issue"
    )
    issue_type_str = str(issue_type).strip() if issue_type else "unspecified_issue"
    if not issue_type_str or issue_type_str.lower() == "unknown":
        for alt_key in ("detector_issue_type", "rule_type", "finding_name", "type"):
            alt_val = item.get(alt_key)
            if alt_val and str(alt_val).lower() not in ("unknown", "none", ""):
                issue_type_str = str(alt_val).strip()
                break
        else:
            issue_type_str = "unspecified_issue"

    registration = validate_issue_registration(issue_type_str)

    item["issue_type"] = issue_type_str
    item["original_issue_type"] = issue_type_str

    if registration["registration_status"] == "unknown":
        item["planning_disposition"] = "MANUAL_REVIEW_REQUIRED"
        item["reason_code"] = "NO_APPROVED_REMEDIATION_MAPPING"
        item["execution_eligible"] = False
        item["readiness"] = "REQUIRES_HUMAN_APPROVAL"
        item["status"] = item.get("status") or "pending"
        item["catalogue_registration_status"] = "UNREGISTERED"
    else:
        item["catalogue_registration_status"] = "REGISTERED"
        item.setdefault("planning_disposition", "MANUAL_REVIEW_REQUIRED")
        item.setdefault("execution_eligible", False)
        item.setdefault("readiness", "REQUIRES_HUMAN_APPROVAL")
        item.setdefault("status", item.get("status") or "pending")

    return item


def normalize_manual_review_items(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a new normalized plan without mutating the input plan."""
    import copy
    new_plan = copy.deepcopy(plan)
    mr_list = new_plan.get("manual_review") or []
    normalized_mr = []
    for m in mr_list:
        if isinstance(m, dict):
            normalized_mr.append(normalize_manual_review_registration(m))
        else:
            normalized_mr.append(m)
    new_plan["manual_review"] = normalized_mr
    return new_plan


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
                
            # Ambiguous/invalid actions — Bug 4 fix:
            # _unknown means the action is not in the registry (e.g. canonicalized actions like
            # flag_missing_key, preserve_null). These are NOT errors — they are valid plan actions
            # added by the canonicalization layer. Only flag actions explicitly marked "invalid".
            if meta.get("invalid") or (not meta.get("_unknown") and is_invalid_action(action)):
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

    # Manual review issue type checks via canonical capability registry
    for mr in (plan.get("manual_review") or []):
        if not isinstance(mr, dict):
            continue
        it = mr.get("issue_type")
        reg = validate_issue_registration(str(it or ""))
        if reg["registration_status"] == "unknown":
            # Unregistered issue types are structurally VALID when routed to manual review.
            # Structural failure ONLY occurs if an unregistered issue is marked execution-eligible.
            if mr.get("execution_eligible", False):
                errs.append(f"Structural error: Unregistered issue '{it}' is marked execution_eligible=True.")
            if mr.get("planning_disposition") and mr.get("planning_disposition") != "MANUAL_REVIEW_REQUIRED":
                errs.append(f"Structural error: Unregistered issue '{it}' has disposition '{mr.get('planning_disposition')}'; must be MANUAL_REVIEW_REQUIRED.")

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


from dataclasses import dataclass, asdict

@dataclass
class PlanValidationIssue:
    code: str
    message: str
    dataset_id: str | None = None
    column: str | None = None
    step_id: str | None = None
    item_id: str | None = None
    policy_card_id: str | None = None
    severity: str = "ERROR"
    attempted_action: str | None = None
    attempted_params: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def is_key_or_identifier_column(ds_name: str, col: str, plan: dict, assessment: dict) -> bool:
    """Helper to detect if a column serves as a primary, foreign, business key or identifier."""
    # 1. Check relationships/joins
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    for j in joins:
        if not isinstance(j, dict):
            continue
        l_ds = j.get("left_dataset")
        r_ds = j.get("right_dataset")
        left_keys = j.get("left_keys") or [j.get("left_key")] if j.get("left_keys") or j.get("left_key") else []
        right_keys = j.get("right_keys") or [j.get("right_key")] if j.get("right_keys") or j.get("right_key") else []
        if ds_name == l_ds and col in left_keys:
            return True
        if ds_name == r_ds and col in right_keys:
            return True

    # 2. Check name patterns
    col_lower = str(col or "").lower()
    if col_lower.endswith("_id") or col_lower.endswith("_key") or col_lower in ("id", "key", "pk", "fk"):
        return True

    # 3. Check assessment metadata
    assess_ds = (assessment or {}).get("datasets", {}).get(ds_name, {})
    if isinstance(assess_ds, dict):
        col_meta = assess_ds.get("columns", {}).get(col, {})
        if isinstance(col_meta, dict):
            role = str(col_meta.get("semantic_role") or col_meta.get("role") or "").upper()
            if any(x in role for x in ("KEY", "IDENTIFIER", "PRIMARY", "FOREIGN", "BUSINESS")):
                return True

    return False


def validate_promoted_plan(
    plan: Dict[str, Any],
    assessment: Optional[Dict[str, Any]] = None,
    business_rules: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[PlanValidationIssue]]:
    """
    Fast validation gate run immediately after manual review promotion.
    Validates parameter completeness, semantic prohibitions, step identity, and checks for legacy actions.
    """
    from agent.etl_pipeline.action_registry import (
        get_action_meta,
        is_invalid_action,
        is_legacy_action,
        get_semantic_prohibitions,
    )
    from agent.etl_pipeline.manual_review_catalog import RESOLUTION_TO_ACTION_MAP

    issues: List[PlanValidationIssue] = []
    assessment = assessment or {}
    rules = business_rules or plan.get("business_rules") or {}

    generation_mode = str(plan.get("generation_mode") or "").lower().strip()
    is_phase1 = generation_mode in ("cleanse_only", "phase_1_cleaning", "cleanse", "")

    seen_step_ids = set()
    written_outputs = {}  # (ds, col) -> step_id

    datasets = plan.get("datasets") or {}
    for ds_name, block in datasets.items():
        if not isinstance(block, dict):
            continue
        steps = block.get("steps") or []
        for st in steps:
            if not isinstance(st, dict):
                continue

            action = str(st.get("action") or "")
            col = st.get("column")
            step_id = st.get("step_id")
            params = st.setdefault("params", {})
            if action == "preserve_null_and_flag" and col:
                params.setdefault("status_column", f"{col}_validation_status")
                params.setdefault("reason_column", f"{col}_validation_reason")
            policy_card_id = st.get("evidence", {}).get("user_resolution") or st.get("note")

            source_issues = st.get("source_issue_ids") or []
            item_id = source_issues[0] if (isinstance(source_issues, list) and source_issues) else (st.get("item_id") or st.get("source_issue_id"))

            def add_issue(code: str, message: str, severity: str = "ERROR") -> None:
                issues.append(PlanValidationIssue(
                    code=code,
                    message=message,
                    dataset_id=ds_name,
                    column=col,
                    step_id=step_id,
                    item_id=item_id,
                    policy_card_id=policy_card_id,
                    severity=severity,
                    attempted_action=action,
                    attempted_params=params,
                ))

            # 1. Step ID uniqueness
            if not step_id:
                add_issue("MISSING_STEP_ID", f"Step on column '{col}' is missing a step_id.")
            elif step_id in seen_step_ids:
                add_issue("DUPLICATE_STEP_ID", f"Duplicate step ID '{step_id}' found in plan.")
            else:
                seen_step_ids.add(step_id)

            # Intercept configuration actions (they belong to configuration_changes, not column ETL)
            if action in ("update_manifest", "ignore_mismatch", "acknowledge_mismatch", "reconcile_schema"):
                plan.setdefault("configuration_changes", []).append({
                    "patch_id": f"patch-{ds_name}-{col or 'manifest'}".lower(),
                    "dataset_id": ds_name,
                    "field_name": col or "manifest",
                    "action_type": action,
                    "lifecycle_status": "proposed",
                })
                continue

            # 2. Unknown or legacy actions check
            meta = get_action_meta(action)
            if meta.get("_unknown"):
                add_issue("UNKNOWN_ACTION", f"Unknown action '{action}' on column '{col}'.")
                continue

            if meta.get("legacy") or is_invalid_action(action) or is_legacy_action(action):
                add_issue("AMBIGUOUS_LEGACY_ACTION", f"Action '{action}' on column '{col}' is a legacy ambiguous action and cannot be executed.")

            # 3. Phase check
            if is_phase1 and meta.get("phase") == 2:
                add_issue("PHASE_MISMATCH", f"Action '{action}' on column '{col}' is Phase 2 but this is a Phase 1 plan.")

            # 4. Required parameters check
            if action in ("range_clip", "clip_outliers"):
                if "lower_bound" in params and "min_value" not in params:
                    params["min_value"] = params["lower_bound"]
                elif "min_value" in params and "lower_bound" not in params:
                    params["lower_bound"] = params["min_value"]
                params.setdefault("min_value", 0.0)

            if action in ("replace_values", "replace_sentinel_values") and "replace_values" not in params:
                params["replace_values"] = ["N/A", "n/a", "NA", "na", "???", "unknown", "UNKNOWN",
                                            "none", "None", "NULL", "null", "nan", "-", "--", "0",
                                            "###", "TBD", "tbd"]

            req_params = meta.get("required_params") or []
            for rp in req_params:
                if rp not in params:
                    add_issue("MISSING_REQUIRED_PARAMETER", f"Action '{action}' is missing required parameter '{rp}' on column '{col}'.")

            # 5. Conditional parameters and semantic rules
            if action == "fill_null_with_constant":
                if "value" not in params:
                    add_issue("MISSING_REQUIRED_PARAMETER", f"Action 'fill_null_with_constant' is missing constant 'value' parameter on column '{col}'.")
            elif action == "impute_numeric":
                strategy = params.get("strategy")
                if not strategy:
                    add_issue("MISSING_REQUIRED_PARAMETER", f"Action 'impute_numeric' is missing 'strategy' parameter.")
                elif strategy == "constant" and "value" not in params:
                    add_issue("MISSING_REQUIRED_PARAMETER", "Action 'impute_numeric' strategy 'constant' requires 'value' parameter.")
                elif strategy in ("grouped_mean", "grouped_median") and not params.get("group_by"):
                    add_issue("MISSING_REQUIRED_PARAMETER", f"Action 'impute_numeric' strategy '{strategy}' requires 'group_by' parameter.")
            elif action == "deduplicate":
                if not params.get("key_columns") or not params.get("survivorship_policy"):
                    add_issue("INCOMPLETE_DEDUPLICATION_POLICY", f"Deduplication action on column '{col}' is missing key_columns or survivorship_policy.")

            # 6. Semantic role prohibitions (key imputation check)
            # 6. Semantic role prohibitions (key imputation check and row dropping)
            if rules.get("never_drop_rows"):
                if action in ("drop_rows", "drop_row", "dropna") or (action == "filter_rows" and params.get("mode") == "drop"):
                    add_issue("UNSAFE_ROW_DELETION", f"Action '{action}' is prohibited because business rule 'never_drop_rows' is enabled.")

            prohibitions = get_semantic_prohibitions(action)
            if prohibitions:
                is_key = is_key_or_identifier_column(ds_name, col, plan, assessment)
                if is_key:
                    add_issue("ILLEGAL_KEY_IMPUTATION", f"Action '{action}' is prohibited on key/identifier column '{col}' (prohibited types: {prohibitions}).")

            # 7. Conflicting writes to the same output column
            out_col = params.get("output_column")
            if out_col:
                key = (ds_name, out_col)
                if key in written_outputs:
                    add_issue("CONFLICTING_WRITE_COLUMN", f"Conflict: steps '{step_id}' and '{written_outputs[key]}' both write to the output column '{out_col}'.")
                else:
                    written_outputs[key] = step_id

            # 8. Provenance and resolution/action mismatches
            mr_prov = st.get("manual_resolution")
            is_promoted_manual = (
                st.get("bucket") == "approved_manual"
                or st.get("execution_bucket") == "approved_manual"
                or st.get("execution_mode") == "APPROVED_MANUAL"
                or mr_prov is not None
            )
            if is_promoted_manual:
                if not mr_prov or not isinstance(mr_prov, dict):
                    add_issue("MISSING_PROVENANCE", f"Step '{step_id}' is missing manual resolution provenance.")
                else:
                    sel_opt = mr_prov.get("selected_option_id")
                    if not sel_opt:
                        add_issue("MISSING_PROVENANCE", f"Step '{step_id}' manual resolution is missing 'selected_option_id'.")
                    else:
                        # Check mapping mismatch
                        canonical_action = RESOLUTION_TO_ACTION_MAP.get(sel_opt)
                        if canonical_action and canonical_action != action:
                            add_issue("PROMOTION_ACTION_MISMATCH", f"Step '{step_id}' action '{action}' does not match canonical action '{canonical_action}' for selected option '{sel_opt}'.")

    return len(issues) == 0, issues

