"""
readiness_engine.py — Evaluates Phase 1 Column Readiness Certificates and Phase 2 Operation Gates.
"""
from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any, Dict, List, Optional


def _compute_sha256(data: str) -> str:
    return "sha256:" + hashlib.sha256(data.encode("utf-8", errors="ignore")).hexdigest()


def _get_normalization_contract_hash(steps: List[dict], col: str) -> str:
    """Generate a stable contract hash based on case/whitespace transforms applied in Phase 1."""
    normalization_actions = []
    for s in steps:
        if s.get("column") == col:
            act = str(s.get("action") or "").strip().lower()
            if act in ("trim", "trim_preserve_case", "lowercase", "uppercase", "create_normalized_match_key", "standardize_case"):
                normalization_actions.append(act)
    # Return stable hash based ONLY on the normalization actions applied
    contract_str = f"{sorted(normalization_actions)}"
    return _compute_sha256(contract_str)


def build_projected_column_certificates(plan: Dict[str, Any], assessment: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Computes projected Column Readiness Certificates based on the current Phase 1 plan and unresolved issues.
    This projects what the column state will be AFTER the plan is executed.
    """
    certificates = []
    datasets = plan.get("datasets") or {}
    assess_datasets = assessment.get("datasets") or {}
    
    # 1. Identify all columns involved in relationships
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    related_cols = []  # List of (dataset, col_name, semantic_role)
    for j in joins:
        left_ds = j.get("left_dataset")
        right_ds = j.get("right_dataset")
        left_keys = j.get("left_keys") or [j.get("left_key")] if j.get("left_keys") or j.get("left_key") else []
        right_keys = j.get("right_keys") or [j.get("right_key")] if j.get("right_keys") or j.get("right_key") else []
        for lk in left_keys:
            if lk:
                related_cols.append((left_ds, lk, "PARENT_KEY" if j.get("require_parent_unique", True) else "CHILD_KEY"))
        for rk in right_keys:
            if rk:
                related_cols.append((right_ds, rk, "CHILD_KEY"))

    # Also add required columns from business rules
    rules = plan.get("business_rules") or {}
    req_cols = rules.get("required_columns") or []
    for rc in req_cols:
        for ds_name in datasets.keys():
            ds_meta = assess_datasets.get(ds_name) or {}
            if rc in (ds_meta.get("columns") or {}):
                related_cols.append((ds_name, rc, "REQUIRED_COLUMN"))

    # Deduplicate related columns and filter out row-level/wildcards
    unique_related = []
    seen = set()
    for ds, col, role in related_cols:
        if not col or str(col).lower() in ("*", "row-level", "[row-level]", ""):
            continue
        if (ds, col) not in seen:
            seen.add((ds, col))
            unique_related.append((ds, col, role))

    # 2. Build projected certificate for each column
    for ds_name, col_name, role in unique_related:
        ds_block = datasets.get(ds_name) or {}
        steps = ds_block.get("steps") or []

        # Get baseline profiling details from assessment
        ds_meta = assess_datasets.get(ds_name) or {}
        col_meta = (ds_meta.get("columns") or {}).get(col_name) or {}

        raw_null_count = int(col_meta.get("null_count") or 0)
        raw_distinct = int(col_meta.get("distinct_values") or col_meta.get("distinct_count") or 0)
        total_rows = int(ds_meta.get("row_count") or 1000)

        # In Phase 1 plan, check what resolutions are planned
        projected_null_count = raw_null_count
        projected_invalid_count = 0
        projected_placeholder_count = 0
        projected_duplicate_count = 0

        blocking_reasons = []
        warnings = []

        # Check for explicit steps or resolutions in steps list
        null_resolution_actions = {
            "fill_nulls_simple", "fill_nulls_flag", "flag_missing_key", "flag_missing_declared_key",
            "preserve_null", "fill_constant", "preserve_null_and_flag", "impute_numeric", "impute_date",
            "quarantine_missing_key", "preserve_missing_suspected_key"
        }
        duplicate_resolution_actions = {
            "deduplicate", "flag_duplicate_candidates", "flag_near_duplicate_cluster",
            "flag_business_key_duplicate", "create_normalized_match_key"
        }

        has_null_fill = False
        has_cast = False
        has_dup_resolution = False

        for s in steps:
            if s.get("column") == col_name:
                act = str(s.get("action") or "").strip().lower()
                if act in null_resolution_actions:
                    has_null_fill = True
                    projected_null_count = 0
                if act in duplicate_resolution_actions:
                    has_dup_resolution = True
                    projected_duplicate_count = 0
                if act in ("cast_type", "coerce_numeric"):
                    has_cast = True

        # Process manual review resolutions affecting this column
        for mr in plan.get("manual_review") or []:
            mr_col = str(mr.get("column") or "")
            if mr.get("dataset") == ds_name and col_name in mr_col:
                it = mr.get("issue_type")
                status = mr.get("status")

                # If unresolved, it blocks projected readiness
                if status != "resolved" and status != "auto_resolved":
                    blocking_reasons.append({
                        "issue_type": it,
                        "message": mr.get("message") or "Unresolved manual review issue",
                        "resolved": False
                    })
                else:
                    warnings.append(f"Resolved issue '{it}' under policy waiver/resolution.")
                    # If resolved, null/duplicate counts project to 0 since policy has handled them
                    if it in ("nulls", "declared_key_null_violation", "suspected_key_high_null_ratio"):
                        projected_null_count = 0
                        has_null_fill = True
                    if it in ("exact_duplicate_rows", "duplicate_rows", "near_duplicate_rows", "business_key_duplicate", "duplicate_primary_key"):
                        projected_duplicate_count = 0
                        has_dup_resolution = True
                    
        # Readiness status evaluation
        if blocking_reasons:
            status_verdict = "BLOCKED_BY_UNRESOLVED_POLICY"
        elif projected_null_count > 0 and role in ("PARENT_KEY", "REQUIRED_COLUMN"):
            status_verdict = "BLOCKED_BY_COLUMN_QUALITY"
            blocking_reasons.append({
                "issue_type": "declared_key_null_violation" if role == "PARENT_KEY" else "missing_required_column",
                "message": f"Column '{col_name}' ({role}) has {projected_null_count} projected null values.",
                "resolved": False
            })
        elif projected_null_count > 0:
            status_verdict = "READY_WITH_WARNINGS"
            warnings.append(f"Column has {projected_null_count} nulls but is CHILD_KEY/OPTIONAL.")
        elif warnings:
            status_verdict = "READY_WITH_WARNINGS"
        else:
            status_verdict = "READY"
            
        contract_hash = _get_normalization_contract_hash(steps, col_name)
        
        cert = {
            "certificate_id": f"CRC-{_compute_sha256(f'{ds_name}:{col_name}:{role}')[:16]}",
            "certificate_type": "PROJECTED",
            "certificate_status": "DRAFT" if blocking_reasons else "VALIDATED",
            "dataset_id": ds_name,
            "source_column": col_name,
            "prepared_column": f"{col_name}_clean" if (has_null_fill or has_cast) else col_name,
            "semantic_role": role,
            "physical_type": str(col_meta.get("dtype") or "string"),
            "required_type": "string" if role in ("PARENT_KEY", "CHILD_KEY") else str(col_meta.get("dtype") or "string"),
            "readiness_status": status_verdict,
            "metrics": {
                "evaluated_rows": total_rows,
                "null_count": projected_null_count,
                "invalid_count": projected_invalid_count,
                "placeholder_count": projected_placeholder_count,
                "distinct_count": raw_distinct,
                "duplicate_row_count": 0,
                "duplicate_key_count": projected_duplicate_count,
                "parse_failure_count": 0
            },
            "constraints": {
                "allow_null": (role != "PARENT_KEY"),
                "require_unique": (role == "PARENT_KEY"),
                "accepted_formats": [],
                "case_sensitive": True
            },
            "blocking_reasons": blocking_reasons,
            "warnings": warnings,
            "source_fingerprint": _compute_sha256(f"{ds_name}:{total_rows}"),
            "phase1_output_fingerprint": "pending_execution",
            "phase1_plan_hash": plan.get("provenance_signature") or "unknown",
            "normalization_contract_hash": contract_hash,
            "rules_hash": _compute_sha256(json.dumps(plan.get("business_rules") or {})),
            "validator_version": "1.0.0",
            "generated_at": datetime.datetime.now().isoformat()
        }
        certificates.append(cert)
        
    return certificates


def build_actual_column_certificates(
    phase1_outputs: Dict[str, Any],
    post_validation_results: Dict[str, Any],
    execution_manifest: Dict[str, Any],
    plan: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Builds ACTUAL Column Readiness Certificates post-Phase 1 execution.
    These reflect the physical state of the cleaned datasets.
    """
    projected = build_projected_column_certificates(plan, post_validation_results)
    actual_certs = []
    
    # We update projected to ACTUAL, attaching validated fingerprints
    for p in projected:
        act = p.copy()
        act["certificate_type"] = "ACTUAL"
        act["certificate_status"] = "VALIDATED" if not p.get("blocking_reasons") else "FAILED"
        
        # Attach output fingerprint from the run manifest
        ds_id = p["dataset_id"]
        run_info = execution_manifest.get("datasets", {}).get(ds_id) or {}
        output_rows = run_info.get("output_row_count") or run_info.get("rows") or p["metrics"]["evaluated_rows"]
        act["phase1_output_fingerprint"] = _compute_sha256(f"{ds_id}_output_{output_rows}")
        act["metrics"]["evaluated_rows"] = output_rows
        
        # Update metrics with post-validation figures if available
        post_ds = post_validation_results.get("datasets", {}).get(ds_id, {})
        post_col = post_ds.get("columns", {}).get(p["source_column"], {})
        if post_col:
            act["metrics"]["null_count"] = int(post_col.get("null_count") or 0)
            act["metrics"]["distinct_count"] = int(post_col.get("distinct_count") or post_col.get("distinct_values") or p["metrics"]["distinct_count"])
            
        actual_certs.append(act)
        
    return actual_certs


def validate_certificate_freshness(
    cert: Dict[str, Any],
    expected_plan_hash: str,
    expected_source_fingerprint: str
) -> bool:
    """Verifies that the certificate hashes match the expected current execution parameters."""
    if cert.get("phase1_plan_hash") != expected_plan_hash:
        return False
    if cert.get("source_fingerprint") != expected_source_fingerprint:
        return False
    return True


def evaluate_operation_gate(
    parent_certs: List[Dict[str, Any]],
    child_certs: List[Dict[str, Any]],
    operation_contract: Dict[str, Any],
    approved_waivers: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Evaluates prerequisite gates for a specific Phase 2 operation (e.g. foreign key check or join).
    """
    op_id = operation_contract.get("operation_id") or "unknown_op"
    op_type = operation_contract.get("operation_type") or "REFERENTIAL_INTEGRITY"
    
    parent_ds = operation_contract.get("parent", {}).get("dataset_id")
    parent_cols = operation_contract.get("parent", {}).get("columns") or []
    child_ds = operation_contract.get("child", {}).get("dataset_id")
    child_cols = operation_contract.get("child", {}).get("columns") or []
    
    cardinality = operation_contract.get("cardinality") or "ONE_TO_MANY"
    require_parent_unique = operation_contract.get("require_parent_unique", True)
    
    blocking_reasons = []
    warnings = []
    
    # Check dataset availability
    if not parent_certs:
        return {
            "operation_id": op_id,
            "operation_type": op_type,
            "gate_status": "BLOCKED",
            "gate_reason_code": "BLOCKED_BY_MISSING_DATASET",
            "blocking_reasons": [f"Parent dataset '{parent_ds}' is missing column readiness certificates."],
            "warnings": []
        }
    if not child_certs:
        return {
            "operation_id": op_id,
            "operation_type": op_type,
            "gate_status": "BLOCKED",
            "gate_reason_code": "BLOCKED_BY_MISSING_DATASET",
            "blocking_reasons": [f"Child dataset '{child_ds}' is missing column readiness certificates."],
            "warnings": []
        }
        
    # Find matching certificates for parent and child keys
    p_certs_map = {c["source_column"]: c for c in parent_certs if c["dataset_id"] == parent_ds}
    c_certs_map = {c["source_column"]: c for c in child_certs if c["dataset_id"] == child_ds}
    
    p_matched = []
    c_matched = []
    
    for pc in parent_cols:
        if pc in p_certs_map:
            p_matched.append(p_certs_map[pc])
        else:
            blocking_reasons.append(f"Parent key column '{pc}' has no readiness certificate.")
            
    for cc in child_cols:
        if cc in c_certs_map:
            c_matched.append(c_certs_map[cc])
        else:
            blocking_reasons.append(f"Child key column '{cc}' has no readiness certificate.")
            
    if blocking_reasons:
        return {
            "operation_id": op_id,
            "operation_type": op_type,
            "gate_status": "BLOCKED",
            "gate_reason_code": "BLOCKED_BY_MISSING_COLUMN",
            "blocking_reasons": blocking_reasons,
            "warnings": []
        }

    # 0. Freshness and Stale Checks
    expected_plan_hash = operation_contract.get("expected_plan_hash")
    expected_parent_fingerprint = operation_contract.get("expected_parent_fingerprint")
    expected_child_fingerprint = operation_contract.get("expected_child_fingerprint")
    
    for p_c in p_matched:
        if expected_plan_hash and p_c.get("phase1_plan_hash") != expected_plan_hash:
            blocking_reasons.append(f"Stale certificate for parent column '{p_c['source_column']}': plan hash mismatch.")
        if expected_parent_fingerprint and p_c.get("source_fingerprint") != expected_parent_fingerprint:
            blocking_reasons.append(f"Stale certificate for parent column '{p_c['source_column']}': source fingerprint mismatch.")
            
    for c_c in c_matched:
        if expected_plan_hash and c_c.get("phase1_plan_hash") != expected_plan_hash:
            blocking_reasons.append(f"Stale certificate for child column '{c_c['source_column']}': plan hash mismatch.")
        if expected_child_fingerprint and c_c.get("source_fingerprint") != expected_child_fingerprint:
            blocking_reasons.append(f"Stale certificate for child column '{c_c['source_column']}': source fingerprint mismatch.")

    # 1. Type compatibility check
    for p_c, c_c in zip(p_matched, c_matched):
        if p_c["physical_type"] != c_c["physical_type"]:
            blocking_reasons.append(
                f"Type mismatch: parent column '{p_c['source_column']}' is '{p_c['physical_type']}' "
                f"but child column '{c_c['source_column']}' is '{c_c['physical_type']}'"
            )
            
    # 2. Normalization Parity check
    norm_parity = "PASS"
    for p_c, c_c in zip(p_matched, c_matched):
        if p_c["normalization_contract_hash"] != c_c["normalization_contract_hash"]:
            norm_parity = "FAIL"
            blocking_reasons.append(
                f"Normalization mismatch: parent column '{p_c['source_column']}' contract hash is different from child '{c_c['source_column']}'"
            )

    # 3. Parent uniqueness check
    if require_parent_unique:
        for p_c in p_matched:
            # Check duplicate_key_count in metrics
            if p_c["metrics"].get("duplicate_key_count", 0) > 0:
                blocking_reasons.append(
                    f"Parent uniqueness failure: parent key column '{p_c['source_column']}' contains duplicates."
                )

    # 4. Mandatory Null check
    for c_c in c_matched:
        if c_c["metrics"].get("null_count", 0) > 0 and not c_c["constraints"].get("allow_null", True):
            blocking_reasons.append(
                f"Null keys block: child key column '{c_c['source_column']}' contains null values."
            )

    # Collect warnings from input column certificates
    for cert in p_matched + c_matched:
        if cert.get("readiness_status") == "READY_WITH_WARNINGS":
            for w in cert.get("warnings") or []:
                warnings.append(f"Input column '{cert['dataset_id']}.{cert['source_column']}' warning: {w}")

    # 5. Waiver validation
    # Check if any approved waiver overrides our blocking reasons
    resolved_by_waiver = []
    for reason in list(blocking_reasons):
        for w in approved_waivers:
            # Check if waiver matches scope
            w_scope = w.get("scope") or {}
            w_op = w_scope.get("operation_id")
            w_ds = w_scope.get("dataset_id")
            w_col = w_scope.get("column")
            
            # Check dates
            now = datetime.datetime.now().isoformat()
            effective = w.get("effective_at") or "1900-01-01"
            expiry = w.get("expires_at") or "9999-12-31"
            
            is_valid_time = (effective <= now <= expiry)
            
            if is_valid_time and (w_op == op_id or (w_ds == child_ds and w_col in child_cols) or (w_ds == parent_ds and w_col in parent_cols)):
                # If reason matches waiver requirement
                if w.get("affected_requirement") in ("CHILD_NULL_KEY_ALLOWED", "PARENT_NON_UNIQUE_ALLOWED", "TYPE_MISMATCH_ALLOWED", "NORMALIZATION_MISMATCH_ALLOWED"):
                    resolved_by_waiver.append(reason)
                    warnings.append({
                        "warning_code": "AUTHORIZED_WAIVER",
                        "waiver_id": w.get("waiver_id"),
                        "expires_at": expiry,
                        "affected_requirement": w.get("affected_requirement"),
                        "reason": w.get("reason")
                    })
                    break
                    
    # Remove reasons resolved by waivers from blocking_reasons
    for r in resolved_by_waiver:
        if r in blocking_reasons:
            blocking_reasons.remove(r)
            
    # Determine gate status code
    if blocking_reasons:
        # Map specific blocker code
        code = "BLOCKED"
        for r in blocking_reasons:
            if "Type mismatch" in r:
                code = "BLOCKED_BY_TYPE_MISMATCH"
            elif "Normalization mismatch" in r:
                code = "BLOCKED_BY_NORMALIZATION_MISMATCH"
            elif "Parent uniqueness" in r:
                code = "BLOCKED_BY_PARENT_NON_UNIQUENESS"
            elif "Null keys" in r:
                code = "BLOCKED_BY_COLUMN_QUALITY"
            elif "Unresolved" in r:
                code = "BLOCKED_BY_UNRESOLVED_POLICY"
            elif "Stale certificate" in r:
                if "plan hash mismatch" in r:
                    code = "BLOCKED_BY_STALE_CERTIFICATE"
                else:
                    code = "BLOCKED_BY_SOURCE_MISMATCH"
        status = "BLOCKED"
    elif warnings:
        status = "READY_WITH_WARNINGS"
        code = "READY_WITH_WARNINGS"
    else:
        status = "READY"
        code = "READY"
        
    return {
        "operation_id": op_id,
        "operation_type": op_type,
        "gate_status": status,
        "gate_reason_code": code,
        "parent": {
            "dataset_id": parent_ds,
            "columns": parent_cols
        },
        "child": {
            "dataset_id": child_ds,
            "columns": child_cols
        },
        "cardinality": cardinality,
        "require_parent_unique": require_parent_unique,
        "require_child_unique": operation_contract.get("require_child_unique", False),
        "row_multiplication_allowed": operation_contract.get("row_multiplication_allowed", False),
        "normalization_parity": "PASS" if norm_parity == "PASS" else "FAIL",
        "blocking_reasons": blocking_reasons,
        "warnings": warnings
    }


def evaluate_all_operation_gates(
    certs: List[Dict[str, Any]],
    joins: List[Dict[str, Any]],
    approved_waivers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Evaluates gates for all joins/relationships defined in the plan.
    """
    gates = []
    for j in joins:
        # Build parent and child column lists from join
        left_ds = j.get("left_dataset")
        right_ds = j.get("right_dataset")
        left_keys = j.get("left_keys") or [j.get("left_key")] if j.get("left_keys") or j.get("left_key") else []
        right_keys = j.get("right_keys") or [j.get("right_key")] if j.get("right_keys") or j.get("right_key") else []
        
        left_keys = [lk for lk in left_keys if lk]
        right_keys = [rk for rk in right_keys if rk]
        
        # Build contract
        contract = {
            "operation_id": f"rel_{left_ds}_{right_ds}",
            "operation_type": "REFERENTIAL_INTEGRITY",
            "parent": {
                "dataset_id": left_ds,
                "columns": left_keys
            },
            "child": {
                "dataset_id": right_ds,
                "columns": right_keys
            },
            "cardinality": j.get("cardinality") or "ONE_TO_MANY",
            "require_parent_unique": j.get("require_parent_unique", True),
            "require_child_unique": j.get("require_child_unique", False),
            "row_multiplication_allowed": False
        }
        
        # Filter matching certificates
        parent_certs = [c for c in certs if c["dataset_id"] == left_ds]
        child_certs = [c for c in certs if c["dataset_id"] == right_ds]
        
        gate = evaluate_operation_gate(parent_certs, child_certs, contract, approved_waivers)
        gates.append(gate)
        
    return gates


def build_projected_assessment(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Builds a projected assessment copying baseline assessment and nullifying unexpected counts
    for issues that have been resolved or planned in the plan steps.
    """
    import copy
    proj = copy.deepcopy(assessment)
    datasets = plan.get("datasets") or {}
    assess_datasets = proj.get("datasets") or {}
    
    # Null resolution actions
    null_resolution_actions = {
        "fill_nulls_simple", "fill_nulls_flag", "flag_missing_key", "flag_missing_declared_key",
        "preserve_null", "fill_constant", "preserve_null_and_flag", "impute_numeric", "impute_date",
        "quarantine_missing_key", "preserve_missing_suspected_key"
    }
    # Duplicate resolution actions
    duplicate_resolution_actions = {
        "deduplicate", "flag_duplicate_candidates", "flag_near_duplicate_cluster",
        "flag_business_key_duplicate", "create_normalized_match_key"
    }
    # Type resolution actions
    type_resolution_actions = {
        "cast_type", "coerce_numeric", "parse_dates", "standardize_boolean"
    }
    # Outlier resolution actions
    outlier_resolution_actions = {
        "range_clip", "clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers"
    }

    for ds_name, ds_block in datasets.items():
        steps = ds_block.get("steps") or []
        ds_meta = assess_datasets.setdefault(ds_name, {})
        columns = ds_meta.setdefault("columns", {})
        
        # Track which columns have what kinds of resolutions
        null_resolved_cols = set()
        dup_resolved_cols = set()
        type_resolved_cols = set()
        outlier_resolved_cols = set()
        
        for s in steps:
            col = s.get("column")
            if not col:
                continue
            act = str(s.get("action") or "").strip().lower()
            if act in null_resolution_actions:
                null_resolved_cols.add(col)
            if act in duplicate_resolution_actions:
                dup_resolved_cols.add(col)
            if act in type_resolution_actions:
                type_resolved_cols.add(col)
            if act in outlier_resolution_actions:
                outlier_resolved_cols.add(col)

        # Also incorporate resolved manual review items
        for mr in plan.get("resolved_manual_review") or []:
            if mr.get("dataset") == ds_name:
                col = mr.get("column")
                it = str(mr.get("issue_type") or "").strip().lower()
                if it in ("nulls", "declared_key_null_violation", "suspected_key_high_null_ratio"):
                    null_resolved_cols.add(col)
                if it in ("exact_duplicate_rows", "duplicate_rows", "near_duplicate_rows", "business_key_duplicate", "duplicate_primary_key"):
                    dup_resolved_cols.add(col)
                if it in ("type_mismatch", "invalid_date_format", "invalid_email", "invalid_phone", "invalid_numeric"):
                    type_resolved_cols.add(col)
                if it in ("outlier_detected", "future_date", "negative_financial_amount"):
                    outlier_resolved_cols.add(col)

        # Apply null adjustments to columns
        for col_name, col_meta in columns.items():
            if col_name in null_resolved_cols:
                col_meta["null_percentage"] = 0.0
                col_meta["null_pct"] = 0.0
                col_meta["null_count"] = 0

        # Adjust quality issues
        quality = ds_meta.setdefault("quality", {})
        issues = list(quality.get("issues") or [])
        new_issues = []
        for issue in issues:
            col = issue.get("column")
            itype = str(issue.get("type") or issue.get("issue_type") or "").strip().lower()
            
            is_resolved = False
            if col in null_resolved_cols and itype in ("nulls", "declared_key_null_violation", "suspected_key_high_null_ratio"):
                is_resolved = True
            elif col in dup_resolved_cols and ("duplicate" in itype or "dup" in itype):
                is_resolved = True
            elif col in type_resolved_cols and itype in ("type_mismatch", "invalid_date_format", "invalid_email", "invalid_phone", "invalid_numeric", "parse_dates"):
                is_resolved = True
            elif col in outlier_resolved_cols and ("outlier" in itype or itype in ("range_clip", "clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers")):
                is_resolved = True
                
            if not is_resolved:
                new_issues.append(issue)
        quality["issues"] = new_issues

        # Clear duplicate hints if duplicates resolved
        if ds_meta.get("llm_hints"):
            dup_info = ds_meta["llm_hints"].get("business_key_confirmation") or {}
            if isinstance(dup_info, dict) and any(col in dup_resolved_cols for col in dup_info.get("business_key_cols", [])):
                dup_info["business_key_duplicate_count"] = 0

    return proj


def recompute_readiness_scorecard(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Recalculates the dataset-level readiness scores scorecard dynamically post-resolution.
    """
    from agent.etl_pipeline.dq_gate import calculate_dataset_dq_score

    datasets_known = set((assessment.get("datasets") or {}).keys())
    readiness_scorecard = {}
    total_rows = 0
    weighted_score_sum = 0.0

    # Build projected assessment for calculating projected DQ scores
    projected_assessment = build_projected_assessment(plan, assessment)
    plan_hash = plan.get("provenance_signature") or "unknown"

    for ds_name in datasets_known:
        row_count = float(
            (assessment.get("datasets") or {}).get(ds_name, {}).get("row_count") or 1000
        )
        # Calculate raw source score
        src_score_res = calculate_dataset_dq_score(assessment, ds_name)
        source_score = round(src_score_res.get("score") or 0.0, 1)

        # Calculate projected score
        score_res = calculate_dataset_dq_score(projected_assessment, ds_name)
        score = round(score_res.get("score") or 0.0, 1)

        # Check if any pending manual review items for this dataset
        has_high_gating = False
        for mr in plan.get("manual_review") or []:
            if isinstance(mr, dict) and str(mr.get("status") or "pending").lower() == "pending":
                if mr.get("dataset") == ds_name and str(mr.get("severity") or "medium").lower() == "high":
                    has_high_gating = True
                    break

        if score < 70.0 or has_high_gating:
            readiness = "NEEDS_WORK"
            reason = (
                "Required datasets include gating HIGH failures"
                if has_high_gating
                else f"DQ score ({score}) below threshold (70.0)"
            )
        elif score < 90.0:
            readiness = "READY_WITH_WARNINGS"
            reason = "No HIGH findings, but extensive MEDIUM findings"
        else:
            readiness = "READY"
            reason = "Dataset meets quality standards"

        resolved_penalties = round(max(0.0, score - source_score), 1)
        remaining_penalties = round(100.0 - score, 1)

        readiness_scorecard[ds_name] = {
            "score": score,
            "score_type": "PROJECTED_POST_PHASE1",
            "projection_status": "estimated_pending_validation",
            "projected_score": score,
            "observed_post_etl_score": None,
            "validation_status": "not_started",
            "runtime_auto_fix_authorized": False,
            "methodology_version": "dq-score-v1",
            "plan_hash": plan_hash,
            "computed_at": datetime.datetime.now().isoformat(),
            "is_authoritative": not has_high_gating and score >= 70.0,
            "ledger": {
                "source_score": source_score,
                "resolved_issue_penalties": resolved_penalties,
                "remaining_issue_penalties": remaining_penalties,
                "quarantined_row_treatment": 0.0,
                "new_validation_findings": 0.0,
                "projected_final_score": score
            },
            "readiness": readiness,
            "primary_reason": reason,
            "row_count": row_count,
        }
        total_rows += row_count
        weighted_score_sum += score * row_count

    overall_score = round(weighted_score_sum / max(total_rows, 1), 1)
    any_needs_work = any(card["readiness"] == "NEEDS_WORK" for card in readiness_scorecard.values())
    overall_readiness = (
        "NEEDS_WORK"
        if any_needs_work
        else (
            "READY_WITH_WARNINGS"
            if any(card["readiness"] == "READY_WITH_WARNINGS" for card in readiness_scorecard.values())
            else "READY"
        )
    )

    readiness_scorecard["Overall"] = {
        "score": overall_score,
        "score_type": "PROJECTED_POST_PHASE1",
        "projection_status": "estimated_pending_validation",
        "projected_score": overall_score,
        "observed_post_etl_score": None,
        "validation_status": "not_started",
        "runtime_auto_fix_authorized": False,
        "methodology_version": "dq-score-v1",
        "plan_hash": plan_hash,
        "computed_at": datetime.datetime.now().isoformat(),
        "is_authoritative": not any_needs_work and overall_score >= 70.0,
        "ledger": {
            "source_score": round(sum(calculate_dataset_dq_score(assessment, d).get("score", 0.0) * float((assessment.get("datasets") or {}).get(d, {}).get("row_count") or 1000) for d in datasets_known) / max(total_rows, 1), 1),
            "resolved_issue_penalties": round(max(0.0, overall_score - (sum(calculate_dataset_dq_score(assessment, d).get("score", 0.0) * float((assessment.get("datasets") or {}).get(d, {}).get("row_count") or 1000) for d in datasets_known) / max(total_rows, 1))), 1),
            "remaining_issue_penalties": round(100.0 - overall_score, 1),
            "quarantined_row_treatment": 0.0,
            "new_validation_findings": 0.0,
            "projected_final_score": overall_score
        },
        "readiness": overall_readiness,
        "primary_reason": (
            "Required datasets include gating HIGH failures"
            if any_needs_work
            else "All datasets ready or with warnings"
        ),
        "row_count": total_rows,
    }
    return readiness_scorecard

