from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def normalize_business_rules(raw: Any) -> Dict[str, Any]:
    """
    Normalize UI / JSON payload into a canonical rules dict used by planner + codegen.

    Supported keys (all optional):
    - never_drop_rows: bool
    - required_columns: list[str] — global display names; matched case-insensitively per dataset
    - non_nullable: list[str] — columns that must not be nulled by transforms (best-effort)
    - exclude_columns: list[str] — skip any auto step touching these columns
    - valid_values: dict[str, list[str]] — column -> allowed values (metadata for future codegen)
    - notes: str — free-text business context (stored on plan, not auto-executed)
    """
    if not isinstance(raw, dict):
        raw = {}

    def _bool(v: Any, default: bool = False) -> bool:
        if isinstance(v, bool):
            return v
        if v in (1, "1", "true", "True", "yes", "on"):
            return True
        if v in (0, "0", "false", "False", "no", "off", ""):
            return False
        return default

    def _str_list(v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = re.split(r"[\s,;]+", v.strip())
            return [p for p in parts if p]
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        return []

    req = _str_list(raw.get("required_columns") or raw.get("requiredColumns"))
    excl = _str_list(raw.get("exclude_columns") or raw.get("excludeColumns"))
    nn = _str_list(raw.get("non_nullable") or raw.get("nonNullable"))

    vv = raw.get("valid_values") or raw.get("validValues")
    if not isinstance(vv, dict):
        vv = {}

    notes_raw = raw.get("notes") or raw.get("business_notes") or ""
    notes = "\n".join(
        line for line in str(notes_raw).strip().splitlines() if line.strip()
    ) if notes_raw else ""

    assertions_raw = raw.get("custom_assertions") or raw.get("customAssertions") or raw.get("assertions") or []
    if not isinstance(assertions_raw, list):
        assertions_raw = [assertions_raw] if assertions_raw else []
    
    normalized_assertions = []
    for entry in assertions_raw:
        if isinstance(entry, dict) and entry.get("assertion"):
            normalized_assertions.append({
                "assertion": str(entry["assertion"]).strip(),
                "severity": str(entry.get("severity") or "medium").strip().lower(),
                "message": str(entry.get("message") or "").strip()
            })
        elif isinstance(entry, str) and entry.strip():
            normalized_assertions.append({
                "assertion": entry.strip(),
                "severity": "medium",
                "message": ""
            })

    # Pass through SCD configuration for per-dataset SCD type selection
    scd_raw = raw.get("scd") or {}
    if not isinstance(scd_raw, dict):
        scd_raw = {}

    # Pass through force_unlock list
    force_unlock_raw = raw.get("force_unlock") or []
    if not isinstance(force_unlock_raw, list):
        force_unlock_raw = [force_unlock_raw] if force_unlock_raw else []

    semantic_overrides_raw = raw.get("semantic_overrides") or raw.get("semanticOverrides") or {}
    if not isinstance(semantic_overrides_raw, dict):
        semantic_overrides_raw = {}

    return {
        "never_drop_rows": _bool(raw.get("never_drop_rows") or raw.get("neverDropRows"), False),
        "auto_resolve_pending": _bool(raw.get("auto_resolve_pending") or raw.get("autoResolvePending"), False),
        "auto_resolve_safe_defaults": _bool(raw.get("auto_resolve_safe_defaults") or raw.get("autoResolveSafeDefaults"), False),
        "required_columns": req,
        "non_nullable": nn,
        "exclude_columns": sorted(set(excl)),
        "valid_values": {str(k): list(v) if isinstance(v, list) else [str(v)] for k, v in vv.items()},
        "custom_assertions": normalized_assertions,
        "outlier_strategy": str(raw.get("outlier_strategy") or raw.get("outlierStrategy") or "flag").lower(),
        "dq_threshold": float(raw.get("dq_threshold") or raw.get("dqThreshold") or 70.0),
        "notes": notes,
        "scd": scd_raw,
        "force_unlock": force_unlock_raw,
        "semantic_overrides": semantic_overrides_raw,
    }


def column_is_excluded(column: str | None, exclude: Any) -> bool:
    if not column or not exclude:
        return False
    # Use lowercase set for matching but preserve original case in rules
    ex_lower = {str(c).lower() for c in (exclude if isinstance(exclude, (list, set, frozenset)) else list(exclude))}
    return column.strip().lower() in ex_lower


def to_tagged_rules(rules: Dict[str, Any], dataset_name: str, assessment: Optional[Dict[str, Any]] = None) -> List[Any]:
    from agent.etl_pipeline.rule_provenance import TaggedRule, RuleProvenance
    tagged = []
    
    # Map case and filter if assessment is provided
    ds_cols = {}
    has_schema = False
    if assessment and assessment.get("datasets") and dataset_name in assessment["datasets"]:
        ds_info = assessment["datasets"][dataset_name] or {}
        cols_info = ds_info.get("columns") or {}
        if cols_info:
            has_schema = True
            for c in cols_info.keys():
                ds_cols[str(c).lower()] = str(c)
    
    # 1. Non-nullable columns
    nn = rules.get("non_nullable") or []
    for col in nn:
        if "." in col:
            parts = col.split(".")
            if len(parts) >= 2 and parts[-2].lower() in dataset_name.lower():
                col_name = parts[-1]
            else:
                continue
        else:
            col_name = col
            if has_schema and col_name.lower() not in ds_cols:
                continue
        
        # Fix casing
        col_lower = col_name.lower()
        if has_schema and col_lower in ds_cols:
            col_name = ds_cols[col_lower]
        
        action = "fill_nulls_simple" if rules.get("never_drop_rows") else "fill_or_drop"
        tagged.append(TaggedRule(
            dataset=dataset_name,
            column=col_name,
            issue_type="nulls",
            action=action,
            provenance=RuleProvenance.BUSINESS_RULE,
            source_detail="Business rule: non-nullable requirement"
        ))
        
    # 2. Valid values
    vv = rules.get("valid_values") or {}
    for col, vals in vv.items():
        col_name = col.split(".")[-1] if "." in col else col
        if "." in col:
            parts = col.split(".")
            if not (len(parts) >= 2 and parts[-2].lower() in dataset_name.lower()):
                continue
        else:
            if has_schema and col_name.lower() not in ds_cols:
                continue
        
        # Fix casing
        col_lower = col_name.lower()
        if has_schema and col_lower in ds_cols:
            col_name = ds_cols[col_lower]
            
        tagged.append(TaggedRule(
            dataset=dataset_name,
            column=col_name,
            issue_type="invalid_lookup_value",
            action="replace_values",
            provenance=RuleProvenance.BUSINESS_RULE,
            source_detail="Business rule: valid lookup values validation",
            metadata={"valid_values": vals}
        ))
        
    # 3. Steps from notes if assessment is provided
    if assessment:
        from agent.etl_pipeline.planner import _steps_from_business_notes
        for ds, col, act, note in _steps_from_business_notes(rules, assessment):
            if ds.lower() == dataset_name.lower():
                tagged.append(TaggedRule(
                    dataset=dataset_name,
                    column=col,
                    issue_type="business_notes",
                    action=act,
                    provenance=RuleProvenance.BUSINESS_RULE,
                    source_detail=f"Business rule note: {note}"
                ))
                
    return tagged
