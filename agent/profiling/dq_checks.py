from __future__ import annotations
import re
import logging
from typing import Any, Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger("agent.profiling.dq_checks")

from agent.profiling.constants import *
from agent.profiling.contracts import FIXABILITY_BY_ISSUE_TYPE, DQ_ISSUE_RECOMMENDATIONS, _DEFAULT_REC
from agent.profiling.type_inference import detect_semantic_type, _is_actual_numeric_column
from agent.profiling.format_validators import _detect_phone_formats, _detect_date_formats
from agent.profiling.statistical_profiling import safe_nunique

# ── Engine 3.0 integration ────────────────────────────────────────────────────
try:
    from agent.feature_flags import get_flags as _get_flags
    from agent.profiling.engine3_pipeline import run_engine3_pipeline as _run_engine3
    from agent.runtime_component_manifest import log_component_startup as _log_startup
    _ENGINE3_AVAILABLE = True
    _log_startup("DQChecks", "3.1.0")
except ImportError as _e:
    _ENGINE3_AVAILABLE = False
    logger.warning("Engine3 pipeline not available: %s", _e)

def match_column_key(col_key: str, dataset_name: str, column_name: str) -> bool:
    col_key_parts = [p.strip().lower() for p in col_key.split(".")]
    ds_parts = [p.strip().lower() for p in dataset_name.split(".")]
    col_name = column_name.strip().lower()
    
    if not col_key_parts or not col_name:
        return False
        
    if col_key_parts[-1] != col_name:
        return False
        
    if len(col_key_parts) == 1:
        return True
        
    prefix_parts = col_key_parts[:-1]
    min_len = min(len(ds_parts), len(prefix_parts))
    for idx in range(1, min_len + 1):
        if ds_parts[-idx] != prefix_parts[-idx]:
            return False
            
    return True

def get_valid_values_for_column(
    business_rules: Optional[Dict[str, Any]],
    dataset_name: str,
    column_name: str
) -> Optional[List[str]]:
    if not business_rules:
        return None
    vv = business_rules.get("valid_values")
    if not vv or not isinstance(vv, dict):
        return None
    for col_key, vals in vv.items():
        if match_column_key(col_key, dataset_name, column_name):
            if isinstance(vals, list):
                return vals
            return [str(vals)]
    return None

def evaluate_custom_assertion(df: pd.DataFrame, assertion: str) -> Tuple[pd.Series, List[str]]:
    """
    Evaluates a custom assertion expression on the DataFrame.
    Returns a tuple of (boolean Series, list of referenced columns).
    """
    # 1. First attempt: standard pandas eval (very fast if it works)
    try:
        res = df.eval(assertion, engine='python')
        if isinstance(res, pd.Series):
            ref_cols = []
            for col in df.columns:
                pattern = r'\b' + re.escape(col) + r'\b'
                if re.search(pattern, assertion):
                    ref_cols.append(col)
            return res, ref_cols
    except Exception:
        pass

    # 2. Second attempt: fallback namespace evaluation
    RESERVED_WORDS = {
        "and", "or", "not", "in", "is", "if", "else", "for", "while", "def", "class",
        "import", "from", "as", "try", "except", "finally", "with", "assert", "pd", "np",
        "str", "int", "float", "bool", "list", "dict", "set", "tuple", "len", "sum", "min",
        "max", "any", "all", "true", "false", "none", "nan", "isna", "isnull", "notna", "notnull"
    }

    namespace = {
        'pd': pd,
        'np': np,
        'true': True,
        'false': False,
        'none': None,
        'True': True,
        'False': False,
        'None': None
    }
    
    sorted_cols = sorted(df.columns, key=len, reverse=True)
    sanitized_assertion = assertion
    ref_cols = []

    # First, handle backticked column references (e.g. `Email ID`)
    for col in sorted_cols:
        backticked = f"`{col}`"
        if backticked in sanitized_assertion:
            safe_var = re.sub(r'[^a-zA-Z0-9_]', '_', col)
            if not safe_var or safe_var[0].isdigit():
                safe_var = "_" + safe_var
            
            sanitized_assertion = sanitized_assertion.replace(backticked, safe_var)
            namespace[safe_var] = df[col]
            if col not in ref_cols:
                ref_cols.append(col)

    # Second, handle non-backticked column references (case-insensitive word boundary matching)
    for col in sorted_cols:
        if col.lower() in RESERVED_WORDS:
            pattern = r'\b' + re.escape(col) + r'\b'
        else:
            pattern = r'\b' + re.escape(col) + r'\b'

        if re.search(pattern, sanitized_assertion, re.IGNORECASE):
            safe_var = re.sub(r'[^a-zA-Z0-9_]', '_', col)
            if not safe_var or safe_var[0].isdigit():
                safe_var = "_" + safe_var
            
            sanitized_assertion = re.sub(pattern, safe_var, sanitized_assertion, flags=re.IGNORECASE)
            namespace[safe_var] = df[col]
            if col not in ref_cols:
                ref_cols.append(col)

    # Add any remaining columns that weren't explicitly replaced but might be in the expression
    for col in df.columns:
        safe_var = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        if not safe_var or safe_var[0].isdigit():
            safe_var = "_" + safe_var
        if safe_var not in namespace:
            namespace[safe_var] = df[col]
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
            namespace[col] = df[col]
            namespace[col.lower()] = df[col]

    try:
        res = eval(sanitized_assertion, namespace)
        if isinstance(res, pd.Series):
            return res, ref_cols
        else:
            if isinstance(res, (bool, np.bool_)):
                return pd.Series([res] * len(df), index=df.index), ref_cols
            raise ValueError(f"Custom assertion did not return a boolean series or value (returned type {type(res)})")
    except Exception as e_inner:
        raise Exception(f"Failed to evaluate custom assertion '{assertion}' (parsed as '{sanitized_assertion}'): {str(e_inner)}")

def check_custom_assertions(df: pd.DataFrame, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Evaluates custom/formula cross-column assertions.
    Unlike check_formula_rules, this does not coerce columns to numeric automatically
    unless they are already numeric, supporting string evaluations (e.g. col == 'IT').
    """
    issues = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        assertion = rule.get("assertion")
        if not assertion:
            continue
        severity = str(rule.get("severity", "medium")).lower()
        custom_msg = rule.get("message")
        
        try:
            res, ref_cols = evaluate_custom_assertion(df, assertion)
            viol_mask = ~res.fillna(False)
            viol_cnt = int(viol_mask.sum())
            if viol_cnt > 0:
                rows = df.index[viol_mask].tolist()
                msg = custom_msg or f"Custom rule violation: '{assertion}' ({viol_cnt} violations)"
                issues.append(dq_issue(
                    severity,
                    "custom_rule_violation",
                    msg,
                    column=",".join(ref_cols),
                    count=viol_cnt,
                    rows=rows,
                    sample=df.loc[viol_mask, ref_cols].head(5).to_dict(orient="records")
                ))
        except Exception as e:
            issues.append(dq_issue(
                "low",
                "custom_rule_error",
                f"Failed to evaluate custom assertion '{assertion}': {str(e)}"
            ))
    return issues

def enrich_issue_with_recommendation(issue: Dict[str, Any]) -> None:
    if issue.get("recommendation"):
        return
    issue["recommendation"] = DQ_ISSUE_RECOMMENDATIONS.get(
        issue.get("type") or "", _DEFAULT_REC
    )

def enrich_issue_with_fixability(issue: Dict[str, Any], business_rules: Optional[Dict[str, Any]] = None) -> None:
    if "gating" not in issue:
        issue["gating"] = True
        issue.setdefault("contract_warnings", []).append("gating_defaulted_for_legacy_issue")
        
    issue_type = issue.get("type") or issue.get("issue_type") or ""
    
    if issue.get("fixability_details"):
        return
        
    from agent.profiling.contracts import FIXABILITY_BY_ISSUE_TYPE, FIXABILITY_TIERS
    
    fixability = FIXABILITY_BY_ISSUE_TYPE.get(issue_type, "HUMAN_REVIEW_REQUIRED")
    
    ingestion_rules = (business_rules or {}).get("ingestion_rules") or {}
    if issue_type == "exact_duplicate_rows" and ingestion_rules.get("safe_exact_deduplication", False):
        fixability = "AUTO_FIXABLE"
        
    issue["fixability"] = fixability
    
    issue["fixability_details"] = {
        "fixability": fixability,
        "automation_allowed": fixability == "AUTO_FIXABLE",
        "requires_policy": fixability == "CONDITIONALLY_FIXABLE",
        "recommended_disposition": (
            "AUTO_CLEAN" if fixability == "AUTO_FIXABLE" else
            "QUARANTINE" if fixability == "HUMAN_REVIEW_REQUIRED" else
            "ESCALATE" if fixability == "ROOT_CAUSE_ONLY" else
            "POLICY_GATE"
        ),
        "fixability_rationale": FIXABILITY_TIERS.get(fixability, ""),
    }

def dq_issue(
    sev: str,
    typ: str,
    msg: str,
    *,
    column: Optional[str] = None,
    count: Optional[int] = None,
    rows: Optional[List[int]] = None,
    sample: Optional[List[Any]] = None,
    gating: bool = True
) -> Dict[str, Any]:
    """
    Create a normalized DQ issue record with 4-tier fixability.
    """
    from agent.profiling.contracts import FIXABILITY_BY_ISSUE_TYPE, FIXABILITY_TIERS
    
    fixability = FIXABILITY_BY_ISSUE_TYPE.get(typ, "HUMAN_REVIEW_REQUIRED")
    
    return {
        "severity": sev,
        "type": typ,
        "column": column,
        "count": count,
        "row_indexes": rows[:50] if rows else [],
        "sample_values": sample[:10] if sample else [],
        "message": msg,
        "fixability": fixability,
        "gating": gating,
        "fixability_details": {
            "fixability": fixability,
            "automation_allowed": fixability == "AUTO_FIXABLE",
            "requires_policy": fixability == "CONDITIONALLY_FIXABLE",
            "recommended_disposition": (
                "AUTO_CLEAN" if fixability == "AUTO_FIXABLE" else
                "QUARANTINE" if fixability == "HUMAN_REVIEW_REQUIRED" else
                "ESCALATE" if fixability == "ROOT_CAUSE_ONLY" else
                "POLICY_GATE"
            ),
            "fixability_rationale": FIXABILITY_TIERS.get(fixability, ""),
        }
    }

def make_json_serializable(obj: Any) -> Any:
    """Recursively convert datetime/Timestamp/System.DateTime/numpy types to JSON-safe types."""
    import pandas as pd
    import json
    
    if isinstance(obj, dict):
        return {str(k): make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(make_json_serializable(v) for v in obj)
    elif isinstance(obj, set):
        return {make_json_serializable(v) for v in obj}
    
    if not isinstance(obj, (dict, list, tuple, set)):
        if pd.isna(obj):
            return None
            
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
        
    tname = type(obj).__name__
    if tname in ("DateTime", "Timestamp", "datetime", "date"):
        try:
            return str(obj.ToString() if hasattr(obj, "ToString") else obj)
        except Exception:
            return str(obj)
            
    if hasattr(obj, "item") and callable(obj.item):
        try:
            return obj.item()
        except Exception:
            return str(obj)
            
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)

def analyze_column(
    series: pd.Series,
    col: str,
    semantic: str,
    thresholds: Optional[Dict[str, Any]] = None,
    is_priority: bool = True,
    non_nullable: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Per-column data quality checks using Great Expectations core engine.
    """
    df = pd.DataFrame({col: series})
    profile = {
        "columns": {
            col: {"semantic_type": semantic}
        }
    }
    if is_priority:
        profile["priority_columns"] = [col]
    else:
        profile["priority_columns"] = []
        
    business_rules = {}
    if non_nullable:
        business_rules["non_nullable"] = list(non_nullable)
        
    res = analyze_dataset_quality(
        name="temp_dataset",
        df=df,
        profile=profile,
        thresholds=thresholds,
        business_rules=business_rules
    )
    return make_json_serializable(res.get("issues") or [])

def analyze_dataset_quality(
    name: str,
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    approved_semantics: Optional[Dict[str, str]] = None,
    relationship_issues: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Validation engine powered by Great Expectations.
    Runs validations using GX and translates the failed expectations to the unified issues format.
    """
    from agent.specialists.gx_validation_specialist import run_gx_validation
    
    thresholds = thresholds or {}
    business_rules = business_rules or {}
    issues: List[Dict[str, Any]] = []
    n = len(df)

    # Build declared key set from thresholds declared relationships
    declared_key_set = set()
    rel_cfg = thresholds.get("relationships") or {}
    for rel_meta in rel_cfg.get("declared", []):
        p_ds = rel_meta.get("parent", {}).get("dataset")
        p_cols = rel_meta.get("parent", {}).get("columns") or []
        c_ds = rel_meta.get("child", {}).get("dataset")
        c_cols = rel_meta.get("child", {}).get("columns") or []
        
        if p_ds == name:
            for col_col in p_cols:
                declared_key_set.add(str(col_col).lower())
        if c_ds == name:
            for col_col in c_cols:
                declared_key_set.add(str(col_col).lower())

    # Build declared non-negative set from thresholds formula_rules
    declared_non_negative_set = set()
    configured_non_negative_severity = "high"
    for rule in thresholds.get("formula_rules", []):
        assertion = rule.get("assertion") or ""
        if ">= 0" in assertion:
            col_part = assertion.split(">=")[0].strip()
            declared_non_negative_set.add(col_part.lower())
            configured_non_negative_severity = rule.get("severity", "high")
    
    # 1. Run Great Expectations validation
    try:
        gx_res = run_gx_validation(
            datasets={name: df},
            profile_results={"datasets": {name: profile}},
            thresholds=thresholds,
            business_rules=business_rules
        )
    except Exception as e:
        logger.error(f"GX Validation call failed for {name}: {e}")
        gx_res = {}
        
    ds_res = gx_res.get(name) or {}
    results = ds_res.get("results") or []
    
    # Non-nullable set for severity mapping
    non_nullable_set = set()
    nn_list = business_rules.get("non_nullable") or []
    req_list = business_rules.get("required_columns") or []
    for c in nn_list:
        non_nullable_set.add(str(c).lower())
    for c in req_list:
        non_nullable_set.add(str(c).lower())
    for col_name, col_meta in profile.get("columns", {}).items():
        if col_meta.get("nullable") == "NO" or col_meta.get("candidate_primary_key") is True:
            non_nullable_set.add(str(col_name).lower())
            
    # Map failed results to legacy DQ issues
    for r in results:
        if r.get("success"):
            continue
            
        col = r.get("column") or "-"
        exp = str(r.get("expectation") or "").lower()
        unexp_cnt = int(r.get("unexpected_count") or 0)
        unexp_idx = r.get("unexpected_index_list") or []
        unexp_vals = r.get("unexpected_values") or []
        details = r.get("details") or ""
        
        # Heuristically determine severity and issue type
        severity = "medium"
        issue_type = "custom_rule_violation"
        gating = True
        if details and details != "Validation failed for some values.":
            msg = f"{col}: {details} ({exp})" if exp else f"{col}: {details}"
        elif exp:
            msg = f"{col}: expectation '{exp}' failed ({unexp_cnt} invalid)" if unexp_cnt > 0 else f"{col}: expectation '{exp}' failed."
        else:
            msg = f"{col}: expectation failed."
        
        if exp == "placeholder_detected":
            issue_type = "placeholder_detected"
            severity = "medium"
            msg = f"{unexp_cnt} placeholder/semantic null value(s) (e.g. '???', 'N/A')" if unexp_cnt > 0 else "Placeholder value(s) found"
        elif "not be null" in exp:
            col_lower = str(col).lower()
            null_pct = unexp_cnt / max(1, n)
            gating = False

            if col_lower in declared_key_set:
                severity = "high"
                gating = True
                configured_high_ratio = 0.05  # 5%
                if null_pct >= configured_high_ratio:
                    issue_type = "declared_key_high_null_ratio"
                    msg = (
                        f"{unexp_cnt} null value(s) in declared key column '{col}' (high null ratio: {null_pct:.1%}). "
                        f"Observed ratio: {null_pct:.1%}, High-ratio threshold: {configured_high_ratio:.1%}, Rule source: declared"
                    )
                else:
                    issue_type = "declared_key_null_violation"
                    msg = (
                        f"{unexp_cnt} null value(s) in declared key column '{col}' (null violation). "
                        f"Observed ratio: {null_pct:.1%}, High-ratio threshold: {configured_high_ratio:.1%}, Rule source: declared"
                    )
            elif col_lower in non_nullable_set:
                severity = "high"
                issue_type = "nulls"
                gating = True
                msg = f"{unexp_cnt} null value(s) in declared non-nullable column '{col}'"


            elif profile.get("columns", {}).get(col, {}).get("nullable") == "NO":
                severity = "high"
                issue_type = "nulls"
                gating = True
                msg = f"{unexp_cnt} null value(s) in database-defined NOT NULL column '{col}'"
            elif profile.get("columns", {}).get(col, {}).get("semantic_type") in {"identifier", "primary_key", "foreign_key"}:
                if null_pct >= 0.50:
                    severity = "high"
                    issue_type = "suspected_key_high_null_ratio"
                    gating = False
                elif null_pct >= 0.20:
                    severity = "medium"
                    issue_type = "suspected_key_high_null_ratio"
                    gating = False
                else:
                    severity = "low"
                    issue_type = "nulls"
                msg = f"{unexp_cnt} null value(s) in semantic key column '{col}'"
            elif any(col_lower.endswith(s) or col_lower == s for s in ("_id", "_key", "_code", "id")):
                if null_pct >= 0.50:
                    severity = "medium"
                    issue_type = "suspected_key_high_null_ratio"
                    gating = False
                else:
                    severity = "low"
                    issue_type = "nulls"
                msg = f"{unexp_cnt} null value(s) in suspected key column '{col}'"
            else:
                severity = "low"
                issue_type = "nulls"
                msg = f"{unexp_cnt} null value(s)" if unexp_cnt > 0 else "Null value(s) found"
        elif "be unique" in exp:
            col_lower = str(col).lower()
            col_is_declared_key = col_lower in declared_key_set
            col_is_inferred_pk = (profile.get("columns", {}).get(col, {})
                                  .get("candidate_primary_key", False))
            
            if col_is_declared_key:
                issue_type = "declared_key_violation"
                severity = "high"
                gating = True
                msg = f"{unexp_cnt} duplicate value(s) on declared key column '{col}'"
            elif col_is_inferred_pk:
                issue_type = "candidate_key_violation"
                severity = "high"
                gating = False
                msg = (f"{unexp_cnt} duplicate value(s) on inferred candidate key '{col}'. "
                       f"Key role is inferred, not declared — verify before treating as blocker.")
            else:
                issue_type = "exact_duplicate_rows"
                severity = "medium"
                gating = False
                msg = f"{unexp_cnt} exact duplicate row(s) detected"
        elif "compound columns to be unique" in exp or "compound_columns_to_be_unique" in exp:
            issue_type = "exact_duplicate_rows"
            severity = "medium"
            gating = False
            msg = f"{unexp_cnt} duplicate row(s)" if unexp_cnt > 0 else "Duplicate rows found"
        elif exp == "string_with_only_digits_in_text_column":
            col_l = str(col).lower()
            if col_l in ("country", "country_code", "customer_id", "order_id", "payment_id", "status", "date"):
                continue
            is_num_col = any(kw in col_l for kw in ["qty", "quantity", "amount", "budget", "price", "sum", "count"])
            if is_num_col:
                issue_type = "numeric_value_serialized_as_string"
                severity = "medium"
                msg = f"{unexp_cnt} numeric value(s) serialized as string in column '{col}'"
            else:
                issue_type = "string_with_only_digits_in_text_column"
                severity = "medium"
                msg = f"{unexp_cnt} digit string(s) in categorical column '{col}'"
        elif exp == "invalid_numeric_values":
            col_l = str(col).lower()
            if col_l in ("country", "country_code", "customer_id", "order_id", "payment_id", "status", "date"):
                continue
            issue_type = "invalid_numeric"
            severity = "medium"
            msg = f"{unexp_cnt} non-numeric value(s) in numeric column '{col}'"
        elif exp == "invalid_date_format":
            issue_type = "invalid_date_format"
            severity = "medium"
            msg = f"{unexp_cnt} invalid date/sentinel date value(s) in column '{col}'"
        elif "be in type list" in exp or "be of type" in exp or "invalid_numeric" in exp or exp == "invalid_numeric_values":
            col_l = str(col).lower()
            if col_l in ("country", "country_code", "customer_id", "order_id", "payment_id", "status", "date"):
                continue
            issue_type = "invalid_numeric"
            severity = "high" if any(k in col_l for k in ("amount", "price", "total", "cost")) else "medium"
            msg = f"{unexp_cnt} non-numeric value(s) in numeric column '{col}'" if unexp_cnt > 0 else "Type mismatch"

        elif exp == "mixed_scalar_types":
            issue_type = "mixed_scalar_types"
            severity = "medium"
            msg = details or f"{unexp_cnt} mixed scalar types detected"
        elif exp == "internal_whitespace":
            issue_type = "internal_whitespace"
            severity = "low"
            msg = f"{unexp_cnt} value(s) with consecutive spaces"
        elif "whitespace" in exp:
            issue_type = "whitespace"
            severity = "low"
            msg = f"{unexp_cnt} leading/trailing spaces"
        elif exp == "html_tags_in_text":
            issue_type = "html_tags_in_text"
            severity = "medium"
            msg = f"{unexp_cnt} value(s) containing HTML tags"
        elif exp == "punctuation_only_value":
            issue_type = "punctuation_only_value"
            severity = "medium"
            msg = f"{unexp_cnt} punctuation-only value(s)"
        elif exp == "invalid_email":
            issue_type = "invalid_email"
            severity = "medium"
            msg = f"{unexp_cnt} invalid email(s)"
        elif exp == "invalid_phone":
            issue_type = "invalid_phone"
            severity = "medium"
            msg = f"{unexp_cnt} invalid phone number(s)"
        elif exp == "invalid_uuid":
            issue_type = "invalid_uuid"
            severity = "high"
            msg = f"{unexp_cnt} value(s) do not match UUID format"
        elif "match regex" in exp or "match_regex" in exp:
            if "email" in exp:
                issue_type = "invalid_email"
                severity = "medium"
                msg = f"{unexp_cnt} invalid email(s)"
            elif "phone" in exp:
                issue_type = "invalid_phone"
                severity = "medium"
                msg = f"{unexp_cnt} invalid phone number(s)"
            elif "uuid" in exp:
                issue_type = "invalid_uuid"
                severity = "high"
                msg = f"{unexp_cnt} value(s) do not match UUID format"
            elif "url" in exp or (col and "url" in str(col).lower()):
                issue_type = "invalid_url"
                severity = "medium"
                msg = f"{unexp_cnt} structurally invalid URL(s)"
            else:
                issue_type = "custom_regex"
                severity = "medium"
                msg = f"{unexp_cnt} value(s) failed regex check"
        elif "dateutil parseable" in exp:
            issue_type = "invalid_date_format"
            severity = "medium"
            msg = f"{unexp_cnt} bad date(s) (failed parsing)"
        elif exp == "negative_values":
            col_lower = str(col).lower()
            sem_type  = profile.get("columns", {}).get(col, {}).get("semantic_type", "")
            gating    = False

            BIPOLAR_SEMANTICS   = {"amount", "revenue", "balance", "profit", "adjustment",
                                   "margin", "credit", "debit"}
            PHYSICAL_SEMANTICS  = {"quantity", "count", "duration", "age"}

            if col_lower in declared_non_negative_set:
                issue_type = "declared_non_negative_rule_violation"
                severity = configured_non_negative_severity
                gating = True
                msg = f"{unexp_cnt} negative value(s) violating declared non-negative rule in '{col}'"
            elif sem_type in BIPOLAR_SEMANTICS:
                issue_type = "negative_values_review_required"
                severity = "low"
                gating = False
                msg = (f"{unexp_cnt} negative value(s) in '{col}'. Column has a bipolar-financial "
                       "semantic type. Negatives may represent refunds/credits/reversals. "
                       "Validate against transaction_type before rejecting.")
            elif sem_type in PHYSICAL_SEMANTICS:
                issue_type = "negative_values_in_non_negative_measure"
                severity = "high"
                gating = True
                msg = f"{unexp_cnt} negative value(s) in '{col}' — physically impossible for quantity/count/age"
            elif any(k in col_lower for k in ("amount","revenue","balance","profit",
                                               "margin","credit","debit","adjustment")):
                issue_type = "negative_values_review_required"
                severity = "low"
                gating = False
                msg = (f"{unexp_cnt} negative value(s) in '{col}'. Name suggests financial field — "
                       "may be valid (refunds/credits). Confirm with domain owner.")
            elif any(k in col_lower for k in ("qty","quantity","count","age","duration")):
                issue_type = "negative_values_in_non_negative_measure"
                severity = "medium"
                gating = False
                msg = f"{unexp_cnt} negative value(s) in '{col}' — unexpected for quantity/count fields"
            else:
                issue_type = "negative_values_review_required"
                severity = "medium"
                gating = False
                msg = f"{unexp_cnt} negative value(s) — domain unknown, review required"
        elif "be between" in exp:
            if "age" in str(col).lower():
                issue_type = "implausible_age"
                severity = "high"
                msg = f"{unexp_cnt} age value(s) outside range 0-150"
            elif any(k in str(col).lower() for k in ("percent", "pct", "rate", "ratio", "share")):
                issue_type = "implausible_percentage"
                severity = "medium"
                msg = f"{unexp_cnt} value(s) outside expected 0-100% range"
            else:
                issue_type = "out_of_range"
                severity = "medium"
                msg = f"{unexp_cnt} value(s) outside expected range"
        elif exp == "suspicious_zero":
            issue_type = "suspicious_zero"
            severity = "medium"
            msg = f"{unexp_cnt} suspicious zero(s) in ID column"
        elif "not be in set" in exp or exp == "sentinel_numeric_value":
            issue_type = "sentinel_numeric_value"
            severity = "medium"
            msg = details or f"{unexp_cnt} sentinel/magic number(s) detected"
        elif exp == "conditional_not_null":
            issue_type = "conditional_not_null"
            severity = "medium"
            msg = details
        elif exp == "conditional_range":
            issue_type = "conditional_range"
            severity = "medium"
            msg = details
        elif exp == "formula_rule_violation":
            issue_type = "formula_rule_violation"
            severity = "medium"
            msg = details
        elif exp == "date_range_violation":
            issue_type = "date_range_violation"
            severity = "high"
            msg = details
        elif exp == "invalid_lookup_value":
            issue_type = "invalid_lookup_value"
            severity = "medium"
            msg = details
        elif exp == "near_duplicate_rows":
            issue_type = "near_duplicate_rows"
            severity = "medium"
            msg = details
        elif exp == "intra_dataset_orphan_fk":
            issue_type = "intra_dataset_orphan_fk"
            severity = "high"
            msg = details
        elif exp == "multivariate_outliers":
            issue_type = "multivariate_outliers"
            severity = "medium"
            msg = details
        elif exp == "functional_dependency_violation":
            issue_type = "functional_dependency_violation"
            severity = "medium"
            msg = details
        elif exp == "mixed_date_formats":
            issue_type = "mixed_date_formats"
            severity = "medium"
            msg = details
        elif exp == "all_caps_values":
            issue_type = "all_caps_values"
            severity = "low"
            msg = details
        elif exp == "duplicate_uuid":
            issue_type = "duplicate_uuid"
            severity = "high"
            msg = details
        elif exp == "ambiguous_boolean":
            issue_type = "ambiguous_boolean"
            severity = "medium"
            msg = details
        elif exp == "custom_rule_violation":
            issue_type = "custom_rule_violation"
            severity = "medium"
            msg = details
        # --- Track B: Numeric Anomalies ---
        elif exp == "numeric_outliers_iqr":
            issue_type = "numeric_outliers_iqr"
            severity = "medium"
            msg = details
        elif exp == "numeric_outliers_zscore":
            issue_type = "numeric_outliers_zscore"
            severity = "high"
            msg = details
        elif exp == "low_variance_numeric":
            issue_type = "low_variance_numeric"
            severity = "low"
            msg = details
        elif exp == "numeric_precision_anomaly":
            issue_type = "numeric_precision_anomaly"
            severity = "low"
            msg = details
        elif exp == "round_number_anomaly":
            issue_type = "round_number_anomaly"
            severity = "low"
            msg = details
        # --- Track B: Date/Time Anomalies ---
        elif exp == "future_dates":
            issue_type = "future_dates"
            severity = "high"
            msg = details
        elif exp == "ancient_dates":
            issue_type = "ancient_dates"
            severity = "medium"
            msg = details
        elif exp == "very_wide_date_span":
            issue_type = "very_wide_date_span"
            severity = "low"
            msg = details
        elif exp == "date_clumping_jan1":
            issue_type = "date_clumping_jan1"
            severity = "medium"
            msg = details
        elif exp == "date_clumping_month_end":
            issue_type = "date_clumping_month_end"
            severity = "low"
            msg = details
        elif exp == "weekend_date_anomaly":
            issue_type = "weekend_date_anomaly"
            severity = "low"
            msg = details
        elif exp == "timezone_inconsistency":
            issue_type = "timezone_inconsistency"
            severity = "medium"
            msg = details
        # --- Track B: Text Anomalies ---
        elif exp == "non_ascii_characters":
            issue_type = "non_ascii_characters"
            severity = "low"
            msg = details
        elif exp == "control_characters_in_text":
            issue_type = "control_characters_in_text"
            severity = "medium"
            msg = details
        elif exp == "string_length_outlier":
            issue_type = "string_length_outlier"
            severity = "low"
            msg = details
        elif exp == "string_with_only_digits_in_text_column":
            is_num_col = any(kw in str(col).lower() for kw in ["qty", "quantity", "amount", "budget", "price", "sum", "count"])
            if is_num_col:
                issue_type = "numeric_value_serialized_as_string"
                severity = "medium"
                msg = f"Numeric value serialized as string in numeric column '{col}': {details}"
            else:
                issue_type = "string_with_only_digits_in_text_column"
                severity = "medium"
                msg = details
        elif exp == "repeated_token_in_string":
            issue_type = "repeated_token_in_string"
            severity = "low"
            msg = details
        # --- Track B: Semantic Checks ---
        elif exp == "implausible_age":
            issue_type = "implausible_age"
            severity = "high"
            msg = details
        elif exp == "implausible_percentage":
            issue_type = "implausible_percentage"
            severity = "medium"
            msg = details
        elif exp == "duplicate_insensitive_values":
            issue_type = "duplicate_insensitive_values"
            severity = "low"
            msg = details
            
        dup_metrics = None
        if issue_type in {"exact_duplicate_rows", "declared_key_violation", "candidate_key_violation", "duplicate_primary_key"}:
            try:
                import json
                def _stable_scalar(v):
                    if isinstance(v, (dict, list)):
                        return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
                    if pd.isna(v):
                        return "__NULL__"
                    return v

                normalized_for_dup = df.map(_stable_scalar)
                if issue_type == "exact_duplicate_rows" and col == "-":
                    group_sizes = normalized_for_dup.value_counts(dropna=False)
                elif col in df.columns:
                    group_sizes = normalized_for_dup[col].value_counts(dropna=False)
                else:
                    group_sizes = normalized_for_dup.value_counts(dropna=False)

                dup_groups = group_sizes[group_sizes > 1]

                dup_metrics = {
                    "group_count": int(len(dup_groups)),
                    "rows_in_groups": int(dup_groups.sum()),
                    "redundant_rows": int(dup_groups.sum() - len(dup_groups)),
                    "largest_group_size": int(dup_groups.max()) if len(dup_groups) else 0,
                }
            except Exception as e:
                logger.warning(f"Error computing duplicate group metrics: {e}")
                dup_metrics = {
                    "group_count": 0,
                    "rows_in_groups": 0,
                    "redundant_rows": 0,
                    "largest_group_size": 0,
                }

        if issue_type == "exact_duplicate_rows" and dup_metrics:
            msg = (
                f"{unexp_cnt} exact duplicate row(s) detected. "
                f"Duplicate groups: {dup_metrics['group_count']}, "
                f"Rows in duplicate groups: {dup_metrics['rows_in_groups']}, "
                f"Redundant rows: {dup_metrics['redundant_rows']}, "
                f"Largest group size: {dup_metrics['largest_group_size']}"
            )

        issues.append({
            "severity": severity,
            "type": issue_type,
            "column": col,
            "count": unexp_cnt,
            "row_indexes": unexp_idx[:50],
            "sample_values": unexp_vals[:10],
            "message": msg,
            "fixability": FIXABILITY_BY_ISSUE_TYPE.get(issue_type, "COMPLEX"),
            "gating": gating,
            "duplicate_group_metrics": dup_metrics
        })
        
    # India-specific validation DQ issue generation (Component 13)
    from agent.validators.india_domain import INDIA_COLUMN_PATTERNS
    for col_name in df.columns:
        col_lower = col_name.lower()
        matched_validator = None
        issue_type = None
        label = None
        
        for pattern, validator in INDIA_COLUMN_PATTERNS.items():
            if pattern == col_lower or f"_{pattern}" in col_lower or f"{pattern}_" in col_lower:
                matched_validator = validator
                issue_type = f"invalid_{pattern}"
                label = pattern.upper()
                break
                
        if matched_validator:
            series = df[col_name].dropna()
            if not series.empty:
                failures = []
                fail_indexes = []
                for idx, v in series.items():
                    val_str = str(v)
                    if not matched_validator(val_str):
                        failures.append(val_str)
                        fail_indexes.append(idx)
                
                fail_count = len(failures)
                if fail_count > 0:
                    issues.append({
                        "severity": "medium" if "aadhaar" not in issue_type else "high",
                        "type": issue_type,
                        "column": col_name,
                        "count": fail_count,
                        "row_indexes": list(fail_indexes)[:50],
                        "sample_values": list(dict.fromkeys(failures))[:10],
                        "message": f"{fail_count} invalid {label} value(s) in column '{col_name}'",
                        "fixability": "COMPLEX"
                    })

        # Check disposable email (Gap 11)
        if "email" in col_lower:
            series = df[col_name].dropna()
            if not series.empty:
                from agent.profiling.format_validators import is_disposable_email
                disposables = []
                disp_indexes = []
                for idx, v in series.items():
                    val_str = str(v)
                    if is_disposable_email(val_str):
                        disposables.append(val_str)
                        disp_indexes.append(idx)
                if disposables:
                    issues.append({
                        "severity": "low",
                        "type": "disposable_email",
                        "column": col_name,
                        "count": len(disposables),
                        "row_indexes": list(disp_indexes)[:50],
                        "sample_values": list(dict.fromkeys(disposables))[:10],
                        "message": f"{len(disposables)} disposable email address(es) detected in '{col_name}'",
                        "fixability": "COMPLEX"
                    })

        # Check mojibake (Gap 12)
        dtype_str = str(df[col_name].dtype).lower()
        if "object" in dtype_str or "str" in dtype_str or "category" in dtype_str:
            series = df[col_name].dropna()
            if not series.empty:
                from agent.profiling.format_validators import has_mojibake
                mojibakes = []
                moji_indexes = []
                for idx, v in series.items():
                    if has_mojibake(v):
                        mojibakes.append(v)
                        moji_indexes.append(idx)
                if mojibakes:
                    issues.append({
                        "severity": "medium",
                        "type": "encoding_corruption",
                        "column": col_name,
                        "count": len(mojibakes),
                        "row_indexes": list(moji_indexes)[:50],
                        "sample_values": list(dict.fromkeys(mojibakes))[:10],
                        "message": f"{len(mojibakes)} encoding corruption (mojibake) value(s) in '{col_name}'",
                        "fixability": "COMPLEX"
                    })
        
    # ── PII scanning — route to privacy pipeline, NOT to quality issues ─────
    # Engine 3.0: PII findings are separated from DQ scores (privacy_separation flag).
    # They are collected here and passed to run_engine3_pipeline() which routes them
    # to privacy_findings. Legacy mode: still appended to issues for backward compat.
    _pii_raw_findings: List[Dict[str, Any]] = []
    try:
        from agent.pii_masking import scan_dataframe_for_pii
        pii_scan = scan_dataframe_for_pii(df)
        for f in (pii_scan or []):
            pii_t = f.get("pii_type")
            issue_type = (
                "pii_email" if pii_t == "email" else
                "pii_phone" if pii_t == "phone" else
                "pii_sensitive"
            )
            pii_rec = {
                "severity": str(f.get("severity") or "medium").lower(),
                "type": issue_type,
                "column": f.get("column") or "",
                "count": f.get("matches_in_sample") or 1,
                "row_indexes": [],
                "sample_values": [],
                "message": f.get("message") or f"Embedded PII detected in column '{f.get('column')}'",
                "source": "pii_scanner",
                "fixability": "COMPLEX",
                "pipeline": "privacy",
            }
            _pii_raw_findings.append(pii_rec)
            # Legacy mode: also append to main issues list for backward compatibility
            _legacy_mode = not _ENGINE3_AVAILABLE or (
                _ENGINE3_AVAILABLE and not _get_flags().privacy_score_separation
            )
            if _legacy_mode:
                issues.append(pii_rec)
    except Exception as e:
        logger.warning("PII content scanning failed for %s: %s", name, e)

    # Append relationship issues (orphans) to local issues list
    if relationship_issues:
        for iss in relationship_issues:
            issues.append(iss)

    # Calculate Scorecard Summary Metrics
    try:
        score_cfg = thresholds.get("dq_score") or {}
        w = score_cfg.get("weights") or {}
        wh = float(w.get("high", 3.0))
        wm = float(w.get("medium", 1.0))
        wl = float(w.get("low", 0.3))

        # Build mutually exclusive row severity buckets for GATING issues
        highest_sev_by_row = {}
        SEV_RANK = {"high": 3, "medium": 2, "low": 1}
        synth_idx = n
        has_synthesized = False

        for it in issues:
            if not it.get("gating", True):
                continue
            
            sev = str(it.get("severity") or "low").lower()
            sev_r = SEV_RANK.get(sev, 0)
            rows = it.get("row_indexes") or []
            cnt = int(it.get("count") or len(rows) or 1)
            cnt = min(cnt, n)

            explicit_rows = set(rows)
            for r_idx in explicit_rows:
                cur_sev = highest_sev_by_row.get(r_idx)
                cur_r = SEV_RANK.get(cur_sev, 0)
                if sev_r > cur_r:
                    highest_sev_by_row[r_idx] = sev

            remaining_cnt = cnt - len(explicit_rows)
            if remaining_cnt > 0:
                for r_idx in range(synth_idx, synth_idx + remaining_cnt):
                    cur_sev = highest_sev_by_row.get(r_idx)
                    cur_r = SEV_RANK.get(cur_sev, 0)
                    if sev_r > cur_r:
                        highest_sev_by_row[r_idx] = sev
                synth_idx += remaining_cnt
                has_synthesized = True

        high_rows   = {i for i, s in highest_sev_by_row.items() if s == "high"}
        medium_rows = {i for i, s in highest_sev_by_row.items() if s == "medium"}
        low_rows    = {i for i, s in highest_sev_by_row.items() if s == "low"}

        if has_synthesized:
            import logging
            logging.getLogger("agent.profiling.dq_checks").debug(
                f"DQ scoring: count-based index synthesis used for issues without row_indexes (dataset size: {n})"
            )

        # Flat penalty based on issue diversity of gating issues
        num_high_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "high" and i.get("gating", True))
        num_med_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "medium" and i.get("gating", True))
        num_low_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "low" and i.get("gating", True))
        
        flat_penalty = min(15.0, (2.5 * num_high_types) + (0.5 * num_med_types) + (0.1 * num_low_types))

        # Row fraction penalty
        frac_h = min(1.0, len(high_rows) / max(1, n))
        frac_m = min(1.0, len(medium_rows) / max(1, n))
        frac_l = min(1.0, len(low_rows) / max(1, n))

        row_penalty = (30.0 * frac_h) + (15.0 * frac_m) + (5.0 * frac_l)
        
        total_penalty = flat_penalty + row_penalty
        dq_score = max(0.0, min(100.0, 100.0 - total_penalty))

        clean_est_high = max(0, n - len(high_rows))
        clean_est_high_med = max(0, n - len(high_rows.union(medium_rows)))

        # Score components for ledger reconciliation
        score_components = {
            "flat_penalty": round(flat_penalty, 4),
            "row_penalty": round(row_penalty, 4),
            "total_penalty": round(total_penalty, 4),
            "weights": {
                "high_flat_per_type": 2.5, "medium_flat_per_type": 0.5, "low_flat_per_type": 0.1,
                "high_row_fraction": 30.0, "medium_row_fraction": 15.0, "low_row_fraction": 5.0,
                "flat_penalty_cap": 15.0
            },
            "high_issue_type_count": num_high_types,
            "medium_issue_type_count": num_med_types,
            "distinct_high_rows": len(high_rows),
            "distinct_medium_rows": len(medium_rows),
            "distinct_low_rows": len(low_rows),
            "row_severity_buckets_are_mutually_exclusive": True,
            "advisory_issues_included_in_score": False,
            "row_count": n
        }

        # Dimension breakdown calculations
        # 1. Completeness
        total_cols = len(df.columns)
        evaluated_cells = n * total_cols
        null_issues_cnt = sum(int(i.get("count") or 1) for i in issues if i.get("type") in {
            "nulls", "empty_string_values", "placeholder_detected",
            "declared_key_high_null_ratio", "declared_key_null_violation", "suspected_key_high_null_ratio"
        })

        passed_cells = max(0, evaluated_cells - null_issues_cnt)
        completeness = {
            "score": round((passed_cells / max(1, evaluated_cells)) * 100.0, 1),
            "passed_cells": passed_cells,
            "evaluated_cells": evaluated_cells,
            "excluded_cells": 0
        }

        # 2. Validity
        total_exp = len(results)
        passed_exp = sum(1 for r in results if r.get("success"))
        validity = {
            "score": round((passed_exp / max(1, total_exp)) * 100.0, 1) if total_exp > 0 else 100.0,
            "passed_expectations": passed_exp,
            "evaluated_expectations": total_exp
        }

        # 3. Uniqueness
        redundant_rows_sum = 0
        for i in issues:
            if i.get("duplicate_group_metrics"):
                redundant_rows_sum += i["duplicate_group_metrics"].get("redundant_rows", 0)
        uniqueness = {
            "score": round((max(0, n - redundant_rows_sum) / max(1, n)) * 100.0, 1),
            "duplicate_rows": redundant_rows_sum,
            "total_rows": n
        }

        # 4. Consistency
        consistency_issues_cnt = sum(int(i.get("count") or 1) for i in issues if i.get("type") in {
            "formula_rule_violation", "custom_rule_violation", "custom_one_of", "custom_range",
            "custom_regex", "custom_not_null", "orphan_foreign_key_rows"
        })
        total_consistency_rules = len(thresholds.get("formula_rules") or []) + len(thresholds.get("custom_rules") or [])
        total_consistency_rules += len(relationship_issues or [])
        if total_consistency_rules == 0:
            consistency = None
        else:
            passed_rules = max(0, total_consistency_rules - consistency_issues_cnt)
            consistency = {
                "score": round((passed_rules / max(1, total_consistency_rules)) * 100.0, 1),
                "cross_field_violations": consistency_issues_cnt,
                "evaluated_rules": total_consistency_rules
            }


        # 5. Timeliness
        date_cols = [c for c in df.columns if "date" in str(c).lower() or "time" in str(c).lower()]
        if date_cols:
            evaluated_date_cells = n * len(date_cols)
            date_issues_cnt = sum(int(i.get("count") or 1) for i in issues if i.get("type") in {
                "invalid_date_format", "mixed_date_formats", "future_dates", "ancient_dates", "impossible_date"
            })
            passed_date_cells = max(0, evaluated_date_cells - date_issues_cnt)
            timeliness = {
                "score": round((passed_date_cells / max(1, evaluated_date_cells)) * 100.0, 1),
                "valid_date_cells": passed_date_cells,
                "evaluated_date_cells": evaluated_date_cells
            }
        else:
            timeliness = None

        dimension_breakdown = {
            "completeness": completeness,
            "validity": validity,
            "uniqueness": uniqueness,
            "consistency": consistency,
            "timeliness": timeliness
        }

        # Root-Cause Grouping (P5)
        ROOT_CAUSE_FAMILIES = {
            "missing_or_null": {
                "nulls", "empty_string_values", "placeholder_detected",
                "declared_key_high_null_ratio", "declared_key_null_violation", "suspected_key_high_null_ratio"
            },

            "format_invalid": {
                "invalid_numeric", "mixed_scalar_types", "invalid_date_format",
                "mixed_date_formats", "impossible_date", "invalid_email", "invalid_uuid"
            },
            "content_anomaly": {
                "punctuation_only_value", "string_with_only_digits_in_text_column",
                "sentinel_numeric_value", "negative_values", "negative_values_review_required",
                "negative_values_in_non_negative_measure"
            },
            "uniqueness": {
                "exact_duplicate_rows", "declared_key_violation", "candidate_key_violation",
                "duplicate_primary_key", "duplicate_rows", "near_duplicate_rows"
            },
            "referential": {
                "orphan_foreign_key_rows", "orphan_foreign_key"
            },
            "casing_style": {
                "case_inconsistency", "whitespace", "internal_whitespace",
                "all_caps_values", "duplicate_representation_candidate"
            }
        }

        def get_family(itype: str) -> str:
            for fam, types in ROOT_CAUSE_FAMILIES.items():
                if itype in types:
                    return fam
            return "other"

        global_cells = set()
        global_rows = set()
        family_cells_map = {f: set() for f in ROOT_CAUSE_FAMILIES.keys()}
        family_rows_map = {f: set() for f in ROOT_CAUSE_FAMILIES.keys()}
        family_types_map = {f: set() for f in ROOT_CAUSE_FAMILIES.keys()}
        family_cells_map["other"] = set()
        family_rows_map["other"] = set()
        family_types_map["other"] = set()

        for it in issues:
            itype = it.get("type") or it.get("issue_type") or "other"
            fam = get_family(itype)
            
            rows = it.get("row_indexes") or []
            cols = it.get("columns") or []
            if not cols and it.get("column"):
                cols = [it["column"]]
            if not cols:
                cols = ["__global__"]
                
            family_types_map[fam].add(itype)
            
            for r in rows:
                global_rows.add(r)
                family_rows_map[fam].add(r)
                for c in cols:
                    global_cells.add((r, c))
                    family_cells_map[fam].add((r, c))

        root_cause_groups = []
        for fam in sorted(family_types_map.keys()):
            if family_types_map[fam]:
                root_cause_groups.append({
                    "family": fam,
                    "issue_types_triggered": sorted(list(family_types_map[fam])),
                    "distinct_cells_affected": len(family_cells_map[fam]),
                    "distinct_rows_affected": len(family_rows_map[fam])
                })

        root_cause_summary = {
            "rule_hit_count": len(issues),
            "distinct_issue_types": len({it.get("type") or it.get("issue_type") for it in issues if it.get("type") or it.get("issue_type")}),
            "distinct_rows_affected": len(global_rows),
            "distinct_cells_affected": len(global_cells),
            "root_cause_family_count": len(root_cause_groups),
            "family_counts_are_non_additive": True,
            "root_cause_groups": root_cause_groups
        }

    except Exception as e:
        logger.warning(f"DQ scoring failed: {e}")
        dq_score = None
        clean_est_high = None
        clean_est_high_med = None
        score_components = {}
        dimension_breakdown = {}
        root_cause_summary = {}

    from agent.etl_pipeline.dq_issue_schema import normalize_issue_dict
    issues = [normalize_issue_dict(i, name, "profiler") for i in issues]

    from agent.profiling.issue_contract import validate_issue

    # Map gating flag and duplicate_group_metrics to normalized issues
    for i, issue_data in enumerate(issues):
        orig = issues[i]
        orig["gating"] = issues[i].get("gating", True)
        orig["duplicate_group_metrics"] = issues[i].get("duplicate_group_metrics")
        orig["issue_type"] = orig.get("type") or orig.get("issue_type") or "unknown"
        orig["rule_source"] = orig.get("rule_source") or "built_in"
        orig.setdefault("fixability_details", {
            "fixability": orig.get("fixability", "HUMAN_REVIEW_REQUIRED"),
            "automation_allowed": orig.get("fixability") == "AUTO_FIXABLE",
            "requires_policy": orig.get("fixability") == "CONDITIONALLY_FIXABLE",
            "recommended_disposition": "QUARANTINE"
        })
        validate_issue(orig)

    # ── Engine 3.0 Post-Processing Pipeline ──────────────────────────────────
    # This is the integration point that makes Engine 3.0 run in production.
    # Handles: PII separation, finding deduplication, dimension scoring, manifest.
    engine3_result: Dict[str, Any] = {}
    if _ENGINE3_AVAILABLE and _get_flags().dq_engine_v3:
        try:
            # Combine raw issues with any collected PII findings for pipeline routing
            all_issues_for_pipeline = issues + _pii_raw_findings
            engine3_result = _run_engine3(
                name=name,
                df=df,
                raw_issues=all_issues_for_pipeline,
                gx_results_raw=results,   # raw GX result list from run_gx_validation
                profile=profile,
                thresholds=thresholds,
                relationship_issues=relationship_issues,
            )
            # Replace the issues list with the deduped, privacy-separated quality issues
            issues = engine3_result.get("quality_issues", issues)
            # Use Engine 3.0 score if available
            if engine3_result.get("dq_score_v3") is not None:
                dq_score = engine3_result["dq_score_v3"]
            logger.info("[DQ3] Engine3 pipeline complete: score=%.1f readiness=%s",
                        dq_score or 0.0, engine3_result.get("readiness"))
        except Exception as _e3:
            logger.error("[DQ3] Engine3 pipeline failed, falling back to legacy: %s", _e3)
            engine3_result = {}

    # Build final return dict — keep backward-compatible shape, add v3 enrichments
    base_result = {
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "high_severity": sum(1 for i in issues if str(i.get("severity") or "").lower() == "high"),
            "medium_severity": sum(1 for i in issues if str(i.get("severity") or "").lower() == "medium"),
            "low_severity": sum(1 for i in issues if str(i.get("severity") or "").lower() == "low"),
            "dq_score_0_100": dq_score,
            "score_methodology_version": engine3_result.get("score_methodology", "dimension_scorer_v3"),
            "score_is_comparable_to_prior_version": False,
            "score_formula": "dimension_scorer_v3 (weighted sum of evaluated quality dimensions)",

            "score_components": score_components,
            "dimension_breakdown": engine3_result.get("dimension_breakdown_v3") or dimension_breakdown,
            "root_cause_summary": root_cause_summary,
            "estimated_clean_rows_after_high": clean_est_high,
            "estimated_clean_rows_after_high_and_medium": clean_est_high_med,
            # ── Engine 3.0 additions ──────────────────────────────────────────
            "readiness":             engine3_result.get("readiness", "UNKNOWN"),
            "gating_blockers":       engine3_result.get("gating_blockers", []),
            "assessment_confidence": engine3_result.get("assessment_confidence", 100.0),
            "rule_coverage":         engine3_result.get("rule_coverage", {}),
        },
        # ── Engine 3.0 separate output buckets ───────────────────────────────
        "privacy_findings":          engine3_result.get("privacy_findings", _pii_raw_findings),
        "rule_results":              engine3_result.get("rule_results", []),
        "component_manifest":        engine3_result.get("component_manifest", {}),
        "deduplication_diagnostics": engine3_result.get("deduplication_diagnostics", []),
    }
    return make_json_serializable(base_result)

def run_custom_rules(
    datasets: Dict[str, pd.DataFrame],
    custom_rules: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Apply custom rules from config. Each rule: dataset (or "*"), column, rule, params.
    rule: one_of, not_one_of, range, regex, not_null.
    Returns extra issues per dataset name.
    """
    extra: Dict[str, List[Dict[str, Any]]] = {}
    if not custom_rules:
        return extra

    for rule_cfg in custom_rules:
        dataset_pattern = (rule_cfg.get("dataset") or "*").strip()
        column = rule_cfg.get("column")
        rule_type = (rule_cfg.get("rule") or "").strip().lower()
        params = rule_cfg.get("params")
        if not column or not rule_type:
            continue

        for ds_name, df in datasets.items():
            if dataset_pattern != "*" and dataset_pattern != ds_name:
                continue
            if column not in df.columns:
                continue
            s = df[column].dropna().astype(str)
            if s.empty:
                continue
            issues: List[Dict[str, Any]] = []
            if rule_type == "one_of" and isinstance(params, list):
                allowed = set(str(x).strip().lower() for x in params)
                bad = ~s.str.strip().str.lower().isin(allowed)
                if bad.any():
                    cnt = int(bad.sum())
                    issues.append(dq_issue("medium", "custom_one_of",
                        f"Value not in allowed list ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
            elif rule_type == "range" and isinstance(params, dict):
                try:
                    num = pd.to_numeric(s, errors="coerce")
                    min_v = params.get("min")
                    max_v = params.get("max")
                    bad = pd.Series(False, index=s.index)
                    if min_v is not None:
                        bad = bad | (num < float(min_v))
                    if max_v is not None:
                        bad = bad | (num > float(max_v))
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("high", "custom_range",
                            f"Value outside range ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except (TypeError, ValueError):
                    pass
            elif rule_type == "regex" and isinstance(params, (str, dict)):
                pattern = params if isinstance(params, str) else params.get("pattern", "")
                if not pattern:
                    continue
                try:
                    import re as re_mod
                    pat = re_mod.compile(pattern)
                    bad = ~s.str.strip().apply(lambda v: bool(pat.match(v)) if isinstance(v, str) else False)
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("medium", "custom_regex",
                            f"Value does not match pattern ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except Exception:
                    pass
            elif rule_type == "not_null":
                null_mask = df[column].isna() | (df[column].astype(str).str.strip() == "")
                if null_mask.any():
                    cnt = int(null_mask.sum())
                    issues.append(dq_issue("high", "custom_not_null",
                        f"Null or empty not allowed ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[null_mask].tolist()[:50], sample=list(df.loc[null_mask, column].head(5))))
            for i in issues:
                if ds_name not in extra:
                    extra[ds_name] = []
                extra[ds_name].append(i)
    return extra

