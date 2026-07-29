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

def enrich_issue_with_fixability(issue: Dict[str, Any]) -> None:
    if issue.get("fixability"):
        return
    issue["fixability"] = FIXABILITY_BY_ISSUE_TYPE.get(issue.get("type") or "", "COMPLEX")

def dq_issue(
    sev: str,
    typ: str,
    msg: str,
    *,
    column: Optional[str] = None,
    count: Optional[int] = None,
    rows: Optional[List[int]] = None,
    sample: Optional[List[Any]] = None
) -> Dict[str, Any]:
    """
    Create a normalized DQ issue record.
    - severity: "low" | "medium" | "high"
    - row_indexes: list of 0-based indexes (capped to 50)
    - sample_values: capped to 10
    """
    return {
        "severity": sev,
        "type": typ,
        "column": column,
        "count": count,
        "row_indexes": rows[:50] if rows else [],
        "sample_values": sample[:10] if sample else [],
        "message": msg,
        "fixability": FIXABILITY_BY_ISSUE_TYPE.get(typ, "COMPLEX"),
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
        msg = f"{col}: expectation failed."
        
        if exp == "placeholder_detected":
            issue_type = "placeholder_detected"
            severity = "medium"
            msg = f"{unexp_cnt} placeholder/semantic null value(s) (e.g. '???', 'N/A')" if unexp_cnt > 0 else "Placeholder value(s) found"
        elif "not be null" in exp:
            issue_type = "nulls"
            severity = "high" if str(col).lower() in non_nullable_set else "low"
            msg = f"{unexp_cnt} null value(s)" if unexp_cnt > 0 else "Null value(s) found"
        elif "be unique" in exp:
            issue_type = "duplicate_primary_key"
            severity = "high"
            msg = f"{unexp_cnt} duplicate in candidate PK" if unexp_cnt > 0 else "Duplicate key values found"
        elif "compound columns to be unique" in exp or "compound_columns_to_be_unique" in exp:
            issue_type = "duplicate_rows"
            severity = "low"
            msg = f"{unexp_cnt} duplicate row(s)" if unexp_cnt > 0 else "Duplicate rows found"
        elif exp == "string_with_only_digits_in_text_column":
            issue_type = "string_with_only_digits_in_text_column"
            severity = "medium"
            msg = f"{unexp_cnt} digit string(s) in categorical column '{col}'"
        elif exp == "invalid_numeric_values":
            issue_type = "invalid_numeric"
            severity = "medium"
            msg = f"{unexp_cnt} non-numeric value(s) in numeric column '{col}'"
        elif exp == "invalid_date_format":
            issue_type = "invalid_date_format"
            severity = "medium"
            msg = f"{unexp_cnt} invalid date/sentinel date value(s) in column '{col}'"
        elif "be in type list" in exp or "be of type" in exp or "invalid_numeric" in exp or exp == "invalid_numeric_values":
            issue_type = "invalid_numeric"
            severity = "high" if any(k in str(col).lower() for k in ("amount", "price", "total", "cost")) else "medium"
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
            issue_type = "negative_values"
            severity = "medium"
            msg = f"{unexp_cnt} negative value(s)"
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
            
        issues.append({
            "severity": severity,
            "type": issue_type,
            "column": col,
            "count": unexp_cnt,
            "row_indexes": unexp_idx[:50],
            "sample_values": unexp_vals[:10],
            "message": msg,
            "fixability": FIXABILITY_BY_ISSUE_TYPE.get(issue_type, "COMPLEX")
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
        
    # Run PII scanner on text columns and convert findings to DQIssues (P0 task)
    try:
        from agent.pii_masking import scan_dataframe_for_pii
        pii_findings = scan_dataframe_for_pii(df)
        for f in pii_findings:
            pii_t = f.get("pii_type")
            issue_type = "pii_email" if pii_t == "email" else "pii_phone" if pii_t == "phone" else "pii_sensitive"
            issues.append({
                "severity": str(f.get("severity") or "medium").lower(),
                "type": issue_type,
                "column": f.get("column") or "",
                "count": f.get("matches_in_sample") or 1,
                "row_indexes": [],
                "sample_values": [],
                "message": f.get("message") or f"Embedded PII detected in column '{f.get('column')}'",
                "source": "pii_scanner",
                "fixability": "COMPLEX"
            })
    except Exception as e:
        logger.warning(f"PII content scanning failed for {name}: {e}")

    # Calculate Scorecard Summary Metrics
    try:
        score_cfg = thresholds.get("dq_score") or {}
        w = score_cfg.get("weights") or {}
        wh = float(w.get("high", 3.0))
        wm = float(w.get("medium", 1.0))
        wl = float(w.get("low", 0.3))
        sev_w = {"high": wh, "medium": wm, "low": wl}

        high_rows, med_rows, low_rows = set(), set(), set()
        synth_idx = n
        has_synthesized = False
        
        for it in issues:
            sev = str(it.get("severity") or "low").lower()
            rows = it.get("row_indexes") or []
            cnt = int(it.get("count") or it.get("unexpected_count") or len(rows) or 1)
            cnt = min(cnt, n)
            
            explicit_rows = set(rows)
            if sev == "high":
                high_rows.update(explicit_rows)
            elif sev == "medium":
                med_rows.update(explicit_rows)
            else:
                low_rows.update(explicit_rows)
                
            remaining_cnt = cnt - len(explicit_rows)
            if remaining_cnt > 0:
                dummy_rows = range(synth_idx, synth_idx + remaining_cnt)
                synth_idx += remaining_cnt
                has_synthesized = True
                if sev == "high":
                    high_rows.update(dummy_rows)
                elif sev == "medium":
                    med_rows.update(dummy_rows)
                else:
                    low_rows.update(dummy_rows)

        med_rows = set(med_rows) - set(high_rows)
        low_rows = set(low_rows) - set(high_rows) - set(med_rows)

        if has_synthesized:
            import logging
            logging.getLogger("agent.profiling.dq_checks").debug(
                f"DQ scoring: count-based index synthesis used for issues without row_indexes (dataset size: {n})"
            )

        # Flat penalty based on issue diversity (capped at 15.0 max)
        num_high_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "high")
        num_med_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "medium")
        num_low_types = sum(1 for i in issues if str(i.get("severity") or "low").lower() == "low")
        
        flat_penalty = min(15.0, (2.5 * num_high_types) + (0.5 * num_med_types) + (0.1 * num_low_types))

        # Row fraction penalty
        frac_h = min(1.0, len(high_rows) / max(1, n))
        frac_m = min(1.0, len(med_rows) / max(1, n))
        frac_l = min(1.0, len(low_rows) / max(1, n))

        row_penalty = (30.0 * frac_h) + (15.0 * frac_m) + (5.0 * frac_l)
        
        total_penalty = flat_penalty + row_penalty
        dq_score = max(0.0, min(100.0, 100.0 - total_penalty))

        clean_est_high = max(0, n - len(high_rows))
        clean_est_high_med = max(0, n - len(high_rows.union(med_rows)))
    except Exception:
        dq_score = None
        clean_est_high = None
        clean_est_high_med = None

    from agent.etl_pipeline.dq_issue_schema import normalize_issue_dict
    issues = [normalize_issue_dict(i, name, "profiler") for i in issues]

    return make_json_serializable({
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "high_severity": sum(1 for i in issues if i["severity"] == "high"),
            "medium_severity": sum(1 for i in issues if i["severity"] == "medium"),
            "low_severity": sum(1 for i in issues if i["severity"] == "low"),
            "dq_score_0_100": dq_score,
            "estimated_clean_rows_after_high": clean_est_high,
            "estimated_clean_rows_after_high_and_medium": clean_est_high_med,
        }
    })

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

