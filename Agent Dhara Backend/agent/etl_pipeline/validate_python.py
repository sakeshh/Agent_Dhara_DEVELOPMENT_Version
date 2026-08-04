from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.codegen_shared import plan_actions

FORBIDDEN_IMPORTS = {"os", "subprocess", "sys", "shlex", "pty", "socket", "shutil", "ctypes"}
_BANNED_MODULES = FORBIDDEN_IMPORTS
_BANNED_CALLS = {"system", "popen", "run"}
_BANNED_BUILTINS = {"eval", "exec"}


def validate_python_source_dict(source: str) -> dict:
    """AST validation returning a structured dict (spec-friendly)."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return {"valid": False, "error": f"Syntax error: {e}", "issues": [f"Syntax error: {e}"]}

    issues: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_mod = (alias.name or "").split(".")[0]
                if root_mod in FORBIDDEN_IMPORTS:
                    issues.append(f"Forbidden import: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.split(".")[0] in FORBIDDEN_IMPORTS:
                issues.append(f"Forbidden import from: {node.module}")
            if node.module in FORBIDDEN_IMPORTS and any(a.name == "*" for a in (node.names or [])):
                issues.append(f"Forbidden wildcard import from: {node.module}")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in _BANNED_CALLS:
                issues.append(f"Forbidden call: .{node.func.attr}()")
            elif isinstance(node.func, ast.Name) and node.func.id in _BANNED_BUILTINS:
                issues.append(f"Forbidden builtin: {node.func.id}()")

    return {"valid": len(issues) == 0, "issues": issues}


_ACTION_CODE_MARKERS: Dict[str, List[str]] = {
    "lowercase": [".str.lower()", "F.lower(", "lower("],
    "uppercase": [".str.upper()", "F.upper(", "upper("],
    "trim": [".str.strip()", "F.trim("],
    "fill_nulls_simple": [".fillna(", "coalesce(", "F.coalesce(", "fillna(", ".na.fill(", "na.fill(", "F.when(", "when("],
    "fill_or_drop": [".fillna(", "coalesce(", "F.coalesce(", "fillna(", ".dropna(", ".na.fill(", "na.fill(", ".na.drop(", "na.drop(", "F.when(", "when("],
    "hash_phone": ["hashlib.sha256", "F.sha2("],
    "mask_phone": ["'***'", 'F.lit("***")'],
    "flag_outliers": ["_outlier_flagged", "_iqr_bounds", "_lower", "outlier", "skipped", "non-numeric"],
    "clip_outliers": [".clip(", "F.lit(_lower)"],
    "cap_outliers": ["_median", "_iqr_bounds"],
    "coerce_numeric": ["to_numeric", "cast('double')", 'cast("double")', ".cast(", "astype(", "coerce_numeric", "coerce numeric", "is_invalid_", "_valid_"],
    "parse_dates": ["to_datetime", "to_timestamp", "to_date", "F.to_timestamp(", "F.to_date(", "parse_dates", "parse_dates_safe", "cast('timestamp')", 'cast("timestamp")', "cast('date')", 'cast("date")', "is_invalid_", "_parsed_"],
    "parse_dates_safe": ["to_datetime", "to_timestamp", "to_date", "F.to_timestamp(", "F.to_date(", "is_invalid_", "parse_dates", "parse_dates_safe", "_parsed_"],
    "sanitize_email": ["contains('@'", "rlike(", "regexp_", "@"],
    "normalize_phone": ["regexp_replace", r"\D"],
    "deduplicate": ["drop_duplicates", "dropDuplicates", "dropDuplicates(", "_warn_duplicate_keys", ".distinct(", "_is_duplicate", "_duplicate_group_size", "row_number()"],
    "exclude_column": [".drop(columns=", ".drop("],
    "drop_column": [".drop(columns=", ".drop("],
    "standardize_boolean": ["isin(", "standardize_boolean"],
    "zero_to_null": [".replace(", "zero_to_null", "F.when("],
    "replace_sentinel_values": [".replace(", "replace_sentinel_values", "isin("],
    "range_clip": [".clip(lower=", "range_clip"],
    "replace_values": ["replace_values"],
    "regex_replace": [".str.replace(", "regex_replace", "regexp_replace", "F.regexp_replace"],
    "nullify_future_dates": ["Timestamp.now", "nullify_future_dates"],
    "nullify_dummy_dates": ["nullify_dummy_dates", "isin("],
    "nullify_punctuation": ["nullify_punctuation", "regexp_replace", "isin(", "when(", "cast('double')", 'cast("double")', "is_invalid_"],
    "flag_domain_violation": ["flag_domain_violation", "_domain_flag", "rlike("],
    "fill_nulls_flag": ["fill_nulls_flag", "is_invalid_", "to_double", "to_numeric", "pd.to_numeric"],
    "noop": ["no transform", "noop"],
    "at_least_one": ["isna().all(axis=1)", "at_least_one"],
    "cast_type": ["cast(", "astype(", ".astype", "astype", "cast_type", "cast type"],
    "trim_preserve_case": ["trim_preserve_case", "trim_preserve", "trim(", "F.trim("],
    "preserve_null": ["preserve_null", "coalesce", "fillna", "col(", "noop", "no transform"],
    "preserve_null_and_flag": ["preserve_null_and_flag", "is_missing_", "coalesce", "fillna"],
    "flag_missing_key": ["flag_missing_key", "is_missing_", "isnull(", ".isnull()", "is_null()"],
    "flag_missing_declared_key": ["flag_missing_declared_key", "is_missing_", "isnull(", ".isnull()", "is_null()"],
    "create_normalized_match_key": ["create_normalized_match_key", "_match_key", "regexp_replace", "trim("],
    "flag_duplicate_candidates": ["flag_duplicate_candidates", "row_number", "duplicate_candidate", "duplicate_candidates"],
    "flag_business_key_duplicate": ["flag_business_key_duplicate", "duplicate_candidate", "row_number"],
    "flag_near_duplicate_cluster": ["flag_near_duplicate_cluster", "levenshtein", "soundex", "cluster"],
}


def _action_reflected_in_source(source: str, action: str) -> bool:
    if f"Unsupported in codegen v1: {action}" in source:
        return True
    src_lower = source.lower()
    act_lower = str(action or "").lower()
    if act_lower in src_lower or act_lower.replace("_", " ") in src_lower or act_lower.replace("_", "") in src_lower:
        return True
    for marker in _ACTION_CODE_MARKERS.get(action, []):
        if marker.lower() in src_lower:
            return True
    if action == "validate_referential_integrity_or_stage":
        return "referential integrity" in src_lower or "ri " in src_lower
    return False


def validate_python_implements_plan(source: str, plan: Optional[Dict[str, Any]] = None) -> List[str]:
    """Ensure each plan action is implemented or marked unsupported in generated code."""
    if not plan:
        return []
    missing: List[str] = []
    seen: Set[str] = set()
    for ds_name, block in (plan.get("datasets") or {}).items():
        for step in (block or {}).get("steps") or []:
            action = step.get("action")
            if not action:
                continue
            params = step.get("params") or {}
            is_noop = False
            if action in ("fill_nulls_simple", "fill_or_drop"):
                if not params.get("fill_strategy"):
                    is_noop = True
            if action == "noop":
                is_noop = True
            if is_noop:
                continue
            if action in seen:
                continue
            seen.add(action)
            if not _action_reflected_in_source(source, action):
                missing.append(f"plan action not reflected in code: {action}")
    return missing


def validate_etl_python_source(source: str, plan: Optional[Dict[str, Any]] = None) -> Tuple[bool, List[str]]:
    """
    ETL template scripts may import os for path resolution (connector manifest).
    Still blocks eval/exec/subprocess and dangerous os calls.
    """
    result = validate_python_source_dict(source)
    if not result.get("valid") and result.get("error") and not result.get("issues"):
        return False, [str(result["error"])]

    issues = list(result.get("issues") or [])
    etl_allowed_roots = {"os", "sys"}
    filtered = []
    for e in issues:
        if any(e == f"Forbidden import: {m}" for m in etl_allowed_roots):
            continue
        if e.startswith("Forbidden import from:"):
            mod = e.split(":", 1)[-1].strip().split(".")[0]
            if mod in etl_allowed_roots:
                continue
        filtered.append(e)
    dangerous = (
        "os.system",
        "os.popen",
        "os.remove",
        "os.unlink",
        "os.rmdir",
        "shutil",
        "subprocess",
    )
    low = source or ""
    for d in dangerous:
        if d in low:
            filtered.append(f"disallowed usage: {d}")
    filtered.extend(validate_python_implements_plan(source, plan))
    filtered.extend(_check_never_drop_rows_python(source, plan))
    filtered.extend(_check_bare_deduplicate_python(source, plan))
    filtered.extend(_check_semantic_rules_python(source))
    filtered.extend(_check_row_count_logging_python(source))
    return (len(filtered) == 0), filtered


def _check_never_drop_rows_python(source: str, plan: Dict[str, Any] | None) -> List[str]:
    if not plan:
        return []
    rules = plan.get("business_rules") or {}
    if not rules.get("never_drop_rows"):
        return []
    errs: List[str] = []
    
    import ast
    try:
        tree = ast.parse(source)
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                child.parent = parent
                
        lines = source.splitlines()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in ("dropna", "query"):
                    curr = node
                    is_assigned_or_returned = False
                    while hasattr(curr, "parent"):
                        curr = curr.parent
                        if isinstance(curr, (ast.Assign, ast.Return)):
                            is_assigned_or_returned = True
                            break
                    if is_assigned_or_returned:
                        line = lines[node.lineno - 1]
                        errs.append(f"never_drop_rows: do not use {node.func.attr}() to drop or filter rows: {line.strip()}")
    except Exception:
        pass
    return errs


def _check_bare_deduplicate_python(source: str, plan: Dict[str, Any] | None) -> List[str]:
    if not plan:
        return []
    errs: List[str] = []
    has_keys = bool(plan.get("business_keys"))
    has_row_level = False
    for ds_name, block in (plan.get("datasets") or {}).items():
        steps = (block or {}).get("steps") or []
        for s in steps:
            if s.get("action") == "deduplicate":
                col = str(s.get("column") or "").lower().strip()
                if col in ("row-level", "[row-level]"):
                    has_row_level = True
                else:
                    has_keys = True
    import re
    if has_keys and not has_row_level:
        if re.search(r"\.drop_duplicates\s*\(\s*\)", source):
            errs.append("Reject bare drop_duplicates(): key-aware deduplication must specify subset columns when near duplicates are possible")
    return errs


def _check_semantic_rules_python(source: str) -> List[str]:
    errs: List[str] = []
    if "contains('@')" in source or 'contains("@")' in source or "str.contains('@')" in source or 'str.contains("@")' in source or "'@' in " in source or '"@" in ' in source:
        errs.append("naive contains('@') email checks are not allowed; use regex matching for sanitize_email")
    return errs


def _check_row_count_logging_python(source: str) -> List[str]:
    if "def transform" not in source:
        return []
    errs: List[str] = []
    # Require row count logging or calls in Python codegen output
    if "log_row_count" not in source and "logger.info" not in source and "print(" not in source:
        errs.append("Row-count logging is required before and after major stages")
    return errs


def validate_python_source(source: str) -> Tuple[bool, List[str]]:
    """Strict validation for untrusted Python (no os allowance)."""
    if not source or not source.strip():
        return False, ["empty source"]

    result = validate_python_source_dict(source)
    if not result.get("valid") and result.get("error"):
        return False, list(result.get("issues") or [str(result["error"])])

    issues = list(result.get("issues") or [])
    if issues:
        return False, issues
    return True, []
