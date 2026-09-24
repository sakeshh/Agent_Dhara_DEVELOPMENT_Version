from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from agent.etl_pipeline.codegen_shared import (
    UnsupportedCodegenActionError,
    get_sql_cast_type,
    outlier_multiplier,
    sql_fill_update_lines,
    step_params,
    tsql_qualified_name,
)
from agent.etl_pipeline.join_emitters import emit_sql_joins
from agent.etl_pipeline.sql_templates.infrastructure import (
    ETL_DEFAULT_VALUES_SCHEMA,
    ETL_INVALID_VALUES_SCHEMA,
    ETL_LOG_SCHEMA,
    ETL_REJECT_SCHEMA,
    ETL_REJECTS_SCHEMA,
    ETL_WATERMARK_SCHEMA,
    get_infrastructure_ddl,
)


def quote_sql_server_identifier(identifier: str) -> str:
    """Escapes single T-SQL identifier by escaping ']' to ']]' and wrapping in brackets."""
    clean = str(identifier or "").strip()
    if clean.startswith("[") and clean.endswith("]"):
        clean = clean[1:-1]
    if "," in clean:
        sub_parts = [p.strip().strip("[]") for p in clean.split(",") if p.strip()]
        return ", ".join(f"[{p.replace(']', ']]')}]" for p in sub_parts)
    escaped = clean.replace("]", "]]")
    return f"[{escaped}]"


def quote_multipart_identifier(multipart_ident: str) -> str:
    """Quotes multi-part T-SQL identifier separately (e.g. 'dbo.Customers' -> '[dbo].[Customers]')."""
    parts = str(multipart_ident or "").split(".")
    return ".".join(quote_sql_server_identifier(p) for p in parts)


def _brk(ident: str) -> str:
    """T-SQL style bracket quoting (delegates to quote_sql_server_identifier)."""
    return quote_sql_server_identifier(ident)


_NON_WATERMARK_DATE_PATTERNS = (
    "birth", "dob", "effective", "hire", "start", "end", "expiry",
    "due", "order", "invoice", "payment", "ship", "delivery", "event",
    "admission", "discharge", "booking", "flight", "contract"
)


def _resolve_watermark_column(cols_info: Dict[str, Any]) -> Optional[str]:
    """Dynamically resolves a reliable audit modification timestamp for incremental extraction.
    Strictly excludes business dates (e.g. DateOfBirth, HireDate, OrderDate)."""
    if not cols_info:
        return None
    # 1. Primary: Explicit modification audit timestamps
    for col_name in cols_info.keys():
        col_l = col_name.lower()
        if any(w in col_l for w in ("modified", "updated", "change_date", "sys_change_version", "source_updated_at", "last_modified")):
            return col_name
        if col_l.endswith("_at") or col_l.endswith("_ts"):
            if not any(nw in col_l for nw in _NON_WATERMARK_DATE_PATTERNS):
                return col_name
    # 2. Secondary: Explicit watermark / change-tracking columns
    for col_name in cols_info.keys():
        col_l = col_name.lower()
        if any(w in col_l for w in ("watermark", "etl_timestamp", "source_timestamp", "last_run")):
            if not any(nw in col_l for nw in _NON_WATERMARK_DATE_PATTERNS):
                return col_name
    return None


def translate_assertion_to_sql(assertion: str, cols: List[str] = None) -> str:
    """Translates Python/Pandas assertion expression to standard SQL expression."""
    expr = assertion.strip()
    
    # 1. String check conversion
    endswith_match = re.search(r'([\w\[\]\d_]+)\.str\.endswith\([\'"]([^\'"]+)[\'"]\)', expr, re.IGNORECASE)
    if endswith_match:
        col = endswith_match.group(1)
        suffix = endswith_match.group(2)
        expr = f"[{col}] LIKE '%{suffix}'"
        
    startswith_match = re.search(r'([\w\[\]\d_]+)\.str\.startswith\([\'"]([^\'"]+)[\'"]\)', expr, re.IGNORECASE)
    if startswith_match:
        col = startswith_match.group(1)
        prefix = startswith_match.group(2)
        expr = f"[{col}] LIKE '{prefix}%'"

    contains_match = re.search(r'([\w\[\]\d_]+)\.str\.contains\([\'"]([^\'"]+)[\'"]\)', expr, re.IGNORECASE)
    if contains_match:
        col = contains_match.group(1)
        pattern = contains_match.group(2)
        expr = f"[{col}] LIKE '%{pattern}%'"

    # 1b. Match regex checks
    match_match = re.search(r'([\w\[\]\d_]+)\.str\.match\([\'"]([^\'"]+)[\'"]\)', expr, re.IGNORECASE)
    if match_match:
        col = match_match.group(1)
        pat = match_match.group(2)
        if pat == r'^\d{10}$' or pat == r'^[0-9]{10}$':
            expr = f"(LEN([{col}]) = 10 AND [{col}] NOT LIKE '%[^0-9]%')"
        elif pat == r'^\d+$' or pat == r'^[0-9]+$':
            expr = f"(LEN([{col}]) > 0 AND [{col}] NOT LIKE '%[^0-9]%')"
        elif pat == r'^E\d+$' or pat == r'^E[0-9]+$':
            expr = f"([{col}] LIKE 'E%' AND LEN([{col}]) > 1 AND SUBSTRING([{col}], 2, 8000) NOT LIKE '%[^0-9]%')"
        else:
            clean_pat = pat.lstrip('^').rstrip('$')
            clean_pat = clean_pat.replace(r'\d', '[0-9]')
            expr = f"[{col}] LIKE '{clean_pat}'"

    # 1c. isin check conversion
    isin_match = re.search(r'([\w\[\]\d_]+)\.isin\(\[([^\]]+)\]\)', expr, re.IGNORECASE)
    if isin_match:
        col = isin_match.group(1)
        list_str = isin_match.group(2)
        expr = f"[{col}] IN ({list_str})"

    # 2. Null checks replacement
    expr = re.sub(r'([\w\[\]\d_]+)\.isnull\(\)', r'[\1] IS NULL', expr, flags=re.IGNORECASE)
    expr = re.sub(r'([\w\[\]\d_]+)\.isna\(\)', r'[\1] IS NULL', expr, flags=re.IGNORECASE)
    expr = re.sub(r'([\w\[\]\d_]+)\.notnull\(\)', r'[\1] IS NOT NULL', expr, flags=re.IGNORECASE)
    expr = re.sub(r'([\w\[\]\d_]+)\.notna\(\)', r'[\1] IS NOT NULL', expr, flags=re.IGNORECASE)

    # 2b. Length checks
    expr = re.sub(r'([\w\[\]\d_]+)\.str\.len\(\)', r'LEN([\1])', expr, flags=re.IGNORECASE)
    
    # 2c. Strip checks
    expr = re.sub(r'([\w\[\]\d_]+)\.str\.strip\(\)', r'LTRIM(RTRIM([\1]))', expr, flags=re.IGNORECASE)
    
    # 2d. Alpha/Space/Numeric checks
    expr = re.sub(r'([\w\[\]\d_]+)\.str\.isalpha\(\)', r"(LEN([\1]) > 0 AND [\1] NOT LIKE '%[^a-zA-Z]%')", expr, flags=re.IGNORECASE)
    expr = re.sub(r'([\w\[\]\d_]+)\.str\.isspace\(\)', r"(LEN([\1]) > 0 AND [\1] NOT LIKE '%[^ ]%')", expr, flags=re.IGNORECASE)

    # 3. Replace python operators with SQL equivalents
    expr = re.sub(r'\bnot\b', 'NOT', expr, flags=re.IGNORECASE)
    expr = re.sub(r'\band\b', 'AND', expr, flags=re.IGNORECASE)
    expr = re.sub(r'\bor\b', 'OR', expr, flags=re.IGNORECASE)
    
    # Replace Pandas logical bitwise operators used in series
    expr = re.sub(r'\s*&\s*', ' AND ', expr)
    expr = re.sub(r'\s*\|\s*', ' OR ', expr)
    
    expr = expr.replace("==", "=")
    expr = expr.replace("!=", "<>")
    
    if cols:
        sorted_cols = sorted(cols, key=len, reverse=True)
        for c in sorted_cols:
            pattern = r'(?<!\[)\b' + re.escape(c) + r'\b(?!\])'
            expr = re.sub(pattern, f"[{c}]", expr, flags=re.IGNORECASE)
            
    return expr


def _get_clean_table_name(ds_name: str, target_schema: str = None, timestamp_suffix: str = "") -> str:
    """Resolve raw table name to its clean table equivalent."""
    parts = ds_name.split(".", 1)
    if len(parts) == 2:
        schema, tbl_name = parts[0], parts[1]
    else:
        schema, tbl_name = "dbo", ds_name
    
    schema = schema.strip("[]")
    tbl_name = tbl_name.strip("[]")
    
    if target_schema:
        schema = target_schema.strip("[]")
    
    tbl_lower = tbl_name.lower()
    if tbl_lower.endswith("_clean"):
        # Already a clean table — return as-is to avoid double-suffix (_Clean_Clean)
        clean_tbl = tbl_name
    elif tbl_lower.endswith("_staging"):
        clean_tbl = tbl_name[:-8] + "_Clean"
    elif tbl_lower.endswith("_raw"):
        clean_tbl = tbl_name[:-4] + "_Clean"
    else:
        clean_tbl = tbl_name + "_Clean"
        
    if timestamp_suffix:
        clean_tbl = f"{clean_tbl}_{timestamp_suffix}"
        
    return f"{schema}.{clean_tbl}"



def _get_transformed_table_name(ds_name: str) -> str:
    """Resolve raw table name to its transformed table equivalent."""
    parts = ds_name.split(".", 1)
    if len(parts) == 2:
        schema, tbl_name = parts[0], parts[1]
    else:
        schema, tbl_name = "dbo", ds_name
    
    schema = schema.strip("[]")
    tbl_name = tbl_name.strip("[]")
    
    if tbl_name.lower().endswith("_raw"):
        trans_tbl = tbl_name[:-4] + "_Transformed"
    elif tbl_name.lower().endswith("_staging"):
        trans_tbl = tbl_name[:-8] + "_Transformed"
    elif tbl_name.lower().endswith("_clean"):
        trans_tbl = tbl_name[:-6] + "_Transformed"
    else:
        trans_tbl = tbl_name + "_Transformed"
        
    return f"{schema}.{trans_tbl}"


def _indent(lines_list: List[str], spaces: int = 8) -> List[str]:
    """Prefix all non-empty lines with indentation spaces."""
    prefix = " " * spaces
    return [prefix + line if line.strip() else line for line in lines_list]


def _classify_column(
    col_name: str,
    col_meta: dict | None,
    sem_schema: dict | None = None,
    ds_name: str = "",
) -> str:
    from agent.etl_pipeline.semantic_classifier import profile_column
    meta = col_meta or {}
    dtype = meta.get("dtype") or meta.get("target_dtype") or meta.get("inferred_type") or "string"
    semantic_type = meta.get("semantic_type") or ""
    
    if sem_schema:
        desc = sem_schema.get(f"{ds_name}.{col_name}") or {}
        if desc.get("semantic_type"):
            semantic_type = desc.get("semantic_type")
            
    prof = profile_column(col_name, dtype, semantic_type)
    if prof.is_temporal:
        return "date"
    if prof.is_numeric:
        return "metric"
    if prof.is_categorical:
        return "categorical"
    if prof.is_identifier:
        return "id"
    return "string"


def _is_numeric_column(col_name: str, col_meta: dict) -> bool:
    from agent.etl_pipeline.semantic_classifier import profile_column
    dtype = col_meta.get("dtype") or col_meta.get("target_dtype") or col_meta.get("inferred_type") or "string"
    semantic_type = col_meta.get("semantic_type") or ""
    return profile_column(col_name, dtype, semantic_type).is_numeric



def compile_column_expression(col_name: str, transforms: List[dict], col_meta: dict, business_rules: dict, sem_schema: dict = None, ds_name: str = "") -> str:
    # Start with the raw column identifier
    expr = f"[{col_name}]"
    
    def is_already_string(val: str) -> bool:
        v = val.strip().upper()
        return any(v.startswith(prefix) for prefix in (
            "LOWER(", "UPPER(", "LTRIM(", "RTRIM(", "REPLACE(", "CAST(", "TRY_CAST(", 
            "TRY_CONVERT(", "COALESCE(", "CONVERT(", "SUBSTRING(", "RIGHT(", "LEFT(", 
            "N'", "'"
        ))
        
    def _wrap_string_cast(val: str) -> str:
        if is_already_string(val):
            return val
        return f"CAST({val} AS NVARCHAR(MAX))"
        
    for st in transforms:
        action = st.get("action")
        col_class = _classify_column(col_name, col_meta, sem_schema, ds_name)
        
        if action in ("trim", "trim_preserve_case"):
            if col_class not in ("metric", "date"):
                is_internal = any(
                    (s.get("action") == "trim") and 
                    (s.get("source_issue_type") == "internal_whitespace" or s.get("issue_type") == "internal_whitespace")
                    for s in transforms
                )
                if is_internal:
                    inner = expr
                    if expr.strip().upper().startswith("LTRIM(RTRIM("):
                        inner = expr.strip()[12:-2]
                    expr = f"LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE({_wrap_string_cast(inner)}, '  ', ' '), '  ', ' '), '  ', ' '), '  ', ' ')))"
                else:
                    if not (expr.strip().upper().startswith("LTRIM(RTRIM(") or expr.strip().upper().startswith("RTRIM(LTRIM(")):
                        expr = f"LTRIM(RTRIM({_wrap_string_cast(expr)}))"
        elif action == "lowercase":
            if col_class not in ("metric", "date"):
                if not expr.strip().upper().startswith("LOWER("):
                    expr = f"LOWER({_wrap_string_cast(expr)})"
        elif action == "uppercase":
            if col_class not in ("metric", "date"):
                if not expr.strip().upper().startswith("UPPER("):
                    expr = f"UPPER({_wrap_string_cast(expr)})"
        elif action == "sanitize_email":
            if col_class not in ("metric", "date"):
                expr_str = _wrap_string_cast(expr)
                cleaned = f"LOWER(LTRIM(RTRIM({expr_str})))"
                expr = (
                    f"CASE "
                    f"WHEN {cleaned} LIKE '%_@_%._%' AND {cleaned} NOT LIKE '%@%@%' AND {cleaned} NOT LIKE '%..%' AND {cleaned} NOT LIKE '% %' "
                    f"AND {cleaned} NOT IN ('n/a', 'na', 'null', 'none', 'unknown', '???', 'test@test.com', 'dummy@dummy.com') "
                    f"THEN {cleaned} ELSE NULL END"
                )
        elif action == "normalize_phone":
            inner_cast = expr if is_already_string(expr) else f"CAST({expr} AS NVARCHAR(200))"
            clean_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({inner_cast}, N'-', N''), N' ', N''), N'(', N''), N')', N''), N'+', N''), N'.', N'')"
            expr = f"CASE WHEN LEN({clean_expr}) BETWEEN 7 AND 15 AND {clean_expr} NOT LIKE '%[^0-9]%' THEN {clean_expr} ELSE NULL END"
        elif action == "nullify_dummy_dates":
            expr = f"CASE WHEN {expr} = '1900-01-01' OR (YEAR(TRY_CAST({expr} AS DATE)) <= 1900 AND FORMAT(TRY_CAST({expr} AS DATE), 'MM-dd') = '01-01') THEN NULL ELSE {expr} END"
        elif action == "nullify_punctuation":
            if col_class not in ("metric", "date"):
                expr = f"CASE WHEN {_wrap_string_cast(expr)} NOT LIKE '%[a-zA-Z0-9]%' THEN NULL ELSE {expr} END"
        elif action == "hash_phone":
            expr = f"CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', {_wrap_string_cast(expr)}), 2)"
        elif action == "mask_phone":
            inner_cast = expr if is_already_string(expr) else f"CAST({expr} AS NVARCHAR(200))"
            expr = f"N'***' + RIGHT(REPLACE(REPLACE({inner_cast}, N'-', N''), N' ', N''), 4)"
        elif action == "standardize_boolean":
            inner_b = f"LOWER(LTRIM(RTRIM({_wrap_string_cast(expr)})))"
            expr = (
                f"CASE "
                f"WHEN {inner_b} IN ('1', 'true', 'yes', 'y', 't') THEN 1 "
                f"WHEN {inner_b} IN ('0', 'false', 'no', 'n', 'f') THEN 0 "
                f"ELSE NULL END"
            )
        elif action == "regex_replace":
            expr = f"REPLACE(REPLACE({_wrap_string_cast(expr)}, N'''', N''), N'\"', N'')"
        elif action in ("range_clip", "clip_outliers"):
            p = st.get("params") or {}
            lower = p.get("lower_bound", p.get("min_value"))
            upper = p.get("upper_bound", p.get("max_value"))
            cast_f = f"TRY_CAST({expr} AS FLOAT)"
            col_l = col_name.lower()
            is_price = any(k in col_l for k in ("price", "cost", "fee", "amount", "budget", "salary", "revenue", "sales", "fare", "balance"))
            if is_price:
                expr = f"CASE WHEN {cast_f} < 0 THEN NULL ELSE {cast_f} END"
            elif col_l == "age":
                expr = f"CASE WHEN TRY_CAST({expr} AS INT) < 0 OR TRY_CAST({expr} AS INT) > 120 THEN NULL ELSE TRY_CAST({expr} AS INT) END"
            elif lower is not None and upper is not None:
                expr = f"CASE WHEN {cast_f} < {lower} THEN NULL WHEN {cast_f} > {upper} THEN NULL ELSE {cast_f} END"
            elif lower is not None:
                expr = f"CASE WHEN {cast_f} < {lower} THEN NULL ELSE {cast_f} END"
            elif upper is not None:
                expr = f"CASE WHEN {cast_f} > {upper} THEN NULL ELSE {cast_f} END"
            else:
                expr = f"CASE WHEN {cast_f} < 0 THEN NULL ELSE {cast_f} END"
        elif action in ("replace_sentinel_values", "normalize_registered_placeholder"):
            expr_str = f"LOWER(LTRIM(RTRIM({_wrap_string_cast(expr)})))"
            expr = (
                f"CASE "
                f"WHEN {expr_str} IN ('???', 'n/a', 'na', 'null', 'none', 'unknown', 'dummy', 'temp', 'test', '###', '-', 'unspecified', '0000', '') "
                f"OR TRY_CAST({expr} AS BIGINT) IN (999, -999, 999999, 9999999, 99999) "
                f"OR TRY_CAST({expr} AS FLOAT) IN (999, -999, 999999, 9999999, -9999, 99999) "
                f"THEN NULL ELSE {expr} END"
            )
        elif action in ("fill_nulls_simple", "fill_or_drop"):
            # ✅ CRITICAL FIX: Implement fill null logic
            params = st.get("params") or {}
            evidence = st.get("evidence") or {}
            fill_val = params.get("fill_value") or evidence.get("recommended_fill")

            # Try to get statistical fill values
            if fill_val == "median" or (not fill_val and evidence.get("median") is not None):
                fill_val = evidence.get("median")
            elif fill_val == "mean" or (not fill_val and evidence.get("mean") is not None and not evidence.get("median")):
                fill_val = evidence.get("mean")

            if fill_val is not None and str(fill_val).replace(".", "").replace("-", "").isdigit():
                # Numeric fill value
                expr = f"COALESCE({expr}, {fill_val})"
            elif fill_val is not None:
                # String fill value
                fill_str = str(fill_val).replace("'", "''")
                expr = f"COALESCE({expr}, N'{fill_str}')"
            else:
                # Fallback: fill with type-appropriate default
                if _is_numeric_column(col_name, col_meta):
                    expr = f"COALESCE({expr}, 0)"
                else:
                    expr = f"COALESCE({expr}, N'unknown')"
        elif action == "cast_type":
            # ✅ CRITICAL FIX: Implement type casting
            params = st.get("params") or {}
            target_type = params.get("target_type") or col_meta.get("target_dtype")
            if not target_type:
                # Infer target type from semantic type
                semantic = col_meta.get("semantic_type") or ""
                if "date" in semantic.lower() or "time" in semantic.lower():
                    target_type = "DATE"
                elif "int" in semantic.lower() or "id" in col_name.lower():
                    target_type = "BIGINT"
                elif "float" in semantic.lower() or "decimal" in semantic.lower():
                    target_type = "DECIMAL(18, 2)"
                else:
                    target_type = "NVARCHAR(256)"
            expr = f"TRY_CAST({expr} AS {target_type})"
        elif action == "coerce_numeric":
            is_decimal = False
            col_lower = col_name.lower()
            col_type = col_meta.get("dtype")
            if col_type:
                is_decimal = any(x in str(col_type).lower() for x in ("float", "decimal", "double", "numeric", "real"))
            if not is_decimal:
                is_decimal = any(x in col_lower for x in ("fee", "price", "amount", "rate", "cost", "total", "balance", "tax", "decimal"))
            cast_target = "DECIMAL(18, 2)" if is_decimal else "BIGINT"
            expr = f"TRY_CAST({_wrap_string_cast(expr)} AS {cast_target})"
        elif action in ("parse_dates", "parse_dates_safe"):
            parsed_expr = f"COALESCE(TRY_CONVERT(date, {expr}, 120), TRY_CONVERT(date, {expr}, 103), TRY_CONVERT(date, {expr}, 101), TRY_CONVERT(date, {expr}, 111))"
            expr = (
                f"CASE "
                f"WHEN {parsed_expr} IS NULL THEN NULL "
                f"WHEN {parsed_expr} IN ('1900-01-01', '9999-12-31', '0001-01-01') THEN NULL "
                f"WHEN {parsed_expr} < '1900-01-01' OR {parsed_expr} > '2099-12-31' THEN NULL "
                f"ELSE {parsed_expr} END"
            )
        elif action == "zero_to_null":
            expr = f"CASE WHEN TRY_CAST({expr} AS FLOAT) = 0 THEN NULL ELSE {expr} END"
        elif action == "replace_values":
            mapping = (business_rules.get("replace_values") or {}).get(col_name) or {}
            if isinstance(mapping, dict) and mapping:
                case_expr = f"CASE"
                for old_v, new_v in list(mapping.items())[:20]:
                    ov = str(old_v).replace("'", "''")
                    nv = str(new_v).replace("'", "''")
                    case_expr += f" WHEN {_wrap_string_cast(expr)} = N'{ov}' THEN N'{nv}'"
                case_expr += f" ELSE {expr} END"
                expr = case_expr
        elif action in ("fill_constant", "fill_null_with_constant"):
            params = st.get("params") or {}
            cval = str(params.get("constant_value", params.get("fill_value", "unknown"))).replace("'", "''")
            expr = f"COALESCE({expr}, N'{cval}')"
        elif action in (
            # Actions that are handled at row-level or table-level, not column expression level
            "deduplicate", "flag_business_key_duplicate", "flag_duplicate_candidates",
            "flag_near_duplicate_cluster", "validate_referential_integrity_or_stage",
            "at_least_one", "validate_and_partition_relationship", "drop_column", "exclude_column",
            "join_datasets", "drop_rows",
            # Flag-only actions (add flag columns, not transform values)
            "preserve_null_and_flag", "preserve_and_flag", "preserve_null",
            "preserve_missing_suspected_key", "accept_null_as_optional",
            "fill_nulls_flag", "flag_only", "nullify_clean_value",
            "flag_missing_key", "flag_missing_declared_key", "flag_domain_violation",
            "flag_range_violation", "flag_cross_field_violation", "flag_high_null_key",
            # Quarantine actions (handled at row routing level)
            "quarantine_row", "quarantine_null_rows", "quarantine_missing_key",
            "quarantine_orphan_fk", "quarantine_key_null",
            # Imputation actions (TODO: implement statistical imputation)
            "impute_numeric", "impute_date", "create_normalized_match_key",
            # Administrative actions
            "skip_etl", "skip_requirement", "review_manually", "force_unlock",
            # True no-ops
            "noop", "keep_as_is",
        ):
            pass
        else:
            step_id_str = str(st.get("id") or st.get("step_id") or "unknown")
            raise UnsupportedCodegenActionError(step_id=step_id_str, action=str(action or "unknown"), engine="sql")
                
    return expr


def _accumulate_status(col_clean: str, status_expr: str) -> str:
    """Accumulates validation status without overwriting previous rule findings."""
    return (
        f"CASE "
        f"WHEN [{col_clean}_validation_status] IS NULL OR [{col_clean}_validation_status] = 'valid' THEN {status_expr} "
        f"WHEN {status_expr} IS NOT NULL AND {status_expr} != 'valid' AND [{col_clean}_validation_status] NOT LIKE '%' + {status_expr} + '%' "
        f"THEN [{col_clean}_validation_status] + ';' + {status_expr} "
        f"ELSE [{col_clean}_validation_status] END"
    )


def _accumulate_reason(col_clean: str, reason_expr: str) -> str:
    """Accumulates validation reason without overwriting previous rule findings."""
    return (
        f"CASE "
        f"WHEN [{col_clean}_validation_reason] IS NULL THEN {reason_expr} "
        f"WHEN {reason_expr} IS NOT NULL AND [{col_clean}_validation_reason] NOT LIKE '%' + {reason_expr} + '%' "
        f"THEN [{col_clean}_validation_reason] + '; ' + {reason_expr} "
        f"ELSE [{col_clean}_validation_reason] END"
    )


def _supplement_steps_with_dq_issues(
    existing_steps: List[Dict[str, Any]],
    ds_name: str,
    assessment: Dict[str, Any],
    plan: Dict[str, Any],
    known_cols: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    steps = list(existing_steps)
    stepped_cols = {str(st.get("column") or "").lower(): str(st.get("action") or "").lower() for st in steps if st.get("column")}
    
    issues_for_ds: List[Dict[str, Any]] = []
    ds_clean = ds_name.replace("dbo.", "").strip()
    
    # 1. From assessment.data_quality_issues
    dq_issues_ds = (assessment.get("data_quality_issues") or {}).get("datasets") or {}
    for dsk, block in dq_issues_ds.items():
        if dsk == ds_name or dsk == ds_clean or dsk.replace("dbo.", "") == ds_clean:
            issues_for_ds.extend(block.get("issues") or [])
            
    # 2. From assessment.datasets.<ds>.quality.issues
    ds_info = (assessment.get("datasets") or {}).get(ds_name) or (assessment.get("datasets") or {}).get(ds_clean) or {}
    if isinstance(ds_info, dict):
        q = ds_info.get("quality") or {}
        issues_for_ds.extend(q.get("issues") or [])
        for cname, cmeta in (ds_info.get("columns") or {}).items():
            if isinstance(cmeta, dict):
                for iss in (cmeta.get("issues") or []):
                    issues_for_ds.append({**iss, "column": cname})
                
    # 3. From plan.manual_review
    for item in (plan.get("manual_review") or []):
        dsk = str(item.get("dataset") or "")
        if dsk == ds_name or dsk == ds_clean or dsk.replace("dbo.", "") == ds_clean:
            issues_for_ds.append(item)
            
    seen_issues = set()
    for iss in issues_for_ds:
        col = iss.get("column")
        col_clean_name = str(col).strip("[]").lower() if col else "*"
        col_l = col_clean_name
        
        # Entity boundary enforcement: reject rules for columns outside this dataset schema
        if known_cols and col and col not in ("*", "[Row-level]", "[row-level]") and col_clean_name not in known_cols:
            continue
            
        itype = str(iss.get("issue_type") or iss.get("rule_id") or "").lower()
        iss_id = str(iss.get("id") or iss.get("issue_id") or "").strip()
        if iss_id and iss_id in seen_issues:
            continue
        if (col_l, itype) in seen_issues:
            continue
        seen_issues.add((col_l, itype))
        if iss_id:
            seen_issues.add(iss_id)
        
        existing_act = stepped_cols.get(col_l)
        
        # 1. Currency/Price Negative values
        is_price = any(k in col_l for k in ("price", "cost", "fee", "amount", "budget", "salary", "revenue", "sales", "fare", "balance"))
        if is_price and any(k in itype for k in ("negative", "non_negative", "cross_field_non_negative", "negative_values")):
            if not existing_act or existing_act not in ("range_clip", "clip_outliers"):
                steps.append({
                    "action": "range_clip",
                    "column": col,
                    "params": {"lower_bound": 0.0, "min_value": 0.0},
                    "source_issue_type": "negative_values",
                    "note": "Remediate negative price/measure values with lower bound 0"
                })
                stepped_cols[col_l] = "range_clip"
                continue

        # 2. Age bounds
        if col_l == "age" and any(k in itype for k in ("negative", "outlier", "impossible", "range", "implausible", "bound", "invalid", "age")):
            if not existing_act or existing_act not in ("range_clip", "clip_outliers"):
                steps.append({
                    "action": "range_clip",
                    "column": col,
                    "params": {"lower_bound": 0, "upper_bound": 120, "min_value": 0, "max_value": 120},
                    "source_issue_type": "implausible_age",
                    "note": "Remediate age bounds [0, 120]"
                })
                stepped_cols[col_l] = "range_clip"
                continue

        # 3. Dates (impossible calendar dates or sentinels)
        is_date = any(k in col_l for k in ("date", "created", "timestamp", "dob", "birth"))
        if is_date and any(k in itype for k in ("date", "impossible", "sentinel", "dummy", "future", "format")):
            if not existing_act or existing_act not in ("parse_dates", "parse_dates_safe", "nullify_date"):
                steps.append({
                    "action": "parse_dates",
                    "column": col,
                    "params": {"filter_impossible": True},
                    "source_issue_type": "impossible_date",
                    "note": "Safely parse dates and normalize sentinel/impossible dates to NULL"
                })
                stepped_cols[col_l] = "parse_dates"
                continue

        # 4. Email
        if "email" in col_l and ("email" in itype or "syntax" in itype or "format" in itype or "placeholder" in itype):
            if not existing_act or existing_act != "sanitize_email":
                steps.append({
                    "action": "sanitize_email",
                    "column": col,
                    "source_issue_type": "invalid_email",
                    "note": "Sanitize and validate email address format"
                })
                stepped_cols[col_l] = "sanitize_email"
                continue

        # 5. Phone
        if "phone" in col_l and ("phone" in itype or "format" in itype or "digit" in itype):
            if not existing_act or existing_act != "normalize_phone":
                steps.append({
                    "action": "normalize_phone",
                    "column": col,
                    "source_issue_type": "invalid_phone",
                    "note": "Normalize phone number to digits"
                })
                stepped_cols[col_l] = "normalize_phone"
                continue

        # 5b. Numeric sentinels (e.g. InstructorID, or any sentinel_numeric_value)
        if any(k in col_l for k in ("instructorid", "instructor_id")) or any(k in itype for k in ("sentinel_numeric_value", "sentinel_value", "magic_number")):
            if not existing_act or existing_act not in ("replace_sentinel_values", "normalize_registered_placeholder"):
                steps.append({
                    "action": "replace_sentinel_values",
                    "column": col,
                    "params": {"replace_values": [999, -999, 999999, 9999999]},
                    "source_issue_type": "sentinel_value",
                    "note": "Nullify configured sentinel values (999, -999, 999999, 9999999)"
                })
                stepped_cols[col_l] = "replace_sentinel_values"
                continue

        # 6. Text placeholders
        if any(k in itype for k in ("placeholder", "sentinel")) or (col_l in ("customername", "city", "name", "address", "state", "title") and "null" not in itype and "unique" not in itype and "duplicate" not in itype):
            if not existing_act or existing_act not in ("replace_sentinel_values", "normalize_registered_placeholder"):
                steps.append({
                    "action": "replace_sentinel_values",
                    "column": col,
                    "source_issue_type": "placeholder_detected",
                    "note": "Normalize text placeholders ('???', 'n/a', 'unknown') to NULL"
                })
                stepped_cols[col_l] = "replace_sentinel_values"
                continue

        # 7. Duplicates
        if any(k in itype for k in ("duplicate", "uniqueness", "exact_duplicate_rows", "business_key_duplicate")):
            has_dedup = any(str(st.get("action") or "").lower() in ("deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate") for st in steps)
            if not has_dedup:
                steps.append({
                    "action": "flag_duplicate_candidates",
                    "column": "[Row-level]",
                    "source_issue_type": "exact_duplicate_rows",
                    "note": "Flag duplicate records in staging"
                })

    return steps



def generate_dataset_sql(
    dataset_name: str,
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    dialect: str = "tsql",
    execution_target: Optional[str] = None,
    clean_schema: Optional[str] = None,
    timestamp_suffix: str = "",
) -> str:
    """
    Generate isolated, independent ETL SQL for a single dataset.
    Does not merge multiple dataset pipelines into one script.
    """
    import copy
    isolated_plan = copy.deepcopy(plan)
    norm_target = dataset_name.replace("dbo.", "").strip("[]").lower()
    matching_datasets = {}
    for k, v in (plan.get("datasets") or {}).items():
        if k.replace("dbo.", "").strip("[]").lower() == norm_target:
            matching_datasets[k] = v

    if not matching_datasets:
        matching_datasets[dataset_name] = {"steps": []}

    isolated_plan["datasets"] = matching_datasets
    isolated_plan["relationships"] = {}

    isolated_assessment = copy.deepcopy(assessment) if assessment else {}
    if isolated_assessment and isinstance(isolated_assessment.get("datasets"), dict):
        matching_assess = {}
        for k, v in isolated_assessment["datasets"].items():
            if k.replace("dbo.", "").strip("[]").lower() == norm_target:
                matching_assess[k] = v
        isolated_assessment["datasets"] = matching_assess

    if clean_schema:
        isolated_plan.setdefault("business_rules", {})["clean_schema"] = clean_schema
    if timestamp_suffix:
        isolated_plan["timestamp_suffix"] = timestamp_suffix

    return generate_sql_etl(
        isolated_plan,
        isolated_assessment,
        dialect=dialect,
        execution_target=execution_target,
        target_dataset=dataset_name,
    )


def generate_master_orchestrator(
    plan: Dict[str, Any],
    assessment: Dict[str, Any] = None,
    *,
    dialect: str = "tsql",
    execution_target: Optional[str] = None,
    dataset_names: Optional[List[str]] = None,
) -> str:
    """
    Generate a standalone master orchestrator package that coordinates
    independent dataset ETL procedures (etl_clean_*, etl_transform_*).
    """
    assessment = assessment or {}
    dialect = (dialect or "tsql").lower()
    is_odbc_or_azure = (
        (execution_target and str(execution_target).upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
        or (str((plan.get("business_rules") or {}).get("execution_target") or "").upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
        or (str(plan.get("execution_target") or "").upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
    )

    datasets = dataset_names or list((plan.get("datasets") or {}).keys())
    if not datasets and assessment and isinstance(assessment.get("datasets"), dict):
        datasets = list(assessment["datasets"].keys())

    emitted_objects: Set[str] = set()
    lines: List[str] = [
        f"-- Master ETL Orchestrator — Agent Dhara — plan_id={plan.get('plan_id', 'unknown')}",
        f"-- dialect={dialect} — execution_target={execution_target or 'default'}",
        "",
    ]

    # Infrastructure
    infra_ddl = get_infrastructure_ddl(
        include_rejects=True,
        include_watermark=True,
        dialect=dialect,
        include_go=True,
        emitted_objects=emitted_objects,
    )
    if infra_ddl:
        lines.append("-- ============================================================")
        lines.append("-- Infrastructure Tables")
        lines.append("-- ============================================================")
        lines.append(infra_ddl)

    # Master procedure
    lines.append("-- ============================================================")
    lines.append("-- Master Orchestrator Stored Procedure")
    lines.append("-- ============================================================")
    lines.append("CREATE OR ALTER PROCEDURE dbo.etl_main")
    lines.append("    @load_type VARCHAR(20) = 'FULL',")
    lines.append("    @last_run DATETIME = NULL")
    lines.append("AS BEGIN")
    lines.append("    SET NOCOUNT ON;")
    lines.append("    -- Retrieve last run watermark if not provided")
    lines.append("    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL")
    lines.append("    BEGIN")
    lines.append("        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';")
    lines.append("    END;")
    lines.append("")
    lines.append("    INSERT INTO dbo.etl_log (process_name, start_time, status)")
    lines.append("    VALUES ('etl_main', GETDATE(), 'RUNNING');")
    lines.append("    DECLARE @run_id INT = SCOPE_IDENTITY();")
    lines.append("")
    lines.append("    BEGIN TRY")

    generation_mode = str(plan.get("generation_mode") or "full").lower()
    for ds_name in datasets:
        tbl_base = ds_name.split(".")[-1].strip("[]")
        if generation_mode in ("cleanse", "cleanse_only", "phase1", "full"):
            lines.append(f"        EXEC dbo.etl_clean_{tbl_base} @load_type = @load_type, @last_run = @last_run;")
        if generation_mode in ("transform", "transform_only", "phase2", "full"):
            lines.append(f"        EXEC dbo.etl_transform_{tbl_base} @load_type = @load_type, @last_run = @last_run;")

    lines.append("        -- Update master process watermark")
    lines.append("        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL")
    lines.append("        BEGIN")
    lines.append("            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target")
    lines.append("            USING (SELECT 'etl_main' AS process_name) AS source")
    lines.append("            ON target.process_name = source.process_name")
    master_wm = "COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE())"
    lines.append("            WHEN MATCHED THEN")
    lines.append(f"                UPDATE SET last_run_time = {master_wm}")
    lines.append("            WHEN NOT MATCHED THEN")
    lines.append(f"                INSERT (process_name, last_run_time) VALUES (source.process_name, {master_wm});")
    lines.append("        END")
    lines.append("")
    lines.append("        UPDATE dbo.etl_log")
    lines.append("        SET end_time = GETDATE(), status = 'SUCCESS'")
    lines.append("        WHERE id = @run_id;")
    lines.append("    END TRY")
    lines.append("    BEGIN CATCH")
    lines.append("        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;")
    if is_odbc_or_azure:
        lines.append("        DECLARE @err_msg NVARCHAR(4000);")
        lines.append("        SET @err_msg = ERROR_MESSAGE();")
        lines.append("")
        lines.append("        UPDATE dbo.etl_log")
        lines.append("        SET status = 'FAILED',")
        lines.append("            error_message = @err_msg")
        lines.append("        WHERE id = @run_id;")
        lines.append("")
        lines.append("        RAISERROR(@err_msg, 16, 1);")
    else:
        lines.append("        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();")
        lines.append("        UPDATE dbo.etl_log")
        lines.append("        SET end_time = GETDATE(), status = 'FAILED', error_message = @err")
        lines.append("        WHERE id = @run_id;")
        lines.append("        THROW;")
    lines.append("    END CATCH;")
    lines.append("END;")
    lines.append("GO")
    lines.append("")
    lines.append("-- ============================================================")
    lines.append("-- Auto-Execute: Run Master ETL pipeline")
    lines.append("-- ============================================================")
    lines.append("PRINT 'Starting Master ETL pipeline execution...';")
    lines.append("EXEC dbo.etl_main @load_type = 'FULL';")
    lines.append("PRINT 'Master ETL pipeline execution complete.';")
    lines.append("GO")
    lines.append("")

    result_sql = "\n".join(lines)
    _check_forbidden_generator_tokens(result_sql)
    return result_sql


def _check_forbidden_generator_tokens(sql: str) -> None:
    """Validate that no unresolved generator variables or dynamic SQL fragments leak into final SQL."""
    forbidden_patterns = [
        "+ @clean_table_name +",
        "+ @target_table +",
        "+ @table_name +",
        "'+ @clean_table_name",
        "'+ @target_table",
        "'+ @table_name",
        "' + @clean_table_name",
        "' + @target_table",
        "' + @table_name",
        "@clean_table_name",
        "@target_table",
        "@clean_table",
        "@clean_suffix",
        "+ N'",
        "_Clean_]",
        "_clean_]",
        "_Clean_ ",
        "_clean_ ",
    ]
    detected = [p for p in forbidden_patterns if p in sql]
    if detected:
        raise ValueError(
            f"Unresolved dynamic SQL generator token(s) {detected} detected in generated SQL. Generation failed."
        )


def generate_sql_etl(plan: Dict[str, Any], assessment: Dict[str, Any], *, dialect: str = "tsql", execution_target: Optional[str] = None, target_dataset: Optional[str] = None) -> str:

    """
    Generate commented SQL scripts (T-SQL biased: UPDATE / TRY_CAST / QUOTENAME patterns).
    `dialect`: 'tsql' | 'ansi' (ansi uses portable comments only for risky bits).
    """
    gen_mode = str(plan.get("generation_mode") or "full").lower().strip()
    if gen_mode == "cleanse_only":
        from agent.etl_pipeline.phase_classifier import split_plan_phases
        cleanse_plan, _ = split_plan_phases(plan)
        cleanse_plan["relationships"] = {}  # Phase 1 (Cleanse Only): no joins or bridge tables
        cleanse_plan["generation_mode"] = "cleanse_only"
        plan = cleanse_plan
    elif gen_mode == "transform_only":
        from agent.etl_pipeline.phase_classifier import split_plan_phases
        _, transform_plan = split_plan_phases(plan)
        transform_plan["generation_mode"] = "transform_only"
        plan = transform_plan

    dialect = (dialect or "tsql").lower()
    is_odbc_or_azure = (
        (execution_target and str(execution_target).upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
        or (str((plan.get("business_rules") or {}).get("execution_target") or "").upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
        or (str(plan.get("execution_target") or "").upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL"))
    )
    emitted_objects: Set[str] = set()
    default_values_to_seed = {}
    invalid_values_to_seed = {}
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules: Dict[str, Any] = plan.get("business_rules") or {}
    clean_schema: Optional[str] = business_rules.get("clean_schema") or plan.get("clean_schema") or None
    timestamp_suffix: str = str(plan.get("timestamp_suffix") or business_rules.get("timestamp_suffix") or "")
    if not timestamp_suffix and bool(business_rules.get("include_timestamp") or plan.get("include_timestamp")):
        from datetime import datetime
        timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    excluded_columns: List[str] = business_rules.get("exclude_columns") or []
    valid_values = business_rules.get("valid_values") or {}

    from agent.etl_pipeline.action_registry import ACTION_REGISTRY
    _SQL_SUPPORTED_ACTIONS = frozenset(
        act for act, meta in ACTION_REGISTRY.items() if not meta.get("invalid")
    ) | frozenset({
        "trim", "trim_preserve_case", "lowercase", "uppercase", "sanitize_email", "normalize_phone",
        "hash_phone", "mask_phone", "standardize_boolean", "regex_replace",
        "range_clip", "clip_outliers", "cap_outliers", "flag_outliers", "clip_or_flag",
        "replace_sentinel_values", "normalize_registered_placeholder", "nullify_sentinels",
        "nullify_dummy_dates", "nullify_punctuation", "nullify_date", "nullify_future_dates",
        "coerce_numeric", "cast_type", "replace_values", "zero_to_null",
        "parse_dates", "parse_dates_safe",
        "fill_nulls_simple", "fill_or_drop", "fill_constant", "fill_null_with_constant", "fill_nulls_flag",
        "preserve_null_and_flag", "preserve_and_flag", "preserve_null",
        "preserve_missing_suspected_key", "accept_null_as_optional",
        "flag_only", "nullify_clean_value",
        "flag_missing_key", "flag_missing_declared_key", "flag_domain_violation",
        "flag_range_violation", "flag_cross_field_violation", "flag_high_null_key",
        "quarantine_row", "quarantine_null_rows", "quarantine_missing_key",
        "quarantine_orphan_fk", "quarantine_key_null", "drop_rows",
        "impute_numeric", "impute_date",
        "create_normalized_match_key", "join_datasets",
        "accept_null_as_optional", "skip_etl", "skip_requirement", "review_manually", "force_unlock",
        "deduplicate", "flag_business_key_duplicate", "flag_duplicate_candidates",
        "flag_near_duplicate_cluster", "validate_referential_integrity_or_stage",
        "validate_and_partition_relationship", "at_least_one",
        "drop_column", "exclude_column", "noop", "keep_as_is",
    })
    for ds_val in (plan.get("datasets") or {}).values():
        for st in (ds_val.get("steps") or []):
            st_act = str(st.get("action") or "").lower()
            if st_act and st_act not in _SQL_SUPPORTED_ACTIONS:
                st_id = str(st.get("id") or st.get("step_id") or "unknown")
                raise UnsupportedCodegenActionError(step_id=st_id, action=st_act, engine="sql")

    lines: List[str] = [
        f"-- ETL SQL — Agent Dhara — plan_id={plan_id}",
        f"-- dialect={dialect} — review before executing against production.",
        "",
    ]
    if dialect != "tsql":
        lines.insert(
            1,
            "-- ANSI mode is template-only; adjust statements to your SQL engine before running.",
        )

    notes_raw = business_rules.get("notes") or ""
    notes = "\n".join(
        line for line in str(notes_raw).strip().splitlines() if line.strip()
    )
    if notes:
        lines.extend(["-- Business notes:", "-- " + notes.replace("\n", "\n-- "), ""])

    # Emit excluded columns as an audit block so the user knows what was skipped
    if excluded_columns:
        lines.append(f"-- Excluded columns (business rule — no transforms generated for these):")
        for col in excluded_columns:
            lines.append(f"--   [{col}]")
        lines.append("")

    manual = plan.get("manual_review") or []
    if manual:
        lines.append(f"-- ⚠ {len(manual)} item(s) flagged for manual review before production run.")
        for item in manual:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = (item.get("message") or "")[:200]
            lines.append(f"--   [{ds}] {col}: {msg}")
        lines.append("")

    ectx = assessment.get("enriched_etl_context") or {}
    sem_pkg = assessment.get("semantic_context") or {}
    if isinstance(ectx, dict) and (
        ectx.get("semantic_overall_confidence") is not None
        or ectx.get("drift_summary")
        or ectx.get("drift_score") is not None
    ):
        lines.append("-- Governance / readiness context (from latest assessment)")
        if ectx.get("semantic_overall_confidence") is not None:
            lines.append(f"--   semantic_overall_confidence={ectx.get('semantic_overall_confidence')}")
        if ectx.get("drift_summary"):
            lines.append(f"--   drift_severity_by_dataset={ectx.get('drift_summary')}")
        if ectx.get("reconciliation_ok") is not None:
            lines.append(f"--   reconciliation_ok={ectx.get('reconciliation_ok')}")
        if ectx.get("gx_evaluated") is not None:
            lines.append(f"--   gx_evaluated={ectx.get('gx_evaluated')}")
        if ectx.get("drift_score") is not None:
            lines.append(f"--   drift_score={ectx.get('drift_score')}")
        lines.append("")
    if isinstance(sem_pkg, dict) and sem_pkg.get("by_dataset"):
        lines.append("-- Key business columns (semantic_context)")
        for ds_name, ctx in list((sem_pkg.get("by_dataset") or {}).items())[:12]:
            if not isinstance(ctx, dict):
                continue
            crit = ", ".join(str(c) for c in (ctx.get("critical_columns") or [])[:8])
            if crit:
                lines.append(f"--   [{ds_name}] critical: {crit}")
        lines.append("")

    ds_plan = dict(plan.get("datasets") or {})
    if target_dataset:
        norm_target = target_dataset.replace("dbo.", "").strip("[]").lower()
        ds_plan = {k: v for k, v in ds_plan.items() if k.replace("dbo.", "").strip("[]").lower() == norm_target}
        if not ds_plan:
            ds_plan[target_dataset] = {"steps": []}
    elif assessment and isinstance(assessment.get("datasets"), dict):
        for ds_k in assessment["datasets"].keys():
            norm_k = ds_k.replace("dbo.", "").strip("[]").lower()
            matched = any(k.replace("dbo.", "").strip("[]").lower() == norm_k for k in ds_plan)
            if not matched:
                ds_plan[ds_k] = {"steps": []}

    # Emit logging table definition for T-SQL dialect
    if dialect == "tsql":
        lines.append("-- ============================================================")
        lines.append("-- Infrastructure Section: Logging, rejects, watermark, configuration")
        lines.append("-- ============================================================")
        infra_ddl = get_infrastructure_ddl(
            include_rejects=not bool(business_rules.get("never_drop_rows")),
            include_watermark=True,
            include_defaults=bool(default_values_to_seed),
            include_invalid=True,
            dialect=dialect,
            include_go=True,
            emitted_objects=emitted_objects,
        )
        if infra_ddl:
            lines.append(infra_ddl)
        lines.append("-- __DEFAULT_VALUES_DDL_PLACEHOLDER__")
        lines.append("-- __DEFAULT_VALUES_SEED_PLACEHOLDER__")
        lines.append("")

        if clean_schema and clean_schema.lower() != "dbo" and dialect == "tsql":
            lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{clean_schema}')")
            lines.append(f"BEGIN")
            lines.append(f"    EXEC('CREATE SCHEMA [{clean_schema}]');")
            lines.append(f"END;")
            lines.append(f"GO")
            lines.append("")

        # Initialize target clean tables at script level to ensure dependent views compile
        lines.append("-- ============================================================")
        lines.append("-- Initialize Clean Tables if not exists")
        lines.append("-- ============================================================")
        generation_mode = str(plan.get("generation_mode") or "full").lower()
        is_clean_script = (generation_mode != "transform_only")
        phase_suffix_script = "Clean" if is_clean_script else "Transformed"
        for ds_name, block in ds_plan.items():
            tbl_clean = _get_clean_table_name(ds_name, target_schema=clean_schema, timestamp_suffix=timestamp_suffix) if is_clean_script else _get_transformed_table_name(ds_name)
            tbl_base = ds_name.split(".")[-1].strip("[]")
            clean_tbl = tsql_qualified_name(tbl_clean)
            tbl_norm = tbl_clean.lower().strip("[]")
            if tbl_norm in emitted_objects or f"dbo.{tbl_norm.split('.')[-1]}" in emitted_objects:
                continue
            emitted_objects.add(tbl_norm)
            emitted_objects.add(f"dbo.{tbl_norm.split('.')[-1]}")
            
            # Get columns info
            ds_info = assessment.get("datasets", {}).get(ds_name) or {} if assessment else {}
            cols_info = ds_info.get("columns") or {}
            local_excluded_columns = set(excluded_columns)
            for st in (block.get("steps") or []):
                if st.get("action") in ("exclude_column", "drop_column") and st.get("column"):
                    local_excluded_columns.add(st.get("column"))
            cols = [c for c in cols_info.keys() if c not in local_excluded_columns]
            
            # Fallback primary key search
            pk_col = None
            if cols:
                for col_name, cmeta in cols_info.items():
                    if cmeta.get("candidate_primary_key") and not pk_col:
                        pk_col = col_name
                if not pk_col:
                    for c in cols:
                        if c.lower().endswith("id") or c.lower().endswith("key"):
                            pk_col = c
                            break
                    if not pk_col:
                        pk_col = cols[0]
            
            scd_config = (business_rules.get("scd") or {}).get(ds_name) or {}
            scd_type_raw = scd_config.get("type") or block.get("scd_type") or business_rules.get("scd_type") or "type1"
            scd_type = str(scd_type_raw).lower().replace(" ", "")
            if scd_type not in ("type1", "type2", "append", "truncate"):
                scd_type = "type1"
            if not pk_col and scd_type in ("type1", "type2"):
                scd_type = "append"
            
            index_keys = []
            rel = plan.get("relationships") or {}
            for j in rel.get("joins") or []:
                if j.get("parent_dataset") == ds_name:
                    index_keys.append(j.get("parent_key"))
                elif j.get("child_dataset") == ds_name:
                    index_keys.append(j.get("child_key"))
            
            # resolve watermark column
            watermark_col = _resolve_watermark_column(cols_info)
            if watermark_col:
                index_keys.append(watermark_col)
            index_keys = sorted(list(set(index_keys)))

            # Explicit CREATE TABLE
            create_cols = []
            for c in cols:
                c_meta = cols_info.get(c) or {}
                c_type = c_meta.get("dtype") or c_meta.get("target_dtype") or ""
                target_type = get_sql_cast_type(c_type, c)
                # Primary key and index columns cannot be NVARCHAR(MAX) or VARCHAR(MAX) in SQL Server (Msg 1919)
                if (c == pk_col or c in index_keys) and "MAX" in target_type.upper():
                    target_type = "NVARCHAR(256)"
                
                # Check nullability
                non_nullable_cols = business_rules.get("non_nullable") or []
                if c == pk_col or c in non_nullable_cols:
                    null_str = "NOT NULL"
                else:
                    null_str = "NULL"
                create_cols.append(f"    [{c}] {target_type} {null_str}")
            
            # Add metadata columns
            create_cols.append("    etl_created_at DATETIME DEFAULT GETDATE()")
            create_cols.append("    etl_updated_at DATETIME DEFAULT GETDATE()")
            create_cols.append("    etl_batch_id INT NULL")
            
            if scd_type == "type2":
                create_cols.append("    start_date DATETIME DEFAULT GETDATE()")
                create_cols.append("    end_date DATETIME DEFAULT '9999-12-31'")
                create_cols.append("    is_current BIT DEFAULT 1")
            
            if pk_col:
                if scd_type == "type2":
                    create_cols.append(f"    CONSTRAINT [PK_{tbl_base}_{phase_suffix_script}] PRIMARY KEY ([{pk_col}], start_date)")
                else:
                    create_cols.append(f"    CONSTRAINT [PK_{tbl_base}_{phase_suffix_script}] PRIMARY KEY ([{pk_col}])")
            
            lines.append(f"IF OBJECT_ID('{tbl_clean}', 'U') IS NULL")
            lines.append("BEGIN")
            lines.append(f"    CREATE TABLE {clean_tbl} (")
            lines.append(",\n".join(create_cols))
            lines.append("    );")
            
            for ik in index_keys:
                ik_type = get_sql_cast_type((cols_info.get(ik) or {}).get("dtype"), ik)
                if "MAX" in ik_type.upper():
                    continue
                lines.append(f"    CREATE NONCLUSTERED INDEX idx_{tbl_base}_{phase_suffix_script}_{ik} ON {clean_tbl}([{ik}]);")
            lines.append("END;")
            lines.append("GO")
            lines.append("")

    # Outlier handling uses static compile-time CTE expressions directly in stored procedures

    if not ds_plan:
        lines.append("-- No datasets found in plan.")

    generation_mode = str(plan.get("generation_mode") or "full").lower()

    for ds_name, block in ds_plan.items():
        tbl_base = ds_name.split(".")[-1].strip("[]")
        tbl_clean = _get_clean_table_name(ds_name, target_schema=clean_schema, timestamp_suffix=timestamp_suffix)
        tbl_transformed = _get_transformed_table_name(ds_name)
        
        phases_to_gen = []
        if generation_mode in ("cleanse", "cleanse_only", "phase1", "full"):
            phases_to_gen.append("clean")
        if generation_mode in ("transform", "transform_only", "phase2", "full"):
            phases_to_gen.append("transform")
            
        for ph in phases_to_gen:
            is_clean = (ph == "clean")
            sp_name = f"etl_clean_{tbl_base}" if is_clean else f"etl_transform_{tbl_base}"
            
            # Resolve SCD Type for this dataset phase
            # Check per-dataset SCD config first (e.g. business_rules["scd"]["dbo.table"]["type"])
            scd_config = (business_rules.get("scd") or {}).get(ds_name) or {}
            scd_type_raw = scd_config.get("type") or block.get("scd_type") or business_rules.get("scd_type") or "type1"
            scd_type = str(scd_type_raw).lower().replace(" ", "")
            # Normalize common aliases
            if scd_type in ("scd2", "scd_type_2", "scd_type2", "type_2"):
                scd_type = "type2"
            elif scd_type in ("scd1", "scd_type_1", "scd_type1", "type_1"):
                scd_type = "type1"
            if scd_type not in ("type1", "type2", "append", "truncate"):
                scd_type = "type1"
            
            tbl_clean = _get_clean_table_name(ds_name, target_schema=clean_schema, timestamp_suffix=timestamp_suffix)
            target_tbl_str = tbl_clean if is_clean else tbl_transformed
            src_tbl_expr = tsql_qualified_name(ds_name) if is_clean else tsql_qualified_name(tbl_clean)
            tgt_tbl_expr = tsql_qualified_name(tbl_clean) if is_clean else tsql_qualified_name(tbl_transformed)
            if dialect != "tsql":
                src_tbl_expr = _brk(ds_name) if is_clean else _brk(tbl_clean)
                tgt_tbl_expr = _brk(tbl_clean) if is_clean else _brk(tbl_transformed)
                
            raw_tbl = src_tbl_expr
            clean_tbl = tgt_tbl_expr
            tbl_base = ds_name.split(".")[-1].strip("[]")
            stg_base = tbl_base[:-8] if tbl_base.lower().endswith("_staging") else tbl_base
            tbl_staging = f"#{stg_base}_Staging" if is_clean else f"#{stg_base}_Transform_Staging"
        
        
            # Filter steps for current phase
            from agent.etl_pipeline.phase_classifier import classify_action_phase
            steps_raw = block.get("steps") or []
            steps_phase = []
            for st in steps_raw:
                act_phase = classify_action_phase(st.get("action"))
                if is_clean and act_phase == "cleanse":
                    steps_phase.append(st)
                elif not is_clean and act_phase == "transform":
                    steps_phase.append(st)

            # Consolidate, filter, and sort steps for this dataset
            local_excluded_columns = set(excluded_columns)
            skipped_comments = []
            for st in (block.get("steps") or []):
                if st.get("action") in ("exclude_column", "drop_column") and st.get("column"):
                    col_name = st.get("column")
                    local_excluded_columns.add(col_name)
                    skipped_comments.append(f"-- Column {col_name} skipped via exclude_column transform step")

            ds_info = assessment.get("datasets", {}).get(ds_name) or {} if assessment else {}
            if not ds_info and assessment:
                ds_clean = ds_name.replace("dbo.", "").strip()
                ds_info = assessment.get("datasets", {}).get(ds_clean) or {}
            cols_info = ds_info.get("columns") or {}
            cols = [c for c in cols_info.keys() if c not in local_excluded_columns]
            cols_lower = {c.strip("[]").lower(): c for c in cols}
            known_cols = set(cols_lower.keys())

            if is_clean:
                steps_phase = _supplement_steps_with_dq_issues(steps_phase, ds_name, assessment, plan, known_cols=known_cols)
        
            MULTI_COLUMN_ACTIONS = {
                "deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate",
                "flag_near_duplicate_cluster", "at_least_one", "validate_referential_integrity_or_stage"
            }
            expanded_steps_phase = []
            for st in steps_phase:
                if not isinstance(st, dict):
                    continue
                act = str(st.get("action") or "").strip().lower()
                c = st.get("column")
                if not c and isinstance(st.get("columns"), list):
                    c = ", ".join(str(x) for x in st.get("columns"))
                if c and act not in MULTI_COLUMN_ACTIONS and "," in str(c):
                    sub_parts = [p.strip().strip("[]") for p in str(c).split(",") if p.strip() and p.strip() not in ("*", "[Row-level]", "[row-level]")]
                    for sub in sub_parts:
                        st_sub = dict(st)
                        st_sub["column"] = sub
                        expanded_steps_phase.append(st_sub)
                else:
                    expanded_steps_phase.append(st)

            filtered_steps = []
            seen_operations = set()
            seen_issue_ids = set()
        
            for st in expanded_steps_phase:
                if not isinstance(st, dict):
                    continue
                action = str(st.get("action") or "").strip().lower()
                col = st.get("column")
                if not col and isinstance(st.get("columns"), list):
                    col = ", ".join(str(x) for x in st.get("columns"))
                
                # 1. Schema Grounding & Entity Boundary Enforcement:
                # Never emit rules for columns that do not exist in this dataset's schema
                if col and known_cols:
                    if action in MULTI_COLUMN_ACTIONS:
                        col_parts = [p.strip().strip("[]").lower() for p in str(col).split(",") if p.strip()]
                        invalid_parts = [p for p in col_parts if p not in known_cols and p not in ("*", "row-level")]
                        if invalid_parts:
                            iss_id = st.get("issue_id") or st.get("id") or action
                            if isinstance(plan, dict):
                                plan.setdefault("generation_report", {}).setdefault("skipped_rules", []).append({
                                    "issue_id": iss_id, "dataset": ds_name, "columns": col, "reason": f"Columns {invalid_parts} not in dataset schema"
                                })
                            continue
                    else:
                        col_clean_key = str(col).strip("[]").lower()
                        if col_clean_key not in known_cols and col not in ("*", "[Row-level]", "[row-level]"):
                            # Drop rules for columns that do not exist in this table schema
                            iss_id = st.get("issue_id") or st.get("id") or action
                            if isinstance(plan, dict):
                                plan.setdefault("generation_report", {}).setdefault("skipped_rules", []).append({
                                    "issue_id": iss_id, "dataset": ds_name, "column": col, "reason": "Column not in dataset schema"
                                })
                            continue
                        if col_clean_key in cols_lower:
                            st = dict(st)
                            st["column"] = cols_lower[col_clean_key]
                            col = st["column"]

                # 2. Duplicate Rule Suppression (by issue_id)
                issue_id = str(st.get("issue_id") or st.get("id") or "").strip()
                if issue_id and issue_id in seen_issue_ids:
                    continue
                if issue_id:
                    seen_issue_ids.add(issue_id)
            
                # Type-aware filtering for string operations
                if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
                    if col:
                        col_meta = cols_info.get(col) or {}
                        col_class = _classify_column(col, col_meta, plan.get("semantic_schema"), ds_name)
                        if col_class in ("metric", "date"):
                            continue
                        
                # Type-aware filtering for outlier actions
                if action in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
                    if col:
                        col_meta = cols_info.get(col) or {}
                        col_class = _classify_column(col, col_meta, plan.get("semantic_schema"), ds_name)
                        if col_class != "metric":
                            continue
                        
                # Operation deduplication / normalization
                norm_action = action
                if action in ("clip_or_flag", "flag_outliers"):
                    norm_action = "flag_outliers"
                elif action in ("fill_nulls_simple", "fill_or_drop"):
                    norm_action = "fill_or_drop"
                elif action in ("clip_outliers", "cap_outliers"):
                    norm_action = "modify_outliers"
                
                is_internal_trim = (norm_action == "trim") and (st.get("source_issue_type") == "internal_whitespace" or st.get("issue_type") == "internal_whitespace")
                op_key = (norm_action, str(col).lower() if col else None, is_internal_trim)
                if op_key in seen_operations:
                    continue
                seen_operations.add(op_key)
            
                filtered_steps.append(st)
            
            priority = {
                "trim": 10,
                "lowercase": 11,
                "uppercase": 11,
                "sanitize_email": 12,
                "zero_to_null": 30,
                "replace_sentinel_values": 31,
                "cast_type": 35,
                "coerce_numeric": 40,
                "nullify_punctuation": 32,
                "parse_dates": 50,
                "nullify_dummy_dates": 51,
                "regex_replace": 60,
                "replace_values": 61,
                "standardize_boolean": 62,
                "normalize_phone": 63,
                "hash_phone": 64,
                "mask_phone": 65,
                "range_clip": 70,
                "clip_or_flag": 71,
                "flag_outliers": 72,
                "clip_outliers": 73,
                "cap_outliers": 74,
                "fill_or_drop": 78,
                "fill_nulls_simple": 78,
                "deduplicate": 80,
                "flag_duplicate_candidates": 80,
                "flag_business_key_duplicate": 80,
                "flag_near_duplicate_cluster": 80,
                "validate_referential_integrity_or_stage": 90
            }
        
            def get_step_priority(st):
                act = str(st.get("action") or "").strip().lower()
                return priority.get(act, 99)
            
            steps = sorted(filtered_steps, key=get_step_priority)
            for idx, st in enumerate(steps):
                st_copy = dict(st)
                st_copy["order"] = idx + 1
                steps[idx] = st_copy

            never_drop = bool(business_rules.get("never_drop_rows"))
            non_nullable_cols = business_rules.get("non_nullable") or []
        
            # Meta extraction for keys, indexing and incremental loading
            pk_col = None
            watermark_col = None
            if cols_info:
                for col_name, cmeta in cols_info.items():
                    if cmeta.get("candidate_primary_key") and not pk_col:
                        pk_col = col_name
            
                # Prioritized audit watermark selection (strictly excludes business dates like DateOfBirth)
                watermark_col = _resolve_watermark_column(cols_info)
        
            # Fallback key search
            if cols and not pk_col:
                for c in cols:
                    if c.lower().endswith("id") or c.lower().endswith("key"):
                        pk_col = c
                        break
                if not pk_col:
                    pk_col = cols[0]
                
            if not pk_col and scd_type in ("type1", "type2"):
                scd_type = "append"
                
            col_list = ", ".join(f"[{c}]" for c in cols) if cols else "*"
        
            lines.append(f"-- === dataset: {ds_name} === ")

            # Pre-procedure One-Time DDL Setup (Executed outside procedure to avoid Sch-M locks)
            tbl_norm_phase = target_tbl_str.lower().strip("[]")
            if dialect == "tsql" and (tbl_norm_phase not in emitted_objects and f"dbo.{tbl_norm_phase.split('.')[-1]}" not in emitted_objects):
                emitted_objects.add(tbl_norm_phase)
                emitted_objects.add(f"dbo.{tbl_norm_phase.split('.')[-1]}")
                lines.append(f"-- Initialize Clean Table Structure")
                lines.append(f"IF OBJECT_ID('{target_tbl_str}', 'U') IS NULL")
                lines.append("BEGIN")
            
                # Setup index keys
                index_keys = []
                rel = plan.get("relationships") or {}
                for j in rel.get("joins") or []:
                    if j.get("parent_dataset") == ds_name:
                        index_keys.append(j.get("parent_key"))
                    elif j.get("child_dataset") == ds_name:
                        index_keys.append(j.get("child_key"))
                if watermark_col:
                    index_keys.append(watermark_col)
                index_keys = sorted(list(set(index_keys)))

                # Explicit CREATE TABLE
                create_cols = []
                for c in cols:
                    c_meta = cols_info.get(c) or {}
                    c_type = c_meta.get("dtype") or c_meta.get("target_dtype") or ""
                    target_type = get_sql_cast_type(c_type, c)
                    # Primary key and index columns cannot be NVARCHAR(MAX) or VARCHAR(MAX) in SQL Server (Msg 1919)
                    if (c == pk_col or c in index_keys) and "MAX" in target_type.upper():
                        target_type = "NVARCHAR(256)"
                
                    # Check nullability
                    if c == pk_col or c in non_nullable_cols:
                        null_str = "NOT NULL"
                    else:
                        null_str = "NULL"
                    create_cols.append(f"        [{c}] {target_type} {null_str}")
            
                # Add metadata columns
                create_cols.append("        etl_created_at DATETIME DEFAULT GETDATE()")
                create_cols.append("        etl_updated_at DATETIME DEFAULT GETDATE()")
                create_cols.append("        etl_batch_id INT NULL")
                
                # Add SCD Type 2 columns if applicable
                if scd_type == "type2":
                    create_cols.append("        start_date DATETIME DEFAULT GETDATE()")
                    create_cols.append("        end_date DATETIME DEFAULT '9999-12-31'")
                    create_cols.append("        is_current BIT DEFAULT 1")
            
                # Add primary key constraint inline if pk_col exists
                phase_suffix = "Clean" if is_clean else "Transformed"
                if pk_col:
                    if scd_type == "type2":
                        create_cols.append(f"        CONSTRAINT [PK_{tbl_base}_{phase_suffix}] PRIMARY KEY ([{pk_col}], start_date)")
                    else:
                        create_cols.append(f"        CONSTRAINT [PK_{tbl_base}_{phase_suffix}] PRIMARY KEY ([{pk_col}])")
                
                lines.append(f"    CREATE TABLE {clean_tbl} (")
                lines.append(",\n".join(create_cols))
                lines.append("    );")
            
                for ik in index_keys:
                    ik_type = get_sql_cast_type((cols_info.get(ik) or {}).get("dtype"), ik)
                    if "MAX" in ik_type.upper():
                        continue
                    lines.append(f"    CREATE NONCLUSTERED INDEX idx_{tbl_base}_{phase_suffix}_{ik} ON {clean_tbl}([{ik}]);")
                lines.append("END;")
                lines.append("")
            
                # DDL alterations for companion columns
                ddl_lines = []
                seen_clean_companions = set()
                for st in steps:
                    col = st.get("column")
                    action = str(st.get("action") or "").lower()
                    if not col or col in excluded_columns or col in ("*", "[Row-level]", "[row-level]"):
                        continue
                    if action in ("deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate", "flag_near_duplicate_cluster", "validate_referential_integrity_or_stage"):
                        continue
                    sub_cols = [c.strip().strip("[]") for c in str(col).split(",") if c.strip() and c.strip() not in ("*", "[Row-level]", "[row-level]")]
                    for single_col in sub_cols:
                        if single_col in excluded_columns:
                            continue
                        col_clean = str(single_col).replace("'", "''")
                        if action in ("flag_outliers", "clip_or_flag"):
                            ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{target_tbl_str}') AND name = '{col_clean}_outlier_flagged')")
                            ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{col_clean}_outlier_flagged] BIT NOT NULL DEFAULT 0;")
                        elif action == "cast_type":
                            target_t = str(st.get("params", {}).get("target_type") or st.get("params", {}).get("dtype") or col_meta.get("target_dtype") or "").lower()
                            is_monetary = any(m in single_col.lower() for m in ("salary", "price", "amount", "cost", "fee", "revenue", "balance", "budget", "total"))
                            if any(it in target_t for it in ("int", "bigint", "integer")) and not is_monetary:
                                ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{target_tbl_str}') AND name = '{col_clean}_int')")
                                ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{col_clean}_int] BIGINT;")

                        # Companion columns for clean target
                        col_meta = cols_info.get(single_col) or {}
                        col_type = col_meta.get("dtype") or col_meta.get("target_dtype") or ""
                        target_type = get_sql_cast_type(col_type, single_col)
                        for comp, ctype in [
                            (f"{col_clean}_raw", "NVARCHAR(MAX) NULL"),
                            (f"{col_clean}_clean", f"{target_type} NULL"),
                            (f"{col_clean}_validation_status", "VARCHAR(50) NULL"),
                            (f"{col_clean}_validation_reason", "VARCHAR(500) NULL")
                        ]:
                            if comp not in seen_clean_companions:
                                seen_clean_companions.add(comp)
                                ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{target_tbl_str}') AND name = '{comp}')")
                                ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{comp}] {ctype};")

                for comp, ctype in [
                    ("row_validation_status", "VARCHAR(50) NULL"),
                    ("row_validation_reason", "VARCHAR(500) NULL"),
                    ("is_duplicate_row", "BIT NOT NULL DEFAULT 0"),
                ]:
                    if comp not in seen_clean_companions:
                        seen_clean_companions.add(comp)
                        ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{target_tbl_str}') AND name = '{comp}')")
                        ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{comp}] {ctype};")
                
                if ddl_lines:
                    lines.append("-- Add generated transformation columns to Clean Table")
                    lines.extend(ddl_lines)
                    lines.append("")
                
                lines.append("GO")
                lines.append("")
        
            # Stored Procedure boundary setup for T-SQL
            proc_lines: List[str] = []
            proc_lines.extend(skipped_comments)
            if dialect == "tsql":
                lines.append(f"CREATE OR ALTER PROCEDURE dbo.{sp_name}")
                lines.append("    @load_type VARCHAR(20) = 'FULL',")
                lines.append("    @last_run DATETIME = NULL")
                lines.append("AS BEGIN")
                lines.append("    SET NOCOUNT ON;")
                lines.append("    SET XACT_ABORT ON;")
                lines.append("    -- Retrieve last run watermark if not provided")
                lines.append("    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL")
                lines.append("    BEGIN")
                lines.append(f"        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = '{sp_name}';")
                lines.append("    END;")
                lines.append("")
                lines.append(f"    INSERT INTO dbo.etl_log (process_name, start_time, status)")
                lines.append(f"    VALUES ('{sp_name}', GETDATE(), 'RUNNING');")
                lines.append("    DECLARE @run_id INT = SCOPE_IDENTITY();")
                lines.append("")
                lines.append("    BEGIN TRY")
                
                # S3-02: Preflight check warnings
                for col in cols:
                    col_brk = _brk(col)
                    col_safe = re.sub(r'[^a-zA-Z0-9_]', '_', col)
                    if col in non_nullable_cols:
                        lines.append(f"        DECLARE @null_count_{col_safe} INT = (SELECT COUNT(*) FROM {src_tbl_expr} WHERE {col_brk} IS NULL);")
                        lines.append(f"        IF @null_count_{col_safe} > 0")
                        lines.append(f"        BEGIN")
                        lines.append(f"            INSERT INTO dbo.etl_log (process_name, start_time, end_time, status, error_message)")
                        lines.append(f"            VALUES ('{sp_name}', GETDATE(), GETDATE(), 'WARNING', 'Preflight check: column {col_brk} has ' + CAST(@null_count_{col_safe} AS VARCHAR(10)) + ' NULL values in source.');")
                        lines.append(f"        END;")
                    
                    allowed_list = valid_values.get(col) or valid_values.get(col.lower())
                    if allowed_list:
                        sql_list = ", ".join(f"N'{str(val).replace("'", "''")}'" for val in allowed_list)
                        lines.append(f"        DECLARE @invalid_count_{col_safe} INT = (SELECT COUNT(*) FROM {src_tbl_expr} WHERE {col_brk} IS NOT NULL AND {col_brk} NOT IN ({sql_list}));")
                        lines.append(f"        IF @invalid_count_{col_safe} > 0")
                        lines.append(f"        BEGIN")
                        lines.append(f"            INSERT INTO dbo.etl_log (process_name, start_time, end_time, status, error_message)")
                        lines.append(f"            VALUES ('{sp_name}', GETDATE(), GETDATE(), 'WARNING', 'Preflight check: column {col_brk} has ' + CAST(@invalid_count_{col_safe} AS VARCHAR(10)) + ' invalid values not in allowed list.');")
                        lines.append(f"        END;")
                        
                lines.append("        BEGIN TRAN;")
                lines.append("")
            
            # Check if row-level deduplication step exists
            has_row_dedup = any(
                str(st.get("action") or "").lower() == "deduplicate" and str(st.get("column") or "").lower() in ("row-level", "[row-level]")
                for st in steps
            )

            if dialect == "tsql":
                proc_lines.append(f"-- Create Staging Table with raw column types to preserve raw strings")
                proc_lines.append(f"IF OBJECT_ID('tempdb..{tbl_staging}') IS NOT NULL DROP TABLE {tbl_staging};")
                staging_cols_ddl = []
                seen_staging_cols = set()
                for col_name in cols:
                    c_clean = str(col_name).strip("[]")
                    if c_clean.lower() not in seen_staging_cols:
                        seen_staging_cols.add(c_clean.lower())
                        staging_cols_ddl.append(f"[{c_clean}] NVARCHAR(MAX) NULL")
                if "etl_batch_id" not in seen_staging_cols:
                    seen_staging_cols.add("etl_batch_id")
                    staging_cols_ddl.append("etl_batch_id INT NULL")
                for st in steps:
                    st_col = st.get("column")
                    if not st_col or st_col in excluded_columns or st_col in ("*", "[Row-level]", "[row-level]"):
                        continue
                    st_sub_cols = [c.strip().strip("[]") for c in str(st_col).split(",") if c.strip() and c.strip() not in ("*", "[Row-level]", "[row-level]")]
                    for single_st_col in st_sub_cols:
                        if single_st_col in excluded_columns:
                            continue
                        if st.get("action") in ("flag_outliers", "clip_or_flag"):
                            c_name = f"{single_st_col}_outlier_flagged"
                            if c_name.lower() not in seen_staging_cols:
                                seen_staging_cols.add(c_name.lower())
                                staging_cols_ddl.append(f"[{c_name}] INT NULL")
                        if st.get("action") == "cast_type":
                            target_t = str(st.get("params", {}).get("target_type") or st.get("params", {}).get("dtype") or col_meta.get("target_dtype") or "").lower()
                            is_monetary = any(m in single_st_col.lower() for m in ("salary", "price", "amount", "cost", "fee", "revenue", "balance", "budget", "total"))
                            if any(it in target_t for it in ("int", "bigint", "integer")) and not is_monetary:
                                c_name = f"{single_st_col}_int"
                                if c_name.lower() not in seen_staging_cols:
                                    seen_staging_cols.add(c_name.lower())
                                    staging_cols_ddl.append(f"[{c_name}] BIGINT NULL")

                # Add companion columns to staging table so UPDATE statements succeed without schema errors
                for st in steps:
                    st_col = st.get("column")
                    if not st_col or st_col in excluded_columns or st_col in ("*", "[Row-level]", "[row-level]"):
                        continue
                    st_act = str(st.get("action") or "").lower()
                    if st_act in ("deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate", "flag_near_duplicate_cluster", "validate_referential_integrity_or_stage"):
                        continue
                    st_sub_cols = [c.strip().strip("[]") for c in str(st_col).split(",") if c.strip() and c.strip() not in ("*", "[Row-level]", "[row-level]")]
                    for single_st_col in st_sub_cols:
                        if single_st_col in excluded_columns:
                            continue
                        clean_c = str(single_st_col).replace("'", "''")
                        col_meta = cols_info.get(single_st_col) or {}
                        col_type = col_meta.get("dtype") or col_meta.get("target_dtype") or ""
                        target_type = get_sql_cast_type(col_type, single_st_col)
                        for comp in [f"{clean_c}_raw", f"{clean_c}_clean", f"{clean_c}_validation_status", f"{clean_c}_validation_reason"]:
                            if comp.lower() not in seen_staging_cols:
                                seen_staging_cols.add(comp.lower())
                                if "status" in comp:
                                    comp_type = "VARCHAR(50)"
                                elif "reason" in comp:
                                    comp_type = "VARCHAR(500)"
                                elif comp.endswith("_clean"):
                                    comp_type = target_type
                                else:
                                    comp_type = "NVARCHAR(MAX)"
                                staging_cols_ddl.append(f"[{comp}] {comp_type} NULL")
                if "row_validation_status" not in seen_staging_cols:
                    seen_staging_cols.add("row_validation_status")
                    staging_cols_ddl.append("[row_validation_status] VARCHAR(50) NULL")
                if "row_validation_reason" not in seen_staging_cols:
                    seen_staging_cols.add("row_validation_reason")
                    staging_cols_ddl.append("[row_validation_reason] VARCHAR(500) NULL")
                if "is_duplicate_row" not in seen_staging_cols:
                    seen_staging_cols.add("is_duplicate_row")
                    staging_cols_ddl.append("[is_duplicate_row] BIT NOT NULL DEFAULT 0")

                ddl_joined = ", ".join(staging_cols_ddl)
                proc_lines.append(f"CREATE TABLE {tbl_staging} ({ddl_joined});")
                proc_lines.append("")

            # Data copy logic
            if dialect == "tsql":
                proc_lines.append("-- Copy data from Raw to Staging")
            
                def get_copy_sql_lines(where_cond: str = "") -> List[str]:
                    where_clause = f" WHERE {where_cond}" if where_cond else ""
                    col_target_list = f" ({col_list}, etl_batch_id)" if col_list != "*" else ""
                    select_list = f"{col_list}, @run_id" if col_list != "*" else "*, @run_id"
                    return [
                        f"    INSERT INTO {tbl_staging}{col_target_list}",
                        f"    SELECT {select_list} FROM {raw_tbl}{where_clause};"
                    ]

                if watermark_col:
                    parsed_watermark = (
                        f"COALESCE("
                        f"TRY_CONVERT(datetime, [{watermark_col}], 120), "
                        f"TRY_CONVERT(datetime, [{watermark_col}], 103), "
                        f"TRY_CONVERT(datetime, [{watermark_col}], 101), "
                        f"TRY_CONVERT(datetime, [{watermark_col}], 111)"
                        f")"
                    )
                    proc_lines.append("IF @load_type = 'FULL' OR @last_run IS NULL")
                    proc_lines.append("BEGIN")
                    proc_lines.extend(get_copy_sql_lines())
                    proc_lines.append("END")
                    proc_lines.append("ELSE")
                    proc_lines.append("BEGIN")
                    proc_lines.extend(get_copy_sql_lines(f"{parsed_watermark} > @last_run"))
                    proc_lines.append("END")
                else:
                    proc_lines.extend(get_copy_sql_lines())
                proc_lines.append("")

                # Preserve immutable raw snapshot for validations and reject logging
                raw_copies = [f"[{c}_raw] = [{c}]" for c in cols if f"{c}_raw".lower() in seen_staging_cols]
                if raw_copies:
                    proc_lines.append("-- Preserve raw column values before any cleaning or mutation")
                    proc_lines.append(f"UPDATE {tbl_staging}")
                    proc_lines.append("SET " + ",\n    ".join(raw_copies) + ";")
                    proc_lines.append("")
            else:
                lines.append(f"-- ANSI Init Target: CREATE TABLE {tbl_staging} AS SELECT * FROM {raw_tbl};")
                lines.append("")
        
            step_lines = []
            phase_placeholder_norm_lines = []
        
            # Step 0: Normalize empty strings and placeholders to NULL before validation
            if dialect == "tsql" and cols:
                nullify_clauses = [f"[{c}] = CASE WHEN LOWER(LTRIM(RTRIM([{c}]))) IN ('', 'n/a', 'na', 'null', 'unknown', 'dummy', 'test', 'temp', 'placeholder', 'not set') THEN NULL ELSE [{c}] END" for c in cols]
                phase_placeholder_norm_lines.append(f"-- Normalize empty strings and placeholders to NULL before validation")
                phase_placeholder_norm_lines.append(f"UPDATE {tbl_staging}")
                phase_placeholder_norm_lines.append(f"SET " + ",\n    ".join(nullify_clauses) + ";")
                phase_placeholder_norm_lines.append(f"")

            # Phase 1: Validations and Quarantines (deletes/rejects) on the Staging Table
            if is_clean and pk_col and dialect == "tsql" and not never_drop:
                step_lines.append(f"-- Quarantine rows where primary key [{pk_col}] is NULL to dbo.etl_rejects")
                step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                step_lines.append(f"       'Primary key [{pk_col}] is NULL'")
                step_lines.append(f"FROM {tbl_staging} r")
                step_lines.append(f"WHERE r.[{pk_col}] IS NULL;")
                step_lines.append(f"")
                step_lines.append(f"DELETE FROM {tbl_staging} WHERE [{pk_col}] IS NULL;")
                step_lines.append(f"")

            # Manual review quarantines (B3)
            if is_clean and dialect == "tsql":
                for mr in plan.get("manual_review") or []:
                    if mr.get("dataset") == ds_name:
                        col = mr.get("column")
                        if col and col in cols:
                            it = str(mr.get("issue_type") or "").lower()
                            where_cond = f"r.[{col}] IS NULL"
                            if "email" in it:
                                where_cond = f"r.[{col}] NOT LIKE '%@%'"
                            elif "numeric" in it or "number" in it or "type" in it:
                                where_cond = f"TRY_CAST(r.[{col}] AS float) IS NULL AND r.[{col}] IS NOT NULL"
                            elif "phone" in it:
                                where_cond = f"LEN(r.[{col}]) < 7 AND r.[{col}] IS NOT NULL"
                            
                            step_lines.append(f"-- Manual review quarantine for column [{col}] due to {mr.get('issue_type')}")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'manual_review_required: [{col}] - {mr.get('message')}'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE {where_cond};")
                            step_lines.append("")
                            if not never_drop:
                                step_lines.append(f"DELETE FROM {tbl_staging} WHERE {where_cond};")
                                step_lines.append("")

            # Reject non-numeric IDs and decimals/floats
            if dialect == "tsql":
                for c_name in cols:
                    c_meta = cols_info.get(c_name) or {}
                    c_type = c_meta.get("dtype") or c_meta.get("target_dtype") or ""
                    sql_t = get_sql_cast_type(c_type, c_name)
                    c_brk = _brk(c_name)
                    
                    col_class = _classify_column(c_name, c_meta, plan.get("semantic_schema"), ds_name)
                    if col_class == "metric" and not (sql_t in ("BIGINT", "INT", "FLOAT", "REAL") or sql_t.startswith("DECIMAL")):
                        sql_t = "DECIMAL(18, 2)"
                
                    # Check for BIGINT IDs (only if column dtype is actually numeric/integer)
                    is_numeric_id_type = any(x in str(c_type).lower() for x in ("int", "bigint", "smallint", "tinyint", "numeric"))
                    if sql_t == "BIGINT" and is_numeric_id_type and (c_name.lower().endswith("id") or c_name.lower().endswith("key")):
                        step_lines.append(f"-- Quarantine rows where ID column [{c_name}] is not numeric to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       '{c_name} is not numeric'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c_brk} IS NOT NULL AND TRY_CAST(r.{c_brk} AS BIGINT) IS NULL;")
                        step_lines.append(f"")
                        if not never_drop:
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c_brk} IS NOT NULL AND TRY_CAST({c_brk} AS BIGINT) IS NULL;")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Nullify invalid non-numeric ID values for [{c_name}]")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c_brk} = NULL WHERE {c_brk} IS NOT NULL AND TRY_CAST({c_brk} AS BIGINT) IS NULL;")
                            step_lines.append(f"")
                
                    # Check for decimals/floats (e.g. OrderAmount)
                    elif sql_t.startswith("DECIMAL") or sql_t in ("FLOAT", "REAL"):
                        step_lines.append(f"-- Quarantine rows where numeric column [{c_name}] is not numeric to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       '{c_name} is not numeric'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c_brk} IS NOT NULL AND TRY_CAST(r.{c_brk} AS {sql_t}) IS NULL;")
                        step_lines.append(f"")
                        if not never_drop:
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c_brk} IS NOT NULL AND TRY_CAST({c_brk} AS {sql_t}) IS NULL;")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Nullify invalid non-numeric values for [{c_name}]")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c_brk} = NULL WHERE {c_brk} IS NOT NULL AND TRY_CAST({c_brk} AS {sql_t}) IS NULL;")
                            step_lines.append(f"")
            
            if is_clean:
                for nn_col in non_nullable_cols:
                    if nn_col in cols and nn_col != pk_col:
                        nn_c = _brk(nn_col)
                        if not never_drop:
                            step_lines.append(f"-- Quarantine rows where non-nullable [{nn_col}] is NULL to dbo.etl_rejects")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Required column [{nn_col}] is NULL'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{nn_c} IS NULL;")
                            step_lines.append(f"")
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {nn_c} IS NULL;")
                            step_lines.append(f"")
                    
            for st in steps:
                col = st.get("column")
                action = str(st.get("action") or "")
                if not col or col in local_excluded_columns:
                    continue
                c = _brk(col)
            
                if action == "parse_dates":
                    if dialect == "tsql":
                        # 1. Reject NULL dates if it is a high-severity DQ issue (Option A)
                        has_high_null = False
                        if assessment:
                            dq_issues = assessment.get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
                            has_high_null = any(
                                str(iss.get("column")).lower() == str(col).lower()
                                and str(iss.get("type")).lower() == "nulls"
                                and str(iss.get("severity")).lower() == "high"
                                for iss in dq_issues
                            )
                        
                        if has_high_null:
                            # Log null-date rows to etl_rejects (audit only — do NOT delete, nulls are kept)
                            step_lines.append(f"-- Log null dates from {tbl_staging}.{c} to dbo.etl_rejects (audit only; row is kept)")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Date column [{col}] is NULL (row kept)'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NULL;")
                            step_lines.append(f"")
                            # Phase 1: keep the row; the null date stays NULL in the clean table

                        # 2. Unparseable dates handling per policy
                        if not never_drop:
                            step_lines.append(f"-- Quarantine unparseable dates from {tbl_staging}.{c} to dbo.etl_rejects (row quarantined and removed)")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid date (row quarantined)'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND COALESCE(")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 120),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 103),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 101),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 111)")
                            step_lines.append(f") IS NULL;")
                            step_lines.append(f"")
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND COALESCE(TRY_CONVERT(date, {c}, 120), TRY_CONVERT(date, {c}, 103), TRY_CONVERT(date, {c}, 101), TRY_CONVERT(date, {c}, 111)) IS NULL;")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Log and nullify unparseable dates from {tbl_staging}.{c} (audit only; row kept, bad date set to NULL)")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid date (set to NULL, row kept)'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND COALESCE(")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 120),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 103),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 101),")
                            step_lines.append(f"    TRY_CONVERT(date, r.{c}, 111)")
                            step_lines.append(f") IS NULL;")
                            step_lines.append(f"")
                            # Nullify the bad date value — row survives with NULL date
                            step_lines.append(f"-- Nullify unparseable date values in {tbl_staging}.{c} (keep row, set bad date to NULL)")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL")
                            step_lines.append(f"WHERE {c} IS NOT NULL AND COALESCE(")
                            step_lines.append(f"    TRY_CONVERT(date, {c}, 120),")
                            step_lines.append(f"    TRY_CONVERT(date, {c}, 103),")
                            step_lines.append(f"    TRY_CONVERT(date, {c}, 101),")
                            step_lines.append(f"    TRY_CONVERT(date, {c}, 111)")
                            step_lines.append(f") IS NULL;")
                            step_lines.append(f"")
                elif action == "sanitize_email":
                    if dialect == "tsql":
                        is_required = col in non_nullable_cols
                        email_cond = f"(NOT (CAST(r.{c} AS NVARCHAR(MAX)) LIKE '%_@_%._%') OR CAST(r.{c} AS NVARCHAR(MAX)) LIKE '%@%@%' OR CAST(r.{c} AS NVARCHAR(MAX)) LIKE '%.@%' OR CAST(r.{c} AS NVARCHAR(MAX)) LIKE '%..%')"
                        email_cond_del = f"(NOT (CAST({c} AS NVARCHAR(MAX)) LIKE '%_@_%._%') OR CAST({c} AS NVARCHAR(MAX)) LIKE '%@%@%' OR CAST({c} AS NVARCHAR(MAX)) LIKE '%.@%' OR CAST({c} AS NVARCHAR(MAX)) LIKE '%..%')"
                        step_lines.append(f"-- Quarantine invalid emails from {tbl_staging}.{c} to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] with value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid email format'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND {email_cond};")
                        step_lines.append(f"")
                        if not never_drop and is_required:
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND {email_cond_del};")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Nullify invalid email format for column [{col}] (keep row, set value to NULL)")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND {email_cond_del};")
                            step_lines.append(f"")
                elif action == "normalize_phone":
                    if dialect == "tsql":
                        cleaned_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.{c} AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N''), N'+', N''), N'.', N'')"
                        cleaned_expr_del = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST({c} AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N''), N'+', N''), N'.', N'')"
                        is_required = col in non_nullable_cols
                        phone_cond = f"(LEN({cleaned_expr}) NOT BETWEEN 7 AND 15 OR {cleaned_expr} LIKE '%[^0-9]%')"
                        phone_cond_del = f"(LEN({cleaned_expr_del}) NOT BETWEEN 7 AND 15 OR {cleaned_expr_del} LIKE '%[^0-9]%')"
                        step_lines.append(f"-- Quarantine invalid phones from {tbl_staging}.{c} to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] with value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid phone format (must be 7-15 digits)'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND {phone_cond};")
                        step_lines.append(f"")
                        if not never_drop and is_required:
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND {phone_cond_del};")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Nullify invalid phone format for column [{col}] (keep row, set value to NULL)")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND {phone_cond_del};")
                            step_lines.append(f"")
                elif action == "coerce_numeric":
                    if dialect == "tsql":
                        is_required = col in non_nullable_cols
                        col_meta = cols_info.get(col) or {}
                        col_type = col_meta.get("dtype")
                        col_class = _classify_column(col, col_meta, plan.get("semantic_schema"), ds_name)
                        cast_target = "DECIMAL(18, 2)" if col_class == "metric" else "BIGINT"
                        
                        if not never_drop and is_required:
                            step_lines.append(f"-- Quarantine invalid numeric format for required column [{col}]")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Required column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid numeric format (row quarantined)'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND TRY_CAST(r.{c} AS {cast_target}) IS NULL;")
                            step_lines.append(f"")
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND TRY_CAST({c} AS {cast_target}) IS NULL;")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"-- Nullify invalid numeric format for column [{col}] (keep row, set value to NULL)")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is not a valid numeric format (set to NULL)'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND TRY_CAST(r.{c} AS {cast_target}) IS NULL;")
                            step_lines.append(f"")
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND TRY_CAST({c} AS {cast_target}) IS NULL;")
                            step_lines.append(f"")
                elif action == "at_least_one":
                    cols_split = [str(x).strip() for x in str(col).split(",")]
                    cols_brackets = [_brk(x) for x in cols_split]
                    all_null_cond = " AND ".join(f"r.{cb} IS NULL" for cb in cols_brackets)
                    all_null_cond_delete = " AND ".join(f"{cb} IS NULL" for cb in cols_brackets)
                    if dialect == "tsql":
                        if not never_drop:
                            step_lines.append(f"-- Quarantine rows where all of {col} are NULL to dbo.etl_rejects")
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'All of columns [{col}] are NULL'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE {all_null_cond};")
                            step_lines.append(f"")
                            step_lines.append(f"DELETE FROM {tbl_staging} WHERE {all_null_cond_delete};")
                            step_lines.append(f"")
                elif action in ("range_clip", "clip_outliers"):
                    if dialect == "tsql":
                        p = step_params(st)
                        lower_b = p.get("lower_bound", p.get("min_value", 0.0))
                        upper_b = p.get("upper_bound", p.get("max_value"))
                        col_l = col.lower()
                        if upper_b is not None:
                            out_cond = f"(TRY_CAST(r.{c} AS FLOAT) < {lower_b} OR TRY_CAST(r.{c} AS FLOAT) > {upper_b})"
                            err_msg = f"'Column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' is outside valid bounds [{lower_b}, {upper_b}]'"
                        else:
                            out_cond = f"TRY_CAST(r.{c} AS FLOAT) < {lower_b}"
                            err_msg = f"'Column [{col}] value ' + ISNULL(CAST(r.{c} AS NVARCHAR(MAX)), 'NULL') + ' cannot be negative (review required)'"
                        
                        step_lines.append(f"-- Log out-of-range/negative values for [{col}] to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       {err_msg}")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND {out_cond};")
                        step_lines.append(f"")
                elif action in ("replace_sentinel_values", "normalize_registered_placeholder"):
                    if dialect == "tsql":
                        raw_ref = f"r.[{col}_raw]" if f"{col}_raw".lower() in seen_staging_cols else f"r.{c}"
                        placeholder_cond = (
                            f"LOWER(LTRIM(RTRIM(CAST({raw_ref} AS NVARCHAR(MAX))))) IN ('???', 'n/a', 'na', 'null', 'none', 'unknown', 'dummy', 'temp', 'test', '###', '-', 'unspecified') "
                            f"OR TRY_CAST({raw_ref} AS FLOAT) IN (999, -999, 999999, -9999, 99999)"
                        )
                        step_lines.append(f"-- Log placeholder / sentinel values for [{col}] to dbo.etl_rejects (audit only; replaced with NULL)")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] contains registered placeholder/sentinel token ' + CAST({raw_ref} AS NVARCHAR(MAX))")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE {raw_ref} IS NOT NULL AND ({placeholder_cond});")
                        step_lines.append(f"")
                elif action == "validate_referential_integrity_or_stage":
                    p = step_params(st)
                    rel_ds = p.get("related_dataset") or "?"
                    rel_col = p.get("related_column") or "?"
                    fk_action = p.get("fk_action") or "flag"
                    parent_tbl = _get_clean_table_name(rel_ds, target_schema=clean_schema, timestamp_suffix=timestamp_suffix)
                
                    if dialect == "tsql":
                        step_lines.append(f"-- Referential integrity check: {col} -> {rel_ds}.{rel_col} (action={fk_action})")
                        if fk_action == "reject_orphans":
                            if not never_drop:
                                step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                                step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                                step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                                step_lines.append(f"       'Referential integrity violation: [{col}] value ' + CAST(r.{c} AS NVARCHAR(MAX)) + ' does not exist in {parent_tbl}.[{rel_col}]'")
                                step_lines.append(f"FROM {tbl_staging} r")
                                step_lines.append(f"WHERE r.{c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = r.{c});")
                                step_lines.append(f"")
                                step_lines.append(f"DELETE FROM {tbl_staging}")
                                step_lines.append(f"WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                                step_lines.append(f"")
                            else:
                                step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                        elif fk_action == "null_fill_fk":
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                        elif fk_action == "create_unknown_dim_record":
                            step_lines.append(f"INSERT INTO {parent_tbl} ([{rel_col}])")
                            step_lines.append(f"SELECT DISTINCT r.{c}")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = r.{c});")
                            step_lines.append(f"")

            # Business Rule validations based on custom_assertions in business_rules
            custom_assertions = business_rules.get("custom_assertions") or []
            for rule in custom_assertions:
                if not isinstance(rule, dict):
                    continue
                ds_pattern = str(rule.get("dataset") or "*").strip()
                if ds_pattern != "*" and ds_pattern.lower() != ds_name.lower():
                    continue
                assertion_expr = rule.get("assertion")
                if not assertion_expr:
                    continue
                
                sql_cond = translate_assertion_to_sql(assertion_expr, cols)
                severity = str(rule.get("severity") or "medium").lower()
                msg = rule.get("message") or f"Custom assertion failed: {assertion_expr}"
                
                if dialect == "tsql":
                    step_lines.append(f"-- Business rule validation: {msg}")
                    msg_escaped = msg.replace("'", "''")
                    if not never_drop:
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Business rule violation: {msg_escaped}'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE NOT ({sql_cond});")
                        step_lines.append(f"")
                        step_lines.append(f"DELETE FROM {tbl_staging} WHERE NOT ({sql_cond});")
                        step_lines.append(f"")
                    else:
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Business rule violation (kept): {msg_escaped}'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE NOT ({sql_cond});")
                        step_lines.append(f"")

            phase_validate_lines = list(step_lines)
            step_lines = []

            # Post-validation type parsing phase (runs AFTER validation to preserve raw values for reject logging)
            parse_update_clauses = []
            parsed_cols_seen = set()
            for st in steps:
                col = st.get("column")
                if not col or col in local_excluded_columns:
                    continue
                if st.get("action") == "parse_dates" and col not in parsed_cols_seen:
                    parsed_cols_seen.add(col)
                    col_meta = cols_info.get(col) or {}
                    compiled_expr = compile_column_expression(col, [st], col_meta, business_rules, plan.get("semantic_schema"), ds_name)
                    if compiled_expr != f"[{col}]":
                        parse_update_clauses.append(f"[{col}] = {compiled_expr}")

            if parse_update_clauses:
                step_lines.append(f"-- Post-validation date/type parsing on {tbl_staging}")
                update_sql = f"UPDATE {tbl_staging}\nSET " + ",\n    ".join(parse_update_clauses) + "\nWHERE 1=1;"
                step_lines.extend(update_sql.splitlines())
                step_lines.append("")

            phase_parse_lines = list(step_lines)
            step_lines = []

            # Phase 2: Expression-Based Chained Transformations (Single-Pass Update)
            column_transforms = {}
            for st in steps:
                col = st.get("column")
                if not col or col in local_excluded_columns:
                    continue
                action = st.get("action")
                # Only expressions
                if action in (
                    "trim", "trim_preserve_case", "lowercase", "uppercase", "sanitize_email", "normalize_phone", 
                    "hash_phone", "mask_phone", "standardize_boolean", "regex_replace", 
                    "range_clip", "clip_outliers", "replace_sentinel_values", "normalize_registered_placeholder",
                    "coerce_numeric", "cast_type", "replace_values", "nullify_dummy_dates", 
                    "nullify_punctuation", "parse_dates", "parse_dates_safe", "nullify_date", "zero_to_null",
                    "preserve_null_and_flag", "preserve_and_flag", "preserve_null",
                    "preserve_missing_suspected_key", "accept_null_as_optional",
                    "flag_only", "fill_constant", "fill_null_with_constant",
                ):
                    if col not in column_transforms:
                        column_transforms[col] = []
                    column_transforms[col].append(st)
                
            update_clauses = []
            for col_name, col_steps in column_transforms.items():
                has_cast_type = any(st.get("action") == "cast_type" for st in col_steps)
                base_steps = [st for st in col_steps if st.get("action") != "cast_type"]
                col_meta = cols_info.get(col_name) or {}
            
                if base_steps:
                    col_clean = str(col_name).replace("'", "''")
                    compiled_expr = compile_column_expression(col_name, base_steps, col_meta, business_rules, plan.get("semantic_schema"), ds_name)
                    update_clauses.append(f"[{col_clean}_raw] = [{col_name}]")
                    update_clauses.append(f"[{col_name}] = {compiled_expr}")
                    update_clauses.append(f"[{col_clean}_clean] = {compiled_expr}")
                    
                    actions = [st.get("action") for st in base_steps]
                    if any(a in ("range_clip", "clip_outliers") for a in actions):
                        col_l = col_name.lower()
                        is_price = any(k in col_l for k in ("price", "cost", "fee", "amount", "budget", "salary", "revenue", "sales", "fare", "balance"))
                        p = base_steps[0].get("params") or {}
                        if col_l == "age":
                            lower_b = p.get("lower_bound", p.get("min_value", 0))
                            upper_b = p.get("upper_bound", p.get("max_value", 120))
                            status_expr = f"CASE WHEN TRY_CAST([{col_name}] AS INT) < {lower_b} OR TRY_CAST([{col_name}] AS INT) > {upper_b} THEN 'invalid_out_of_range' ELSE 'valid' END"
                            reason_expr = f"CASE WHEN TRY_CAST([{col_name}] AS INT) < {lower_b} OR TRY_CAST([{col_name}] AS INT) > {upper_b} THEN 'Age must be between {lower_b} and {upper_b}' ELSE NULL END"
                        else:
                            lower_b = p.get("lower_bound", p.get("min_value", 0.0))
                            upper_b = p.get("upper_bound", p.get("max_value"))
                            if upper_b is not None:
                                status_expr = f"CASE WHEN TRY_CAST([{col_name}] AS FLOAT) NOT BETWEEN {lower_b} AND {upper_b} THEN 'invalid_out_of_range' ELSE 'valid' END"
                                reason_expr = f"CASE WHEN TRY_CAST([{col_name}] AS FLOAT) NOT BETWEEN {lower_b} AND {upper_b} THEN 'Value out of bounds [{lower_b}, {upper_b}]' ELSE NULL END"
                            else:
                                status_expr = f"CASE WHEN TRY_CAST([{col_name}] AS FLOAT) < {lower_b} THEN 'negative_value_review_required' ELSE 'valid' END"
                                reason_expr = f"CASE WHEN TRY_CAST([{col_name}] AS FLOAT) < {lower_b} THEN 'Catalog price/measure cannot be negative' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif any(a in ("replace_sentinel_values", "normalize_registered_placeholder") for a in actions):
                        status_expr = f"CASE WHEN LOWER(LTRIM(RTRIM(CAST([{col_clean}_raw] AS NVARCHAR(MAX))))) IN ('???', 'n/a', 'na', 'null', 'none', 'unknown', 'dummy', 'temp', 'test', '###', '-', 'unspecified', '0000') OR TRY_CAST([{col_clean}_raw] AS FLOAT) IN (999, -999, 999999, 9999999, -9999, 99999) THEN 'placeholder_normalized_to_null' ELSE 'valid' END"
                        reason_expr = f"CASE WHEN LOWER(LTRIM(RTRIM(CAST([{col_clean}_raw] AS NVARCHAR(MAX))))) IN ('???', 'n/a', 'na', 'null', 'none', 'unknown', 'dummy', 'temp', 'test', '###', '-', 'unspecified', '0000') OR TRY_CAST([{col_clean}_raw] AS FLOAT) IN (999, -999, 999999, 9999999, -9999, 99999) THEN 'Configured placeholder token normalized to NULL' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif any(a in ("parse_dates", "parse_dates_safe", "nullify_date", "nullify_dummy_dates") for a in actions):
                        parsed_dt = f"COALESCE(TRY_CONVERT(date, [{col_clean}_raw], 120), TRY_CONVERT(date, [{col_clean}_raw], 103), TRY_CONVERT(date, [{col_clean}_raw], 101), TRY_CONVERT(date, [{col_clean}_raw], 111))"
                        status_expr = f"CASE WHEN [{col_clean}_raw] IS NULL OR LTRIM(RTRIM(CAST([{col_clean}_raw] AS NVARCHAR(MAX)))) = '' THEN 'missing_value' WHEN {parsed_dt} IN ('1900-01-01', '9999-12-31', '0001-01-01') THEN 'sentinel_value' WHEN {parsed_dt} IS NULL OR {parsed_dt} < '1900-01-01' OR {parsed_dt} > '2099-12-31' THEN 'impossible_calendar_date' ELSE 'valid' END"
                        reason_expr = f"CASE WHEN {parsed_dt} IN ('1900-01-01', '9999-12-31', '0001-01-01') THEN 'Sentinel date replaced with NULL' WHEN {parsed_dt} IS NULL AND [{col_clean}_raw] IS NOT NULL THEN 'Invalid calendar date or unparseable format' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif "standardize_boolean" in actions:
                        inner_raw = f"LOWER(LTRIM(RTRIM(CAST([{col_clean}_raw] AS NVARCHAR(50)))))"
                        status_expr = (
                            f"CASE "
                            f"WHEN [{col_clean}_raw] IS NULL OR {inner_raw} = '' THEN 'missing_value' "
                            f"WHEN {inner_raw} IN ('1', 'true', 'yes', 'y', 't', '0', 'false', 'no', 'n', 'f') THEN 'valid' "
                            f"ELSE 'invalid_boolean_format' END"
                        )
                        reason_expr = (
                            f"CASE "
                            f"WHEN [{col_clean}_raw] IS NOT NULL AND {inner_raw} <> '' AND {inner_raw} NOT IN ('1', 'true', 'yes', 'y', 't', '0', 'false', 'no', 'n', 'f') "
                            f"THEN 'Value ' + CAST([{col_clean}_raw] AS NVARCHAR(50)) + ' cannot be parsed as boolean' "
                            f"ELSE NULL END"
                        )
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif "normalize_phone" in actions:
                        inner_p = f"CAST([{col_clean}_raw] AS NVARCHAR(200))"
                        cln_p = f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({inner_p}, N'-', N''), N' ', N''), N'(', N''), N')', N''), N'+', N''), N'.', N'')"
                        status_expr = f"CASE WHEN [{col_clean}_raw] IS NULL THEN 'missing_value' WHEN LEN({cln_p}) BETWEEN 7 AND 15 AND {cln_p} NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid_phone_format' END"
                        reason_expr = f"CASE WHEN [{col_clean}_raw] IS NOT NULL AND (LEN({cln_p}) NOT BETWEEN 7 AND 15 OR {cln_p} LIKE '%[^0-9]%') THEN 'Phone length must be between 7 and 15 digits' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif "sanitize_email" in actions:
                        cln_e = f"LOWER(LTRIM(RTRIM(CAST([{col_clean}_raw] AS NVARCHAR(MAX)))))"
                        status_expr = f"CASE WHEN [{col_clean}_raw] IS NULL THEN 'missing_value' WHEN {cln_e} LIKE '%_@_%._%' AND {cln_e} NOT LIKE '%@%@%' AND {cln_e} NOT LIKE '%..%' THEN 'valid' ELSE 'invalid_email_format' END"
                        reason_expr = f"CASE WHEN [{col_clean}_raw] IS NOT NULL AND NOT ({cln_e} LIKE '%_@_%._%' AND {cln_e} NOT LIKE '%@%@%' AND {cln_e} NOT LIKE '%..%') THEN 'Invalid email syntax or domain' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif any(a in ("preserve_null_and_flag", "preserve_and_flag", "preserve_null", "preserve_missing_suspected_key", "accept_null_as_optional") for a in actions):
                        status_expr = f"CASE WHEN [{col_name}] IS NULL THEN 'missing_value' ELSE 'valid' END"
                        reason_expr = f"CASE WHEN [{col_name}] IS NULL THEN 'Missing value preserved per policy' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif any(a in ("fill_constant", "fill_null_with_constant") for a in actions):
                        status_expr = f"CASE WHEN [{col_clean}_raw] IS NULL THEN 'filled_constant' ELSE 'valid' END"
                        reason_expr = f"CASE WHEN [{col_clean}_raw] IS NULL THEN 'Missing value filled with constant' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    elif any(a in ("flag_only", "flag_missing_key", "flag_missing_declared_key") for a in actions):
                        status_expr = f"CASE WHEN [{col_name}] IS NULL THEN 'missing_value_flagged' ELSE 'valid' END"
                        reason_expr = f"CASE WHEN [{col_name}] IS NULL THEN 'Missing value flagged for review' ELSE NULL END"
                        update_clauses.append(f"[{col_clean}_validation_status] = {status_expr}")
                        update_clauses.append(f"[{col_clean}_validation_reason] = {_accumulate_reason(col_clean, reason_expr)}")
                    else:
                        update_clauses.append(f"[{col_clean}_validation_status] = COALESCE([{col_clean}_validation_status], 'valid')")
                if has_cast_type:
                    target_t = str(col_meta.get("target_dtype") or "").lower()
                    is_monetary = any(m in col_name.lower() for m in ("salary", "price", "amount", "cost", "fee", "revenue", "balance", "budget", "total"))
                    if any(it in target_t for it in ("int", "bigint", "integer")) and not is_monetary:
                        col_clean = str(col_name).replace("'", "''")
                        update_clauses.append(f"[{col_clean}_int] = TRY_CAST([{col_name}] AS BIGINT)")

            stepped_cols = set()
            for s in steps:
                sc = s.get("column")
                if sc:
                    for sub in str(sc).split(","):
                        sub_clean = sub.strip().strip("[]")
                        if sub_clean:
                            stepped_cols.add(sub_clean)
            for col_name, col_meta in cols_info.items():
                if col_name in stepped_cols or col_name in local_excluded_columns:
                    continue
                dtype = str(col_meta.get("dtype") or col_meta.get("target_dtype") or "").lower()
                if any(x in dtype for x in ("varchar", "nvarchar", "char", "text", "string")):
                    update_clauses.append(f"[{col_name}] = LTRIM(RTRIM(NULLIF([{col_name}], '')))")
                elif any(x in dtype for x in ("int", "float", "decimal", "numeric", "bigint")):
                    cast_type = get_sql_cast_type(dtype, col_name)
                    update_clauses.append(f"[{col_name}] = TRY_CAST([{col_name}] AS {cast_type})")
                
            if update_clauses:
                covered_issues = []
                for col_steps in column_transforms.values():
                    for st in col_steps:
                        iid = str(st.get("issue_id") or st.get("id") or "").strip()
                        if iid and iid not in covered_issues:
                            covered_issues.append(iid)
                for iid in covered_issues:
                    step_lines.append(f"-- COVERS: {iid}")
                step_lines.append(f"-- Single-Pass expression updates on {tbl_staging}")
                update_sql = f"UPDATE {tbl_staging}\nSET " + ",\n    ".join(update_clauses) + "\nWHERE 1=1;"
                step_lines.extend(update_sql.splitlines())
                step_lines.append("")

            phase_trim_lines = list(step_lines)
            step_lines = []

            # Phase 3: Outlier Flags dynamically processed via stored procedure or custom rules
            for st in steps:
                col = st.get("column")
                action = str(st.get("action") or "")
                if not col or col in local_excluded_columns:
                    continue
                col_clean = str(col).replace("'", "''")
                c = _brk(col)
                if action in ("flag_outliers", "clip_or_flag"):
                    col_meta = cols_info.get(col) or {}
                    is_custom_rule = (st.get("source_issue_type") == "custom_rule_violation" or st.get("issue_type") == "custom_rule_violation")
                    if is_custom_rule and not _is_numeric_column(col, col_meta):
                        # Flag using custom assertions that reference this column
                        assertions = [rule for rule in business_rules.get("custom_assertions") or [] if col.lower() in str(rule.get("assertion")).lower()]
                        if assertions:
                            step_lines.append(f"-- Flag custom rule violations for {col}")
                            for rule in assertions:
                                assertion = rule.get("assertion")
                                sql_cond = translate_assertion_to_sql(assertion, ds_name)
                                step_lines.append(f"UPDATE {tbl_staging} SET [{col_clean}_outlier_flagged] = CASE WHEN NOT ({sql_cond}) THEN 1 ELSE 0 END;")
                        else:
                            # If no custom assertion is defined yet, default to flagging NULLs or empty strings
                            step_lines.append(f"-- Flag custom rule violations for {col} (default: flag empty/null)")
                            step_lines.append(f"UPDATE {tbl_staging} SET [{col_clean}_outlier_flagged] = CASE WHEN {c} IS NULL OR LTRIM(RTRIM({c})) = '' THEN 1 ELSE 0 END;")
                    else:
                        step_lines.append(f"-- Flag IQR outliers for {col}")
                        if dialect == "tsql":
                            step_lines.append(f"IF EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = OBJECT_ID('tempdb..{tbl_staging}') AND name = '{col_clean}_outlier_flagged')")
                            step_lines.append("BEGIN")
                            step_lines.append("    WITH OutlierStats AS (")
                            step_lines.append(f"        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST([{col_clean}] AS FLOAT)) OVER () AS q1,")
                            step_lines.append(f"               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST([{col_clean}] AS FLOAT)) OVER () AS q3")
                            step_lines.append(f"        FROM {tbl_staging} WHERE [{col_clean}] IS NOT NULL")
                            step_lines.append("    )")
                            step_lines.append(f"    UPDATE s SET s.[{col_clean}_outlier_flagged] = CASE")
                            step_lines.append(f"        WHEN TRY_CAST(s.[{col_clean}] AS FLOAT) < (o.q1 - 1.5 * (o.q3 - o.q1)) THEN 1")
                            step_lines.append(f"        WHEN TRY_CAST(s.[{col_clean}] AS FLOAT) > (o.q3 + 1.5 * (o.q3 - o.q1)) THEN 1")
                            step_lines.append("        ELSE 0 END")
                            step_lines.append(f"    FROM {tbl_staging} s CROSS JOIN (SELECT TOP 1 q1, q3 FROM OutlierStats) o")
                            step_lines.append(f"    WHERE s.[{col_clean}] IS NOT NULL;")
                            step_lines.append("END;")
                        else:
                            step_lines.append(f"-- ANSI outlier flagging placeholder")

            phase_outlier_lines = list(step_lines)
            step_lines = []

            # Phase 4: Config-driven Default Seed & Single-Pass Join-based updates (defaults + invalid value replacements)
            config_fill_columns = []
            config_invalid_columns = []
        
            for st in steps:
                col = st.get("column")
                if not col or col in local_excluded_columns:
                    continue
                action = st.get("action")
                if action in ("zero_to_null",):
                    config_invalid_columns.append(col)
                elif action in ("fill_or_drop", "fill_nulls_simple"):
                    config_fill_columns.append(st)
                
            config_columns = list(set([st.get("column") for st in config_fill_columns] + config_invalid_columns))
            if config_columns:
                pre_queries = []
                set_clauses = []
                join_clauses = []
                where_clauses = []
                fill_step_map = {st.get("column"): st for st in config_fill_columns}
                zero_to_null_map = {st.get("column"): st for st in steps if st.get("action") == "zero_to_null"}
            
                for col in sorted(config_columns):
                    col_bracket = _brk(col)
                    col_clean_name = str(col)
                    col_meta = cols_info.get(col) or {}
                    col_type = col_meta.get("dtype")
                    col_class = _classify_column(col, col_meta, plan.get("semantic_schema"), ds_name)
                
                    cast_type = get_sql_cast_type(col_type, col_clean_name)
                    cast_func = "TRY_CAST" if dialect == "tsql" else "CAST"
                
                    fval = None
                    strat = None
                    if col in fill_step_map:
                        p = step_params(fill_step_map[col])
                        strat = p.get("fill_strategy")
                        fval = p.get("fill_value")
                    
                    if fval is not None:
                        val = fval
                    else:
                        if col_class in ("date", "id", "categorical"):
                            val = None
                        elif col_class == "metric":
                            val = '0'
                        else:
                            val = None
                        
                    has_invalid = col in config_invalid_columns
                    clean_tbl_base = tbl_clean.split(".")[-1].strip("[]")
                    tbl_key = clean_tbl_base
                    key = f"{tbl_key}.{col_clean_name}"
                    expr = f"c.{col_bracket}"
                
                    if has_invalid:
                        replace_vals = ["0", "-999", "999999", "9999999", "###"]
                        st_invalid = zero_to_null_map.get(col) or fill_step_map.get(col)
                        if st_invalid:
                            replace_vals = step_params(st_invalid).get("replace_values") or replace_vals
                        invalid_values_to_seed[key] = [str(v) for v in replace_vals]
                    
                        alias_iv = f"iv_{col_clean_name.replace('.', '_').replace(' ', '_')[:35]}"
                        # Compare string-to-string for all columns to avoid conversion errors with sentinel values like '###' or 'nan' in staging
                        join_cond = f"CAST(c.{col_bracket} AS NVARCHAR(MAX)) = {alias_iv}.invalid_value"
                        join_clauses.append(
                            f"LEFT JOIN dbo.etl_invalid_values {alias_iv} ON {alias_iv}.column_name = '{key}' "
                            f"AND {join_cond}"
                        )
                        expr = f"CASE WHEN {alias_iv}.invalid_value IS NOT NULL THEN NULL ELSE {expr} END"
                        where_clauses.append(f"{alias_iv}.invalid_value IS NOT NULL")
                    
                    if col in fill_step_map:
                        if strat == "mean" and fval is None and dialect == "tsql":
                            expr = f"COALESCE({expr}, (SELECT AVG(TRY_CAST(c2.{col_bracket} AS FLOAT)) FROM {tbl_staging} c2 WHERE TRY_CAST(c2.{col_bracket} AS FLOAT) IS NOT NULL))"
                            where_clauses.append(f"c.{col_bracket} IS NULL")
                        elif strat == "median" and fval is None and dialect == "tsql":
                            var_name = f"@fill_{col_clean_name.replace('.', '_').replace(' ', '_')[:40]}"
                            pre_queries.append(f"DECLARE {var_name} FLOAT;")
                            pre_queries.append(
                                f"SELECT {var_name} = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TRY_CAST({col_bracket} AS FLOAT)) FROM {tbl_staging} WHERE TRY_CAST({col_bracket} AS FLOAT) IS NOT NULL;"
                            )
                            expr = f"COALESCE({expr}, {var_name})"
                            where_clauses.append(f"c.{col_bracket} IS NULL")
                        else:
                            if val is not None:
                                default_values_to_seed[key] = {"default_value": val, "data_type": cast_type}
                                alias_dv = f"dv_{col_clean_name.replace('.', '_').replace(' ', '_')[:35]}"
                                join_clauses.append(
                                    f"LEFT JOIN dbo.etl_default_values {alias_dv} ON {alias_dv}.column_name = '{key}'"
                                )
                                expr = f"COALESCE({expr}, {cast_func}({alias_dv}.default_value AS {cast_type}))"
                                where_clauses.append(f"c.{col_bracket} IS NULL")
                            
                    if expr != f"c.{col_bracket}":
                        set_clauses.append(f"c.{col_bracket} = {expr}")
                
                if set_clauses:
                    step_lines.append(f"-- Grouped config and null updates on {tbl_staging}")
                    step_lines.extend(pre_queries)
                    update_sql = f"UPDATE c\nSET " + ",\n    ".join(set_clauses)
                    update_sql += f"\nFROM {tbl_staging} c"
                    if join_clauses:
                        unique_joins = []
                        for j in join_clauses:
                            if j not in unique_joins:
                                unique_joins.append(j)
                        update_sql += "\n" + "\n".join(unique_joins)
                    unique_where = []
                    for w in where_clauses:
                        if w not in unique_where:
                            unique_where.append(w)
                    if unique_where:
                        update_sql += "\nWHERE " + " OR ".join(unique_where) + ";"
                    else:
                        update_sql += ";"
                    step_lines.extend(update_sql.splitlines())
                    step_lines.append("")

            phase_sentinel_lines = list(step_lines)
            step_lines = []

            # Phase 5: Final Copy from Staging to target Clean table
            if dialect == "tsql":
                clean_cols = list(cols)
                for st in steps:
                    c = st.get("column")
                    if not c or c in excluded_columns or c in ("*", "[Row-level]", "[row-level]"):
                        continue
                    action = str(st.get("action") or "").lower()
                    if action in ("deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate", "flag_near_duplicate_cluster", "validate_referential_integrity_or_stage"):
                        continue
                    sub_cols = [col_item.strip().strip("[]") for col_item in str(c).split(",") if col_item.strip() and col_item.strip() not in ("*", "[Row-level]", "[row-level]")]
                    for single_c in sub_cols:
                        if single_c in excluded_columns:
                            continue
                        cc = str(single_c).replace("'", "''")
                        for comp in [f"{cc}_raw", f"{cc}_clean", f"{cc}_validation_status", f"{cc}_validation_reason"]:
                            if comp not in clean_cols:
                                clean_cols.append(comp)
                        if action in ("flag_outliers", "clip_or_flag"):
                            if f"{cc}_outlier_flagged" not in clean_cols:
                                clean_cols.append(f"{cc}_outlier_flagged")
                        if action == "cast_type":
                            target_t = str(st.get("params", {}).get("target_type") or st.get("params", {}).get("dtype") or cols_info.get(single_c, {}).get("target_dtype") or "").lower()
                            is_monetary = any(m in single_c.lower() for m in ("salary", "price", "amount", "cost", "fee", "revenue", "balance", "budget", "total"))
                            if any(it in target_t for it in ("int", "bigint", "integer")) and not is_monetary:
                                if f"{cc}_int" not in clean_cols:
                                    clean_cols.append(f"{cc}_int")
                for comp in ["row_validation_status", "row_validation_reason", "is_duplicate_row"]:
                    if comp not in clean_cols:
                        clean_cols.append(comp)

                col_list_clean = ", ".join(f"[{c}]" for c in clean_cols)

                # Build typed SELECT expressions for safe insert from NVARCHAR(MAX) staging
                select_parts = []
                for c in clean_cols:
                    # Derived columns and companion columns are already typed in staging — no cast needed
                    if (
                        c.endswith("_outlier_flagged")
                        or c.endswith("_int")
                        or c.endswith("_raw")
                        or c.endswith("_clean")
                        or c.endswith("_validation_status")
                        or c.endswith("_validation_reason")
                        or c in ("row_validation_status", "row_validation_reason", "is_duplicate_row")
                    ):
                        select_parts.append(f"[{c}]")
                        continue
                    col_meta = cols_info.get(c) or {}
                    col_type = col_meta.get("dtype") or col_meta.get("target_dtype") or ""
                    target_type = get_sql_cast_type(col_type, c)
                    if target_type == "NVARCHAR(MAX)":
                        select_parts.append(f"[{c}]")
                    else:
                        select_parts.append(f"TRY_CAST([{c}] AS {target_type})")
                select_list_typed = ", ".join(select_parts)

                if is_clean and pk_col and dialect == "tsql":
                    step_lines.append(f"-- Mandatory Gate: Ensure primary key [{pk_col}] is NOT NULL before target clean table insertion")
                    step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                    step_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                    step_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                    step_lines.append(f"       'Primary key [{pk_col}] cannot be NULL on clean table insert'")
                    step_lines.append(f"FROM {tbl_staging} r")
                    step_lines.append(f"WHERE r.[{pk_col}] IS NULL;")
                    step_lines.append(f"DELETE FROM {tbl_staging} WHERE [{pk_col}] IS NULL;")
                    step_lines.append("")

                if scd_type == "type2":
                    step_lines.append("-- Copy fully transformed data from Staging to target Clean table (SCD Type 2)")
                    step_lines.append("IF @load_type = 'FULL' OR @last_run IS NULL")
                    step_lines.append("BEGIN")
                    step_lines.append(f"    TRUNCATE TABLE {clean_tbl};")
                    step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, start_date, end_date, is_current, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"    SELECT {select_list_typed}, GETDATE(), '9999-12-31', 1, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                    step_lines.append("END")
                    step_lines.append("ELSE")
                    step_lines.append("BEGIN")
                    step_lines.append(f"    -- Expire existing active records")
                    step_lines.append(f"    UPDATE target")
                    step_lines.append(f"    SET target.end_date = GETDATE(), target.is_current = 0")
                    step_lines.append(f"    FROM {clean_tbl} target")
                    step_lines.append(f"    INNER JOIN {tbl_staging} staging ON target.[{pk_col}] = staging.[{pk_col}]")
                    step_lines.append(f"    WHERE target.is_current = 1;")
                    step_lines.append(f"")
                    step_lines.append(f"    -- Insert new active records")
                    step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, start_date, end_date, is_current, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"    SELECT {select_list_typed}, GETDATE(), '9999-12-31', 1, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                    step_lines.append("END;")
                elif scd_type == "append":
                    step_lines.append("-- Copy fully transformed data from Staging to target Clean table (Append-only)")
                    step_lines.append(f"INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"SELECT {select_list_typed}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                elif scd_type == "truncate":
                    step_lines.append("-- Copy fully transformed data from Staging to target Clean table (Truncate and Load)")
                    step_lines.append(f"TRUNCATE TABLE {clean_tbl};")
                    step_lines.append(f"INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"SELECT {select_list_typed}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                else:  # type1
                    step_lines.append("-- Copy fully transformed data from Staging to target Clean table (SCD Type 1)")
                    step_lines.append("IF @load_type = 'FULL' OR @last_run IS NULL")
                    step_lines.append("BEGIN")
                    step_lines.append(f"    TRUNCATE TABLE {clean_tbl};")
                    step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"    SELECT {select_list_typed}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                    step_lines.append("END")
                    step_lines.append("ELSE")
                    step_lines.append("BEGIN")
                    if pk_col:
                        step_lines.append(f"    DELETE FROM {clean_tbl} WHERE [{pk_col}] IN (SELECT [{pk_col}] FROM {tbl_staging});")
                    step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
                    step_lines.append(f"    SELECT {select_list_typed}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
                    step_lines.append("END;")
                
                step_lines.append("")
                # S3-01: Post-load Row Count Verification
                step_lines.append(f"DECLARE @staging_rows INT = (SELECT COUNT(*) FROM {tbl_staging});")
                step_lines.append(f"DECLARE @target_rows INT = (SELECT COUNT(*) FROM {clean_tbl});")
                step_lines.append("IF @staging_rows > 0 AND @target_rows = 0")
                step_lines.append("BEGIN")
                step_lines.append("    RAISERROR('Post-load assertion failed: Target table is empty but staging had rows.', 16, 1);")
                step_lines.append("END;")
                step_lines.append("")
            
                # Compute max watermark date from staging before cleanup
                if watermark_col:
                    step_lines.append(f"DECLARE @max_watermark DATETIME = COALESCE((SELECT MAX(TRY_CAST([{watermark_col}] AS DATETIME)) FROM {tbl_staging}), @last_run, GETDATE());")
                    step_lines.append("")
                # Clean up the staging table
                step_lines.append(f"IF OBJECT_ID('tempdb..{tbl_staging}') IS NOT NULL DROP TABLE {tbl_staging};")
                step_lines.append("")

            phase_copy_lines = list(step_lines)
            step_lines = []

            # Deduplication Phase: CTE-based dedup on staging after all validations
            dedup_lines = []
            if dialect == "tsql":
                dedup_steps = [
                    st for st in steps 
                    if str(st.get("action") or "").lower() in ("deduplicate", "flag_duplicate_candidates", "flag_business_key_duplicate", "flag_near_duplicate_cluster")
                ]
                processed_dedup_keys = set()
                for st in dedup_steps:
                    col_val = str(st.get("column") or "").strip()
                    is_row_level = col_val.lower() in ("row-level", "[row-level]", "", "*", "[*]")
                    is_pk_dedup = (st.get("source_issue_type") == "duplicate_primary_key" or st.get("issue_type") == "duplicate_primary_key")
                    
                    if is_row_level:
                        if is_pk_dedup and pk_col:
                            partition_cols = [pk_col]
                        else:
                            exclude_cols = {pk_col} if pk_col else set()
                            if watermark_col:
                                exclude_cols.add(watermark_col)
                            partition_cols = [
                                c for c in cols
                                if c not in exclude_cols and c.lower() not in (
                                    "etl_batch_id", "run_id", "etl_created_at", "etl_updated_at",
                                    "_rn", "_dedup_rn", "etl_run_id", "etl_created_date", "etl_updated_date"
                                ) and not c.lower().startswith("etl_")
                            ]
                            if not partition_cols:
                                if pk_col:
                                    partition_cols = [pk_col]
                                elif cols:
                                    partition_cols = [c for c in cols if not c.lower().startswith("etl_")]
                                else:
                                    continue
                    else:
                        raw_cols = [c.strip().strip("[]") for c in col_val.split(",") if c.strip()]
                        if "*" in raw_cols:
                            exclude_cols = {pk_col} if pk_col else set()
                            if watermark_col:
                                exclude_cols.add(watermark_col)
                            partition_cols = [
                                c for c in cols
                                if c not in exclude_cols and c.lower() not in (
                                    "etl_batch_id", "run_id", "etl_created_at", "etl_updated_at",
                                    "_rn", "_dedup_rn", "etl_run_id", "etl_created_date", "etl_updated_date"
                                ) and not c.lower().startswith("etl_")
                            ]
                            if not partition_cols:
                                if pk_col:
                                    partition_cols = [pk_col]
                                elif cols:
                                    partition_cols = [c for c in cols if not c.lower().startswith("etl_")]
                                else:
                                    continue
                        else:
                            partition_cols = raw_cols
                            
                    dedup_key = tuple(sorted(partition_cols))
                    if dedup_key in processed_dedup_keys:
                        continue
                    processed_dedup_keys.add(dedup_key)
                    
                    partition_exprs = [
                        f"LOWER(LTRIM(RTRIM(CAST([{c}] AS NVARCHAR(400)))))"
                        for c in partition_cols
                    ]
                    partition_clause = ", ".join(partition_exprs)
                    order_parts = []
                    if watermark_col:
                        order_parts.append(f"[{watermark_col}] DESC")
                    if pk_col and pk_col not in partition_cols:
                        order_parts.append(f"[{pk_col}] ASC")
                    elif pk_col and not watermark_col:
                        order_parts.append(f"[{pk_col}] ASC")
                    
                    # Deterministic tie-breaker fallback (avoids non-deterministic ORDER BY (SELECT NULL))
                    if not order_parts:
                        candidate_tie_breakers = [c for c in cols if c not in partition_cols and not c.lower().startswith("etl_")]
                        if candidate_tie_breakers:
                            order_parts.append(f"[{candidate_tie_breakers[0]}] ASC")
                        elif pk_col:
                            order_parts.append(f"[{pk_col}] ASC")
                        elif partition_cols:
                            order_parts.append(f"[{partition_cols[0]}] ASC")
                        else:
                            order_parts.append("etl_batch_id ASC")
                            
                    order_clause = ", ".join(order_parts)
                        
                    label = "primary key" if is_row_level and is_pk_dedup else "near-duplicate attributes"
                    st_act = str(st.get("action") or "").lower()
                    is_flagging_only = never_drop or st_act.startswith("flag_")

                    dedup_lines.append(f"-- Log duplicate records from {tbl_staging} to dbo.etl_rejects")
                    dedup_lines.append(f";WITH _staging_dup_log AS (")
                    dedup_lines.append(f"    SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {order_clause}) AS _rn")
                    dedup_lines.append(f"    FROM {tbl_staging}")
                    dedup_lines.append(f")")
                    dedup_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                    dedup_lines.append(f"SELECT '{sp_name}', '{tbl_clean}',")
                    dedup_lines.append(f"       (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                    dedup_lines.append(f"       'Duplicate candidate detected by {label}'")
                    dedup_lines.append(f"FROM _staging_dup_log r")
                    dedup_lines.append(f"WHERE r._rn > 1;")
                    dedup_lines.append(f"")

                    if is_flagging_only:
                        dedup_lines.append(f"-- Flag duplicate records in staging table by {label}")
                        dedup_lines.append(f";WITH _staging_dedup AS (")
                        dedup_lines.append(f"    SELECT is_duplicate_row, row_validation_status, row_validation_reason,")
                        dedup_lines.append(f"           ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {order_clause}) AS _rn")
                        dedup_lines.append(f"    FROM {tbl_staging}")
                        dedup_lines.append(f")")
                        dedup_lines.append(f"UPDATE _staging_dedup")
                        dedup_lines.append(f"SET is_duplicate_row = 1,")
                        dedup_lines.append(f"    row_validation_status = COALESCE(row_validation_status, 'DUPLICATE_CANDIDATE'),")
                        dedup_lines.append(f"    row_validation_reason = CASE WHEN row_validation_reason IS NULL THEN 'Duplicate record detected by ' + '{label}' ELSE row_validation_reason + '; duplicate candidate' END")
                        dedup_lines.append(f"WHERE _rn > 1;")
                        dedup_lines.append(f"")
                    else:
                        dedup_lines.append(f"-- Deduplicate staging table by {label}")
                        dedup_lines.append(f";WITH _staging_dedup AS (")
                        dedup_lines.append(f"    SELECT ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {order_clause}) AS _rn")
                        dedup_lines.append(f"    FROM {tbl_staging}")
                        dedup_lines.append(f")")
                        dedup_lines.append(f"DELETE FROM _staging_dedup WHERE _rn > 1;")
                        dedup_lines.append(f"")

            # Combine all phases in correct execution order:
            # 1. Trims & Casings
            # 2. Placeholder Normalization to NULL
            # 3. Apply Defaults (Grouped config and null updates)
            # 4. Format & Null Validations (and Quarantines)
            # 5. Post-validation Date/Type Parsing
            # 6. Outlier Flags
            # 7. Deduplication
            # 8. Final Copy to target clean table
            step_lines = (
                phase_trim_lines +
                phase_placeholder_norm_lines +
                phase_sentinel_lines +
                phase_validate_lines +
                phase_parse_lines +
                phase_outlier_lines +
                dedup_lines +
                phase_copy_lines
            )

            if dialect == "tsql":
                proc_lines.extend(step_lines)
                proc_lines.append("")
                proc_lines.append("-- Update process watermark")
                proc_lines.append("IF @load_type = 'INCREMENTAL' OR @last_run IS NULL")
                proc_lines.append("BEGIN")
                proc_lines.append("    MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target")
                proc_lines.append(f"    USING (SELECT '{sp_name}' AS process_name) AS source")
                proc_lines.append("    ON target.process_name = source.process_name")
                wm_expr = "@max_watermark" if watermark_col else "GETDATE()"
                proc_lines.append("    WHEN MATCHED THEN")
                proc_lines.append(f"        UPDATE SET last_run_time = {wm_expr}")
                proc_lines.append("    WHEN NOT MATCHED THEN")
                proc_lines.append(f"        INSERT (process_name, last_run_time) VALUES (source.process_name, {wm_expr});")
                proc_lines.append("END")
            
                # Indent and append child procedure body
                lines.extend(_indent(proc_lines, spaces=8))
                lines.append("")
                lines.append("        -- Log success atomically within transaction before commit")
                lines.append("        UPDATE dbo.etl_log")
                lines.append("        SET end_time = GETDATE(), status = 'SUCCEEDED'")
                where_id_str = "        WHERE id = @run_id;"
                lines.append(where_id_str)
                lines.append("        COMMIT TRANSACTION;")
                lines.append("    END TRY")
                lines.append("    BEGIN CATCH")
                lines.append("        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;")
                if is_odbc_or_azure:
                    lines.append("        DECLARE @err_msg NVARCHAR(4000);")
                    lines.append("        SET @err_msg = ERROR_MESSAGE();")
                    lines.append("")
                    lines.append("        UPDATE dbo.etl_log")
                    lines.append("        SET status = 'FAILED',")
                    lines.append("            error_message = @err_msg")
                    lines.append(where_id_str)
                    lines.append("")
                    lines.append("        RAISERROR(@err_msg, 16, 1);")
                else:
                    lines.append("        DECLARE @err_msg NVARCHAR(4000) = ERROR_MESSAGE();")
                    lines.append("        DECLARE @err_num INT = ERROR_NUMBER();")
                    lines.append("        BEGIN TRY")
                    lines.append("            UPDATE dbo.etl_log")
                    lines.append("            SET end_time = GETDATE(), status = 'FAILED', error_message = @err_msg")
                    lines.append(where_id_str)
                    lines.append("        END TRY")
                    lines.append("        BEGIN CATCH")
                    lines.append("            -- Secondary telemetry exception ignored so original THROW is preserved")
                    lines.append("        END CATCH;")
                    lines.append("        THROW;")
                lines.append("    END CATCH;")
                lines.append("END;")
                lines.append("GO")
                lines.append("")
            else:
                lines.extend(step_lines)
                lines.append("")

    # Generate master orchestrator procedure for T-SQL
    if dialect == "tsql" and "etl_main" not in emitted_objects:
        emitted_objects.add("etl_main")
        emitted_objects.add("dbo.etl_main")
        lines.append("-- ============================================================")
        lines.append("-- Master Orchestrator Stored Procedure")
        lines.append("-- ============================================================")
        lines.append("CREATE OR ALTER PROCEDURE dbo.etl_main")
        lines.append("    @load_type VARCHAR(20) = 'FULL',")
        lines.append("    @last_run DATETIME = NULL")
        lines.append("AS BEGIN")
        lines.append("    SET NOCOUNT ON;")
        lines.append("    -- Retrieve last run watermark if not provided")
        lines.append("    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL")
        lines.append("    BEGIN")
        lines.append("        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';")
        lines.append("    END;")
        lines.append("")
        lines.append("    INSERT INTO dbo.etl_log (process_name, start_time, status)")
        lines.append("    VALUES ('etl_main', GETDATE(), 'RUNNING');")
        lines.append("    DECLARE @run_id INT = SCOPE_IDENTITY();")
        lines.append("")
        lines.append("    BEGIN TRY")
        
        # Execute procedures based on generation_mode
        for ds_name in ds_plan.keys():
            tbl_base = ds_name.split('.')[-1].strip('[]')
            if generation_mode in ('cleanse', 'cleanse_only', 'phase1', 'full'):
                lines.append(f'        EXEC dbo.etl_clean_{tbl_base} @load_type = @load_type, @last_run = @last_run;')
            
            if generation_mode in ('transform', 'transform_only', 'phase2', 'full'):
                from agent.etl_pipeline.dq_gate import check_dq_gate
                threshold = float(business_rules.get('dq_threshold', 70.0))
                gate_res = check_dq_gate(assessment, ds_name, threshold=threshold, force_unlock=ds_name in business_rules.get("force_unlock", []), sem_schema=plan.get("semantic_schema"))
                if generation_mode == 'transform_only' or gate_res['passed']:
                    lines.append(f'        EXEC dbo.etl_transform_{tbl_base} @load_type = @load_type, @last_run = @last_run;')
                else:
                    lines.append(f"        -- ⚠ Phase 2 transform blocked: dataset '{ds_name}' did not pass the DQ gate (Score: {gate_res['score']} < {threshold})")

        lines.append("        -- Update master process watermark")
        lines.append("        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL")
        lines.append("        BEGIN")
        lines.append("            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target")
        lines.append("            USING (SELECT 'etl_main' AS process_name) AS source")
        lines.append("            ON target.process_name = source.process_name")
        master_wm = "COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE())"
        lines.append("            WHEN MATCHED THEN")
        lines.append(f"                UPDATE SET last_run_time = {master_wm}")
        lines.append("            WHEN NOT MATCHED THEN")
        lines.append(f"                INSERT (process_name, last_run_time) VALUES (source.process_name, {master_wm});")
        lines.append("        END")
        lines.append("")
        lines.append("        UPDATE dbo.etl_log")
        lines.append("        SET end_time = GETDATE(), status = 'SUCCESS'")
        lines.append("        WHERE id = @run_id;")
        lines.append("    END TRY")
        lines.append("    BEGIN CATCH")
        lines.append("        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;")
        if is_odbc_or_azure:
            lines.append("        DECLARE @err_msg NVARCHAR(4000);")
            lines.append("        SET @err_msg = ERROR_MESSAGE();")
            lines.append("")
            lines.append("        UPDATE dbo.etl_log")
            lines.append("        SET status = 'FAILED',")
            lines.append("            error_message = @err_msg")
            lines.append("        WHERE id = @run_id;")
            lines.append("")
            lines.append("        RAISERROR(@err_msg, 16, 1);")
        else:
            lines.append("        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();")
            lines.append("        UPDATE dbo.etl_log")
            lines.append("        SET end_time = GETDATE(), status = 'FAILED', error_message = @err")
            lines.append("        WHERE id = @run_id;")
            lines.append("        THROW;")
        lines.append("    END CATCH;")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        # -------------------------------------------------------
        # Auto-execute: Call the orchestrator so data actually
        # flows from Raw → Staging → Clean tables.
        # Without this, the script only defines procedures but
        # never runs them, leaving Customers_Clean with 0 rows.
        # -------------------------------------------------------
        if "exec_dbo.etl_main" not in emitted_objects:
            emitted_objects.add("exec_dbo.etl_main")
            lines.append("-- ============================================================")
            lines.append("-- Auto-Execute: Run ETL pipeline to populate Clean tables")
            lines.append("-- ============================================================")
            lines.append("PRINT 'Starting ETL pipeline execution...';")
            lines.append("EXEC dbo.etl_main @load_type = 'FULL';")
            lines.append("PRINT 'ETL pipeline execution complete.';")
            lines.append("GO")
            lines.append("")

    for st in plan.get("global_steps") or []:
        lines.append(f"-- global: {st.get('action')} {st.get('column') or ''}")

    manifest = plan.get("connector_manifest") or {}
    rel = plan.get("relationships") or {}
    # Views and join queries are Phase 2 (transform) artifacts — skip in cleanse_only mode
    if generation_mode != "cleanse_only" and (rel.get("joins") or rel.get("many_to_many") or manifest.get("datasets")):
        lines.append("")
        lines.append("-- ============================================================")
        lines.append("-- Phase 2: Joined Views over Clean Tables")
        lines.append("-- ============================================================")
        # Call emit_sql_joins and map output lines from Raw to Clean for views
        join_lines = emit_sql_joins(plan, manifest, assessment=assessment, dialect=dialect)
        if dialect == "tsql":
            clean_join_lines = []
            for line in join_lines:
                new_line = line
                for ds_name in ds_plan.keys():
                    tbl_raw = tsql_qualified_name(ds_name)
                    tbl_cl = tsql_qualified_name(_get_clean_table_name(ds_name, target_schema=clean_schema, timestamp_suffix=timestamp_suffix))
                    new_line = new_line.replace(tbl_raw, tbl_cl)
                    
                    raw_base = ds_name.split(".")[-1].strip("[]")
                    cl_base = _get_clean_table_name(ds_name, target_schema=clean_schema, timestamp_suffix=timestamp_suffix).split(".")[-1].strip("[]")
                    new_line = new_line.replace(f"[{raw_base}]", f"[{cl_base}]")
                    # Prevent double-replacements by only matching raw_base as a full word boundary
                    new_line = re.sub(rf"\b{re.escape(raw_base)}\b", cl_base, new_line)
                clean_join_lines.append(new_line)
            join_lines = clean_join_lines
            
        lines.extend(join_lines)
    elif generation_mode == "cleanse_only" and (rel.get("joins") or rel.get("many_to_many")):
        lines.append("")
        lines.append("-- NOTE: Cross-dataset views omitted (cleanse_only mode).")
        lines.append("-- Re-generate in 'full' mode to emit Phase 2 join views.")

    # Generate seed lines for default_values and invalid_values
    seed_lines = []
    if dialect == "tsql":
        if default_values_to_seed:
            seed_lines.append("-- Seed ETL default configuration values")
            for key, info in default_values_to_seed.items():
                val_escaped = str(info["default_value"]).replace("'", "''")
                dt_escaped = str(info["data_type"]).replace("'", "''")
                seed_lines.append(f"IF NOT EXISTS (SELECT 1 FROM dbo.etl_default_values WHERE column_name = '{key}')")
                seed_lines.append(f"    INSERT INTO dbo.etl_default_values (column_name, default_value, data_type) VALUES ('{key}', N'{val_escaped}', '{dt_escaped}');")
        if invalid_values_to_seed:
            seed_lines.append("-- Seed ETL invalid/sentinel configuration values")
            for key, vals in invalid_values_to_seed.items():
                for val in vals:
                    val_escaped = str(val).replace("'", "''")
                    seed_lines.append(f"IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = '{key}' AND invalid_value = '{val_escaped}')")
                    seed_lines.append(f"    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('{key}', N'{val_escaped}');")
        if seed_lines:
            seed_lines.append("GO")
        seed_sql = "\n".join(seed_lines)
    else:
        seed_sql = ""
        
    full_sql = "\n".join(lines)
    if dialect == "tsql" and default_values_to_seed:
        ddl_lines = [
            "IF OBJECT_ID('dbo.etl_default_values', 'U') IS NULL",
            "BEGIN",
            "    CREATE TABLE dbo.etl_default_values (",
            "        column_name VARCHAR(256) PRIMARY KEY,",
            "        default_value VARCHAR(256) NOT NULL,",
            "        data_type VARCHAR(50) NOT NULL",
            "    );",
            "END;",
            "GO",
            ""
        ]
        ddl_sql = "\n".join(ddl_lines)
    else:
        ddl_sql = ""
    full_sql = full_sql.replace("-- __DEFAULT_VALUES_DDL_PLACEHOLDER__", ddl_sql)
    full_sql = full_sql.replace("-- __DEFAULT_VALUES_SEED_PLACEHOLDER__", seed_sql)
    
    # S3-05: SQL Quality assessment badge
    from agent.etl_pipeline.validate_sql import validate_sql_basic_dict, validate_sql_assembly
    val_res = validate_sql_basic_dict(full_sql)
    issues = list(val_res.get("issues") or [])
    warnings = list(val_res.get("warnings") or [])
    asm_ok, asm_issues = validate_sql_assembly(full_sql, execution_target=execution_target)
    issues.extend(asm_issues)

    # Check for hard syntax smells
    clean_code = re.sub(r"'(?:''|[^'])*'", "''", full_sql)
    clean_code = re.sub(r"--[^\n]*", "", clean_code)
    if "[*]" in clean_code or "CAST([*]" in clean_code:
        issues.append("Fatal syntax: Wildcard [*] used as column identifier in query or window clause")
    if re.search(r"\[\s*[^\]\n']+,[^\]\n']+\]", clean_code):
        issues.append("Fatal syntax: Bracketed identifier contains comma (composite column leak in DDL/DML)")
    proc_blocks = re.findall(r"CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE[\s\S]*?END\s*;", full_sql, re.IGNORECASE)
    for pb in proc_blocks:
        if re.search(r"\bALTER\s+TABLE\b", pb, re.IGNORECASE):
            warnings.append("Performance/Locking: ALTER TABLE detected inside stored procedure body")
            break

    score = max(0, 100 - len(issues) * 20 - len(warnings) * 4)
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    else:
        grade = "F"
        
    badge_lines = [
        "-- ============================================================",
        f"-- SQL QUALITY ASSESSMENT BADGE",
        f"-- Score: {score}/100",
        f"-- Grade: {grade}",
    ]
    if issues or warnings:
        all_reported = [f"[BLOCKER] {iss}" for iss in issues] + [f"[ADVISORY] {w}" for w in warnings]
        badge_lines.append(f"-- Issues Detected ({len(all_reported)}):")
        for iss in all_reported:
            badge_lines.append(f"--   - {iss}")
    else:
        badge_lines.append("-- No static code-generation issues detected.")
        badge_lines.append("-- Generated SQL still requires compilation and integration validation.")
    badge_lines.extend([
        "-- ============================================================",
        ""
    ])
    
    full_sql = "\n".join(badge_lines) + full_sql
    full_sql = re.sub(r"#([a-zA-Z0-9_]+?)_Staging_Staging\b", r"#\1_Staging", full_sql, flags=re.IGNORECASE)
    full_sql = re.sub(r"#([a-zA-Z0-9_]+?)_staging_staging\b", r"#\1_Staging", full_sql, flags=re.IGNORECASE)
    final_output = full_sql.strip() + "\n"
    _check_forbidden_generator_tokens(final_output)
    return final_output
