from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from agent.etl_pipeline.codegen_shared import step_params
from agent.etl_pipeline.join_emitters import emit_python_load_and_join
from agent.etl_pipeline.io_snippets import python_read_snippet, resolve_path_python_helper


def _safe_ident(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    if s and s[0].isdigit():
        s = "ds_" + s
    return s or "dataset"


def _col_expr(col: str) -> str:
    return repr(str(col))


_DESTRUCTIVE_ACTIONS = frozenset(
    {
        "exclude_column",
        "drop_column",
        "deduplicate",
        "cast_type",
        "clip_outliers",
        "cap_outliers",
        "range_clip",
        "clip_or_flag",
        "flag_outliers",
    }
)


def _log_destructive(ds_var: str, ds_name: str, action: str, col: Optional[str]) -> List[str]:
    if action not in _DESTRUCTIVE_ACTIONS:
        return []
    c = _col_expr(col) if col else "None"
    lines = [
        f"_rows_before = len({ds_var})",
        f"logging.info('Applying {action} on {col} in {ds_name} (rows before={{_rows_before}})')",
    ]
    return lines


def _log_destructive_after(ds_var: str, ds_name: str, action: str) -> List[str]:
    if action not in _DESTRUCTIVE_ACTIONS:
        return []
    return [
        f"logging.info('{action} on {ds_name}: rows after=%s', len({ds_var}))",
    ]


def _emit_fill(action: str, col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    strategy = params.get("fill_strategy")
    fill_val = params.get("fill_value")
    lines: List[str] = []
    if strategy == "median":
        if fill_val is not None:
            lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna({fill_val})")
        else:
            lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna({ds_var}[{c}].median())")
    elif strategy == "mean":
        if fill_val is not None:
            lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna({fill_val})")
        else:
            lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna({ds_var}[{c}].mean())")
    elif strategy == "value" and fill_val is not None:
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna({repr(fill_val)})")
    elif strategy == "value":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna('')")
    else:
        lines.append(f"logging.warning('No fill_strategy for {col}; using pd.NA')")
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna(pd.NA)")
    return lines


def _emit_outliers(action: str, col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    flag_col = _col_expr(f"{col}_outlier_flagged")
    mult = params.get("outlier_iqr_multiplier") or 1.5
    method = params.get("outlier_method") or (
        "clip" if action == "clip_outliers" else "cap" if action == "cap_outliers" else "flag"
    )
    lines = [
        f"_q1 = {ds_var}[{c}].quantile(0.25)",
        f"_q3 = {ds_var}[{c}].quantile(0.75)",
        f"_iqr = _q3 - _q1",
        f"_lower = _q1 - {mult} * _iqr",
        f"_upper = _q3 + {mult} * _iqr",
    ]
    if method == "clip":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].clip(lower=_lower, upper=_upper)")
    elif method == "cap":
        med = params.get("fill_value")
        if med is not None:
            lines.append(f"_median = {med}")
        else:
            lines.append(f"_median = {ds_var}[{c}].median()")
        lines.append(f"_mask = ({ds_var}[{c}] < _lower) | ({ds_var}[{c}] > _upper)")
        lines.append(f"{ds_var}.loc[_mask, {c}] = _median")
    else:
        lines.append(
            f"{ds_var}[{flag_col}] = ((({ds_var}[{c}] < _lower) | ({ds_var}[{c}] > _upper)) & {ds_var}[{c}].notna()).astype(bool)"
        )
    return lines


NON_NUMERIC_COLS = {
    "country", "country_code", "customer_id", "order_id", "payment_id",
    "client_id", "user_id", "status", "date", "phone", "email"
}


def _emit_trim(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    if _p.get("source_issue_type") == "internal_whitespace" or _p.get("issue_type") == "internal_whitespace":
        return [f"{ds_var}[{c}] = {ds_var}[{c}].astype('string').str.strip().str.replace(r'\\s+', ' ', regex=True)"]
    return [f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.strip()"]


def _emit_fill_step(col: str, ds_var: str, p: Dict[str, Any]) -> List[str]:
    return _emit_fill("fill_nulls_simple", col, ds_var, p)


def _emit_coerce_numeric(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c_lower = str(col).lower().strip()
    if c_lower in NON_NUMERIC_COLS or any(c_lower.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        return [
            f"# Step: coerce_numeric on '{col}' (non-numeric column -- keeping as text string to prevent data loss)",
            f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype('string').str.strip()"
        ]
    return [f"{ds_var}[{_col_expr(col)}] = pd.to_numeric({ds_var}[{_col_expr(col)}], errors='coerce')"]



def _emit_cast_type(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype('Int64')"]


def _emit_parse_dates(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = pd.to_datetime({ds_var}[{_col_expr(col)}], errors='coerce')"]


def _emit_parse_dates_safe(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    safe = _safe_ident(col)
    parsed = f"_parsed_{safe}"
    flag_col = _col_expr(f"is_invalid_{col}")
    return [
        f"# Safe date parsing with audit flag for {col}",
        f"{parsed} = pd.to_datetime({ds_var}[{c}], errors='coerce')",
        f"{ds_var}[{flag_col}] = {ds_var}[{c}].notna() & {parsed}.isna()",
        f"{ds_var}[{c}] = {parsed}",
    ]


def _emit_fill_nulls_flag(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    safe = _safe_ident(col)
    parsed = f"_parsed_{safe}"
    flag_col = _col_expr(f"is_invalid_{col}")
    clean_col = _col_expr(f"{col}_numeric")
    return [
        f"# Preserve original values and flag invalid numeric entries for {col}",
        f"{parsed} = pd.to_numeric({ds_var}[{c}], errors='coerce')",
        f"{ds_var}[{flag_col}] = {ds_var}[{c}].notna() & {parsed}.isna()",
        f"{ds_var}[{clean_col}] = {parsed}",
    ]


def _emit_flag_domain_violation(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    flag_col = _col_expr(f"{col}_domain_flag")
    return [
        f"# Domain violation: digit-only values in categorical column {col}",
        f"if {c} in {ds_var}.columns:",
        f"    {ds_var}[{flag_col}] = {ds_var}[{c}].astype(str).str.fullmatch(r'\\d+')",
        f"    {ds_var}[{flag_col}] = {ds_var}[{flag_col}].fillna(False).astype(bool)",
    ]


def _emit_sanitize_email(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.strip().str.lower()",
        f"_mask = ~{ds_var}[{c}].str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$', na=False)",
        f"{ds_var}.loc[_mask, {c}] = pd.NA",
    ]


def _emit_replace_sentinel_values(col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    svals = params.get("sentinel_values") or [-999.0, 999.0, 9999.0, 99999.0, 999999.0, -9999.0]
    all_replace = []
    for val in svals:
        all_replace.append(val)
        if isinstance(val, (int, float)):
            if val == int(val):
                all_replace.append(int(val))
                all_replace.append(str(int(val)))
            all_replace.append(str(val))
    all_replace = list(set(all_replace))
    return [
        f"# Nullify sentinel values",
        f"{ds_var}[{c}] = {ds_var}[{c}].replace({repr(all_replace)}, pd.NA)"
    ]


def _emit_normalize_phone(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype(str).str.replace(r'\\D', '', regex=True)"]


def _emit_hash_phone(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"{ds_var}[{c}] = {ds_var}[{c}].map("
        f"lambda v: hashlib.sha256(str(v).encode()).hexdigest() if pd.notna(v) else v)"
    ]


def _emit_mask_phone(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.replace(r'\\D', '', regex=True)",
        f"{ds_var}[{c}] = '***' + {ds_var}[{c}].str[-4:].where({ds_var}[{c}].str.len() >= 4, '***')",
    ]


def _emit_lowercase(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype(str).str.lower()"]


def _emit_uppercase(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype(str).str.upper()"]


def _emit_standardize_boolean(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"_v = {ds_var}[{c}].astype(str).str.strip().str.lower()",
        f"{ds_var}[{c}] = _v.isin(('1', 'true', 'yes', 'y', 't')).astype('Int64')",
    ]


def _emit_deduplicate_col(
    col: str,
    ds_var: str,
    _p: Dict[str, Any],
    plan: Optional[Dict[str, Any]] = None,
    ds_name: Optional[str] = None,
) -> List[str]:
    keys = []
    if plan and ds_name:
        keys = plan.get("business_keys", {}).get(ds_name) or []
    if col and str(col).lower() not in ("row-level", "[row-level]"):
        step_cols = [c.strip() for c in str(col).split(",") if c.strip()]
        keys.extend(step_cols)
    unique_keys = []
    for k in keys:
        if k not in unique_keys:
            unique_keys.append(k)
            
    if unique_keys:
        watermark_col = None
        if plan:
            watermark_col = plan.get("business_rules", {}).get("watermark_column")
        lines = []
        if watermark_col:
            lines.append(f"{ds_var} = {ds_var}.sort_values(by={repr(watermark_col)}, ascending=False)")
        cols_list = ", ".join(repr(k) for k in unique_keys)
        lines.append(f"{ds_var} = {ds_var}.drop_duplicates(subset=[{cols_list}], keep='first')")
        return lines
    else:
        return [f"{ds_var} = {ds_var}.drop_duplicates()"]


def _emit_zero_to_null(col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    replace_vals = params.get("replace_values") or ["0", "-999", "999999", "9999999", "###"]
    numeric_vals = []
    for v in replace_vals:
        try:
            numeric_vals.append(float(v))
            if float(v).is_integer():
                numeric_vals.append(int(v))
        except ValueError:
            pass
    all_replace = list(set(replace_vals + numeric_vals))
    return [
        f"# Nullify zero, sentinel, or punctuation placeholder values",
        f"{ds_var}[{c}] = {ds_var}[{c}].replace({repr(all_replace)}, pd.NA)"
    ]


def _emit_range_clip(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"# Range clip on {col}",
        f"{ds_var}[{c}] = pd.to_numeric({ds_var}[{c}], errors='coerce').clip(lower=0)",
    ]


def _emit_replace_values(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"# replace_values on {col}: define mapping in business_rules.replace_values"]


def _emit_regex_replace(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{_col_expr(col)}] = {ds_var}[{_col_expr(col)}].astype(str).str.replace(r'[^\\w\\s]', '', regex=True)"]


def _emit_drop_column(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var} = {ds_var}.drop(columns=[{_col_expr(col)}], errors='ignore')"]


def _emit_nullify_future_dates(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"{ds_var}[{c}] = pd.to_datetime({ds_var}[{c}], errors='coerce')",
        f"{ds_var}.loc[{ds_var}[{c}] > pd.Timestamp.now(tz=None), {c}] = pd.NA",
    ]


def _emit_nullify_punctuation(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"# Nullify punctuation-only strings",
        f"if {c} in {ds_var}.columns:",
        f"    _mask = ~{ds_var}[{c}].astype(str).str.contains(r'[a-zA-Z0-9]', na=False) & {ds_var}[{c}].notna()",
        f"    {ds_var}.loc[_mask, {c}] = pd.NA",
    ]


def _emit_nullify_dummy_dates(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    c = _col_expr(col)
    return [
        f"# Nullify dummy dates (e.g. 1900-01-01 or Jan 1st default dates)",
        f"if {c} in {ds_var}.columns:",
        f"    _dates = pd.to_datetime({ds_var}[{c}], errors='coerce')",
        f"    _mask = (_dates == '1900-01-01') | ((_dates.dt.month == 1) & (_dates.dt.day == 1))",
        f"    {ds_var}.loc[_mask & {ds_var}[{c}].notna(), {c}] = pd.NA",
    ]


def _emit_noop(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"# Column {col}: no transform (user accepted as-is)"]


def _emit_ri(col: str, ds_var: str, p: Dict[str, Any]) -> List[str]:
    rel_ds = p.get("related_dataset") or "?"
    rel_col = p.get("related_column") or "?"
    mode = p.get("enforcement_mode") or "flag"
    fk_action = p.get("fk_action") or "flag"
    
    c = _col_expr(col)
    lines = [
        f"# Referential integrity: {col} -> {rel_ds}.{rel_col} (action={fk_action}, mode={mode})",
        f"if all_dfs is not None and {repr(rel_ds)} in all_dfs:",
        f"    _parent_df = all_dfs[{repr(rel_ds)}]",
        f"    if {repr(rel_col)} in _parent_df.columns:",
        f"        from agent.etl_pipeline.relationship_key_contract import DEFAULT_CONTRACT",
        f"        from agent.etl_pipeline.relationship_key_normalization_pandas import normalize_relationship_key_pandas",
        # Normalize parent key
        f"        _norm_parent_keys = normalize_relationship_key_pandas(_parent_df[{repr(rel_col)}], DEFAULT_CONTRACT)",
        f"        _parent_keys = set(_norm_parent_keys.dropna().unique())",
        # Normalize child key
        f"        _norm_child_keys = normalize_relationship_key_pandas({ds_var}[{c}], DEFAULT_CONTRACT)",
        f"        _is_orphan = ~_norm_child_keys.isin(_parent_keys) & _norm_child_keys.notna()",
        f"        _orphan_count = _is_orphan.sum()",
        f"        if _orphan_count > 0:",
        f"            logging.warning(f'Found {{_orphan_count}} orphan values in {col} referencing {rel_ds}.{rel_col}')",
    ]
    
    if fk_action == "reject_orphans":
        lines.extend([
            f"            {ds_var} = {ds_var}[~_is_orphan]",
            f"            logging.info(f'Dropped {{_orphan_count}} orphan rows')",
        ])
    elif fk_action == "null_fill_fk":
        lines.extend([
            f"            # Action: null_fill_fk",
            f"            {ds_var}.loc[_is_orphan, {c}] = None",
            f"            logging.info(f'Null-filled {{_orphan_count}} orphan values')",
        ])
    elif fk_action == "create_unknown_dim_record":
        lines.extend([
            f"            # Action: create_unknown_dim_record",
            f"            _missing_keys = {ds_var}.loc[_is_orphan, {c}].dropna().unique()",
            f"            if len(_missing_keys) > 0:",
            f"                _new_rows = pd.DataFrame({{ {repr(rel_col)}: _missing_keys }})",
            f"                for _col in _parent_df.columns:",
            f"                    if _col != {repr(rel_col)}:",
            f"                        _new_rows[_col] = None",
            f"                all_dfs[{repr(rel_ds)}] = pd.concat([_parent_df, _new_rows], ignore_index=True)",
            f"                logging.info(f'Created {{len(_missing_keys)}} unknown dimension records in {rel_ds}')",
        ])
    else:  # "flag" or "quarantine"
        lines.extend([
            f"            # Action: flag / warn only",
            f"            pass",
        ])
        
    lines.extend([
        f"else:",
        f"    logging.warning(f'Skipped referential integrity check for {col} -> {rel_ds}.{rel_col} (parent dataset not loaded)')",
    ])
    return lines


def _emit_at_least_one(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    cols_split = [c.strip() for c in col.split(",")]
    return [
        f"# Ensure at least one of {cols_split} is non-null",
        f"_mask = {ds_var}[{cols_split!r}].isna().all(axis=1)",
        f"if _mask.any():",
        f"    logging.warning(f'Quarantining {{_mask.sum()}} rows where all of {cols_split} are null')",
        f"    {ds_var} = {ds_var}[~_mask]",
    ]


def _emit_unsupported(action: str, col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"# Unsupported in codegen v1: {action} ({col})"]


def _emit_review_manually(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [
        f"# MANUAL REVIEW REQUIRED: {col}",
        f"logger.warning('Column {col} requires manual review — excluded from auto-clean')",
    ]


def _emit_preserve_null(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"# preserve_null on {col}: column passes through null unchanged"]

def _emit_fill_constant(col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    const_val = params.get("constant_value") or "unknown"
    return [f"{ds_var}[{repr(col)}] = {ds_var}[{repr(col)}].fillna({repr(const_val)})"]

def _emit_preserve_null_and_flag(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [f"{ds_var}[{repr(f'is_missing_{col}')}] = {ds_var}[{repr(col)}].isna()"]

def _emit_impute_numeric(col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    strategy = params.get("strategy") or "median"
    fill_val = params.get("value") or 0.0
    return [
        f"# Impute numeric column: strategy={strategy}",
        f"{ds_var}[{repr(f'is_imputed_{col}')}] = {ds_var}[{repr(col)}].isna()",
        f"{ds_var}[{repr(col)}] = {ds_var}[{repr(col)}].fillna({repr(fill_val)})"
    ]

def _emit_impute_date(col: str, ds_var: str, params: Dict[str, Any]) -> List[str]:
    strategy = params.get("strategy") or "constant"
    fill_val = params.get("value") or "1970-01-01"
    return [
        f"# Impute date column: strategy={strategy}",
        f"{ds_var}[{repr(f'is_imputed_{col}')}] = {ds_var}[{repr(col)}].isna()",
        f"{ds_var}[{repr(col)}] = pd.to_datetime({ds_var}[{repr(col)}].fillna({repr(fill_val)}))"
    ]

def _emit_quarantine_missing_key(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [
        f"# Quarantine missing key: routing rows with missing '{col}' keys to quarantine",
        f"{ds_var}[{repr(f'_is_missing_{col}')}] = {ds_var}[{repr(col)}].isna()"
    ]

def _emit_preserve_missing_suspected_key(col: str, ds_var: str, _p: Dict[str, Any]) -> List[str]:
    return [
        f"# Preserve suspected missing key: flagging without quarantine",
        f"{ds_var}[{repr(f'is_missing_{col}')}] = {ds_var}[{repr(col)}].isna()"
    ]


def _emit_deduplicate_ds(ds_var: str) -> List[str]:
    return [f"{ds_var} = {ds_var}.drop_duplicates()"]


_ACTION_REGISTRY: Dict[str, Callable[[str, str, Dict[str, Any]], List[str]]] = {
    "review_manually": _emit_review_manually,
    "trim": _emit_trim,
    "fill_or_drop": _emit_fill_step,
    "fill_nulls_simple": _emit_fill_step,
    "preserve_null": _emit_preserve_null,
    "fill_constant": _emit_fill_constant,
    "preserve_null_and_flag": _emit_preserve_null_and_flag,
    "impute_numeric": _emit_impute_numeric,
    "impute_date": _emit_impute_date,
    "quarantine_missing_key": _emit_quarantine_missing_key,
    "preserve_missing_suspected_key": _emit_preserve_missing_suspected_key,
    "flag_missing_key": _emit_preserve_null_and_flag,
    "flag_missing_declared_key": _emit_preserve_null_and_flag,
    "trim_preserve_case": _emit_trim,
    "flag_duplicate_candidates": lambda c, v, p: [f"{v}[{repr(f'is_duplicate_{c}')}] = True"],
    "flag_near_duplicate_cluster": lambda c, v, p: [f"{v}[{repr(f'is_duplicate_{c}')}] = True"],
    "flag_business_key_duplicate": lambda c, v, p: [f"{v}[{repr(f'is_duplicate_{c}')}] = True"],
    "coerce_numeric": _emit_coerce_numeric,
    "cast_type": _emit_cast_type,
    "parse_dates": _emit_parse_dates,
    "sanitize_email": _emit_sanitize_email,
    "normalize_phone": _emit_normalize_phone,
    "hash_phone": _emit_hash_phone,
    "mask_phone": _emit_mask_phone,
    "lowercase": _emit_lowercase,
    "uppercase": _emit_uppercase,
    "standardize_boolean": _emit_standardize_boolean,
    "deduplicate": _emit_deduplicate_col,
    "replace_sentinel_values": _emit_replace_sentinel_values,
    "fill_nulls_flag": _emit_fill_nulls_flag,
    "parse_dates_safe": _emit_parse_dates_safe,
    "flag_domain_violation": _emit_flag_domain_violation,
    "zero_to_null": _emit_zero_to_null,
    "clip_or_flag": lambda c, v, p: _emit_outliers("clip_or_flag", c, v, p),
    "flag_outliers": lambda c, v, p: _emit_outliers("flag_outliers", c, v, p),
    "clip_outliers": lambda c, v, p: _emit_outliers("clip_outliers", c, v, p),
    "cap_outliers": lambda c, v, p: _emit_outliers("cap_outliers", c, v, p),
    "range_clip": _emit_range_clip,
    "replace_values": _emit_replace_values,
    "regex_replace": _emit_regex_replace,
    "drop_column": _emit_drop_column,
    "exclude_column": _emit_drop_column,
    "nullify_future_dates": _emit_nullify_future_dates,
    "nullify_punctuation": _emit_nullify_punctuation,
        "noop": _emit_noop,
    "validate_referential_integrity_or_stage": _emit_ri,
    "validate_and_partition_relationship": _emit_ri,
    "at_least_one": _emit_at_least_one,
}


def _emit_step(
    action: str,
    col: Optional[str],
    ds_var: str,
    step_meta: Optional[Dict[str, Any]] = None,
    plan: Optional[Dict[str, Any]] = None,
    ds_name: Optional[str] = None,
) -> List[str]:
    params = step_params(step_meta)
    act = (action or "").lower()
    if act == "deduplicate":
        return _emit_deduplicate_col(col or "", ds_var, params, plan, ds_name)
    if not col or str(col).lower() in ("row-level", "[row-level]"):
        return []
    handler = _ACTION_REGISTRY.get(act)
    if handler:
        return handler(col, ds_var, params)
    return _emit_unsupported(act, col, ds_var, params)


def _emit_valid_values(ds_var: str, ds_name: str, rules: Dict[str, Any]) -> List[str]:
    valid_values_rules: Dict[str, List[Any]] = rules.get("valid_values") or {}
    if not valid_values_rules:
        return []
    never_drop = bool(rules.get("never_drop_rows"))
    lines: List[str] = ["", "    # Valid-values (business rule)"]
    if never_drop:
        lines.append("    # never_drop_rows: nullify disallowed / invalid values, keep all rows")
        for col, allowed in valid_values_rules.items():
            c = _col_expr(col)
            sid = _safe_ident(col)
            lines.extend([
                f"    if {c} in {ds_var}.columns:",
                f"        _allowed_{sid} = {{str(v).lower() for v in {repr(list(allowed))}}}",
                f"        _bad = ~{ds_var}[{c}].astype(str).str.lower().isin(_allowed_{sid}) & {ds_var}[{c}].notna()",
                f"        if _bad.any():",
                f"            logging.warning(f'Setting {{_bad.sum()}} invalid values to NA on {col} in {ds_name}')",
                f"        {ds_var}.loc[_bad, {c}] = pd.NA",
            ])
    else:
        lines.append("    # Filter rows with disallowed values")
        for col, allowed in valid_values_rules.items():
            c = _col_expr(col)
            sid = _safe_ident(col)
            lines.extend([
                f"    if {c} in {ds_var}.columns:",
                f"        _allowed_{sid} = {{str(v).lower() for v in {repr(list(allowed))}}}",
                f"        _before = len({ds_var})",
                f"        {ds_var} = {ds_var}[{ds_var}[{c}].astype(str).str.lower().isin(_allowed_{sid}) | {ds_var}[{c}].isna()]",
                f"        _dropped = _before - len({ds_var})",
                f"        if _dropped > 0:",
                f"            logging.info(f'Dropped {{_dropped}} rows in {ds_name} due to valid_values on {col}')",
            ])
    return lines


def _emit_run_all_example(plan: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    entries = manifest.get("datasets") or {}
    if not entries:
        return []
    lines = [
        "",
        "def run_all():",
        '    """Load each dataset via connector_manifest, transform, return dict of DataFrames."""',
        "    import os",
        "    results = {}",
    ]
    for ds_name, entry in entries.items():
        read = python_read_snippet(entry if isinstance(entry, dict) else {"location": ds_name, "format": "csv"})
        lines.append(f'    results[{ds_name!r}] = {read}')
    for ds_name, entry in entries.items():
        fn = f"transform_{_safe_ident(ds_name)}"
        lines.append(f'    results[{ds_name!r}] = {fn}(results[{ds_name!r}], results)')
    lines.append("    return results")
    lines.append("")
    return lines


def generate_python_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    """Generate one transform_* function per dataset in the plan."""
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules: Dict[str, Any] = plan.get("business_rules") or {}
    non_nullable_cols: List[str] = business_rules.get("non_nullable") or []
    required_columns: List[str] = business_rules.get("required_columns") or []

    needs_hashlib = any(
        str(st.get("action") or "") == "hash_phone"
        for block in (plan.get("datasets") or {}).values()
        for st in (block or {}).get("steps") or []
    )

    never_drop = bool(business_rules.get("never_drop_rows"))
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    join_strategy = str(joins[0].get("join_type") or "left") if joins else "none"
    privacy_policy = ", ".join(
        str(st.get("params", {}).get("privacy") or st.get("action"))
        for block in (plan.get("datasets") or {}).values()
        for st in (block or {}).get("steps") or []
        if str(st.get("action") or "") in ("hash_phone", "mask_phone", "exclude_column")
    ) or "none"

    lines: List[str] = [
        '"""',
        f"ETL for plan_id: {plan_id}",
        "Generated by: Agent Dhara",
        "Policy:",
        f"- Row preservation: {'preserve all rows' if never_drop else 'subset drops allowed'}",
        f"- Join strategy: {join_strategy}",
        f"- Privacy handling: {privacy_policy}",
        f"- Output format: {plan.get('engine') or 'python'}",
        "Dependencies: pandas",
        '"""',
        "from __future__ import annotations",
        "",
        "import logging",
        "import pandas as pd",
    ]
    if needs_hashlib:
        lines.append("import hashlib")
    lines.extend([
        "",
        'logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")',
        "logger = logging.getLogger('agent_dhara')",
        "",
    ])

    notes = str(business_rules.get("notes") or "").strip()
    if notes:
        lines.extend(["# Business notes:", "# " + notes.replace("\n", "\n# "), ""])

    for item in plan.get("manual_review") or []:
        if not lines or lines[-1] != "":
            pass
    manual = plan.get("manual_review") or []
    if manual:
        lines.append("# Manual review items:")
        for item in manual:
            lines.append(f"#   [{item.get('dataset')}] {item.get('column')}: {item.get('message') or ''}")
        lines.append("")

    ds_plan = plan.get("datasets") or {}
    for ds_name, block in ds_plan.items():
        fn = f"transform_{_safe_ident(ds_name)}"
        lines.append(f"def {fn}(df: pd.DataFrame, all_dfs: dict | None = None) -> pd.DataFrame:")
        lines.append(f'    """Clean transforms for dataset: {ds_name}"""')
        ds_var = "out"
        lines.append(f"    {ds_var} = df.copy()")

        # Manual review warnings in code (B3)
        for mr in plan.get("manual_review") or []:
            if mr.get("dataset") == ds_name:
                col = mr.get("column")
                if col:
                    lines.append(f"    logger.warning('Column {col} requires manual review — skipping automation')")

        if required_columns:
            lines.extend([
                "",
                f"    _missing = [c for c in {required_columns!r} if c not in {ds_var}.columns]",
                "    if _missing:",
                f"        raise ValueError(f'Required columns missing in {ds_name}: {{_missing}}')",
            ])

        steps = sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0))
        for st in steps:
            action = str(st.get("action") or "")
            col = st.get("column")
            lines.append(f"    # Step: {action} on {col}")
            if st.get("note"):
                lines.append(f"    # Note: {st['note']}")
            for sl in _log_destructive(ds_var, ds_name, action, col):
                lines.append(f"    {sl}")
            for sl in _emit_step(action, col, ds_var, step_meta=st, plan=plan, ds_name=ds_name):
                lines.append(f"    {sl}")
            for sl in _log_destructive_after(ds_var, ds_name, action):
                lines.append(f"    {sl}")

        exclude_cols = business_rules.get("exclude_columns") or []
        if exclude_cols:
            lines.extend([
                "",
                f"    _to_drop = [c for c in {exclude_cols!r} if c in {ds_var}.columns]",
                "    if _to_drop:",
                f"        logging.info(f'Dropping excluded columns in {ds_name}: {{_to_drop}}')",
                f"        {ds_var} = {ds_var}.drop(columns=_to_drop)",
            ])

        lines.extend(_emit_valid_values(ds_var, ds_name, business_rules))

        if non_nullable_cols:
            lines.append("")
            for col in non_nullable_cols:
                c = _col_expr(col)
                lines.extend([
                    f"    if {c} in {ds_var}.columns and {ds_var}[{c}].isna().all():",
                    f"        raise ValueError('Column {col} is entirely null — violates non_nullable rule')",
                ])

        lines.append(f"    return {ds_var}")
        lines.append("")

    lines.append("DATASET_NAMES = " + repr(list(ds_plan.keys())))
    lines.append("")

    manifest = plan.get("connector_manifest") or {}
    if plan.get("datasets") or manifest.get("datasets"):
        lines.append(resolve_path_python_helper())
        lines.append("")
    lines.extend(emit_python_load_and_join(plan, manifest))
    lines.extend(_emit_run_all_example(plan, manifest))

    return "\n".join(lines)
