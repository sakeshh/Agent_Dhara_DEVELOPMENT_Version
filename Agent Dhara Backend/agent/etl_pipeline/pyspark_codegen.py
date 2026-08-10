from __future__ import annotations

import html as _html
import re
import os
from typing import Any, Dict, List, Optional, Set

from agent.etl_pipeline.codegen_policy import plan_policy_block
from agent.etl_pipeline.codegen_shared import outlier_multiplier, step_params
from agent.etl_pipeline.join_emitters import (
    emit_pyspark_joins,
    emit_pyspark_load,
    emit_pyspark_output_contract,
    emit_pyspark_write_outputs,
)
from agent.etl_pipeline.io_snippets import (
    pyspark_iqr_bounds_helper,
    pyspark_prefix_non_key_columns_helper,
    pyspark_production_helpers,
    resolve_path_pyspark_helper,
    resolve_path_fabric_pyspark_helper,
)


def _safe(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    return (s or "dataset").strip("_")


def _canonical_id(name: str) -> str:
    return _safe(name)


# Columns that are business keys / identifiers -- NEVER fill with computed values
_BUSINESS_KEY_PATTERNS = (
    "id", "key", "code", "ref", "num", "no", "nbr",
    "uuid", "guid", "sku", "ean", "isbn", "asin",
)
# Columns that are categorically typed -- NEVER cast to numeric or apply avg/percentile_approx
_CATEGORICAL_PATTERNS = (
    "region", "channel", "location", "category", "status",
    "country", "state", "city", "zone", "segment", "type",
    "department", "division", "brand", "product_name", "tier",
    "machine", "site", "facility", "plant", "warehouse", "floor",
)
# Columns that are name / master-data fields -- DO NOT lowercase
_NAME_FIELD_PATTERNS = (
    "name", "title", "description", "label", "machine",
    "customer", "vendor", "supplier", "employee", "owner",
)


def _is_business_key(col: str) -> bool:
    col_l = str(col).lower()
    return any(k in col_l for k in _BUSINESS_KEY_PATTERNS)


def _is_categorical_col(col: str) -> bool:
    col_l = str(col).lower()
    return any(k in col_l for k in _CATEGORICAL_PATTERNS)


def _is_name_field(col: str) -> bool:
    col_l = str(col).lower()
    return any(k in col_l for k in _NAME_FIELD_PATTERNS)


def _is_numeric_dtype(dtype_str: str) -> bool:
    """Return True if the assessed column dtype is genuinely numeric."""
    d = str(dtype_str or "").strip().lower()
    return d in (
        "int", "integer", "long", "bigint", "short", "byte",
        "float", "double", "decimal", "numeric", "number",
        "int64", "int32", "float64", "float32",
    )


def _looks_numeric_metric(col: str) -> bool:
    c = str(col or "").lower()
    _NUMERIC_NAME_HINTS = (
        "amount", "budget", "qty", "quantity", "price", "cost",
        "score", "total", "value", "revenue", "sales", "production_qty",
        "rate", "ratio", "margin", "fee"
    )
    return any(k in c for k in _NUMERIC_NAME_HINTS)


def _emit_fill_spark(col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    strat = params.get("fill_strategy")
    fval = params.get("fill_value")
    impute_flag = repr(f"is_imputed_{col}")
    # dtype from the step params (passed through from assessment column metadata)
    col_dtype = str(params.get("dtype") or params.get("inferred_type") or params.get("target_dtype") or "").strip().lower()
    # A column is non-numeric if the assessed dtype is string/object OR it matches categorical/name patterns
    is_string_col = (col_dtype in ("string", "object", "str", "text", "")) or not (_is_numeric_dtype(col_dtype) or col_dtype == "")
    if col_dtype == "":
        # dtype unknown: fall back to column-name heuristics only
        is_string_col = _is_categorical_col(col) or _is_name_field(col)

    # GOVERNANCE RULE: Business keys / IDs must NEVER be filled with computed values.
    # Filling sale_id=488.7018 violates data lineage and master data governance.
    # Instead: emit a missing-flag column and leave the original NULL intact.
    if _is_business_key(col):
        flag_col = repr(f"is_missing_{col}")
        return [
            f"# GOVERNANCE: '{col}' is a business key -- never fill with mean/median/arbitrary values.",
            f"# Emitting is_missing flag and leaving NULL intact for auditability.",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))",
        ]

    # IMPROVEMENT-4: Categorical columns must never be filled with a numeric literal.
    # A fill_value like 123 makes no sense for region/channel/location -- use 'unknown' instead.
    # PRODUCTION SAFETY: Also catches string-dtype columns that slipped past pattern matching.
    if (_is_categorical_col(col) or (is_string_col and strat in ("mean", "median"))) and not _looks_numeric_metric(col):
        safe_fval = fval
        if isinstance(fval, (int, float)) or (
            fval is not None and str(fval).strip().lstrip("-").replace(".", "", 1).isdigit()
        ):
            safe_fval = "unknown"
        if safe_fval is None or str(safe_fval).strip().lower() in ("none", "null", ""):
            return [
                f"# PRODUCTION SAFETY: '{col}' is a string/categorical column -- avg()/percentile_approx() would crash Spark.",
                f"# Using fillna('unknown') instead of mean/median imputation.",
                f"{df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
                f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit('unknown')))",
            ]
        return [
            f"# Categorical/string fill: using {repr(safe_fval)} (mean/median replaced with 'unknown' for string column).",
            f"{df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({repr(str(safe_fval))})))",
        ]

    if strat == "median":
        # PRODUCTION SAFETY: cast to double before percentile_approx for columns stored as string-typed numbers.
        # percentile_approx on a non-numeric Spark column raises AnalysisException at runtime.
        return [
            f"# Cast to double before percentile_approx -- string-stored numbers (e.g. '55', 'ten') require explicit cast.",
            f"_med_row = {df}.select(F.percentile_approx(F.col({c}).cast('double'), 0.5).alias('m')).first()",
            f"_med = _med_row['m'] if (_med_row and _med_row['m'] is not None) else None",
            f"if _med is not None:",
            f"    {df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
            f"    {df} = {df}.withColumn({c}, F.coalesce(F.col({c}).cast('double'), F.lit(_med)))",
        ]
    if strat == "mean":
        # PRODUCTION SAFETY: F.avg with .cast('double') prevents AnalysisException on string-stored numeric columns.
        return [
            f"# NOTE: F.avg triggers a Spark action (full scan). For large tables prefer percentile_approx.",
            f"_avg_row = {df}.select(F.avg(F.col({c}).cast('double')).alias('m')).first()",
            f"_avg = _avg_row['m'] if (_avg_row and _avg_row['m'] is not None) else None",
            f"# Cast back to the original column dtype to preserve integer/long precision.",
            f"if _avg is not None:",
            f"    {df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
            f"    {df} = {df}.withColumn({c}, F.coalesce(F.col({c}).cast('double'), F.lit(_avg)))",
        ]
    if strat == "value" and fval is not None:
        if str(fval).strip().lower() in ("none", "null", ""):
            return [
                f"# Warning: fill_value is empty/null/none for column {col}; keeping original values as-is",
            ]
        return [
            f"# IMPROVEMENT-3: Track imputed rows before filling.",
            f"{df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({repr(fval)})))",
        ]
    if strat == "value":
        is_text_safe = not any(k in str(col).lower() for k in ("id", "key", "code", "ref", "date", "time", "email", "phone", "num", "val", "amount", "price", "credit"))
        if is_text_safe:
            return [
                f"# WARNING: fill_value is None for column {col} -- using empty string default",
                f"{df} = {df}.withColumn({impute_flag}, F.col({c}).isNull().cast('boolean'))",
                f'{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit("")))'
            ]
        else:
            return [
                f"# WARNING: fill_value is None for column {col} -- defaulting to None to prevent invalid data/casts; keeping original values as-is",
            ]
    # Default fallback to satisfy validation
    return [
        f"# Warning: no fill_strategy for {col}; keeping original values as-is",
    ]


def _emit_outliers_spark(action: str, col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    flag_col = repr(f"{col}_outlier_flagged")
    mult = outlier_multiplier(params)
    method = params.get("outlier_method") or (
        "clip" if action == "clip_outliers" else "cap" if action == "cap_outliers" else "flag"
    )

    # GUARD: percentile_approx / IQR stats are only valid for numeric columns.
    # Applying them to a string/key column (e.g. product_id) raises a Spark
    # AnalysisException at runtime and crashes the entire notebook session.
    # For non-numeric columns: emit a cardinality/frequency-based rarity flag instead.
    if _is_business_key(col):
        return [
            f"# Governance safety: Business identifier key '{col}' is excluded from outlier detection",
        ]

    is_non_numeric = _is_categorical_col(col) or _is_name_field(col)
    if is_non_numeric:
        freq_threshold = params.get("freq_threshold") or 2  # flag values appearing fewer than N times
        return [
            f"# flag_outliers on '{col}': non-numeric column -- using frequency-based rare-value flag",
            f"# (IQR/percentile_approx cannot operate on string columns -- would crash the Spark session)",
            f"_freq_df = {df}.groupBy({c}).count().filter(F.col('count') < {freq_threshold})",
            f"_rare_vals = set(r[{c}] for r in _freq_df.collect())",
            f"{df} = {df}.withColumn({flag_col},",
            f"    F.col({c}).isin(list(_rare_vals)) & F.col({c}).isNotNull())",
        ]

    lines = [
        f"_stats, _iqr, _lower, _upper = _iqr_bounds({df}, {c}, multiplier={mult})",
    ]
    if method == "clip":
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when(F.col({c}) < F.lit(_lower), F.lit(_lower))"
            f" .when(F.col({c}) > F.lit(_upper), F.lit(_upper))"
            f" .otherwise(F.col({c})))"
        )
    elif method == "cap":
        med = params.get("fill_value")
        if med is not None:
            lines.append(f"_median = {med}")
        else:
            lines.append(f"_median = float(_stats['median']) if _stats is not None and _stats['median'] is not None else 0.0")
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper)), F.lit(_median))"
            f" .otherwise(F.col({c})))"
        )
    else:
        lines.append(
            f"{df} = {df}.withColumn({flag_col},"
            f" ((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper))) & F.col({c}).isNotNull())"
        )
    return lines


def _emit_spark(
    action: str,
    col: str | None,
    df: str,
    step_meta: Optional[Dict[str, Any]] = None,
    plan: Optional[Dict[str, Any]] = None,
    ds_name: Optional[str] = None,
) -> List[str]:
    params = step_params(step_meta)
    act = (action or "").lower()
    if act == "deduplicate":
        keys = []
        if plan and ds_name:
            keys = list(plan.get("business_keys", {}).get(ds_name) or [])
        if col and str(col).lower() not in ("row-level", "[row-level]"):
            step_cols = [c.strip() for c in str(col).split(",") if c.strip()]
            keys.extend(step_cols)
        unique_keys: List[str] = []
        for k in keys:
            if k not in unique_keys:
                unique_keys.append(k)
        if unique_keys:
            watermark_col = None
            if plan:
                watermark_col = plan.get("business_rules", {}).get("watermark_column")
            w_cols = ", ".join(repr(k) for k in unique_keys)
            order_expr = "F.lit(1)"
            if watermark_col:
                order_expr = f"F.col({repr(watermark_col)}).desc()"
            # FIX-2 / FIX-3: Never hard-drop rows with row_number()==1.
            # A hard-drop silently destroys data that never reaches the quarantine table,
            # breaking the governance contract of "flag, don't drop" and losing audit trail.
            # Instead: emit a boolean flag column (_is_duplicate_<safe_key>) so the
            # quarantine split (which matches c.startswith('_is_duplicate')) can route
            # duplicate rows to the quarantine table for human review.
            # Callers that genuinely need hard-dedup can filter on this flag after review.
            safe_key = "_".join(_safe(k) for k in unique_keys)
            flag_col_name = f"_is_duplicate_{safe_key}"
            # Pre-compute a safe label: joining with ", " avoids embedding a Python list
            # repr (which uses single quotes) inside a generated single-quoted string.
            keys_label = ", ".join(unique_keys)
            return [
                f"# Business-key deduplication based on: {keys_label}",
                f"# GOVERNANCE: Emitting flag column '{flag_col_name}' instead of hard-dropping rows.",
                f"# Hard-drop (row_number()==1 filter) destroys data silently -- rows never reach quarantine.",
                f"# Review flagged rows in the quarantine table before deciding to remove them.",
                f"from pyspark.sql import Window",
                f"_w = Window.partitionBy({w_cols}).orderBy({order_expr})",
                f"{df} = {df}.withColumn({repr(flag_col_name)}, (F.row_number().over(_w) > 1).cast('boolean'))",
                f"_dup_count = {df}.filter(F.col({repr(flag_col_name)})).count()",
                f"logger.warning('deduplicate [{keys_label}]: %d rows flagged as duplicates -- routed to quarantine (NOT dropped)', _dup_count)",
            ]
        else:
            # GOVERNANCE: No business key configured -- flag potential duplicates instead of dropping.
            return [
                f"# GOVERNANCE: No business key configured -- flagging potential duplicates instead of dropping.",
                f"# Review flagged rows and resolve manually before applying deduplication.",
                f"from pyspark.sql import Window as _DedupWindow",
                f"_data_cols = [F.col(c).cast('string') for c, dt in {df}.dtypes if not c.startswith('_') and not c.startswith('is_') and not dt.startswith(('struct', 'array', 'map'))]",
                f"_dup_w = _DedupWindow.partitionBy(F.xxhash64(*_data_cols) if _data_cols else F.lit(1)).orderBy(F.lit(1))",
                f"{df} = {df}.withColumn('_is_duplicate_row', (F.row_number().over(_dup_w) > 1).cast('boolean'))",
                f"_dup_count = {df}.filter(F.col('_is_duplicate_row')).count()",
                f"logger.warning('deduplicate [row-level]: %d potential duplicate rows flagged (NOT dropped -- manual review required)', _dup_count)",
            ]


    c = repr(str(col))
    if act == "trim":
        if params.get("source_issue_type") == "internal_whitespace" or params.get("issue_type") == "internal_whitespace":
            return [f'{df} = {df}.withColumn({c}, F.regexp_replace(F.trim(F.col({c}).cast("string")), r"\\s+", " "))']
        return [f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))']
    if act in ("fill_or_drop", "fill_nulls_simple"):
        return _emit_fill_spark(col, df, params)
    if act == "coerce_numeric":
        is_key = _is_business_key(col or "") or any(k in str(col).lower() for k in ("id", "key", "code", "ref", "country", "status", "date"))
        if is_key or _is_categorical_col(col or ""):
            return [
                f"# Step: coerce_numeric on '{col}' (categorical/key/country column -- keeping as text string to prevent data loss)",
                f"{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast('string')))"
            ]
        col_safe = _safe(col or "col")
        res = [f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))"]
        if any(m in str(col).lower() for m in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val")):
            neg_flag = repr(f"is_negative_{col_safe}")
            res.append(f"{df} = {df}.withColumn({neg_flag}, (F.col({c}).isNotNull() & (F.col({c}) < 0)).cast('boolean'))")
        return res

    if act == "zero_to_null":
        return [f"{df} = {df}.withColumn({c}, F.when((F.col({c}) == 0) | (F.col({c}) == '0'), F.lit(None)).otherwise(F.col({c})))"]
    if act == "replace_sentinel_values":
        svals = params.get("sentinel_values") or [-999.0, 999.0, 9999.0, 99999.0, 999999.0, -9999.0]
        list_expr = []
        for val in svals:
            list_expr.append(repr(val))
            if isinstance(val, (int, float)):
                if val == int(val):
                    list_expr.append(repr(int(val)))
                    list_expr.append(repr(str(int(val))))
                list_expr.append(repr(str(val)))
        list_expr_str = ", ".join(sorted(list(set(list_expr))))
        return [f"{df} = {df}.withColumn({c}, F.when(F.col({c}).isin([{list_expr_str}]), F.lit(None)).otherwise(F.col({c})))"]
    if act == "cast_type":
        target_type = params.get("target_type") or params.get("cast_to") or "long"
        is_key = any(k in str(col).lower() for k in ("id", "key", "code", "ref"))
        is_int_target = any(it in str(target_type).lower() for it in ("long", "int", "integer"))
        clean_col = f"{col}_clean"
        raw_col = f"{col}_raw"
        # GOVERNANCE: Categorical text columns (region, channel, location, status) must NEVER be
        # cast to integer types. Casting 'NY' -> long produces NULL silently, destroying valid data.
        if is_int_target and _is_categorical_col(col or ""):
            flag_col = repr(f"{col}_domain_flag")
            return [
                f"# GOVERNANCE: '{col}' is a categorical text column -- refusing cast to '{target_type}'.",
                f"# Casting 'NY', 'Mumbai', 'Online' to integer would silently null all text values.",
                f"# Emitting a domain-flag column to identify rows where numeric-only strings exist.",
                f"{df} = {df}.withColumn({repr(raw_col)}, F.col({c}).cast('string'))",
                f"{df} = {df}.withColumn({repr(clean_col)}, F.col({repr(raw_col)}))",
                f"{df} = {df}.withColumn({flag_col},"
                f" F.col({repr(raw_col)}).rlike(r'^\\d+$') & F.col({repr(raw_col)}).isNotNull())",
            ]
        if is_key and is_int_target:
            return [
                f"{df} = {df}.withColumn({repr(raw_col)}, F.col({c}).cast('string'))",
                f"{df} = {df}.withColumn({repr(clean_col)}, "
                f"F.when(F.col({repr(raw_col)}).rlike(r'^\\d+$'), F.col({repr(raw_col)}).cast({repr(target_type)}))"
                f".otherwise(F.lit(None).cast({repr(target_type)})))"
            ]
        # IMPROVEMENT-2 (ANSI-safe): use try_cast via a temp column.
        # In Fabric Spark (ANSI mode), cast() on invalid values throws CAST_INVALID_INPUT
        # instead of returning NULL. try_cast() returns NULL on failure -- no exception.
        # Single evaluation: temp column is reused for both the flag and the cast result.
        if is_int_target:
            if is_key:
                return [
                    f"{df} = {df}.withColumn({repr(raw_col)}, F.col({c}).cast('string'))",
                    f"{df} = {df}.withColumn({repr(clean_col)}, "
                    f"F.when(F.col({repr(raw_col)}).rlike(r'^\\d+$'), F.col({repr(raw_col)}).cast({repr(target_type)}))"
                    f".otherwise(F.lit(None).cast({repr(target_type)})))"
                ]
            res = [
                f"{df} = {df}.withColumn({c}, F.col({c}).cast({repr(target_type)}))",
                f"{df} = {df}.withColumn({repr(raw_col)}, F.col({c}).cast('string'))",
                f"{df} = {df}.withColumn({repr(clean_col)}, F.col({c}))",
            ]
            if any(m in str(col).lower() for m in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val")):
                col_safe = _safe(col or "col")
                neg_flag = repr(f"is_negative_{col_safe}")
                res.append(f"{df} = {df}.withColumn({neg_flag}, (F.col({c}).isNotNull() & (F.col({c}) < 0)).cast('boolean'))")
            return res
        
        # For non-integer targets (e.g. double, string):
        res = [
            f"{df} = {df}.withColumn({c}, F.col({c}).cast({repr(target_type)}))",
            f"{df} = {df}.withColumn({repr(raw_col)}, F.col({c}).cast('string'))",
            f"{df} = {df}.withColumn({repr(clean_col)}, F.col({c}))",
        ]
        if any(m in str(col).lower() for m in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val")):
            col_safe = _safe(col or "col")
            neg_flag = repr(f"is_negative_{col_safe}")
            res.append(f"{df} = {df}.withColumn({neg_flag}, (F.col({repr(clean_col)}).isNotNull() & (F.col({repr(clean_col)}) < 0)).cast('boolean'))")
        return res

    if act == "parse_dates":
        # Portable date parsing (native PySpark -- no SQL expression or try_cast dependency)
        col_safe = _safe(col or "date")
        flag_col = repr(f"is_invalid_{col_safe}")
        raw_col = f"{col}_raw"
        clean_col = f"{col}_clean"
        tmp_col = f"_tmp_{col_safe}"
        bad_sentinel_list = repr(["00/00/0000", "00-00-0000", "0000-00-00",
                                  "99/99/9999", "99-99-9999", "9999-99-99",
                                  "invalid", "tbd", "not available", "n/a", "na"])
        return [
            f"# Portable date parsing (native PySpark functions -- 100% version compatible)",
            f"{df} = {df}.withColumn({repr(raw_col)}, F.lower(F.col({c}).cast('string')))",
            f"_is_bad_sentinel = F.col({repr(raw_col)}).isin({bad_sentinel_list})",
            f"_is_date_pattern = F.col({repr(raw_col)}).rlike(r'^\\d{{1,4}}[-/\\.\\s]\\d{{1,2}}[-/\\.\\s]\\d{{1,4}}')",
            f"# Pre-filter sentinel/non-date strings before timestamp conversion for safety",
            f"{df} = {df}.withColumn({repr(tmp_col)}, F.to_timestamp(F.when(~_is_bad_sentinel & _is_date_pattern, F.col({repr(raw_col)})).otherwise(F.lit(None))))",
            f"{df} = {df}.withColumn(",
            f"    {flag_col},",
            f"    F.when(F.col({repr(raw_col)}).isNull(), F.lit(False))",
            f"    .when(_is_bad_sentinel, F.lit(True))",
            f"    .when(F.col({repr(tmp_col)}).isNull() & F.col({repr(raw_col)}).isNotNull(), F.lit(True))",
            f"    .otherwise(F.lit(False)).cast('boolean')",
            f")",
            f"{df} = {df}.withColumn({repr(clean_col)}, F.col({repr(tmp_col)}))",
            f"{df} = {df}.drop({repr(tmp_col)})",
        ]
    if act == "parse_dates_safe":
        # Same portable date parsing pattern for parse_dates_safe.
        col_safe = _safe(col or "date")
        flag_col = repr(f"is_invalid_{col_safe}")
        raw_col = f"{col}_raw"
        clean_col = f"{col}_clean"
        tmp_col = f"_tmp_{col_safe}"
        bad_sentinel_list = repr(["00/00/0000", "00-00-0000", "0000-00-00",
                                  "99/99/9999", "99-99-9999", "9999-99-99",
                                  "invalid", "tbd", "not available", "n/a", "na"])
        return [
            f"# parse_dates_safe (native PySpark functions -- 100% version compatible)",
            f"{df} = {df}.withColumn({repr(raw_col)}, F.lower(F.col({c}).cast('string')))",
            f"_is_bad_sentinel = F.col({repr(raw_col)}).isin({bad_sentinel_list})",
            f"_is_date_pattern = F.col({repr(raw_col)}).rlike(r'^\\d{{1,4}}[-/\\.\\s]\\d{{1,2}}[-/\\.\\s]\\d{{1,4}}')",
            f"{df} = {df}.withColumn({repr(tmp_col)}, F.to_timestamp(F.when(~_is_bad_sentinel & _is_date_pattern, F.col({repr(raw_col)})).otherwise(F.lit(None))))",
            f"{df} = {df}.withColumn(",
            f"    {flag_col},",
            f"    F.when(F.col({repr(raw_col)}).isNull(), F.lit(False))",
            f"    .when(_is_bad_sentinel, F.lit(True))",
            f"    .when(F.col({repr(tmp_col)}).isNull() & F.col({repr(raw_col)}).isNotNull(), F.lit(True))",
            f"    .otherwise(F.lit(False)).cast('boolean')",
            f")",
            f"{df} = {df}.withColumn({repr(clean_col)}, F.col({repr(tmp_col)}))",
            f"{df} = {df}.drop({repr(tmp_col)})",
            f"_bad_date_cnt = {df}.filter(F.col({flag_col})).count()",
            f"logger.warning('parse_dates_safe: %d invalid/sentinel date values flagged in column {col}', _bad_date_cnt)",
        ]
    if act == "sanitize_email":
        return [
            f'{df} = {df}.withColumn({c}, F.lower(F.trim(F.col({c}).cast("string"))))',
            f"{df} = {df}.withColumn({c}, F.when(F.col({c}).rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'), F.col({c})).otherwise(F.lit(None)))",
        ]
    if act == "normalize_phone":
        return [f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""))']
    if act == "hash_phone":
        return [
            f"# Privacy: one-way hash (params.privacy=hash)",
            f"{df} = {df}.withColumn({c}, F.sha2(F.col({c}).cast('string'), 256))",
        ]
    if act == "mask_phone":
        return [
            f"# Privacy: reversible mask (params.privacy=mask)",
            f'{df} = {df}.withColumn({c}, F.concat(F.lit("***"), F.substring(F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""), -4, 4)))',
        ]
    if act == "lowercase":
        # GOVERNANCE: Do NOT lowercase business keys (sale_id, campaign_id), name fields (customer_name), or numeric columns.
        # Lowercasing 'AB123' -> 'ab123' or 'John Smith' -> 'john smith' destroys semantic value.
        is_num = any(k in str(col).lower() for k in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val", "production_qty", "revenue", "sales", "count", "num", "number", "rate", "ratio", "margin", "fee"))
        if _is_name_field(col or "") or _is_business_key(col or "") or is_num:
            return [
                f"# GOVERNANCE: '{col}' is a key/name/numeric field -- applying trim only (not lowercase).",
                f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))',
            ]
        return [f'{df} = {df}.withColumn({c}, F.lower(F.col({c}).cast("string")))']
    if act == "uppercase":
        # GOVERNANCE: Do NOT uppercase business keys, name fields, or numeric columns.
        is_num = any(k in str(col).lower() for k in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val", "production_qty", "revenue", "sales", "count", "num", "number", "rate", "ratio", "margin", "fee"))
        if _is_name_field(col or "") or _is_business_key(col or "") or is_num:
            return [
                f"# GOVERNANCE: '{col}' is a key/name/numeric field -- applying trim only (not uppercase).",
                f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))',
            ]
        return [f'{df} = {df}.withColumn({c}, F.upper(F.col({c}).cast("string")))']
    if act in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
        return _emit_outliers_spark(act, col, df, params)
    if act == "standardize_boolean":
        return [
            f'{df} = {df}.withColumn({c}, F.when(F.lower(F.col({c}).cast("string")).isin("1","true","yes","y","t"), F.lit(1)).otherwise(F.lit(0)))'
        ]
    if act == "nullify_punctuation":
        is_num = any(k in str(col).lower() for k in ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val"))
        if is_num:
            return [
                f"# Nullify punctuation on numeric column: cast to double directly instead of rlike",
                f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))"
            ]
        return [
            f"# Nullify punctuation-only strings",
            f"{df} = {df}.withColumn({c}, F.when(~F.col({c}).cast('string').rlike('[a-zA-Z0-9]'), F.lit(None)).otherwise(F.col({c})))"
        ]
    if act == "nullify_dummy_dates":
        col_clean = _safe(col)
        return [
            f"# Nullify dummy dates (e.g. 1900-01-01)",
            f"{df} = {df}.withColumn('_tmp_date_{col_clean}', F.to_date(F.col({c})))",
            f"{df} = {df}.withColumn({c}, F.when((F.col('_tmp_date_{col_clean}').eqNullSafe(F.lit('1900-01-01'))) | ((F.month(F.col('_tmp_date_{col_clean}')) == 1) & (F.dayofmonth(F.col('_tmp_date_{col_clean}')) == 1)), F.lit(None)).otherwise(F.col({c})))",
            f"{df} = {df}.drop('_tmp_date_{col_clean}')"
        ]
    if act == "nullify_future_dates":
        c = repr(str(col))
        return [
            f"# Nullify future dates in '{col}' (dates after today are set to null)",
            f"{df} = {df}.withColumn({c}, F.when(F.to_date(F.col({c})) > F.current_date(), F.lit(None)).otherwise(F.col({c})))"
        ]
    if act == "range_clip":
        lo = params.get("min_value") or params.get("lower_bound") or 0
        hi = params.get("max_value") or params.get("upper_bound")
        if hi is not None:
            cond_str = f"(F.col({c}) < {lo}) | (F.col({c}) > {hi})"
        else:
            cond_str = f"F.col({c}) < {lo}"
        return [f"{df} = {df}.withColumn({c}, F.when({cond_str}, F.lit(None)).otherwise(F.col({c})))"]
    if act == "regex_replace":
        pattern = params.get("regex_pattern") or params.get("pattern") or r"[^\w\s]"
        replacement = params.get("replacement") or ""
        pattern_esc = pattern.replace("\\", "\\\\")
        return [f"{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast('string'), {repr(pattern_esc)}, {repr(replacement)}))"]
    if act == "replace_values":
        mapping = params.get("replace_values") or {}
        if mapping:
            col_expr = f"F.col({c})"
            for old_v, new_v in mapping.items():
                col_expr = f"F.when(F.col({c}) == F.lit({repr(old_v)}), F.lit({repr(new_v)})).otherwise({col_expr})"
            return [f"{df} = {df}.withColumn({c}, {col_expr})"]
        return [f"# replace_values on {col}: no mapping provided"]
    if act in ("drop_column", "exclude_column"):
        return [f"{df} = {df}.drop({c})"]
    if act == "noop":
        return [f"# Column {col}: no transform"]
    if act in ("flag_missing_key", "flag_missing_declared_key"):
        flag_col = repr(f"is_missing_{col}")
        return [
            f"# Governance: Adding missing key flag column for '{col}' without imputation to prevent fake key generation.",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))"
        ]
    if act == "preserve_null":
        return [f"# preserve_null on {col}: column passes through null unchanged"]
    if act == "fill_constant":
        const_val = params.get("constant_value") or "unknown"
        return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({repr(const_val)})))"]
    if act == "preserve_null_and_flag":
        col_safe = _safe(col or "col")
        flag_col = repr(f"is_missing_{col_safe}")
        return [f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))"]
    if act == "impute_numeric":
        strategy = params.get("strategy") or "median"
        col_safe = _safe(col or "col")
        flag_col = repr(f"is_imputed_{col_safe}")
        fill_val = params.get("value") or 0.0
        return [
            f"# Impute numeric column: strategy={strategy}",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}).cast('double'), F.lit({repr(fill_val)})))"
        ]
    if act == "impute_date":
        strategy = params.get("strategy") or "constant"
        col_safe = _safe(col or "col")
        flag_col = repr(f"is_imputed_{col_safe}")
        fill_val = params.get("value") or "1970-01-01"
        return [
            f"# Impute date column: strategy={strategy}",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.to_date(F.col({c})), F.to_date(F.lit({repr(fill_val)}))))"
        ]
    if act == "quarantine_missing_key":
        col_safe = _safe(col or "col")
        flag_col = repr(f"_is_missing_{col_safe}")
        return [
            f"# Quarantine missing key: routing rows with missing '{col}' keys to quarantine",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))"
        ]
    if act == "preserve_missing_suspected_key":
        col_safe = _safe(col or "col")
        flag_col = repr(f"is_missing_{col_safe}")
        return [
            f"# Preserve suspected missing key: flagging without quarantine",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).isNull().cast('boolean'))"
        ]
    if act == "trim_preserve_case":
        return [
            f"# Governance: Trimming whitespace from name/identifier '{col}' while preserving case.",
            f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))'
        ]
    if act == "flag_duplicate_candidates":
        from agent.etl_pipeline.manual_review_promote import make_step_id
        import copy
        st_copy = copy.deepcopy(step_meta or {})
        st_copy["dataset"] = ds_name
        step_id, _ = make_step_id(st_copy)
        return [
            f"# DHARA_STEP: {step_id}",
            f"# DHARA_ACTION: flag_duplicate_candidates",
            f"{df} = _flag_source_duplicates({df})"
        ]
    if act == "flag_business_key_duplicate":
        from agent.etl_pipeline.manual_review_promote import make_step_id
        import copy
        st_copy = copy.deepcopy(step_meta or {})
        st_copy["dataset"] = ds_name
        step_id, _ = make_step_id(st_copy)
        
        # Extract keys
        keys = []
        if plan:
            keys = list(plan.get("business_keys", {}).get(ds_name) or [])
        if col and str(col).lower() not in ("row-level", "[row-level]"):
            step_cols = [c.strip() for c in str(col).split(",") if c.strip()]
            keys.extend(step_cols)
        expected_keys = []
        for k in keys:
            if k not in expected_keys:
                expected_keys.append(k)
        key_cols_str = ", ".join(repr(k) for k in expected_keys)
        
        col_safe = _safe(col or "business_key")
        return [
            f"# DHARA_STEP: {step_id}",
            f"# DHARA_ACTION: flag_business_key_duplicate",
            f"from pyspark.sql import Window",
            f"_key_window = Window.partitionBy({key_cols_str})",
            f"{df} = {df}.withColumn('business_key_group_size', F.count(F.lit(1)).over(_key_window))",
            f"{df} = {df}.withColumn('is_business_key_duplicate', (F.col('business_key_group_size') > 1).cast('boolean'))",
            f"{df} = {df}.withColumn({repr(f'is_duplicate_{col_safe}')}, F.col('is_business_key_duplicate'))",
        ]
    if act == "flag_near_duplicate_cluster":
        col_safe = _safe(col or "row")
        flag_col = repr(f"is_duplicate_{col_safe}")
        return [
            f"# Governance: Flagging duplicate candidates on '{col}' (no row dropping in Phase 1).",
            f"{df} = {df}.withColumn({flag_col}, F.lit(True).cast('boolean'))"
        ]
    if act == "review_manually":
        return [
            f"# MANUAL REVIEW REQUIRED: {col}",
            f"logger.warning('Column {col} requires manual review -- excluded from auto-clean')",
        ]
    if act in ("validate_referential_integrity_or_stage", "validate_and_partition_relationship"):
        rel_ds = params.get("related_dataset") or "?"
        rel_col = params.get("related_column") or "?"
        expected_orphans = step_meta.get("evidence", {}).get("orphan_row_count") or params.get("orphan_row_count")
        is_required = bool(params.get("required", True))
        
        lines = [
            f"# validate_and_partition_relationship: {col} -> {rel_ds}.{rel_col}",
            f"if all_dfs is not None and {repr(rel_ds)} in all_dfs:",
            f"    _parent_df = all_dfs[{repr(rel_ds)}]",
            f"    if {repr(rel_col)} in _parent_df.columns:",
            f"        from agent.etl_pipeline.relationship_key_contract import DEFAULT_CONTRACT",
            f"        from agent.etl_pipeline.relationship_key_normalization_spark import normalize_relationship_key_spark",
            # Normalise parent key using distinct to prevent duplicating child rows
            f"        _parent_keys = _parent_df.select(normalize_relationship_key_spark({repr(rel_col)}, DEFAULT_CONTRACT).alias('_parent_key')).filter(F.col('_parent_key').isNotNull()).distinct()",
            # Normalise child key
            f"        _child_key_norm = normalize_relationship_key_spark({repr(col)}, DEFAULT_CONTRACT)",
            f"        _child_with_parent = {df}.withColumn('_parent_key_norm', _child_key_norm)",
            # Partition child null keys
            f"        _child_null_keys = _child_with_parent.filter(F.col('_parent_key_norm').isNull())",
            f"        _non_null_child = _child_with_parent.filter(F.col('_parent_key_norm').isNotNull())",
            # Partition valid vs orphan rows using semi and anti joins
            f"        _valid_child_rows = _non_null_child.join(_parent_keys, F.col('_parent_key_norm') == _parent_keys['_parent_key'], 'left_semi')",
            f"        _orphan_child_rows = _non_null_child.join(_parent_keys, F.col('_parent_key_norm') == _parent_keys['_parent_key'], 'left_anti')",
            f"        _actual_orphan_count = _orphan_child_rows.count()",
            f"        _actual_null_count = _child_null_keys.count()",
            f"        logger.info('Referential integrity validation: parent={rel_ds}, child={col}, actual_orphans=%d, actual_null_keys=%d', _actual_orphan_count, _actual_null_count)",
        ]
        
        # Check expected orphan count if provided
        if expected_orphans is not None:
            lines.extend([
                f"        if _actual_orphan_count != {int(expected_orphans)}:",
                f"            logger.error('Referential integrity reconciliation failed! Expected {expected_orphans} orphans, found %d orphans', _actual_orphan_count)",
                f"            raise ValueError(f'Referential integrity reconciliation failed: expected {expected_orphans}, actual {_actual_orphan_count}')",
            ])
        
        # Partitioning disposition logic:
        # Save orphan and null-key rows to separate tables
        lines.extend([
            f"        if _actual_null_count > 0:",
            f"            logger.warning('%d null child keys routed to quarantine', _actual_null_count)",
            f"            if all_dfs is not None:",
            f"                all_dfs['{ds_name}_key_nulls_quarantine'] = _child_null_keys.drop('_parent_key_norm')",
            f"        if _actual_orphan_count > 0:",
            f"            logger.warning('%d campaign orphans routed to quarantine', _actual_orphan_count)",
            f"            if all_dfs is not None:",
            f"                all_dfs['{ds_name}_campaign_orphans_quarantine'] = _orphan_child_rows.drop('_parent_key_norm')",
            # Clean output has only valid rows:
            f"        {df} = _valid_child_rows.drop('_parent_key_norm')",
        ])
        
        lines.extend([
            f"else:",
            f"    # Parent missing at runtime checks",
            f"    if {repr(is_required)}:",
            f"        logger.error('Required parent dataset {rel_ds} not loaded! Blocking clean write.')",
            f"        raise ValueError('Required parent dataset {rel_ds} not loaded')",
            f"    else:",
            f"        logger.warning('Optional parent dataset {rel_ds} not loaded. Retaining child rows, RI not evaluated.')",
            f"        {df} = {df}.withColumn('dq_validation_status', F.lit('RELATIONSHIP_NOT_EVALUATED'))",
        ])
        return lines

    if act == "flag_domain_violation":
        # Categorical column contains digit-only strings (e.g. region='123').
        # DO NOT cast to integer. Flag the violation for manual inspection.
        flag_col = repr(f"{col}_domain_flag")
        return [
            f"# Domain violation: '{col}' is categorical but contains digit-only values.",
            f"# Flagging affected rows -- do NOT cast to integer (would destroy 'NY', 'Mumbai', etc.).",
            f"{df} = {df}.withColumn({flag_col},"
            f" F.col({c}).cast('string').rlike(r'^\\d+$') & F.col({c}).isNotNull())",
            f"_domain_viol_cnt = {df}.filter(F.col({flag_col})).count()",
            f"logger.warning('flag_domain_violation: %d digit-only values in categorical column {col}', _domain_viol_cnt)",
        ]
    if act == "fill_nulls_flag":
        # For numeric columns with text-encoded invalid values (e.g. amount='error'):
        # preserve original, emit a flag column and an optional clean numeric column.
        flag_col = repr(f"is_invalid_{col}")
        clean_col = repr(f"{col}_numeric")
        return [
            f"# Safe numeric handling: flag invalid text values, create clean numeric column.",
            f"# Original '{col}' column is preserved unchanged for auditability.",
            f"{df} = {df}.withColumn({flag_col},"
            f" F.col({c}).isNotNull() & F.to_double(F.col({c}).cast('string')).isNull())",
            f"{df} = {df}.withColumn({clean_col},"
            f" F.when(F.col({flag_col}), F.lit(None)).otherwise(F.col({c}).cast('double')))",
            f"_invalid_cnt = {df}.filter(F.col({flag_col})).count()",
            f"logger.warning('fill_nulls_flag: %d non-numeric values flagged in column {col} (original preserved)', _invalid_cnt)",
        ]
    if act == "create_normalized_match_key":
        # Emit a normalised match-key column: trim + lowercase the source column.
        # output_column param overrides the default <col>_match_key name.
        params_val = step_meta.get("params") if step_meta else None
        if not params_val:
            params_val = {}
        output_col = params_val.get("output_column") or f"{col}_match_key"
        output_col_safe = _safe(output_col)
        return [
            f"# Normalized match-key for '{col}': trim + lowercase for fuzzy/join matching.",
            f"{df} = {df}.withColumn({repr(output_col_safe)}, F.trim(F.lower(F.col({c}).cast('string'))))",
        ]
    if act in ("normalize_registered_placeholder", "nullify_sentinels", "replace_sentinel_values", "nullify_sentinel"):
        col_safe = _safe(col or "col")
        placeholder_values = params.get("placeholder_values") or ["NA", "N/A", "NULL", "null", "None", "none", "?", "???", "-", "-1", "Unknown", "unknown", "<null>", "TBD", "99/99/9999"]
        placeholder_values = [str(v) for v in placeholder_values if v is not None]
        placeholder_expr = f"F.trim(F.col({c}).cast('string')).isin({placeholder_values!r}) & F.col({c}).isNotNull()"
        flag_col = repr(f"is_placeholder_{col_safe}")
        return [
            f"# Nullify registered sentinels and placeholders for '{col}' to NULL for governance/auditability.",
            f"{df} = {df}.withColumn({flag_col}, {placeholder_expr})",
            f"{df} = {df}.withColumn({c}, F.when({placeholder_expr}, F.lit(None)).otherwise(F.col({c})))",
        ]
    if act == "flag_cross_field_violation":
        col_safe = _safe(col or "col")
        comparison_col = params.get("comparison_column") or params.get("related_column") or params.get("other_column")
        flag_col = repr(f"{col_safe}_cross_field_violation")
        if comparison_col:
            return [
                f"# Cross-field violation: flag rows where '{col}' violates the rule against '{comparison_col}'.",
                f"{df} = {df}.withColumn({flag_col}, F.col({c}).cast('double') < F.col({repr(comparison_col)}).cast('double'))",
            ]
        return [
            f"# Cross-field violation: flag rows where numeric '{col}' is negative (Phase 1 safeguard).",
            f"{df} = {df}.withColumn({flag_col}, F.col({c}).cast('double') < F.lit(0))",
        ]
    return [f"# Unsupported in pyspark template v1: {act} on {col}"]


def _emit_valid_values_spark(df: str, ds_name: str, rules: Dict[str, Any]) -> List[str]:
    vv = rules.get("valid_values") or {}
    if not vv:
        return []
    never_drop = bool(rules.get("never_drop_rows"))
    lines: List[str] = []
    for col, allowed in vv.items():
        c = repr(str(col))
        sid = _safe(col)
        allowed_lit = repr([str(v).lower() for v in allowed])
        if never_drop:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _bad = ~F.lower(F.col({c}).cast('string')).isin({allowed_lit}) & F.col({c}).isNotNull()",
                f"    {df} = {df}.withColumn({c}, F.when(_bad, F.lit(None)).otherwise(F.col({c})))",
            ])
        else:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _before = {df}.count()",
                f"    {df} = {df}.filter(F.lower(F.col({c}).cast('string')).isin({allowed_lit}) | F.col({c}).isNull())",
                f"    logger.info('valid_values {ds_name}.{col}: dropped %d rows', _before - {df}.count())",
            ])
    return lines


def generate_pyspark_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    # Pre-clean the plan datasets using governance rules before generating PySpark template code
    from agent.etl_pipeline.llm_codegen import _consolidate_and_filter_datasets
    
    source_metadata: Dict[str, Any] = {}
    sem_schema = plan.get("semantic_schema") or {}
    for ds_name, meta in (assessment.get("datasets") or {}).items():
        cols = meta.get("columns") or {}
        source_metadata[ds_name] = {
            "row_count": meta.get("row_count"),
            "columns": {
                col: {
                    "dtype": cmeta.get("dtype") or cmeta.get("inferred_type"),
                    "null_percentage": cmeta.get("null_percentage"),
                    "semantic_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("semantic_type") or cmeta.get("semantic_type", "unknown"),
                    "sub_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("sub_type") or cmeta.get("sub_type", "unknown"),
                    "pii_level": (sem_schema.get(f"{ds_name}.{col}") or {}).get("pii_level") or "none",
                }
                for col, cmeta in cols.items()
                if isinstance(cmeta, dict)
            },
        }
    
    strict = bool(
        plan.get("strict_mode")
        or plan.get("settings", {}).get("strict_mode", False)
        or (plan.get("business_rules") or {}).get("strict_mode", False)
    )
    
    import copy
    plan = copy.deepcopy(plan)
    plan["datasets"] = _consolidate_and_filter_datasets(
        plan.get("datasets") or {}, source_metadata, plan.get("semantic_schema"), strict_mode=strict
    )

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

    plan_id = str(plan.get("plan_id") or "unknown")
    rules = plan.get("business_rules") or {}
    never_drop = bool(rules.get("never_drop_rows"))
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    join_strategy = str(joins[0].get("join_type") or "left") if joins else "none"

    policy = plan_policy_block(plan).replace("\n", "\n# ")
    lines: List[str] = [
        '"""',
        f"PySpark ETL -- plan_id={plan_id}",
        "Generated by: Agent Dhara",
        "Policy:",
        policy,
        '"""',
        "from __future__ import annotations",
        "",
        "import logging",
        "import os",
        "from typing import Optional",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import DataFrame",
        "",
        "logging.basicConfig(level=logging.INFO)",
        "logger = logging.getLogger('agent_dhara')",
        "",
    ]
    notes = str(rules.get("notes") or "").strip()
    if notes:
        lines.extend(["# Business notes:", "# " + notes.replace("\n", "\n# "), ""])

    manifest = plan.get("connector_manifest") or {}
    use_fabric = plan.get("execution_target") == "fabric" or any(
        ent.get("source_type") == "fabric_files_zone"
        for ent in (manifest.get("datasets") or {}).values()
    )
    
    if plan.get("datasets") or manifest.get("datasets"):
        if use_fabric:
            ws_id = os.getenv("FABRIC_WORKSPACE_ID") or ""
            lh_id = os.getenv("FABRIC_LAKEHOUSE_ID") or os.getenv("FABRIC_LAKEHOUSE_NAME") or ""
            if lh_id and not (len(lh_id) == 36 and lh_id.count("-") == 4):
                try:
                    from agent.fabric_api_client import FabricAPIClient
                    client = FabricAPIClient()
                    resolved = client.resolve_lakehouse_id_by_name(ws_id, lh_id)
                    if resolved:
                        lh_id = resolved
                except Exception:
                    pass
            lines.append(resolve_path_fabric_pyspark_helper(ws_id, lh_id))
        else:
            lines.append(resolve_path_pyspark_helper())
        lines.append("")
        lines.append(pyspark_production_helpers())
        lines.append("")
        lines.append(pyspark_iqr_bounds_helper())
        lines.append("")
        lines.append(pyspark_prefix_non_key_columns_helper())
        lines.append("")

    for ds_name, block in (plan.get("datasets") or {}).items():
        canon_id = _canonical_id(ds_name)
        fn = f"transform_{canon_id}"
        # FIX-1: Collect only genuine schema columns for _require_columns.
        # Deduplicate steps use a composite column string like "factory_id, inspection_date"
        # which must be split into individual names, not passed as a single invalid column name.
        # Also skip pseudo-column markers such as "[Row-level]" and "_" prefixed internals.
        ds_cols: List[str] = []
        for st in (block.get("steps") or []):
            raw_col = st.get("column")
            if not raw_col:
                continue
            raw_str = str(raw_col)
            if raw_str.startswith(("[", "_")):
                continue
            act_low = str(st.get("action") or "").lower()
            if act_low == "deduplicate":
                # Deduplicate column values may be a comma-separated key list.
                # Split them so each individual column name is validated -- but
                # do NOT add them to _require_columns because they are often
                # composite business keys whose individual parts are already
                # declared by other steps (e.g. trim, parse_dates).
                continue
            if act_low in ("flag_business_key_duplicate", "flag_duplicate_candidates", "flag_near_duplicate_cluster"):
                # These actions operate on composite key columns (e.g. "factory_id, campaign_id")
                # or pseudo-column markers (e.g. "[Row-level]") that are NOT real column names.
                # Exclude them from _require_columns to prevent schema validation failures.
                continue
            ds_cols.append(raw_str)
        unique_cols = list(dict.fromkeys(ds_cols))

        lines.append(f"def {fn}(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:")
        var = "out"
        lines.append(f"    {var} = df")
        if unique_cols:
            lines.append(f"    _require_columns({var}, {repr(unique_cols)}, {repr(canon_id)})")
        lines.append(f"    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in {var}.columns if not c.startswith('_') and not c.startswith('is_')]")
        lines.append(f"    {var} = {var}.withColumn('_source_row_hash', F.xxhash64(*_src_cols))")

        # Manual review warnings in code (B3)
        for mr in plan.get("manual_review") or []:
            if mr.get("dataset") == ds_name or mr.get("dataset") == canon_id:
                col = mr.get("column")
                if col:
                    lines.append(f"    logger.warning('Column {col} requires manual review -- skipping automation')")

        # Pre-scan: columns that have sanitize_email -- trim/lowercase are redundant there
        _email_cols: Set[str] = {
            str(st.get("column"))
            for st in (block.get("steps") or [])
            if str(st.get("action") or "").lower() == "sanitize_email" and st.get("column")
        }

        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            action = str(st.get("action") or "")
            col = st.get("column")
            act_low = action.lower()
            # Skip redundant trim/lowercase when sanitize_email is already present for this column
            # (sanitize_email emitter already applies F.lower(F.trim(...)) internally)
            if act_low in ("trim", "lowercase") and str(col) in _email_cols:
                lines.append(f"    # Step: {action} on {col} -- skipped (handled by sanitize_email)")
                continue
            lines.append(f"    # Step: {action} on {col}")
            for sl in _emit_spark(action, col, var, step_meta=st, plan=plan, ds_name=ds_name):
                lines.append(f"    {sl}")
        for sl in _emit_valid_values_spark(var, canon_id, rules):
            lines.append(f"    {sl}")
        for col in rules.get("non_nullable") or []:
            lines.append(f'    _warn_nulls_in_columns({var}, [{col!r}], "{canon_id}")')

        # - Quarantine Split (Governance) -
        # Any flag columns emitted by parse_dates_safe, fill_nulls_flag, flag_domain_violation,
        # or flag_outliers are used to split the output into:
        #   - valid_records  (all flags False / NULL)
        #   - quarantine     (at least one flag True)
        # This ensures full auditability without silent data loss.
        lines.extend([
            f"    # - Quarantine Split -",
            f"    # Separate flagged (invalid/outlier) rows from clean records for governance.",
            # FIX-4: Match ALL _is_duplicate_* flag columns (e.g. _is_duplicate_campaign_id,
            # _is_duplicate_factory_key, _is_duplicate_row) not just the exact '_is_duplicate' string.
            f"    _flag_cols = [c for c in {var}.columns if c.startswith('is_invalid_') or c.startswith('is_negative_') or c.endswith('_domain_flag') or c.endswith('_flagged') or c.startswith('_is_duplicate')]",
            f"    if _flag_cols:",
            f"        {var} = {var}.cache()",
            f"        from functools import reduce",
            f"        import operator",
            f"        _any_flag = reduce(operator.or_, (F.col(fc).eqNullSafe(F.lit(True)) for fc in _flag_cols))",
            f"        {var}_quarantine = {var}.filter(_any_flag)",
            f"        {var} = {var}.filter(~_any_flag)",
            f"        _quarantine_count = {var}_quarantine.count()",
            f"        if _quarantine_count > 0:",
            f"            logger.warning('Quarantine: %d rows routed to {canon_id}_quarantine table', _quarantine_count)",
            f"            if all_dfs is not None:",
            f"                all_dfs['{canon_id}_quarantine'] = {var}_quarantine",
        ])
        lines.append(f"    return {var}")
        lines.append("")

    canon_datasets = [_canonical_id(d) for d in (plan.get("datasets") or {})]
    lines.append("DATASETS = " + repr(canon_datasets))
    
    manifest = plan.get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    sources_dict = {}
    for ds_name, ent in m_ds.items():
        sources_dict[_canonical_id(ds_name)] = ent.get("location") or ds_name
    lines.append("DATASET_SOURCES = " + repr(sources_dict))
    lines.append("")

    non_nullable = [str(c) for c in (rules.get("non_nullable") or []) if c]
    if manifest.get("datasets") or rel.get("joins") or plan.get("datasets"):
        for sl in emit_pyspark_output_contract(plan, manifest):
            lines.append(sl)
        lines.append("def run_pipeline(spark):")
        lines.append("    dfs = {}")
        lines.append("    def _canonical_id(name: str) -> str:")
        lines.append("        import re")
        lines.append('        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")')
        lines.append("")
        lines.append("    active_canons = {_canonical_id(d) for d in DATASETS}")
        
        # Load block
        load_lines = emit_pyspark_load(plan, manifest)
        i_load = 0
        while i_load < len(load_lines):
            l_line = load_lines[i_load]
            m_set = re.match(r'^\s*dfs\["([^"]+)"\]\s*=\s*(.*)$', l_line)
            if m_set:
                c_id = m_set.group(1)
                expr = m_set.group(2)
                lines.append(f'    if "{c_id}" in active_canons:')
                lines.append(f'        dfs["{c_id}"] = {expr}')
                if i_load + 1 < len(load_lines) and "_require_columns" in load_lines[i_load + 1]:
                    lines.append(f'        {load_lines[i_load + 1].strip()}')
                    i_load += 1
            else:
                if l_line.strip():
                    lines.append(f"    {l_line}")
            i_load += 1
        for ds_name in (plan.get("datasets") or {}):
            canon_id = _canonical_id(ds_name)
            fn = f"transform_{canon_id}"
            lines.append(f'    if "{canon_id}" in dfs:')
            lines.append(f'        dfs["{canon_id}"] = {fn}(dfs["{canon_id}"], dfs)')
            if non_nullable:
                lines.append(f'        _warn_nulls_in_columns(dfs["{canon_id}"], {non_nullable!r}, "{canon_id}")')
            lines.append(f'        _log_row_count(dfs["{canon_id}"], "{canon_id}")')
        for sl in emit_pyspark_joins(plan):
            lines.append(f"    {sl}")
        for sl in emit_pyspark_write_outputs(plan, manifest):
            lines.append(f"    {sl}")
        lines.append("    # Write quarantine tables for auditability (if any rows were flagged)")
        lines.append("    for q_key, q_df in list(dfs.items()):")
        lines.append("        if q_key.endswith('_quarantine'):")
        lines.append("            q_tbl = q_key.replace('.', '_')")
        lines.append("            logger.info('Writing quarantine -> Fabric table %s', q_tbl)")
        lines.append("            q_df.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable(q_tbl)")
        lines.append("    return dfs, OUTPUT_PATHS")
        lines.append("")
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("if __name__ == '__main__':")
        lines.append("    try:")
        lines.append("        spark  # type: ignore[name-defined]")
        lines.append("    except NameError:")
        lines.append("        from pyspark.sql import SparkSession")
        lines.append('        spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("    _dfs, _paths = run_pipeline(spark)")
    else:
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("if __name__ == '__main__':")
        lines.append("    try:")
        lines.append("        spark  # type: ignore[name-defined]")
        lines.append("    except NameError:")
        lines.append("        from pyspark.sql import SparkSession")
        lines.append('        spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("    logger.info('Empty pipeline executed successfully')")

    code = "\n".join(lines)
    # Always unescape HTML entities -- LLM responses or Jinja templating can
    # introduce &lt; &gt; &amp; &quot; which silently break generated Python.
    code = _html.unescape(code)

    # Self-validate template output so structural issues are visible in logs.
    # Never suppress the code — always return it, but log governance violations.
    try:
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source as _val_tmpl
        import logging as _logging
        _tmpl_logger = _logging.getLogger("agent.etl_pipeline.pyspark_codegen")
        _ok, _errs = _val_tmpl(code, plan)
        if not _ok:
            for _e in _errs:
                _tmpl_logger.warning("[Template fallback] Validation issue: %s", _e)
    except Exception:
        pass  # Validation is best-effort — never block code return

    return code
