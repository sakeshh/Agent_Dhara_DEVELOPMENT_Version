"""
composite_field_compiler.py — Consolidates multiple findings on a single column
into a unified FieldRemediationWorkPackage and compiles a single, conflict-free,
canonically-ordered field pipeline with mutually exclusive row routing (clean XOR quarantine).
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Tuple

from agent.etl_pipeline.remediation_models import (
    FieldRemediationWorkPackage,
    GeneratedRemediationArtifact,
    ResolvedRemediationAction,
    resolve_remediation_action,
    detect_field_action_conflicts,
    validate_type_compatibility,
    ActionCodeMismatchError,
    ActionSemanticMismatch,
)
from agent.dq_recommendations_agent import ACTION_OUTCOME_METADATA


# Canonical operation execution order
CANONICAL_OPERATION_ORDER = [
    "preserve_raw",
    "trim",
    "normalize_registered_placeholder",
    "nullify_punctuation",
    "lowercase",
    "uppercase",
    "sanitize_email",
    "normalize_phone",
    "parse_dates",
    "coerce_numeric",
    "standardize_boolean",
    "flag_only",
    "nullify_clean_value",
    "quarantine_row",
]


def create_field_work_package(
    dataset_id: str,
    column_name: str,
    findings: List[Dict[str, Any]],
    semantic_type: Optional[str] = None,
    default_approval_status: Optional[str] = None,
) -> FieldRemediationWorkPackage:
    """Consolidate multiple findings on a single field into an ordered, validated work package."""
    wp_id = f"wp-{dataset_id}-{column_name}".lower()

    # Resolve actions for all findings
    resolved_actions: List[ResolvedRemediationAction] = []
    for f in findings:
        appr = f.get("approval_status")
        if not appr and isinstance(f.get("lifecycle"), dict):
            appr = f.get("lifecycle", {}).get("approval_status")
        if not appr and default_approval_status:
            appr = default_approval_status
        resolved = resolve_remediation_action(
            finding=f,
            approval_status=appr,
        )
        resolved_actions.append(resolved)

    # Detect conflicts among actions
    detect_field_action_conflicts(resolved_actions)

    # Determine execution ordering
    action_set = {a.action_id for a in resolved_actions}
    ordered_transforms = [
        op for op in CANONICAL_OPERATION_ORDER if op in action_set
    ]
    # Any other actions not explicitly in canonical list append at end
    for a_id in action_set:
        if a_id not in ordered_transforms and a_id not in ("keep_as_is", "skip_etl", "noop"):
            ordered_transforms.append(a_id)

    has_quarantine = any(a.quarantines_row for a in resolved_actions)
    has_nullification = any(a.nullifies_clean_value for a in resolved_actions)
    is_provisional = any(a.approval_status == "pending" for a in resolved_actions)

    return FieldRemediationWorkPackage(
        work_package_id=wp_id,
        dataset_id=dataset_id,
        column_name=column_name,
        semantic_type=semantic_type,
        findings=findings,
        resolved_actions=resolved_actions,
        ordered_transforms=ordered_transforms,
        final_disposition_policy="clean_xor_quarantine",
        preserves_raw=True,
        has_quarantine=has_quarantine,
        has_nullification=has_nullification,
        is_provisional=is_provisional,
    )


def compile_field_work_package(
    work_package: FieldRemediationWorkPackage,
    dialect: str = "pyspark",
) -> GeneratedRemediationArtifact:
    """
    Compile a FieldRemediationWorkPackage into a single consolidated executable artifact.
    Guarantees:
    1. Preserves raw column once: <col>_raw = <col>
    2. Executes transformations in strict canonical order
    3. Aggregates status codes and reasons without collision
    4. Mutually exclusive row disposition: clean XOR quarantine
    """
    col = work_package.column_name
    ds = work_package.dataset_id

    # If all actions are no-op, emit no code
    action_ids = {a.action_id for a in work_package.resolved_actions}
    if action_ids.issubset({"keep_as_is", "skip_etl", "noop"}):
        return GeneratedRemediationArtifact(
            action_id="keep_as_is",
            dialect=dialect,
            code=None,
            outcome_metadata=ACTION_OUTCOME_METADATA["keep_as_is"],
            validation_status="not_started",
            is_provisional=work_package.is_provisional,
            artifact_id=f"art-{work_package.work_package_id}-keep_as_is",
            code_hash=None,
            artifact_status="provisional" if work_package.is_provisional else "generated",
        )

    # Validate type compatibility
    for action in work_package.resolved_actions:
        if action.action_id == "parse_dates":
            validate_type_compatibility(work_package.semantic_type, action.action_id, "date")
        elif action.action_id in ("sanitize_email", "normalize_phone"):
            validate_type_compatibility(work_package.semantic_type, action.action_id, "string")

    sem = (work_package.semantic_type or "").lower()
    has_quarantine = work_package.has_quarantine
    has_nullify = work_package.has_nullification

    # 1. Email branch
    if "email" in sem or "email" in col.lower():
        if dialect == "pyspark":
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim -> Null/Placeholder -> Email Validation)\n"
                f"raw = F.col('{col}')\n"
                f"trimmed = F.lower(F.trim(raw.cast('string')))\n"
                f"is_null = raw.isNull() | (trimmed == '')\n"
                f"is_placeholder = trimmed.isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_valid_email = trimmed.rlike(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$')\n"
                f"is_syntax_error = ~is_null & ~is_placeholder & ~is_valid_email\n"
                f"compound_err = is_placeholder | is_syntax_error\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', F.when(compound_err, None).otherwise(trimmed))\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_placeholder, 'placeholder_normalized_to_null')\n"
                f"         .when(is_syntax_error, 'invalid_email_format')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_placeholder, 'Configured placeholder token')\n"
                f"         .when(is_syntax_error, 'Email format violation')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df = df.withColumn('quarantine_reason', F.when(compound_err, 'Email validation violation').otherwise(None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df.filter(~compound_err)\n"
                    f"quarantine_df = df.filter(compound_err)"
                )
            else:
                code += "clean_df = df\nquarantine_df = df.limit(0)"
        elif dialect in ("tsql", "sql"):
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> placeholder_normalized_to_null -> invalid_email_format -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN NULL\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN NULL\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%_@_%._%' THEN NULL\n"
                f"        ELSE LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))))\n"
                f"    END,\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN 'source_null'\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'placeholder_normalized_to_null'\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%_@_%._%' THEN 'invalid_email_format'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'Configured placeholder token'\n"
                f"        WHEN [{col}] IS NOT NULL AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) <> '' AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%_@_%._%' THEN 'Email format violation'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim -> Null/Placeholder -> Email Validation)\n"
                f"raw = df['{col}']\n"
                f"trimmed = raw.astype('string').str.strip().str.lower()\n"
                f"is_null = raw.isna() | (trimmed == '')\n"
                f"is_placeholder = trimmed.isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_valid_email = trimmed.str.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$').fillna(False)\n"
                f"is_syntax_error = ~is_null & ~is_placeholder & ~is_valid_email\n"
                f"compound_err = is_placeholder | is_syntax_error\n"
                f"\n"
                f"df['{col}_raw'] = raw\n"
                f"df['{col}_clean'] = trimmed.where(~compound_err, None)\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_placeholder, is_syntax_error],\n"
                f"    ['source_null', 'placeholder_normalized_to_null', 'invalid_email_format'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_placeholder, is_syntax_error],\n"
                f"    ['Configured placeholder token', 'Email format violation'],\n"
                f"    default=None\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df['quarantine_reason'] = np.where(compound_err, 'Email validation violation', None)\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df[~compound_err].copy()\n"
                    f"quarantine_df = df[compound_err].copy()"
                )
            else:
                code += "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"

    # 2. Date / Datetime branch
    elif "date" in sem or any(k in col.lower() for k in ["date", "time", "created", "updated"]) or any("date" in a.action_id for a in work_package.resolved_actions):
        if dialect == "pyspark":
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim -> Placeholder/Sentinel -> Date Validation)\n"
                f"raw = F.col('{col}')\n"
                f"trimmed = F.trim(raw.cast('string'))\n"
                f"is_null = raw.isNull() | (trimmed == '')\n"
                f"is_placeholder = F.lower(trimmed).isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_sentinel = trimmed.isin(['9999-12-31', '1900-01-01', '9999-01-01', '1970-01-01'])\n"
                f"parsed_dt = F.coalesce(\n"
                f"    F.to_timestamp(trimmed, 'yyyy-MM-dd HH:mm:ss'),\n"
                f"    F.to_timestamp(trimmed, 'yyyy-MM-dd'),\n"
                f"    F.to_timestamp(trimmed, 'dd/MM/yyyy'),\n"
                f"    F.to_timestamp(trimmed, 'MM/dd/yyyy')\n"
                f")\n"
                f"is_impossible = ~is_null & ~is_placeholder & ~is_sentinel & parsed_dt.isNull()\n"
                f"compound_err = is_placeholder | is_sentinel | is_impossible\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', F.when(compound_err, None).otherwise(parsed_dt.cast('date')))\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_placeholder, 'placeholder_normalized_to_null')\n"
                f"         .when(is_sentinel, 'sentinel_date')\n"
                f"         .when(is_impossible, 'impossible_date')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_placeholder, 'Configured date placeholder token')\n"
                f"         .when(is_sentinel, 'Configured sentinel date')\n"
                f"         .when(is_impossible, 'Calendar impossible date')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df = df.withColumn('quarantine_reason', F.when(is_impossible, 'Calendar impossible date').otherwise(None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df.filter(~compound_err)\n"
                    f"quarantine_df = df.filter(compound_err)"
                )
            else:
                code += "clean_df = df\nquarantine_df = df.limit(0)"
        elif dialect in ("tsql", "sql"):
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> placeholder_normalized_to_null -> sentinel_date -> impossible_date -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN NULL\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN NULL\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) IN ('9999-12-31', '1900-01-01', '9999-01-01', '1970-01-01') THEN NULL\n"
                f"        ELSE COALESCE(TRY_CONVERT(DATETIME, [{col}], 120), TRY_CONVERT(DATETIME, [{col}], 103), TRY_CONVERT(DATETIME, [{col}], 101), TRY_CONVERT(DATETIME, [{col}], 111))\n"
                f"    END,\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN 'source_null'\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'placeholder_normalized_to_null'\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) IN ('9999-12-31', '1900-01-01', '9999-01-01', '1970-01-01') THEN 'sentinel_date'\n"
                f"        WHEN COALESCE(TRY_CONVERT(DATETIME, [{col}], 120), TRY_CONVERT(DATETIME, [{col}], 103), TRY_CONVERT(DATETIME, [{col}], 101), TRY_CONVERT(DATETIME, [{col}], 111)) IS NULL THEN 'impossible_date'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'Configured date placeholder token'\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) IN ('9999-12-31', '1900-01-01', '9999-01-01', '1970-01-01') THEN 'Configured sentinel date'\n"
                f"        WHEN [{col}] IS NOT NULL AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) <> '' AND COALESCE(TRY_CONVERT(DATETIME, [{col}], 120), TRY_CONVERT(DATETIME, [{col}], 103), TRY_CONVERT(DATETIME, [{col}], 101), TRY_CONVERT(DATETIME, [{col}], 111)) IS NULL THEN 'Calendar impossible date'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim -> Placeholder/Sentinel -> Date Validation)\n"
                f"raw = df['{col}']\n"
                f"str_val = raw.astype('string').str.strip()\n"
                f"is_null = raw.isna() | (str_val == '')\n"
                f"is_placeholder = str_val.str.lower().isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_sentinel = str_val.isin(['9999-12-31', '1900-01-01', '9999-01-01', '1970-01-01'])\n"
                f"parsed_date = pd.to_datetime(str_val, errors='coerce')\n"
                f"is_impossible = ~is_null & ~is_placeholder & ~is_sentinel & parsed_date.isna()\n"
                f"compound_err = is_placeholder | is_sentinel | is_impossible\n"
                f"\n"
                f"df['{col}_raw'] = raw\n"
                f"df['{col}_clean'] = pd.Series(np.where(~compound_err & ~is_null, parsed_date.dt.date, None), index=df.index)\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_placeholder, is_sentinel, is_impossible],\n"
                f"    ['source_null', 'placeholder_normalized_to_null', 'sentinel_date', 'impossible_date'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_placeholder, is_sentinel, is_impossible],\n"
                f"    ['Configured date placeholder token', 'Configured sentinel date', 'Calendar impossible date'],\n"
                f"    default=None\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df['quarantine_reason'] = np.where(is_impossible, 'Calendar impossible date', None)\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df[~compound_err].copy()\n"
                    f"quarantine_df = df[compound_err].copy()"
                )
            else:
                code += "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"

    # 3. Age and Integer-semantic numeric fields
    elif "age" in col.lower() or ("int" in sem and not any(k in col.lower() for k in ["id", "key", "code"])):
        is_flag_only = (
            "flag_only" in work_package.ordered_transforms 
            or any("flag" in a.action_id for a in work_package.resolved_actions)
            or not work_package.has_nullification
        )
        if dialect == "pyspark":
            clean_expr = "num_val.cast('bigint')" if is_flag_only else "F.when(compound_err, None).otherwise(num_val.cast('bigint'))"
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Parse Numeric -> Range/Integer Validation)\n"
                f"raw = F.col('{col}')\n"
                f"num_val = raw.cast('double')\n"
                f"is_null = raw.isNull()\n"
                f"is_unparseable = raw.isNotNull() & num_val.isNull()\n"
                f"is_fractional = num_val.isNotNull() & (num_val % 1 != 0)\n"
                f"is_negative = num_val.isNotNull() & (num_val < 0)\n"
                f"is_implausible = num_val.isNotNull() & (num_val > 150)\n"
                f"compound_err = is_unparseable | is_fractional | is_negative | is_implausible\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', {clean_expr})\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_unparseable, 'unparseable_numeric')\n"
                f"         .when(is_fractional, 'fractional_integer_anomaly')\n"
                f"         .when(is_negative, 'negative_value_review_required')\n"
                f"         .when(is_implausible, 'implausible_age')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_unparseable, 'Value cannot be parsed as numeric')\n"
                f"         .when(is_fractional, 'Floating point representation of integer age')\n"
                f"         .when(is_negative, 'Age cannot be negative')\n"
                f"         .when(is_implausible, 'Age out of plausible range [0, 150]')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df = df.withColumn('quarantine_reason', F.when(is_implausible, 'Age out of plausible range [0, 150]').otherwise(None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df.filter(~compound_err)\n"
                    f"quarantine_df = df.filter(compound_err)"
                )
            else:
                code += "clean_df = df\nquarantine_df = df.limit(0)"
        elif dialect in ("tsql", "sql"):
            clean_tsql_expr = "CAST(TRY_CAST([{col}] AS DECIMAL(18,4)) AS INT)" if is_flag_only else "CASE WHEN [{col}] IS NULL OR TRY_CAST([{col}] AS DECIMAL(18,4)) IS NULL OR TRY_CAST([{col}] AS DECIMAL(18,4)) <> FLOOR(TRY_CAST([{col}] AS DECIMAL(18,4))) OR TRY_CAST([{col}] AS DECIMAL(18,4)) < 0 OR TRY_CAST([{col}] AS DECIMAL(18,4)) > 150 THEN NULL ELSE CAST(TRY_CAST([{col}] AS DECIMAL(18,4)) AS INT) END"
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> unparseable_numeric -> fractional_integer_anomaly -> negative_value -> implausible_age -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = {clean_tsql_expr},\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL THEN 'source_null'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) IS NULL THEN 'unparseable_numeric'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) <> FLOOR(TRY_CAST([{col}] AS DECIMAL(18,4))) THEN 'fractional_integer_anomaly'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) < 0 THEN 'negative_value_review_required'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) > 150 THEN 'implausible_age'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN [{col}] IS NOT NULL AND TRY_CAST([{col}] AS DECIMAL(18,4)) IS NULL THEN 'Value cannot be parsed as numeric'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) <> FLOOR(TRY_CAST([{col}] AS DECIMAL(18,4))) THEN 'Floating point representation of integer age'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) < 0 THEN 'Age cannot be negative'\n"
                f"        WHEN TRY_CAST([{col}] AS DECIMAL(18,4)) > 150 THEN 'Age out of plausible range [0, 150]'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            clean_pandas_expr = "clean_int" if is_flag_only else "clean_int.where(~compound_err & ~is_null, None)"
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Parse Numeric -> Range/Integer Validation)\n"
                f"raw_val = df['{col}']\n"
                f"num_val = pd.to_numeric(raw_val, errors='coerce')\n"
                f"is_null = raw_val.isna()\n"
                f"is_unparseable = raw_val.notna() & num_val.isna()\n"
                f"is_fractional = num_val.notna() & (num_val % 1 != 0)\n"
                f"is_negative = num_val.notna() & (num_val < 0)\n"
                f"is_implausible = num_val.notna() & (num_val > 150)\n"
                f"compound_err = is_unparseable | is_fractional | is_negative | is_implausible\n"
                f"# Convert integer-equivalent values to Int64, masking fractional or invalid entries safely\n"
                f"is_int_candidate = num_val.notna() & ~is_fractional & ~is_negative & ~is_implausible\n"
                f"clean_int = num_val.where(is_int_candidate).astype('Int64')\n"
                f"\n"
                f"df['{col}_raw'] = raw_val\n"
                f"df['{col}_clean'] = {clean_pandas_expr}\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_unparseable, is_fractional, is_negative, is_implausible],\n"
                f"    ['source_null', 'unparseable_numeric', 'fractional_integer_anomaly', 'negative_value_review_required', 'implausible_age'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_unparseable, is_fractional, is_negative, is_implausible],\n"
                f"    ['Value cannot be parsed as numeric', 'Floating point representation of integer age', 'Age cannot be negative', 'Age out of plausible range [0, 150]'],\n"
                f"    default=None\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df['quarantine_reason'] = np.where(is_implausible, 'Age out of plausible range [0, 150]', None)\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df[~compound_err].copy()\n"
                    f"quarantine_df = df[compound_err].copy()"
                )
            else:
                code += "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"

    # 4. Phone fields
    elif "phone" in col.lower() or "tel" in col.lower() or "mobile" in col.lower() or any("phone" in a.action_id for a in work_package.resolved_actions):
        if dialect == "pyspark":
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Strip Symbols -> Phone Validation)\n" \
                f"# Configured valid digit length: 7 to 15 (Source: rule_registry/phone_v1.0)\n" \
                f"# Configured valid digit length: 7 to 15 (Source: rule_registry/phone_v1.0)\n"
                f"raw = F.col('{col}')\n"
                f"digits = F.regexp_replace(raw.cast('string'), r'[^0-9]', '')\n"
                f"trimmed = F.trim(raw.cast('string'))\n"
                f"is_null = raw.isNull() | (trimmed == '')\n"
                f"is_placeholder = F.lower(trimmed).isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-', '0000000000', '9999999999'])\n"
                f"is_invalid_phone = ~is_null & ~is_placeholder & ((F.length(digits) < 7) | (F.length(digits) > 15))\n"
                f"compound_err = is_placeholder | is_invalid_phone\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', F.when(compound_err, None).otherwise(digits))\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_placeholder, 'placeholder_normalized_to_null')\n"
                f"         .when(is_invalid_phone, 'invalid_phone_format')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_placeholder, 'Configured phone placeholder')\n"
                f"         .when(is_invalid_phone, 'Phone number format or length violation')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df = df.withColumn('quarantine_reason', F.when(compound_err, 'Phone format violation').otherwise(None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df.filter(~compound_err)\n"
                    f"quarantine_df = df.filter(compound_err)"
                )
            else:
                code += "clean_df = df\nquarantine_df = df.limit(0)"
        elif dialect in ("tsql", "sql"):
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> placeholder_normalized_to_null -> invalid_phone_format -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN NULL\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-', '0000000000', '9999999999') THEN NULL\n"
                f"        WHEN LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '')) NOT BETWEEN 7 AND 15\n"
                f"             OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '') LIKE '%[^0-9]%' THEN NULL\n"
                f"        ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '')\n"
                f"    END,\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN 'source_null'\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-', '0000000000', '9999999999') THEN 'placeholder_normalized_to_null'\n"
                f"        WHEN LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '')) NOT BETWEEN 7 AND 15\n"
                f"             OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '') LIKE '%[^0-9]%' THEN 'invalid_phone_format'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-', '0000000000', '9999999999') THEN 'Configured phone placeholder'\n"
                f"        WHEN [{col}] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '')) NOT BETWEEN 7 AND 15 OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '-', ''), ' ', ''), '(', ''), ')', ''), '.', '') LIKE '%[^0-9]%') THEN 'Phone number length must be between 7 and 15 digits with numeric characters only'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Strip Symbols -> Phone Validation)\n" \
                f"# Configured valid digit length: 7 to 15 (Source: rule_registry/phone_v1.0)\n" \
                f"# Configured valid digit length: 7 to 15 (Source: rule_registry/phone_v1.0)\n"
                f"raw = df['{col}']\n"
                f"str_val = raw.astype('string').str.strip()\n"
                f"digits = str_val.str.replace(r'[^0-9]', '', regex=True)\n"
                f"is_null = raw.isna() | (str_val == '')\n"
                f"is_placeholder = str_val.str.lower().isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-', '0000000000', '9999999999'])\n"
                f"is_invalid_phone = ~is_null & ~is_placeholder & ((digits.str.len() < 7) | (digits.str.len() > 15))\n"
                f"compound_err = is_placeholder | is_invalid_phone\n"
                f"\n"
                f"df['{col}_raw'] = raw\n"
                f"df['{col}_clean'] = digits.where(~compound_err & ~is_null, None)\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_placeholder, is_invalid_phone],\n"
                f"    ['source_null', 'placeholder_normalized_to_null', 'invalid_phone_format'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_placeholder, is_invalid_phone],\n"
                f"    ['Configured phone placeholder', 'Phone number format or length violation'],\n"
                f"    default=None\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df['quarantine_reason'] = np.where(compound_err, 'Phone format violation', None)\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df[~compound_err].copy()\n"
                    f"quarantine_df = df[compound_err].copy()"
                )
            else:
                code += "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"

    # 5. Sentinel identifier fields
    elif any("sentinel" in a.action_id for a in work_package.resolved_actions) or any(k in col.lower() for k in ["instructorid", "sentinel"]):
        if dialect == "pyspark":
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Sentinel Nullification)\n"
                f"raw = F.col('{col}')\n"
                f"is_null = raw.isNull()\n"
                f"is_sentinel = raw.isin([999, -999, 999999, 9999999]) | F.lower(raw.cast('string')).isin(['999', '-999', '999999', '9999999', 'unknown', 'none'])\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', F.when(is_sentinel, None).otherwise(raw))\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_sentinel, 'sentinel_value')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_sentinel, 'Configured sentinel identifier')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
                f"clean_df = df\n"
                f"quarantine_df = df.limit(0)"
            )
        elif dialect in ("tsql", "sql"):
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> sentinel_value -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = CASE\n"
                f"        WHEN [{col}] IS NULL THEN NULL\n"
                f"        WHEN [{col}] IN (999, -999, 999999, 9999999) OR CAST([{col}] AS NVARCHAR(MAX)) IN ('999', '-999', '999999', '9999999') THEN NULL\n"
                f"        ELSE [{col}]\n"
                f"    END,\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL THEN 'source_null'\n"
                f"        WHEN [{col}] IN (999, -999, 999999, 9999999) OR CAST([{col}] AS NVARCHAR(MAX)) IN ('999', '-999', '999999', '9999999') THEN 'sentinel_value'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN [{col}] IN (999, -999, 999999, 9999999) OR CAST([{col}] AS NVARCHAR(MAX)) IN ('999', '-999', '999999', '9999999') THEN 'Configured sentinel identifier'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Sentinel Nullification)\n"
                f"raw = df['{col}']\n"
                f"is_null = raw.isna()\n"
                f"is_sentinel = raw.isin([999, -999, 999999, 9999999]) | raw.astype('string').isin(['999', '-999', '999999', '9999999'])\n"
                f"\n"
                f"df['{col}_raw'] = raw\n"
                f"df['{col}_clean'] = raw.where(~is_sentinel, None)\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_sentinel],\n"
                f"    ['source_null', 'sentinel_value'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_sentinel],\n"
                f"    ['Configured sentinel identifier'],\n"
                f"    default=None\n"
                f")\n"
                f"clean_df = df.copy()\n"
                f"quarantine_df = df.iloc[0:0].copy()"
            )

    # 6. Price / numeric financial branch
    elif any(k in col.lower() for k in ["price", "cost", "amount", "revenue", "qty"]):
        is_flag_only = (
            "flag_only" in work_package.ordered_transforms 
            or any("flag" in a.action_id for a in work_package.resolved_actions)
            or not work_package.has_nullification
        )
        if dialect == "pyspark":
            clean_pyspark_expr = "num_val" if is_flag_only else "F.when(compound_err, None).otherwise(num_val)"
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Parse Numeric -> Isolate Negative -> Validate)\n"
                f"raw_val = F.col('{col}')\n"
                f"num_val = raw_val.cast('double')\n"
                f"is_null = raw_val.isNull()\n"
                f"is_unparseable = raw_val.isNotNull() & num_val.isNull()\n"
                f"is_negative = num_val.isNotNull() & (num_val < 0)\n"
                f"compound_err = is_unparseable | is_negative\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw_val)\n"
                f"    .withColumn('{col}_clean', {clean_pyspark_expr})\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_unparseable, 'unparseable_numeric')\n"
                f"         .when(is_negative, 'negative_value_review_required')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_unparseable, 'Value cannot be parsed as decimal')\n"
                f"         .when(is_negative, '{col} cannot be negative')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df = df.withColumn('quarantine_reason', F.when(is_negative, '{col} cannot be negative').when(is_unparseable, 'Unparseable numeric value').otherwise(None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df.filter(~compound_err)\n"
                    f"quarantine_df = df.filter(compound_err)"
                )
            else:
                code += "clean_df = df\nquarantine_df = df.limit(0)"
        elif dialect in ("tsql", "sql"):
            target_cast = "DECIMAL(18,2)" if any(k in col.lower() for k in ["price", "cost", "amount", "revenue"]) else "INT"
            clean_tsql_val = f"TRY_CAST([{col}] AS {target_cast})" if is_flag_only else f"CASE WHEN TRY_CAST([{col}] AS {target_cast}) IS NULL OR TRY_CAST([{col}] AS {target_cast}) < 0 THEN NULL ELSE TRY_CAST([{col}] AS {target_cast}) END"
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> unparseable_numeric -> negative_value -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = {clean_tsql_val},\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL THEN 'source_null'\n"
                f"        WHEN TRY_CAST([{col}] AS {target_cast}) IS NULL THEN 'unparseable_numeric'\n"
                f"        WHEN TRY_CAST([{col}] AS {target_cast}) < 0 THEN 'negative_value_review_required'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN [{col}] IS NOT NULL AND TRY_CAST([{col}] AS {target_cast}) IS NULL THEN 'Value cannot be parsed as numeric'\n"
                f"        WHEN TRY_CAST([{col}] AS {target_cast}) < 0 THEN '{col} cannot be negative'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            clean_pandas_expr = "num_val" if is_flag_only else "num_val.where(~compound_err, None)"
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Parse Numeric -> Isolate Negative -> Validate)\n"
                f"raw_val = df['{col}']\n"
                f"num_val = pd.to_numeric(raw_val, errors='coerce')\n"
                f"is_null = raw_val.isna()\n"
                f"is_unparseable = raw_val.notna() & num_val.isna()\n"
                f"is_negative = num_val.notna() & (num_val < 0)\n"
                f"compound_err = is_unparseable | is_negative\n"
                f"\n"
                f"df['{col}_raw'] = raw_val\n"
                f"df['{col}_clean'] = {clean_pandas_expr}\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_unparseable, is_negative],\n"
                f"    ['source_null', 'unparseable_numeric', 'negative_value_review_required'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_unparseable, is_negative],\n"
                f"    ['Value cannot be parsed as decimal', '{col} cannot be negative'],\n"
                f"    default=None\n"
                f")\n"
            )
            if has_quarantine:
                code += (
                    f"df['quarantine_reason'] = np.where(is_negative, '{col} cannot be negative', np.where(is_unparseable, 'Unparseable numeric value', None))\n"
                    f"# Mutually exclusive row routing: clean XOR quarantine\n"
                    f"clean_df = df[~compound_err].copy()\n"
                    f"quarantine_df = df[compound_err].copy()"
                )
            else:
                code += "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"

    # 7. Text / general branch (CustomerName, City, etc.)
    else:
        if dialect == "pyspark":
            code = (
                f"import pyspark.sql.functions as F\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim/Clean Spaces -> Placeholder/Punctuation Validation)\n"
                f"raw = F.col('{col}')\n"
                f"trimmed = F.regexp_replace(F.trim(raw.cast('string')), r'\\s+', ' ')\n"
                f"is_null = raw.isNull() | (trimmed == '')\n"
                f"is_placeholder = F.lower(trimmed).isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_punct_only = ~is_null & ~is_placeholder & trimmed.rlike(r'^[^a-zA-Z0-9]+$')\n"
                f"compound_err = is_placeholder | is_punct_only\n"
                f"\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col}_raw', raw)\n"
                f"    .withColumn('{col}_clean', F.when(compound_err, None).otherwise(trimmed))\n"
                f"    .withColumn('{col}_validation_status',\n"
                f"        F.when(is_null, 'source_null')\n"
                f"         .when(is_placeholder, 'placeholder_normalized_to_null')\n"
                f"         .when(is_punct_only, 'punctuation_only_normalized_to_null')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col}_validation_reason',\n"
                f"        F.when(is_placeholder, 'Configured text placeholder token')\n"
                f"         .when(is_punct_only, 'Punctuation-only string anomaly')\n"
                f"         .otherwise(None)\n"
                f"    )\n"
                f")\n"
                f"clean_df = df\n"
                f"quarantine_df = df.limit(0)"
            )
        elif dialect in ("tsql", "sql"):
            code = (
                f"-- Atomic Composite Field Pipeline (T-SQL): [{ds}].[{col}]\n"
                f"-- Precedence: source_null -> placeholder_normalized_to_null -> punctuation_only_normalized_to_null -> valid\n"
                f"UPDATE #{ds.split('.')[-1]}_Staging\n"
                f"SET\n"
                f"    [{col}_raw] = [{col}],\n"
                f"    [{col}_clean] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN NULL\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN NULL\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%[a-zA-Z0-9]%' THEN NULL\n"
                f"        ELSE LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))\n"
                f"    END,\n"
                f"    [{col}_validation_status] = CASE\n"
                f"        WHEN [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '' THEN 'source_null'\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'placeholder_normalized_to_null'\n"
                f"        WHEN LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%[a-zA-Z0-9]%' THEN 'punctuation_only_normalized_to_null'\n"
                f"        ELSE 'valid'\n"
                f"    END,\n"
                f"    [{col}_validation_reason] = CASE\n"
                f"        WHEN LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX))))) IN ('n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-') THEN 'Configured text placeholder token'\n"
                f"        WHEN [{col}] IS NOT NULL AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) <> '' AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) NOT LIKE '%[a-zA-Z0-9]%' THEN 'Punctuation-only string anomaly'\n"
                f"        ELSE NULL\n"
                f"    END;"
            )
        else:
            code = (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Composite Field Pipeline: {ds}.{col} (Canonical Order: Raw -> Trim/Clean Spaces -> Placeholder/Punctuation Validation)\n"
                f"raw = df['{col}']\n"
                f"str_val = raw.astype('string').str.strip()\n"
                f"cleaned_str = str_val.str.replace(r'\\s+', ' ', regex=True)\n"
                f"is_null = raw.isna() | (str_val == '')\n"
                f"is_placeholder = str_val.str.lower().isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###', '-'])\n"
                f"is_punct_only = ~is_null & ~is_placeholder & ~str_val.str.contains(r'[a-zA-Z0-9]', regex=True)\n"
                f"compound_err = is_placeholder | is_punct_only\n"
                f"\n"
                f"df['{col}_raw'] = raw\n"
                f"df['{col}_clean'] = cleaned_str.where(~compound_err & ~is_null, None)\n"
                f"df['{col}_validation_status'] = np.select(\n"
                f"    [is_null, is_placeholder, is_punct_only],\n"
                f"    ['source_null', 'placeholder_normalized_to_null', 'punctuation_only_normalized_to_null'],\n"
                f"    default='valid'\n"
                f")\n"
                f"df['{col}_validation_reason'] = np.select(\n"
                f"    [is_placeholder, is_punct_only],\n"
                f"    ['Configured text placeholder token', 'Punctuation-only string anomaly'],\n"
                f"    default=None\n"
                f")\n"
                f"clean_df = df.copy()\n"
                f"quarantine_df = df.iloc[0:0].copy()"
            )

    code_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    primary_action = work_package.ordered_transforms[-1] if work_package.ordered_transforms else "composite_remediation"
    outcome_meta = ACTION_OUTCOME_METADATA.get(primary_action, ACTION_OUTCOME_METADATA["flag_only"])

    output_cols = [f"{col}_raw", f"{col}_clean", f"{col}_validation_status", f"{col}_validation_reason"]
    val_cols = [f"{col}_validation_status", f"{col}_validation_reason"]
    
    if any(k in col.lower() for k in ["date", "time"]):
        clean_target_type = "DATETIME"
    elif any(k in col.lower() for k in ["price", "cost", "amount", "revenue"]):
        clean_target_type = "DECIMAL(18,2)"
    elif any(k in col.lower() for k in ["age", "qty", "count", "instructorid"]):
        clean_target_type = "INT"
    else:
        clean_target_type = "NVARCHAR(320)"

    output_types = {
        f"{col}_raw": "NVARCHAR(MAX)",
        f"{col}_clean": clean_target_type,
        f"{col}_validation_status": "VARCHAR(50)",
        f"{col}_validation_reason": "VARCHAR(500)",
    }
    row_disp = "clean_xor_quarantine" if has_quarantine else "retain_all"

    return GeneratedRemediationArtifact(
        action_id=primary_action,
        dialect=dialect,
        code=code,
        outcome_metadata=outcome_meta,
        validation_status="not_started",
        is_provisional=work_package.is_provisional,
        provisional_label="Provisional code preview for the selected option. This transformation is not approved or authorized for execution." if work_package.is_provisional else None,
        artifact_id=f"art-{work_package.work_package_id}-{primary_action}",
        recommendation_id=work_package.findings[0].get("id") if work_package.findings else None,
        selected_option_id=work_package.resolved_actions[0].option_id if work_package.resolved_actions else None,
        generator_id="composite_field_generator",
        generator_version="2.4.1",
        code_hash=code_hash,
        artifact_status="provisional" if work_package.is_provisional else "generated",
        output_columns=output_cols,
        output_types=output_types,
        validation_columns=val_cols,
        row_disposition=row_disp,
    )
