"""
LLM-assisted DQ cleaning recommendations.

Consumes merged DQ issues and produces a prioritized cleaning plan.
Designed for production: returns structured JSON; falls back to rule-based hints if LLM not configured.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from agent.model_config import load_llm_config
from agent.openai_usage import usage_dict_from_response
from agent.session_store import SessionJSONEncoder


@dataclass(frozen=True)
class DQRecommendations:
    recommendations: List[Dict[str, Any]]
    summary: Dict[str, Any]


_SYSTEM = """
You are a data quality remediation planner.
Given data quality issues across datasets, output a prioritized, actionable cleaning plan.

Return ONLY valid JSON. No markdown.

JSON schema:
{
  "recommendations": [
    {
      "priority": 1,
      "dataset": "string|null",
      "column": "string|null",
      "issue_type": "string",
      "severity": "high|medium|low",
      "why_it_matters": "string",
      "suggested_fix": "string",
      "mapped_action": "trim|fill_nulls_simple|cast_type|regex_replace|deduplicate|parse_dates|sanitize_email|normalize_phone|clip_or_flag|flag_outliers|zero_to_null|lowercase|uppercase|exclude_column|drop_column|noop",
      "example_sql": "string|null",
      "example_pandas": "string|null",
      "risk": "string"
    }
  ],
  "summary": {
    "top_themes": ["string", "..."],
    "notes": "string"
  }
}
""".strip()


def _fallback(dq: Dict[str, Any]) -> DQRecommendations:
    # Minimal safe fallback: promote existing per-issue recommendation fields if present.
    try:
        from agent.profiling.assessment_orchestrator import load_thresholds
        thresholds = load_thresholds()
    except Exception:
        thresholds = {}
    platform_target = thresholds.get("platform", {}).get("target", "fabric_spark_sql")

    from agent.profiling.sql_dialect_adapter import get_remediation_sql

def get_pandas_code(col: Optional[str], issue_type: str, related_ds: Optional[str] = None, related_col: Optional[str] = None) -> str:
    col_safe = col or "column_name"
    col_l = col_safe.lower().strip()
    itype = issue_type.lower()
    
    NON_NUMERIC_SEMANTICS = {
        "country", "country_code", "customer_id", "order_id", "payment_id",
        "client_id", "user_id", "status", "date", "phone", "email", "full_name"
    }

    if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        if "numeric" in itype or "mixed" in itype:
            return (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Step: text normalization for '{col_safe}' (preserving string/identifier format)\n"
                f"raw = df['{col_safe}']\n"
                f"df['{col_safe}_clean'] = raw.astype('string').str.strip().str.upper()\n"
                f"df['{col_safe}_validation_status'] = np.select([\n"
                f"    raw.isna(),\n"
                f"    raw.astype(str).str.strip() == ''\n"
                f"], ['source_null', 'empty'], default='valid')"
            )

    if "numeric" in itype or "mixed" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"raw = df['{col_safe}']\n"
            f"clean = pd.to_numeric(raw, errors='coerce')\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = clean\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    condlist=[\n"
            f"        raw.isna(),\n"
            f"        raw.astype(str).str.strip() == '',\n"
            f"        clean.isna()\n"
            f"    ],\n"
            f"    choicelist=['source_null', 'empty', 'invalid_numeric'],\n"
            f"    default='valid'\n"
            f")"
        )
        
    elif "whitespace" in itype:
        return f"df['{col_safe}'] = df['{col_safe}'].astype('string').str.strip().str.replace(r'\\s+', ' ', regex=True)"
        
    elif "date" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"raw = df['{col_safe}']\n"
            f"parsed = pd.to_datetime(raw, format='%Y-%m-%d', errors='coerce')\n"
            f"df['{col_safe}_clean'] = parsed\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    condlist=[raw.isna(), parsed.isna()],\n"
            f"    choicelist=['source_null', 'invalid_date'],\n"
            f"    default='valid'\n"
            f")"
        )

    elif "placeholder" in itype or "punctuation" in itype:
        return (
            f"placeholder_tokens = ['???', 'n/a', 'unknown', 'tbd', 'not available']\n"
            f"normalized = df['{col_safe}'].astype(str).str.strip().str.lower()\n"
            f"df['{col_safe}_clean'] = df['{col_safe}'].where(~normalized.isin(placeholder_tokens), None)"
        )

    elif "orphan" in itype or "relationship" in itype:
        p_ds_safe = (related_ds or "parent_dataset").replace(".", "_").replace("-", "_")
        p_col_safe = related_col or "parent_column"
        return (
            f"# Quarantine the child rows. Remove only under approved retention & remediation policy.\n"
            f"parent_keys = set({p_ds_safe}_df['{p_col_safe}'].dropna().unique())\n"
            f"orphan_{col_safe}_rows = df[~df['{col_safe}'].isin(parent_keys)]\n"
            f"valid_{col_safe}_rows = df[df['{col_safe}'].isin(parent_keys)]"
        )

    elif "null" in itype or "violation" in itype:
        if "suspected" in itype:
            return (
                f"# Profile and review these rows. Do not quarantine automatically until key is declared mandatory.\n"
                f"view_uniques = df['{col_safe}'].value_counts()"
            )
        is_key = col_safe.lower().endswith(("_id", "_key", "_code")) or col_safe.lower() == "id" or "key" in itype
        if is_key:
            return (
                f"invalid_key_rows = df[df['{col_safe}'].isna()].copy()\n"
                f"valid_rows = df[df['{col_safe}'].notna()].copy()"
            )
        else:
            return (
                f"# Preserve as null, quarantine if required, or impute only under a declared field-specific policy.\n"
                f"# Apply only the field-specific imputation function approved by the data owner."
            )
            
    elif "negative" in itype:
        return (
            f"# Preserve raw amount for auditability; quarantine negative entries for business review\n"
            f"quarantine_df = df[df['{col_safe}'] < 0]\n"
            f"clean_df = df[df['{col_safe}'] >= 0]"
        )
        
    else:
        return f"# Remediate {issue_type} manually"


def get_pyspark_code(col: Optional[str], issue_type: str, related_ds: Optional[str] = None, related_col: Optional[str] = None) -> str:
    col_safe = col or "column_name"
    col_l = col_safe.lower().strip()
    itype = issue_type.lower()
    
    NON_NUMERIC_SEMANTICS = {
        "country", "country_code", "customer_id", "order_id", "payment_id",
        "client_id", "user_id", "status", "date", "phone", "email", "full_name"
    }

    if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        if "numeric" in itype or "mixed" in itype:
            return (
                f"import pyspark.sql.functions as F\n"
                f"# Step: text normalization for '{col_safe}' (preserving string/identifier format)\n"
                f"raw = F.col('{col_safe}')\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col_safe}_raw', raw)\n"
                f"    .withColumn('{col_safe}_clean', F.upper(F.trim(raw.cast('string'))))\n"
                f"    .withColumn(\n"
                f"        '{col_safe}_validation_status',\n"
                f"        F.when(raw.isNull(), 'source_null')\n"
                f"         .when(F.trim(raw.cast('string')) == '', 'empty')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f")"
            )

    if "numeric" in itype or "mixed" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"raw = F.col('{col_safe}')\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', raw.cast('double'))\n"
            f"    .withColumn(\n"
            f"        '{col_safe}_validation_status',\n"
            f"        F.when(raw.isNull(), 'source_null')\n"
            f"         .when(F.trim(raw.cast('string')) == '', 'empty')\n"
            f"         .when(raw.cast('double').isNull(), 'invalid_numeric')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f")"
        )
    
    elif "whitespace" in itype:
        return f"import pyspark.sql.functions as F\ndf = df.withColumn('{col_safe}', F.regexp_replace(F.trim(F.col('{col_safe}').cast('string')), r'\\s+', ' '))"
        
    elif "date" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"parsed = F.to_date(F.col('{col_safe}'), 'yyyy-MM-dd')\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_clean', parsed)\n"
            f"    .withColumn(\n"
            f"        '{col_safe}_validation_status',\n"
            f"        F.when(F.col('{col_safe}').isNull(), 'source_null')\n"
            f"         .when(parsed.isNull(), 'invalid_date')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f")"
        )

    elif "placeholder" in itype or "punctuation" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"placeholder_tokens = ['???', 'n/a', 'unknown', 'tbd', 'not available']\n"
            f"normalized = F.lower(F.trim(F.col('{col_safe}')))\n"
            f"df = df.withColumn(\n"
            f"    '{col_safe}_clean',\n"
            f"    F.when(normalized.isin(placeholder_tokens), None)\n"
            f"     .otherwise(F.col('{col_safe}'))\n"
            f")"
        )

    elif "orphan" in itype or "relationship" in itype:
        p_ds_safe = (related_ds or "parent_dataset").replace(".", "_").replace("-", "_")
        p_col_safe = related_col or "parent_column"
        return (
            f"# Quarantine the child rows. Remove only under approved retention & remediation policy.\n"
            f"parent_keys = {p_ds_safe}_df.select('{p_col_safe}').dropDuplicates()\n"
            f"orphan_{col_safe}_rows = df.join(parent_keys, on='{col_safe}', how='left_anti')\n"
            f"valid_{col_safe}_rows = df.join(parent_keys, on='{col_safe}', how='left_semi')"
        )

    elif "null" in itype or "violation" in itype:
        if "suspected" in itype:
            return (
                f"# Profile and review these rows. Do not quarantine automatically until key is declared mandatory.\n"
                f"# df.select('{col_safe}').groupBy('{col_safe}').count().show()"
            )
        is_key = col_safe.lower().endswith(("_id", "_key", "_code")) or col_safe.lower() == "id" or "key" in itype
        if is_key:
            return (
                f"quarantine_df = df.filter(df['{col_safe}'].isNull())\n"
                f"clean_df = df.filter(df['{col_safe}'].isNotNull())"
            )
        else:
            return (
                f"# Preserve as null or impute under approved policy\n"
                f"# Apply only the field-specific imputation function approved by the data owner."
            )
            
    elif "negative" in itype:
        return (
            f"# Preserve raw amount for auditability; quarantine negative entries for business review\n"
            f"import pyspark.sql.functions as F\n"
            f"quarantine_df = df.filter(F.col('{col_safe}') < 0)\n"
            f"clean_df = df.filter(F.col('{col_safe}') >= 0)"
        )

        
    else:
        return f"# Remediate {issue_type} using PySpark manually"


def _fallback(dq: Dict[str, Any]) -> DQRecommendations:
    # Minimal safe fallback: promote existing per-issue recommendation fields if present.
    try:
        from agent.profiling.assessment_orchestrator import load_thresholds
        thresholds = load_thresholds()
    except Exception:
        thresholds = {}
    platform_target = thresholds.get("platform", {}).get("target", "fabric_spark_sql")

    from agent.profiling.sql_dialect_adapter import get_remediation_sql

    recs: List[Dict[str, Any]] = []
    datasets = (dq.get("datasets") or {}) if isinstance(dq.get("datasets"), dict) else {}
    for ds_name, block in datasets.items():
        issues = (block.get("issues") or []) if isinstance(block, dict) else []
        for it in issues[:60]:
            if not isinstance(it, dict):
                continue
            issue_type = it.get("type") or it.get("issue_type") or "issue"
            
            # Simple fallback mapping to mapped_action
            mapped = "noop"
            itl = issue_type.lower()
            if "whitespace" in itl:
                mapped = "trim"
            elif "null" in itl:
                mapped = "fill_nulls_simple"
            elif "date" in itl:
                mapped = "parse_dates"
            elif "email" in itl:
                mapped = "sanitize_email"
            elif "phone" in itl:
                mapped = "normalize_phone"
            elif "numeric" in itl or "mixed" in itl:
                col_l = str(it.get("column") or "").lower().strip()
                NON_NUMERIC_SEMANTICS = {"country", "country_code", "customer_id", "order_id", "payment_id", "client_id", "user_id", "status", "date", "phone", "email", "full_name"}
                if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
                    mapped = "trim"
                else:
                    mapped = "coerce_numeric"
            elif "negative" in itl:
                mapped = "review_manually"
            elif "duplicate" in itl:
                mapped = "deduplicate"
            elif "outlier" in itl:
                mapped = "flag_outliers"
            elif "skew" in itl:
                mapped = "flag_outliers"
            elif "custom_rule_violation" in itl:
                mapped = "review_manually"


            col_name = it.get("column")
            related_ds = it.get("related_dataset")
            related_col = it.get("related_column")
            example_sql = get_remediation_sql(platform_target, ds_name, col_name, issue_type)
            example_pandas = get_pandas_code(col_name, issue_type, related_ds, related_col)
            example_pyspark = get_pyspark_code(col_name, issue_type, related_ds, related_col)

            # Compute meaningful recommendation priority
            is_gating = it.get("gating", True)
            sev = (it.get("severity") or "medium").lower()
            is_rel = any(k in itl for k in ["relationship", "orphan", "foreign_key"])
            
            if is_gating and sev == "high" and is_rel:
                priority = 0
            elif is_gating and sev == "high":
                priority = 1
            elif sev == "medium":
                priority = 2
            elif sev == "low":
                priority = 3
            else:
                priority = 4

            # Only allow auto-remediation execution for safe and tested issue types
            auto_fix = issue_type in ("whitespace", "whitespace_only")

            recs.append(
                {
                    "priority": priority,
                    "dataset": ds_name,
                    "column": col_name,
                    "issue_type": issue_type,
                    "related_dataset": related_ds,
                    "related_column": related_col,
                    "severity": it.get("severity") or "medium",
                    "why_it_matters": it.get("message") or "",
                    "suggested_fix": it.get("recommendation") or "Review and remediate based on business rules.",
                    "mapped_action": mapped,
                    "auto_fixable": auto_fix,
                    "example_sql": example_sql,
                    "example_pandas": example_pandas,
                    "example_pyspark": example_pyspark,
                    "risk": "Fallback mode (LLM not configured). Validate before applying changes.",
                }
            )
            
    # Sort recommendations by priority (P0 to P4)
    recs.sort(key=lambda r: r.get("priority", 99))

    return DQRecommendations(
        recommendations=recs[:80],
        summary={
            "top_themes": [],
            "notes": f"LLM not configured; returned fallback recommendations mapped to {platform_target} target.",
        },
    )


class DQRecommendationsAgent:
    def recommend(
        self,
        *,
        merged_dq: Dict[str, Any],
        user_intent: str = "",
    ) -> Tuple[DQRecommendations, Optional[Dict[str, int]]]:
        cfg = load_llm_config(purpose="dq_recommendations")
        if cfg is None:
            return _fallback(merged_dq), None

        payload = {
            "user_intent": user_intent,
            "data_quality_issues": merged_dq,
            "constraints": {
                "prefer_safe_fixes": True,
                "no_destructive_steps_without_warning": True,
            },
        }
        prompt = json.dumps(payload, ensure_ascii=False, cls=SessionJSONEncoder)
        try:
            if cfg.provider == "azure_openai":
                from openai import AzureOpenAI  # type: ignore

                client = AzureOpenAI(
                    api_key=cfg.api_key,
                    api_version=cfg.api_version or "2024-02-01",
                    azure_endpoint=cfg.endpoint,
                )
                resp = client.chat.completions.create(
                    model=cfg.model,
                    messages=[{"role": "system", "content": _SYSTEM}, {"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=900,
                )
            else:
                from openai import OpenAI  # type: ignore

                client = OpenAI(api_key=cfg.api_key)
                resp = client.chat.completions.create(
                    model=cfg.model,
                    messages=[{"role": "system", "content": _SYSTEM}, {"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=900,
                )
            raw = (resp.choices[0].message.content or "").strip()
            obj = json.loads(raw)
            recs = obj.get("recommendations")
            summ = obj.get("summary")
            if not isinstance(recs, list):
                recs = []
            if not isinstance(summ, dict):
                summ = {"notes": "LLM returned invalid summary; using empty summary."}
            
            # Enrich LLM recommendations with Pandas & Spark code blocks
            from agent.profiling.sql_dialect_adapter import get_remediation_sql
            try:
                from agent.profiling.assessment_orchestrator import load_thresholds
                thresholds = load_thresholds()
            except Exception:
                thresholds = {}
            platform_target = thresholds.get("platform", {}).get("target", "fabric_spark_sql")

            for i, r in enumerate(recs):
                if not isinstance(r, dict):
                    continue
                if "priority" not in r:
                    r["priority"] = i + 1
                col_name = r.get("column")
                issue_type = r.get("issue_type")
                related_ds = r.get("related_dataset")
                related_col = r.get("related_column")
                if "example_pandas" not in r:
                    r["example_pandas"] = get_pandas_code(col_name, issue_type, related_ds, related_col)
                if "example_pyspark" not in r:
                    r["example_pyspark"] = get_pyspark_code(col_name, issue_type, related_ds, related_col)
                if "example_sql" not in r:
                    r["example_sql"] = get_remediation_sql(platform_target, r.get("dataset", ""), col_name, issue_type)

            usage = usage_dict_from_response(resp)
            return DQRecommendations(recommendations=recs[:120], summary=summ), usage
        except Exception:
            return _fallback(merged_dq), None


def dq_recommendations_to_dict(r: DQRecommendations) -> Dict[str, Any]:
    return {"recommendations": r.recommendations, "summary": r.summary}


