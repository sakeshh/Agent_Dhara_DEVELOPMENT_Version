from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.key_semantics import classify_column_semantics
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.rules import BusinessRule
from agent.compiler.models.tier3_resolution import ALLOWED_TIER3_ACTION_TYPES
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge


# Configuration Helpers
def is_tier3_llm_enabled() -> bool:
    """Returns True if Tier 3B governed LLM resolution is active in environment."""
    val = os.getenv("TIER3_LLM_ENABLED", "1").strip().lower()
    return val in ("1", "true", "yes", "on")


def is_tier3_auto_resolution_enabled() -> bool:
    """Returns True if auto-resolution of safe Tier 3 proposals is allowed."""
    val = os.getenv("TIER3_AUTO_RESOLUTION_ENABLED", "1").strip().lower()
    return val in ("1", "true", "yes", "on")


def is_tier3_cache_enabled() -> bool:
    """Returns True if Tier 3 replay caching is active."""
    val = os.getenv("TIER3_CACHE_ENABLED", "1").strip().lower()
    return val in ("1", "true", "yes", "on")


def get_tier3_model_deployment() -> str:
    """Returns the configured model deployment name for Tier 3 LLM resolution."""
    dep = os.getenv("TIER3_MODEL_DEPLOYMENT")
    if dep and dep.strip():
        return dep.strip()
    try:
        from agent.model_config import load_llm_config
        cfg = (
            load_llm_config(purpose="tier3_resolution")
            or load_llm_config(purpose="etl_codegen")
            or load_llm_config(purpose="general")
        )
        if cfg and cfg.model:
            return cfg.model
    except Exception:
        pass
    return "gpt-4o"


@dataclass(frozen=True)
class Tier3EligibilityResult:
    """Detailed classification of an issue's eligibility for Tier 3B governed LLM resolution."""
    eligible: bool
    reason_code: str
    risk_level: str
    requires_human_authority: bool
    supported_action_types: List[str] = field(default_factory=list)


# Purely observational or non-blocking diagnostic findings
INFORMATIONAL_ISSUE_TYPES: Set[str] = {
    "all_caps_values",
    "all_caps",
    "string_length_outlier",
    "binary_like_column",
    "binary_like",
    "multivariate_outliers",
    "multivariate_outlier",
    "digits_in_text",
    "string_digits_in_text",
    "only_digits",
    "string_with_only_digits_in_text_column",
    "unusual_distribution",
    "custom_observation",
    "statistical_observation",
    "planned_flag_outliers",
    "flag_outliers",
    "planned_flag_near_duplicate_cluster",
    "flag_near_duplicate_cluster",
    "planned_flag_duplicate_candidates",
    "flag_duplicate_candidates",
    "duplicate_candidates",
    "near_duplicate_cluster",
    "potential_identifier",
    "canonical_schema_policy",
    "canonical_schema_constraint",
    "policy_never_drop_rows",
    "policy_constraint_applied",
}

# Unsupported physical operations that compiler cannot compile to SQL
UNSUPPORTED_PRIMITIVES: Set[str] = {
    "title_case",
    "sentence_case",
    "complex_graph_solve",
    "recursive_mutation",
    "statistical_imputation",
    "knn_impute",
    "mice_impute",
    "external_api_lookup",
    "arbitrary_procedural_sql",
    "unsupported_primitive_required",
}


def is_tier3b_eligible(
    issue: CanonicalIssue,
    profiles: Optional[Dict[str, DatasetProfile]] = None,
    rules: Optional[List[Any]] = None,
    compiler_capabilities: Optional[Set[str]] = None,
) -> Tier3EligibilityResult:
    """
    Evaluates whether an unresolved canonical issue is eligible for Tier 3B LLM resolution.

    Strict Invariants:
      1. Informational findings must NOT invoke the model (classified as INFORMATIONAL).
      2. Missing human business authority (sentinel policies, undefined keys, unknown thresholds)
         must NOT be invented by the LLM (classified as requiring human authority).
      3. Unsupported physical operations that the compiler cannot render must NOT invoke the model
         (classified as UNSUPPORTED_PRIMITIVE_REQUIRED).
      4. Only complex, semantic, AST-expressible issues with verified schema context are eligible.
    """
    itype = (issue.issue_type or "").lower().strip()
    res_path = (issue.resolution_path or "").lower().strip()
    col = (issue.column or "").lower().strip()
    msg = (issue.message or "").lower().strip()
    sug = (issue.suggested_action or "").lower().strip()

    # 1. Already resolved by deterministic compiler or prior manual review
    if issue.resolved_action is not None or "resolved_by_manual_review" in res_path:
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="ALREADY_RESOLVED",
            risk_level="LOW",
            requires_human_authority=False,
            supported_action_types=[],
        )

    # 2. Informational observations - no transformation requested
    if itype in INFORMATIONAL_ISSUE_TYPES or any(inf in res_path for inf in ("informational", "statistical_observation")):
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="INFORMATIONAL",
            risk_level="LOW",
            requires_human_authority=False,
            supported_action_types=[],
        )

    # 3. Unsupported Physical Primitives
    if sug in UNSUPPORTED_PRIMITIVES or itype in UNSUPPORTED_PRIMITIVES or "unsupported_primitive" in res_path:
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="UNSUPPORTED_PRIMITIVE_REQUIRED",
            risk_level="HIGH",
            requires_human_authority=False,
            supported_action_types=[],
        )

    # 4. Missing Authoritative Business Semantics (Steward Authority Mandatory)
    # The LLM must not invent unknown sentinel lists, key generation policies, or salary limits
    is_financial_field = any(f in col for f in ("salary", "wage", "compensation", "bonus", "revenue", "cost", "price"))
    if "missing_sentinel" in res_path or "missing_sentinel_list" in res_path or itype == "planned_normalize_registered_placeholder":
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="MISSING_AUTHORITATIVE_SENTINEL_POLICY",
            risk_level="HIGH",
            requires_human_authority=True,
            supported_action_types=[],
        )

    if "missing_range_bounds" in res_path or (is_financial_field and "outlier" in itype and not issue.parameters.get("max_value")):
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="MISSING_AUTHORITATIVE_BUSINESS_THRESHOLD",
            risk_level="HIGH",
            requires_human_authority=True,
            supported_action_types=[],
        )

    if "missing_declared_key" in res_path or itype in ("planned_flag_missing_declared_key", "missing_declared_key"):
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="MISSING_AUTHORITATIVE_BUSINESS_KEY",
            risk_level="HIGH",
            requires_human_authority=True,
            supported_action_types=[],
        )

    # Self-referencing loops or manager hierarchies without authoritative root
    if ("manager" in col or "parent" in col) and ("orphan" in itype or "cycle" in itype or "hierarchy" in itype):
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="MISSING_AUTHORITATIVE_HIERARCHY_POLICY",
            risk_level="HIGH",
            requires_human_authority=True,
            supported_action_types=[],
        )

    # 5. Verify Schema Context
    ds_prof = profiles.get(issue.dataset) if profiles else None
    if profiles and not ds_prof:
        # Check case-insensitive match
        for dname, prof in profiles.items():
            if dname.lower().strip("[]") == issue.dataset.lower().strip("[]") or dname.replace("dbo.", "") == issue.dataset:
                ds_prof = prof
                break

    if not ds_prof:
        return Tier3EligibilityResult(
            eligible=False,
            reason_code="INSUFFICIENT_SCHEMA_CONTEXT",
            risk_level="HIGH",
            requires_human_authority=True,
            supported_action_types=[],
        )

    if issue.column and not IngestionBridge.is_pseudo_column(issue.column):
        col_clean = issue.column.strip("[]").lower()
        known_cols = {c.strip("[]").lower() for c in ds_prof.columns.keys()}
        if known_cols and col_clean not in known_cols:
            return Tier3EligibilityResult(
                eligible=False,
                reason_code="UNKNOWN_COLUMN_SCHEMA_CONTEXT",
                risk_level="HIGH",
                requires_human_authority=True,
                supported_action_types=[],
            )

    # 6. Eligible for Tier 3B Governed LLM Resolution
    # Map issue characteristics to allowed Tier 3 actions
    supported_actions: List[str] = []
    if "null" in itype or "missing" in itype:
        supported_actions.extend(["FILL_NULLS", "CONDITIONAL_UPDATE", "FLAG_ROW"])
    elif "referential" in itype or "orphan" in itype or "foreign" in itype:
        supported_actions.extend(["VALIDATE_REFERENTIAL_INTEGRITY", "FLAG_ROW"])
    elif "date" in itype or "time" in itype:
        supported_actions.extend(["PARSE_DATES", "CONDITIONAL_UPDATE"])
    elif "format" in itype or "pattern" in itype or "invalid" in itype or "anomaly" in itype:
        supported_actions.extend(["CONDITIONAL_UPDATE", "FLAG_ROW", "NULLIFY", "VALUE_MAP"])
    elif "range" in itype or "bound" in itype:
        supported_actions.extend(["VALIDATE_RANGE", "CONDITIONAL_UPDATE", "FLAG_ROW"])
    else:
        supported_actions.extend(["CONDITIONAL_UPDATE", "FLAG_ROW", "NULLIFY"])

    # Intersect with compiler capabilities or ALLOWED_TIER3_ACTION_TYPES
    allowed_set = compiler_capabilities or ALLOWED_TIER3_ACTION_TYPES
    filtered_actions = [a for a in supported_actions if a in allowed_set]
    if not filtered_actions:
        filtered_actions = ["CONDITIONAL_UPDATE", "FLAG_ROW"]

    # Determine risk level based on candidate actions
    risk_level = "MEDIUM"
    if any(a in ("NULLIFY", "REJECT_ROW") for a in filtered_actions):
        risk_level = "HIGH"

    return Tier3EligibilityResult(
        eligible=True,
        reason_code="COMPLEX_SEMANTIC_REMEDIATION",
        risk_level=risk_level,
        requires_human_authority=False,
        supported_action_types=filtered_actions,
    )
