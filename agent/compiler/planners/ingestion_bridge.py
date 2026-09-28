from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity
from agent.compiler.models.logical import ActionType
from agent.compiler.models.rules import BusinessRule, RuleScope
from agent.compiler.schema.sql_identifier import parse_table_identifier

logger = logging.getLogger("agent.compiler.ingestion_bridge")


@dataclass
class CanonicalIssue:
    """Canonical representation of any data quality finding or transformation requirement."""
    issue_id: str
    dataset: str
    raw_dataset: str
    column: Optional[str]
    issue_type: str
    suggested_action: Optional[str]
    severity: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"
    rule_id: Optional[str] = None
    message: str = ""
    affected_rows: int = 0
    resolved_action: Optional[ActionType] = None
    resolution_path: Optional[str] = None


class IngestionBridge:
    """Extracts, normalizes, deduplicates, and maps assessment findings and planned rules
    into canonical Issue and StructuredAction models for the CompilerPipeline.
    """

    PSEUDO_COLUMNS: Set[str] = {
        "row-level", "[row-level]", "table-level", "[table-level]",
        "dataset-level", "[dataset-level]", "dataset", "table",
        "unknown", "none", "null", "all", "[all]", "-", "global", "*", "[*]", "",
        "__row__", "[__row__]",
    }

    DATASET_LEVEL_ACTIONS: Set[ActionType] = {
        ActionType.DEDUPLICATE,
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
    }

    @classmethod
    def is_pseudo_column(cls, col: Any) -> bool:
        if col is None:
            return True
        raw = str(col).strip()
        clean = raw.lower().strip("[]").strip()
        return clean in cls.PSEUDO_COLUMNS or raw.lower() in cls.PSEUDO_COLUMNS or not clean

    # Tier 1 & 2 Action mapping
    EXPLICIT_ACTION_MAP: Dict[str, ActionType] = {
        "trim": ActionType.TRIM,
        "str_strip": ActionType.TRIM,
        "strip": ActionType.TRIM,
        "lowercase": ActionType.LOWERCASE,
        "lower": ActionType.LOWERCASE,
        "str_lower": ActionType.LOWERCASE,
        "uppercase": ActionType.UPPERCASE,
        "upper": ActionType.UPPERCASE,
        "str_upper": ActionType.UPPERCASE,
        "cast": ActionType.CAST_TYPE,
        "cast_type": ActionType.CAST_TYPE,
        "coerce_numeric": ActionType.COERCE_NUMERIC,
        "to_numeric": ActionType.COERCE_NUMERIC,
        "parse_dates": ActionType.PARSE_DATES,
        "standardize_dates": ActionType.PARSE_DATES,
        "sanitize_email": ActionType.SANITIZE_EMAIL,
        "clean_email": ActionType.SANITIZE_EMAIL,
        "normalize_phone": ActionType.NORMALIZE_PHONE,
        "clean_phone": ActionType.NORMALIZE_PHONE,
        "mask_phone": ActionType.MASK_PHONE,
        "mask_pii": ActionType.MASK_PHONE,
        "range_clip": ActionType.RANGE_CLIP,
        "clip_outliers": ActionType.RANGE_CLIP,
        "cap_outliers": ActionType.CAP_OUTLIERS,
        "standardize_boolean": ActionType.STANDARDIZE_BOOLEAN,
        "boolean": ActionType.STANDARDIZE_BOOLEAN,
        "fill_nulls": ActionType.FILL_NULLS,
        "fillna": ActionType.FILL_NULLS,
        "zero_to_null": ActionType.ZERO_TO_NULL,
        "nullif_zero": ActionType.ZERO_TO_NULL,
        "replace_sentinels": ActionType.REPLACE_SENTINELS,
        "deduplicate": ActionType.DEDUPLICATE,
        "drop_duplicates": ActionType.DEDUPLICATE,
        "validate_referential_integrity": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "ri_check": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "conditional_update": ActionType.CONDITIONAL_UPDATE,
        "prefix_add": ActionType.PREFIX_ADD,
        "suffix_add": ActionType.SUFFIX_ADD,
        "pattern_replace": ActionType.PATTERN_REPLACE,
        "value_map": ActionType.VALUE_MAP,
        "pad": ActionType.PAD,
        "drop_column": ActionType.DROP_COLUMN,
    }

    # Tier 3 Normalized Issue Type mapping
    ISSUE_TYPE_MAP: Dict[str, ActionType] = {
        "whitespace": ActionType.TRIM,
        "untrimmed_whitespace": ActionType.TRIM,
        "leading_trailing_whitespace": ActionType.TRIM,
        "trailing_whitespace": ActionType.TRIM,
        "leading_whitespace": ActionType.TRIM,
        "lowercase": ActionType.LOWERCASE,
        "case_inconsistency": ActionType.LOWERCASE,
        "mixed_case": ActionType.LOWERCASE,
        "uppercase": ActionType.UPPERCASE,
        "null_values": ActionType.FILL_NULLS,
        "missing_values": ActionType.FILL_NULLS,
        "high_null_percentage": ActionType.FILL_NULLS,
        "nulls": ActionType.FILL_NULLS,
        "zero_to_null": ActionType.ZERO_TO_NULL,
        "invalid_zero": ActionType.ZERO_TO_NULL,
        "zeros_as_null": ActionType.ZERO_TO_NULL,
        "sentinel_values": ActionType.REPLACE_SENTINELS,
        "replace_sentinels": ActionType.REPLACE_SENTINELS,
        "sentinels": ActionType.REPLACE_SENTINELS,
        "mixed_types": ActionType.CAST_TYPE,
        "type_mismatch": ActionType.CAST_TYPE,
        "datatype_mismatch": ActionType.CAST_TYPE,
        "incompatible_types": ActionType.CAST_TYPE,
        "non_numeric_characters": ActionType.COERCE_NUMERIC,
        "numeric_coercion": ActionType.COERCE_NUMERIC,
        "non_numeric_in_id": ActionType.COERCE_NUMERIC,
        "invalid_date": ActionType.PARSE_DATES,
        "invalid_date_format": ActionType.PARSE_DATES,
        "date_format": ActionType.PARSE_DATES,
        "multi_format_date": ActionType.PARSE_DATES,
        "future_dates": ActionType.PARSE_DATES,
        "invalid_email": ActionType.SANITIZE_EMAIL,
        "invalid_email_format": ActionType.SANITIZE_EMAIL,
        "email_syntax": ActionType.SANITIZE_EMAIL,
        "email_domain_invalid": ActionType.SANITIZE_EMAIL,
        "invalid_phone": ActionType.NORMALIZE_PHONE,
        "invalid_phone_format": ActionType.NORMALIZE_PHONE,
        "phone_length": ActionType.NORMALIZE_PHONE,
        "non_numeric_phone": ActionType.NORMALIZE_PHONE,
        "pii_exposure": ActionType.MASK_PHONE,
        "pii_phone": ActionType.MASK_PHONE,
        "mask_phone": ActionType.MASK_PHONE,
        "range_violation": ActionType.RANGE_CLIP,
        "range_outlier": ActionType.RANGE_CLIP,
        "range_outliers": ActionType.RANGE_CLIP,
        "outlier": ActionType.RANGE_CLIP,
        "outliers": ActionType.RANGE_CLIP,
        "negative_values": ActionType.RANGE_CLIP,
        "non_negative": ActionType.RANGE_CLIP,
        "negative_salary": ActionType.RANGE_CLIP,
        "negative_quantity": ActionType.RANGE_CLIP,
        "negative_price": ActionType.RANGE_CLIP,
        "min_zero": ActionType.RANGE_CLIP,
        "extreme_outliers": ActionType.RANGE_CLIP,
        "negative_currency": ActionType.RANGE_CLIP,
        "duplicates": ActionType.DEDUPLICATE,
        "duplicate_rows": ActionType.DEDUPLICATE,
        "duplicate_primary_key": ActionType.DEDUPLICATE,
        "duplicate_pk": ActionType.DEDUPLICATE,
        "orphan_foreign_keys": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "orphan_foreign_key": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "referential_integrity": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "unmatched_parent_key": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "boolean_inconsistency": ActionType.STANDARDIZE_BOOLEAN,
        "boolean_format": ActionType.STANDARDIZE_BOOLEAN,
        "invalid_boolean": ActionType.STANDARDIZE_BOOLEAN,
    }

    # Category mapping for typed Issue model
    CATEGORY_BY_ACTION: Dict[ActionType, IssueCategory] = {
        ActionType.TRIM: IssueCategory.UNTRIMMED_WHITESPACE,
        ActionType.LOWERCASE: IssueCategory.FORMAT_ANOMALY,
        ActionType.UPPERCASE: IssueCategory.FORMAT_ANOMALY,
        ActionType.FILL_NULLS: IssueCategory.MISSING_VALUE,
        ActionType.ZERO_TO_NULL: IssueCategory.MISSING_VALUE,
        ActionType.REPLACE_SENTINELS: IssueCategory.MISSING_VALUE,
        ActionType.CAST_TYPE: IssueCategory.TYPE_MISMATCH,
        ActionType.COERCE_NUMERIC: IssueCategory.TYPE_MISMATCH,
        ActionType.PARSE_DATES: IssueCategory.FORMAT_ANOMALY,
        ActionType.SANITIZE_EMAIL: IssueCategory.FORMAT_ANOMALY,
        ActionType.NORMALIZE_PHONE: IssueCategory.FORMAT_ANOMALY,
        ActionType.MASK_PHONE: IssueCategory.PII_EXPOSURE,
        ActionType.RANGE_CLIP: IssueCategory.OUTLIER,
        ActionType.CAP_OUTLIERS: IssueCategory.OUTLIER,
        ActionType.STANDARDIZE_BOOLEAN: IssueCategory.FORMAT_ANOMALY,
        ActionType.DEDUPLICATE: IssueCategory.DUPLICATE,
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: IssueCategory.REFERENTIAL_INTEGRITY,
        ActionType.DROP_COLUMN: IssueCategory.OTHER,
        ActionType.CONDITIONAL_UPDATE: IssueCategory.OTHER,
        ActionType.PREFIX_ADD: IssueCategory.FORMAT_ANOMALY,
        ActionType.SUFFIX_ADD: IssueCategory.FORMAT_ANOMALY,
        ActionType.PATTERN_REPLACE: IssueCategory.FORMAT_ANOMALY,
        ActionType.VALUE_MAP: IssueCategory.FORMAT_ANOMALY,
        ActionType.PAD: IssueCategory.FORMAT_ANOMALY,
        ActionType.CUSTOM_EXPRESSION: IssueCategory.OTHER,
    }

    def __init__(self, default_schema: str = "dbo"):
        self.default_schema = default_schema

    def normalize_dataset_name(self, raw_name: str, known_datasets: Optional[Set[str]] = None) -> str:
        """Normalizes dataset name using SqlTableIdentifier and matches against known_datasets."""
        if not raw_name:
            return list(known_datasets)[0] if known_datasets else "default"
        raw_clean = str(raw_name).strip()
        if known_datasets and raw_clean in known_datasets:
            return raw_clean

        try:
            ident = parse_table_identifier(raw_clean, default_schema=self.default_schema)
            if known_datasets:
                for kd in known_datasets:
                    kd_clean = kd.strip()
                    if (
                        kd_clean == ident.table
                        or kd_clean == ident.qualified_name
                        or kd_clean.replace("dbo.", "").strip("[]") == ident.table
                    ):
                        return kd_clean
            return ident.table
        except Exception:
            return re.sub(r"[\[\]]", "", raw_clean).strip()

    def resolve_action(
        self,
        planned_action: Optional[str],
        business_rule: Optional[Dict[str, Any]],
        issue_type: str,
        suggested_action: Optional[str],
        message: str = "",
    ) -> Tuple[Optional[ActionType], str]:
        """Resolves an ActionType following strict controlled precedence:
        1. Explicit planned action
        2. Business-rule action
        3. Normalized issue type
        4. Suggested action text
        5. None (Unsupported)
        """
        # 1. Explicit planned action
        if planned_action:
            act_clean = str(planned_action).strip().lower()
            if act_clean in self.EXPLICIT_ACTION_MAP:
                return self.EXPLICIT_ACTION_MAP[act_clean], "explicit_planned_action"

        # 2. Business-rule action
        if business_rule:
            br_act = str(business_rule.get("action") or business_rule.get("rule_type") or "").strip().lower()
            if br_act in self.EXPLICIT_ACTION_MAP:
                return self.EXPLICIT_ACTION_MAP[br_act], "business_rule_action"

        # 3. Normalized issue type
        itype_clean = str(issue_type or "").strip().lower()
        if itype_clean in self.ISSUE_TYPE_MAP:
            return self.ISSUE_TYPE_MAP[itype_clean], "normalized_issue_type"

        # Substring search in issue type
        for key, act in self.ISSUE_TYPE_MAP.items():
            if key in itype_clean:
                return act, f"issue_type_match:{key}"

        # 4. Suggested action text keywords
        sug_clean = str(suggested_action or "").strip().lower()
        if sug_clean in self.EXPLICIT_ACTION_MAP:
            return self.EXPLICIT_ACTION_MAP[sug_clean], "suggested_action_exact"

        for key, act in self.EXPLICIT_ACTION_MAP.items():
            if key in sug_clean:
                return act, f"suggested_action_match:{key}"

        # Additional keyword inspection on description / message
        msg_clean = str(message or "").strip().lower()
        if "whitespace" in msg_clean or "trim" in msg_clean:
            return ActionType.TRIM, "message_keyword:trim"
        if "email" in msg_clean:
            return ActionType.SANITIZE_EMAIL, "message_keyword:email"
        if "phone" in msg_clean:
            return ActionType.NORMALIZE_PHONE, "message_keyword:phone"
        if "duplicate" in msg_clean:
            return ActionType.DEDUPLICATE, "message_keyword:duplicate"
        if "null" in msg_clean:
            return ActionType.FILL_NULLS, "message_keyword:null"
        if "date" in msg_clean:
            return ActionType.PARSE_DATES, "message_keyword:date"

        # 5. Unsupported
        return None, "unsupported"

    def extract_and_normalize(
        self,
        assess: Dict[str, Any],
        plan: Dict[str, Any],
        known_datasets: Optional[Set[str]] = None,
    ) -> Tuple[List[CanonicalIssue], List[BusinessRule], Dict[str, Any]]:
        """Harvests issues and rules from all assessment and planning containers."""
        raw_issues: List[CanonicalIssue] = []
        business_rules: List[BusinessRule] = []

        total_assessment_discovered = 0
        total_plan_discovered = 0

        # Helper to extract an issue dict
        def _add_raw(
            ds_name: str,
            col: Optional[str],
            itype: str,
            sug: Optional[str],
            sev: str,
            params: Dict[str, Any],
            source: str,
            msg: str = "",
            aff_rows: int = 0,
            rule_id: Optional[str] = None,
            explicit_act: Optional[str] = None,
        ):
            norm_ds = self.normalize_dataset_name(ds_name, known_datasets=known_datasets)
            if col:
                col_str = str(col).strip()
                if self.is_pseudo_column(col_str):
                    col_clean = None
                else:
                    col_clean = col_str.strip("[]")
            else:
                col_clean = None
            iss_id = f"iss_{norm_ds}_{col_clean or 'global'}_{len(raw_issues) + 1}"
            
            action, res_path = self.resolve_action(
                planned_action=explicit_act,
                business_rule=None,
                issue_type=itype,
                suggested_action=sug,
                message=msg,
            )

            # GUARD: Column-bound actions require a real target column
            if action is not None and action not in self.DATASET_LEVEL_ACTIONS and not col_clean:
                action = None
                res_path = "unsupported_missing_target_columns"

            # Parameter enrichment for Tier 2 decomposable primitives
            final_params = dict(params)
            if action == ActionType.RANGE_CLIP:
                if "min_value" not in final_params:
                    if "lower_bound" in final_params:
                        final_params["min_value"] = final_params["lower_bound"]
                    elif "min" in final_params:
                        final_params["min_value"] = final_params["min"]
                if "max_value" not in final_params:
                    if "upper_bound" in final_params:
                        final_params["max_value"] = final_params["upper_bound"]
                    elif "max" in final_params:
                        final_params["max_value"] = final_params["max"]

                has_min = final_params.get("min_value") is not None
                has_max = final_params.get("max_value") is not None

                if not has_min and not has_max:
                    itype_l = itype.lower()
                    col_l = (col_clean or "").lower()
                    msg_l = (msg or "").lower()
                    sug_l = (sug or "").lower()

                    # Domain semantic classification
                    # Columns that may legitimately have negative values in accounting/business domains.
                    # Never clamp these to 0 automatically without explicit business rule.
                    unsafe_neg_cols = (
                        "adjustment", "adjustments", "variance", "temperature", "temp",
                        "profit", "loss", "balance", "coordinates", "coord", "refund", "delta",
                        "amount", "total_amount", "cost", "fee", "rate", "discount", "margin"
                    )
                    explicit_non_neg_types = (
                        "negative_salary", "negative_currency", "negative_quantity", "negative_price",
                        "non_negative", "min_zero"
                    )

                    is_negative_issue = (
                        "negative" in itype_l or "negative" in msg_l or "0" in sug_l
                    )

                    if any(u in col_l for u in unsafe_neg_cols) and is_negative_issue:
                        # Negative values may be legitimate in this domain; clamping to 0 risks silent data corruption.
                        action = None
                        res_path = "unsupported_ambiguous_negative_policy"
                    elif any(t in itype_l for t in explicit_non_neg_types):
                        final_params["min_value"] = 0.0
                        final_params["parameter_source"] = f"issue_type:{itype}"
                        final_params["parameter_confidence"] = "high"
                        final_params["requires_approval"] = False
                    elif "clip" in sug_l and "0" in sug_l:
                        final_params["min_value"] = 0.0
                        final_params["parameter_source"] = "suggested_action:clip_zero"
                        final_params["parameter_confidence"] = "high"
                        final_params["requires_approval"] = False
                    else:
                        # CRITICAL GUARD: Both bounds missing, do NOT infer bounds merely because
                        # column is Salary, Quantity, Age, etc. Bounds must come from assessment evidence,
                        # an approved business rule, approved catalogue metadata, or an approved Tier 3 proposal.
                        action = None
                        res_path = "unsupported_missing_range_bounds"

            elif action == ActionType.REPLACE_SENTINELS:
                if "sentinel_values" not in final_params:
                    # Require explicit detected evidence from assessment
                    detected = (
                        final_params.get("detected_values")
                        or final_params.get("sentinels")
                        or final_params.get("values")
                        or final_params.get("placeholders")
                    )
                    if detected and isinstance(detected, (list, set, tuple)):
                        final_params["sentinel_values"] = list(detected)
                        final_params["parameter_source"] = "assessment_evidence"
                        final_params["parameter_confidence"] = "high"
                    else:
                        # Distinguish explicitly detected vs assumed standard sentinels
                        # Mark clearly with parameter_source and flag for approval
                        final_params["sentinel_values"] = ['NA', 'N/A', 'NULL', 'None', '-999', '?', '-', 'UNKNOWN']
                        final_params["parameter_source"] = "assumed_standard_sentinels"
                        final_params["parameter_confidence"] = "low"
                        if not final_params.get("approved"):
                            final_params["requires_approval"] = True

            elif action == ActionType.CAST_TYPE:
                target_type = (
                    final_params.get("target_type")
                    or final_params.get("to_type")
                    or final_params.get("dtype")
                    or final_params.get("data_type")
                    or final_params.get("type")
                )
                if target_type and str(target_type).strip():
                    final_params["target_type"] = str(target_type).strip()
                else:
                    action = None
                    res_path = "unsupported_missing_target_type"

            elif action == ActionType.TRIM:
                final_params.setdefault("side", "both")

            if source in ("plan_domain_rules_decomposed", "plan_dataset_steps"):
                final_params.setdefault("approved", True)
                final_params["requires_approval"] = False

            raw_issues.append(
                CanonicalIssue(
                    issue_id=iss_id,
                    dataset=norm_ds,
                    raw_dataset=ds_name,
                    column=col_clean,
                    issue_type=itype,
                    suggested_action=sug,
                    severity=sev,
                    parameters=final_params,
                    source=source,
                    rule_id=rule_id,
                    message=msg,
                    affected_rows=aff_rows,
                    resolved_action=action,
                    resolution_path=res_path,
                )
            )

        # ── 1. assess["data_quality_issues"]["datasets"] ───────────────────────
        dq_root = (assess.get("data_quality_issues") or {}).get("datasets") or {}
        for ds_k, block in dq_root.items():
            if isinstance(block, dict):
                for item in (block.get("issues") or []):
                    total_assessment_discovered += 1
                    _add_raw(
                        ds_name=item.get("dataset") or ds_k,
                        col=item.get("column") or item.get("column_name"),
                        itype=item.get("issue_type") or item.get("category") or "unknown_issue",
                        sug=item.get("suggested_action") or item.get("recommendation"),
                        sev=item.get("severity") or "medium",
                        params=item.get("parameters") or {},
                        source="assessment_dq_datasets",
                        msg=item.get("message") or item.get("description") or "",
                        aff_rows=int(item.get("row_count_affected") or item.get("affected_rows") or 0),
                        rule_id=item.get("rule_id"),
                    )

        # ── 2. assess["datasets"][dataset]["quality"]["issues"] & columns ─────
        for ds_k, ds_info in (assess.get("datasets") or {}).items():
            if isinstance(ds_info, dict):
                # 2a. Quality block
                q_block = ds_info.get("quality") or {}
                for item in (q_block.get("issues") or []):
                    total_assessment_discovered += 1
                    _add_raw(
                        ds_name=item.get("dataset") or ds_k,
                        col=item.get("column") or item.get("column_name"),
                        itype=item.get("issue_type") or item.get("category") or "unknown_issue",
                        sug=item.get("suggested_action") or item.get("recommendation"),
                        sev=item.get("severity") or "medium",
                        params=item.get("parameters") or {},
                        source="assessment_dataset_quality",
                        msg=item.get("message") or item.get("description") or "",
                        aff_rows=int(item.get("row_count_affected") or item.get("affected_rows") or 0),
                        rule_id=item.get("rule_id"),
                    )
                # 2b. Columns block
                for col_k, col_meta in (ds_info.get("columns") or {}).items():
                    if isinstance(col_meta, dict):
                        for item in (col_meta.get("issues") or []):
                            total_assessment_discovered += 1
                            _add_raw(
                                ds_name=item.get("dataset") or ds_k,
                                col=col_k,
                                itype=item.get("issue_type") or item.get("category") or "unknown_issue",
                                sug=item.get("suggested_action") or item.get("recommendation"),
                                sev=item.get("severity") or "medium",
                                params=item.get("parameters") or {},
                                source="assessment_column_issues",
                                msg=item.get("message") or item.get("description") or "",
                                aff_rows=int(item.get("row_count_affected") or item.get("affected_rows") or 0),
                                rule_id=item.get("rule_id"),
                            )

        # ── 3. assess["data_quality_issues"]["global_issues"] ──────────────────
        gi = (assess.get("data_quality_issues") or {}).get("global_issues") or {}
        for orphan in (gi.get("orphan_foreign_keys") or []):
            total_assessment_discovered += 1
            child_ds = orphan.get("child_dataset") or orphan.get("dataset") or ""
            child_col = orphan.get("child_key") or orphan.get("column") or ""
            parent_ds = orphan.get("parent_dataset") or ""
            parent_col = orphan.get("parent_key") or ""
            params = {
                "child_key": child_col,
                "parent_dataset": self.normalize_dataset_name(parent_ds, known_datasets=known_datasets),
                "parent_key": parent_col,
            }
            _add_raw(
                ds_name=child_ds,
                col=child_col,
                itype="orphan_foreign_keys",
                sug="validate_referential_integrity",
                sev="high",
                params=params,
                source="assessment_global_orphan_keys",
                msg=f"Orphan foreign key in {child_col} referencing {parent_ds}.{parent_col}",
                explicit_act="validate_referential_integrity",
            )

        for rri in (gi.get("relationship_row_issues") or []):
            total_assessment_discovered += 1
            _add_raw(
                ds_name=rri.get("dataset") or "",
                col=rri.get("column"),
                itype=rri.get("issue_type") or "relationship_row_issue",
                sug="validate_referential_integrity",
                sev="high",
                params=rri.get("parameters") or {},
                source="assessment_relationship_row_issues",
                msg=rri.get("message") or "",
            )

        # ── 4. plan["datasets"][dataset]["steps"] ─────────────────────────────
        for ds_k, p_ds in (plan.get("datasets") or {}).items():
            if isinstance(p_ds, dict):
                for step in (p_ds.get("steps") or []):
                    total_plan_discovered += 1
                    act = step.get("action") or step.get("strategy")
                    _add_raw(
                        ds_name=step.get("dataset") or ds_k,
                        col=step.get("column"),
                        itype=step.get("issue_type") or f"planned_{act}",
                        sug=act,
                        sev="medium",
                        params=step.get("params") or step.get("parameters") or {},
                        source="plan_dataset_steps",
                        msg=step.get("description") or f"Planned action: {act}",
                        rule_id=step.get("step_id") or step.get("rule_id"),
                        explicit_act=act,
                    )

        # ── 5. plan["domain_rules"] ───────────────────────────────────────────
        from agent.compiler.rules.decomposer import RuleDecomposer
        from agent.compiler.rules.models import DecompositionStatus
        decomposer = RuleDecomposer()

        for rule_k, rule_data in (plan.get("domain_rules") or {}).items():
            if isinstance(rule_data, dict):
                ds = rule_data.get("dataset") or (rule_k.split(".")[0] if "." in rule_k else "")
                col = rule_data.get("column") or (rule_k.split(".")[1] if "." in rule_k else None)
                norm_ds = self.normalize_dataset_name(ds, known_datasets=known_datasets)
                rule_text = rule_data.get("rule") or rule_data.get("expression") or rule_data.get("text") or rule_data.get("description") or ""

                # Tier 2: Decompose business rule text into StructuredActions
                if rule_text:
                    decomp_res = decomposer.decompose(rule_text, norm_ds)
                    if decomp_res.status == DecompositionStatus.RESOLVED and decomp_res.actions:
                        for act in decomp_res.actions:
                            _add_raw(
                                ds_name=norm_ds,
                                col=act.target_columns[0] if act.target_columns else col,
                                itype=f"rule_{act.action_type.value}",
                                sug=act.action_type.value,
                                sev="medium",
                                params=dict(act.parameters),
                                source="plan_domain_rules_decomposed",
                                msg=rule_text,
                                rule_id=rule_k,
                                explicit_act=act.action_type.value,
                            )

                b_rule = BusinessRule(
                    rule_id=rule_data.get("rule_id") or rule_k,
                    name=rule_data.get("name") or rule_k,
                    scope=RuleScope.COLUMN if col else RuleScope.DATASET,
                    dataset_name=norm_ds,
                    column_name=col,
                    parameters=rule_data.get("parameters") or {},
                    enabled=rule_data.get("enabled", True),
                    never_drop_rows=rule_data.get("never_drop_rows", False),
                    description=rule_data.get("description") or f"Business rule for {rule_k}",
                )
                business_rules.append(b_rule)

        # ── 6. Deduplication & Consolidation ──────────────────────────────────
        deduped: List[CanonicalIssue] = []
        seen_keys: Set[Tuple[str, Optional[str], Optional[ActionType], str]] = set()
        duplicates_removed = 0

        for iss in raw_issues:
            # Key combines dataset, column, resolved action (if any), and normalized issue type
            key = (
                iss.dataset.lower(),
                (iss.column or "").lower() if iss.column else None,
                iss.resolved_action,
                iss.issue_type.lower(),
            )
            if key in seen_keys:
                duplicates_removed += 1
                # Merge parameters with existing issue if complementary
                for existing in deduped:
                    existing_key = (
                        existing.dataset.lower(),
                        (existing.column or "").lower() if existing.column else None,
                        existing.resolved_action,
                        existing.issue_type.lower(),
                    )
                    if existing_key == key:
                        for pk, pv in iss.parameters.items():
                            if pk not in existing.parameters:
                                existing.parameters[pk] = pv
                continue

            seen_keys.add(key)
            deduped.append(iss)

        # ── 7. Separate Mapped vs Unsupported vs Manual Review ────────────────
        mapped_count = 0
        manual_review_issues: List[Dict[str, Any]] = []
        unsupported_issues: List[Dict[str, Any]] = []

        for iss in deduped:
            if iss.source == "manual_review" or iss.suggested_action == "manual_review":
                manual_info = {
                    "issue_id": iss.issue_id,
                    "dataset": iss.dataset,
                    "column": iss.column,
                    "issue_type": iss.issue_type,
                    "message": iss.message,
                    "source": iss.source,
                    "reason": "Manual review finding in assessment",
                    "action_type": None,
                }
                manual_review_issues.append(manual_info)
            elif iss.resolved_action is not None:
                mapped_count += 1
            else:
                unsupported_info = {
                    "issue_id": iss.issue_id,
                    "dataset": iss.dataset,
                    "column": iss.column,
                    "issue_type": iss.issue_type,
                    "message": iss.message,
                    "source": iss.source,
                    "reason": iss.resolution_path or "No supported action for issue",
                }
                unsupported_issues.append(unsupported_info)
                logger.warning(
                    "[IngestionBridge] UNSUPPORTED: %s on %s%s (type=%s)",
                    iss.issue_id,
                    iss.dataset,
                    f".{iss.column}" if iss.column else "",
                    iss.issue_type,
                )

        missing_range_bounds_count = sum(1 for iss in deduped if iss.resolution_path == "unsupported_missing_range_bounds")
        missing_target_cols_count = sum(1 for iss in deduped if iss.resolution_path == "unsupported_missing_target_columns")
        missing_target_type_count = sum(1 for iss in deduped if iss.resolution_path == "unsupported_missing_target_type")

        telemetry = {
            "assessment_discovered_count": total_assessment_discovered,
            "plan_discovered_count": total_plan_discovered,
            "normalized_issue_count": len(deduped),
            "duplicates_removed_count": duplicates_removed,
            "mapped_issue_count": mapped_count,
            "mapped_action_count": mapped_count,
            "unsupported_issue_count": len(unsupported_issues),
            "unsupported_missing_range_bounds_count": missing_range_bounds_count,
            "unsupported_missing_target_columns_count": missing_target_cols_count,
            "unsupported_missing_target_type_count": missing_target_type_count,
            "manual_review_issue_count": len(manual_review_issues),
            "unsupported_issues": unsupported_issues,
            "manual_review_issues": manual_review_issues,
        }

        logger.info(
            "[IngestionBridge] Summary: assessment_discovered=%d, plan_discovered=%d, "
            "normalized=%d, deduped_removed=%d, mapped=%d, unsupported=%d, manual_review=%d",
            total_assessment_discovered,
            total_plan_discovered,
            len(deduped),
            duplicates_removed,
            mapped_count,
            len(unsupported_issues),
            len(manual_review_issues),
        )

        return deduped, business_rules, telemetry

    def bridge_to_compiler_models(
        self,
        canonical_issues: List[CanonicalIssue],
    ) -> Tuple[List[Issue], List[StructuredAction]]:
        """Converts canonical issues into typed Issue models and executable StructuredAction models."""
        issues: List[Issue] = []
        structured_actions: List[StructuredAction] = []

        for idx, c_iss in enumerate(canonical_issues):
            action = c_iss.resolved_action
            cat = self.CATEGORY_BY_ACTION.get(action, IssueCategory.OTHER) if action else IssueCategory.OTHER

            # Build typed Issue model
            issue_model = Issue(
                issue_id=c_iss.issue_id,
                dataset_name=c_iss.dataset,
                column_name=c_iss.column,
                category=cat,
                severity=IssueSeverity(c_iss.severity.lower()) if c_iss.severity.lower() in ("high", "medium", "low", "info") else IssueSeverity.MEDIUM,
                message=c_iss.message or f"Issue {c_iss.issue_type} in {c_iss.column or c_iss.dataset}",
                affected_rows=c_iss.affected_rows,
                suggested_action=action.value if action else c_iss.suggested_action,
                parameters=dict(c_iss.parameters),
            )
            issues.append(issue_model)

            # Build executable StructuredAction if action resolved
            if action is not None and c_iss.source != "manual_review" and c_iss.suggested_action != "manual_review":
                cols = [c_iss.column] if c_iss.column else []
                # Referential integrity actions use child_key from parameters if column is missing
                if action == ActionType.VALIDATE_REFERENTIAL_INTEGRITY and not cols and c_iss.parameters.get("child_key"):
                    cols = [c_iss.parameters["child_key"]]

                # Handle composite column strings (e.g., "EmployeeID, EffectiveFrom")
                if cols and "," in cols[0]:
                    cols = [c.strip().strip("[]") for c in cols[0].split(",") if c.strip()]

                # Filter out pseudo-column markers from target_columns
                cols = [
                    c for c in cols
                    if c and not self.is_pseudo_column(c)
                ]

                # Deduplicate actions: if target_columns is empty, auto-assign from parameters keys/subset_columns
                if action == ActionType.DEDUPLICATE:
                    if not cols:
                        candidate_keys = (
                            c_iss.parameters.get("subset_columns")
                            or c_iss.parameters.get("keys")
                            or c_iss.parameters.get("candidate_keys")
                            or c_iss.parameters.get("column_keys")
                            or []
                        )
                        if isinstance(candidate_keys, str):
                            candidate_keys = [k.strip().strip("[]") for k in candidate_keys.split(",") if k.strip()]
                        if isinstance(candidate_keys, (list, tuple)):
                            cols = [
                                k for k in candidate_keys
                                if k and not self.is_pseudo_column(k)
                            ]
                    # Ensure parameters have subset_columns set
                    c_iss.parameters["subset_columns"] = list(cols)

                # DEFENSE IN DEPTH GUARD 1: Missing target columns for column-bound action
                if action not in self.DATASET_LEVEL_ACTIONS and not cols:
                    logger.warning(
                        "[IngestionBridge] Dropping action '%s' on %s because target_columns is empty after pseudo-column removal.",
                        action.value,
                        c_iss.dataset,
                    )
                    c_iss.resolved_action = None
                    c_iss.resolution_path = "unsupported_missing_target_columns"
                    continue

                # DEFENSE IN DEPTH GUARD 2: Missing bounds for RANGE_CLIP
                if action == ActionType.RANGE_CLIP:
                    has_min = c_iss.parameters.get("min_value") is not None
                    has_max = c_iss.parameters.get("max_value") is not None
                    if not has_min and not has_max:
                        logger.warning(
                            "[IngestionBridge] Dropping RANGE_CLIP on %s.%s because neither min_value nor max_value was provided.",
                            c_iss.dataset,
                            cols,
                        )
                        c_iss.resolved_action = None
                        c_iss.resolution_path = "unsupported_missing_range_bounds"
                        continue

                # DEFENSE IN DEPTH GUARD 3: Missing target_type for CAST_TYPE
                if action == ActionType.CAST_TYPE:
                    if not c_iss.parameters.get("target_type"):
                        logger.warning(
                            "[IngestionBridge] Dropping CAST_TYPE on %s.%s because target_type was not provided.",
                            c_iss.dataset,
                            cols,
                        )
                        c_iss.resolved_action = None
                        c_iss.resolution_path = "unsupported_missing_target_type"
                        continue

                source_enum = ActionSource.ASSESSMENT
                if c_iss.source.startswith("plan_"):
                    source_enum = ActionSource.CATALOG

                s_action = StructuredAction(
                    action_id=f"action_{c_iss.issue_id}",
                    action_type=action,
                    dataset_name=c_iss.dataset,
                    target_columns=cols,
                    parameters=dict(c_iss.parameters),
                    source=source_enum,
                    source_reference=c_iss.rule_id or c_iss.issue_id,
                    source_issue_ids=[c_iss.issue_id],
                    extraction_method=c_iss.resolution_path or "deterministic",
                    confidence=1.0,
                    requires_approval=bool(c_iss.parameters.get("requires_approval", False)),
                    metadata={"canonical_source": c_iss.source, "issue_type": c_iss.issue_type},
                )

                # UNIVERSAL DEFENSE IN DEPTH GUARD: Validate typed parameter contract before emission
                try:
                    s_action.get_typed_parameters()
                except Exception as val_err:
                    logger.warning(
                        "[IngestionBridge] Dropping action '%s' on %s.%s due to invalid parameter contract: %s",
                        action.value,
                        c_iss.dataset,
                        cols,
                        val_err,
                    )
                    c_iss.resolved_action = None
                    c_iss.resolution_path = f"unsupported_invalid_parameters:{action.value}"
                    continue

                structured_actions.append(s_action)

        return issues, structured_actions
