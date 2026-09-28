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
        "outliers": ActionType.RANGE_CLIP,
        "negative_values": ActionType.RANGE_CLIP,
        "non_negative": ActionType.RANGE_CLIP,
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
            col_clean = str(col).strip("[]") if col else None
            iss_id = f"iss_{norm_ds}_{col_clean or 'global'}_{len(raw_issues) + 1}"
            
            action, res_path = self.resolve_action(
                planned_action=explicit_act,
                business_rule=None,
                issue_type=itype,
                suggested_action=sug,
                message=msg,
            )

            # Parameter enrichment for Tier 2 decomposable primitives
            final_params = dict(params)
            if action == ActionType.RANGE_CLIP:
                if "min_value" not in final_params and "max_value" not in final_params:
                    if any(w in itype.lower() for w in ("negative", "non_negative", "min_zero")):
                        final_params["min_value"] = 0.0
            elif action == ActionType.REPLACE_SENTINELS:
                if "sentinel_values" not in final_params:
                    final_params["sentinel_values"] = ['NA', 'N/A', 'NULL', 'None', '-999', '?', '-', 'UNKNOWN']
            elif action == ActionType.TRIM:
                final_params.setdefault("side", "both")

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

        # ── 7. Separate Mapped vs Unsupported ─────────────────────────────────
        mapped_count = 0
        unsupported_issues: List[Dict[str, Any]] = []

        for iss in deduped:
            if iss.resolved_action is not None:
                mapped_count += 1
            else:
                unsupported_info = {
                    "issue_id": iss.issue_id,
                    "dataset": iss.dataset,
                    "column": iss.column,
                    "issue_type": iss.issue_type,
                    "message": iss.message,
                    "source": iss.source,
                }
                unsupported_issues.append(unsupported_info)
                logger.warning(
                    "[IngestionBridge] UNSUPPORTED: %s on %s%s (type=%s)",
                    iss.issue_id,
                    iss.dataset,
                    f".{iss.column}" if iss.column else "",
                    iss.issue_type,
                )

        manual_review_count = sum(1 for iss in deduped if iss.source == "manual_review")
        telemetry = {
            "assessment_discovered_count": total_assessment_discovered,
            "plan_discovered_count": total_plan_discovered,
            "normalized_issue_count": len(deduped),
            "duplicates_removed_count": duplicates_removed,
            "mapped_issue_count": mapped_count,
            "mapped_action_count": mapped_count,
            "unsupported_issue_count": len(unsupported_issues),
            "manual_review_issue_count": manual_review_count,
            "unsupported_issues": unsupported_issues,
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
            manual_review_count,
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
            if action is not None:
                cols = [c_iss.column] if c_iss.column else []
                # Referential integrity actions use child_key from parameters if column is missing
                if action == ActionType.VALIDATE_REFERENTIAL_INTEGRITY and not cols and c_iss.parameters.get("child_key"):
                    cols = [c_iss.parameters["child_key"]]

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
                    metadata={"canonical_source": c_iss.source, "issue_type": c_iss.issue_type},
                )
                structured_actions.append(s_action)

        return issues, structured_actions
