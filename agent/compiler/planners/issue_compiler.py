from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity
from agent.compiler.models.logical import ActionType, DataEffect, LogicalStep


class IssueCompiler:
    """Translates raw assessment issues & findings into typed Issue models and LogicalSteps."""

    CATEGORY_MAP: Dict[str, IssueCategory] = {
        "null_values": IssueCategory.MISSING_VALUE,
        "missing_values": IssueCategory.MISSING_VALUE,
        "mixed_types": IssueCategory.TYPE_MISMATCH,
        "type_mismatch": IssueCategory.TYPE_MISMATCH,
        "invalid_email": IssueCategory.FORMAT_ANOMALY,
        "invalid_phone": IssueCategory.FORMAT_ANOMALY,
        "invalid_date": IssueCategory.FORMAT_ANOMALY,
        "outliers": IssueCategory.OUTLIER,
        "duplicates": IssueCategory.DUPLICATE,
        "referential_integrity": IssueCategory.REFERENTIAL_INTEGRITY,
        "pii": IssueCategory.PII_EXPOSURE,
        "whitespace": IssueCategory.UNTRIMMED_WHITESPACE,
    }

    ACTION_MAP: Dict[IssueCategory, ActionType] = {
        IssueCategory.UNTRIMMED_WHITESPACE: ActionType.TRIM,
        IssueCategory.MISSING_VALUE: ActionType.FILL_NULLS,
        IssueCategory.TYPE_MISMATCH: ActionType.CAST_TYPE,
        IssueCategory.FORMAT_ANOMALY: ActionType.PARSE_DATES,
        IssueCategory.OUTLIER: ActionType.RANGE_CLIP,
        IssueCategory.DUPLICATE: ActionType.DEDUPLICATE,
        IssueCategory.REFERENTIAL_INTEGRITY: ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        IssueCategory.PII_EXPOSURE: ActionType.MASK_PHONE,
    }

    def compile_raw_issue(self, raw: Dict[str, Any], dataset_name: str, index: int) -> Issue:
        raw_cat = str(raw.get("issue_type") or raw.get("category") or "other").lower()
        
        category = IssueCategory.OTHER
        for key, cat in self.CATEGORY_MAP.items():
            if key in raw_cat:
                category = cat
                break

        raw_sev = str(raw.get("severity") or "medium").lower()
        severity = IssueSeverity.MEDIUM
        if raw_sev == "high":
            severity = IssueSeverity.HIGH
        elif raw_sev == "low":
            severity = IssueSeverity.LOW
        elif raw_sev == "info":
            severity = IssueSeverity.INFO

        col = raw.get("column") or raw.get("column_name")
        affected = int(raw.get("row_count_affected") or raw.get("affected_rows") or 0)
        
        return Issue(
            issue_id=f"iss_{dataset_name}_{col or 'global'}_{index}",
            dataset_name=dataset_name,
            column_name=col,
            category=category,
            severity=severity,
            message=str(raw.get("message") or raw.get("description") or f"Issue in {col}"),
            affected_rows=affected,
            suggested_action=raw.get("suggested_action") or raw.get("recommendation"),
            parameters=dict(raw.get("parameters") or {}),
        )

    def issue_to_logical_step(self, issue: Issue, step_index: int) -> LogicalStep:
        action = self.ACTION_MAP.get(issue.category, ActionType.CUSTOM_EXPRESSION)
        
        # Check suggested_action first
        sug = str(issue.suggested_action or "").strip().lower()
        if sug in ("trim", "str_strip"):
            action = ActionType.TRIM
        elif sug in ("lowercase", "lower", "str_lower"):
            action = ActionType.LOWERCASE
        elif sug in ("uppercase", "upper", "str_upper"):
            action = ActionType.UPPERCASE
        elif sug in ("zero_to_null", "nullif_zero"):
            action = ActionType.ZERO_TO_NULL
        elif sug in ("replace_sentinels", "sentinel"):
            action = ActionType.REPLACE_SENTINELS
        elif sug in ("drop_duplicates", "deduplicate"):
            action = ActionType.DEDUPLICATE
        elif issue.category == IssueCategory.FORMAT_ANOMALY or issue.category == IssueCategory.OTHER:
            msg = issue.message.lower()
            if "lower" in msg:
                action = ActionType.LOWERCASE
            elif "upper" in msg:
                action = ActionType.UPPERCASE
            elif "email" in msg:
                action = ActionType.SANITIZE_EMAIL
            elif "phone" in msg:
                action = ActionType.NORMALIZE_PHONE
            elif "date" in msg:
                action = ActionType.PARSE_DATES

        cols = [issue.column_name] if issue.column_name else []
        effect = DataEffect.TRANSFORM_IN_PLACE
        if action == ActionType.DEDUPLICATE:
            effect = DataEffect.FILTER_ROWS
        elif action == ActionType.DROP_COLUMN:
            effect = DataEffect.DROP_COLUMN
        elif action == ActionType.VALIDATE_REFERENTIAL_INTEGRITY:
            effect = DataEffect.QUARANTINE_ROWS

        return LogicalStep(
            step_id=f"step_{issue.dataset_name}_{action.value}_{step_index}",
            dataset_name=issue.dataset_name,
            action=action,
            target_columns=cols,
            parameters=dict(issue.parameters),
            effect=effect,
            source_issue_ids=[issue.issue_id],
            description=f"Remediate {issue.category.value}: {issue.message}",
        )
