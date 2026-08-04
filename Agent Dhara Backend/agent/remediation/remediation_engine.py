"""
Semantic-Aware Safe Remediation Engine.
Generates structured recommendations with preconditions, transformations, risk levels,
rollback plans, and post-fix validation rules.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from agent.models.assessment_result import DQIssue


class RecommendationCategory:
    SAFE_AUTO_FIX = "safe_automatic_fix"
    CONDITIONAL_AUTO_FIX = "conditional_automatic_fix"
    REFERENCE_MAPPING = "reference_data_mapping"
    MANUAL_REVIEW = "manual_review"
    RECORD_QUARANTINE = "record_quarantine"
    SOURCE_SYSTEM_CORRECTION = "source_system_correction"
    CONTRACT_CORRECTION = "contract_correction"


class StructuredRecommendation(BaseModel):
    model_config = ConfigDict(extra="allow")
    rule_id: str
    column: Optional[str] = None
    category: str = RecommendationCategory.MANUAL_REVIEW
    precondition: str
    transformation: str
    expected_effect: str
    risk_level: str = "LOW"  # LOW, MEDIUM, HIGH
    rollback_plan: str
    post_fix_validation_rule: str
    confidence: float = 1.0


def generate_safe_remediations(issues: List[DQIssue]) -> List[StructuredRecommendation]:
    """
    Generates semantic-aware, safe remediation plans for identified DQ issues.
    """
    recommendations: List[StructuredRecommendation] = []

    for issue in issues:
        rule_str = issue.rule_id or issue.type
        col_str = issue.column

        if issue.type in ("whitespace_inconsistency", "leading_trailing_whitespace"):
            recommendations.append(StructuredRecommendation(
                rule_id=rule_str,
                column=col_str,
                category=RecommendationCategory.SAFE_AUTO_FIX,
                precondition=f"Values in column {col_str} differ only by leading/trailing whitespace",
                transformation=f"df['{col_str}'] = df['{col_str}'].astype(str).str.strip()",
                expected_effect="Eliminates outer whitespace without altering character content",
                risk_level="LOW",
                rollback_plan=f"Restore original column {col_str} from raw_values snapshot",
                post_fix_validation_rule="check_no_outer_whitespace",
                confidence=1.0
            ))
        elif issue.type in ("missing_values", "null_check"):
            recommendations.append(StructuredRecommendation(
                rule_id=rule_str,
                column=col_str,
                category=RecommendationCategory.MANUAL_REVIEW,
                precondition=f"Null values present in required column {col_str}",
                transformation=f"Flag or fill nulls in {col_str} using business default value or source lookup",
                expected_effect="Ensures 100% completeness for required column",
                risk_level="MEDIUM",
                rollback_plan="Revert default fill",
                post_fix_validation_rule="check_null_percentage_zero",
                confidence=0.85
            ))
        elif issue.type in ("invalid_email", "invalid_phone"):
            recommendations.append(StructuredRecommendation(
                rule_id=rule_str,
                column=col_str,
                category=RecommendationCategory.RECORD_QUARANTINE,
                precondition=f"Invalid format detected in sensitive column {col_str}",
                transformation=f"Quarantine rows failing regex/checksum for column {col_str}",
                expected_effect="Prevents invalid contact data from reaching production downstream",
                risk_level="MEDIUM",
                rollback_plan="Re-ingest quarantined rows after manual correction",
                post_fix_validation_rule="check_format_validity",
                confidence=0.90
            ))
        else:
            recommendations.append(StructuredRecommendation(
                rule_id=rule_str,
                column=col_str,
                category=RecommendationCategory.MANUAL_REVIEW,
                precondition=f"Issue {issue.type} detected on column {col_str}",
                transformation=issue.recommended_action or f"Inspect and resolve issue {issue.type}",
                expected_effect="Resolve data defect",
                risk_level="MEDIUM",
                rollback_plan="Restore from raw dataset snapshot",
                post_fix_validation_rule="re_evaluate_rule",
                confidence=0.80
            ))

    return recommendations
