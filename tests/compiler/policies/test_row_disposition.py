import pytest
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition
from agent.compiler.policies.risk_classifier import RiskClassifier
from agent.compiler.policies.cleaning_policy import CleaningPolicy
from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType, ValidationResult


def test_risk_classification_destructive_removal():
    risk = RiskClassifier.classify_action("deduplicate_business_key", RowDisposition.REMOVE)
    assert risk == RiskLevel.HIGH

    # Exact duplicates are low risk
    risk_exact = RiskClassifier.classify_action("deduplicate_exact", RowDisposition.REMOVE)
    assert risk_exact == RiskLevel.LOW


def test_cleaning_policy_blocks_unapproved_destructive_action():
    decision = CleaningPolicy.evaluate_step(
        action="deduplicate_business_key",
        row_disposition=RowDisposition.REMOVE,
        is_user_approved=False,
    )
    assert decision.risk_level == RiskLevel.HIGH
    assert decision.requires_approval is True
    assert decision.is_auto_applicable is False


def test_cleaning_policy_allows_approved_action():
    decision = CleaningPolicy.evaluate_step(
        action="deduplicate_business_key",
        row_disposition=RowDisposition.REMOVE,
        is_user_approved=True,
    )
    assert decision.requires_approval is False
    assert decision.is_auto_applicable is True


def test_validation_result_catches_blocking_errors():
    val = ValidationResult(
        is_valid=False,
        target_engine="python",
        issues=[
            DiagnosticIssue(
                issue_code="ERR_BLOCKING_UNAPPROVED",
                issue_type=ValidationIssueType.DESTRUCTIVE_UNAPPROVED,
                severity=DiagnosticSeverity.BLOCKING,
                message="Unapproved destructive deduplication step detected",
            )
        ],
    )
    assert val.has_blocking_errors is True
    assert val.error_count == 1
