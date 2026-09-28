import pytest
from agent.compiler.verification.post_cleaning_verifier import (
    CleaningStatus,
    PostCleaningVerifier,
)


def test_post_cleaning_verification_perfect_clean():
    # 1000 input = 900 clean + 50 quarantined + 50 removed
    report = PostCleaningVerifier.verify(
        dataset_name="sales_orders",
        input_rows=1000,
        clean_rows=900,
        quarantined_rows=50,
        removed_redundant_rows=50,
        remaining_duplicates=0,
        expected_clean_cols=["order_id", "customer_id", "total_amount"],
        actual_clean_cols=["order_id", "customer_id", "total_amount"],
    )
    assert report.status == CleaningStatus.CLEANED
    assert report.is_fully_verified is True
    assert report.row_count_reconciled is True
    assert len(report.violations) == 0


def test_post_cleaning_verification_catches_row_loss():
    # 1000 input != 800 clean + 0 quarantined + 50 removed (150 rows silently disappeared!)
    report = PostCleaningVerifier.verify(
        dataset_name="payments",
        input_rows=1000,
        clean_rows=800,
        quarantined_rows=0,
        removed_redundant_rows=50,
        remaining_duplicates=0,
    )
    assert report.status == CleaningStatus.FAILED
    assert report.is_fully_verified is False
    assert report.row_count_reconciled is False
    assert any("Row reconciliation failed" in v for v in report.violations)


def test_post_cleaning_verification_audit_only_status():
    report = PostCleaningVerifier.verify(
        dataset_name="logs",
        input_rows=500,
        clean_rows=500,
        is_audit_only=True,
    )
    assert report.status == CleaningStatus.AUDITED_NOT_CLEANED
    assert report.is_fully_verified is True


def test_post_cleaning_verification_catches_unresolved_duplicates():
    # Deduplication executed but 5 duplicates remained
    report = PostCleaningVerifier.verify(
        dataset_name="customers",
        input_rows=100,
        clean_rows=95,
        removed_redundant_rows=5,
        remaining_duplicates=5,
    )
    assert report.status == CleaningStatus.PARTIALLY_CLEANED
    assert report.is_fully_verified is False
    assert any("5 targeted duplicates remain" in v for v in report.violations)
