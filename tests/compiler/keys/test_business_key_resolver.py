import pytest
from agent.compiler.keys.business_key_resolver import BusinessKeyResolver
from agent.compiler.keys.key_candidate import KeySource
from agent.compiler.keys.key_policy import KeyPolicy
from agent.compiler.models.profile import ColumnProfile, DatasetProfile


def test_business_key_priority_user_approved():
    resolver = BusinessKeyResolver()
    profile = DatasetProfile(dataset_name="users", row_count=100)
    
    res = resolver.resolve_key(
        dataset_name="users",
        profile=profile,
        user_approved_keys=["user_id"],
        db_primary_keys=["legacy_id"],
    )
    assert res.source == KeySource.USER_APPROVED
    assert res.columns == ["user_id"]
    assert res.confidence == 100.0
    assert res.is_usable_for_destructive_dedup is True


def test_business_key_priority_database_pk():
    resolver = BusinessKeyResolver()
    profile = DatasetProfile(dataset_name="orders", row_count=500)

    res = resolver.resolve_key(
        dataset_name="orders",
        profile=profile,
        db_primary_keys=["order_number"],
    )
    assert res.source == KeySource.DATABASE_PK
    assert res.columns == ["order_number"]
    assert res.confidence >= 95.0
    assert res.is_usable_for_destructive_dedup is True


def test_business_key_penalizes_mutable_and_dates():
    penalized_date, date_reasons = KeyPolicy.is_penalized_column("created_at")
    assert penalized_date is True
    assert any("date/time" in r for r in date_reasons)

    penalized_status, status_reasons = KeyPolicy.is_penalized_column("order_status")
    assert penalized_status is True
    assert any("status" in r for r in status_reasons)

    penalized_id, id_reasons = KeyPolicy.is_penalized_column("customer_id")
    assert penalized_id is False
