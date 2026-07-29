import pytest
from agent.etl_pipeline.semantic_classifier import profile_column

def test_profile_column_numeric():
    prof = profile_column("age", "int", "metric")
    assert prof.is_numeric is True
    assert prof.is_categorical is False
    assert prof.is_temporal is False
    assert prof.is_identifier is False

def test_profile_column_date():
    prof = profile_column("created_at", "datetime", "date")
    assert prof.is_numeric is False
    assert prof.is_categorical is False
    assert prof.is_temporal is True
    assert prof.is_identifier is False

def test_profile_column_id():
    prof = profile_column("student_id", "varchar", "id")
    assert prof.is_numeric is False
    assert prof.is_categorical is False
    assert prof.is_temporal is False
    assert prof.is_identifier is True

def test_profile_column_categorical():
    prof = profile_column("status", "varchar", "categorical")
    assert prof.is_numeric is False
    assert prof.is_categorical is True
    assert prof.is_temporal is False
    assert prof.is_identifier is False
