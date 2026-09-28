import pytest
from agent.compiler.models.cleaning import CleaningMode
from agent.compiler.schema.column_registry import ColumnKind, ColumnLifecycleRegistry


def test_column_lifecycle_registry_registration_and_projection():
    registry = ColumnLifecycleRegistry(
        dataset_id="inventory",
        mode=CleaningMode.CLEANSE_ONLY,
    )
    registry.register_source("item_id", "string")
    registry.register_source("qty", "integer")

    # Internal scratch column generated during deduplication
    registry.register_generated("__row_number", ColumnKind.INTERNAL, producer_step_id="step_dedup")

    # Audit column generated during flagging
    registry.register_generated("flag_qty", ColumnKind.AUDIT, producer_step_id="step_flag")

    # In CLEANSE_ONLY, final projection must ONLY include source/clean columns
    clean_cols = registry.final_projection(preserve_source_schema=True)
    assert clean_cols == ["item_id", "qty"]
    assert "__row_number" not in clean_cols
    assert "flag_qty" not in clean_cols


def test_column_lifecycle_registry_unresolved_reference():
    registry = ColumnLifecycleRegistry(
        dataset_id="employees",
        mode=CleaningMode.CLEANSE_WITH_AUDIT,
    )
    registry.register_source("emp_id", "string")

    # Valid reference
    registry.require("emp_id", "step_trim_id")

    # Invalid unresolved reference must fail
    with pytest.raises(ValueError, match="Unresolved column reference"):
        registry.require("non_existent_column", "step_invalid")


def test_column_lifecycle_registry_builds_valid_contract():
    registry = ColumnLifecycleRegistry(
        dataset_id="enrollments",
        mode=CleaningMode.CLEANSE_ONLY,
    )
    registry.register_source("enrollment_id", "string")
    registry.register_source("course_id", "string")
    registry.register_generated("is_duplicate_row", ColumnKind.AUDIT, producer_step_id="step_dedup")

    contract = registry.build_contract(preserve_source_schema=True)
    assert contract.source_columns == ["enrollment_id", "course_id"]
    assert contract.clean_columns == ["enrollment_id", "course_id"]
    assert contract.audit_columns == ["is_duplicate_row"]
    assert contract.mode == CleaningMode.CLEANSE_ONLY
