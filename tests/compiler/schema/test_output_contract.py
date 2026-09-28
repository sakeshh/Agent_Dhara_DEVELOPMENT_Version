import pytest
from agent.compiler.models.cleaning import CleaningMode
from agent.compiler.schema.output_contract import OutputSchemaContract, GeneratedColumn


def test_output_schema_contract_clean_projection_cleanse_only():
    contract = OutputSchemaContract(
        dataset_id="customers",
        mode=CleaningMode.CLEANSE_ONLY,
        source_columns=["customer_id", "name", "email"],
        clean_columns=["customer_id", "name", "email"],
        audit_columns=["flag_duplicate", "is_duplicate_row"],
        internal_columns=["__row_number"],
        preserve_source_schema=True,
    )

    valid, violations = contract.validate_clean_projection(["customer_id", "name", "email"])
    assert valid is True
    assert len(violations) == 0


def test_output_schema_contract_rejects_audit_leak_in_cleanse_only():
    contract = OutputSchemaContract(
        dataset_id="orders",
        mode=CleaningMode.CLEANSE_ONLY,
        source_columns=["order_id", "amount"],
        clean_columns=["order_id", "amount", "is_duplicate_row"],
        audit_columns=["is_duplicate_row", "row_validation_reason"],
        internal_columns=["__row_number"],
        preserve_source_schema=True,
    )

    valid, violations = contract.validate_clean_projection(["order_id", "amount", "is_duplicate_row"])
    assert valid is False
    assert any("Audit column 'is_duplicate_row' leaked" in v for v in violations)


def test_output_schema_contract_rejects_internal_scratch_leak():
    contract = OutputSchemaContract(
        dataset_id="transactions",
        mode=CleaningMode.CLEANSE_ONLY,
        source_columns=["txn_id", "amount"],
        clean_columns=["txn_id", "amount", "__row_number"],
        audit_columns=[],
        internal_columns=["__row_number"],
        preserve_source_schema=True,
    )

    valid, violations = contract.validate_clean_projection(["txn_id", "amount", "__row_number"])
    assert valid is False
    assert any("Internal scratch column '__row_number' leaked" in v for v in violations)
