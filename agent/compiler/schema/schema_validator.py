from __future__ import annotations

from typing import List, Tuple
from agent.compiler.schema.output_contract import OutputSchemaContract
from agent.compiler.models.cleaning import CleaningMode


class SchemaValidator:
    """
    Validates output schema conformance before code generation and execution.
    Fails compilation if unauthorized schema expansion or leaked internal columns occur.
    """

    @classmethod
    def validate_contract(cls, contract: OutputSchemaContract) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        if not contract.clean_columns:
            errors.append(f"Dataset '{contract.dataset_id}' has an empty clean projection list.")

        # In CLEANSE_ONLY mode, audit and internal columns must strictly NEVER be in clean_columns
        if contract.mode == CleaningMode.CLEANSE_ONLY:
            prohibited_suffixes = ("_raw", "_clean", "_validation_status", "_validation_reason")
            prohibited_names = {
                "is_duplicate_row",
                "row_validation_status",
                "row_validation_reason",
                "etl_batch_id",
                "etl_created_at",
                "etl_updated_at",
            }
            for col in contract.clean_columns:
                if col in contract.audit_columns:
                    errors.append(
                        f"Schema Violation: Audit column '{col}' is present in clean_columns for '{contract.dataset_id}' in CLEANSE_ONLY mode."
                    )
                if col in contract.internal_columns:
                    errors.append(
                        f"Schema Violation: Internal temporary column '{col}' leaked into clean_columns for '{contract.dataset_id}'."
                    )
                if contract.preserve_source_schema and col not in contract.source_columns:
                    errors.append(
                        f"Schema Expansion Violation: Generated column '{col}' leaked into clean_columns while preserve_source_schema=True."
                    )
                if col not in contract.source_columns:
                    if any(col.lower().endswith(s) for s in prohibited_suffixes):
                        errors.append(
                            f"Schema Violation: Cleanse-only output column '{col}' leaked prohibited audit companion suffix."
                        )
                    if col.lower() in prohibited_names:
                        errors.append(
                            f"Schema Violation: Cleanse-only output column '{col}' leaked prohibited audit/metadata column."
                        )

        # Disallow collision between clean and internal
        collisions = set(contract.clean_columns).intersection(set(contract.internal_columns))
        if collisions:
            for c in collisions:
                errors.append(f"Fatal Column Collision: '{c}' cannot be both clean and internal in '{contract.dataset_id}'.")

        return (len(errors) == 0, errors)
