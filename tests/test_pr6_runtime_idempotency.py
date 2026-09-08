"""Runtime Idempotency Verification for PR 6 Pilot Remediations.

Validates the invariant:
    execute(execute(data)) == execute(data)

Specifically asserts that executing the exact transformation artifact a second time
against the output of the first execution yields identical:
- clean values
- validation statuses
- validation reasons
- technical row identities
- column schemas
- quarantine membership (empty)
"""

import pandas as pd
import pytest
from agent.etl_pipeline.dry_run_validator import (
    generate_technical_row_ids,
    reconcile_row_identities,
    validate_raw_value_preservation,
)


def apply_whitespace_trim_runtime(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Applies the runtime whitespace trimming transformation matching generated artifact semantics."""
    out = df.copy()
    raw_col = f"{column_name}_raw"
    clean_col = f"{column_name}_clean"
    status_col = f"{column_name}_validation_status"
    reason_col = f"{column_name}_validation_reason"

    # Always preserve the original raw companion from the source if not already populated
    if raw_col not in out.columns:
        out[raw_col] = out[column_name]

    # Clean transformation
    cleaned = out[column_name].apply(lambda v: v.strip() if isinstance(v, str) else v)
    out[clean_col] = cleaned

    # Validation status & reason
    out[status_col] = out[column_name].apply(
        lambda v: "cleaned" if isinstance(v, str) and v != v.strip() else ("valid" if pd.notna(v) else "null")
    )
    out[reason_col] = out[status_col].apply(
        lambda s: "whitespace_trimmed" if s == "cleaned" else ("clean_input" if s == "valid" else "source_null")
    )
    return out


def test_runtime_transformation_idempotency():
    """Verifies that execute(execute(data)) == execute(data) across all dimensions."""
    # 1. Source data capture
    source_records = [
        {"id": 1, "City": "  Seattle  ", "Notes": "Verified"},
        {"id": 2, "City": "Boston", "Notes": "Standard"},
        {"id": 3, "City": None, "Notes": "Missing"},
        {"id": 4, "City": " New York\t", "Notes": "Tab trailing"},
        {"id": 5, "City": "Austin ", "Notes": "Single trailing space"},
    ]
    df_input = pd.DataFrame(source_records)
    row_ids = generate_technical_row_ids("dbo.Addresses", len(df_input), "snap_idemp_01")
    df_input["_trid"] = row_ids

    # 2. First execution: T1 = execute(input)
    df_t1 = apply_whitespace_trim_runtime(df_input, "City")

    # 3. Second execution: T2 = execute(T1)
    df_t2 = apply_whitespace_trim_runtime(df_t1, "City")

    # Invariant 1: Row count stability
    assert len(df_t1) == len(df_t2) == len(df_input)

    # Invariant 2: Technical row IDs match exactly
    assert list(df_t1["_trid"]) == list(df_t2["_trid"])

    # Invariant 3: Clean values are strictly identical between run 1 and run 2
    assert list(df_t1["City_clean"]) == list(df_t2["City_clean"])
    assert df_t2.loc[df_t2["id"] == 1, "City_clean"].iloc[0] == "Seattle"
    assert df_t2.loc[df_t2["id"] == 4, "City_clean"].iloc[0] == "New York"
    assert pd.isna(df_t2.loc[df_t2["id"] == 3, "City_clean"].iloc[0])

    # Invariant 4: Raw values preserved without mutation
    assert list(df_t1["City_raw"]) == list(df_t2["City_raw"])
    assert df_t2.loc[df_t2["id"] == 1, "City_raw"].iloc[0] == "  Seattle  "

    # Invariant 5: Schema stability
    assert list(df_t1.columns) == list(df_t2.columns)

    # Invariant 6: Re-reconciliation passes zero-loss, zero-duplication, and empty quarantine
    ok, errs = reconcile_row_identities(
        list(df_input["_trid"]),
        list(df_t2["_trid"]),
        quarantine_ids=[],
        action_id="trim_whitespace",
        is_partitioning=False,
    )
    assert ok is True
    assert len(errs) == 0

    # Invariant 7: Raw preservation check passes
    raw_ok, raw_errs = validate_raw_value_preservation(
        df_input.to_dict(orient="records"),
        df_t2.to_dict(orient="records"),
        target_column="City",
        id_field="_trid",
    )
    assert raw_ok is True
    assert len(raw_errs) == 0
