"""
tests/test_composite_field_compilation.py — Step 3 Backend Test Suite

Validates:
1. Work package aggregation across overlapping findings on a single column.
2. Conflict detection (flag_only vs quarantine_row, keep_as_is + active transform).
3. Canonical 9-step execution ordering in emitted PySpark and Pandas code.
4. Mutually exclusive row routing (clean XOR quarantine) and mathematical row invariance:
   len(clean_df) + len(quarantine_df) == total_rows with 0 overlap.
5. Cryptographic SHA-256 code_hash calculation and artifact_status assignment.
6. Type compatibility validation guard (e.g., date vs numeric cast rejection).
"""
import hashlib
import numpy as np
import pandas as pd
import pytest

from agent.etl_pipeline.remediation_models import (
    FieldRemediationWorkPackage,
    GeneratedRemediationArtifact,
    ResolvedRemediationAction,
    resolve_remediation_action,
    detect_field_action_conflicts,
    validate_type_compatibility,
    ConflictingRemediationActions,
    TypeCompatibilityError,
)
from agent.etl_pipeline.composite_field_compiler import (
    create_field_work_package,
    compile_field_work_package,
    CANONICAL_OPERATION_ORDER,
)


# ---------------------------------------------------------------------------
# 1. Work Package Creation & Canonical Ordering
# ---------------------------------------------------------------------------

def test_create_field_work_package_overlapping_email_findings():
    """Overlapping whitespace, placeholder, and syntax findings on Email are grouped and ordered."""
    findings = [
        {
            "id": "rec-email-syntax",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "sanitize_email",
            "selected_option_id": "opt-quarantine",
            "supported_options": [
                {"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine invalid emails"}
            ],
            "approval_status": "approved",
        },
        {
            "id": "rec-email-ws",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "leading_trailing_whitespace",
            "suggested_action": "trim",
            "selected_option_id": "opt-trim",
            "supported_options": [
                {"option_id": "opt-trim", "action_id": "trim", "title": "Trim whitespace"}
            ],
            "approval_status": "approved",
        },
        {
            "id": "rec-email-ph",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "placeholder_values",
            "suggested_action": "normalize_registered_placeholder",
            "selected_option_id": "opt-nullify",
            "supported_options": [
                {"option_id": "opt-nullify", "action_id": "nullify_clean_value", "title": "Nullify placeholder"}
            ],
            "approval_status": "approved",
        },
    ]

    wp = create_field_work_package(
        dataset_id="Customers_Raw",
        column_name="Email",
        findings=findings,
        semantic_type="email",
    )

    assert wp.work_package_id == "wp-customers_raw-email"
    assert wp.dataset_id == "Customers_Raw"
    assert wp.column_name == "Email"
    assert len(wp.resolved_actions) == 3
    assert wp.preserves_raw is True
    assert wp.has_quarantine is True
    assert wp.has_nullification is True
    assert wp.is_provisional is False

    # Canonical order: trim before placeholder before sanitize_email/quarantine
    expected_indices = [
        CANONICAL_OPERATION_ORDER.index("trim"),
        CANONICAL_OPERATION_ORDER.index("normalize_registered_placeholder"),
        CANONICAL_OPERATION_ORDER.index("quarantine_row"),
    ]
    # Check that in wp.ordered_transforms, trim appears before nullify_clean_value / quarantine_row
    assert wp.ordered_transforms.index("trim") < wp.ordered_transforms.index("quarantine_row")


def test_work_package_provisional_when_pending():
    """If any finding in the package is pending approval, the package is marked provisional."""
    findings = [
        {
            "id": "rec-email-ws",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "leading_trailing_whitespace",
            "suggested_action": "trim",
            "selected_option_id": "opt-trim",
            "supported_options": [{"option_id": "opt-trim", "action_id": "trim", "title": "Trim"}],
            "approval_status": "approved",
        },
        {
            "id": "rec-email-syntax",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "pending",
        },
    ]

    wp = create_field_work_package(
        dataset_id="Customers_Raw",
        column_name="Email",
        findings=findings,
    )
    assert wp.is_provisional is True


# ---------------------------------------------------------------------------
# 2. Conflict Detection
# ---------------------------------------------------------------------------

def test_conflict_detection_flag_only_vs_quarantine():
    """Combining 'flag_only' and 'quarantine_row' on the same column must raise ConflictingRemediationActions."""
    findings = [
        {
            "id": "rec-1",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "flag_only",
            "selected_option_id": "opt-flag",
            "supported_options": [{"option_id": "opt-flag", "action_id": "flag_only", "title": "Flag only"}],
            "approval_status": "approved",
        },
        {
            "id": "rec-2",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        },
    ]
    with pytest.raises(ConflictingRemediationActions, match="cannot both apply to the same field"):
        create_field_work_package(
            dataset_id="Customers_Raw",
            column_name="Email",
            findings=findings,
        )


def test_conflict_detection_noop_vs_active_transform():
    """Combining a no-op action ('keep_as_is' or 'skip_etl') with active transform raises error."""
    findings = [
        {
            "id": "rec-1",
            "dataset": "Orders_Raw",
            "column": "Notes",
            "issue_type": "missing_values",
            "suggested_action": "keep_as_is",
            "selected_option_id": "opt-noop",
            "supported_options": [{"option_id": "opt-noop", "action_id": "keep_as_is", "title": "Keep as is"}],
            "approval_status": "approved",
        },
        {
            "id": "rec-2",
            "dataset": "Orders_Raw",
            "column": "Notes",
            "issue_type": "placeholder_values",
            "suggested_action": "nullify_clean_value",
            "selected_option_id": "opt-nullify",
            "supported_options": [{"option_id": "opt-nullify", "action_id": "nullify_clean_value", "title": "Nullify"}],
            "approval_status": "approved",
        },
    ]
    with pytest.raises(ConflictingRemediationActions, match="no-op action.*cannot be combined with active transformations"):
        create_field_work_package(
            dataset_id="Orders_Raw",
            column_name="Notes",
            findings=findings,
        )


# ---------------------------------------------------------------------------
# 3. Canonical Compilation & Emitted Code Verification
# ---------------------------------------------------------------------------

def test_compile_email_work_package_pyspark():
    """Email compilation in PySpark produces raw preservation, compound err mask, clean XOR quarantine."""
    findings = [
        {
            "id": "rec-email",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pyspark")

    assert artifact.dialect == "pyspark"
    assert artifact.action_id == "quarantine_row"
    assert artifact.generator_id == "composite_field_generator"
    assert artifact.artifact_status == "generated"
    assert not artifact.is_provisional

    # Inspect code structure
    code = artifact.code
    assert "F.col('Email')" in code
    assert "Email_raw" in code
    assert "Email_clean" in code
    assert "Email_validation_status" in code
    assert "Email_validation_reason" in code
    assert "clean_df = df.filter(~compound_err)" in code
    assert "quarantine_df = df.filter(compound_err)" in code


def test_compile_email_work_package_pandas():
    """Email compilation in Pandas produces raw preservation, regex validation, clean XOR quarantine."""
    findings = [
        {
            "id": "rec-email",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pandas")

    assert artifact.dialect == "pandas"
    code = artifact.code
    assert "df['Email_raw'] = raw" in code
    assert "df['Email_clean'] = trimmed.where(~compound_err, None)" in code
    assert "df['Email_validation_status']" in code
    assert "clean_df = df[~compound_err].copy()" in code
    assert "quarantine_df = df[compound_err].copy()" in code


# ---------------------------------------------------------------------------
# 4. Mathematical Row Invariance (clean XOR quarantine) Execution Test
# ---------------------------------------------------------------------------

def test_pandas_email_execution_row_invariance():
    """Executing generated Pandas email code guarantees clean XOR quarantine partitioning."""
    findings = [
        {
            "id": "rec-email",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pandas")

    # Construct test dataframe with:
    # Row 0: Valid email with surrounding whitespace
    # Row 1: Valid clean email
    # Row 2: Placeholder token 'N/A'
    # Row 3: Invalid email format 'no-domain'
    # Row 4: True null / None
    test_df = pd.DataFrame({
        "Customer_ID": [101, 102, 103, 104, 105],
        "Email": ["  valid.user@example.com  ", "good@company.org", "N/A", "bad-format-at", None],
    })
    total_rows = len(test_df)

    local_env = {"df": test_df.copy(), "pd": pd, "np": np}
    exec(artifact.code, local_env)

    clean_df = local_env["clean_df"]
    quarantine_df = local_env["quarantine_df"]
    processed_df = local_env["df"]

    # 1. Row count invariance: clean + quarantine == total
    assert len(clean_df) + len(quarantine_df) == total_rows

    # 2. Strict mutual exclusivity: 0 overlap between index sets
    clean_indices = set(clean_df.index)
    quarantine_indices = set(quarantine_df.index)
    assert clean_indices.isdisjoint(quarantine_indices)
    assert clean_indices.union(quarantine_indices) == set(test_df.index)

    # 3. Clean partition verification
    # Rows 0, 1, 4 are clean (row 4 is source null, which is retained unless null-quarantined)
    # Rows 2 and 3 are in quarantine (placeholder and invalid syntax)
    assert 0 in clean_indices
    assert 1 in clean_indices
    assert 2 in quarantine_indices
    assert 3 in quarantine_indices

    # 4. Raw preservation and trimming verification
    assert processed_df.loc[0, "Email_raw"] == "  valid.user@example.com  "
    assert processed_df.loc[0, "Email_clean"] == "valid.user@example.com"
    assert processed_df.loc[0, "Email_validation_status"] == "valid"

    # 5. Quarantine rows have nullified clean values and descriptive status
    assert pd.isna(processed_df.loc[2, "Email_clean"])
    assert processed_df.loc[2, "Email_validation_status"] == "placeholder_normalized_to_null"
    assert pd.isna(processed_df.loc[3, "Email_clean"])
    assert processed_df.loc[3, "Email_validation_status"] == "invalid_email_format"


def test_pandas_numeric_execution_row_invariance():
    """Executing generated Pandas numeric code guarantees clean XOR quarantine partitioning for negative/unparseable."""
    findings = [
        {
            "id": "rec-price",
            "dataset": "Orders_Raw",
            "column": "Price",
            "issue_type": "negative_values",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Orders_Raw", "Price", findings, semantic_type="numeric")
    artifact = compile_field_work_package(wp, dialect="pandas")

    test_df = pd.DataFrame({
        "Order_ID": [1, 2, 3, 4],
        "Price": [19.99, -4.50, "CORRUPT_NOT_NUMERIC", None],
    })
    total_rows = len(test_df)

    local_env = {"df": test_df.copy(), "pd": pd, "np": np}
    exec(artifact.code, local_env)

    clean_df = local_env["clean_df"]
    quarantine_df = local_env["quarantine_df"]
    processed_df = local_env["df"]

    # Invariance check
    assert len(clean_df) + len(quarantine_df) == total_rows
    assert set(clean_df.index).isdisjoint(set(quarantine_df.index))

    # Row 0 is valid, Row 3 is source null -> clean_df
    # Row 1 is negative, Row 2 is unparseable -> quarantine_df
    assert 0 in clean_df.index
    assert 3 in clean_df.index
    assert 1 in quarantine_df.index
    assert 2 in quarantine_df.index

    # Validation statuses
    assert processed_df.loc[1, "Price_validation_status"] == "negative_value_review_required"
    assert processed_df.loc[2, "Price_validation_status"] == "unparseable_numeric"


# ---------------------------------------------------------------------------
# 5. Cryptographic Code Hash & Artifact Status
# ---------------------------------------------------------------------------

def test_code_hash_and_artifact_status():
    """Generated artifacts contain a valid 64-character SHA-256 code_hash."""
    findings = [
        {
            "id": "rec-status",
            "dataset": "Orders_Raw",
            "column": "Price",
            "issue_type": "negative_values",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Orders_Raw", "Price", findings)
    artifact = compile_field_work_package(wp, dialect="pyspark")

    assert len(artifact.code_hash) == 64
    expected_hash = hashlib.sha256(artifact.code.encode("utf-8")).hexdigest()
    assert artifact.code_hash == expected_hash
    assert artifact.artifact_status == "generated"
    assert artifact.generator_version == "2.4.1"


def test_provisional_artifact_status_and_banner():
    """Provisional work package produces provisional artifact status and banner."""
    findings = [
        {
            "id": "rec-status-pending",
            "dataset": "Orders_Raw",
            "column": "Price",
            "issue_type": "negative_values",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row", "title": "Quarantine"}],
            "approval_status": "pending",
        }
    ]
    wp = create_field_work_package("Orders_Raw", "Price", findings)
    artifact = compile_field_work_package(wp, dialect="pandas")

    assert artifact.artifact_status == "provisional"
    assert artifact.is_provisional is True
    assert "Provisional code preview" in artifact.provisional_label


# ---------------------------------------------------------------------------
# 6. Type Compatibility Guard
# ---------------------------------------------------------------------------

def test_type_compatibility_guard_rejects_date_numeric_cast():
    """Rejecting numeric cast on date columns prevents corrupted epoch/nan conversions."""
    with pytest.raises(TypeCompatibilityError, match="cannot be cast to numeric"):
        validate_type_compatibility(
            semantic_type="date",
            action_id="coerce_numeric",
            output_dtype="long",
        )


def test_type_compatibility_guard_allows_compatible_types():
    """Compatible types (e.g. date to date, numeric to double) pass validation without error."""
    # Should not raise:
    validate_type_compatibility(
        semantic_type="date",
        action_id="parse_dates",
        output_dtype="date",
    )
    validate_type_compatibility(
        semantic_type="numeric",
        action_id="coerce_numeric",
        output_dtype="double",
    )
    validate_type_compatibility(
        semantic_type="email",
        action_id="sanitize_email",
        output_dtype="string",
    )


# ---------------------------------------------------------------------------
# 7. Precedence & Multi-Condition Collision Tests
# ---------------------------------------------------------------------------

def test_composite_email_precedence_multi_condition_cases():
    """
    Validates explicit documented precedence for multi-condition email records:
    source_null -> registered_placeholder -> invalid_email_format -> valid.
    """
    findings = [
        {
            "id": "rec-email",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pandas")

    test_df = pd.DataFrame({
        "RowID": [1, 2, 3, 4, 5],
        "Email": [
            "   ",                         # Case 1: whitespace only -> source_null
            "N/A",                          # Case 2: placeholder that also fails email format -> placeholder_token
            "  clean.user@example.com  ",   # Case 3: valid with leading/trailing whitespace -> valid
            "invalid-no-at-sign",          # Case 4: syntax violation -> invalid_email_format
            None,                           # Case 5: source null -> source_null
        ]
    })

    local_env = {"df": test_df.copy(), "pd": pd, "np": np}
    exec(artifact.code, local_env)
    processed = local_env["df"]

    # Case 1: Whitespace only becomes source_null
    assert processed.loc[0, "Email_validation_status"] == "source_null"
    # Case 2: 'N/A' matches placeholder token first, not generic syntax error
    assert processed.loc[1, "Email_validation_status"] == "placeholder_normalized_to_null"
    # Case 3: Trimmed clean email is valid
    assert processed.loc[2, "Email_validation_status"] == "valid"
    assert processed.loc[2, "Email_clean"] == "clean.user@example.com"
    # Case 4: Invalid format matches invalid_email_format
    assert processed.loc[3, "Email_validation_status"] == "invalid_email_format"
    # Case 5: True None becomes source_null
    assert processed.loc[4, "Email_validation_status"] == "source_null"


def test_composite_numeric_precedence_multi_condition_cases():
    """
    Validates numeric pipeline precedence:
    source_null -> unparseable_numeric -> negative_value_review_required -> valid.
    """
    findings = [
        {
            "id": "rec-price",
            "dataset": "Orders_Raw",
            "column": "Price",
            "issue_type": "negative_values",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Orders_Raw", "Price", findings, semantic_type="numeric")
    artifact = compile_field_work_package(wp, dialect="pandas")

    test_df = pd.DataFrame({
        "RowID": [1, 2, 3, 4],
        "Price": [
            "NOT_A_NUMBER",    # Case 1: unparseable -> unparseable_numeric
            -99.5,             # Case 2: negative -> negative_value_review_required
            49.99,             # Case 3: positive numeric -> valid
            None,              # Case 4: true null -> source_null
        ]
    })

    local_env = {"df": test_df.copy(), "pd": pd, "np": np}
    exec(artifact.code, local_env)
    processed = local_env["df"]

    assert processed.loc[0, "Price_validation_status"] == "unparseable_numeric"
    assert processed.loc[1, "Price_validation_status"] == "negative_value_review_required"
    assert processed.loc[2, "Price_validation_status"] == "valid"
    assert processed.loc[3, "Price_validation_status"] == "source_null"


# ---------------------------------------------------------------------------
# 8. Idempotency Verification
# ---------------------------------------------------------------------------

def test_composite_pipeline_idempotency():
    """
    Executing composite transformation twice preserves original raw column and partition counts.
    """
    findings = [
        {
            "id": "rec-email",
            "dataset": "Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_email_format",
            "suggested_action": "quarantine_row",
            "selected_option_id": "opt-quarantine",
            "supported_options": [{"option_id": "opt-quarantine", "action_id": "quarantine_row"}],
            "approval_status": "approved",
        }
    ]
    wp = create_field_work_package("Customers_Raw", "Email", findings, semantic_type="email")
    artifact = compile_field_work_package(wp, dialect="pandas")

    initial_df = pd.DataFrame({
        "ID": [1, 2],
        "Email": ["  user@example.com  ", "invalid-email"],
    })

    # First execution
    env1 = {"df": initial_df.copy(), "pd": pd, "np": np}
    exec(artifact.code, env1)
    df_first = env1["df"]
    clean_first = env1["clean_df"]
    quar_first = env1["quarantine_df"]

    # Second execution on the processed dataframe
    env2 = {"df": df_first.copy(), "pd": pd, "np": np}
    exec(artifact.code, env2)
    df_second = env2["df"]
    clean_second = env2["clean_df"]
    quar_second = env2["quarantine_df"]

    # Check that raw column was preserved intact (not replaced by already-trimmed or double-prefixed)
    assert "Email_raw_raw" not in df_second.columns
    assert df_second.loc[0, "Email_raw"] == "  user@example.com  "
    assert len(clean_first) == len(clean_second)
    assert len(quar_first) == len(quar_second)
    assert (clean_first.index == clean_second.index).all()
    assert (quar_first.index == quar_second.index).all()

