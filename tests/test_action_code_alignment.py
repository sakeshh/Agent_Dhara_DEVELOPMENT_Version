import pytest
from typing import Dict, Any

from agent.etl_pipeline.remediation_models import (
    ResolvedRemediationAction,
    GeneratedRemediationArtifact,
    UnsupportedRemediationAction,
    RemediationApprovalRequired,
    NoOpActionHasGenerator,
    ActionSemanticMismatch,
    ActionCodeMismatchError,
    validate_resolved_action,
    resolve_remediation_action,
    generate_aligned_remediation,
)
from agent.etl_pipeline.remediation_capability_registry import (
    get_capability_entry,
    REMEDIATION_CAPABILITY_REGISTRY,
    UNKNOWN_CAPABILITY_FALLBACK,
)
from agent.dq_recommendations_agent import (
    get_pandas_code,
    get_pyspark_code,
    ACTION_OUTCOME_METADATA,
)


class TestActionCodeAlignment:
    """PR 4.5 Step 2: Selected Option to Generated Code Alignment & Semantics."""

    def test_flag_only_semantics(self):
        """flag_only retains affected rows, preserves clean value, and does not quarantine."""
        spark_code = get_pyspark_code("Price", "cross_field_non_negative", action_id="flag_only")
        assert spark_code is not None
        assert "clean_df = df" in spark_code
        assert "quarantine_df = df.limit(0)" in spark_code
        assert "Price_clean', num_val" in spark_code  # Clean value preserved
        assert "quarantine_reason" not in spark_code  # No quarantine reason

        pandas_code = get_pandas_code("Price", "cross_field_non_negative", action_id="flag_only")
        assert pandas_code is not None
        assert "clean_df = df.copy()" in pandas_code
        assert "quarantine_df = df.iloc[0:0].copy()" in pandas_code
        assert "df['Price_clean'] = num_val" in pandas_code
        assert "df['quarantine_reason']" not in pandas_code

    def test_nullify_clean_value_semantics(self):
        """nullify_clean_value retains affected rows in clean_df and nullifies only the clean companion."""
        spark_code = get_pyspark_code("Price", "cross_field_non_negative", action_id="nullify_clean_value")
        assert spark_code is not None
        assert "clean_df = df" in spark_code
        assert "quarantine_df = df.limit(0)" in spark_code
        assert "F.when(is_negative, None).otherwise(num_val)" in spark_code
        assert "quarantine_reason" not in spark_code

        pandas_code = get_pandas_code("Price", "cross_field_non_negative", action_id="nullify_clean_value")
        assert pandas_code is not None
        assert "clean_df = df.copy()" in pandas_code
        assert "quarantine_df = df.iloc[0:0].copy()" in pandas_code
        assert "num_val.where(~is_negative, None)" in pandas_code
        assert "df['quarantine_reason']" not in pandas_code

    def test_quarantine_row_semantics_and_predicate_reuse(self):
        """quarantine_row filters rows using calculated boolean predicates, never raw column conditions."""
        spark_code = get_pyspark_code("Price", "cross_field_non_negative", action_id="quarantine_row")
        assert spark_code is not None
        # Must NOT use raw column filter
        assert 'F.col("Price") >= 0' not in spark_code
        assert "Price >= 0" not in spark_code
        # Must reuse validated boolean predicates
        assert "quarantine_df = df.filter(is_negative | is_unparseable)" in spark_code
        assert "clean_df = df.filter(~is_negative & ~is_unparseable)" in spark_code
        assert "quarantine_reason" in spark_code

        pandas_code = get_pandas_code("Price", "cross_field_non_negative", action_id="quarantine_row")
        assert pandas_code is not None
        assert "clean_df = df[~is_negative & ~is_unparseable].copy()" in pandas_code
        assert "quarantine_df = df[is_negative | is_unparseable].copy()" in pandas_code
        assert "df['quarantine_reason']" in pandas_code

    def test_keep_as_is_and_skip_etl_generate_no_code(self):
        """keep_as_is and skip_etl must generate no cleansing or transformation code."""
        assert get_pyspark_code("Price", "cross_field_non_negative", action_id="keep_as_is") is None
        assert get_pyspark_code("Price", "cross_field_non_negative", action_id="skip_etl") is None
        assert get_pyspark_code("CreatedDate", "sentinel_date", action_id="keep_as_is") is None
        assert get_pyspark_code("Age", "implausible_age", action_id="keep_as_is") is None

        assert get_pandas_code("Price", "cross_field_non_negative", action_id="keep_as_is") is None
        assert get_pandas_code("Price", "cross_field_non_negative", action_id="skip_etl") is None
        assert get_pandas_code("CreatedDate", "sentinel_date", action_id="keep_as_is") is None

    def test_sentinel_date_date_safe_handling(self):
        """sentinel_date must use date placeholders and cast to date, not numeric long."""
        spark_code = get_pyspark_code("CreatedDate", "sentinel_date", action_id="nullify_clean_value")
        assert spark_code is not None
        assert "9999-12-31" in spark_code
        assert "1900-01-01" in spark_code
        assert "F.to_date" in spark_code
        assert ".cast('long')" not in spark_code

        pandas_code = get_pandas_code("CreatedDate", "sentinel_date", action_id="nullify_clean_value")
        assert pandas_code is not None
        assert "pd.to_datetime" in pandas_code
        assert "9999-12-31" in pandas_code

    def test_integer_stored_as_float_safe_validation(self):
        """integer_stored_as_float validates num == floor(num) before casting."""
        spark_code = get_pyspark_code("Age", "integer_stored_as_float", action_id="cast_type")
        assert spark_code is not None
        assert "num == F.floor(num)" in spark_code

    def test_validator_fails_unsupported_action(self):
        """Unregistered action for a capability raises UnsupportedRemediationAction."""
        cap = get_capability_entry("whitespace")
        action = ResolvedRemediationAction(
            issue_id="test_issue",
            remediation_unit_id="ds.col",
            option_id="illegal_opt",
            action_id="drop_rows",
            dataset_id="ds",
            column_name="col",
            authority_source="DETERMINISTIC_REGISTRY",
            approval_status="approved",
            preserves_raw=True,
            retains_row=False,
            nullifies_clean_value=False,
            quarantines_row=False,
            drops_row=True,
            generator_id="drop_rows_generator",
        )
        with pytest.raises(UnsupportedRemediationAction):
            validate_resolved_action(action, cap)

    def test_validator_fails_unapproved_action_when_required(self):
        """Unapproved contextual action requiring approval raises RemediationApprovalRequired."""
        cap = get_capability_entry("cross_field_non_negative")
        assert cap.approval_required
        action = ResolvedRemediationAction(
            issue_id="test_issue",
            remediation_unit_id="ds.col",
            option_id="flag_opt",
            action_id="flag_only",
            dataset_id="ds",
            column_name="col",
            authority_source="LLM_ADVISORY",
            approval_status="pending",
            preserves_raw=True,
            retains_row=True,
            nullifies_clean_value=False,
            quarantines_row=False,
            drops_row=False,
            generator_id="flag_only_generator",
        )
        with pytest.raises(RemediationApprovalRequired):
            validate_resolved_action(action, cap)

    def test_validator_fails_noop_with_generator(self):
        """keep_as_is configured with an active generator raises NoOpActionHasGenerator."""
        cap = get_capability_entry("whitespace")
        action = ResolvedRemediationAction(
            issue_id="test_issue",
            remediation_unit_id="ds.col",
            option_id="keep_opt",
            action_id="keep_as_is",
            dataset_id="ds",
            column_name="col",
            authority_source="DETERMINISTIC_REGISTRY",
            approval_status="approved",
            preserves_raw=True,
            retains_row=True,
            nullifies_clean_value=False,
            quarantines_row=False,
            drops_row=False,
            generator_id="active_generator_error",
        )
        with pytest.raises(NoOpActionHasGenerator):
            validate_resolved_action(action, cap)

    def test_validator_fails_semantic_mismatch(self):
        """Semantic mismatch raises ActionSemanticMismatch."""
        cap = get_capability_entry("cross_field_non_negative")
        # flag_only claiming to quarantine rows
        action_flag_err = ResolvedRemediationAction(
            issue_id="test_issue",
            remediation_unit_id="ds.col",
            option_id="opt",
            action_id="flag_only",
            dataset_id="ds",
            column_name="col",
            authority_source="DETERMINISTIC_REGISTRY",
            approval_status="approved",
            preserves_raw=True,
            retains_row=True,
            nullifies_clean_value=False,
            quarantines_row=True,  # VIOLATION
            drops_row=False,
            generator_id="flag_only_generator",
        )
        with pytest.raises(ActionSemanticMismatch):
            validate_resolved_action(action_flag_err, cap)

        # nullify_clean_value claiming not to retain rows
        action_null_err = ResolvedRemediationAction(
            issue_id="test_issue",
            remediation_unit_id="ds.col",
            option_id="opt",
            action_id="nullify_clean_value",
            dataset_id="ds",
            column_name="col",
            authority_source="DETERMINISTIC_REGISTRY",
            approval_status="approved",
            preserves_raw=True,
            retains_row=False,  # VIOLATION
            nullifies_clean_value=True,
            quarantines_row=False,
            drops_row=False,
            generator_id="nullify_clean_value_generator",
        )
        with pytest.raises(ActionSemanticMismatch):
            validate_resolved_action(action_null_err, cap)

    def test_generator_chain_invariant_enforcement(self):
        """Invariant: selected_option.action_id == compiled_step.action_id == generated_artifact.action_id."""
        cap = get_capability_entry("cross_field_non_negative")
        resolved = resolve_remediation_action(
            finding={"id": "price_neg", "dataset": "Courses", "column": "Price", "issue_type": "cross_field_non_negative"},
            selected_option={"id": "opt_flag", "action": "flag_only"},
            approval_status="approved",
        )
        assert resolved.action_id == "flag_only"
        assert resolved.retains_row is True
        assert resolved.quarantines_row is False

        artifact = generate_aligned_remediation(resolved, cap, dialect="pyspark")
        assert artifact.action_id == resolved.action_id == "flag_only"
        assert artifact.is_provisional is False
        assert "clean_df = df" in artifact.code
        assert artifact.outcome_metadata["status_code"] == "flagged_only"

    def test_provisional_preview_on_pending_approval(self):
        """Pending approval marks code preview provisional with exact warning label."""
        cap = get_capability_entry("whitespace")
        resolved = resolve_remediation_action(
            finding={"id": "ws_1", "dataset": "Customers", "column": "Name", "issue_type": "whitespace"},
            selected_option={"id": "opt_trim", "action": "trim"},
            approval_status="pending",
        )
        artifact = generate_aligned_remediation(resolved, cap, dialect="pyspark")
        assert artifact.is_provisional is True
        assert artifact.provisional_label == "Provisional code preview for the selected option. This transformation is not approved or authorized for execution."


class TestReportDefectRegressions:
    """Explicit fixtures recreating the defects observed in the assessment report."""

    def test_regression_courses_price_flag_only_retains_rows(self):
        """Courses.Price + flag_only must retain negative-price rows and only flag them."""
        spark_code = get_pyspark_code("Price", "cross_field_non_negative", action_id="flag_only")
        assert "clean_df = df" in spark_code
        assert "quarantine_df = df.limit(0)" in spark_code
        assert "negative_value_review_required" in spark_code

        pandas_code = get_pandas_code("Price", "cross_field_non_negative", action_id="flag_only")
        assert "clean_df = df.copy()" in pandas_code
        assert "quarantine_df = df.iloc[0:0].copy()" in pandas_code

    def test_regression_customers_raw_created_date_keep_as_is_no_code(self):
        """Customers_Raw.CreatedDate + keep_as_is must generate no cleansing code."""
        spark_code = get_pyspark_code("CreatedDate", "sentinel_date", action_id="keep_as_is")
        assert spark_code is None

        pandas_code = get_pandas_code("CreatedDate", "sentinel_date", action_id="keep_as_is")
        assert pandas_code is None

    def test_regression_customers_age_keep_as_is_does_not_cast(self):
        """Customers.Age + keep_as_is must not cast the column."""
        spark_code = get_pyspark_code("Age", "integer_stored_as_float", action_id="keep_as_is")
        assert spark_code is None

        pandas_code = get_pandas_code("Age", "integer_stored_as_float", action_id="keep_as_is")
        assert pandas_code is None

    def test_outcome_metadata_alignment(self):
        """Rendered outcome metadata aligns with action semantics."""
        assert ACTION_OUTCOME_METADATA["flag_only"]["drops_rows"] is False
        assert ACTION_OUTCOME_METADATA["flag_only"]["status_code"] == "flagged_only"
        assert ACTION_OUTCOME_METADATA["nullify_clean_value"]["status_code"] == "clean_nullified"
        assert ACTION_OUTCOME_METADATA["quarantine_row"]["status_code"] == "quarantined_row"
        assert ACTION_OUTCOME_METADATA["keep_as_is"]["status_code"] == "retained_as_is"
