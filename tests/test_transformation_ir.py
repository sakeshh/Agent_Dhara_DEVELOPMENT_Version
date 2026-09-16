"""
Unit tests for Phase 2: Refined Transformation Intermediate Representation (IR).
Tests expression primitives, separation of VALIDATE from QUARANTINE, dedicated
HASH, MASK, IMPUTE, PASS_THROUGH, SCHEMA_MUTATION families, composite step
expansion, and concrete data effects.
"""
import pytest
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    ClipExpression,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    FlagExpression,
    HashExpression,
    ImputeExpression,
    LiteralValue,
    MaskExpression,
    NullCheck,
    PassThroughExpression,
    PreImageRequirement,
    QuarantineExpression,
    ScalarMapExpression,
    SchemaMutationExpression,
    StepKind,
    TransformationEffects,
    TransformationStepIR,
    UnaryOp,
    UnaryOperator,
    ValidateExpression,
    ValueVersion,
    plan_step_to_ir,
    plan_step_to_ir_steps,
)


# ============================================================================
# 1. Primitives & Value-Version Tests
# ============================================================================

def test_value_version_semantics():
    """ValueVersion distinguishes original pre-image, current, and after-step values."""
    col_orig = ColumnRef(column_name="discount", version=ValueVersion.ORIGINAL)
    assert col_orig.version == ValueVersion.ORIGINAL

    col_curr = ColumnRef(column_name="discount", version=ValueVersion.CURRENT)
    assert col_curr.version == ValueVersion.CURRENT

    # AFTER_STEP requires referenced_step_id
    with pytest.raises(ValueError, match="must declare referenced_step_id"):
        ColumnRef(column_name="discount", version=ValueVersion.AFTER_STEP)

    col_after = ColumnRef(
        column_name="discount",
        version=ValueVersion.AFTER_STEP,
        referenced_step_id="step_clip_01",
    )
    assert col_after.referenced_step_id == "step_clip_01"


# ============================================================================
# 2. Separation of VALIDATE and QUARANTINE
# ============================================================================

def test_validate_is_pure_assertion_without_row_routing():
    """ValidateExpression evaluates predicate without altering row population or routing."""
    col = ColumnRef(column_name="age", dataset_name="users")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=0, data_type=DataType.INTEGER))
    val_expr = ValidateExpression(predicate=cond, severity="critical")
    effects = val_expr.derive_effects("users")

    assert effects.reads == ("age",)
    assert effects.writes == ()
    assert effects.creates == ()
    assert effects.filters_rows is False
    assert effects.partitions_rows is False


def test_quarantine_partitions_rows():
    """QuarantineExpression partitions failing rows and creates rejection output."""
    col = ColumnRef(column_name="customer_id", dataset_name="customers")
    cond = NullCheck(operand=col, is_null=True)
    quar_expr = QuarantineExpression(
        predicate=cond,
        accepted_output_ref="customers_clean",
        rejected_output_ref="customers_rejects",
        reason_column="rejection_reason",
    )
    effects = quar_expr.derive_effects("customers")

    assert effects.reads == ("customer_id",)
    assert effects.creates == ("rejection_reason",)
    assert effects.filters_rows is True
    assert effects.partitions_rows is True


def test_composite_relationship_decomposes_into_validate_and_quarantine():
    """validate_and_partition_relationship decomposes into VALIDATE followed by QUARANTINE."""
    step = {
        "step_id": "rel_01",
        "dataset": "orders",
        "column": "customer_id",
        "action": "validate_and_partition_relationship",
    }
    steps = plan_step_to_ir_steps(step)
    assert len(steps) == 2

    # Step 1: VALIDATE
    assert steps[0].family == ActionFamily.VALIDATE
    assert steps[0].kind == StepKind.ASSERTION
    assert steps[0].effects.filters_rows is False

    # Step 2: QUARANTINE with dependency
    assert steps[1].family == ActionFamily.QUARANTINE
    assert steps[1].kind == StepKind.QUARANTINE_ROUTING
    assert steps[1].effects.partitions_rows is True
    assert steps[0].step_id in steps[1].prerequisites


# ============================================================================
# 3. Dedicated Security, Privacy & Pass-Through Families
# ============================================================================

def test_hash_family_and_effects():
    """HashExpression models cryptographic one-way hashing with pre-image execution requirement."""
    col = ColumnRef(column_name="ssn", dataset_name="users")
    h_expr = HashExpression(
        input_column=col,
        output_column="ssn_hash",
        algorithm="sha256",
    )
    effects = h_expr.derive_effects("users")
    assert effects.reads == ("ssn",)
    assert effects.creates == ("ssn_hash",)

    step = plan_step_to_ir({"step_id": "s_h", "dataset": "users", "column": "phone", "action": "hash_phone"})
    assert step.family == ActionFamily.HASH
    assert step.pre_image.required_for_execution is True


def test_mask_family_and_effects():
    """MaskExpression models redaction with configurable strategies."""
    col = ColumnRef(column_name="card_number", dataset_name="orders")
    m_expr = MaskExpression(
        input_column=col,
        output_column="card_number_masked",
        masking_strategy="redact_preserve_last4",
        visible_suffix_length=4,
    )
    effects = m_expr.derive_effects("orders")
    assert effects.reads == ("card_number",)
    assert effects.creates == ("card_number_masked",)

    step = plan_step_to_ir({"step_id": "s_m", "dataset": "users", "column": "phone", "action": "mask_phone"})
    assert step.family == ActionFamily.MASK
    assert step.pre_image.required_for_rollback is True


def test_impute_family_and_effects():
    """ImputeExpression models statistical and policy imputation with audit pre-image."""
    step = {
        "step_id": "s_imp",
        "dataset": "finance",
        "column": "salary",
        "action": "impute_numeric",
        "params": {"strategy": "grouped_median", "group_by": ["department"]},
    }
    ir_step = plan_step_to_ir(step)
    assert ir_step.family == ActionFamily.IMPUTE
    assert "salary" in ir_step.effects.reads
    assert "department" in ir_step.effects.reads
    assert "salary" in ir_step.effects.writes
    assert ir_step.pre_image.required_for_audit is True


def test_pass_through_control_construct_and_effects():
    """PassThroughExpression represents intentional non-mutation."""
    step = {"step_id": "s_pt", "dataset": "customers", "column": "notes", "action": "preserve_null"}
    ir_step = plan_step_to_ir(step)
    assert ir_step.family == ActionFamily.PASS_THROUGH
    assert ir_step.effects.writes == ()
    assert ir_step.effects.creates == ()
    assert ir_step.effects.filters_rows is False


def test_schema_mutation_drop_vs_project_exclude():
    """SchemaMutationExpression distinguishes DROP_COLUMN from PROJECT_EXCLUDE."""
    step_drop = {"step_id": "s_d", "dataset": "users", "column": "temp_col", "action": "drop_column"}
    ir_drop = plan_step_to_ir(step_drop)
    assert ir_drop.family == ActionFamily.SCHEMA_MUTATION
    assert ir_drop.effects.drops == ("temp_col",)

    step_excl = {"step_id": "s_e", "dataset": "users", "column": "internal_ref", "action": "exclude_column"}
    ir_excl = plan_step_to_ir(step_excl)
    assert ir_excl.family == ActionFamily.SCHEMA_MUTATION
    assert ir_excl.effects.drops == ()  # project_exclude does not drop from working source


# ============================================================================
# 4. Vertical Slice 1: Discount Cap and Flag Scenario
# ============================================================================

def test_vertical_slice_1_discount_cap_and_flag_scenario():
    """
    Vertical Slice 1 Scenario:
    'If discount_percent is greater than 50, flag it as excessive_discount and cap it at 50.'
    - Step A: FLAG, reads ORIGINAL discount_percent, creates excessive_discount
    - Step B: CLIP, reads/writes CURRENT discount_percent, capped at 50
    - Prerequisite: Step B depends on Step A
    - Deterministic execution in Pandas verifies pre-image snapshot and clipping.
    """
    import numpy as np
    import pandas as pd
    from agent.etl_pipeline.transformation_ir import (
        compile_pandas_plan,
        compile_pandas_step,
    )

    # Step A: FLAG reading pre-transform value
    col_pre = ColumnRef(column_name="discount_percent", version=ValueVersion.ORIGINAL)
    predicate = BinaryOp(
        left=col_pre,
        op=BinaryOperator.GT,
        right=LiteralValue(value=50.0, data_type=DataType.FLOAT),
    )
    flag_expr = FlagExpression(
        predicate=predicate,
        output_column="excessive_discount",
        true_value=True,
        false_value=False,
    )
    step_a = TransformationStepIR(
        step_id="step_flag_excessive_discount",
        dataset_name="orders",
        family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=flag_expr,
        target_column="excessive_discount",
        effects=flag_expr.derive_effects("orders"),
        supporting_rule_ids=("RULE_DISCOUNT_THRESHOLD",),
        supporting_evidence_ids=("EV_DISCOUNT_ANOMALY",),
    )

    assert step_a.effects.reads == ("discount_percent",)
    assert step_a.effects.creates == ("excessive_discount",)
    assert step_a.effects.writes == ()
    assert step_a.effects.filters_rows is False

    # Step B: CLIP reading and writing current value
    col_curr = ColumnRef(column_name="discount_percent", version=ValueVersion.CURRENT)
    clip_expr = ClipExpression(
        target_column=col_curr,
        upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
    )
    step_b = TransformationStepIR(
        step_id="step_clip_discount",
        dataset_name="orders",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=clip_expr,
        target_column="discount_percent",
        effects=clip_expr.derive_effects("orders"),
        prerequisites=(step_a.step_id,),
        supporting_rule_ids=("RULE_DISCOUNT_THRESHOLD",),
        supporting_evidence_ids=("EV_DISCOUNT_ANOMALY",),
    )

    assert step_b.effects.reads == ("discount_percent",)
    assert step_b.effects.writes == ("discount_percent",)
    assert step_b.prerequisites == (step_a.step_id,)

    # Semantic hashes are deterministic and distinct
    assert isinstance(step_a.semantic_hash, str) and len(step_a.semantic_hash) == 64
    assert isinstance(step_b.semantic_hash, str) and len(step_b.semantic_hash) == 64
    assert step_a.semantic_hash != step_b.semantic_hash

    # Compile the plan to executable Pandas code
    compiled_code = compile_pandas_plan([step_a, step_b], df_var="df")
    assert "pre_df = df.copy()" in compiled_code
    assert "excessive_discount" in compiled_code
    assert "clip(upper=50.0)" in compiled_code

    # Execute compiled block on test DataFrame fixture
    df = pd.DataFrame({"discount_percent": [10.0, 60.0, 45.0, 80.0]})
    exec_scope = {"df": df, "np": np, "pd": pd}
    exec(compiled_code, exec_scope)
    result_df = exec_scope["df"]

    # Assert exact outputs
    expected_discounts = [10.0, 50.0, 45.0, 50.0]
    expected_flags = [False, True, False, True]
    assert result_df["discount_percent"].tolist() == expected_discounts
    assert result_df["excessive_discount"].tolist() == expected_flags


# ============================================================================
# 5. Engine Capability Maturity & Preflight Rules
# ============================================================================

def test_engine_capability_preflight_authorization():
    """Compilers must authorize only VERIFIED capabilities for production by default."""
    from agent.etl_pipeline.transformation_ir import (
        CapabilityMaturity,
        EngineCapability,
        TargetEngine,
        authorize_engine_capability,
    )

    cap_verified = EngineCapability(
        engine=TargetEngine.PANDAS,
        maturity=CapabilityMaturity.VERIFIED,
        verified_test_ids=("TEST_FLAG_01", "TEST_CLIP_01"),
    )
    assert authorize_engine_capability(cap_verified) is True

    cap_partial = EngineCapability(
        engine=TargetEngine.SQL_TSQL,
        maturity=CapabilityMaturity.PARTIAL,
        limitations=("Safe date parsing requires TRY_CONVERT",),
    )
    # Fails by default without explicit policy decision
    assert authorize_engine_capability(cap_partial, allow_partial=False) is False
    # Authorized only when explicit waiver granted
    assert authorize_engine_capability(cap_partial, allow_partial=True) is True

    cap_unsupported = EngineCapability(
        engine=TargetEngine.ADF,
        maturity=CapabilityMaturity.UNSUPPORTED,
    )
    assert authorize_engine_capability(cap_unsupported, allow_partial=True) is False

    cap_not_tested = EngineCapability(
        engine=TargetEngine.SQL_SPARK,
        maturity=CapabilityMaturity.NOT_TESTED,
    )
    assert authorize_engine_capability(cap_not_tested, allow_partial=True) is False


# ============================================================================
# 6. Pre-Image Formal Contract & Reversibility Claims
# ============================================================================

def test_preimage_formal_contract_and_reversibility():
    """Pre-image requirements and reversibility attributes must be strictly typed."""
    from agent.etl_pipeline.transformation_ir import (
        PreimagePurpose,
        PreimageRequirement,
        PreimageRetention,
    )

    req = PreimageRequirement(
        purposes=(PreimagePurpose.ROLLBACK, PreimagePurpose.AUDIT),
        retention=PreimageRetention.ROLLBACK_PERSISTED,
        required_columns=("ssn", "phone"),
        encryption_required=True,
    )
    assert req.required_for_rollback is True
    assert req.required_for_audit is True
    assert req.required_for_execution is False
    assert req.encryption_required is True

    # HASH is one-way and irreversible without external store
    col = ColumnRef(column_name="tax_id", dataset_name="users")
    h_expr = HashExpression(input_column=col, output_column="tax_id_hash")
    assert h_expr.reversible is False
    assert h_expr.security_class == "one_way_pseudonymization"

    # SCHEMA_MUTATION drop_column is irreversible without pre-image
    s_expr = SchemaMutationExpression(mutation_kind="drop_column", target_column="temp_id")
    assert s_expr.reversible is False


# ============================================================================
# 7. MAP_VALUE Typed Operations & Parameter Validation
# ============================================================================

def test_scalar_map_typed_operations_and_validation():
    """ScalarMapExpression validates required parameters per operation."""
    from agent.etl_pipeline.transformation_ir import (
        ParseFailurePolicy,
        ScalarMapOperation,
    )

    col = ColumnRef(column_name="raw_phone", dataset_name="users")

    # REGEX_SUBSTITUTE requires pattern and replacement
    with pytest.raises(ValueError, match="requires 'pattern' and 'replacement'"):
        ScalarMapExpression(
            source_column=col,
            output_column="clean_phone",
            operation=ScalarMapOperation.REGEX_SUBSTITUTE,
            parameters={"pattern": r"\D+"},
        )

    # Valid REGEX_SUBSTITUTE
    valid_regex = ScalarMapExpression(
        source_column=col,
        output_column="clean_phone",
        operation=ScalarMapOperation.REGEX_SUBSTITUTE,
        parameters={"pattern": r"\D+", "replacement": ""},
    )
    assert valid_regex.operation == ScalarMapOperation.REGEX_SUBSTITUTE

    # LOOKUP_MAP requires mapping
    with pytest.raises(ValueError, match="requires 'mapping'"):
        ScalarMapExpression(
            source_column=col,
            output_column="status_desc",
            operation=ScalarMapOperation.LOOKUP_MAP,
            parameters={},
        )

    # CAST requires target_type
    with pytest.raises(ValueError, match="requires 'target_type'"):
        ScalarMapExpression(
            source_column=col,
            output_column="age",
            operation=ScalarMapOperation.CAST,
            parameters={},
        )


# ============================================================================
# 8. Authoritative Taxonomy & LegacyActionInventoryRecord
# ============================================================================

def test_authoritative_action_taxonomy_and_inventory_record():
    """Taxonomy contains 18 canonical action families + PASS_THROUGH control construct."""
    from agent.etl_pipeline.transformation_ir import (
        ActionRisk,
        CapabilityMaturity,
        EngineCapability,
        LegacyActionInventoryRecord,
        TargetEngine,
    )

    # The 18 canonical transformation families
    expected_families = {
        "validate", "quarantine", "flag", "map_value", "conditional_replace",
        "clip", "impute", "standardize", "parse", "extract", "derive",
        "filter", "join", "aggregate", "deduplicate", "hash", "mask",
        "schema_mutation", "pass_through",
    }
    actual_families = {f.value for f in ActionFamily}
    assert actual_families == expected_families

    # Authoritative inventory record model check
    record = LegacyActionInventoryRecord(
        legacy_action_name="range_clip",
        canonical_family=ActionFamily.CLIP,
        expression_model="ClipExpression",
        step_kind=StepKind.ROW_TRANSFORMATION,
        target_scope="column",
        changes_existing_values=True,
        changes_row_count=False,
        changes_schema=False,
        creates_columns=False,
        deletes_columns=False,
        creates_datasets=False,
        requires_preimage=True,
        risk_class=ActionRisk.AUDITABLE,
        idempotent=True,
        deterministic=True,
        reversible=False,
        required_parameters=("lower_bound", "upper_bound"),
        engine_capabilities=(
            EngineCapability(
                engine=TargetEngine.PANDAS,
                maturity=CapabilityMaturity.VERIFIED,
            ),
        ),
        required_postconditions=("min <= value <= max",),
    )
    assert record.legacy_action_name == "range_clip"
    assert record.canonical_family == ActionFamily.CLIP
    assert record.idempotent is True
    assert record.deterministic is True


# ============================================================================
# 9. Vertical Slice 2: Dynamic Migration Ledger Reconciliation
# ============================================================================

def test_dynamic_legacy_action_migration_registry_reconciliation():
    """
    Vertical Slice 2: Asserts 100% set equality between ACTION_REGISTRY and LEGACY_ACTION_MIGRATION_REGISTRY.
    Validates per-disposition contracts and explicit alias handling.
    """
    from agent.etl_pipeline.action_registry import ACTION_REGISTRY
    from agent.etl_pipeline.transformation_ir import (
        LEGACY_ACTION_MIGRATION_REGISTRY,
        MigrationDisposition,
        TransformationIRInvariantError,
    )

    registry_actions = set(ACTION_REGISTRY.keys())
    migration_actions = set(LEGACY_ACTION_MIGRATION_REGISTRY.keys())

    # Set equality: no missing and no unexpected actions
    missing = registry_actions - migration_actions
    unexpected = migration_actions - registry_actions
    assert not missing, f"Missing actions in migration registry: {missing}"
    assert not unexpected, f"Unexpected actions in migration registry: {unexpected}"
    assert len(LEGACY_ACTION_MIGRATION_REGISTRY) == len(ACTION_REGISTRY)

    # Validate disposition-specific invariants
    for name, record in LEGACY_ACTION_MIGRATION_REGISTRY.items():
        assert record.legacy_action_name == name
        if record.disposition == MigrationDisposition.MAPPED_TO_IR:
            assert record.canonical_family is not None
            assert record.expression_model is not None
        elif record.disposition == MigrationDisposition.COMPOSITE_IR:
            assert record.expression_model is not None
        elif record.disposition == MigrationDisposition.PASS_THROUGH_DECISION:
            assert record.canonical_family is None
        elif record.disposition == MigrationDisposition.DEPRECATED:
            assert record.replacement_action is not None
            assert record.removal_target is not None
        elif record.disposition == MigrationDisposition.MANUAL_REVIEW:
            assert record.manual_review_owner is not None

    # Verify alias resolution
    trim_alias = LEGACY_ACTION_MIGRATION_REGISTRY["trim_preserve_case"]
    assert trim_alias.alias_of == "trim"
    assert trim_alias.disposition == MigrationDisposition.MAPPED_TO_IR


# ============================================================================
# 10. Vertical Slice 2: Effect Derivation & Invariant Tamper Rejection
# ============================================================================

def test_effect_derivation_invariant_tamper_rejection():
    """
    Vertical Slice 2: Steps deterministically derive effects from expressions.
    Tampered / forged caller effects raise TransformationIRInvariantError.
    """
    from agent.etl_pipeline.transformation_ir import (
        TransformationIRInvariantError,
        derive_effects,
    )

    col = ColumnRef(column_name="salary", dataset_name="payroll")
    clip_expr = ClipExpression(
        target_column=col,
        upper_bound=LiteralValue(value=100000.0, data_type=DataType.FLOAT),
    )

    # 1. Step constructed without effects automatically computes derived effects
    step = TransformationStepIR(
        step_id="step_auto_effects",
        dataset_name="payroll",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=clip_expr,
        target_column="salary",
    )
    assert step.effects.reads == ("salary",)
    assert step.effects.writes == ("salary",)
    assert step.effects.creates == ()
    assert step.effects.filters_rows is False

    # 2. Forged effects (e.g. caller claiming step drops rows or creates columns)
    forged_effects = TransformationEffects(
        reads=("salary",),
        writes=(),
        creates=("bonus",),
        filters_rows=True,
    )
    with pytest.raises((TransformationIRInvariantError, ValueError), match="Declared effects do not match expression semantics"):
        TransformationStepIR(
            step_id="step_forged",
            dataset_name="payroll",
            family=ActionFamily.CLIP,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=clip_expr,
            target_column="salary",
            effects=forged_effects,
        )

    # 3. ValidateExpression derived effects are strictly assertion (READ only)
    val_expr = ValidateExpression(
        predicate=BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=0)),
        severity="error",
    )
    val_eff = derive_effects(val_expr, "payroll")
    assert val_eff.reads == ("salary",)
    assert val_eff.writes == ()
    assert val_eff.creates == ()
    assert val_eff.drops == ()
    assert val_eff.filters_rows is False
    assert val_eff.partitions_rows is False


# ============================================================================
# 11. Vertical Slice 2: Two-Tier Hashing (Transformation vs Plan-Node)
# ============================================================================

def test_two_tier_semantic_and_plan_node_hashing():
    """
    Vertical Slice 2:
    - Same expression, different step_id -> same transformation_hash
    - Same expression, different prerequisites -> same transformation_hash, different plan_node_hash
    - Different expression parameter -> different transformation_hash, different plan_node_hash
    """
    col = ColumnRef(column_name="score", dataset_name="students")
    expr_a = ClipExpression(
        target_column=col,
        upper_bound=LiteralValue(value=100.0, data_type=DataType.FLOAT),
    )

    step_1 = TransformationStepIR(
        step_id="step_score_clip_01",
        dataset_name="students",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr_a,
        target_column="score",
        prerequisites=("step_prev_01",),
    )

    step_2_diff_id = TransformationStepIR(
        step_id="step_score_clip_02",  # Different step_id
        dataset_name="students",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr_a,
        target_column="score",
        prerequisites=("step_prev_01",),
    )

    step_3_diff_prereq = TransformationStepIR(
        step_id="step_score_clip_03",
        dataset_name="students",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr_a,
        target_column="score",
        prerequisites=("step_other_pre",),  # Different prerequisite
    )

    expr_b = ClipExpression(
        target_column=col,
        upper_bound=LiteralValue(value=90.0, data_type=DataType.FLOAT),  # Different bound
    )
    step_4_diff_bound = TransformationStepIR(
        step_id="step_score_clip_04",
        dataset_name="students",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr_b,
        target_column="score",
        prerequisites=("step_prev_01",),
    )

    # 1. Same expression, different step_id -> identical transformation_hash
    assert step_1.transformation_hash == step_2_diff_id.transformation_hash
    assert step_1.plan_node_hash == step_2_diff_id.plan_node_hash

    # 2. Same expression, different prerequisite -> identical transformation_hash, distinct plan_node_hash
    assert step_1.transformation_hash == step_3_diff_prereq.transformation_hash
    assert step_1.plan_node_hash != step_3_diff_prereq.plan_node_hash

    # 3. Different expression parameter -> distinct transformation_hash, distinct plan_node_hash
    assert step_1.transformation_hash != step_4_diff_bound.transformation_hash
    assert step_1.plan_node_hash != step_4_diff_bound.plan_node_hash


# ============================================================================
# 12. Vertical Slice 2: Layer 1 Intrinsic Expression Validators
# ============================================================================

def test_layer_1_intrinsic_expression_validators():
    """
    Vertical Slice 2: Pure intrinsic model validation without external schema state.
    """
    from agent.etl_pipeline.transformation_ir import (
        ClipNullPolicy,
        ExistingColumnPolicy,
        ScalarMapOperation,
        UnmappedValuePolicy,
    )

    col = ColumnRef(column_name="val", dataset_name="data")

    # ClipExpression: lower cannot exceed upper
    with pytest.raises(ValueError, match="lower_bound .* cannot exceed upper_bound"):
        ClipExpression(
            target_column=col,
            lower_bound=LiteralValue(value=100.0, data_type=DataType.FLOAT),
            upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
        )

    # FlagExpression: empty output_column rejected
    with pytest.raises(ValueError, match="requires a non-empty output_column"):
        FlagExpression(
            predicate=NullCheck(operand=col, is_null=True),
            output_column="   ",
        )

    # ScalarMapExpression: LOOKUP_MAP requires mapping
    with pytest.raises(ValueError, match="requires 'mapping'"):
        ScalarMapExpression(
            source_column=col,
            output_column="val_mapped",
            operation=ScalarMapOperation.LOOKUP_MAP,
            parameters={},
            unmapped_policy=UnmappedValuePolicy.SET_NULL,
        )


# ============================================================================
# 13. Vertical Slice 2: Layer 2 Schema Binding
# ============================================================================

def test_layer_2_schema_binding_and_validation():
    """
    Vertical Slice 2: Schema binding validates column existence, numeric types,
    flag collision policies, and AFTER_STEP reference lineage.
    """
    from agent.etl_pipeline.transformation_ir import (
        DatasetColumnSchema,
        DatasetSchema,
        ExistingColumnPolicy,
        TransformationIRInvariantError,
        bind_expression_to_schema,
    )

    schemas = {
        "accounts": DatasetSchema(
            dataset_name="accounts",
            columns={
                "account_id": DatasetColumnSchema(name="account_id", data_type=DataType.STRING),
                "balance": DatasetColumnSchema(name="balance", data_type=DataType.FLOAT),
                "status": DatasetColumnSchema(name="status", data_type=DataType.STRING),
            },
        )
    }

    # 1. Valid binding
    clip_valid = ClipExpression(
        target_column=ColumnRef(column_name="balance"),
        lower_bound=LiteralValue(value=0.0, data_type=DataType.FLOAT),
    )
    bound = bind_expression_to_schema(clip_valid, "accounts", schemas)
    assert bound.read_columns == ("balance",)
    assert bound.written_columns == ("balance",)

    # 2. Non-existent column rejected
    clip_missing = ClipExpression(
        target_column=ColumnRef(column_name="non_existent_col"),
        lower_bound=LiteralValue(value=0.0, data_type=DataType.FLOAT),
    )
    with pytest.raises(TransformationIRInvariantError, match="does not exist in dataset"):
        bind_expression_to_schema(clip_missing, "accounts", schemas)

    # 3. Non-numeric target column in ClipExpression rejected
    clip_str = ClipExpression(
        target_column=ColumnRef(column_name="status"),
        lower_bound=LiteralValue(value=0.0, data_type=DataType.FLOAT),
    )
    with pytest.raises(TransformationIRInvariantError, match="must be numeric"):
        bind_expression_to_schema(clip_str, "accounts", schemas)

    # 4. Flag collision with ExistingColumnPolicy.ERROR rejected
    flag_coll = FlagExpression(
        predicate=NullCheck(operand=ColumnRef(column_name="balance"), is_null=True),
        output_column="balance",  # Collides with existing column
        existing_column_policy=ExistingColumnPolicy.ERROR,
    )
    with pytest.raises(TransformationIRInvariantError, match="already exists in schema"):
        bind_expression_to_schema(flag_coll, "accounts", schemas)

    # 5. AFTER_STEP reference unresolvable in ancestor_step_ids rejected
    col_after = ColumnRef(
        column_name="balance",
        version=ValueVersion.AFTER_STEP,
        referenced_step_id="step_unknown_ancestor",
    )
    flag_after = FlagExpression(
        predicate=BinaryOp(left=col_after, op=BinaryOperator.GT, right=LiteralValue(value=0)),
        output_column="is_positive",
    )
    with pytest.raises(TransformationIRInvariantError, match="is not in ancestor steps"):
        bind_expression_to_schema(flag_after, "accounts", schemas, ancestor_step_ids=("step_01", "step_02"))


# ============================================================================
# 14. Vertical Slice 2: Quarantine Row-Identity Strategy & Copy vs Partition
# ============================================================================

def test_quarantine_row_identity_and_routing_modes():
    """
    Vertical Slice 2: Tests partition vs copy routing modes and stable row identity strategies.
    """
    from agent.etl_pipeline.transformation_ir import (
        RowIdentityStrategy,
    )

    col = ColumnRef(column_name="customer_id", dataset_name="crm")
    cond = NullCheck(operand=col, is_null=True)

    # Partition mode: filters_rows and partitions_rows are True
    quar_part = QuarantineExpression(
        predicate=cond,
        curated_output_ref="crm_clean",
        quarantine_output_ref="crm_rejects",
        routing_mode="partition",
        identity_strategy=RowIdentityStrategy.GENERATED_OCCURRENCE_ID,
    )
    eff_part = quar_part.derive_effects("crm")
    assert eff_part.filters_rows is True
    assert eff_part.partitions_rows is True
    assert quar_part.generated_identity_column == "_source_row_id"
    assert "_source_row_id" in eff_part.reads

    # Copy mode: filters_rows and partitions_rows are False (curated retains all rows, copy routed to quarantine)
    quar_copy = QuarantineExpression(
        predicate=cond,
        curated_output_ref="crm_curated",
        quarantine_output_ref="crm_audit_quarantine",
        routing_mode="copy",
        identity_strategy=RowIdentityStrategy.EXISTING_KEY,
        identity_columns=("customer_id",),
    )
    eff_copy = quar_copy.derive_effects("crm")
    assert eff_copy.filters_rows is False
    assert eff_copy.partitions_rows is False


# ============================================================================
# 15. Vertical Slice 2: TransformationIRGraph Envelope & Acyclic Invariants
# ============================================================================

def test_transformation_ir_graph_envelope_and_acyclic_invariants():
    """
    Vertical Slice 2: Tests TransformationIRGraph envelope, dependency checking, and cycle detection.
    """
    from agent.etl_pipeline.transformation_ir import (
        IRDependency,
        TransformationIRGraph,
        TransformationIRInvariantError,
    )

    col = ColumnRef(column_name="price", dataset_name="products")
    step_a = TransformationStepIR(
        step_id="step_a",
        dataset_name="products",
        family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=FlagExpression(
            predicate=BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=100.0)),
            output_column="high_price",
        ),
        target_column="high_price",
    )
    step_b = TransformationStepIR(
        step_id="step_b",
        dataset_name="products",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            upper_bound=LiteralValue(value=100.0, data_type=DataType.FLOAT),
        ),
        target_column="price",
        prerequisites=("step_a",),
    )

    # 1. Valid Acyclic Graph
    graph = TransformationIRGraph(
        graph_id="graph_products_cleansing",
        steps=(step_a, step_b),
        dependencies=(IRDependency(source_step_id="step_a", target_step_id="step_b"),),
    )
    assert len(graph.steps) == 2
    assert isinstance(graph.graph_hash, str) and len(graph.graph_hash) == 64

    # 2. Self-dependency rejected
    with pytest.raises((TransformationIRInvariantError, ValueError), match="Self-dependency detected"):
        TransformationIRGraph(
            graph_id="graph_self_dep",
            steps=(step_a,),
            dependencies=(IRDependency(source_step_id="step_a", target_step_id="step_a"),),
        )

    # 3. Cycle rejected
    with pytest.raises((TransformationIRInvariantError, ValueError), match="Cycle detected"):
        TransformationIRGraph(
            graph_id="graph_cycle",
            steps=(step_a, step_b),
            dependencies=(
                IRDependency(source_step_id="step_a", target_step_id="step_b"),
                IRDependency(source_step_id="step_b", target_step_id="step_a"),
            ),
        )


# ============================================================================
# 16. Vertical Slice 2: Authoritative Legacy Adapters & Graph Conversion
# ============================================================================

def test_authoritative_legacy_adapters_and_graph_conversion():
    """
    Vertical Slice 2: Tests plan_step_to_ir_graph produces consistent graph envelopes
    and verifies adapters for trim, uppercase, lowercase, replace_values,
    replace_sentinel_values, zero_to_null, range_clip, flag_range_violation.
    """
    from agent.etl_pipeline.transformation_ir import plan_step_to_ir_graph

    # 1. Single action: trim -> 1-step graph
    g_trim = plan_step_to_ir_graph({"step_id": "s_t", "dataset": "users", "column": "name", "action": "trim"})
    assert len(g_trim.steps) == 1
    assert len(g_trim.dependencies) == 0
    assert g_trim.steps[0].family == ActionFamily.MAP_VALUE

    # 2. Composite action: validate_and_partition_relationship -> 2-step graph with dependency
    g_comp = plan_step_to_ir_graph({
        "step_id": "s_rel",
        "dataset": "orders",
        "column": "customer_id",
        "action": "validate_and_partition_relationship",
    })
    assert len(g_comp.steps) == 2
    assert len(g_comp.dependencies) == 1
    assert g_comp.dependencies[0].source_step_id == g_comp.steps[0].step_id
    assert g_comp.dependencies[0].target_step_id == g_comp.steps[1].step_id

    # 3. replace_values
    s_rep = plan_step_to_ir({"step_id": "s_rv", "dataset": "users", "column": "state", "action": "replace_values", "params": {"replace_values": {"CA": "California"}}})
    assert s_rep.family == ActionFamily.MAP_VALUE
    assert isinstance(s_rep.expression, ScalarMapExpression)
    assert s_rep.expression.parameters["mapping"] == {"CA": "California"}

    # 4. flag_range_violation
    s_frv = plan_step_to_ir({"step_id": "s_frv", "dataset": "users", "column": "age", "action": "flag_range_violation", "params": {"lower_bound": 18, "upper_bound": 65}})
    assert s_frv.family == ActionFamily.FLAG
    assert isinstance(s_frv.expression, FlagExpression)
    assert s_frv.expression.output_column == "is_out_of_range_age"


# ============================================================================
# 17. Vertical Slice 2: IR JSON Serialization Round-Trip
# ============================================================================

def test_ir_json_serialization_round_trip():
    """
    Vertical Slice 2: Verifies lossless round-trip JSON serialization and deserialization.
    """
    col = ColumnRef(column_name="discount", version=ValueVersion.ORIGINAL)
    clip_expr = ClipExpression(
        target_column=col,
        upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
    )
    step = TransformationStepIR(
        step_id="step_serialize_test",
        dataset_name="sales",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=clip_expr,
        target_column="discount",
        prerequisites=("step_prev",),
        required_postconditions=("discount <= 50.0",),
    )

    json_str = step.model_dump_json()
    loaded_step = TransformationStepIR.model_validate_json(json_str)

    assert loaded_step.step_id == step.step_id
    assert loaded_step.family == step.family
    assert loaded_step.transformation_hash == step.transformation_hash
    assert loaded_step.plan_node_hash == step.plan_node_hash
    assert loaded_step.effects == step.effects


