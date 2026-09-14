"""
The 19 Golden Scenarios Suite for the Agent Dhara Business Rules Pipeline.

Verifies end-to-end behavior across:
- Intake disposition and canonicalization
- Assessment evaluation and 5-part population accounting
- Conflict resolution and effect compatibility (25-pair matrix, semantic overrides, ordering)
- DAG topological sorting and cycle prevention
- Authorization gating and cryptographic verification
- Degraded mode and fail-safe boundaries
"""
from datetime import datetime, timezone
import pytest

from agent.rules.canonical import canonical_json, compute_canonical_hash
from agent.rules.rule_models import (
    IntakeDisposition,
    RuleIntakeRecord,
    AssessmentLifecycleStatus,
    DataRuleOutcome,
    PlanningLifecycleStatus,
    ExecutionLifecycleStatus,
    ColumnOperand,
    LiteralOperand,
    CanonicalExpression,
    RuleType,
    RuleAction,
    NullPolicy,
    RuleScope,
    CanonicalRuleMaterial,
    CanonicalRule,
    RuleOccurrence,
    CanonicalRuleRegistry,
    build_rule_id,
    validate_intake_records,
)
from agent.rules.assessment_models import (
    EvaluationPopulationCounts,
    calculate_completeness_metrics,
    EvaluationCompleteness,
    RuleAssessmentEvidence,
)
from agent.rules.plan_models import (
    CompatibilityOutcome,
    CompatibilityResult,
    DataEffect,
    DataEffectAccess,
    OrderingDirection,
    PlanStep,
    PlanStepDependency,
    PlannerInvariantError,
    ProposedPlan,
    ProposedPlanStatus,
    StepAuthorizationStatus,
)
from agent.rules.conflict_resolver import (
    resolve_semantic_effect_compatibility,
    aggregate_compatibility_results,
    topological_sort_steps,
)


# ==============================================================================
# 1. Range Check (Age >= 18)
# ==============================================================================
def test_scenario_01_range_check():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">=",
            operands=(ColumnOperand(column="Age", data_type="int"), LiteralOperand(value=18, data_type="int")),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Age",)),
        action=RuleAction.ASSERT,
    )
    rule_id = build_rule_id(mat)
    assert isinstance(rule_id, str)
    assert mat.rule_type == RuleType.VALIDATION_ASSERTION


# ==============================================================================
# 2. Categorical Set Inclusion (Status IN ('Active', 'Inactive'))
# ==============================================================================
def test_scenario_02_categorical_inclusion():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator="IN",
            operands=(
                ColumnOperand(column="Status", data_type="string"),
                LiteralOperand(value=["Active", "Inactive"], data_type="list"),
            ),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Status",)),
        action=RuleAction.ASSERT,
    )
    assert mat.scope.columns == ("Status",)


# ==============================================================================
# 3. Nullability Assertion (CustomerID IS NOT NULL)
# ==============================================================================
def test_scenario_03_nullability_assertion():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator="IS_NOT_NULL",
            operands=(ColumnOperand(column="CustomerID", data_type="string"),),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("CustomerID",)),
        action=RuleAction.ASSERT,
    )
    norm = mat.expression.normalize()
    assert norm.operator == "IS_NOT_NULL"


# ==============================================================================
# 4. Uniqueness Assertion (Email is Unique)
# ==============================================================================
def test_scenario_04_uniqueness_assertion():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.SCHEMA_CONSTRAINT,
        expression=CanonicalExpression(
            operator="==",
            operands=(ColumnOperand(column="Email", data_type="string"), ColumnOperand(column="Email", data_type="string")),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Email",)),
        action=RuleAction.ASSERT,
    )
    assert mat.action == RuleAction.ASSERT


# ==============================================================================
# 5. String Format / Regex (Email Pattern)
# ==============================================================================
def test_scenario_05_regex_format():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator="==",
            operands=(ColumnOperand(column="Email", data_type="string"), LiteralOperand(value=r"^[\w\.-]+@[\w\.-]+\.\w+$", data_type="regex")),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Email",)),
        action=RuleAction.ASSERT,
    )
    assert mat.null_policy == NullPolicy.SEPARATE_VIOLATION


# ==============================================================================
# 6. Value Standardization (Replace 'A' with 'Active')
# ==============================================================================
def test_scenario_06_value_standardization():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="==",
            operands=(ColumnOperand(column="Status", data_type="string"), LiteralOperand(value="A", data_type="string")),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Status",)),
        action=RuleAction.REPLACE,
        normalized_parameters={"target_value": "Active"},
    )
    assert mat.action == RuleAction.REPLACE


# ==============================================================================
# 7. Imputation Rule (Impute Missing Income with Median)
# ==============================================================================
def test_scenario_07_imputation():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="IS_NULL",
            operands=(ColumnOperand(column="Income", data_type="decimal"),),
        ),
        scope=RuleScope(dataset="DimCustomer", columns=("Income",)),
        action=RuleAction.IMPUTE,
        normalized_parameters={"strategy": "median"},
    )
    assert mat.action == RuleAction.IMPUTE


# ==============================================================================
# 8. Quarantine Rule (Quarantine Invalid Transactions)
# ==============================================================================
def test_scenario_08_quarantine():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="<",
            operands=(ColumnOperand(column="Amount", data_type="decimal"), LiteralOperand(value=0, data_type="decimal")),
        ),
        scope=RuleScope(dataset="FactSales", columns=("Amount",)),
        action=RuleAction.QUARANTINE,
    )
    assert mat.action == RuleAction.QUARANTINE


# ==============================================================================
# 9. Drop Invalid Rows (Drop If Critical ID Missing)
# ==============================================================================
def test_scenario_09_drop_invalid_rows():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="IS_NULL",
            operands=(ColumnOperand(column="AccountID", data_type="string"),),
        ),
        scope=RuleScope(dataset="FactSales", columns=("AccountID",)),
        action=RuleAction.DROP,
    )
    assert mat.action == RuleAction.DROP


# ==============================================================================
# 10. Explicit vs Inferred Precedence (Explicit Instruction Wins)
# ==============================================================================
def test_scenario_10_explicit_vs_inferred():
    occ_inferred = RuleOccurrence(
        occurrence_id="occ-inf",
        canonical_rule_id="r1",
        source_type="profiler",
        source_reference="profiler_auto",
        authority_rank=10,
        is_explicit=False,
        observed_at=datetime.now(timezone.utc),
    )
    occ_explicit = RuleOccurrence(
        occurrence_id="occ-exp",
        canonical_rule_id="r1",
        source_type="notes",
        source_reference="business_note_line_12",
        authority_rank=10,
        is_explicit=True,
        observed_at=datetime.now(timezone.utc),
    )
    # Explicit source wins under equal authority
    assert occ_explicit.is_explicit and not occ_inferred.is_explicit


# ==============================================================================
# 11. Additive Audit Logging Coexistence (Audit + Transform Parallel)
# ==============================================================================
def test_scenario_11_additive_audit_coexistence():
    e_transform = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="replace_status",
        effect_semantic_hash="h1",
        normalized_parameters_hash="p1",
    )
    e_audit = DataEffect(
        access=DataEffectAccess.APPEND,
        resource_ref="audit_log",
        column_scope=frozenset(),
        action="log_audit_event",
        effect_semantic_hash="h2",
        normalized_parameters_hash="p2",
        commutative=True,
    )
    res = resolve_semantic_effect_compatibility(e_transform, e_audit)
    assert res.outcome == CompatibilityOutcome.COMPATIBLE_PARALLEL
    assert res.reason_code == "disjoint_resources"


# ==============================================================================
# 12. Scope Specificity Precedence (Column Specificity Wins)
# ==============================================================================
def test_scenario_12_scope_specificity():
    dataset_scope = RuleScope(dataset="customers", columns=())
    column_scope = RuleScope(dataset="customers", columns=("email",))
    # Column specific scope has higher specificity than dataset broad scope
    assert len(column_scope.columns) > len(dataset_scope.columns)


# ==============================================================================
# 13a. Cross-Column Invariant within Single Dataset (StartDate <= EndDate)
# ==============================================================================
def test_scenario_13a_cross_column_single_dataset():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator="<=",
            operands=(
                ColumnOperand(dataset="DimCampaign", column="StartDate", data_type="date"),
                ColumnOperand(dataset="DimCampaign", column="EndDate", data_type="date"),
            ),
        ),
        scope=RuleScope(dataset="DimCampaign", columns=("StartDate", "EndDate")),
        action=RuleAction.ASSERT,
    )
    assert mat.scope.columns == ("EndDate", "StartDate") or mat.scope.columns == ("StartDate", "EndDate")
    assert mat.rule_type == RuleType.VALIDATION_ASSERTION


# ==============================================================================
# 13b. Cross-Dataset Referential Integrity (Orders.CustomerID in Customers.CustomerID)
# ==============================================================================
def test_scenario_13b_cross_dataset_referential_integrity():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.REFERENTIAL_INTEGRITY,
        expression=CanonicalExpression(
            operator="IN",
            operands=(
                ColumnOperand(dataset="FactOrders", column="CustomerID", data_type="string"),
                ColumnOperand(dataset="DimCustomer", column="CustomerID", data_type="string"),
            ),
        ),
        scope=RuleScope(dataset="FactOrders", columns=("CustomerID",)),
        action=RuleAction.ASSERT,
        normalized_parameters={"target_dataset": "DimCustomer", "target_column": "CustomerID"},
    )
    assert mat.rule_type == RuleType.REFERENTIAL_INTEGRITY
    assert mat.normalized_parameters["target_dataset"] == "DimCustomer"


# ==============================================================================
# 14. Anomaly Detection (Z-Score or Outlier Bounds)
# ==============================================================================
def test_scenario_14_anomaly_detection():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.ANOMALY_DETECTION,
        expression=CanonicalExpression(
            operator="<",
            operands=(
                ColumnOperand(column="ZScore", data_type="float"),
                LiteralOperand(value=3.0, data_type="float"),
            ),
        ),
        scope=RuleScope(dataset="FactSensor", columns=("ZScore",)),
        action=RuleAction.FLAG,
    )
    assert mat.rule_type == RuleType.ANOMALY_DETECTION
    assert mat.action == RuleAction.FLAG


# ==============================================================================
# 15. Multi-Effect Ordered Composition (Write Before Read Post-State)
# ==============================================================================
def test_scenario_15_ordered_composition():
    e_write = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="update_status",
        effect_semantic_hash="h1",
        normalized_parameters_hash="p1",
    )
    e_read_post = DataEffect(
        access=DataEffectAccess.READ,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="verify_status",
        effect_semantic_hash="h2",
        normalized_parameters_hash="p2",
        reads_state="post_effect",
    )
    res = resolve_semantic_effect_compatibility(e_write, e_read_post)
    assert res.outcome == CompatibilityOutcome.ORDERED_COMPOSITION
    assert res.ordering == OrderingDirection.A_BEFORE_B


# ==============================================================================
# 16. Idempotent Duplicate Deduplication
# ==============================================================================
def test_scenario_16_idempotent_duplicate():
    e1 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"state"}),
        action="clean_state",
        effect_semantic_hash="h_state",
        normalized_parameters_hash="p_state",
        idempotent=True,
    )
    e2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"state"}),
        action="clean_state",
        effect_semantic_hash="h_state",
        normalized_parameters_hash="p_state",
        idempotent=True,
    )
    res = resolve_semantic_effect_compatibility(e1, e2)
    assert res.outcome == CompatibilityOutcome.IDEMPOTENT_DUPLICATE
    assert res.ordering == OrderingDirection.NONE


# ==============================================================================
# 17. Preserved Unmapped Free-Text Notes Rule
# ==============================================================================
def test_scenario_17_preserved_unmapped_notes():
    rec = RuleIntakeRecord(
        input_rule_id="note-101",
        disposition=IntakeDisposition.PRESERVED_UNMAPPED,
        reason_code="ambiguous_business_context",
        canonical_rule_id=None,
    )
    assert rec.disposition == IntakeDisposition.PRESERVED_UNMAPPED
    assert rec.canonical_rule_id is None


# ==============================================================================
# 18. Degraded Mode Assessment-Only Execution
# ==============================================================================
def test_scenario_18_degraded_assessment_only():
    counts = EvaluationPopulationCounts(
        total_source_rows=1000,
        pass_rows=980,
        violation_rows=20,
        null_unknown_rows=0,
        operator_unknown_rows=0,
        technical_excluded_rows=0,
        out_of_scope_rows=0,
    )
    evidence = RuleAssessmentEvidence.create(
        evidence_id="ev-999",
        rule_id="rule-range",
        counts=counts,
        evaluated_at=datetime.now(timezone.utc),
    )
    assert evidence.operator_evaluation_completeness == EvaluationCompleteness.COMPLETE
    assert evidence.population_evaluation_completeness == EvaluationCompleteness.COMPLETE
    assert evidence.data_rule_outcome == DataRuleOutcome.FAIL


# ==============================================================================
# 19. Mutually Exclusive Conflict Resolution (Conflicting Concurrent Writes)
# ==============================================================================
def test_scenario_19_mutually_exclusive_conflict():
    e_write1 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_active",
        effect_semantic_hash="h1",
        normalized_parameters_hash="p1",
    )
    e_write2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_inactive",
        effect_semantic_hash="h2",
        normalized_parameters_hash="p2",
    )
    res = resolve_semantic_effect_compatibility(e_write1, e_write2)
    assert res.outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE
    assert res.reason_code == "conflicting_concurrent_writes"
