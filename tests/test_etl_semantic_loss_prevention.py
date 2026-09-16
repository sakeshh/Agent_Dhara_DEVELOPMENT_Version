"""
Tests for ETL Semantic Loss Prevention & Fail-Fast Architecture (Phase 1).
"""
import pytest
from agent.etl_pipeline.llm_resolution_fallback import (
    ResolutionStatus,
    AdvisoryExtensionProposal,
    validate_resolution_options,
)
from agent.etl_pipeline.codegen_shared import (
    UnsupportedCodegenActionError,
    CodegenStatus,
    CodegenResult,
    generate_codegen_result,
)
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.python_codegen import generate_python_etl
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.adf_expressions import adf_expression_for_step


def test_llm_fallback_unmapped_action_preserved_as_advisory():
    """Unmapped actions must never be silently converted to noop."""
    raw = [
        {"id": "opt_1", "label": "Perform Magic Quantum Fix", "action": "quantum_remediate_v2", "confidence": 0.95},
        {"id": "opt_2", "label": "Standard Trim", "action": "trim", "confidence": 0.8},
    ]
    validated = validate_resolution_options(raw)
    unsupported = next(o for o in validated if o["id"] == "opt_1")

    assert unsupported["action"] == "unsupported_action"
    assert unsupported["raw_action"] == "quantum_remediate_v2"
    assert unsupported["execution_eligible"] is False
    assert unsupported["resolution_status"] == ResolutionStatus.UNSUPPORTED_ACTION.value
    assert unsupported["requires_human_approval"] is True
    assert unsupported["recommended"] is False


def test_advisory_proposal_model_validators():
    """Advisory proposals must enforce execution_eligible=False and non-SUPPORTED status."""
    with pytest.raises(ValueError, match=r"(Advisory proposals must never be execution eligible|Input should be False)"):
        AdvisoryExtensionProposal(
            proposal_id="p1",
            issue_id="iss_1",
            raw_proposed_action="custom_code",
            execution_eligible=True,  # Should fail validation
        )

    with pytest.raises(ValueError, match="Supported actions must use typed action models"):
        AdvisoryExtensionProposal(
            proposal_id="p2",
            issue_id="iss_2",
            raw_proposed_action="trim",
            resolution_status=ResolutionStatus.SUPPORTED,  # Should fail validation
        )


def test_pyspark_codegen_fails_fast_on_unsupported_action():
    """PySpark generator must raise UnsupportedCodegenActionError, never emit a comment."""
    plan = {
        "plan_id": "test_spark_unsupported",
        "datasets": {
            "customers": {
                "steps": [
                    {"id": "step_u1", "column": "email", "action": "unsupported_spark_algorithm", "params": {}}
                ]
            }
        }
    }
    with pytest.raises(UnsupportedCodegenActionError) as exc_info:
        generate_pyspark_etl(plan, {})
    assert exc_info.value.step_id == "step_u1"
    assert exc_info.value.action == "unsupported_spark_algorithm"
    assert exc_info.value.engine == "pyspark"


def test_python_codegen_fails_fast_on_unsupported_action():
    """Python generator must raise UnsupportedCodegenActionError, never emit a comment."""
    plan = {
        "plan_id": "test_py_unsupported",
        "datasets": {
            "orders": {
                "steps": [
                    {"id": "step_u2", "column": "total", "action": "unsupported_python_method", "params": {}}
                ]
            }
        }
    }
    with pytest.raises(UnsupportedCodegenActionError) as exc_info:
        generate_python_etl(plan, {})
    assert exc_info.value.step_id == "step_u2"
    assert exc_info.value.action == "unsupported_python_method"
    assert exc_info.value.engine == "python"


def test_sql_codegen_fails_fast_on_unsupported_action():
    """SQL generator must raise UnsupportedCodegenActionError on unmapped actions."""
    plan = {
        "plan_id": "test_sql_unsupported",
        "datasets": {
            "dbo.items": {
                "steps": [
                    {"id": "step_u3", "column": "sku", "action": "unsupported_sql_func", "params": {}}
                ]
            }
        }
    }
    with pytest.raises(UnsupportedCodegenActionError) as exc_info:
        generate_sql_etl(plan, {})
    assert exc_info.value.step_id == "step_u3"
    assert exc_info.value.action == "unsupported_sql_func"
    assert exc_info.value.engine == "sql"


def test_adf_expressions_fails_fast_on_unsupported_action():
    """ADF expressions generator must raise UnsupportedCodegenActionError."""
    step = {"id": "step_u4", "action": "unsupported_adf_macro"}
    with pytest.raises(UnsupportedCodegenActionError) as exc_info:
        adf_expression_for_step("unsupported_adf_macro", "data", step)
    assert exc_info.value.step_id == "step_u4"
    assert exc_info.value.action == "unsupported_adf_macro"
    assert exc_info.value.engine == "adf"


def test_atomic_codegen_result_no_partial_artifacts():
    """Top-level CodegenResult must have 0 artifacts and FAILED_UNSUPPORTED_ACTION status on error."""
    broken_plan = {
        "plan_id": "broken_spark_plan",
        "datasets": {
            "customers": {
                "steps": [
                    {"id": "step_valid", "column": "name", "action": "trim"},
                    {"id": "step_broken", "column": "email", "action": "unsupported_neural_cleanse"},
                ]
            }
        }
    }
    result = generate_codegen_result(generate_pyspark_etl, broken_plan, {}, engine="pyspark")
    assert result.status == CodegenStatus.FAILED_UNSUPPORTED_ACTION
    assert result.artifact is None
    assert result.partial_artifact_count == 0
    assert "step_broken" in result.failed_step_ids


# ============================================================================
# Increment 2 Tests: Evidence-Planning Reconciliation Bridge
# ============================================================================

from agent.etl_pipeline.assessment_planning_bridge import (
    AssessmentPlanningDisposition,
    EvidencePlanningRecord,
    EvidencePlanningRelevance,
    reconcile_assessment_to_plan,
    validate_acyclic_duplicate_graph,
)


def test_reconciliation_bidirectional_success():
    """All planning-relevant evidence linked to plan steps or review items."""
    assessment_issues = [
        {"evidence_id": "ev_null_1", "issue_type": "missing_values", "category": "completeness"},
        {"evidence_id": "ev_dup_1", "issue_type": "exact_duplicates", "category": "uniqueness"},
        {"evidence_id": "ev_info_1", "issue_type": "dataset_profiled", "category": "profiling_summary"},
    ]
    plan_steps = [
        {"id": "step_1", "action": "fill_nulls_simple", "evidence_id": "ev_null_1"},
    ]
    manual_review = [
        {"id": "rev_1", "evidence_id": "ev_dup_1"},
    ]

    result = reconcile_assessment_to_plan(assessment_issues, plan_steps, manual_review_items=manual_review)
    assert result.reconciled is True
    assert result.total_evidence_count == 3
    assert result.planning_relevant_count == 2
    assert len(result.unreconciled_evidence_ids) == 0
    assert len(result.orphan_step_ids) == 0
    assert result.records["ev_info_1"].disposition == AssessmentPlanningDisposition.OUT_OF_SCOPE


def test_reconciliation_detects_unreconciled_evidence():
    """An unaddressed planning-relevant evidence item causes reconciliation to fail."""
    assessment_issues = [
        {"evidence_id": "ev_missing_remediation", "issue_type": "out_of_range", "category": "validity"},
    ]
    result = reconcile_assessment_to_plan(assessment_issues, [])
    assert result.reconciled is False
    assert "ev_missing_remediation" in result.unreconciled_evidence_ids


def test_reconciliation_detects_orphan_steps():
    """Plan steps with no evidence link and no recognized business rule are flagged."""
    plan_steps = [
        {"id": "orphan_step_99", "action": "trim"},
    ]
    result = reconcile_assessment_to_plan([], plan_steps, rule_ids={"BR_KNOWN_01"})
    assert result.reconciled is False
    assert "orphan_step_99" in result.orphan_step_ids


def test_reconciliation_supports_rule_linked_steps():
    """Plan steps generated from business rules without assessment evidence reconcile cleanly."""
    plan_steps = [
        {"id": "rule_step_1", "action": "trim", "rule_id": "BR_GOV_01"},
    ]
    result = reconcile_assessment_to_plan([], plan_steps, rule_ids={"BR_GOV_01"})
    assert result.reconciled is True
    assert len(result.orphan_step_ids) == 0


def test_acyclic_duplicate_graph_validation():
    """Consolidation graph cycles and self-references must be rejected."""
    # Self-reference
    with pytest.raises(ValueError, match="Self-referential duplicate consolidation"):
        validate_acyclic_duplicate_graph({"ev_1": "ev_1"})

    # Cycle: ev_1 -> ev_2 -> ev_3 -> ev_1
    with pytest.raises(ValueError, match="Circular duplicate consolidation"):
        validate_acyclic_duplicate_graph({
            "ev_1": "ev_2",
            "ev_2": "ev_3",
            "ev_3": "ev_1",
        })

    # Acyclic forest: ev_1 -> ev_3, ev_2 -> ev_3 (valid)
    validate_acyclic_duplicate_graph({
        "ev_1": "ev_3",
        "ev_2": "ev_3",
    })


# ============================================================================
# Increment 3 Tests: Canonical Planner Integration
# ============================================================================

from agent.etl_pipeline.planner import PlanningInputMode, build_etl_plan
from agent.rules.rule_models import (
    CanonicalExpression,
    CanonicalRule,
    CanonicalRuleRegistry,
    CanonicalRuleMaterial,
    ColumnOperand,
    LiteralOperand,
    RuleAction,
    RuleScope,
    RuleType,
)


def _make_dummy_assessment():
    return {
        "datasets": {
            "customers": {
                "columns": {
                    "cust_id": {"dtype": "int", "null_percentage": 0.0},
                    "country_code": {"dtype": "string", "null_percentage": 2.0},
                    "phone": {"dtype": "string", "null_percentage": 0.0},
                    "age": {"dtype": "int", "null_percentage": 0.0},
                },
                "row_count": 100,
            }
        }
    }


def test_canonical_rule_intake_bypasses_4_keyword_filter():
    """Actionable canonical rule (e.g. STANDARDIZE/REPLACE on country_code) produces plan steps."""
    reg = CanonicalRuleRegistry()
    rule = CanonicalRule(
        rule_id="RULE_CC_01",
        logical_rule_id="L_CC_01",
        rule_version=1,
        name="Standardize Country Code",
        rule_type=RuleType.TRANSFORMATION,
        material=CanonicalRuleMaterial(
            rule_type=RuleType.TRANSFORMATION,
            action=RuleAction.STANDARDIZE,
            expression=CanonicalExpression(
                operator="!=",
                operands=(ColumnOperand(column="country_code"), LiteralOperand(value="", data_type="string")),
            ),
            scope=RuleScope(dataset="customers", columns=("country_code",)),
            normalized_parameters={"standardize_type": "uppercase"},
        ),
    )
    reg.rules[rule.rule_id] = rule

    assess = _make_dummy_assessment()
    plan = build_etl_plan(
        assess,
        {"canonical_registry": reg, "planning_input_mode": PlanningInputMode.CANONICAL_ONLY},
    )

    steps = plan["datasets"]["customers"]["steps"]
    step = next((s for s in steps if s.get("rule_id") == "RULE_CC_01"), None)
    assert step is not None
    assert step["action"] == "uppercase"
    assert step["column"] == "country_code"
    assert "RULE_CC_01" in step["supporting_rule_ids"]
    assert plan["planning_input_mode"] == "canonical_only"


def test_canonical_assert_rules_strictly_excluded_from_transformation():
    """ASSERT rules must never generate transformation steps."""
    reg = CanonicalRuleRegistry()
    assert_rule = CanonicalRule(
        rule_id="RULE_AGE_CHECK",
        logical_rule_id="L_AGE_CHECK",
        rule_version=1,
        name="Assert Positive Age",
        rule_type=RuleType.VALIDATION_ASSERTION,
        material=CanonicalRuleMaterial(
            rule_type=RuleType.VALIDATION_ASSERTION,
            action=RuleAction.ASSERT,
            expression=CanonicalExpression(
                operator=">=",
                operands=(ColumnOperand(column="age"), LiteralOperand(value=0, data_type="int")),
            ),
            scope=RuleScope(dataset="customers", columns=("age",)),
            normalized_parameters={"expression": "age >= 0"},
        ),
    )
    reg.rules[assert_rule.rule_id] = assert_rule

    assess = _make_dummy_assessment()
    plan = build_etl_plan(
        assess,
        {"canonical_registry": reg, "planning_input_mode": PlanningInputMode.CANONICAL_ONLY},
    )

    steps = plan["datasets"]["customers"]["steps"]
    rule_age_steps = [s for s in steps if s.get("rule_id") == "RULE_AGE_CHECK"]
    assert len(rule_age_steps) == 0, "ASSERT rules must NOT produce transformation steps"


# ============================================================================
# Increment 4 Tests: Manual-Review Workflow Upgrades
# ============================================================================

from datetime import datetime, timezone
from agent.etl_pipeline.manual_review_catalog import (
    OptionReadiness,
    ManualReviewDecision,
    SourceSystemDeferral,
    enrich_manual_review_item,
    tag_option_readiness,
)


def test_manual_review_decision_validation():
    """ManualReviewDecision enforces advisory blocks, promotion boundary, and human approval."""
    # Direct execution is strictly forbidden
    with pytest.raises(ValueError, match="Manual review decisions cannot be executed directly"):
        ManualReviewDecision(
            review_item_id="rev_1",
            selected_option_id="opt_1",
            selected_action="trim",
            readiness=OptionReadiness.READY_FOR_PLAN_PROPOSAL,
            execution_eligible=True,
        )

    # Advisory cannot be plan proposal eligible
    with pytest.raises(ValueError, match="Advisory-only decisions cannot be plan proposal eligible"):
        ManualReviewDecision(
            review_item_id="rev_1",
            selected_option_id="opt_1",
            selected_action="unsupported_action",
            readiness=OptionReadiness.ADVISORY_ONLY,
            proposal_eligible=True,
        )

    # Human approval requires approved_by
    with pytest.raises(ValueError, match="must specify approved_by"):
        ManualReviewDecision(
            review_item_id="rev_2",
            selected_option_id="opt_2",
            selected_action="deduplicate",
            readiness=OptionReadiness.REQUIRES_HUMAN_APPROVAL,
            proposal_eligible=True,
            approved_by=None,
        )

    # Valid human approval
    decision = ManualReviewDecision(
        review_item_id="rev_2",
        selected_option_id="opt_2",
        selected_action="deduplicate",
        readiness=OptionReadiness.REQUIRES_HUMAN_APPROVAL,
        proposal_eligible=True,
        approved_by="DataSteward_Alice",
        approval_timestamp=datetime.now(timezone.utc),
        decision_rationale="Confirmed true duplicate removal policy",
    )
    assert decision.approved_by == "DataSteward_Alice"
    assert decision.proposal_eligible is True

    # Valid ready for plan proposal
    decision_ready = ManualReviewDecision(
        review_item_id="rev_3",
        selected_option_id="opt_3",
        selected_action="trim",
        readiness=OptionReadiness.READY_FOR_PLAN_PROPOSAL,
        proposal_eligible=True,
    )
    assert decision_ready.readiness == OptionReadiness.READY_FOR_PLAN_PROPOSAL
    assert decision_ready.proposal_eligible is True

    # Promotion to RemediationIntent
    intent = decision_ready.to_remediation_intent()
    assert intent.action == "trim"
    assert intent.review_item_id == "rev_3"


def test_source_system_deferral_model():
    """SourceSystemDeferral model creates clean deferral records."""
    deferral = SourceSystemDeferral(
        deferral_id="DEF_001",
        evidence_id="EV_NULL_01",
        source_system="ERP_SAP",
        target_ticket_id="JIRA-4821",
        expected_fix_version="v2.4",
        deferred_by="DataArchitect_Bob",
        deferred_at=datetime.now(timezone.utc),
        rationale="Source upstream bug documented in JIRA-4821; to be fixed at source",
    )
    assert deferral.status == "active"
    assert deferral.target_ticket_id == "JIRA-4821"


def test_enrich_manual_review_item_readiness_tagging():
    """enrich_manual_review_item assigns readiness tags to all options."""
    item = {
        "dataset": "customers",
        "column": "customer_id",
        "issue_type": "exact_duplicate_rows",
    }
    enriched = enrich_manual_review_item(item)
    opts = enriched["resolution_options"]
    assert len(opts) > 0

    # Deduplicate option has REQUIRES_HUMAN_APPROVAL and proposal_eligible=True, execution_eligible=False
    dedup_opt = next((o for o in opts if o.get("action") == "deduplicate"), None)
    assert dedup_opt is not None
    assert dedup_opt["readiness"] == OptionReadiness.REQUIRES_HUMAN_APPROVAL.value
    assert dedup_opt["proposal_eligible"] is True
    assert dedup_opt["execution_eligible"] is False

    # Keep_as_is (noop) has READY_FOR_PLAN_PROPOSAL and proposal_eligible=True, execution_eligible=False
    keep_opt = next((o for o in opts if o.get("action") == "noop"), None)
    assert keep_opt is not None
    assert keep_opt["readiness"] == OptionReadiness.READY_FOR_PLAN_PROPOSAL.value
    assert keep_opt["proposal_eligible"] is True
    assert keep_opt["execution_eligible"] is False

    # Advisory / unsupported action must be tagged ADVISORY_ONLY and not proposal eligible
    unsupported_opt = tag_option_readiness({
        "id": "advisory_opt",
        "label": "Advisory proposal",
        "action": "unsupported_action",
    })
    assert unsupported_opt["readiness"] == OptionReadiness.ADVISORY_ONLY.value
    assert unsupported_opt["proposal_eligible"] is False
    assert unsupported_opt["execution_eligible"] is False


# ============================================================================
# Additional High-Value Certification Tests
# ============================================================================

def test_bidirectional_lineage_strict_validation():
    """PlanStep referencing non-existent evidence or non-planning-relevant evidence fails reverse reconciliation."""
    # 1. Step referencing non-existent evidence
    plan_steps = [
        {"id": "step_ghost", "action": "trim", "evidence_id": "EV_NON_EXISTENT"},
    ]
    res = reconcile_assessment_to_plan([], plan_steps)
    assert res.reconciled is False
    assert "step_ghost" in res.orphan_step_ids
    assert any("references non-existent evidence" in d for d in res.diagnostics)

    # 2. Step referencing informational evidence (cannot be remediation target)
    info_evidence = [
        {"evidence_id": "EV_INFO_01", "category": "informational", "issue_type": "profiling_summary"},
    ]
    plan_steps_info = [
        {"id": "step_info", "action": "trim", "evidence_id": "EV_INFO_01"},
    ]
    res2 = reconcile_assessment_to_plan(info_evidence, plan_steps_info)
    assert res2.reconciled is False
    assert "step_info" in res2.orphan_step_ids
    assert any("references non-planning-relevant evidence" in d for d in res2.diagnostics)


def test_informational_profiling_evidence_retained_not_remediation_target():
    """Informational profiling evidence is retained in records but excluded from ETL planning ledger."""
    evidence = [
        {"evidence_id": "EV_INFO_99", "category": "informational", "issue_type": "profiling_summary"},
        {"evidence_id": "EV_DIAG_01", "category": "system_diagnostic", "issue_type": "column_profile_complete"},
    ]
    res = reconcile_assessment_to_plan(evidence, [])
    assert res.reconciled is True
    assert res.planning_relevant_count == 0
    assert len(res.unreconciled_evidence_ids) == 0
    assert res.records["EV_INFO_99"].relevance.value == "informational"
    assert res.records["EV_DIAG_01"].relevance.value == "assessment_diagnostic"


def test_hybrid_mode_shadow_only_isolation():
    """In HYBRID_COMPATIBILITY mode, canonical rules are authoritative and legacy notes are shadow-only."""
    reg = CanonicalRuleRegistry()
    can_rule = CanonicalRule(
        rule_id="RULE_PHONE_MASK",
        logical_rule_id="L_PHONE_MASK",
        rule_version=1,
        name="Mask Phone Number",
        rule_type=RuleType.TRANSFORMATION,
        material=CanonicalRuleMaterial(
            rule_type=RuleType.TRANSFORMATION,
            action=RuleAction.STANDARDIZE,
            expression=CanonicalExpression(
                operator="!=",
                operands=(ColumnOperand(column="phone"), LiteralOperand(value="", data_type="string")),
            ),
            scope=RuleScope(dataset="customers", columns=("phone",)),
            normalized_parameters={"standardize_type": "mask_phone"},
        ),
    )
    reg.rules[can_rule.rule_id] = can_rule

    # Legacy business notes specifying the same rule instruction
    legacy_notes = "mask phone in customers.phone"

    assess = _make_dummy_assessment()
    plan = build_etl_plan(
        assess,
        {
            "canonical_registry": reg,
            "notes": legacy_notes,
            "planning_input_mode": PlanningInputMode.HYBRID_COMPATIBILITY,
        },
    )

    meta = plan.get("planning_mode_metadata") or {}
    assert meta.get("input_mode") == "hybrid_compatibility"
    assert meta.get("authoritative_source") == "canonical_registry"
    assert meta.get("legacy_shadow_only") is True

    # Executable dataset steps must have ONLY ONE authoritative mask_phone step from canonical (not duplicated by legacy notes)
    steps = plan["datasets"]["customers"]["steps"]
    phone_steps = [s for s in steps if s.get("column") == "phone" and s.get("action") == "mask_phone"]
    assert len(phone_steps) == 1
    assert phone_steps[0].get("rule_id") == "RULE_PHONE_MASK"
    assert phone_steps[0]["action"] == "mask_phone"

    # Legacy step must be preserved in legacy_shadow_steps only
    shadow = plan.get("legacy_shadow_steps") or []
    assert len(shadow) >= 1
    assert any(s.get("source_issue_type") == "business_notes" and s.get("action") == "mask_phone" for s in shadow)


def test_codegen_failure_signing_and_persistence_spy():
    """When codegen compilation fails, signing and persisting functions are NEVER invoked."""
    signed_calls = []
    persisted_calls = []

    def spy_signing(code: str) -> str:
        signed_calls.append(code)
        return f"SIGNED: {code}"

    def spy_persisting(code: str):
        persisted_calls.append(code)

    def failing_generator(plan, assessment):
        raise UnsupportedCodegenActionError(step_id="step_fail_1", action="quantum_teleport", engine="python")

    result = generate_codegen_result(
        failing_generator,
        {"plan_id": "P1"},
        {},
        engine="python",
        signing_fn=spy_signing,
        persister_fn=spy_persisting,
    )

    # Validate exact failure invariant:
    # If CodegenResult.status != SUCCESS: artifact is None, persisted_artifact_count=0, signed_artifact_count=0, execution_eligible=False
    assert result.status == CodegenStatus.FAILED_UNSUPPORTED_ACTION
    assert result.artifact is None
    assert result.persisted_artifact_count == 0
    assert result.signed_artifact_count == 0
    assert result.execution_eligible is False
    assert len(signed_calls) == 0
    assert len(persisted_calls) == 0
    assert "step_fail_1" in result.failed_step_ids


def test_advisory_proposal_cannot_generate_direct_plan_step():
    """AdvisoryExtensionProposal has execution_eligible=False and cannot be directly executed."""
    prop = AdvisoryExtensionProposal(
        proposal_id="ADV_001",
        issue_id="ISSUE_001",
        raw_proposed_action="ml_impute",
        execution_eligible=False,
    )
    assert prop.execution_eligible is False
    # Attempting to assign execution_eligible=True to advisory proposal must fail
    with pytest.raises(Exception):
        AdvisoryExtensionProposal(
            proposal_id="ADV_002",
            issue_id="ISSUE_002",
            raw_proposed_action="ml_impute",
            execution_eligible=True,
        )


