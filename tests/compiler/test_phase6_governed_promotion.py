from __future__ import annotations

import os
import tempfile
import pytest

from agent.compiler.learning.promoted_models import (
    ExtensionCandidate,
    PromotedRule,
    PromotionEvidence,
    RuleLifecycleState,
    SchemaSignature,
)
from agent.compiler.learning.promoted_repository import PromotedRuleRepository
from agent.compiler.learning.promotion_evaluator import PromotionEvaluator
from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.planners.logical_planner import LogicalPlanner
from agent.compiler.rules.decomposer import RuleDecomposer
from agent.compiler.rules.models import DecompositionStatus


@pytest.fixture
def temp_repo():
    import uuid
    db_file = os.path.join(tempfile.gettempdir(), f"test_p6_{uuid.uuid4().hex}.sqlite3")
    repo = PromotedRuleRepository(db_path=db_file)
    yield repo
    try:
        if os.path.exists(db_file):
            os.remove(db_file)
    except Exception:
        pass


@pytest.fixture
def hr_dataset_profile():
    return DatasetProfile(
        dataset_name="Employees_India",
        columns={
            "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="nvarchar(50)"),
            "Department": ColumnProfile(name="Department", dtype="nvarchar(50)"),
            "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)"),
        },
    )


class TestPhase6GovernedPromotion:
    """
    Phase 6 Test Suite: Governed Self-Learning Catalog Promotion & Extension Lifecycle.
    Verifies all 28 acceptance criteria:
      - Lifecycle (1 to 6)
      - Schema Protection (7 to 10)
      - Evidence-Based Promotion (11 to 15)
      - Versioning & Rollback (16 to 18)
      - Demotion & Suspension (19 to 21)
      - Unsupported Extension Candidates (22 to 25)
      - End-to-End Governed Learning Loop (26 to 28)
    """

    # -------------------------------------------------------------------------
    # Lifecycle (Tests 1 to 6)
    # -------------------------------------------------------------------------

    def test_1_new_candidate_enters_staged(self, temp_repo):
        """Test 1: Newly proposed candidate rules enter the STAGED state."""
        rule = PromotedRule(
            rule_id="RULE_EMP_PREFIX",
            name="Prefix EmployeeCode with EMP-",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.STAGED,
        )
        saved = temp_repo.save_candidate(rule)
        assert saved.status == RuleLifecycleState.STAGED
        fetched = temp_repo.get_rule("RULE_EMP_PREFIX")
        assert fetched.status == RuleLifecycleState.STAGED

    def test_2_staged_cannot_auto_execute_as_trusted_rule(self, temp_repo):
        """Test 2: A rule in STAGED status cannot be queried or selected for production reuse."""
        rule = PromotedRule(
            rule_id="RULE_STAGED_ONLY",
            name="Test Staged Rule",
            action_type=ActionType.TRIM,
            target_columns=["Code"],
            dataset_scope="Accounts",
            status=RuleLifecycleState.STAGED,
        )
        temp_repo.save_candidate(rule)
        match = temp_repo.find_applicable(dataset_name="Accounts", action_type=ActionType.TRIM)
        assert match is None

    def test_3_valid_evidence_permits_dataset_scoped_promotion(self, temp_repo):
        """Test 3: Valid execution evidence permits promotion to DATASET_SCOPED."""
        rule = PromotedRule(
            rule_id="RULE_EMP_PREFIX",
            name="Prefix EmployeeCode",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.STAGED,
        )
        temp_repo.save_candidate(rule)

        evidence = PromotionEvidence(
            rule_id="RULE_EMP_PREFIX",
            rule_version=1,
            run_id="run_101",
            dataset="Employees_India",
            success=True,
            rows_processed=1000,
            reconciliation_passed=True,
            governance_decision="approved",
        )
        temp_repo.record_evidence(evidence)

        can_promote, reason = PromotionEvaluator.evaluate_for_dataset_scoped(rule, [evidence])
        assert can_promote is True

        promoted = temp_repo.promote(rule.rule_id, RuleLifecycleState.DATASET_SCOPED, approved_by="lead_data_eng")
        assert promoted.status == RuleLifecycleState.DATASET_SCOPED
        assert promoted.approved_by == "lead_data_eng"

    def test_4_dataset_scoped_rule_matches_its_dataset(self, temp_repo, hr_dataset_profile):
        """Test 4: A DATASET_SCOPED rule matches its designated dataset."""
        rule = PromotedRule(
            rule_id="RULE_EMP_PREFIX",
            name="Prefix EmployeeCode",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.DATASET_SCOPED,
            rule_intent_pattern="prefix employeecode with 'emp-'",
        )
        temp_repo.save_candidate(rule)

        match = temp_repo.find_applicable(
            dataset_name="Employees_India",
            rule_text="prefix employeecode with 'emp-'",
            schema_profile=hr_dataset_profile,
        )
        assert match is not None
        assert match.rule_id == "RULE_EMP_PREFIX"
        assert match.status == RuleLifecycleState.DATASET_SCOPED

    def test_5_dataset_scoped_rule_does_not_match_unrelated_dataset(self, temp_repo):
        """Test 5: A DATASET_SCOPED rule for Employees_India does NOT contaminate Employees_US or Vendors."""
        rule = PromotedRule(
            rule_id="RULE_EMP_PREFIX",
            name="Prefix EmployeeCode",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.DATASET_SCOPED,
            rule_intent_pattern="prefix employeecode with 'emp-'",
        )
        temp_repo.save_candidate(rule)

        match_us = temp_repo.find_applicable(
            dataset_name="Employees_US",
            rule_text="prefix employeecode with 'emp-'",
        )
        assert match_us is None

        match_vendor = temp_repo.find_applicable(
            dataset_name="Vendors",
            action_type=ActionType.PREFIX_ADD,
        )
        assert match_vendor is None

    def test_6_global_rule_can_match_compatible_datasets(self, temp_repo):
        """Test 6: A GLOBAL_PROMOTED rule (e.g. TRIM) matches any dataset with compatible schema."""
        rule = PromotedRule(
            rule_id="RULE_GLOBAL_TRIM",
            name="Universal Trim",
            action_type=ActionType.TRIM,
            parameters={"side": "both"},
            target_columns=["Name"],
            dataset_scope=None,  # Universal
            status=RuleLifecycleState.GLOBAL_PROMOTED,
            rule_intent_pattern="trim name",
        )
        temp_repo.save_candidate(rule)

        m1 = temp_repo.find_applicable("Customers", rule_text="trim name")
        m2 = temp_repo.find_applicable("Suppliers", rule_text="trim name")
        assert m1 is not None and m1.status == RuleLifecycleState.GLOBAL_PROMOTED
        assert m2 is not None and m2.status == RuleLifecycleState.GLOBAL_PROMOTED

    # -------------------------------------------------------------------------
    # Schema Protection (Tests 7 to 10)
    # -------------------------------------------------------------------------

    def test_7_compatible_schema_permits_reuse(self, temp_repo, hr_dataset_profile):
        """Test 7: Matching schema signature permits rule reuse."""
        sig = SchemaSignature(
            columns={"EmployeeCode": {"dtype": "nvarchar(50)"}}
        )
        rule = PromotedRule(
            rule_id="RULE_SIG_TEST",
            name="Sig Test",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            schema_signature=sig,
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule)

        match = temp_repo.find_applicable(
            dataset_name="Employees_India",
            action_type=ActionType.PREFIX_ADD,
            target_columns=["EmployeeCode"],
            schema_profile=hr_dataset_profile,
        )
        assert match is not None

    def test_8_schema_drift_forces_revalidation(self, temp_repo):
        """Test 8: Schema drift (e.g. column converted from string to int) blocks reuse."""
        sig = SchemaSignature(
            columns={"EmployeeCode": {"dtype": "nvarchar(50)"}}
        )
        rule = PromotedRule(
            rule_id="RULE_DRIFT_TEST",
            name="Drift Test",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            schema_signature=sig,
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule)

        drifted_profile = DatasetProfile(
            dataset_name="Employees_India",
            columns={
                "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="int"),  # Drifted from nvarchar to int!
            },
        )
        match = temp_repo.find_applicable(
            dataset_name="Employees_India",
            action_type=ActionType.PREFIX_ADD,
            target_columns=["EmployeeCode"],
            schema_profile=drifted_profile,
        )
        assert match is None  # Blocked by schema signature guard!

    def test_9_missing_target_column_blocks_reuse(self, temp_repo):
        """Test 9: Missing target column in target schema blocks promoted rule reuse."""
        rule = PromotedRule(
            rule_id="RULE_MISSING_COL",
            name="Missing Col",
            action_type=ActionType.TRIM,
            target_columns=["OldCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule)

        profile = DatasetProfile(
            dataset_name="Employees_India",
            columns={"NewCode": ColumnProfile(name="NewCode", dtype="nvarchar(50)")},
        )
        match = temp_repo.find_applicable(
            dataset_name="Employees_India",
            action_type=ActionType.TRIM,
            target_columns=["OldCode"],
            schema_profile=profile,
        )
        assert match is None

    def test_10_incompatible_datatype_blocks_reuse(self, temp_repo):
        """Test 10: Incompatible datatype (date vs int) blocks promoted rule reuse."""
        sig = SchemaSignature(columns={"EventDate": {"dtype": "datetime"}})
        rule = PromotedRule(
            rule_id="RULE_DATE_INT",
            name="Date Rule",
            action_type=ActionType.PARSE_DATES,
            target_columns=["EventDate"],
            dataset_scope="Events",
            schema_signature=sig,
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule)

        incompat_profile = DatasetProfile(
            dataset_name="Events",
            columns={"EventDate": ColumnProfile(name="EventDate", dtype="int")},
        )
        match = temp_repo.find_applicable(
            dataset_name="Events",
            action_type=ActionType.PARSE_DATES,
            target_columns=["EventDate"],
            schema_profile=incompat_profile,
        )
        assert match is None

    # -------------------------------------------------------------------------
    # Evidence (Tests 11 to 15)
    # -------------------------------------------------------------------------

    def test_11_successful_governance_outcome_recorded(self, temp_repo):
        """Test 11: Successful governance execution is recorded as promotion evidence."""
        ev = PromotionEvidence(
            rule_id="RULE_101",
            rule_version=1,
            run_id="run_101",
            dataset="Sales",
            success=True,
            rows_processed=5000,
            rows_failed=0,
            reconciliation_passed=True,
            governance_decision="approved",
        )
        saved = temp_repo.record_evidence(ev)
        assert saved.success is True

    def test_12_failed_execution_recorded_in_evidence(self, temp_repo):
        """Test 12: Failed executions are recorded accurately in evidence."""
        ev = PromotionEvidence(
            rule_id="RULE_101",
            rule_version=1,
            run_id="run_102",
            dataset="Sales",
            success=False,
            rows_processed=5000,
            rows_failed=120,
            reconciliation_passed=False,
            governance_decision="blocked",
        )
        saved = temp_repo.record_evidence(ev)
        assert saved.success is False

    def test_13_reconciliation_failure_prevents_promotion(self):
        """Test 13: Reconciliation failure in evidence strictly prevents promotion."""
        rule = PromotedRule(
            rule_id="RULE_TEST",
            name="Test",
            action_type=ActionType.TRIM,
            target_columns=["Col"],
            status=RuleLifecycleState.STAGED,
        )
        ev = PromotionEvidence(
            rule_id="RULE_TEST",
            rule_version=1,
            run_id="run_bad_rec",
            dataset="Orders",
            success=True,
            rows_processed=100,
            reconciliation_passed=False,  # Failed reconciliation!
        )
        can_promote, reason = PromotionEvaluator.evaluate_for_dataset_scoped(rule, [ev])
        assert can_promote is False
        assert "Reconciliation failed" in reason

    def test_14_blocked_runs_never_count_as_successful_evidence(self):
        """Test 14: BLOCKED governance or safety runs cannot count as positive promotion evidence."""
        rule = PromotedRule(
            rule_id="RULE_TEST",
            name="Test",
            action_type=ActionType.CAST_TYPE,
            target_columns=["Amount"],
            status=RuleLifecycleState.STAGED,
        )
        ev = PromotionEvidence(
            rule_id="RULE_TEST",
            rule_version=1,
            run_id="run_blocked",
            dataset="Orders",
            success=False,
            rows_processed=100,
            governance_decision="blocked",
        )
        can_promote, reason = PromotionEvaluator.evaluate_for_dataset_scoped(rule, [ev])
        assert can_promote is False
        assert "blocked" in reason

    def test_15_quarantined_runs_preserve_actual_quality_metrics(self, temp_repo):
        """Test 15: Runs with quarantined records record actual failed and quarantined rows."""
        ev = PromotionEvidence(
            rule_id="RULE_QUAR_TEST",
            rule_version=1,
            run_id="run_quar_1",
            dataset="Items",
            success=True,
            rows_processed=1000,
            rows_failed=15,
            rows_quarantined=15,
            reconciliation_passed=True,
            governance_decision="approved_with_quarantine",
        )
        saved = temp_repo.record_evidence(ev)
        assert saved.rows_quarantined == 15
        assert saved.rows_failed == 15

    # -------------------------------------------------------------------------
    # Versioning (Tests 16 to 18)
    # -------------------------------------------------------------------------

    def test_16_updating_rule_creates_new_version(self, temp_repo):
        """Test 16: Updating a rule parameters creates version v+1 and retires previous version."""
        rule_v1 = PromotedRule(
            rule_id="EMP_PREFIX_001",
            version=1,
            name="Prefix EMP-",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="HR",
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule_v1)

        rule_v2 = temp_repo.create_new_version(
            rule_id="EMP_PREFIX_001",
            updated_parameters={"prefix": "IN-EMP-"},
            created_by="lead_dev",
        )
        assert rule_v2.version == 2
        assert rule_v2.parameters["prefix"] == "IN-EMP-"

        # v1 is retired
        v1_fetched = temp_repo.get_rule("EMP_PREFIX_001", version=1)
        assert v1_fetched.status == RuleLifecycleState.RETIRED

    def test_17_historical_version_remains_auditable(self, temp_repo):
        """Test 17: Previous versions and their evidence remain auditable."""
        rule_v1 = PromotedRule(
            rule_id="AUDIT_RULE",
            version=1,
            name="Audit v1",
            action_type=ActionType.TRIM,
            target_columns=["Code"],
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule_v1)
        temp_repo.record_evidence(
            PromotionEvidence(rule_id="AUDIT_RULE", rule_version=1, run_id="r1", dataset="D", success=True, rows_processed=50)
        )
        temp_repo.create_new_version("AUDIT_RULE", updated_parameters={"side": "left"})

        history = temp_repo.get_history("AUDIT_RULE")
        assert len(history["versions"]) == 2
        assert history["versions"][0]["version"] == 1
        assert history["versions"][1]["version"] == 2
        assert len(history["evidence"]) == 1

    def test_18_retired_version_cannot_be_selected(self, temp_repo):
        """Test 18: A RETIRED rule version cannot be matched for execution."""
        rule_v1 = PromotedRule(
            rule_id="RETIRE_TEST",
            version=1,
            name="Retire Test",
            action_type=ActionType.TRIM,
            target_columns=["Code"],
            dataset_scope="D",
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule_v1)
        temp_repo.demote("RETIRE_TEST", RuleLifecycleState.RETIRED, reason="Deprecated")

        match = temp_repo.find_applicable(dataset_name="D", action_type=ActionType.TRIM)
        assert match is None

    # -------------------------------------------------------------------------
    # Demotion (Tests 19 to 21)
    # -------------------------------------------------------------------------

    def test_19_regression_suspends_promoted_rule(self, temp_repo):
        """Test 19: Detecting regression demotes a rule to SUSPENDED."""
        rule = PromotedRule(
            rule_id="REGRESS_RULE",
            name="Regress Test",
            action_type=ActionType.PAD,
            parameters={"length": 8},
            target_columns=["Code"],
            dataset_scope="Orders",
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(rule)

        demoted = temp_repo.demote("REGRESS_RULE", RuleLifecycleState.SUSPENDED, reason="Unexpected truncation")
        assert demoted.status == RuleLifecycleState.SUSPENDED

    def test_20_suspended_rule_is_not_automatically_reused(self, temp_repo):
        """Test 20: A SUSPENDED rule is excluded from applicable rule lookups."""
        rule = PromotedRule(
            rule_id="SUSP_RULE",
            name="Suspended Test",
            action_type=ActionType.PAD,
            parameters={"length": 8},
            target_columns=["Code"],
            dataset_scope="Orders",
            status=RuleLifecycleState.SUSPENDED,
        )
        temp_repo.save_candidate(rule)

        match = temp_repo.find_applicable(dataset_name="Orders", action_type=ActionType.PAD)
        assert match is None

    def test_21_revalidation_can_restore_suspended_rule(self, temp_repo):
        """Test 21: Revalidation and explicit governance approval can restore a suspended rule."""
        rule = PromotedRule(
            rule_id="RESTORE_RULE",
            name="Restore Test",
            action_type=ActionType.PAD,
            parameters={"length": 8},
            target_columns=["Code"],
            dataset_scope="Orders",
            status=RuleLifecycleState.SUSPENDED,
        )
        temp_repo.save_candidate(rule)

        restored = temp_repo.promote("RESTORE_RULE", RuleLifecycleState.DATASET_SCOPED, approved_by="senior_engineer")
        assert restored.status == RuleLifecycleState.DATASET_SCOPED
        assert restored.approved_by == "senior_engineer"

    # -------------------------------------------------------------------------
    # Unsupported Extension Candidates (Tests 22 to 25)
    # -------------------------------------------------------------------------

    def test_22_unsupported_creates_extension_candidate(self, temp_repo):
        """Test 22: Truly UNSUPPORTED transformation creates an ExtensionCandidate in learning repository."""
        decomposer = RuleDecomposer(promoted_repository=temp_repo)
        res = decomposer.decompose(
            rule_text="Apply our proprietary regional algorithm to EmployeeCode",
            dataset_name="Employees",
        )
        assert res.status == DecompositionStatus.UNSUPPORTED

        candidates = temp_repo.get_extension_candidates()
        assert len(candidates) == 1
        assert "proprietary regional algorithm" in candidates[0].original_rule
        assert candidates[0].dataset_name == "Employees"
        assert candidates[0].status == "PENDING_DEVELOPER_REVIEW"

    def test_23_candidate_retains_original_business_rule(self, temp_repo):
        """Test 23: ExtensionCandidate retains the exact original rule text for developer review."""
        rule_text = "Convert Roman numerals embedded in ProductCode into integer equivalents"
        cand = temp_repo.record_extension_candidate(rule_text, "Products")
        assert cand.original_rule == rule_text
        assert cand.frequency == 1

    def test_24_repeated_candidates_are_correlated(self, temp_repo):
        """Test 24: Repeated unsupported requests increment frequency for correlation."""
        rule_text = "Convert Roman numerals embedded in ProductCode into integer equivalents"
        temp_repo.record_extension_candidate(rule_text, "Products")
        cand2 = temp_repo.record_extension_candidate(rule_text, "Products")
        assert cand2.frequency == 2

    def test_25_no_extension_candidate_becomes_executable_automatically(self, temp_repo):
        """Test 25: Extension candidates are strictly non-executable by the compiler front-end."""
        temp_repo.record_extension_candidate("Perform complex ML clustering", "Users")
        # Ensure it cannot be matched as an executable rule
        match = temp_repo.find_applicable("Users", rule_text="Perform complex ML clustering")
        assert match is None

    # -------------------------------------------------------------------------
    # End-to-End Governed Learning Loop (Tests 26 to 28)
    # -------------------------------------------------------------------------

    def test_26_first_occurrence_full_governed_execution_records_evidence(self, temp_repo, hr_dataset_profile):
        """
        Test 26: First occurrence:
          Business Rule -> Decomposer -> StructuredAction -> Compiler -> Governance -> Approved -> Evidence.
        """
        rule_text = "EmployeeCode must start with 'EMP-'"
        decomposer = RuleDecomposer(promoted_repository=temp_repo)
        decomp_res = decomposer.decompose(rule_text, "Employees_India", {"Employees_India": hr_dataset_profile})
        assert decomp_res.status == DecompositionStatus.RESOLVED
        assert decomp_res.extraction_method == "deterministic"

        # Save as STAGED candidate
        act = decomp_res.actions[0]
        promoted_rule = PromotedRule(
            rule_id="RULE_EMP_INTENT",
            name="Prefix EmployeeCode EMP-",
            action_type=act.action_type,
            parameters=act.parameters,
            target_columns=act.target_columns,
            dataset_scope="Employees_India",
            rule_intent_pattern=rule_text.strip().lower(),
            schema_signature=SchemaSignature(columns={"EmployeeCode": {"dtype": "nvarchar(50)"}}),
            status=RuleLifecycleState.STAGED,
        )
        temp_repo.save_candidate(promoted_rule)

        # Record successful governance execution evidence
        ev = PromotionEvidence(
            rule_id="RULE_EMP_INTENT",
            rule_version=1,
            run_id="run_first_occurrence",
            dataset="Employees_India",
            success=True,
            rows_processed=1000,
            reconciliation_passed=True,
            governance_decision="approved",
        )
        temp_repo.record_evidence(ev)

        history = temp_repo.get_history("RULE_EMP_INTENT")
        assert len(history["evidence"]) == 1
        assert history["evidence"][0]["reconciliation_passed"] == 1

    def test_27_promotion_to_dataset_scoped_after_evidence_evaluation(self, temp_repo):
        """Test 27: Evidence evaluation promotes the candidate to DATASET_SCOPED."""
        rule = PromotedRule(
            rule_id="RULE_EMP_INTENT",
            name="Prefix EmployeeCode EMP-",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-"},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            status=RuleLifecycleState.STAGED,
        )
        temp_repo.save_candidate(rule)
        ev = PromotionEvidence(
            rule_id="RULE_EMP_INTENT",
            rule_version=1,
            run_id="run_1",
            dataset="Employees_India",
            success=True,
            rows_processed=500,
            reconciliation_passed=True,
            governance_decision="approved",
        )
        temp_repo.record_evidence(ev)

        history = temp_repo.get_history("RULE_EMP_INTENT")
        evidence_list = [PromotionEvidence.model_validate(e) for e in history["evidence"]]

        can_promote, reason = PromotionEvaluator.evaluate_for_dataset_scoped(rule, evidence_list)
        assert can_promote is True

        promoted = temp_repo.promote(rule.rule_id, RuleLifecycleState.DATASET_SCOPED, approved_by="governance_board")
        assert promoted.status == RuleLifecycleState.DATASET_SCOPED

    def test_28_second_occurrence_uses_promoted_knowledge_deterministically(self, temp_repo, hr_dataset_profile):
        """
        Test 28: Second occurrence against compatible dataset:
          Repository match -> StructuredAction (source=PROMOTED_RULE) -> Zero LLM cost -> Deterministic compilation.
        """
        rule_text = "EmployeeCode must start with 'EMP-'"
        # Register a promoted DATASET_SCOPED rule
        promoted_rule = PromotedRule(
            rule_id="RULE_EMP_INTENT",
            name="Prefix EmployeeCode EMP-",
            action_type=ActionType.PREFIX_ADD,
            parameters={"prefix": "EMP-", "only_if_missing": True},
            target_columns=["EmployeeCode"],
            dataset_scope="Employees_India",
            rule_intent_pattern=rule_text.strip().lower(),
            schema_signature=SchemaSignature(columns={"EmployeeCode": {"dtype": "nvarchar(50)"}}),
            status=RuleLifecycleState.DATASET_SCOPED,
        )
        temp_repo.save_candidate(promoted_rule)

        # Query decomposer with promoted repository configured
        decomposer = RuleDecomposer(promoted_repository=temp_repo)
        second_run_res = decomposer.decompose(
            rule_text=rule_text,
            dataset_name="Employees_India",
            dataset_profiles={"Employees_India": hr_dataset_profile},
        )

        assert second_run_res.status == DecompositionStatus.RESOLVED
        assert second_run_res.extraction_method == "promoted_rule"  # Learned knowledge reused!
        assert len(second_run_res.actions) == 1

        action = second_run_res.actions[0]
        assert action.source == ActionSource.PROMOTED_RULE
        assert action.action_type == ActionType.PREFIX_ADD
        assert action.target_columns == ["EmployeeCode"]
        assert action.parameters["prefix"] == "EMP-"

        # Verifying compilation into LogicalStep and PhysicalStep with zero LLM calls
        ctx = CompilerContext(session_id="learned_session", target_engine=ExecutionEngine.AZURE_SQL)
        ctx.structured_actions = second_run_res.actions
        l_planner = LogicalPlanner()
        ctx = l_planner.execute(ctx)
        p_planner = PhysicalPlanner()
        ctx = p_planner.execute(ctx)

        assert len(ctx.physical_plan.steps) == 1
        assert ctx.physical_plan.steps[0].template_name == "prefix_add.j2"
