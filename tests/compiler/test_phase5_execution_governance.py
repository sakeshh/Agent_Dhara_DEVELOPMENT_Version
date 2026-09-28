from __future__ import annotations

import pytest

from agent.compiler.governance.audit import GovernanceAuditLog, NotificationSink
from agent.compiler.governance.authorizer import RuntimeAuthorizer
from agent.compiler.governance.models import (
    DecisionOutcome,
    ExecutionDecision,
    GovernanceEvent,
    NotificationType,
)
from agent.compiler.governance.reconciliation import ReconciliationLedger, ReconciliationResult
from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.execution import ExecutionMode, FailureBehavior, TransformationPolicy
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition
from agent.compiler.safety.models import SafetyStatus, StepSafetyMetrics
from agent.errors import ReconciliationError


class CustomMockNotificationSink(NotificationSink):
    def __init__(self):
        self.received_events: list[GovernanceEvent] = []

    def emit(self, event: GovernanceEvent) -> None:
        self.received_events.append(event)


class TestPhase5ExecutionGovernance:
    """
    Phase 5 Test Suite: Execution Governance, Reconciliation & Auditability.
    Verifies all 22 acceptance criteria:
      - Governance (1 to 8)
      - Circuit Breaker Integration (9 to 12)
      - Reconciliation Invariant (13 to 16)
      - Audit & Provenance Logging (17 to 22)
    """

    # -------------------------------------------------------------------------
    # Governance Decisions (Tests 1 to 8)
    # -------------------------------------------------------------------------

    def test_1_interactive_low_risk_automatic_path(self):
        """Test 1: Interactive mode auto-approves known low-risk transformations without pausing."""
        step = LogicalStep(
            step_id="step_trim_1",
            dataset_name="Employees",
            action=ActionType.TRIM,
            target_columns=["EmployeeCode"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.TRIM,
            row_disposition=RowDisposition.TRANSFORM,
            risk_level=RiskLevel.LOW,
            requires_approval=False,
            execution_mode=ExecutionMode.INTERACTIVE,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.INTERACTIVE,
            total_input_rows=1000,
        )
        assert dec.decision == DecisionOutcome.APPROVED
        assert dec.requires_user_approval is False
        assert dec.decision_source == "policy"
        assert dec.clean_rows == 1000

    def test_2_interactive_approval_required_path(self):
        """Test 2: Medium/high-risk transforms pause and return APPROVAL_REQUIRED in interactive mode."""
        step = LogicalStep(
            step_id="step_drop_1",
            dataset_name="Employees",
            action=ActionType.DROP_COLUMN,
            target_columns=["LegacySSN"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.DROP_COLUMN,
            row_disposition=RowDisposition.REMOVE,
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            execution_mode=ExecutionMode.INTERACTIVE,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.INTERACTIVE,
            user_approved=None,  # Not yet approved
            total_input_rows=1000,
        )
        assert dec.decision == DecisionOutcome.APPROVAL_REQUIRED
        assert dec.requires_user_approval is True
        assert dec.risk_level == RiskLevel.HIGH

    def test_3_user_approval_allows_execution(self):
        """Test 3: Providing user_approved=True yields an APPROVED execution decision with user identity."""
        step = LogicalStep(
            step_id="step_clip_1",
            dataset_name="Salaries",
            action=ActionType.RANGE_CLIP,
            target_columns=["BasePay"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.RANGE_CLIP,
            risk_level=RiskLevel.MEDIUM,
            requires_approval=True,
            execution_mode=ExecutionMode.INTERACTIVE,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.INTERACTIVE,
            user_approved=True,
            user_identity="compliance_officer_sally",
            total_input_rows=500,
        )
        assert dec.decision == DecisionOutcome.APPROVED
        assert dec.decision_source == "interactive_user"
        assert dec.approval_metadata.get("approver") == "compliance_officer_sally"

    def test_4_user_rejection_prevents_execution(self):
        """Test 4: Providing user_approved=False yields BLOCKED and records the rejecting identity."""
        step = LogicalStep(
            step_id="step_replace_1",
            dataset_name="Accounts",
            action=ActionType.PATTERN_REPLACE,
            target_columns=["IBAN"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.PATTERN_REPLACE,
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            execution_mode=ExecutionMode.INTERACTIVE,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.INTERACTIVE,
            user_approved=False,
            user_identity="auditor_bob",
            total_input_rows=200,
        )
        assert dec.decision == DecisionOutcome.BLOCKED
        assert dec.decision_source == "interactive_user"
        assert dec.approval_metadata.get("rejector") == "auditor_bob"
        assert "Rejected by user" in dec.reason

    def test_5_batch_low_risk_automatic_execution(self):
        """Test 5: Unattended batch mode automatically executes low-risk transforms when safety checks pass."""
        step = LogicalStep(
            step_id="step_prefix_1",
            dataset_name="Products",
            action=ActionType.PREFIX_ADD,
            target_columns=["SKU"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.PREFIX_ADD,
            risk_level=RiskLevel.LOW,
            execution_mode=ExecutionMode.BATCH,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=2500,
        )
        assert dec.decision == DecisionOutcome.APPROVED
        assert dec.requires_user_approval is False
        assert dec.clean_rows == 2500

    def test_6_batch_critical_risk_block(self):
        """Test 6: High-risk actions in unattended batch mode are blocked from running autonomously."""
        step = LogicalStep(
            step_id="step_dedup_high_risk",
            dataset_name="Orders",
            action=ActionType.DEDUPLICATE,
            target_columns=["CustomerKey"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.DEDUPLICATE,
            risk_level=RiskLevel.HIGH,
            execution_mode=ExecutionMode.BATCH,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=10000,
        )
        assert dec.decision == DecisionOutcome.BLOCKED
        assert "High-risk transformation blocked in unattended batch mode" in dec.reason

    def test_7_batch_identifiable_bad_rows_quarantine(self):
        """Test 7: Identifiable bad rows with QUARANTINE_ROW policy yield APPROVED_WITH_QUARANTINE."""
        step = LogicalStep(
            step_id="step_cast_1",
            dataset_name="Employees",
            action=ActionType.CAST_TYPE,
            target_columns=["Age"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            risk_level=RiskLevel.LOW,
            execution_mode=ExecutionMode.BATCH,
            on_failure=FailureBehavior.QUARANTINE_ROW,
        )
        metrics = StepSafetyMetrics(
            step_id="step_cast_1",
            action_type=ActionType.CAST_TYPE,
            input_rows=1000,
            passed_rows=980,
            failed_rows=20,
            quarantined_rows=20,
            status=SafetyStatus.QUARANTINED,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            safety_metrics=metrics,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=1000,
        )
        assert dec.decision == DecisionOutcome.APPROVED_WITH_QUARANTINE
        assert dec.quarantined_rows == 20
        assert dec.clean_rows == 980

    def test_8_ambiguous_violation_cannot_pretend_to_quarantine(self):
        """Test 8: Ambiguous violation with unidentifiable rows cannot pretend to quarantine; must block."""
        step = LogicalStep(
            step_id="step_ambiguous",
            dataset_name="Customers",
            action=ActionType.CUSTOM_EXPRESSION,
            target_columns=["Region"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.CUSTOM_EXPRESSION,
            on_failure=FailureBehavior.QUARANTINE_ROW,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            ambiguity_detected=True,  # Ambiguous!
            total_input_rows=500,
        )
        assert dec.decision == DecisionOutcome.BLOCKED
        assert dec.quarantined_rows == 0
        assert "cannot be quarantined" in dec.reason

    # -------------------------------------------------------------------------
    # Circuit Breaker Integration (Tests 9 to 12)
    # -------------------------------------------------------------------------

    def test_9_phase4_block_cannot_be_overridden_by_governance(self):
        """Test 9: Phase 4 Circuit Breaker BLOCK cannot be overridden by Phase 5 governance."""
        step = LogicalStep(
            step_id="step_cb_blocked",
            dataset_name="Transactions",
            action=ActionType.CAST_TYPE,
            target_columns=["Amount"],
        )
        metrics = StepSafetyMetrics(
            step_id="step_cb_blocked",
            action_type=ActionType.CAST_TYPE,
            input_rows=100,
            failed_rows=80,
            status=SafetyStatus.BLOCKED,  # Blocked by circuit breaker
        )
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            on_failure=FailureBehavior.CONTINUE,  # Even if policy says continue!
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            safety_metrics=metrics,
            user_approved=True,  # Even if user approved!
        )
        assert dec.decision == DecisionOutcome.BLOCKED
        assert dec.decision_source == "circuit_breaker"

    def test_10_warn_policy_controlled_continuation(self):
        """Test 10: WARN policy allows execution to proceed with diagnostics logged."""
        step = LogicalStep(
            step_id="step_warn_1",
            dataset_name="Notes",
            action=ActionType.CAST_TYPE,
            target_columns=["CommentID"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            execution_mode=ExecutionMode.BATCH,
            on_failure=FailureBehavior.WARN,
        )
        metrics = StepSafetyMetrics(
            step_id="step_warn_1",
            action_type=ActionType.CAST_TYPE,
            input_rows=50,
            failed_rows=2,
            status=SafetyStatus.WARNING,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            safety_metrics=metrics,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=50,
        )
        assert dec.decision == DecisionOutcome.APPROVED
        assert "Approved with warning" in dec.reason

    def test_11_skip_rule_allows_remaining_transformations_to_continue(self):
        """Test 11: SKIP_RULE decision skips the offending transformation without blocking the pipeline."""
        step = LogicalStep(
            step_id="step_skip_1",
            dataset_name="Catalog",
            action=ActionType.PATTERN_REPLACE,
            target_columns=["SKU"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.PATTERN_REPLACE,
            execution_mode=ExecutionMode.BATCH,
            on_failure=FailureBehavior.SKIP_RULE,
        )
        metrics = StepSafetyMetrics(
            step_id="step_skip_1",
            action_type=ActionType.PATTERN_REPLACE,
            input_rows=100,
            failed_rows=10,
            status=SafetyStatus.SKIPPED,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            safety_metrics=metrics,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=100,
        )
        assert dec.decision == DecisionOutcome.SKIP_RULE

    def test_12_quarantine_row_leaves_clean_rows_eligible(self):
        """Test 12: QUARANTINE_ROW leaves the clean population eligible for subsequent transforms."""
        step = LogicalStep(
            step_id="step_quarantine_flow",
            dataset_name="Orders",
            action=ActionType.COERCE_NUMERIC,
            target_columns=["Total"],
        )
        policy = TransformationPolicy(
            action_type=ActionType.COERCE_NUMERIC,
            execution_mode=ExecutionMode.BATCH,
            on_failure=FailureBehavior.QUARANTINE_ROW,
        )
        metrics = StepSafetyMetrics(
            step_id="step_quarantine_flow",
            action_type=ActionType.COERCE_NUMERIC,
            input_rows=200,
            failed_rows=5,
            status=SafetyStatus.QUARANTINED,
        )
        dec = RuntimeAuthorizer.authorize(
            step=step,
            policy=policy,
            safety_metrics=metrics,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=200,
        )
        assert dec.decision == DecisionOutcome.APPROVED_WITH_QUARANTINE
        assert dec.clean_rows == 195
        assert dec.quarantined_rows == 5

    # -------------------------------------------------------------------------
    # Reconciliation Invariant (Tests 13 to 16)
    # -------------------------------------------------------------------------

    def test_13_reconciliation_exact_mathematical_balance(self):
        """Test 13: Mathematical balance: N_input == N_clean + N_quarantined + N_dropped + N_merged."""
        res = ReconciliationLedger.reconcile(
            dataset_name="Customers",
            input_rows=1000,
            clean_rows=950,
            quarantined_rows=30,
            dropped_rows=20,
            merged_rows=0,
            rejected_rows=0,
        )
        assert res.is_reconciled is True
        assert res.unaccounted_rows == 0
        assert "1000 (input) == 950 (clean) + 30 (quarantined) + 20 (dropped)" in res.equation

    def test_14_reconciliation_deduplication_accounts_for_merged_rows(self):
        """Test 14: Deduplication turning 100 rows into 90 rows explicitly accounts for 10 merged rows."""
        res = ReconciliationLedger.reconcile(
            dataset_name="Orders",
            input_rows=100,
            clean_rows=90,
            quarantined_rows=0,
            dropped_rows=0,
            merged_rows=10,  # 10 duplicate rows merged
            rejected_rows=0,
        )
        assert res.is_reconciled is True
        assert res.unaccounted_rows == 0
        assert res.merged_rows == 10

    def test_15_reconciliation_mismatch_blocks_commit(self):
        """Test 15: Row discrepancy (unaccounted data loss) raises ReconciliationError and blocks commit."""
        with pytest.raises(ReconciliationError) as exc_info:
            ReconciliationLedger.reconcile(
                dataset_name="Payments",
                input_rows=1000,
                clean_rows=950,
                quarantined_rows=20,  # 30 rows unaccounted for!
                dropped_rows=0,
                merged_rows=0,
                rejected_rows=0,
                raise_on_mismatch=True,
            )
        assert "30 rows unaccounted for (lost)" in str(exc_info.value)
        assert exc_info.value.delta == 30

    def test_16_zero_rows_lost_without_disposition(self):
        """Test 16: Ledger verifies that zero rows are lost without an explicit disposition."""
        res = ReconciliationLedger.reconcile(
            dataset_name="Inventory",
            input_rows=500,
            clean_rows=480,
            quarantined_rows=15,
            dropped_rows=5,
            raise_on_mismatch=False,
        )
        assert res.is_reconciled is True
        assert res.unaccounted_rows == 0

    # -------------------------------------------------------------------------
    # Audit & Provenance Logging (Tests 17 to 22)
    # -------------------------------------------------------------------------

    def test_17_execution_decision_recorded_in_audit_log(self):
        """Test 17: Execution decisions are recorded in the central GovernanceAuditLog."""
        audit_log = GovernanceAuditLog()
        dec = ExecutionDecision(
            decision=DecisionOutcome.APPROVED,
            reason="Test Approval",
            risk_level=RiskLevel.LOW,
            requires_user_approval=False,
            affected_rows=100,
            clean_rows=100,
            step_id="step_1",
            dataset_name="Users",
        )
        event = audit_log.record_decision(dec, run_id="run_101")
        assert len(audit_log.get_decisions()) == 1
        assert len(audit_log.get_events()) == 1
        assert event.event_type == NotificationType.TRANSFORMATION_APPROVED
        assert event.dataset == "Users"

    def test_18_policy_reason_recorded(self):
        """Test 18: Exact policy reason is recorded into the audit payload."""
        audit_log = GovernanceAuditLog()
        dec = ExecutionDecision(
            decision=DecisionOutcome.APPROVED_WITH_QUARANTINE,
            reason="Identifiable bad rows diverted to quarantine",
            risk_level=RiskLevel.MEDIUM,
            requires_user_approval=False,
            affected_rows=500,
            clean_rows=490,
            quarantined_rows=10,
        )
        event = audit_log.record_decision(dec, run_id="run_102")
        assert event.payload["reason"] == "Identifiable bad rows diverted to quarantine"

    def test_19_approval_identity_and_timestamp_recorded(self):
        """Test 19: Approver identity and timestamp survive into the audit payload."""
        audit_log = GovernanceAuditLog()
        dec = ExecutionDecision(
            decision=DecisionOutcome.APPROVED,
            reason="Approved by user 'lead_architect'",
            risk_level=RiskLevel.HIGH,
            requires_user_approval=True,
            decision_source="interactive_user",
            approval_metadata={"approver": "lead_architect", "approved_at": "2026-09-28T12:00:00Z"},
        )
        event = audit_log.record_decision(dec, run_id="run_103")
        assert event.payload["approval_metadata"]["approver"] == "lead_architect"
        assert event.payload["decision_source"] == "interactive_user"

    def test_20_quarantine_count_recorded(self):
        """Test 20: Quarantined count is explicitly tracked in the audit event."""
        audit_log = GovernanceAuditLog()
        dec = ExecutionDecision(
            decision=DecisionOutcome.APPROVED_WITH_QUARANTINE,
            reason="Quarantine bad rows",
            risk_level=RiskLevel.LOW,
            requires_user_approval=False,
            affected_rows=100,
            clean_rows=85,
            quarantined_rows=15,
        )
        event = audit_log.record_decision(dec, run_id="run_104")
        assert event.quarantined_rows == 15
        assert event.clean_rows == 85
        assert event.event_type == NotificationType.BATCH_COMPLETED_WITH_QUARANTINE

    def test_21_safety_metrics_linked_to_decision(self):
        """Test 21: Phase 4 safety metrics link directly to the Phase 5 decision and audit record."""
        audit_log = GovernanceAuditLog()
        metrics_dict = {"failed_rows": 5, "unexpected_nulls": 5, "passed_rows": 95}
        dec = ExecutionDecision(
            decision=DecisionOutcome.APPROVED_WITH_QUARANTINE,
            reason="Clean execution with quarantine",
            risk_level=RiskLevel.LOW,
            requires_user_approval=False,
            safety_metrics=metrics_dict,
        )
        event = audit_log.record_decision(dec, run_id="run_105")
        assert event.payload["safety_metrics"]["failed_rows"] == 5

    def test_22_provenance_survives_into_audit_record(self):
        """Test 22: Transformation provenance (source, rule reference, extraction method) survives into audit."""
        mock_sink = CustomMockNotificationSink()
        audit_log = GovernanceAuditLog(sinks=[mock_sink])

        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["Code"],
            parameters={"prefix": "EMP-"},
            source=ActionSource.BUSINESS_RULE,
            source_reference="RULE-BR-042",
            extraction_method="deterministic",
        )
        dec = RuntimeAuthorizer.authorize(
            step=action,
            execution_mode=ExecutionMode.BATCH,
            total_input_rows=100,
        )

        event = audit_log.record_decision(dec, run_id="run_106")

        assert len(mock_sink.received_events) == 1
        assert event.payload["provenance"]["source"] == "business_rule"
        assert event.payload["provenance"]["source_reference"] == "RULE-BR-042"
        assert event.payload["provenance"]["extraction_method"] == "deterministic"
