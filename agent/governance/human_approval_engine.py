from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional
from agent.governance.models import (
    ApprovalDecision,
    ApprovalStatus,
    ApprovalTier,
    RiskLevel,
)
from agent.ir.models import IrPlan

logger = logging.getLogger("agent.governance.approval")


class HumanApprovalEngine:
    """
    Phase 9 Human Approval Workflow for Agent Dhara.
    Enforces governance boundaries and mandatory approvals before code execution:
    - Auto-Approved: SELECT, Projection, Filter, Rename, Deduplication, DQ Validation
    - Warning Required: Overwrites, Column Drops, Schema Evolution
    - Mandatory Human Approval: DROP TABLE, DELETE, TRUNCATE, MERGE ... DELETE, REPLACE TABLE, CASCADE
    """

    # Regex patterns for destructive DDL/DML
    _CRITICAL_PATTERNS = [
        (re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE), "DROP TABLE detected"),
        (re.compile(r"\bTRUNCATE\s+TABLE\b", re.IGNORECASE), "TRUNCATE TABLE detected"),
        (re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE), "DELETE FROM detected"),
        (re.compile(r"\bMERGE\b.*\bDELETE\b", re.IGNORECASE | re.DOTALL), "MERGE ... DELETE clause detected"),
        (re.compile(r"\bDROP\s+DATABASE\b", re.IGNORECASE), "DROP DATABASE detected"),
        (re.compile(r"\bREPLACE\s+TABLE\b", re.IGNORECASE), "REPLACE TABLE detected"),
        (re.compile(r"\bCASCADE\b", re.IGNORECASE), "CASCADE drop operation detected"),
    ]

    _WARNING_PATTERNS = [
        (re.compile(r"\bOVERWRITE\b", re.IGNORECASE), "Table or partition overwrite detected"),
        (re.compile(r"\bDROP\s+COLUMN\b", re.IGNORECASE), "ALTER TABLE DROP COLUMN detected"),
        (re.compile(r"\bmode\s*=\s*['\"]overwrite['\"]", re.IGNORECASE), "Spark DataFrame overwrite mode detected"),
    ]

    def __init__(self) -> None:
        self._decisions: Dict[str, ApprovalDecision] = {}
        self._session_decisions: Dict[str, str] = {}
        self._lock = threading.RLock()

    def evaluate(
        self,
        session_id: str,
        code: str = "",
        ir_plan: Optional[IrPlan] = None,
        pattern_id: Optional[str] = None,
        requestor: str = "engineer",
    ) -> ApprovalDecision:
        with self._lock:
            approval_id = f"appr_{int(time.time() * 1000)}"
            critical_hits: List[str] = []
            warning_hits: List[str] = []

            # 1. Analyze Executable Code Snippet
            if code:
                for regex, desc in self._CRITICAL_PATTERNS:
                    if regex.search(code):
                        critical_hits.append(desc)

                for regex, desc in self._WARNING_PATTERNS:
                    if regex.search(code):
                        warning_hits.append(desc)

            # 2. Analyze Canonical IR Intent if provided
            if ir_plan:
                intent_type = ir_plan.intent.type.lower()
                if intent_type in ("drop", "delete", "truncate"):
                    critical_hits.append(f"IR intent requests destructive action: '{intent_type}'")
                elif intent_type in ("overwrite", "schema_evolution"):
                    warning_hits.append(f"IR intent requests table overwrite or schema change: '{intent_type}'")

            # 3. Categorize Approval Tier & Risk Level
            if critical_hits:
                tier = ApprovalTier.MANDATORY_HUMAN_APPROVAL
                risk = RiskLevel.CRITICAL
                status = ApprovalStatus.PENDING
                reasons = critical_hits
            elif warning_hits:
                tier = ApprovalTier.WARNING_REQUIRED
                risk = RiskLevel.MEDIUM
                status = ApprovalStatus.PENDING
                reasons = warning_hits
            else:
                tier = ApprovalTier.AUTO_APPROVED
                risk = RiskLevel.LOW
                status = ApprovalStatus.AUTO_APPROVED
                reasons = ["Pure read/cleaning transformation; zero destructive DDL or DML detected."]

            snippet = code.strip()[:150] if code else "Canonical IR execution"

            decision = ApprovalDecision(
                approval_id=approval_id,
                session_id=session_id,
                pattern_id=pattern_id,
                requestor=requestor,
                approval_tier=tier,
                risk_level=risk,
                status=status,
                reasons=reasons,
                destructive_actions=critical_hits + warning_hits,
                code_summary=snippet,
                audit_log=[
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Evaluated tier: {tier.value}, risk: {risk.value}, status: {status.value}"
                ],
            )

            self._decisions[approval_id] = decision
            self._session_decisions[session_id] = approval_id

            logger.info(
                f"[HumanApprovalEngine] Session '{session_id}' -> Tier: {tier.value}, Status: {status.value}"
            )
            return decision

    def approve(
        self,
        approval_id: str,
        approver: str,
        reason: str = "Authorized by lead data engineer",
    ) -> ApprovalDecision:
        with self._lock:
            decision = self._decisions.get(approval_id)
            if not decision:
                raise KeyError(f"Approval ID not found: {approval_id}")

            decision.status = ApprovalStatus.APPROVED
            decision.approver = approver
            decision.decided_at = time.time()
            decision.audit_log.append(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Approved by '{approver}': {reason}"
            )
            logger.info(f"[HumanApprovalEngine] Approval '{approval_id}' granted by '{approver}'")
            return decision

    def reject(
        self,
        approval_id: str,
        approver: str,
        reason: str = "Operation rejected due to data loss risk",
    ) -> ApprovalDecision:
        with self._lock:
            decision = self._decisions.get(approval_id)
            if not decision:
                raise KeyError(f"Approval ID not found: {approval_id}")

            decision.status = ApprovalStatus.REJECTED
            decision.approver = approver
            decision.decided_at = time.time()
            decision.audit_log.append(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Rejected by '{approver}': {reason}"
            )
            logger.info(f"[HumanApprovalEngine] Approval '{approval_id}' rejected by '{approver}'")
            return decision

    def is_execution_permitted(self, approval_id_or_session: str) -> bool:
        with self._lock:
            app_id = (
                approval_id_or_session
                if approval_id_or_session in self._decisions
                else self._session_decisions.get(approval_id_or_session)
            )
            if not app_id or app_id not in self._decisions:
                return False

            decision = self._decisions[app_id]
            return decision.status in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED)

    def get_decision(self, approval_id_or_session: str) -> Optional[ApprovalDecision]:
        with self._lock:
            app_id = (
                approval_id_or_session
                if approval_id_or_session in self._decisions
                else self._session_decisions.get(approval_id_or_session)
            )
            return self._decisions.get(app_id) if app_id else None

    def list_pending(self) -> List[ApprovalDecision]:
        with self._lock:
            return [d for d in self._decisions.values() if d.status == ApprovalStatus.PENDING]


# Global Singleton
_GLOBAL_APPROVAL_ENGINE = HumanApprovalEngine()


def get_approval_engine() -> HumanApprovalEngine:
    return _GLOBAL_APPROVAL_ENGINE
