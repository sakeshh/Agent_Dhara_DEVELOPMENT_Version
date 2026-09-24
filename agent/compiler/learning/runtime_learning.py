"""Runtime Learning: Ingests post-execution metrics and updates strategy knowledge."""

from typing import Any, Dict, Optional
from agent.compiler.learning.models import RuntimeResultRequest
from agent.compiler.learning.repository import LearningRepository


class RuntimeLearningAgent:
    """Closes the feedback loop by recording execution telemetry into the learning knowledge base."""

    def __init__(self, repo: Optional[LearningRepository] = None):
        self.repo = repo or LearningRepository()

    def record_runtime_result(self, request: RuntimeResultRequest) -> Dict[str, Any]:
        # 1. Record issue resolution in history
        issue_id = self.repo.record_issue(
            dataset_name=request.dataset_name or "dataset",
            issue_type=request.issue_type,
            column_name=request.column_name or "*",
            strategy_used=request.strategy,
            success=request.success,
        )

        # 2. Record repair memory if sample pairs provided
        repair_id = None
        if request.original_sample and request.fixed_sample:
            repair_id = self.repo.record_repair(
                issue_type=request.issue_type,
                original_value=request.original_sample,
                fixed_value=request.fixed_sample,
                success=request.success,
            )

        # 3. Update cleaning pattern stats
        domain = "General"
        if "email" in request.issue_type.lower() or "customer" in (request.dataset_name or "").lower():
            domain = "Customer"
        elif "invoice" in (request.dataset_name or "").lower():
            domain = "Invoice"
        elif "employee" in (request.dataset_name or "").lower():
            domain = "Employee"

        self.repo.upsert_pattern(
            pattern_name=f"{domain} Master",
            domain=domain,
            confidence=95.0,
            success=request.success,
        )

        return {
            "status": "LEARNING_UPDATED",
            "issue_history_id": issue_id,
            "repair_history_id": repair_id,
            "rows_fixed": request.rows_fixed,
            "success": request.success,
            "message": f"Successfully integrated telemetry for strategy '{request.strategy}' into Knowledge Base.",
        }
