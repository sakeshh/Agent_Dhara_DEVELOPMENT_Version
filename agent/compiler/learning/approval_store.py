"""Approval Store: Records and manages user review decisions on proposed cleaning plans."""

import threading
from typing import Any, Dict, List, Optional
from agent.compiler.learning.models import (
    ApproveCleaningRequest,
    RejectCleaningRequest,
    ApprovalDecision,
)
from agent.compiler.learning.feedback_store import FeedbackStore

_APPROVAL_LOCK = threading.Lock()
_SESSION_DECISIONS: Dict[str, Dict[str, Any]] = {}


class ApprovalStore:
    """Manages cleaning plan approval state across sessions."""

    def __init__(self, feedback_store: Optional[FeedbackStore] = None):
        self.feedback_store = feedback_store or FeedbackStore()

    def record_approval(self, request: ApproveCleaningRequest) -> Dict[str, Any]:
        with _APPROVAL_LOCK:
            decisions_list = [d.dict() for d in request.decisions]
            _SESSION_DECISIONS[request.session_id] = {
                "status": "APPROVED",
                "approve_all": request.approve_all,
                "decisions": decisions_list,
            }

            # Feed into feedback store for learning
            for d in request.decisions:
                self.feedback_store.record_user_feedback(
                    issue_type=d.column or "general_issue",
                    generated_strategy=d.strategy,
                    user_action="approve" if d.approved else "reject",
                    modified_strategy=None,
                )

            return {
                "session_id": request.session_id,
                "status": "APPROVED",
                "approved_count": len([d for d in request.decisions if d.approved]) if not request.approve_all else "ALL",
            }

    def record_rejection(self, request: RejectCleaningRequest) -> Dict[str, Any]:
        with _APPROVAL_LOCK:
            _SESSION_DECISIONS[request.session_id] = {
                "status": "REJECTED",
                "reason": request.reason,
            }
            return {
                "session_id": request.session_id,
                "status": "REJECTED",
                "reason": request.reason,
            }

    def get_approval_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        with _APPROVAL_LOCK:
            return _SESSION_DECISIONS.get(session_id)
