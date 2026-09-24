"""Feedback store for recording user approvals, rejections, and custom modifications."""

from typing import Any, Dict, List, Optional
from agent.compiler.learning.repository import LearningRepository
from agent.compiler.learning.models import FeedbackRecord


class FeedbackStore:
    """Stores user reviews and feeds back into strategy prioritization."""

    def __init__(self, repo: Optional[LearningRepository] = None):
        self.repo = repo or LearningRepository()

    def record_user_feedback(
        self,
        issue_type: str,
        generated_strategy: str,
        user_action: str,
        modified_strategy: Optional[str] = None,
    ) -> int:
        return self.repo.record_feedback(
            issue_type=issue_type,
            generated_strategy=generated_strategy,
            user_action=user_action,
            modified_strategy=modified_strategy,
        )

    def get_recent_feedback(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.repo.list_feedback(limit=limit)
