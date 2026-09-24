"""Repair memory tracking previous dirty-to-clean value pairs."""

from typing import Any, Dict, List, Optional
from agent.compiler.learning.repository import LearningRepository


class RepairMemory:
    """Remembers how dirty values were repaired in the past to accelerate auto-repair."""

    def __init__(self, repo: Optional[LearningRepository] = None):
        self.repo = repo or LearningRepository()

    def record_repair(
        self,
        issue_type: str,
        original_value: str,
        fixed_value: str,
        success: bool = True,
    ) -> int:
        return self.repo.record_repair(
            issue_type=issue_type,
            original_value=original_value,
            fixed_value=fixed_value,
            success=success,
        )

    def get_recent_repairs(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.repo.list_repairs(limit=limit)
