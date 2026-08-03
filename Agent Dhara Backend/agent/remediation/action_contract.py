from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

@dataclass
class RemediationAction:
    action_id: str = field(default_factory=lambda: f"ACT-{uuid.uuid4().hex[:12].upper()}")
    mode: str = "RECOMMEND_ONLY"  # DRY_RUN | QUARANTINE | APPLY_APPROVED_FIXES | ROLLBACK
    dataset: str = ""
    column: str = ""
    issue_type: str = ""
    
    rows_affected: int = 0
    before_invalid_count: int = 0
    after_invalid_count: int = 0
    
    idempotent: bool = True
    raw_preserved: bool = True
    
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: Dict[str, Any] = field(default_factory=dict)
