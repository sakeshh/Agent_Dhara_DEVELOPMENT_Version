from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class SessionSummary(BaseModel):
    session_id: str
    workflow_status: str
    active_dataset_ids: List[str] = Field(default_factory=list)
    target_engine: Optional[str] = None
    cleaning_mode: Optional[str] = None
    plan_id: Optional[str] = None
    plan_status: Optional[str] = None
    plan_hash: Optional[str] = None
    latest_artifact_id: Optional[str] = None
    latest_artifact_version: Optional[int] = None
    generation_path: Optional[str] = None
    updated_at: Optional[str] = None
