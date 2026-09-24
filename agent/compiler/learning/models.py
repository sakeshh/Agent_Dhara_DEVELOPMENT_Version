"""Data models for Agent Dhara Self-Learning Data Cleaning Platform."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CleaningPatternRecord(BaseModel):
    """Database entity representing a learned cleaning pattern."""
    id: Optional[int] = None
    pattern_name: str
    domain: str
    confidence: float = 95.0
    usage_count: int = 1
    success_rate: float = 98.0
    created_at: Optional[str] = None


class IssueHistoryRecord(BaseModel):
    """Database entity tracking issue resolution history."""
    id: Optional[int] = None
    dataset_name: str
    issue_type: str
    column_name: str
    strategy_used: str
    success: bool = True
    created_at: Optional[str] = None


class RepairHistoryRecord(BaseModel):
    """Database entity storing historical value transformations."""
    id: Optional[int] = None
    issue_type: str
    original_value: str
    fixed_value: str
    success: bool = True


class FeedbackRecord(BaseModel):
    """Database entity recording user feedback and manual overrides."""
    id: Optional[int] = None
    issue_type: str
    generated_strategy: str
    user_action: str  # approve, reject, modify
    modified_strategy: Optional[str] = None


# --- API Models ---

class KnowledgeSearchRequest(BaseModel):
    issue: str = Field(..., description="Issue type to look up (e.g. invalid_email, null_impute)")
    domain: Optional[str] = Field(None, description="Optional business domain filter")


class KnowledgeSearchResponse(BaseModel):
    issue: str
    recommended_strategy: str
    success_rate: float
    confidence: float
    usage_count: int
    historical_examples: List[Dict[str, Any]] = Field(default_factory=list)


class PatternMatchRequest(BaseModel):
    columns: List[str] = Field(..., description="List of dataset column names")
    dataset_name: Optional[str] = None


class PatternMatchResponse(BaseModel):
    pattern: str
    domain: str
    confidence: float
    matched_columns: List[str]
    suggested_cleaning_actions: List[str]


class RecommendationItem(BaseModel):
    strategy: str
    issue_type: str
    column_name: Optional[str] = None
    confidence: float
    reason: str
    success_rate: float


class RecommendationsRequest(BaseModel):
    dataset_name: Optional[str] = "dataset"
    columns: List[str] = Field(default_factory=list)
    detected_issues: List[Dict[str, Any]] = Field(default_factory=list)
    domain: Optional[str] = None


class RecommendationsResponse(BaseModel):
    dataset_name: str
    recommendations: List[RecommendationItem]


class ImpactPreviewRequest(BaseModel):
    session_id: Optional[str] = None
    dataset_name: Optional[str] = "dataset"
    total_rows: int = 100000
    planned_strategies: List[str] = Field(default_factory=list)


class ImpactPreviewResponse(BaseModel):
    rows_scanned: int
    rows_modified: int
    rows_removed: int
    duplicates_removed: int
    emails_fixed: int
    nulls_fixed: int
    pii_fields_masked: int
    risk: str  # LOW, MEDIUM, HIGH


class ApprovalDecision(BaseModel):
    strategy: str
    column: Optional[str] = None
    approved: bool
    reason: Optional[str] = None


class ApproveCleaningRequest(BaseModel):
    session_id: str
    decisions: List[ApprovalDecision] = Field(default_factory=list)
    approve_all: bool = False


class RejectCleaningRequest(BaseModel):
    session_id: str
    reason: Optional[str] = "User rejected proposed cleaning plan"


class RuntimeResultRequest(BaseModel):
    dataset_name: Optional[str] = "dataset"
    strategy: str
    issue_type: str
    column_name: Optional[str] = "unknown"
    rows_fixed: int = 0
    success: bool = True
    original_sample: Optional[str] = None
    fixed_sample: Optional[str] = None
