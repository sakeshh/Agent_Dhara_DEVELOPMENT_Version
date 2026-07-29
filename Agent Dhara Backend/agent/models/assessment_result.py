from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

class DatasetColumnProfile(BaseModel):
    model_config = ConfigDict(extra="allow")

    dtype: str
    dtype_inference: Optional[str] = None
    null_percentage: float = 0.0
    unique_count: int = 0
    semantic_type: Optional[str] = None
    candidate_primary_key: bool = False
    likely_key_columns: Optional[List[str]] = None

class DatasetProfile(BaseModel):
    model_config = ConfigDict(extra="allow")

    row_count: int = 0
    column_count: int = 0
    data_volume_bytes: int = 0
    source_root: Optional[str] = None
    columns: Dict[str, DatasetColumnProfile] = Field(default_factory=dict)
    likely_key_columns: Optional[List[str]] = None

class DataQualitySummary(BaseModel):
    model_config = ConfigDict(extra="allow")

    high_severity: int = 0
    medium_severity: int = 0
    low_severity: int = 0
    dq_score_0_100: Optional[float] = None
    estimated_clean_rows_after_high: Optional[int] = None
    estimated_clean_rows_after_high_and_medium: Optional[int] = None

class DQIssue(BaseModel):
    model_config = ConfigDict(extra="allow")

    column: Optional[str] = None
    type: str
    severity: str
    count: Optional[int] = None
    row_indexes: Optional[List[int]] = None
    sample_values: Optional[List[Any]] = None
    message: str
    recommended_action: Optional[str] = None
    auto_fixable: bool = False
    fixability: Optional[str] = None
    manual_guidance: Optional[str] = None

class DatasetDQBlock(BaseModel):
    model_config = ConfigDict(extra="allow")

    summary: DataQualitySummary = Field(default_factory=DataQualitySummary)
    issues: List[DQIssue] = Field(default_factory=list)

class GlobalDQIssues(BaseModel):
    model_config = ConfigDict(extra="allow")

    orphan_foreign_keys: List[Dict[str, Any]] = Field(default_factory=list)
    cross_dataset_inconsistencies: List[Dict[str, Any]] = Field(default_factory=list)
    relationship_row_issues: List[Dict[str, Any]] = Field(default_factory=list)
    relationship_warnings: List[Dict[str, Any]] = Field(default_factory=list)
    relationship_row_issues_supplemental: Optional[List[Dict[str, Any]]] = None

class DataQualityBlock(BaseModel):
    model_config = ConfigDict(extra="allow")

    datasets: Dict[str, DatasetDQBlock] = Field(default_factory=dict)
    global_issues: GlobalDQIssues = Field(default_factory=GlobalDQIssues)

class SemanticContext(BaseModel):
    model_config = ConfigDict(extra="allow")

    overall_semantic_confidence: float = 0.0
    by_dataset: Dict[str, Any] = Field(default_factory=dict)
    contract: Optional[Any] = None

class DriftSignal(BaseModel):
    model_config = ConfigDict(extra="allow")

    column: str
    metric: str
    baseline: Any
    current: Any
    severity: str
    message: str

class DatasetDrift(BaseModel):
    model_config = ConfigDict(extra="allow")

    dataset: str
    severity: str
    signal_count: int = 0
    has_baseline: bool = False
    signals: List[DriftSignal] = Field(default_factory=list)

class DriftRollup(BaseModel):
    model_config = ConfigDict(extra="allow")

    drift_score: float = 100.0
    worst_severity: str = "none"
    total_signal_count: int = 0
    per_dataset: List[DatasetDrift] = Field(default_factory=list)

class ReconciliationDeltas(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_to_parsed_loss: float = 0.0
    parsed_to_written_loss: float = 0.0

class DatasetReconciliation(BaseModel):
    model_config = ConfigDict(extra="allow")

    deltas: ReconciliationDeltas = Field(default_factory=ReconciliationDeltas)
    explainable_losses: List[str] = Field(default_factory=list)

class ReconciliationBlock(BaseModel):
    model_config = ConfigDict(extra="allow")

    by_dataset: Dict[str, DatasetReconciliation] = Field(default_factory=dict)

class ReadinessScore(BaseModel):
    model_config = ConfigDict(extra="allow")

    score: float = 100.0
    verdict: str = "READY"
    blockers: List[Dict[str, Any]] = Field(default_factory=list)

class GovernanceMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    manifest_version: str = "1.0"
    schema_hash: str = ""
    glossary_hash: Optional[str] = ""
    sql_execution_status: Optional[str] = None
    sql_execution_summary: Optional[Dict[str, Any]] = None

class AssessmentResultModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    datasets: Dict[str, DatasetProfile] = Field(default_factory=dict)
    relationships: List[Dict[str, Any]] = Field(default_factory=list)
    data_quality_issues: DataQualityBlock = Field(default_factory=DataQualityBlock)
    semantic_context: Optional[SemanticContext] = None
    drift_analysis: Optional[DriftRollup] = None
    reconciliation_analysis: Optional[ReconciliationBlock] = None
    etl_readiness: Optional[ReadinessScore] = None
    governance: Optional[GovernanceMetadata] = None
    run_metadata: Optional[Dict[str, Any]] = None
    transformation_suggestions: Optional[Dict[str, Any]] = None
    llm_insights: Optional[Dict[str, Any]] = None
    executive_summary_items: Optional[List[Dict[str, Any]]] = None
