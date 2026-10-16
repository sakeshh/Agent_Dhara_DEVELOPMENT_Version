"""Assessment Data Models."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from datetime import datetime


class IssueSeverity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class IssueCategory(Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"
    CONFORMITY = "conformity"
    BUSINESS_LOGIC = "business_logic"


class FixabilityTier(Enum):
    AUTO_FIXABLE = "auto_fixable"
    CONDITIONALLY_FIXABLE = "conditionally_fixable"
    HUMAN_REVIEW_REQUIRED = "human_review_required"
    ROOT_CAUSE_ONLY = "root_cause_only"


class SemanticType(Enum):
    # Identifiers
    PRIMARY_KEY = "primary_key"
    FOREIGN_KEY = "foreign_key"
    UNIQUE_ID = "unique_id"
    UUID = "uuid"
    
    # Text
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    NAME = "name"
    ADDRESS = "address"
    TEXT = "text"
    DESCRIPTION = "description"
    CATEGORY = "category"
    STATUS = "status"
    CODE = "code"
    
    # Numeric
    CURRENCY = "currency"
    PRICE = "price"
    AMOUNT = "amount"
    QUANTITY = "quantity"
    COUNT = "count"
    PERCENTAGE = "percentage"
    RATIO = "ratio"
    SCORE = "score"
    RATING = "rating"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    
    # Temporal
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    TIME = "time"
    DURATION = "duration"
    AGE = "age"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    
    # Boolean
    BOOLEAN = "boolean"
    FLAG = "flag"
    
    # Special
    JSON = "json"
    XML = "xml"
    BINARY = "binary"
    UNKNOWN = "unknown"


@dataclass
class ColumnProfile:
    """Profile of a single column."""
    name: str
    data_type: str
    semantic_type: SemanticType = SemanticType.UNKNOWN
    semantic_confidence: float = 0.0
    
    # Statistics
    null_count: int = 0
    null_percentage: float = 0.0
    distinct_count: int = 0
    distinct_percentage: float = 0.0
    
    # Numeric stats
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    quantiles: Dict[str, float] = field(default_factory=dict)
    
    # Text stats
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    pattern_distribution: Dict[str, int] = field(default_factory=dict)
    
    # Date stats
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    
    # Quality
    is_primary_key_candidate: bool = False
    is_foreign_key_candidate: bool = False
    pii_level: str = "none"  # none, low, medium, high
    
    # Sample values
    sample_values: List[Any] = field(default_factory=list)
    top_values: List[Dict[str, Any]] = field(default_factory=dict)
    
    # Issues
    issues: List["QualityIssue"] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, (list, dict)):
                result[key] = value
            else:
                result[key] = value
        return result


@dataclass
class DatasetProfile:
    """Profile of a dataset/table."""
    name: str
    schema: Optional[str] = None
    source_type: str = "unknown"
    row_count: int = 0
    column_count: int = 0
    size_bytes: int = 0
    
    columns: Dict[str, ColumnProfile] = field(default_factory=dict)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, Any]] = field(default_factory=list)
    
    # Quality summary
    total_issues: int = 0
    issues_by_severity: Dict[str, int] = field(default_factory=dict)
    issues_by_category: Dict[str, int] = field(default_factory=dict)
    
    # Relationships
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    profiled_at: datetime = field(default_factory=datetime.utcnow)
    profiling_duration_ms: int = 0
    sample_size: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                result[key] = {k: v.to_dict() if hasattr(v, 'to_dict') else v for k, v in value.items()}
            elif isinstance(value, list):
                result[key] = [v.to_dict() if hasattr(v, 'to_dict') else v for v in value]
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class QualityIssue:
    """A data quality issue."""
    id: str
    column: str
    dataset: str
    category: IssueCategory
    severity: IssueSeverity
    message: str
    
    # Details
    count: int = 0
    percentage: float = 0.0
    sample_values: List[Any] = field(default_factory=list)
    affected_rows: List[int] = field(default_factory=list)
    
    # Rule info
    rule_id: Optional[str] = None
    rule_name: Optional[str] = None
    rule_parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Fixability
    fixability: FixabilityTier = FixabilityTier.HUMAN_REVIEW_REQUIRED
    suggested_action: Optional[str] = None
    suggested_parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    detected_at: datetime = field(default_factory=datetime.utcnow)
    detector: str = "unknown"
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result


@dataclass
class Relationship:
    """Relationship between datasets."""
    id: str
    parent_dataset: str
    parent_columns: List[str]
    child_dataset: str
    child_columns: List[str]
    relationship_type: str = "many_to_one"  # one_to_one, one_to_many, many_to_one, many_to_many
    confidence: float = 0.0
    overlap_count: int = 0
    overlap_percentage: float = 0.0
    orphan_count: int = 0
    orphan_percentage: float = 0.0
    detected_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AssessmentResult:
    """Complete assessment result."""
    assessment_id: str
    datasets: Dict[str, DatasetProfile] = field(default_factory=dict)
    relationships: List[Relationship] = field(default_factory=list)
    cross_dataset_issues: List[Any] = field(default_factory=list)
    
    # Global metrics
    total_rows: int = 0
    total_columns: int = 0
    total_issues: int = 0
    issues_by_severity: Dict[str, int] = field(default_factory=dict)
    issues_by_category: Dict[str, int] = field(default_factory=dict)
    
    # Business logic
    discovered_rules: List[Any] = field(default_factory=list)
    business_logic_summary: Dict[str, Any] = field(default_factory=dict)
    
    # Drift
    drift_report: Optional[Any] = None
    
    # Metadata
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_ms: int = 0
    version: str = "2.0.0"
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                result[key] = {k: v.to_dict() if hasattr(v, 'to_dict') else v for k, v in value.items()}
            elif isinstance(value, list):
                result[key] = [v.to_dict() if hasattr(v, 'to_dict') else v for v in value]
            elif hasattr(value, 'to_dict'):
                result[key] = value.to_dict()
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class SemanticProfile:
    """Semantic understanding of a dataset."""
    dataset_name: str
    entities: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # Entity name -> attributes
    entity_columns: Dict[str, str] = field(default_factory=dict)  # Column -> entity
    column_roles: Dict[str, str] = field(default_factory=dict)  # Column -> role (identifier, measure, attribute, timestamp, key)
    inferred_constraints: List[Dict[str, Any]] = field(default_factory=list)
    domain: Optional[str] = None  # Finance, HR, Supply Chain, etc.
    confidence: float = 0.0