# Core interfaces for pipeline orchestration
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple


class PipelinePhase(str, Enum):
    """Phases in the ETL pipeline."""
    ASSESSMENT = "assessment"
    PLANNING = "planning"
    REVIEW = "review"
    CODE_GENERATION = "code_generation"
    VALIDATION = "validation"
    EXECUTION = "execution"
    COMPLETION = "completion"


class PipelineStatus(str, Enum):
    """Status of a pipeline run."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PhaseGate(str, Enum):
    """Gates that control pipeline progression."""
    GATE_ASSESSMENT = "gate_assessment"
    GATE_PLANNING = "gate_planning"
    GATE_REVIEW = "gate_review"
    GATE_CODEGEN = "gate_codegen"
    GATE_VALIDATION = "gate_validation"
    GATE_EXECUTION = "gate_execution"


class PipelineEventType(str, Enum):
    """Types of pipeline events for observability."""
    EVENT_START = "event_start"
    EVENT_COMPLETE = "event_complete"
    EVENT_ERROR = "event_error"
    EVENT_GATE_DECISION = "gate_decision"
    EVENT_STEP_COMPLETE = "step_complete"


@dataclass
class PipelineState:
    """Current state of a pipeline execution."""
    phase: PipelinePhase = PipelinePhase.ASSESSMENT
    status: PipelineStatus = PipelineStatus.PENDING
    current_step: Optional[str] = None
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class GateContext:
    """Context for phase gate decisions."""
    pipeline_id: str
    current_phase: PipelinePhase
    decision: str  # "proceed", "review", "block", "retry"
    reason: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    blocked_by: List[str] = field(default_factory=list)


@dataclass
class PhaseGateResult:
    """Result of a phase gate evaluation."""
    can_proceed: bool
    decision: str
    next_phase: PipelinePhase
    reason: str
    actions: List[str] = field(default_factory=list)


class PipelineEvent:
    """Event for pipeline observability."""
    event_type: PipelineEventType
    pipeline_id: str
    pipeline_name: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    phase: Optional[PipelinePhase] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)
    user_id: Optional[str] = None


# Infrastructure DDL templates
ETL_DEFAULT_VALUES_SCHEMA = """
CREATE TABLE dbo.etl_default_values (
    column_name NVARCHAR(255) NOT NULL,
    default_value NVARCHAR(MAX) NOT NULL,
    data_type NVARCHAR(100) NOT NULL,
    last_updated DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_etl_default_values PRIMARY KEY (column_name)
);
"""

ETL_INVALID_VALUES_SCHEMA = """
CREATE TABLE dbo.etl_invalid_values (
    column_name NVARCHAR(255) NOT NULL,
    invalid_value NVARCHAR(MAX) NOT NULL,
    row_count BIGINT NOT NULL,
    last_seen DATETIME2 NOT NULL,
    CONSTRAINT PK_etl_invalid_values PRIMARY KEY (column_name, invalid_value)
);
"""

ETL_LOG_SCHEMA = """
CREATE TABLE dbo.etl_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    process_name NVARCHAR(255) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2,
    status NVARCHAR(50) NOT NULL,
    error_message NVARCHAR(MAX),
    records_processed BIGINT NOT NULL DEFAULT 0,
    records_written BIGINT NOT NULL DEFAULT 0
);
"""

ETL_REJECT_SCHEMA = """
CREATE TABLE dbo.etl_rejects (
    id INT IDENTITY(1,1) PRIMARY KEY,
    process_name NVARCHAR(255) NOT NULL,
    dataset_name NVARCHAR(255) NOT NULL,
    column_name NVARCHAR(255),
    reject_value NVARCHAR(MAX),
    reject_reason NVARCHAR(MAX),
    rejected_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
"""

ETL_WATERMARK_SCHEMA = """
CREATE TABLE dbo.etl_watermark (
    process_name NVARCHAR(255) NOT NULL,
    last_run_time DATETIME2 NOT NULL,
    CONSTRAINT UC_etl_watermark UNIQUE (process_name)
);
"""

ETL_INVALID_VALUES_SCHEMA = """
CREATE TABLE dbo.etl_invalid_values (
    id INT IDENTITY(1,1) PRIMARY KEY,
    process_name NVARCHAR(255) NOT NULL,
    dataset_name NVARCHAR(255) NOT NULL,
    column_name NVARCHAR(255) NOT NULL,
    invalid_value NVARCHAR(MAX) NOT NULL,
    reject_reason NVARCHAR(MAX),
    rejected_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
"""


def get_infrastructure_ddl(
    include_rejects: bool = True,
    include_watermark: bool = True,
    include_default_values: bool = True,
    include_log: bool = True,
    dialect: str = "tsql",
    include_go: bool = False,
    emitted_objects: Optional[Set[str]] = None,
) -> str:
    """Generate infrastructure DDL for ETL pipeline."""
    if emitted_objects is None:
        emitted_objects = set()
    
    lines: List[str] = []
    
    if include_log:
        lines.append(ETL_LOG_SCHEMA)
        if emitted_objects:
            emitted_objects.add("etl_log")
    
    if include_default_values:
        lines.append(ETL_DEFAULT_VALUES_SCHEMA)
        if emitted_objects:
            emitted_objects.add("etl_default_values")
    
    if include_invalid_values or include_rejects:
        lines.append(ETL_INVALID_VALUES_SCHEMA)
        if emitted_objects:
            emitted_objects.add("etl_invalid_values")
    
    if include_rejects:
        lines.append(ETL_REJECT_SCHEMA)
        if emitted_objects:
            emitted_objects.add("etl_rejects")
    
    if include_watermark:
        lines.append(ETL_WATERMARK_SCHEMA)
        if emitted_objects:
            emitted_objects.add("etl_watermark")
    
    return "\n".join(lines)


# Pipeline definition classes
@dataclass
class PipelineDefinition:
    """Definition of an ETL pipeline."""
    pipeline_id: str
    name: str
    description: str
    phases: List[PipelinePhase]
    config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledRun:
    """A scheduled pipeline run."""
    pipeline_id: str
    schedule_name: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cron_expression: Optional[str] = None
    trigger: str = "one_time"  # "cron", "interval", "one_time", "event_based"
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_completed: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    configured: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "pipeline_id": self.pipeline_id,
            "schedule_name": self.schedule_name,
            "trigger": self.trigger,
            "status": self.status,
            "cron_expression": self.cron_expression,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_completed": self.last_completed.isoformat() if self.last_completed else None,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
        }


# Gate decision types
@dataclass
class GateDecision:
    """Decision from a phase gate."""
    gate: PhaseGate
    approved: bool
    reason: str
    conditions: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)


@dataclass
class GateDecisionRecord:
    """Record of gate decisions for audit trail."""
    gate: PhaseGate
    decision: GateDecision
    decided_at: datetime = field(default_factory=datetime.utcnow)
    decided_by: Optional[str] = None


# Relationship and connector info (shared interfaces)
@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    schema: str
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None


@dataclass
class FileInfo:
    """Information about a file source."""
    path: str
    file_format: str  # "csv", "json", "parquet", "xml", etc.
    header_row: bool = True
    infer_schema: bool = True


@dataclass
class SourceType:
    """Source type for connectors."""
    SQL = "sql"
    BLOB = "blob"
    DELTA = "delta"
    API = "api"
    KAFKA = "kafka"
    ADLS = "adls"


@dataclass
class FileFormat:
    """File format for data sources."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    XML = "xml"
    AVRO = "avro"


# Agent metadata
@dataclass
class AgentMetadata:
    """Metadata about an agent instance."""
    agent_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "unnamed_agent"
    version: str = "1.0.0"
    capabilities: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    status: str = "active"  # "active", "idle", "offline"
    tags: Dict[str, str] = field(default_factory=dict)


class PipelineOrchestratorInterface:
    """Interface for pipeline orchestrator implementations."""
    async def run_assessment(self, context: Any) -> Any:
        """Run assessment on dataset."""
        raise NotImplementedError
    async def generate_plan(self, assessment: Any) -> Any:
        """Generate ETL plan from assessment."""
        raise NotImplementedError
    async def generate_code(self, plan: Any) -> Any:
        """Generate code from ETL plan."""
        raise NotImplementedError
    async def validate_code(self, code: Any, plan: Any) -> Any:
        """Validate generated code."""
        raise NotImplementedError
    async def execute_plan(self, plan: Any) -> Any:
        """Execute the ETL plan."""
        raise NotImplementedError

# Backwards compatibility exports
PipelineStateType = PipelinePhase
PipelineStatusType = PipelineStatus