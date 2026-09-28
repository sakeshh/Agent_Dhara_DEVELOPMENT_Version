from __future__ import annotations

from enum import Enum
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, Field

from agent.ir.rule_expression import RuleExpression


class IntentComplexity(str, Enum):
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


class IntentSpec(BaseModel):
    type: str = "filter_and_project"
    complexity: IntentComplexity = IntentComplexity.SIMPLE
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    description: Optional[str] = None


class LineageSpec(BaseModel):
    source_tables: List[str] = Field(default_factory=list)
    target_tables: List[str] = Field(default_factory=list)
    intermediate_views: List[str] = Field(default_factory=list)


class ValidationSpec(BaseModel):
    strict_schema: bool = True
    allow_missing_columns: bool = False
    max_repair_attempts: int = 3


class PhysicalHints(BaseModel):
    partition_count: Optional[int] = None
    broadcast_tables: List[str] = Field(default_factory=list)
    repartition_columns: List[str] = Field(default_factory=list)
    caching_enabled: bool = False


class SourceNode(BaseModel):
    table: str
    schema_name: str = "dbo"
    alias: Optional[str] = None
    columns: List[str] = Field(default_factory=list)
    filter_predicate: Optional[str] = None


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class JoinNode(BaseModel):
    left_table: str
    right_table: str
    join_type: JoinType = JoinType.INNER
    left_keys: List[str] = Field(default_factory=list)
    right_keys: List[str] = Field(default_factory=list)
    custom_condition: Optional[str] = None


class FilterNode(BaseModel):
    table: Optional[str] = None
    column: Optional[str] = None
    operator: Optional[str] = "eq"
    value: Optional[Any] = None
    rule: Optional[RuleExpression] = None


class AggregationFunc(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MIN = "min"
    MAX = "max"


class AggregationSpec(BaseModel):
    column: str
    func: AggregationFunc
    alias: str


class AggregateNode(BaseModel):
    table: Optional[str] = None
    group_by: List[str] = Field(default_factory=list)
    aggregations: List[AggregationSpec] = Field(default_factory=list)


class ProjectionNode(BaseModel):
    table: Optional[str] = None
    columns: List[str] = Field(default_factory=list)
    aliases: Dict[str, str] = Field(default_factory=dict)
    computed: Dict[str, str] = Field(default_factory=dict)


class OutputMode(str, Enum):
    NEW_TABLE = "new_table"
    OVERWRITE = "overwrite"
    APPEND = "append"
    VIEW = "view"


class OutputNode(BaseModel):
    target_table: str
    schema_name: str = "dbo"
    mode: OutputMode = OutputMode.NEW_TABLE
    format: str = "sql"  # sql, delta, parquet


class IrStep(BaseModel):
    step_id: str
    step_type: str  # source, join, filter, aggregate, projection, clean, rule
    description: Optional[str] = None
    source_node: Optional[SourceNode] = None
    join_node: Optional[JoinNode] = None
    filter_node: Optional[FilterNode] = None
    aggregate_node: Optional[AggregateNode] = None
    projection_node: Optional[ProjectionNode] = None
    rule_expression: Optional[RuleExpression] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class RepairRecord(BaseModel):
    original: str
    corrected: str
    strategy: str  # "case_normalization" | "fuzzy_match" | "llm_recovery"
    confidence: float
    target_entity: str = "column"  # "column" | "table"
    table: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)


class IrRepairMetrics(BaseModel):
    validation_attempts: int = 0
    fixer_invocations: int = 0
    repairs_applied: int = 0
    ambiguous_conflicts: int = 0
    repair_duration_ms: float = 0.0
    final_validation_status: str = "passed"  # "passed" | "repaired" | "failed"


class IrMetadata(BaseModel):
    session_id: str = "default"
    plan_id: str = ""
    planner_model: str = "deterministic"
    schema_hash: Optional[str] = None
    created_at: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    repair_history: List[RepairRecord] = Field(default_factory=list)
    repair_metrics: Optional[IrRepairMetrics] = None


class IrPlan(BaseModel):
    ir_version: str = "1.0"
    intent: IntentSpec = Field(default_factory=IntentSpec)
    sources: List[SourceNode] = Field(default_factory=list)
    logical_plan: List[IrStep] = Field(default_factory=list)
    physical_hints: PhysicalHints = Field(default_factory=PhysicalHints)
    output: Optional[OutputNode] = None
    lineage: LineageSpec = Field(default_factory=LineageSpec)
    validation: ValidationSpec = Field(default_factory=ValidationSpec)
    business_rules: List[RuleExpression] = Field(default_factory=list)
    metadata: IrMetadata = Field(default_factory=IrMetadata)

    def get_source_tables(self) -> List[str]:
        if self.lineage.source_tables:
            return self.lineage.source_tables
        return [s.table for s in self.sources]
