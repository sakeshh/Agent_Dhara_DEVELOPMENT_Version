"""
Explicit Rule Execution States and Evaluation Result Models for High-Accuracy Data Quality Engine.
Enforces that every rule evaluation produces exactly one of 7 explicit states with 0% silent skips.
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class RuleStatus(str, Enum):
    """
    Explicit execution states for configured data quality rules.
    Every applicable rule MUST end in exactly one explicit state.
    """
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_EVALUATED = "NOT_EVALUATED"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class RuleEvaluationResult(BaseModel):
    """
    Standardized, reproducible result container for a single rule execution.
    """
    model_config = ConfigDict(extra="allow")

    rule_id: str
    rule_version: str = "3.0.0"
    status: RuleStatus
    dataset: str
    column: Optional[str] = None
    dimension: str = "validity"  # completeness, validity, uniqueness, consistency, conformity, referential_integrity, accuracy, timeliness
    severity: str = "HIGH"       # HIGH, MEDIUM, LOW, CRITICAL
    confidence: float = 1.0
    evaluated_count: int = 0
    failed_count: int = 0
    failure_rate: float = 0.0
    affected_rows: List[int] = Field(default_factory=list)
    samples: List[Any] = Field(default_factory=list)
    evidence: Dict[str, Any] = Field(default_factory=dict)
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None


class RuleCoverageSummary(BaseModel):
    """
    Evaluation coverage rollup across all configured rules.
    """
    model_config = ConfigDict(extra="allow")

    total_configured_rules: int = 0
    evaluated_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    not_applicable_count: int = 0
    not_evaluated_count: int = 0
    configuration_error_count: int = 0
    execution_error_count: int = 0
    insufficient_evidence_count: int = 0
    coverage_percentage: float = 100.0
    silent_failure_count: int = 0
