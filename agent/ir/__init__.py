from __future__ import annotations

from agent.ir.models import (
    IrPlan,
    IntentSpec,
    IntentComplexity,
    SourceNode,
    JoinNode,
    JoinType,
    FilterNode,
    AggregateNode,
    AggregationSpec,
    AggregationFunc,
    ProjectionNode,
    OutputNode,
    OutputMode,
    IrStep,
    LineageSpec,
    ValidationSpec,
    PhysicalHints,
    IrMetadata,
)
from agent.ir.rule_expression import (
    RuleExpression,
    RuleType,
    ComparisonOperator,
    CompoundLogicalOperator,
)
from agent.ir.errors import (
    IrError,
    IrErrorCode,
    IrValidationResult,
)
from agent.ir.validator import IrValidator
from agent.ir.normalizer import IrNormalizer
from agent.ir.extractor import IrExtractor
from agent.ir.fixer import IrFixer
from agent.ir.builder import IrBuilder
from agent.ir.migrations import IrMigrator

__all__ = [
    "IrPlan",
    "IntentSpec",
    "IntentComplexity",
    "SourceNode",
    "JoinNode",
    "JoinType",
    "FilterNode",
    "AggregateNode",
    "AggregationSpec",
    "AggregationFunc",
    "ProjectionNode",
    "OutputNode",
    "OutputMode",
    "IrStep",
    "LineageSpec",
    "ValidationSpec",
    "PhysicalHints",
    "IrMetadata",
    "RuleExpression",
    "RuleType",
    "ComparisonOperator",
    "CompoundLogicalOperator",
    "IrError",
    "IrErrorCode",
    "IrValidationResult",
    "IrValidator",
    "IrNormalizer",
    "IrExtractor",
    "IrFixer",
    "IrBuilder",
    "IrMigrator",
]
