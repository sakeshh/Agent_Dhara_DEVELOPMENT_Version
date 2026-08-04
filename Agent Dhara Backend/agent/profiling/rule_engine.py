"""
Universal Rule Engine Framework for High-Accuracy Data Quality Engine.
Provides standard QualityRule interface, RuleRegistry, RulePlanner, and RuleExecutor
enforcing 100% rule evaluation coverage with zero silent failures.
"""

import time
import traceback
from typing import Any, Dict, List, Optional, Set, Type

from pydantic import BaseModel, Field

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult, RuleCoverageSummary


class QualityRuleContext(BaseModel):
    """
    Evaluation context passed to quality rules.
    Contains raw dataset, metadata, schema, semantic context, contracts, and locators.
    """
    dataset_name: str
    df: Any  # pandas DataFrame
    raw_df: Optional[Any] = None
    column_name: Optional[str] = None
    semantic_type: Optional[str] = None
    semantic_confidence: float = 1.0
    contract: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    all_datasets: Dict[str, Any] = Field(default_factory=dict)


NUMERIC_SEMANTIC_TYPES = {"integer", "decimal", "float", "currency_amount", "percentage", "quantity", "numeric"}
DATE_SEMANTIC_TYPES = {"date", "datetime", "timestamp"}
COUNTRY_SEMANTIC_TYPES = {"country", "country_code", "geography"}
IDENTIFIER_SEMANTIC_TYPES = {"identifier", "primary_key", "foreign_key", "sensitive_identifier"}

RULE_SEMANTIC_COMPATIBILITY: Dict[str, Set[str]] = {
    "numeric_range": NUMERIC_SEMANTIC_TYPES,
    "decimal_scale": NUMERIC_SEMANTIC_TYPES,
    "invalid_numeric": NUMERIC_SEMANTIC_TYPES,
    "numeric_outlier": NUMERIC_SEMANTIC_TYPES,
    "future_date": DATE_SEMANTIC_TYPES,
    "date_parseability": DATE_SEMANTIC_TYPES,
    "allowed_country": COUNTRY_SEMANTIC_TYPES,
    "identifier_pattern": IDENTIFIER_SEMANTIC_TYPES,
}


class QualityRule:
    """
    Base class for all Data Quality Rules in Engine 3.0.
    """
    rule_id: str = "generic_rule"
    version: str = "3.0.0"
    dimension: str = "validity"  # completeness, validity, uniqueness, consistency, conformity, referential_integrity, accuracy, timeliness
    scope: str = "column"        # "column" or "dataset"
    applicable_semantic_types: Optional[List[str]] = None  # None means all semantic types applicable
    rule_family: Optional[str] = None
    required_inputs: List[str] = Field(default_factory=list)
    default_severity: str = "HIGH"

    def is_applicable(self, context: QualityRuleContext) -> bool:
        """
        Determines whether this rule is applicable to the given context.
        """
        if self.scope == "column" and not context.column_name:
            return False
        if self.applicable_semantic_types is not None:
            if not context.semantic_type or context.semantic_type not in self.applicable_semantic_types:
                return False

        # Enforce universal semantic guards across rule families
        family = self.rule_family or self.rule_id
        allowed_types = RULE_SEMANTIC_COMPATIBILITY.get(family)
        if allowed_types and context.semantic_type:
            if context.semantic_type.lower() not in allowed_types:
                return False

        return True


    def execute(self, context: QualityRuleContext) -> RuleEvaluationResult:
        """
        Wrapper around evaluate that guarantees an explicit 7-state result and timing.
        Catch exceptions to avoid silent skips and output EXECUTION_ERROR.
        """
        start_time = time.time()
        try:
            if not self.is_applicable(context):
                return RuleEvaluationResult(
                    rule_id=self.rule_id,
                    rule_version=self.version,
                    status=RuleStatus.NOT_APPLICABLE,
                    dataset=context.dataset_name,
                    column=context.column_name,
                    dimension=self.dimension,
                    severity=self.default_severity,
                    execution_time_ms=(time.time() - start_time) * 1000.0,
                    error_message="Rule not applicable for dataset/column semantic type"
                )

            result = self.evaluate(context)
            result.execution_time_ms = (time.time() - start_time) * 1000.0
            return result

        except Exception as e:
            return RuleEvaluationResult(
                rule_id=self.rule_id,
                rule_version=self.version,
                status=RuleStatus.EXECUTION_ERROR,
                dataset=context.dataset_name,
                column=context.column_name,
                dimension=self.dimension,
                severity=self.default_severity,
                execution_time_ms=(time.time() - start_time) * 1000.0,
                error_message=f"{type(e).__name__}: {str(e)}",
                evidence={"traceback": traceback.format_exc()}
            )

    def evaluate(self, context: QualityRuleContext) -> RuleEvaluationResult:
        """
        Subclasses must implement actual evaluation logic returning RuleEvaluationResult.
        """
        raise NotImplementedError("Subclasses must implement evaluate()")


class RuleRegistry:
    """
    Registry for managing available QualityRules.
    """
    def __init__(self):
        self._rules: Dict[str, QualityRule] = {}

    def register(self, rule: QualityRule):
        self._rules[rule.rule_id] = rule

    def get_rule(self, rule_id: str) -> Optional[QualityRule]:
        return self._rules.get(rule_id)

    def list_rules(self) -> List[QualityRule]:
        return list(self._rules.values())


# Global Rule Registry instance
rule_registry = RuleRegistry()


class RulePlanner:
    """
    Plans rule execution across datasets and columns.
    Enforces 100% configured-rule evaluation coverage.
    """
    def __init__(self, registry: Optional[RuleRegistry] = None):
        self.registry = registry or rule_registry

    def plan_rules_for_dataset(
        self,
        dataset_name: str,
        df: Any,
        column_semantic_types: Optional[Dict[str, str]] = None,
        contract: Optional[Dict[str, Any]] = None
    ) -> List[tuple[QualityRule, QualityRuleContext]]:
        """
        Generates planned pairs of (QualityRule, QualityRuleContext) for execution.
        """
        planned: List[tuple[QualityRule, QualityRuleContext]] = []
        column_semantic_types = column_semantic_types or {}

        for rule in self.registry.list_rules():
            if rule.scope == "dataset":
                ctx = QualityRuleContext(
                    dataset_name=dataset_name,
                    df=df,
                    contract=contract
                )
                planned.append((rule, ctx))
            else:
                for col in df.columns:
                    sem_type = column_semantic_types.get(col)
                    ctx = QualityRuleContext(
                        dataset_name=dataset_name,
                        df=df,
                        column_name=col,
                        semantic_type=sem_type,
                        contract=contract
                    )
                    planned.append((rule, ctx))

        return planned



class RuleExecutor:
    """
    Executes planned rules and builds RuleCoverageSummary.
    """
    def execute_plan(
        self,
        planned_rules: List[tuple[QualityRule, QualityRuleContext]]
    ) -> tuple[List[RuleEvaluationResult], RuleCoverageSummary]:
        results: List[RuleEvaluationResult] = []
        summary = RuleCoverageSummary(total_configured_rules=len(planned_rules))

        for rule, ctx in planned_rules:
            res = rule.execute(ctx)
            results.append(res)

            # Rollup counts
            if res.status == RuleStatus.PASS:
                summary.passed_count += 1
                summary.evaluated_count += 1
            elif res.status == RuleStatus.FAIL:
                summary.failed_count += 1
                summary.evaluated_count += 1
            elif res.status == RuleStatus.NOT_APPLICABLE:
                summary.not_applicable_count += 1
            elif res.status == RuleStatus.NOT_EVALUATED:
                summary.not_evaluated_count += 1
            elif res.status == RuleStatus.CONFIGURATION_ERROR:
                summary.configuration_error_count += 1
            elif res.status == RuleStatus.EXECUTION_ERROR:
                summary.execution_error_count += 1
            elif res.status == RuleStatus.INSUFFICIENT_EVIDENCE:
                summary.insufficient_evidence_count += 1

        if summary.total_configured_rules > 0:
            summary.coverage_percentage = (
                (summary.evaluated_count + summary.not_applicable_count)
                / summary.total_configured_rules
            ) * 100.0

        return results, summary
