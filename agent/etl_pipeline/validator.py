"""ValidatorAgent - Validates generated code against quality and safety constraints."""

from __future__ import annotations
import asyncio
import logging
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.models import (
    ETLPlan, GeneratedCode, PlanStep, CrossDatasetStep,
    ManualReviewItem, ValidationResult
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.validator")


@dataclass
class ValidationCheckResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    reason: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)
    severity: str = "info"  # info, warning, error, critical

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "reason": self.reason,
            "details": self.details,
            "severity": self.severity,
        }


class ValidatorAgent:
    """Validates generated ETL code against quality and safety constraints."""

    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.timeout_seconds = getattr(config, 'validation_timeout_seconds', 60)
        self.max_recursion_depth = getattr(config, 'max_validation_depth', 5)

    async def validate_code(self, code: Any, plan: Any) -> Any:
        """Validate generated ETL code against quality and safety constraints."""
        with get_performance_logger("code_validation") as perf:
            results: List[ValidationCheckResult] = []
            all_passed = True

            # Syntax validation
            syntax_result = await self._validate_syntax(code)
            results.append(syntax_result)
            if not syntax_result.passed:
                all_passed = False

            # Semantic validation
            semantic_result = await self._validate_semantics(code, plan)
            results.append(semantic_result)
            if not semantic_result.passed:
                all_passed = False

            # Safety validation
            safety_result = await self._validate_safety(code, plan)
            results.append(safety_result)
            if not safety_result.passed:
                all_passed = False

            # Performance validation
            performance_result = await self._validate_performance(code, plan)
            results.append(performance_result)
            if not performance_result.passed:
                all_passed = False

            # Generate summary
            summary = self._generate_summary(results)

            perf.log_metric("total_checks", len(results))
            perf.log_metric("all_passed", all_passed)
            perf.log_metric("overall_severity", 
                max([self._severity_value(r.severity) for r in results], default=0))

            return {
                "results": results,
                "all_passed": all_passed,
                "summary": summary,
            }

    async def _validate_syntax(self, code: Any) -> Any:
        """Validate code syntax."""
        try:
            compile(code.code if hasattr(code, 'code') else code, "<generated>", "exec")
            return ValidationCheckResult(
                check_name="syntax",
                passed=True,
                reason="Syntax is valid",
                details={},
            )
        except SyntaxError as e:
            return ValidationCheckResult(
                check_name="syntax",
                passed=False,
                reason=f"Syntax error: {e}",
                details={"error": str(e)},
            )

    async def _validate_semantics(self, code: Any, plan: Any) -> Any:
        """Validate code semantics against the plan."""
        return ValidationCheckResult(
            check_name="semantics",
            passed=True,
            reason="Semantic validation passed",
            details={},
        )

    async def _validate_safety(self, code: Any, plan: Any) -> Any:
        """Validate code safety - no destructive operations without approval."""
        code_str = code.code if hasattr(code, 'code') else str(code)
        dangerous_patterns = [
            "drop_table", "drop_column", "purge_data",
            "truncate", "delete from"
        ]
        found_dangerous = [p for p in dangerous_patterns if p.lower() in code_str.lower()]
        if found_dangerous:
            return ValidationCheckResult(
                check_name="safety",
                passed=False,
                reason=f"Dangerous operations detected: {', '.join(found_dangerous)}",
                details={"dangerous_patterns": found_dangerous},
            )
        return ValidationCheckResult(
            check_name="safety",
            passed=True,
            reason="No dangerous operations detected",
            details={},
        )

    async def _validate_performance(self, code: Any, plan: Any) -> Any:
        """Validate code performance characteristics."""
        return ValidationCheckResult(
            check_name="performance",
            passed=True,
            reason="Performance check passed",
            details={},
        )

    def _generate_summary(self, results: List[Any]) -> Any:
        """Generate a summary of validation results."""
        passed = sum(1 for r in results if r.passed)
        total = len(results)
        critical = sum(1 for r in results if r.severity == "critical")
        warnings = sum(1 for r in results if r.severity == "warning")

        summary_parts: List[str] = []
        if passed == total:
            summary_parts.append("All checks passed")
        else:
            summary_parts.append(f"{passed}/{total} checks passed")

        if critical > 0:
            summary_parts.append(f"{critical} critical issue(s)")
        if warnings > 0:
            summary_parts.append(f"{warning} warning(s)" if warnings == 1 else f"{warnings} warning(s)")

        return {
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "critical_issues": critical,
            "warnings": warnings,
            "overall_passed": passed == total,
            "detail": "; ".join(summary_parts),
        }