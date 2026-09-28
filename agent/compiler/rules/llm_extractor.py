from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.actions import (
    ActionSource,
    StructuredAction,
)
from agent.compiler.models.logical import ActionType
from agent.compiler.rules.column_resolver import ColumnResolver
from agent.compiler.rules.models import DecompositionStatus, RuleDecompositionResult

logger = logging.getLogger("agent.compiler.rules.llm_extractor")


class LLMActionCandidate(BaseModel):
    action_type: str = Field(..., description="ActionType enum string")
    target_columns: List[str] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.9, ge=0.0, le=1.0)


class LLMExtractor:
    """
    Tier 2 LLM Parameter Extractor.
    Extracts ONLY typed transformation parameters into a structured schema.
    Strictly forbidden from outputting executable code or SQL.
    Safely degrades upon network failure or invalid output.
    """

    ALLOWED_ACTIONS = {a.value for a in ActionType}

    def __init__(self, llm_callable: Optional[Callable[[str], str]] = None):
        """
        llm_callable: optional custom client/function for dependency injection / testing.
        """
        self._llm_callable = llm_callable

    def extract(
        self,
        rule_text: str,
        dataset_name: str,
        available_columns: List[str],
    ) -> RuleDecompositionResult:
        # 1. Check if mock/callable is provided or fallback to safe degraded response
        if not self._llm_callable:
            # When no LLM backend is configured, degrade gracefully
            return RuleDecompositionResult(
                status=DecompositionStatus.UNSUPPORTED,
                original_rule=rule_text,
                diagnostics=["No LLM service configured or available for rule extraction."],
                extraction_method="none",
            )

        try:
            # 2. Invoke LLM callable
            response_text = self._llm_callable(rule_text)
            return self._parse_llm_response(response_text, rule_text, dataset_name, available_columns)
        except Exception as e:
            logger.warning(f"LLM extraction encountered an error: {e}")
            return RuleDecompositionResult(
                status=DecompositionStatus.UNSUPPORTED,
                original_rule=rule_text,
                diagnostics=[f"LLM extraction failed or timed out: {str(e)}"],
                extraction_method="llm",
            )

    def _parse_llm_response(
        self,
        response_text: str,
        original_rule: str,
        dataset_name: str,
        available_columns: List[str],
    ) -> RuleDecompositionResult:
        # Check if LLM attempted to emit prohibited SQL commands
        prohibited_sql = ["DROP", "SELECT", "UPDATE", "INSERT", "DELETE", "ALTER", "CREATE", "EXEC", "TRUNCATE", ";", "--"]
        for kw in prohibited_sql:
            # Check for keyword surrounded by spaces or punctuation
            if re.search(rf"\b{kw}\b", response_text, re.IGNORECASE):
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=original_rule,
                    diagnostics=[f"Prohibited executable SQL '{kw}' detected in LLM extraction response. Only structured JSON parameters are permitted."],
                    extraction_method="llm",
                )

        # Parse JSON
        try:
            # Extract JSON block if markdown code fences were used
            json_str = response_text.strip()
            fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", json_str)
            if fence_match:
                json_str = fence_match.group(1).strip()

            raw_obj = json.loads(json_str)
            candidate = LLMActionCandidate.model_validate(raw_obj)
        except Exception as e:
            return RuleDecompositionResult(
                status=DecompositionStatus.INVALID,
                original_rule=original_rule,
                diagnostics=[f"LLM returned malformed JSON or failed schema validation: {e}"],
                extraction_method="llm",
            )

        # Validate action_type is an allowed ActionType
        act_lower = candidate.action_type.lower()
        if act_lower not in self.ALLOWED_ACTIONS:
            return RuleDecompositionResult(
                status=DecompositionStatus.UNSUPPORTED,
                original_rule=original_rule,
                diagnostics=[f"LLM proposed unrecognized action_type '{candidate.action_type}'."],
                extraction_method="llm",
            )

        # Resolve target columns against schema
        resolved_cols = []
        for col_name in candidate.target_columns:
            res = ColumnResolver.resolve_column(col_name, available_columns)
            if not res:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=original_rule,
                    diagnostics=[f"LLM hallucinated or referenced non-existent column '{col_name}'."],
                    extraction_method="llm",
                )
            resolved_cols.append(res)

        action_type_enum = ActionType(act_lower)
        structured_action = StructuredAction(
            action_type=action_type_enum,
            dataset_name=dataset_name,
            target_columns=resolved_cols,
            parameters=candidate.parameters,
            source=ActionSource.BUSINESS_RULE,
            extraction_method="llm",
            confidence=candidate.confidence,
        )

        return RuleDecompositionResult(
            status=DecompositionStatus.RESOLVED,
            actions=[structured_action],
            extraction_method="llm",
            confidence=candidate.confidence,
            original_rule=original_rule,
        )
