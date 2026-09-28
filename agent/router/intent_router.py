from __future__ import annotations

import logging
import re
import time
from typing import Dict, List, Optional
from agent.compiler.models.profile import DatasetProfile
from agent.ir.models import IntentComplexity
from agent.router.models import IntentClassification, RouteTarget

logger = logging.getLogger("agent.router.intent_router")

# Regex pattern groups
_COMPLEX_PATTERNS = [
    (r"\b(scd|scd2|type[\s\-_]?2|slowly\s+changing)\b", "scd_type_2"),
    (r"\b(cdc|change\s+data\s+capture)\b", "change_data_capture"),
    (r"\b(incremental\s+merge|merge\s+into|upsert)\b", "incremental_merge"),
    (r"\b(dimensional\s+model|star\s+schema|fact\s+table|dimension\s+table)\b", "dimensional_modeling"),
    (r"\b(adf\s+pipeline|data\s+factory\s+pipeline|orchestrat\w+)\b", "pipeline_orchestration"),
]

_MODERATE_PATTERNS = [
    (r"\b(join|inner\s+join|left\s+join|right\s+join|cross\s+join)\b", "join"),
    (r"\b(group\s+by|aggregate|summarize|rollup|cube)\b", "aggregation"),
    (r"\b(window\s+function|row_number|rank\(\)|dense_rank|partition\s+by)\b", "window_function"),
    (r"\b(pivot|unpivot)\b", "pivot"),
]

_SIMPLE_PATTERNS = [
    (r"\b(filter|where|find\s+all|show\s+all|list|display)\b", "filter"),
    (r"\b(dedup|deduplicate|duplicate|distinct|unique)\b", "deduplicate"),
    (r"\b(remove\s+null|drop\s+null|fill\s+null|impute)\b", "null_handling"),
    (r"\b(trim|uppercase|lowercase|standardize)\b", "string_cleaning"),
    (r"\b(rename|cast|convert)\b", "transformation"),
]


class IntentRouter:
    """
    Sub-millisecond intent router for incoming user prompts.
    Routes queries based on structural and semantic complexity:
    - Simple: Direct compiler path (zero LLM overhead, <5ms).
    - Moderate: LLM-powered IR planner (joins, aggregations, conditional logic).
    - Complex: Multi-agent planner (SCD Type 2, CDC, incremental merge, ADF workflows).
    """

    @classmethod
    def classify(
        cls,
        user_prompt: str,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
    ) -> IntentClassification:
        t0 = time.perf_counter()
        prompt_lower = user_prompt.lower().strip()

        matched_ops: List[str] = []
        matched_patterns: List[str] = []

        # 1. Check for Complex Patterns (Highest Priority)
        for pattern, op_name in _COMPLEX_PATTERNS:
            if re.search(pattern, prompt_lower):
                matched_ops.append(op_name)
                matched_patterns.append(pattern)

        if matched_ops:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return IntentClassification(
                intent=matched_ops[0],
                complexity=IntentComplexity.COMPLEX,
                confidence=0.96,
                route=RouteTarget.MULTI_AGENT_PLANNER,
                reasoning=f"Detected complex ETL pattern '{matched_ops[0]}'; routing to multi-agent planner.",
                detected_operations=matched_ops,
                matched_patterns=matched_patterns,
                latency_ms=round(elapsed_ms, 3),
            )

        # 2. Check for Multi-table join clues
        if dataset_profiles and len(dataset_profiles) > 1:
            mentioned_tables = [t for t in dataset_profiles.keys() if t.lower() in prompt_lower]
            if len(mentioned_tables) >= 2:
                matched_ops.append("multi_table_join")

        # 3. Check for Moderate Patterns
        for pattern, op_name in _MODERATE_PATTERNS:
            if re.search(pattern, prompt_lower):
                matched_ops.append(op_name)
                matched_patterns.append(pattern)

        if matched_ops:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return IntentClassification(
                intent=matched_ops[0],
                complexity=IntentComplexity.MODERATE,
                confidence=0.92,
                route=RouteTarget.LLM_IR_PLANNER,
                reasoning=f"Detected moderate operational pattern '{matched_ops[0]}'; routing to LLM IR planner.",
                detected_operations=matched_ops,
                matched_patterns=matched_patterns,
                latency_ms=round(elapsed_ms, 3),
            )

        # 4. Check for Simple Patterns
        for pattern, op_name in _SIMPLE_PATTERNS:
            if re.search(pattern, prompt_lower):
                matched_ops.append(op_name)
                matched_patterns.append(pattern)

        primary_intent = matched_ops[0] if matched_ops else "filter_and_project"
        elapsed_ms = (time.perf_counter() - t0) * 1000

        return IntentClassification(
            intent=primary_intent,
            complexity=IntentComplexity.SIMPLE,
            confidence=0.98 if matched_ops else 0.85,
            route=RouteTarget.COMPILER_DIRECT,
            reasoning=f"Query is deterministic '{primary_intent}'; routing directly to compiler.",
            detected_operations=matched_ops or ["filter"],
            matched_patterns=matched_patterns,
            latency_ms=round(elapsed_ms, 3),
        )
