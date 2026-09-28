from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from agent.compiler.models.profile import DatasetProfile
from agent.ir.models import (
    FilterNode,
    IntentComplexity,
    IntentSpec,
    IrMetadata,
    IrPlan,
    IrStep,
    LineageSpec,
    ProjectionNode,
    SourceNode,
    ValidationSpec,
)
from agent.ir.rule_expression import RuleExpression, RuleType
from agent.model_config import load_llm_config
from agent.schema_cache import get_schema_cache

logger = logging.getLogger("agent.ir.extractor")

SYSTEM_EXTRACTOR_PROMPT = """You are the Intent-to-IR Extraction Engine for Agent Dhara.
Your task is to translate the user's natural language ETL request into a strictly structured Intermediate Representation (IrPlan).

CRITICAL INVARIANTS:
1. NEVER output executable SQL (no SELECT, DROP, ALTER, UPDATE).
2. NEVER output Python, Pandas, or PySpark code (no df.filter, df.dropna, eval).
3. ONLY produce structured IrPlan attributes matching the provided schema.
4. Ground all table and column names strictly in the provided Available Schemas.
"""


class IrExtractor:
    """
    Translates Natural Language user requirements into a typed IrPlan.
    Enforces structured output, schema grounding, and strict prevention of raw code generation.
    """

    @classmethod
    def _select_relevant_tables(
        cls,
        user_prompt: str,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Dict[str, DatasetProfile]:
        """Grounding: filters available tables to those relevant to the user query."""
        if len(dataset_profiles) <= 3:
            return dataset_profiles

        prompt_lower = user_prompt.lower()
        selected: Dict[str, DatasetProfile] = {}
        for tbl_name, prof in dataset_profiles.items():
            tbl_lower = tbl_name.lower()
            if tbl_lower in prompt_lower or any(word in prompt_lower for word in tbl_lower.split("_")):
                selected[tbl_name] = prof
                continue
            # Check column names
            for col_name in prof.columns.keys():
                if col_name.lower() in prompt_lower:
                    selected[tbl_name] = prof
                    break

        return selected if selected else dataset_profiles

    @classmethod
    def _get_llm_client(cls):
        cfg = load_llm_config(purpose="general")
        if not cfg:
            return None
        try:
            if cfg.provider == "azure_openai":
                from langchain_openai import AzureChatOpenAI

                return AzureChatOpenAI(
                    azure_endpoint=cfg.endpoint,
                    azure_deployment=cfg.model,
                    api_version=cfg.api_version or "2024-02-01",
                    api_key=cfg.api_key,
                    temperature=0.0,
                    max_tokens=1500,
                )
            else:
                from langchain_openai import ChatOpenAI

                return ChatOpenAI(
                    api_key=cfg.api_key,
                    model=cfg.model,
                    temperature=0.0,
                    max_tokens=1500,
                )
        except Exception as exc:
            logger.warning(f"Could not initialize LLM client for IR extraction: {exc}")
            return None

    @classmethod
    def _heuristic_fallback_extract(
        cls,
        user_prompt: str,
        relevant_profiles: Dict[str, DatasetProfile],
        session_id: str,
    ) -> IrPlan:
        """
        Deterministic parser for simple expressions and offline/test execution.
        Handles filter, projection, and deduplication patterns.
        """
        primary_table = list(relevant_profiles.keys())[0] if relevant_profiles else "SourceTable"
        profile = relevant_profiles.get(primary_table)
        avail_cols = list(profile.columns.keys()) if profile else []

        sources = [
            SourceNode(
                table=primary_table,
                columns=avail_cols,
            )
        ]

        steps: List[IrStep] = []
        rules: List[RuleExpression] = []

        # Check for deduplication
        if any(term in user_prompt.lower() for term in ("dedup", "duplicate", "distinct", "unique")):
            steps.append(
                IrStep(
                    step_id="step_dedup",
                    step_type="deduplicate",
                    description="Deduplicate records",
                )
            )

        # Check for comparison / filters: column > val, column = val
        for col in avail_cols:
            pattern = re.compile(rf"{re.escape(col)}\s*(>|>=|<|<=|=|==|!=)\s*(\d+|'[^']*'|\w+)", re.IGNORECASE)
            match = pattern.search(user_prompt)
            if match:
                op, val = match.groups()
                # Clean numeric values
                if val.isdigit():
                    clean_val = int(val)
                else:
                    try:
                        clean_val = float(val)
                    except ValueError:
                        clean_val = val.strip("'\"")

                steps.append(
                    IrStep(
                        step_id=f"step_filter_{col}",
                        step_type="filter",
                        filter_node=FilterNode(
                            table=primary_table,
                            column=col,
                            operator=op,
                            value=clean_val,
                        ),
                        description=f"Filter {col} {op} {val}",
                    )
                )

        return IrPlan(
            ir_version="1.0",
            intent=IntentSpec(
                type="analytics",
                complexity=IntentComplexity.SIMPLE,
                confidence=0.95,
                description=user_prompt,
            ),
            sources=sources,
            logical_plan=steps,
            lineage=LineageSpec(
                source_tables=[primary_table],
                target_tables=[f"{primary_table}_Cleaned"],
            ),
            validation=ValidationSpec(strict_schema=True),
            business_rules=rules,
            metadata=IrMetadata(
                session_id=session_id,
                planner_model="heuristic_fallback",
            ),
        )

    @classmethod
    def extract(
        cls,
        user_prompt: str,
        dataset_profiles: Dict[str, DatasetProfile],
        session_id: str = "default",
    ) -> IrPlan:
        """
        Extracts an IrPlan from the user prompt using schema grounding and structured output.
        """
        relevant_profiles = cls._select_relevant_tables(user_prompt, dataset_profiles)

        # 1. RAG Grounding & Pattern Memory
        from agent.rag import RagRetriever
        retriever = RagRetriever()
        rag_context = retriever.retrieve_context(user_prompt, list(relevant_profiles.keys()))
        logger.info(
            "[RAG Retriever] retrieved %d business rules, %d data contracts",
            len(rag_context.business_rules),
            len(rag_context.contracts),
        )

        # If a highly confident historical IR pattern matches, reuse it
        if rag_context.matched_pattern:
            logger.info("[IrExtractor] Reusing historical IR pattern from memory for prompt: '%s'", user_prompt[:50])
            reused_plan = IrPlan.model_validate(rag_context.matched_pattern.model_dump())
            reused_plan.metadata.session_id = session_id
            reused_plan.metadata.planner_model = "rag_pattern_memory"
            return reused_plan

        llm = cls._get_llm_client()
        cfg = load_llm_config(purpose="general")
        model_name = getattr(cfg, "model", None) or getattr(cfg, "deployment_name", None) or "gpt-4o-mini"
        logger.info("[IrExtractor] planner_model=%s", model_name)

        if llm is None:
            logger.info("Using deterministic fallback extractor (no active LLM client)")
            plan = cls._heuristic_fallback_extract(user_prompt, relevant_profiles, session_id)
            if rag_context.business_rules:
                plan.metadata.tags.append("rag_grounded")
            return plan

        try:
            # Build schema context string
            schema_lines = []
            for t_name, p in relevant_profiles.items():
                cols_str = ", ".join(f"{c}({p.columns[c].dtype})" for c in p.columns.keys())
                schema_lines.append(f"- Table: `{t_name}` (Columns: {cols_str})")
            schema_context = "\n".join(schema_lines)

            rag_section = f"\n\n{rag_context.formatted_prompt_snippet}" if rag_context.formatted_prompt_snippet else ""

            prompt = (
                f"{SYSTEM_EXTRACTOR_PROMPT}\n\n"
                f"### AVAILABLE SCHEMAS:\n{schema_context}"
                f"{rag_section}\n\n"
                f"### USER REQUEST:\n{user_prompt}\n\n"
                f"Generate the exact IrPlan JSON adhering strictly to available schema columns and business rules."
            )

            structured_llm = llm.with_structured_output(IrPlan, method="function_calling")
            response = structured_llm.invoke(prompt)

            if isinstance(response, IrPlan):
                response.metadata.session_id = session_id
                response.metadata.planner_model = "llm_structured"
                return response

            if isinstance(response, dict):
                plan = IrPlan.model_validate(response)
                plan.metadata.session_id = session_id
                plan.metadata.planner_model = "llm_structured"
                return plan

        except Exception as exc:
            logger.warning(f"Structured LLM extraction failed: {exc}; falling back to heuristic extractor")

        return cls._heuristic_fallback_extract(user_prompt, relevant_profiles, session_id)
