from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.ir.models import IrPlan
from agent.rag.models import RagEntityType, RagItem
from agent.rag.store import RagStore, get_rag_store

logger = logging.getLogger("agent.rag.retriever")


class RagContext(BaseModel):
    business_rules: List[str] = Field(default_factory=list)
    contracts: List[Dict[str, Any]] = Field(default_factory=list)
    matched_pattern: Optional[IrPlan] = None
    formatted_prompt_snippet: str = ""
    retrieval_latency_ms: float = 0.0


class RagRetriever:
    """
    Retrieves grounded domain business rules, contracts, and prior successful IR plans
    to enrich and constrain LLM planning before code generation.
    """

    def __init__(self, store: Optional[RagStore] = None) -> None:
        self.store = store or get_rag_store()

    def retrieve_context(
        self,
        user_prompt: str,
        table_names: Optional[List[str]] = None,
        min_score: float = 0.40,
    ) -> RagContext:
        t0 = time.perf_counter()

        # 1. Search for relevant Business Rules & Standards
        rule_results = self.store.search(
            user_prompt,
            entity_types=[RagEntityType.BUSINESS_RULE, RagEntityType.DQ_RULE, RagEntityType.COMPANY_STANDARD],
            limit=4,
            min_score=min_score,
        )

        # 2. Search for Data Contracts
        contract_query = f"{user_prompt} {' '.join(table_names or [])}"
        contract_results = self.store.search(
            contract_query,
            entity_types=[RagEntityType.DATA_CONTRACT],
            limit=3,
            min_score=min_score,
        )

        # 3. Retrieve Historical IR Pattern Memory
        matched_ir = self.store.get_top_ir_pattern(user_prompt, min_score=0.80)

        # 4. Format Prompt Snippet
        snippet_lines: List[str] = []
        rules_text: List[str] = []
        if rule_results:
            snippet_lines.append("### APPLICABLE BUSINESS RULES & CONSTRAINTS:")
            for r in rule_results:
                line = f"- [{r.item.title}]: {r.item.content}"
                snippet_lines.append(line)
                rules_text.append(r.item.content)

        contracts_data: List[Dict[str, Any]] = []
        if contract_results:
            snippet_lines.append("### DATA CONTRACTS:")
            for c in contract_results:
                snippet_lines.append(f"- [{c.item.title}]: {c.item.content}")
                contracts_data.append(c.item.metadata.get("contract_spec", {}))

        if matched_ir:
            snippet_lines.append(f"### HISTORICAL SUCCESSFUL PATTERN:")
            snippet_lines.append(f"- Reusable structure found with intent '{matched_ir.intent.type}'")

        formatted_snippet = "\n".join(snippet_lines)
        latency = (time.perf_counter() - t0) * 1000

        return RagContext(
            business_rules=rules_text,
            contracts=contracts_data,
            matched_pattern=matched_ir,
            formatted_prompt_snippet=formatted_snippet,
            retrieval_latency_ms=round(latency, 2),
        )
