from __future__ import annotations

import difflib
import logging
import threading
import time
from typing import Any, Dict, List, Optional
from agent.ir.models import IrPlan
from agent.memory_system.providers.default_embedding import DefaultEmbeddingProvider
from agent.memory_system.providers.local_vector_store import cosine_similarity
from agent.rag.models import RagEntityType, RagItem, RagSearchResult

logger = logging.getLogger("agent.rag.store")


class RagStore:
    """
    In-memory hybrid RAG Knowledge Store.
    Indexes business rules, data contracts, company standards, and historical IR patterns
    using dense vector embeddings combined with lexical keyword boost.
    """

    def __init__(self, embedding_provider: Optional[DefaultEmbeddingProvider] = None) -> None:
        self.embedding_provider = embedding_provider or DefaultEmbeddingProvider(dimension=256)
        self._items: Dict[str, RagItem] = {}
        self._lock = threading.RLock()

    def upsert(self, item: RagItem) -> None:
        with self._lock:
            if not item.vector:
                item.vector = self.embedding_provider.embed_text(f"{item.title} {item.content} {' '.join(item.tags)}")
            self._items[item.id] = item

    def add_business_rule(
        self,
        rule_id: str,
        title: str,
        rule_text: str,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RagItem:
        item = RagItem(
            id=rule_id,
            entity_type=RagEntityType.BUSINESS_RULE,
            title=title,
            content=rule_text,
            tags=tags or [],
            metadata=metadata or {},
        )
        self.upsert(item)
        return item

    def add_data_contract(
        self,
        contract_id: str,
        table_name: str,
        contract_spec: Dict[str, Any],
        tags: Optional[List[str]] = None,
    ) -> RagItem:
        content = f"Data Contract for {table_name}: " + " ".join(f"{k}:{v}" for k, v in contract_spec.items())
        item = RagItem(
            id=contract_id,
            entity_type=RagEntityType.DATA_CONTRACT,
            title=f"Contract: {table_name}",
            content=content,
            tags=tags or [table_name],
            metadata={"table_name": table_name, "contract_spec": contract_spec},
        )
        self.upsert(item)
        return item

    def add_ir_pattern(
        self,
        pattern_id: str,
        user_prompt: str,
        ir_plan: IrPlan,
        target_engine: str = "sql",
        success_score: float = 1.0,
        usage_count: int = 1,
        execution_success: float = 1.0,
        user_rating: float = 5.0,
    ) -> RagItem:
        content = f"Prompt: {user_prompt}\nOperations: {[s.step_type for s in ir_plan.logical_plan]}"
        item = RagItem(
            id=pattern_id,
            entity_type=RagEntityType.IR_PATTERN,
            title=f"IR Pattern: {user_prompt[:50]}",
            content=content,
            tags=ir_plan.get_source_tables() + [ir_plan.intent.type],
            metadata={
                "user_prompt": user_prompt,
                "ir_plan": ir_plan.model_dump(),
                "target_engine": target_engine,
                "success_score": success_score,
                "usage_count": usage_count,
                "execution_success": execution_success,
                "user_rating": user_rating,
            },
        )
        self.upsert(item)
        return item

    def search(
        self,
        query: str,
        entity_types: Optional[List[RagEntityType]] = None,
        limit: int = 5,
        min_score: float = 0.40,
    ) -> List[RagSearchResult]:
        if not query or not query.strip():
            return []

        query_vec = self.embedding_provider.embed_text(query)
        query_words = set(query.lower().split())

        results: List[RagSearchResult] = []
        with self._lock:
            for item in self._items.values():
                if entity_types and item.entity_type not in entity_types:
                    continue

                # 1. Cosine vector similarity
                v_score = cosine_similarity(query_vec, item.vector or [])

                # 2. Lexical keyword overlap score
                item_words = set((item.title + " " + item.content + " " + " ".join(item.tags)).lower().split())
                overlap = len(query_words.intersection(item_words)) / max(len(query_words), 1)

                # Hybrid score: 65% semantic vector + 35% lexical keyword boost
                hybrid_score = round(0.65 * v_score + 0.35 * overlap, 3)

                if hybrid_score >= min_score:
                    results.append(
                        RagSearchResult(
                            item=item,
                            score=hybrid_score,
                            relevance_reason=f"Vector sim: {v_score:.2f}, lexical match: {overlap:.2f}",
                        )
                    )

        results.sort(key=lambda r: r.score, reverse=True)
        return results[:limit]

    def get_top_ir_pattern(self, query: str, min_score: float = 0.70) -> Optional[IrPlan]:
        matches = self.search(query, entity_types=[RagEntityType.IR_PATTERN], limit=5, min_score=min_score)
        if not matches:
            return None

        # Composite Pattern Quality Ranking:
        # 40% similarity match + 25% historical success rate + 20% execution success + 10% user rating + 5% recency
        def pattern_rank(res: RagSearchResult) -> float:
            meta = res.item.metadata
            s_rate = float(meta.get("success_score", 1.0))
            e_rate = float(meta.get("execution_success", 1.0))
            u_rating = float(meta.get("user_rating", 5.0)) / 5.0

            # Recency decay: patterns created within 30 days get full 1.0, decaying over 365 days
            age_days = (time.time() - res.item.created_at) / 86400.0
            recency = max(0.0, 1.0 - (age_days / 365.0))

            return (res.score * 0.40) + (s_rate * 0.25) + (e_rate * 0.20) + (u_rating * 0.10) + (recency * 0.05)

        matches.sort(key=pattern_rank, reverse=True)
        top_plan_dict = matches[0].item.metadata.get("ir_plan")
        if top_plan_dict:
            return IrPlan.model_validate(top_plan_dict)
        return None


# Global Singleton
_GLOBAL_RAG_STORE = RagStore()


def get_rag_store() -> RagStore:
    return _GLOBAL_RAG_STORE
