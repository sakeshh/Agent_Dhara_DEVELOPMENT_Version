"""
Phase 6: Vector Search & Retrieval Service.
Abstracted vector search pipeline for indexing and searching across messages, summaries, and memories.
"""
import logging
from typing import List, Optional

from agent.memory_system.interfaces.vector_store import (
    VectorStoreInterface,
    VectorDocument,
    VectorSearchResult,
)
from agent.memory_system.interfaces.embedding_provider import EmbeddingProviderInterface
from agent.memory_system.providers.default_embedding import DefaultEmbeddingProvider
from agent.memory_system.providers.local_vector_store import LocalVectorStore

logger = logging.getLogger("agent.memory_system.vector_search")


class VectorSearchService:
    def __init__(
        self,
        vector_store: Optional[VectorStoreInterface] = None,
        embedding_provider: Optional[EmbeddingProviderInterface] = None,
    ):
        self.vector_store = vector_store or LocalVectorStore()
        self.embedding_provider = embedding_provider or DefaultEmbeddingProvider()

    def index_text(
        self,
        user_id: str,
        entity_type: str,
        entity_id: str,
        content: str,
        metadata: Optional[dict] = None,
    ) -> bool:
        if not content or not content.strip():
            return False

        vector = self.embedding_provider.embed_text(content)
        doc = VectorDocument(
            id=f"vec_{entity_type}_{entity_id}",
            user_id=user_id,
            entity_type=entity_type,
            entity_id=entity_id,
            vector=vector,
            content=content,
            metadata=metadata or {},
        )
        return self.vector_store.upsert([doc])

    def search_semantic(
        self,
        user_id: str,
        query: str,
        entity_types: Optional[List[str]] = None,
        limit: int = 10,
        score_threshold: float = 0.0,
    ) -> List[VectorSearchResult]:
        if not query or not query.strip():
            return []

        query_vector = self.embedding_provider.embed_text(query)
        return self.vector_store.search(
            query_vector=query_vector,
            user_id=user_id,
            entity_types=entity_types,
            limit=limit,
            score_threshold=score_threshold,
        )
