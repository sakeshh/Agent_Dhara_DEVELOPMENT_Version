"""
Local Vector Store Implementation backed by SQLite with Cosine Distance.
Includes pluggable provider hooks for Qdrant, Pinecone, Weaviate, and Azure AI Search.
"""
import json
import math
import logging
from typing import Any, Dict, List, Optional

from agent.memory_system.database import get_db
from agent.memory_system.interfaces.vector_store import (
    VectorStoreInterface,
    VectorDocument,
    VectorSearchResult,
)

logger = logging.getLogger("agent.memory_system.vector_store")


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    norm_a = math.sqrt(sum(a * a for a in v1))
    norm_b = math.sqrt(sum(b * b for b in v2))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class LocalVectorStore(VectorStoreInterface):
    def upsert(self, documents: List[VectorDocument]) -> bool:
        if not documents:
            return True
        try:
            with get_db() as conn:
                for doc in documents:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO text_embeddings
                        (embedding_id, entity_type, entity_id, user_id, vector_json, content_chunk, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            doc.id,
                            doc.entity_type,
                            doc.entity_id,
                            doc.user_id,
                            json.dumps(doc.vector),
                            doc.content,
                        ),
                    )
            return True
        except Exception as e:
            logger.error(f"Error in LocalVectorStore.upsert: {e}")
            return False

    def search(
        self,
        query_vector: List[float],
        user_id: str,
        entity_types: Optional[List[str]] = None,
        limit: int = 10,
        score_threshold: float = 0.0,
    ) -> List[VectorSearchResult]:
        results: List[VectorSearchResult] = []
        try:
            with get_db() as conn:
                query = "SELECT embedding_id, entity_type, entity_id, user_id, vector_json, content_chunk FROM text_embeddings WHERE user_id = ?"
                params: List[Any] = [user_id]

                if entity_types:
                    placeholders = ",".join("?" for _ in entity_types)
                    query += f" AND entity_type IN ({placeholders})"
                    params.extend(entity_types)

                cursor = conn.execute(query, params)
                rows = cursor.fetchall()

                scored_docs = []
                for row in rows:
                    vec = json.loads(row["vector_json"])
                    score = cosine_similarity(query_vector, vec)
                    if score >= score_threshold:
                        doc = VectorDocument(
                            id=row["embedding_id"],
                            user_id=row["user_id"],
                            entity_type=row["entity_type"],
                            entity_id=row["entity_id"],
                            vector=vec,
                            content=row["content_chunk"],
                        )
                        scored_docs.append(VectorSearchResult(document=doc, score=score))

                scored_docs.sort(key=lambda x: x.score, reverse=True)
                return scored_docs[:limit]

        except Exception as e:
            logger.error(f"Error in LocalVectorStore.search: {e}")
            return []

    def delete(self, document_ids: List[str]) -> bool:
        if not document_ids:
            return True
        try:
            with get_db() as conn:
                placeholders = ",".join("?" for _ in document_ids)
                conn.execute(
                    f"DELETE FROM text_embeddings WHERE embedding_id IN ({placeholders})",
                    document_ids,
                )
            return True
        except Exception as e:
            logger.error(f"Error in LocalVectorStore.delete: {e}")
            return False
