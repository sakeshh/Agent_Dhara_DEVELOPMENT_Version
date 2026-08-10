"""
Phase 8: Hybrid Chat Search Service.
Combines keyword matching and semantic vector search across messages, conversations, summaries, and memories.
"""
import logging
from typing import List, Optional

from agent.memory_system.database import get_db
from agent.memory_system.models import SearchQueryRequest, SearchResultItem
from agent.memory_system.services.vector_search import VectorSearchService

logger = logging.getLogger("agent.memory_system.chat_search")


class ChatSearchService:
    def __init__(self, vector_search_service: Optional[VectorSearchService] = None):
        self.vector_search = vector_search_service or VectorSearchService()

    def search(self, request: SearchQueryRequest) -> List[SearchResultItem]:
        user_id = request.user_id or "default_user"
        query_text = request.query.strip()
        search_mode = request.search_type.lower()
        results: List[SearchResultItem] = []

        if not query_text:
            return []

        # 1. Keyword Search (SQL LIKE matching across conversations and messages)
        if search_mode in ("keyword", "hybrid"):
            with get_db() as conn:
                # Search Messages
                m_cursor = conn.execute(
                    """
                    SELECT m.message_id, m.conversation_id, m.role, m.content, m.timestamp, c.title
                    FROM messages m
                    JOIN conversations c ON m.conversation_id = c.conversation_id
                    WHERE c.user_id = ? AND c.is_deleted = 0 AND m.content LIKE ?
                    ORDER BY m.timestamp DESC
                    LIMIT ?
                    """,
                    (user_id, f"%{query_text}%", request.limit),
                )
                for r in m_cursor.fetchall():
                    results.append(
                        SearchResultItem(
                            result_type="message",
                            id=r["message_id"],
                            conversation_id=r["conversation_id"],
                            title=r["title"],
                            content=r["content"],
                            score=0.85, # Base keyword match score
                            timestamp=r["timestamp"],
                        )
                    )

                # Search Conversations by Title
                c_cursor = conn.execute(
                    """
                    SELECT conversation_id, title, created_at, updated_at
                    FROM conversations
                    WHERE user_id = ? AND is_deleted = 0 AND title LIKE ?
                    LIMIT ?
                    """,
                    (user_id, f"%{query_text}%", request.limit),
                )
                for r in c_cursor.fetchall():
                    results.append(
                        SearchResultItem(
                            result_type="conversation",
                            id=r["conversation_id"],
                            conversation_id=r["conversation_id"],
                            title=r["title"],
                            content=f"Conversation Title: {r['title']}",
                            score=0.90,
                            timestamp=r["updated_at"],
                        )
                    )

        # 2. Semantic Search (Vector distance)
        if search_mode in ("semantic", "hybrid"):
            vec_results = self.vector_search.search_semantic(
                user_id=user_id,
                query=query_text,
                limit=request.limit,
                score_threshold=0.15,
            )
            for v_res in vec_results:
                doc = v_res.document
                # Avoid duplicate IDs
                if not any(r.id == doc.entity_id for r in results):
                    results.append(
                        SearchResultItem(
                            result_type=doc.entity_type,
                            id=doc.entity_id,
                            conversation_id=doc.metadata.get("conversation_id"),
                            title=doc.metadata.get("title"),
                            content=doc.content,
                            score=round(v_res.score, 4),
                            timestamp=doc.metadata.get("timestamp"),
                        )
                    )

        # Sort combined results by score descending
        results.sort(key=lambda x: x.score, reverse=True)
        return results[: request.limit]
