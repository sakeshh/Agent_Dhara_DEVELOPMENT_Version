"""
Phase 2: User Memory System Service.
Manages long-term user memories across Profile, Preference, Skill, Goal, Project, Interest categories
with similarity deduplication and confidence scoring.
"""
import uuid
import logging
from typing import List, Optional
from datetime import datetime

from agent.memory_system.database import get_db, ensure_user_exists
from agent.memory_system.config import get_config
from agent.memory_system.models import MemoryCreate, MemoryResponse, MemoryTypeEnum
from agent.memory_system.providers.default_embedding import DefaultEmbeddingProvider
from agent.memory_system.providers.local_vector_store import LocalVectorStore, cosine_similarity
from agent.memory_system.interfaces.vector_store import VectorDocument

logger = logging.getLogger("agent.memory_system.memory_service")


class MemoryService:
    def __init__(self, embedding_provider=None, vector_store=None):
        self.embedding_provider = embedding_provider or DefaultEmbeddingProvider()
        self.vector_store = vector_store or LocalVectorStore()

    def add_memory(
        self,
        user_id: str,
        memory_type: MemoryTypeEnum,
        memory_text: str,
        confidence_score: float = 1.0,
        source_conversation_id: Optional[str] = None,
    ) -> MemoryResponse:
        now = datetime.utcnow().isoformat()
        m_type_val = memory_type.value if isinstance(memory_type, MemoryTypeEnum) else memory_type

        # 1. Deduplication check via similarity
        existing_memories = self.get_user_memories(user_id=user_id, memory_type=memory_type)
        new_vec = self.embedding_provider.embed_text(memory_text)

        for mem in existing_memories:
            ex_vec = self.embedding_provider.embed_text(mem.memory_text)
            sim = cosine_similarity(new_vec, ex_vec)
            if sim > 0.85: # High similarity duplicate
                logger.info(f"Duplicate memory detected (sim={sim:.2f}). Updating existing memory: {mem.memory_id}")
                # Update confidence and text if necessary
                return self.update_memory(
                    memory_id=mem.memory_id,
                    memory_text=memory_text,
                    confidence_score=max(mem.confidence_score, confidence_score),
                )

        # 2. Insert new memory
        mem_id = f"mem_{uuid.uuid4().hex[:12]}"
        with get_db() as conn:
            ensure_user_exists(conn, user_id)
            conn.execute(
                """
                INSERT INTO user_memories
                (memory_id, user_id, memory_type, memory_text, confidence_score, source_conversation_id, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (mem_id, user_id, m_type_val, memory_text, confidence_score, source_conversation_id, now, now),
            )

        # 3. Index in vector store
        doc = VectorDocument(
            id=f"vec_mem_{mem_id}",
            user_id=user_id,
            entity_type="memory",
            entity_id=mem_id,
            vector=new_vec,
            content=f"{m_type_val}: {memory_text}",
            metadata={"memory_type": m_type_val, "confidence": confidence_score},
        )
        self.vector_store.upsert([doc])

        return MemoryResponse(
            memory_id=mem_id,
            user_id=user_id,
            memory_type=MemoryTypeEnum(m_type_val),
            memory_text=memory_text,
            confidence_score=confidence_score,
            source_conversation_id=source_conversation_id,
            is_active=True,
            created_at=now,
            updated_at=now,
        )

    def get_user_memories(
        self, user_id: str, memory_type: Optional[MemoryTypeEnum] = None, min_confidence: float = 0.0
    ) -> List[MemoryResponse]:
        with get_db() as conn:
            query = "SELECT memory_id, user_id, memory_type, memory_text, confidence_score, source_conversation_id, is_active, created_at, updated_at FROM user_memories WHERE user_id = ? AND is_active = 1 AND confidence_score >= ?"
            params = [user_id, min_confidence]

            if memory_type:
                m_val = memory_type.value if isinstance(memory_type, MemoryTypeEnum) else memory_type
                query += " AND memory_type = ?"
                params.append(m_val)

            query += " ORDER BY updated_at DESC"
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

            return [
                MemoryResponse(
                    memory_id=r["memory_id"],
                    user_id=r["user_id"],
                    memory_type=MemoryTypeEnum(r["memory_type"]),
                    memory_text=r["memory_text"],
                    confidence_score=r["confidence_score"],
                    source_conversation_id=r["source_conversation_id"],
                    is_active=bool(r["is_active"]),
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                )
                for r in rows
            ]

    def update_memory(
        self, memory_id: str, memory_text: Optional[str] = None, confidence_score: Optional[float] = None
    ) -> MemoryResponse:
        now = datetime.utcnow().isoformat()
        updates = []
        params = []

        if memory_text is not None:
            updates.append("memory_text = ?")
            params.append(memory_text)
        if confidence_score is not None:
            updates.append("confidence_score = ?")
            params.append(confidence_score)

        updates.append("updated_at = ?")
        params.append(now)
        params.append(memory_id)

        with get_db() as conn:
            conn.execute(
                f"UPDATE user_memories SET {', '.join(updates)} WHERE memory_id = ?",
                params,
            )

            cursor = conn.execute("SELECT * FROM user_memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()

        # Re-index in vector store if text updated
        if memory_text is not None and row:
            vec = self.embedding_provider.embed_text(row["memory_text"])
            doc = VectorDocument(
                id=f"vec_mem_{memory_id}",
                user_id=row["user_id"],
                entity_type="memory",
                entity_id=memory_id,
                vector=vec,
                content=f"{row['memory_type']}: {row['memory_text']}",
                metadata={"memory_type": row["memory_type"], "confidence": row["confidence_score"]},
            )
            self.vector_store.upsert([doc])

        return MemoryResponse(
            memory_id=row["memory_id"],
            user_id=row["user_id"],
            memory_type=MemoryTypeEnum(row["memory_type"]),
            memory_text=row["memory_text"],
            confidence_score=row["confidence_score"],
            source_conversation_id=row["source_conversation_id"],
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def delete_memory(self, memory_id: str) -> bool:
        with get_db() as conn:
            conn.execute("UPDATE user_memories SET is_active = 0 WHERE memory_id = ?", (memory_id,))
        self.vector_store.delete([f"vec_mem_{memory_id}"])
        return True
