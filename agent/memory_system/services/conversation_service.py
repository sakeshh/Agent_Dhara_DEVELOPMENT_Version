"""
Phase 1: Conversation Management Service.
Handles multi-session conversations, soft deletes, message persistence, and pagination.
"""
import uuid
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from agent.memory_system.database import get_db, ensure_user_exists
from agent.memory_system.config import get_config
from agent.memory_system.models import (
    ConversationResponse,
    MessageResponse,
    PaginatedMessagesResponse,
    RoleEnum,
)

logger = logging.getLogger("agent.memory_system.conversation_service")


class ConversationService:
    def create_conversation(self, user_id: Optional[str] = None, title: str = "New Chat") -> ConversationResponse:
        uid = user_id or get_config().default_user_id
        conv_id = f"conv_{uuid.uuid4().hex[:12]}"
        now = datetime.utcnow().isoformat()

        with get_db() as conn:
            ensure_user_exists(conn, uid)
            conn.execute(
                """
                INSERT INTO conversations (conversation_id, user_id, title, status, is_deleted, created_at, updated_at)
                VALUES (?, ?, ?, 'active', 0, ?, ?)
                """,
                (conv_id, uid, title, now, now),
            )

        return self.get_conversation(conv_id)

    def get_conversation(self, conversation_id: str) -> Optional[ConversationResponse]:
        with get_db() as conn:
            cursor = conn.execute(
                """
                SELECT c.conversation_id, c.user_id, c.title, c.status, c.is_deleted, c.created_at, c.updated_at,
                       COUNT(m.message_id) as message_count
                FROM conversations c
                LEFT JOIN messages m ON c.conversation_id = m.conversation_id
                WHERE c.conversation_id = ?
                GROUP BY c.conversation_id
                """,
                (conversation_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            return ConversationResponse(
                conversation_id=row["conversation_id"],
                user_id=row["user_id"],
                title=row["title"],
                status=row["status"],
                is_deleted=bool(row["is_deleted"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                message_count=row["message_count"],
            )

    def update_conversation(self, conversation_id: str, title: Optional[str] = None, status: Optional[str] = None) -> Optional[ConversationResponse]:
        now = datetime.utcnow().isoformat()
        updates = []
        params = []

        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if status is not None:
            updates.append("status = ?")
            params.append(status)

        if not updates:
            return self.get_conversation(conversation_id)

        updates.append("updated_at = ?")
        params.append(now)
        params.append(conversation_id)

        with get_db() as conn:
            conn.execute(
                f"UPDATE conversations SET {', '.join(updates)} WHERE conversation_id = ?",
                params,
            )

        return self.get_conversation(conversation_id)

    def soft_delete_conversation(self, conversation_id: str) -> bool:
        now = datetime.utcnow().isoformat()
        with get_db() as conn:
            conn.execute(
                "UPDATE conversations SET is_deleted = 1, updated_at = ? WHERE conversation_id = ?",
                (now, conversation_id),
            )
        return True

    def restore_conversation(self, conversation_id: str) -> bool:
        now = datetime.utcnow().isoformat()
        with get_db() as conn:
            conn.execute(
                "UPDATE conversations SET is_deleted = 0, updated_at = ? WHERE conversation_id = ?",
                (now, conversation_id),
            )
        return True

    def list_user_conversations(
        self, user_id: Optional[str] = None, include_deleted: bool = False, limit: int = 50, offset: int = 0
    ) -> List[ConversationResponse]:
        uid = user_id or get_config().default_user_id
        with get_db() as conn:
            query = """
                SELECT c.conversation_id, c.user_id, c.title, c.status, c.is_deleted, c.created_at, c.updated_at,
                       COUNT(m.message_id) as message_count
                FROM conversations c
                LEFT JOIN messages m ON c.conversation_id = m.conversation_id
                WHERE c.user_id = ?
            """
            params: List[Any] = [uid]

            if not include_deleted:
                query += " AND c.is_deleted = 0"

            query += " GROUP BY c.conversation_id ORDER BY c.updated_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

            return [
                ConversationResponse(
                    conversation_id=r["conversation_id"],
                    user_id=r["user_id"],
                    title=r["title"],
                    status=r["status"],
                    is_deleted=bool(r["is_deleted"]),
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                    message_count=r["message_count"],
                )
                for r in rows
            ]

    # -------------------------------------------------------------------
    # Message Persistence
    # -------------------------------------------------------------------

    def add_message(
        self,
        conversation_id: str,
        role: RoleEnum,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MessageResponse:
        msg_id = f"msg_{uuid.uuid4().hex[:12]}"
        now = datetime.utcnow().isoformat()
        meta_str = json.dumps(metadata or {})

        with get_db() as conn:
            # Check conversation exists
            cursor = conn.execute("SELECT user_id, title FROM conversations WHERE conversation_id = ?", (conversation_id,))
            conv = cursor.fetchone()
            if not conv:
                # Auto-create if not exists
                uid = get_config().default_user_id
                ensure_user_exists(conn, uid)
                conn.execute(
                    "INSERT INTO conversations (conversation_id, user_id, title, status, is_deleted, created_at, updated_at) VALUES (?, ?, ?, 'active', 0, ?, ?)",
                    (conversation_id, uid, "New Chat", now, now),
                )

            # Insert message
            conn.execute(
                """
                INSERT INTO messages (message_id, conversation_id, role, content, metadata_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (msg_id, conversation_id, role.value if isinstance(role, RoleEnum) else role, content, meta_str, now),
            )

            # Update conversation timestamp & auto-title if first user message
            if role in (RoleEnum.USER, "user"):
                cursor = conn.execute("SELECT COUNT(*) as count FROM messages WHERE conversation_id = ?", (conversation_id,))
                cnt = cursor.fetchone()["count"]
                if cnt <= 2 and (not conv or conv["title"] in ("New Chat", "Untitled Chat")):
                    # Auto title from first message
                    auto_title = content[:30].strip() + ("..." if len(content) > 30 else "")
                    conn.execute(
                        "UPDATE conversations SET title = ?, updated_at = ? WHERE conversation_id = ?",
                        (auto_title, now, conversation_id),
                    )
                else:
                    conn.execute(
                        "UPDATE conversations SET updated_at = ? WHERE conversation_id = ?",
                        (now, conversation_id),
                    )

        return MessageResponse(
            message_id=msg_id,
            conversation_id=conversation_id,
            role=RoleEnum(role),
            content=content,
            metadata=metadata or {},
            timestamp=now,
        )

    def get_messages(
        self, conversation_id: str, limit: int = 50, offset: int = 0
    ) -> PaginatedMessagesResponse:
        with get_db() as conn:
            count_cursor = conn.execute(
                "SELECT COUNT(*) as count FROM messages WHERE conversation_id = ?",
                (conversation_id,),
            )
            total_count = count_cursor.fetchone()["count"]

            cursor = conn.execute(
                """
                SELECT message_id, conversation_id, role, content, metadata_json, timestamp
                FROM messages
                WHERE conversation_id = ?
                ORDER BY timestamp ASC
                LIMIT ? OFFSET ?
                """,
                (conversation_id, limit, offset),
            )
            rows = cursor.fetchall()

            messages = [
                MessageResponse(
                    message_id=r["message_id"],
                    conversation_id=r["conversation_id"],
                    role=RoleEnum(r["role"]),
                    content=r["content"],
                    metadata=json.loads(r["metadata_json"] or "{}"),
                    timestamp=r["timestamp"],
                )
                for r in rows
            ]

            has_more = (offset + len(messages)) < total_count

            return PaginatedMessagesResponse(
                messages=messages,
                total_count=total_count,
                offset=offset,
                limit=limit,
                has_more=has_more,
            )
