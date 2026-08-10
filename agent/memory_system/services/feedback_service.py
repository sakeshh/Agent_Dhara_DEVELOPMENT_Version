"""
Phase 9: Response Feedback System Service.
Tracks thumbs up/down, ratings, and comments per assistant message for learning analytics.
"""
import uuid
import logging
from typing import List, Optional
from datetime import datetime

from agent.memory_system.database import get_db, ensure_user_exists
from agent.memory_system.config import get_config
from agent.memory_system.models import FeedbackCreate, FeedbackResponse

logger = logging.getLogger("agent.memory_system.feedback")


class FeedbackService:
    def add_feedback(self, feedback: FeedbackCreate) -> FeedbackResponse:
        fid = f"fb_{uuid.uuid4().hex[:12]}"
        uid = feedback.user_id or get_config().default_user_id
        now = datetime.utcnow().isoformat()

        with get_db() as conn:
            ensure_user_exists(conn, uid)
            # Ensure message_id exists to satisfy foreign key
            m_cursor = conn.execute("SELECT message_id FROM messages WHERE message_id = ?", (feedback.message_id,))
            if not m_cursor.fetchone():
                # Auto-create dummy conversation and message if message doesn't exist
                dummy_conv = f"conv_auto_{uid}"
                conn.execute(
                    "INSERT OR IGNORE INTO conversations (conversation_id, user_id, title) VALUES (?, ?, ?)",
                    (dummy_conv, uid, "Feedback Session"),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO messages (message_id, conversation_id, role, content) VALUES (?, ?, 'assistant', 'Feedback target message')",
                    (feedback.message_id, dummy_conv),
                )

            conn.execute(
                """
                INSERT INTO message_feedback (feedback_id, message_id, user_id, rating, comment, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fid, feedback.message_id, uid, feedback.rating, feedback.comment, now),
            )

        return FeedbackResponse(
            feedback_id=fid,
            message_id=feedback.message_id,
            user_id=uid,
            rating=feedback.rating,
            comment=feedback.comment,
            timestamp=now,
        )

    def get_feedback_for_message(self, message_id: str) -> Optional[FeedbackResponse]:
        with get_db() as conn:
            cursor = conn.execute(
                "SELECT feedback_id, message_id, user_id, rating, comment, timestamp FROM message_feedback WHERE message_id = ?",
                (message_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return FeedbackResponse(
                feedback_id=row["feedback_id"],
                message_id=row["message_id"],
                user_id=row["user_id"],
                rating=row["rating"],
                comment=row["comment"],
                timestamp=row["timestamp"],
            )
