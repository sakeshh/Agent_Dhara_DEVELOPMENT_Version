"""
Phase 7: User Preferences Service.
Manages user-defined chat preferences including language, tone, code style, depth, and custom instructions.
"""
import logging
from typing import Optional
from datetime import datetime

from agent.memory_system.database import get_db, ensure_user_exists
from agent.memory_system.config import get_config
from agent.memory_system.models import UserPreferenceResponse, UserPreferenceUpdate

logger = logging.getLogger("agent.memory_system.preference_service")


class UserPreferenceService:
    def get_preferences(self, user_id: Optional[str] = None) -> UserPreferenceResponse:
        uid = user_id or get_config().default_user_id
        with get_db() as conn:
            cursor = conn.execute(
                """
                SELECT preference_id, user_id, preferred_language, preferred_coding_language,
                       preferred_response_length, preferred_tone, preferred_depth, custom_instructions, updated_at
                FROM user_preferences
                WHERE user_id = ?
                """,
                (uid,),
            )
            row = cursor.fetchone()

            if not row:
                # Initialize default
                pref_id = f"pref_{uid}"
                now = datetime.utcnow().isoformat()
                ensure_user_exists(conn, uid)
                conn.execute(
                    "INSERT INTO user_preferences (preference_id, user_id, updated_at) VALUES (?, ?, ?)",
                    (pref_id, uid, now),
                )
                return UserPreferenceResponse(
                    preference_id=pref_id,
                    user_id=uid,
                    updated_at=now,
                )

            return UserPreferenceResponse(
                preference_id=row["preference_id"],
                user_id=row["user_id"],
                preferred_language=row["preferred_language"] or "English",
                preferred_coding_language=row["preferred_coding_language"] or "Python",
                preferred_response_length=row["preferred_response_length"] or "detailed",
                preferred_tone=row["preferred_tone"] or "professional",
                preferred_depth=row["preferred_depth"] or "deep_dive",
                custom_instructions=row["custom_instructions"] or "",
                updated_at=row["updated_at"],
            )

    def update_preferences(
        self, user_id: Optional[str], update_data: UserPreferenceUpdate
    ) -> UserPreferenceResponse:
        uid = user_id or get_config().default_user_id
        current = self.get_preferences(uid)
        now = datetime.utcnow().isoformat()

        new_lang = update_data.preferred_language if update_data.preferred_language is not None else current.preferred_language
        new_code_lang = update_data.preferred_coding_language if update_data.preferred_coding_language is not None else current.preferred_coding_language
        new_length = update_data.preferred_response_length if update_data.preferred_response_length is not None else current.preferred_response_length
        new_tone = update_data.preferred_tone if update_data.preferred_tone is not None else current.preferred_tone
        new_depth = update_data.preferred_depth if update_data.preferred_depth is not None else current.preferred_depth
        new_custom = update_data.custom_instructions if update_data.custom_instructions is not None else current.custom_instructions

        with get_db() as conn:
            conn.execute(
                """
                UPDATE user_preferences
                SET preferred_language = ?,
                    preferred_coding_language = ?,
                    preferred_response_length = ?,
                    preferred_tone = ?,
                    preferred_depth = ?,
                    custom_instructions = ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (new_lang, new_code_lang, new_length, new_tone, new_depth, new_custom, now, uid),
            )

        return self.get_preferences(uid)
