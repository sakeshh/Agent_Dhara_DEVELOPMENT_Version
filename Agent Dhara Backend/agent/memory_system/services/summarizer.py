"""
Phase 5: Conversation Summarization Engine.
Generates versioned summaries for long conversations when message count exceeds threshold N.
"""
import logging
from typing import Optional, Tuple
from datetime import datetime

from agent.memory_system.database import get_db
from agent.memory_system.config import get_config
from agent.memory_system.models import ConversationSummaryResponse
from agent.memory_system.services.conversation_service import ConversationService
from agent.memory_system.providers.default_llm import DefaultLLMProvider

logger = logging.getLogger("agent.memory_system.summarizer")

SUMMARIZER_PROMPT = """
You are an expert Conversation Summarizer.
Summarize the key information, decisions, technical context, and ongoing task state from the following conversation history.
Keep the summary concise, objective, and clear. Maintain all important technical facts, code structures, and user requirements.
"""


class SummarizationEngine:
    def __init__(self, conversation_service: Optional[ConversationService] = None, llm_provider=None):
        self.conversation_service = conversation_service or ConversationService()
        self.llm_provider = llm_provider or DefaultLLMProvider()

    def get_latest_summary(self, conversation_id: str) -> Optional[ConversationSummaryResponse]:
        with get_db() as conn:
            cursor = conn.execute(
                """
                SELECT summary_id, conversation_id, summary_text, last_summarized_message_id, version, generated_at
                FROM conversation_summaries
                WHERE conversation_id = ?
                ORDER BY version DESC
                LIMIT 1
                """,
                (conversation_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            return ConversationSummaryResponse(
                summary_id=row["summary_id"],
                conversation_id=row["conversation_id"],
                summary_text=row["summary_text"],
                last_summarized_message_id=row["last_summarized_message_id"],
                version=row["version"],
                generated_at=row["generated_at"],
            )

    def check_and_generate_summary(self, conversation_id: str) -> Optional[ConversationSummaryResponse]:
        """
        Check if message count exceeds threshold N and generate new summary version if needed.
        """
        paginated = self.conversation_service.get_messages(conversation_id, limit=500)
        messages = paginated.messages
        if not messages:
            return None

        threshold = get_config().summary_message_threshold
        latest_summary = self.get_latest_summary(conversation_id)

        # Calculate unsummarized message count
        unsummarized_count = len(messages)
        last_id = None
        current_version = 0

        if latest_summary:
            current_version = latest_summary.version
            # Find index of last summarized message
            idx = -1
            for i, msg in enumerate(messages):
                if msg.message_id == latest_summary.last_summarized_message_id:
                    idx = i
                    break
            if idx != -1:
                unsummarized_count = len(messages) - (idx + 1)

        if unsummarized_count < threshold:
            return latest_summary

        logger.info(f"Generating summary version {current_version + 1} for conversation {conversation_id} ({unsummarized_count} unsummarized messages)")

        # Prepare conversation transcript string for LLM
        formatted_dialogue = []
        for msg in messages:
            formatted_dialogue.append(f"{msg.role.value.upper()}: {msg.content}")
        transcript = "\n".join(formatted_dialogue)

        summary_prompt = transcript
        if latest_summary:
            summary_prompt = f"Previous Summary:\n{latest_summary.summary_text}\n\nNew Interaction Transcript:\n{transcript}"

        new_summary_text = self.llm_provider.generate(
            prompt=summary_prompt,
            system_prompt=SUMMARIZER_PROMPT,
        )

        last_message_id = messages[-1].message_id
        now = datetime.utcnow().isoformat()
        import uuid
        summary_id = f"sum_{uuid.uuid4().hex[:12]}"
        new_version = current_version + 1

        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO conversation_summaries
                (summary_id, conversation_id, summary_text, last_summarized_message_id, version, generated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (summary_id, conversation_id, new_summary_text, last_message_id, new_version, now),
            )

        return ConversationSummaryResponse(
            summary_id=summary_id,
            conversation_id=conversation_id,
            summary_text=new_summary_text,
            last_summarized_message_id=last_message_id,
            version=new_version,
            generated_at=now,
        )

    def get_context_window_messages(self, conversation_id: str, recent_window_size: int = 6) -> Tuple[Optional[str], list]:
        """
        Returns (summary_text, recent_messages) for optimal LLM context packing.
        """
        summary = self.get_latest_summary(conversation_id)
        summary_text = summary.summary_text if summary else None

        paginated = self.conversation_service.get_messages(conversation_id, limit=recent_window_size)
        # Get the last recent_window_size messages
        recent_messages = paginated.messages[-recent_window_size:] if paginated.messages else []

        return summary_text, recent_messages
