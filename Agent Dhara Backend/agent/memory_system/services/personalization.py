"""
Phase 4: Personalization Engine.
Fetches relevant user memories, user preferences, and conversation summary,
and injects them seamlessly into the LLM system prompt context.
"""
import logging
from typing import Dict, List, Optional, Tuple

from agent.memory_system.services.memory_service import MemoryService
from agent.memory_system.services.preference_service import UserPreferenceService
from agent.memory_system.services.summarizer import SummarizationEngine
from agent.memory_system.providers.default_embedding import DefaultEmbeddingProvider
from agent.memory_system.providers.local_vector_store import LocalVectorStore

logger = logging.getLogger("agent.memory_system.personalization")


class PersonalizationEngine:
    def __init__(
        self,
        memory_service: Optional[MemoryService] = None,
        preference_service: Optional[UserPreferenceService] = None,
        summarization_engine: Optional[SummarizationEngine] = None,
        embedding_provider=None,
        vector_store=None,
    ):
        self.memory_service = memory_service or MemoryService()
        self.preference_service = preference_service or UserPreferenceService()
        self.summarization_engine = summarization_engine or SummarizationEngine()
        self.embedding_provider = embedding_provider or DefaultEmbeddingProvider()
        self.vector_store = vector_store or LocalVectorStore()

    def build_personalized_context(
        self,
        user_id: str,
        user_query: str,
        conversation_id: Optional[str] = None,
        max_memories: int = 5,
    ) -> Dict[str, any]:
        """
        Builds complete personalized context including memories, preferences, and summary.
        """
        # 1. Fetch User Preferences
        prefs = self.preference_service.get_preferences(user_id)

        # 2. Semantic Search for Relevant Memories
        query_vec = self.embedding_provider.embed_text(user_query)
        relevant_vec_results = self.vector_store.search(
            query_vector=query_vec,
            user_id=user_id,
            entity_types=["memory"],
            limit=max_memories,
            score_threshold=0.2,
        )

        relevant_memories = [res.document.content for res in relevant_vec_results]

        # If semantic search returns few memories, supplement with active profile/skill memories
        if len(relevant_memories) < max_memories:
            all_mems = self.memory_service.get_user_memories(user_id=user_id)
            for m in all_mems:
                mem_str = f"{m.memory_type.value}: {m.memory_text}"
                if mem_str not in relevant_memories:
                    relevant_memories.append(mem_str)
                    if len(relevant_memories) >= max_memories:
                        break

        # 3. Fetch Conversation Summary if conversation_id provided
        summary_text = None
        recent_messages = []
        if conversation_id:
            summary_text, recent_messages = self.summarization_engine.get_context_window_messages(conversation_id)

        # 4. Construct System Prompt Injection Block
        system_prompt_block = self._format_system_prompt_block(prefs, relevant_memories, summary_text)

        return {
            "system_prompt_injection": system_prompt_block,
            "preferences": prefs,
            "relevant_memories": relevant_memories,
            "summary_text": summary_text,
            "recent_messages": recent_messages,
        }

    def _format_system_prompt_block(
        self, prefs, relevant_memories: List[str], summary_text: Optional[str]
    ) -> str:
        blocks = []

        blocks.append("=== USER PERSONALIZATION & PREFERENCES ===")
        blocks.append(f"- Preferred Language: {prefs.preferred_language}")
        blocks.append(f"- Preferred Coding Language: {prefs.preferred_coding_language}")
        blocks.append(f"- Communication Tone: {prefs.preferred_tone}")
        blocks.append(f"- Response Depth: {prefs.preferred_depth}")
        blocks.append(f"- Response Length: {prefs.preferred_response_length}")
        if prefs.custom_instructions:
            blocks.append(f"- Custom Instructions: {prefs.custom_instructions}")

        if relevant_memories:
            blocks.append("\n=== KNOWN USER MEMORIES & FACTS ===")
            for mem in relevant_memories:
                blocks.append(f"• {mem}")

        if summary_text:
            blocks.append("\n=== CONVERSATION SUMMARY ===")
            blocks.append(summary_text)

        blocks.append("==========================================")
        return "\n".join(blocks)
