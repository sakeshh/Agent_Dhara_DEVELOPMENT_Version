"""
Phase 3: Memory Extraction Pipeline.
Analyzes user interactions to automatically detect and extract durable user facts.
Filters out short-lived queries and temporary requests.
"""
import json
import logging
from typing import Any, Dict, List, Optional

from agent.memory_system.models import MemoryTypeEnum
from agent.memory_system.services.memory_service import MemoryService
from agent.memory_system.providers.default_llm import DefaultLLMProvider

logger = logging.getLogger("agent.memory_system.extraction_pipeline")

EXTRACTION_SYSTEM_PROMPT = """
You are an expert AI Memory Extraction Agent.
Your task is to analyze user messages in a conversation and extract LONG-TERM DURABLE FACTS about the user.

Categories:
- Profile: User's job title, location, background (e.g. "I am a Software Engineer").
- Preference: User's preferred tools, frameworks, formatting (e.g. "I prefer Python over R").
- Skill: User's technical capabilities (e.g. "I know PySpark and Azure Databricks").
- Goal: User's learning objectives or ambitions (e.g. "I want to learn Azure Data Engineering").
- Project: What the user is building (e.g. "Building an ETL pipeline for banking data").
- Interest: User's personal or professional technical interests.

CRITICAL INSTRUCTIONS:
1. ONLY extract long-term, durable user facts.
2. DO NOT extract temporary questions (e.g., "How do I fix this syntax error?"), request commands, or one-off conversation chatter.
3. Assign a confidence score between 0.7 and 1.0.

Respond strictly in JSON format:
{
  "memories": [
    {
      "type": "Goal",
      "value": "Wants to learn Azure Data Engineering",
      "confidence": 0.9
    }
  ]
}
If no durable facts are present, return {"memories": []}.
"""


class MemoryExtractionPipeline:
    def __init__(self, memory_service: Optional[MemoryService] = None, llm_provider=None):
        self.memory_service = memory_service or MemoryService()
        self.llm_provider = llm_provider or DefaultLLMProvider()

    def process_message(
        self, user_id: str, message_content: str, conversation_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Analyze user message and extract memories if durable facts exist.
        """
        if not message_content or len(message_content.strip()) < 10:
            return []

        # Heuristic fast check for fact-bearing keywords to reduce LLM calls
        fact_indicators = ["i am", "i work", "my role", "i prefer", "i know", "learning", "building", "my goal", "my project", "i use"]
        content_lower = message_content.lower()
        has_indicator = any(ind in content_lower for ind in fact_indicators)

        if not has_indicator and "?" in message_content:
            # Simple question without personal declaration -> skip extraction
            return []

        try:
            raw_json = self.llm_provider.generate(
                prompt=f"User Message: {message_content}",
                system_prompt=EXTRACTION_SYSTEM_PROMPT,
                json_mode=True,
            )

            data = json.loads(raw_json) if raw_json else {}
            extracted_items = data.get("memories", [])

            # Rule-based fallback if LLM is offline or returns empty memories
            if not extracted_items and has_indicator:
                import re
                if "i am" in content_lower:
                    m = re.search(r"i am (a|an)?\s*([^,\.]+)", message_content, re.IGNORECASE)
                    if m:
                        extracted_items.append({"type": "Profile", "value": f"User is {m.group(2).strip()}", "confidence": 0.85})
                if "building" in content_lower:
                    m = re.search(r"building\s*([^,\.]+)", message_content, re.IGNORECASE)
                    if m:
                        extracted_items.append({"type": "Project", "value": f"Building {m.group(1).strip()}", "confidence": 0.85})
                if "learning" in content_lower:
                    m = re.search(r"learning\s*([^,\.]+)", message_content, re.IGNORECASE)
                    if m:
                        extracted_items.append({"type": "Goal", "value": f"Learning {m.group(1).strip()}", "confidence": 0.85})

            saved_memories = []

            for item in extracted_items:
                m_type_str = item.get("type", "Profile")
                m_val = item.get("value")
                confidence = float(item.get("confidence", 0.8))

                # Normalize enum
                valid_type = None
                for enum_val in MemoryTypeEnum:
                    if enum_val.value.lower() == m_type_str.lower():
                        valid_type = enum_val
                        break
                if not valid_type:
                    valid_type = MemoryTypeEnum.PROFILE

                if m_val and confidence >= 0.7:
                    saved = self.memory_service.add_memory(
                        user_id=user_id,
                        memory_type=valid_type,
                        memory_text=m_val,
                        confidence_score=confidence,
                        source_conversation_id=conversation_id,
                    )
                    saved_memories.append(saved.model_dump() if hasattr(saved, "model_dump") else saved.dict())
                    logger.info(f"Extracted durable memory for user {user_id}: [{valid_type.value}] {m_val}")

            return saved_memories

        except Exception as e:
            logger.error(f"Error in MemoryExtractionPipeline: {e}")
            return []
