"""
Default LLM Provider implementation supporting OpenAI and fallback completion.
"""
import os
import json
import logging
from typing import Optional

from agent.memory_system.interfaces.llm_provider import LLMProviderInterface

logger = logging.getLogger("agent.memory_system.llm")


class DefaultLLMProvider(LLMProviderInterface):
    def __init__(self, model_name: str = "gpt-4o-mini"):
        self.model_name = model_name
        self._openai_client = None

        if os.getenv("OPENAI_API_KEY"):
            try:
                from openai import OpenAI
                self._openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            except Exception as e:
                logger.warning(f"Could not initialize OpenAI LLM client: {e}")

    def generate(self, prompt: str, system_prompt: Optional[str] = None, json_mode: bool = False) -> str:
        if self._openai_client:
            try:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})

                kwargs = {
                    "model": self.model_name,
                    "messages": messages,
                    "temperature": 0.2,
                }
                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                resp = self._openai_client.chat.completions.create(**kwargs)
                return resp.choices[0].message.content or ""
            except Exception as e:
                logger.error(f"OpenAI LLM generate error: {e}")

        # Basic heuristic fallback when no LLM key is available
        if json_mode:
            return json.dumps({"memories": []})
        return "Summary of conversation: Interaction completed."
