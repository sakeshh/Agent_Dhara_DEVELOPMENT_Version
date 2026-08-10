"""
Abstract interface for LLM calls used in memory extraction and summarization.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LLMProviderInterface(ABC):
    @abstractmethod
    def generate(self, prompt: str, system_prompt: Optional[str] = None, json_mode: bool = False) -> str:
        """Generate response from LLM."""
        pass
