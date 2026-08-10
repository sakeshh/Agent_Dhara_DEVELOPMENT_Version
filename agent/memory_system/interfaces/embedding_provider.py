"""
Abstract interface for Embedding Generation.
"""
from abc import ABC, abstractmethod
from typing import List


class EmbeddingProviderInterface(ABC):
    @abstractmethod
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector for a single string."""
        pass

    @abstractmethod
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embedding vectors for a batch of strings."""
        pass

    @abstractmethod
    def get_dimension(self) -> int:
        """Return the vector dimensionality."""
        pass
