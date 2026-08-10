"""
Default Embedding Provider implementation with OpenAI and lightweight local fallback.
"""
import os
import math
import hashlib
import logging
from typing import List

from agent.memory_system.interfaces.embedding_provider import EmbeddingProviderInterface

logger = logging.getLogger("agent.memory_system.embeddings")


class DefaultEmbeddingProvider(EmbeddingProviderInterface):
    def __init__(self, dimension: int = 1536):
        self.dimension = dimension
        self._openai_client = None

        if os.getenv("OPENAI_API_KEY"):
            try:
                from openai import OpenAI
                self._openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            except Exception as e:
                logger.warning(f"Could not initialize OpenAI embedding client: {e}")

    def get_dimension(self) -> int:
        return self.dimension

    def embed_text(self, text: str) -> List[float]:
        if not text or not text.strip():
            return [0.0] * self.dimension

        if self._openai_client:
            try:
                resp = self._openai_client.embeddings.create(
                    model="text-embedding-3-small",
                    input=text.strip(),
                )
                return resp.data[0].embedding
            except Exception as e:
                logger.debug(f"OpenAI embedding error, falling back to local: {e}")

        # Deterministic local hash-based pseudo-vector representation for fallback
        return self._local_hash_embedding(text)

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [self.embed_text(t) for t in texts]

    def _local_hash_embedding(self, text: str) -> List[float]:
        tokens = text.lower().split()
        vector = [0.0] * self.dimension
        if not tokens:
            return vector

        for token in tokens:
            # Hash token into index
            h = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
            idx = h % self.dimension
            sign = 1.0 if (h % 2 == 0) else -1.0
            vector[idx] += sign

        # L2 normalize
        norm = math.sqrt(sum(v * v for v in vector))
        if norm > 0:
            vector = [v / norm for v in vector]
        return vector
