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
        self._client = None
        self._model = None

        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        azure_key = os.getenv("AZURE_OPENAI_API_KEY")
        azure_emb_dep = (
            os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT")
            or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
            or os.getenv("AZURE_OPENAI_EMBEDDING_MODEL")
        )

        if azure_endpoint and azure_key and azure_emb_dep:
            try:
                from openai import AzureOpenAI
                self._client = AzureOpenAI(
                    azure_endpoint=azure_endpoint,
                    api_key=azure_key,
                    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
                )
                self._model = azure_emb_dep
            except Exception as e:
                logger.warning(f"Could not initialize Azure OpenAI embedding client: {e}")
        elif os.getenv("OPENAI_API_KEY") and not azure_endpoint:
            # Only connect to public OpenAI if Azure OpenAI is not the configured provider
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                self._model = "text-embedding-3-small"
            except Exception as e:
                logger.warning(f"Could not initialize OpenAI embedding client: {e}")

    def get_dimension(self) -> int:
        return self.dimension

    def embed_text(self, text: str) -> List[float]:
        if not text or not text.strip():
            return [0.0] * self.dimension

        if self._client and self._model:
            try:
                resp = self._client.embeddings.create(
                    model=self._model,
                    input=text.strip(),
                )
                return resp.data[0].embedding
            except Exception as e:
                logger.debug(f"OpenAI/Azure embedding error, falling back to local: {e}")

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
