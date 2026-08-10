"""
Abstract Vector Store Interface supporting Qdrant, Pinecone, Weaviate, Azure AI Search, and Local storage.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class VectorDocument(BaseModel):
    id: str
    user_id: str
    entity_type: str # message, summary, memory
    entity_id: str
    vector: List[float]
    content: str
    metadata: Dict[str, Any] = {}


class VectorSearchResult(BaseModel):
    document: VectorDocument
    score: float


class VectorStoreInterface(ABC):
    @abstractmethod
    def upsert(self, documents: List[VectorDocument]) -> bool:
        """Insert or update documents with vectors in the vector store."""
        pass

    @abstractmethod
    def search(
        self,
        query_vector: List[float],
        user_id: str,
        entity_types: Optional[List[str]] = None,
        limit: int = 10,
        score_threshold: float = 0.0,
    ) -> List[VectorSearchResult]:
        """Search vector store by similarity vector."""
        pass

    @abstractmethod
    def delete(self, document_ids: List[str]) -> bool:
        """Delete documents from vector store."""
        pass
