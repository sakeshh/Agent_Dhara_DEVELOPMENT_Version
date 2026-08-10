"""
Configuration module for the Enterprise Conversation Memory System.
"""
import os
from typing import Optional
from pydantic import BaseModel, Field


class MemorySystemConfig(BaseModel):
    # Database
    db_path: str = Field(
        default_factory=lambda: os.getenv(
            "MEMORY_SYSTEM_DB_PATH",
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                "output",
                "enterprise_memory.sqlite3",
            ),
        )
    )

    # Vector Store Config
    vector_store_type: str = Field(default_factory=lambda: os.getenv("VECTOR_STORE_TYPE", "local")) # local, qdrant, pinecone, weaviate, azure_ai
    qdrant_url: Optional[str] = Field(default_factory=lambda: os.getenv("QDRANT_URL"))
    qdrant_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("QDRANT_API_KEY"))
    pinecone_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("PINECONE_API_KEY"))
    pinecone_environment: Optional[str] = Field(default_factory=lambda: os.getenv("PINECONE_ENV"))
    azure_search_endpoint: Optional[str] = Field(default_factory=lambda: os.getenv("AZURE_SEARCH_ENDPOINT"))
    azure_search_key: Optional[str] = Field(default_factory=lambda: os.getenv("AZURE_SEARCH_KEY"))

    # Summarization
    summary_message_threshold: int = Field(default=10) # Generate summary every N messages
    summary_model: str = Field(default="gpt-4o-mini")

    # Memory Extraction
    extraction_enabled: bool = Field(default=True)
    min_confidence_score: float = Field(default=0.7)

    # Personalization
    max_relevant_memories: int = Field(default=5)

    # Default User
    default_user_id: str = Field(default="default_user")


def get_config() -> MemorySystemConfig:
    return MemorySystemConfig()
