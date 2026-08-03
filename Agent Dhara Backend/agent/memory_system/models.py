"""
Data Models and Pydantic Schemas for Enterprise Memory System.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RoleEnum(str, Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"


class MemoryTypeEnum(str, Enum):
    PROFILE = "Profile"
    PREFERENCE = "Preference"
    SKILL = "Skill"
    GOAL = "Goal"
    PROJECT = "Project"
    INTEREST = "Interest"


# -------------------------------------------------------------------
# Conversation Schemas
# -------------------------------------------------------------------

class ConversationCreate(BaseModel):
    user_id: Optional[str] = None
    title: Optional[str] = "New Chat"


class ConversationUpdate(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None


class ConversationResponse(BaseModel):
    conversation_id: str
    user_id: str
    title: str
    status: str = "active"
    is_deleted: bool = False
    created_at: str
    updated_at: str
    message_count: Optional[int] = 0


# -------------------------------------------------------------------
# Message Schemas
# -------------------------------------------------------------------

class MessageCreate(BaseModel):
    conversation_id: str
    role: RoleEnum
    content: str
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class MessageResponse(BaseModel):
    message_id: str
    conversation_id: str
    role: RoleEnum
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class PaginatedMessagesResponse(BaseModel):
    messages: List[MessageResponse]
    total_count: int
    offset: int
    limit: int
    has_more: bool


# -------------------------------------------------------------------
# Memory Schemas
# -------------------------------------------------------------------

class MemoryCreate(BaseModel):
    user_id: str
    memory_type: MemoryTypeEnum
    memory_text: str
    confidence_score: float = 1.0
    source_conversation_id: Optional[str] = None


class MemoryResponse(BaseModel):
    memory_id: str
    user_id: str
    memory_type: MemoryTypeEnum
    memory_text: str
    confidence_score: float
    source_conversation_id: Optional[str] = None
    is_active: bool = True
    created_at: str
    updated_at: str


# -------------------------------------------------------------------
# User Preference Schemas
# -------------------------------------------------------------------

class UserPreferenceUpdate(BaseModel):
    preferred_language: Optional[str] = "English"
    preferred_coding_language: Optional[str] = "Python"
    preferred_response_length: Optional[str] = "detailed"
    preferred_tone: Optional[str] = "professional"
    preferred_depth: Optional[str] = "deep_dive"
    custom_instructions: Optional[str] = ""


class UserPreferenceResponse(BaseModel):
    preference_id: str
    user_id: str
    preferred_language: str = "English"
    preferred_coding_language: str = "Python"
    preferred_response_length: str = "detailed"
    preferred_tone: str = "professional"
    preferred_depth: str = "deep_dive"
    custom_instructions: str = ""
    updated_at: str


# -------------------------------------------------------------------
# Conversation Summary Schemas
# -------------------------------------------------------------------

class ConversationSummaryResponse(BaseModel):
    summary_id: str
    conversation_id: str
    summary_text: str
    last_summarized_message_id: Optional[str] = None
    version: int
    generated_at: str


# -------------------------------------------------------------------
# Feedback Schemas
# -------------------------------------------------------------------

class FeedbackCreate(BaseModel):
    message_id: str
    user_id: Optional[str] = None
    rating: int = Field(..., description="1 for Thumbs Up, -1 for Thumbs Down")
    comment: Optional[str] = None


class FeedbackResponse(BaseModel):
    feedback_id: str
    message_id: str
    user_id: str
    rating: int
    comment: Optional[str] = None
    timestamp: str


# -------------------------------------------------------------------
# Search & Vector Query Schemas
# -------------------------------------------------------------------

class SearchQueryRequest(BaseModel):
    query: str
    user_id: Optional[str] = None
    limit: int = 10
    search_type: str = "hybrid" # hybrid, semantic, keyword


class SearchResultItem(BaseModel):
    result_type: str # message, conversation, memory, summary
    id: str
    conversation_id: Optional[str] = None
    title: Optional[str] = None
    content: str
    score: float
    timestamp: Optional[str] = None
