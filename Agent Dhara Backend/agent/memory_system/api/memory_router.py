"""
FastAPI Router exposing REST API Endpoints for Conversations, Messages, Memories, Preferences, Search, and Feedback.
"""
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

from agent.memory_system.database import init_db
from agent.memory_system.models import (
    ConversationCreate,
    ConversationResponse,
    ConversationUpdate,
    FeedbackCreate,
    FeedbackResponse,
    MemoryCreate,
    MemoryResponse,
    MemoryTypeEnum,
    MessageCreate,
    MessageResponse,
    PaginatedMessagesResponse,
    SearchQueryRequest,
    SearchResultItem,
    UserPreferenceResponse,
    UserPreferenceUpdate,
)
from agent.memory_system.services.conversation_service import ConversationService
from agent.memory_system.services.memory_service import MemoryService
from agent.memory_system.services.extraction_pipeline import MemoryExtractionPipeline
from agent.memory_system.services.personalization import PersonalizationEngine
from agent.memory_system.services.preference_service import UserPreferenceService
from agent.memory_system.services.summarizer import SummarizationEngine
from agent.memory_system.services.chat_search_service import ChatSearchService
from agent.memory_system.services.feedback_service import FeedbackService
from agent.memory_system.services.observability import global_observability

# Initialize Router & Database
router = APIRouter(prefix="/api/v1", tags=["enterprise-memory"])
init_db()

# Services
conversation_service = ConversationService()
memory_service = MemoryService()
extraction_pipeline = MemoryExtractionPipeline(memory_service=memory_service)
personalization_engine = PersonalizationEngine(
    memory_service=memory_service,
    preference_service=UserPreferenceService(),
    summarization_engine=SummarizationEngine(conversation_service=conversation_service),
)
preference_service = UserPreferenceService()
summarizer = SummarizationEngine(conversation_service=conversation_service)
chat_search_service = ChatSearchService()
feedback_service = FeedbackService()


# -------------------------------------------------------------------
# Phase 1: Conversation Management Endpoints
# -------------------------------------------------------------------

@router.post("/conversations", response_model=ConversationResponse)
def create_conversation(payload: ConversationCreate):
    conv = conversation_service.create_conversation(user_id=payload.user_id, title=payload.title or "New Chat")
    global_observability.log_event("conversation_created", {"id": conv.conversation_id})
    return conv


@router.get("/conversations", response_model=List[ConversationResponse])
def list_conversations(
    user_id: Optional[str] = Query(None),
    include_deleted: bool = Query(False),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return conversation_service.list_user_conversations(
        user_id=user_id, include_deleted=include_deleted, limit=limit, offset=offset
    )


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
def get_conversation(conversation_id: str):
    conv = conversation_service.get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


@router.patch("/conversations/{conversation_id}", response_model=ConversationResponse)
def update_conversation(conversation_id: str, payload: ConversationUpdate):
    conv = conversation_service.update_conversation(
        conversation_id=conversation_id, title=payload.title, status=payload.status
    )
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


@router.delete("/conversations/{conversation_id}")
def delete_conversation(conversation_id: str):
    success = conversation_service.soft_delete_conversation(conversation_id)
    return {"ok": success, "message": "Conversation soft deleted"}


@router.post("/conversations/{conversation_id}/restore")
def restore_conversation(conversation_id: str):
    success = conversation_service.restore_conversation(conversation_id)
    return {"ok": success, "message": "Conversation restored"}


# -------------------------------------------------------------------
# Messages Endpoints
# -------------------------------------------------------------------

@router.get("/conversations/{conversation_id}/messages", response_model=PaginatedMessagesResponse)
def get_messages(
    conversation_id: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return conversation_service.get_messages(conversation_id=conversation_id, limit=limit, offset=offset)


@router.post("/conversations/{conversation_id}/messages", response_model=MessageResponse)
def add_message(conversation_id: str, payload: MessageCreate, background_tasks: BackgroundTasks):
    msg = conversation_service.add_message(
        conversation_id=conversation_id,
        role=payload.role,
        content=payload.content,
        metadata=payload.metadata,
    )

    # Background memory extraction & summarization for user messages
    if payload.role.value == "user":
        conv = conversation_service.get_conversation(conversation_id)
        uid = conv.user_id if conv else "default_user"
        background_tasks.add_task(extraction_pipeline.process_message, uid, payload.content, conversation_id)
        background_tasks.add_task(summarizer.check_and_generate_summary, conversation_id)

    return msg


# -------------------------------------------------------------------
# Phase 2: Memory Endpoints
# -------------------------------------------------------------------

@router.get("/memories", response_model=List[MemoryResponse])
def get_memories(
    user_id: Optional[str] = Query(None),
    memory_type: Optional[MemoryTypeEnum] = Query(None),
    min_confidence: float = Query(0.0),
):
    uid = user_id or "default_user"
    return memory_service.get_user_memories(user_id=uid, memory_type=memory_type, min_confidence=min_confidence)


@router.post("/memories", response_model=MemoryResponse)
def add_memory(payload: MemoryCreate):
    return memory_service.add_memory(
        user_id=payload.user_id,
        memory_type=payload.memory_type,
        memory_text=payload.memory_text,
        confidence_score=payload.confidence_score,
        source_conversation_id=payload.source_conversation_id,
    )


@router.delete("/memories/{memory_id}")
def delete_memory(memory_id: str):
    success = memory_service.delete_memory(memory_id)
    return {"ok": success}


# -------------------------------------------------------------------
# Phase 7: User Preferences Endpoints
# -------------------------------------------------------------------

@router.get("/preferences", response_model=UserPreferenceResponse)
def get_preferences(user_id: Optional[str] = Query(None)):
    return preference_service.get_preferences(user_id)


@router.put("/preferences", response_model=UserPreferenceResponse)
def update_preferences(payload: UserPreferenceUpdate, user_id: Optional[str] = Query(None)):
    return preference_service.update_preferences(user_id=user_id, update_data=payload)


# -------------------------------------------------------------------
# Phase 8: Search Endpoint
# -------------------------------------------------------------------

@router.post("/search", response_model=List[SearchResultItem])
def search_chats(payload: SearchQueryRequest):
    return chat_search_service.search(payload)


# -------------------------------------------------------------------
# Phase 9: Feedback Endpoints
# -------------------------------------------------------------------

@router.post("/feedback", response_model=FeedbackResponse)
def submit_feedback(payload: FeedbackCreate):
    return feedback_service.add_feedback(payload)


@router.get("/feedback/{message_id}", response_model=Optional[FeedbackResponse])
def get_feedback(message_id: str):
    return feedback_service.get_feedback_for_message(message_id)


# -------------------------------------------------------------------
# Observability Endpoint
# -------------------------------------------------------------------

@router.get("/observability/stats")
def get_observability_stats():
    return global_observability.get_system_stats()
