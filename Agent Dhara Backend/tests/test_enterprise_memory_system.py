"""
Comprehensive Test Suite for Enterprise Conversation Memory System.
"""
import os
import pytest
from fastapi.testclient import TestClient

from agent.memory_system.database import init_db, get_db
from agent.memory_system.models import (
    RoleEnum,
    MemoryTypeEnum,
    UserPreferenceUpdate,
    FeedbackCreate,
    SearchQueryRequest,
)
from agent.memory_system.services.conversation_service import ConversationService
from agent.memory_system.services.memory_service import MemoryService
from agent.memory_system.services.extraction_pipeline import MemoryExtractionPipeline
from agent.memory_system.services.personalization import PersonalizationEngine
from agent.memory_system.services.preference_service import UserPreferenceService
from agent.memory_system.services.summarizer import SummarizationEngine
from agent.memory_system.services.vector_search import VectorSearchService
from agent.memory_system.services.chat_search_service import ChatSearchService
from agent.memory_system.services.feedback_service import FeedbackService
from agent.mcp_server import app


@pytest.fixture(autouse=True)
def setup_test_environment(tmp_path):
    db_file = str(tmp_path / "test_memory.sqlite3")
    os.environ["MEMORY_SYSTEM_DB_PATH"] = db_file
    init_db()
    yield
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except Exception:
            pass


def test_phase1_conversation_and_message_crud():
    conv_svc = ConversationService()
    user_id = "test_user_101"

    # 1. Create Conversation
    conv = conv_svc.create_conversation(user_id=user_id, title="Data Architecture Strategy")
    assert conv.conversation_id.startswith("conv_")
    assert conv.title == "Data Architecture Strategy"
    assert conv.is_deleted is False

    # 2. Add Messages
    m1 = conv_svc.add_message(conv.conversation_id, RoleEnum.USER, "Hello, I am a Lead Data Engineer.")
    m2 = conv_svc.add_message(conv.conversation_id, RoleEnum.ASSISTANT, "Hello! How can I assist you with data engineering?")
    assert m1.role == RoleEnum.USER
    assert m2.role == RoleEnum.ASSISTANT

    # 3. Paginated Messages
    msgs = conv_svc.get_messages(conv.conversation_id, limit=10)
    assert len(msgs.messages) == 2
    assert msgs.total_count == 2

    # 4. Soft Delete & Restore
    conv_svc.soft_delete_conversation(conv.conversation_id)
    assert conv_svc.get_conversation(conv.conversation_id).is_deleted is True

    conv_svc.restore_conversation(conv.conversation_id)
    assert conv_svc.get_conversation(conv.conversation_id).is_deleted is False


def test_phase2_user_memory_and_deduplication():
    mem_svc = MemoryService()
    user_id = "test_user_102"

    # 1. Add Memory
    m1 = mem_svc.add_memory(
        user_id=user_id,
        memory_type=MemoryTypeEnum.SKILL,
        memory_text="Expert in PySpark and Delta Lake",
        confidence_score=0.95,
    )
    assert m1.memory_type == MemoryTypeEnum.SKILL
    assert m1.memory_text == "Expert in PySpark and Delta Lake"

    # 2. Duplicate Detection
    m2 = mem_svc.add_memory(
        user_id=user_id,
        memory_type=MemoryTypeEnum.SKILL,
        memory_text="Expert in PySpark and Delta Lake",
        confidence_score=0.98,
    )
    assert m2.memory_id == m1.memory_id # Reused ID due to duplicate detection update

    # 3. Retrieve User Memories
    user_mems = mem_svc.get_user_memories(user_id=user_id)
    assert len(user_mems) == 1


def test_phase3_memory_extraction_pipeline():
    extractor = MemoryExtractionPipeline()
    user_id = "test_user_103"

    # Fact-bearing message
    extracted = extractor.process_message(
        user_id=user_id,
        message_content="I am a Senior Azure Solutions Architect building PySpark ETL pipelines.",
    )
    assert len(extracted) >= 1

    # Temporary non-fact question should yield empty extraction
    temp_extracted = extractor.process_message(
        user_id=user_id,
        message_content="What is the command to list files in bash?",
    )
    assert len(temp_extracted) == 0


def test_phase4_personalization_engine():
    pers_engine = PersonalizationEngine()
    user_id = "test_user_104"

    # Seed memory & preferences
    pers_engine.memory_service.add_memory(
        user_id=user_id,
        memory_type=MemoryTypeEnum.GOAL,
        memory_text="Wants to master Databricks Unity Catalog",
    )
    pers_engine.preference_service.update_preferences(
        user_id=user_id,
        update_data=UserPreferenceUpdate(preferred_tone="concise_and_technical"),
    )

    ctx = pers_engine.build_personalized_context(user_id=user_id, user_query="How do I set up grants?")
    assert "concise_and_technical" in ctx["system_prompt_injection"]
    assert "Databricks Unity Catalog" in ctx["system_prompt_injection"]


def test_phase5_summarizer():
    conv_svc = ConversationService()
    summarizer = SummarizationEngine(conversation_service=conv_svc)
    conv = conv_svc.create_conversation(title="Long Session")

    # Add 12 messages to cross threshold N=10
    for i in range(12):
        role = RoleEnum.USER if i % 2 == 0 else RoleEnum.ASSISTANT
        conv_svc.add_message(conv.conversation_id, role, f"Message content payload step {i}")

    summary = summarizer.check_and_generate_summary(conv.conversation_id)
    assert summary is not None
    assert summary.version == 1
    assert len(summary.summary_text) > 0


def test_phase7_user_preferences():
    pref_svc = UserPreferenceService()
    user_id = "test_user_107"

    pref = pref_svc.get_preferences(user_id)
    assert pref.preferred_language == "English"

    updated = pref_svc.update_preferences(
        user_id=user_id,
        update_data=UserPreferenceUpdate(
            preferred_language="Spanish",
            preferred_coding_language="Scala",
            preferred_tone="academic",
        ),
    )
    assert updated.preferred_language == "Spanish"
    assert updated.preferred_coding_language == "Scala"
    assert updated.preferred_tone == "academic"


def test_phase8_hybrid_chat_search():
    conv_svc = ConversationService()
    search_svc = ChatSearchService()
    user_id = "test_user_108"

    conv = conv_svc.create_conversation(user_id=user_id, title="Spark Optimization")
    conv_svc.add_message(conv.conversation_id, RoleEnum.USER, "How to tune partition sizes in PySpark?")

    results = search_svc.search(
        SearchQueryRequest(query="PySpark", user_id=user_id, search_type="hybrid")
    )
    assert len(results) >= 1
    assert any("PySpark" in r.content or "Spark" in (r.title or "") for r in results)


def test_phase9_feedback_system():
    feedback_svc = FeedbackService()
    fb = feedback_svc.add_feedback(
        FeedbackCreate(message_id="msg_sample_123", rating=1, comment="Excellent answer!")
    )
    assert fb.feedback_id.startswith("fb_")
    assert fb.rating == 1
    assert fb.comment == "Excellent answer!"


def test_api_router_endpoints():
    client = TestClient(app)

    # 1. Create Conversation API
    res = client.post("/api/v1/conversations", json={"title": "API Test Chat"})
    assert res.status_code == 200
    c_data = res.json()
    assert c_data["title"] == "API Test Chat"
    conv_id = c_data["conversation_id"]

    # 2. List Conversations API
    res_list = client.get("/api/v1/conversations")
    assert res_list.status_code == 200
    assert len(res_list.json()) >= 1

    # 3. Add Message API
    res_msg = client.post(
        f"/api/v1/conversations/{conv_id}/messages",
        json={"conversation_id": conv_id, "role": "user", "content": "Tell me about data quality rules."},
    )
    assert res_msg.status_code == 200
    assert res_msg.json()["content"] == "Tell me about data quality rules."

    # 4. Memory API
    res_mem = client.post(
        "/api/v1/memories",
        json={"user_id": "default_user", "memory_type": "Skill", "memory_text": "Knows SQL"},
    )
    assert res_mem.status_code == 200

    # 5. Preferences API
    res_pref = client.get("/api/v1/preferences")
    assert res_pref.status_code == 200

    # 6. Observability API
    res_obs = client.get("/api/v1/observability/stats")
    assert res_obs.status_code == 200
    assert "metrics" in res_obs.json()
