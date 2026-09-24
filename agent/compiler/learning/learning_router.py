"""FastAPI router for Self-Learning Data Cleaning Platform."""

from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from agent.compiler.learning.models import (
    KnowledgeSearchRequest,
    KnowledgeSearchResponse,
    PatternMatchRequest,
    PatternMatchResponse,
    RecommendationsRequest,
    RecommendationsResponse,
    ImpactPreviewRequest,
    ImpactPreviewResponse,
    ApproveCleaningRequest,
    RejectCleaningRequest,
    RuntimeResultRequest,
)
from agent.compiler.learning.knowledge_store import KnowledgeStore
from agent.compiler.learning.pattern_retrieval_agent import PatternRetrievalAgent
from agent.compiler.learning.recommendation_agent import RecommendationAgent
from agent.compiler.learning.impact_simulator import ImpactSimulator
from agent.compiler.learning.approval_store import ApprovalStore
from agent.compiler.learning.runtime_learning import RuntimeLearningAgent
from agent.compiler.learning.explanation_agent import ExplanationAgent, ExplainCleaningRequest, ExplainCleaningResponse
from agent.compiler.learning.analytics_service import AnalyticsService
from agent.compiler.learning.cleaning_package_builder import CleaningPackageBuilder

router = APIRouter(tags=["Self-Learning Data Cleaning"])

_knowledge_store = KnowledgeStore()
_pattern_agent = PatternRetrievalAgent()
_recommendation_agent = RecommendationAgent()
_impact_simulator = ImpactSimulator()
_approval_store = ApprovalStore()
_runtime_learning = RuntimeLearningAgent()
_explanation_agent = ExplanationAgent()
_analytics_service = AnalyticsService()
_package_builder = CleaningPackageBuilder()


# --- Phase 1: Knowledge Base ---

@router.post("/knowledge/search", response_model=KnowledgeSearchResponse)
def api_knowledge_search(payload: KnowledgeSearchRequest) -> KnowledgeSearchResponse:
    """Searches historical cleaning runs for proven strategies and success rates."""
    return _knowledge_store.search_knowledge(payload)


# --- Phase 2: Pattern Detection ---

@router.post("/patterns/match", response_model=PatternMatchResponse)
def api_patterns_match(payload: PatternMatchRequest) -> PatternMatchResponse:
    """Matches column signatures against Customer, Employee, Invoice, Orders, Products, Finance domains."""
    return _pattern_agent.match_pattern(payload)


# --- Phase 3: AI Recommendations ---

@router.post("/recommendations", response_model=RecommendationsResponse)
def api_recommendations(payload: RecommendationsRequest) -> RecommendationsResponse:
    """Synthesizes pattern library, knowledge base, repair memory, and feedback into ranked cleaning actions."""
    return _recommendation_agent.generate_recommendations(payload)


# --- Phase 4: Impact Simulator ---

@router.post("/impact/preview", response_model=ImpactPreviewResponse)
def api_impact_preview(payload: ImpactPreviewRequest) -> ImpactPreviewResponse:
    """Predicts transformation impact, modified/removed row volumes, and overall risk rating."""
    return _impact_simulator.simulate_impact(payload)


# --- Phase 5: User Approval Workflow ---

@router.post("/approve-cleaning")
def api_approve_cleaning(payload: ApproveCleaningRequest) -> Dict[str, Any]:
    """Records user approval of cleaning strategy decisions."""
    return _approval_store.record_approval(payload)


@router.post("/reject-cleaning")
def api_reject_cleaning(payload: RejectCleaningRequest) -> Dict[str, Any]:
    """Records user rejection of proposed cleaning strategies."""
    return _approval_store.record_rejection(payload)


# --- Phase 6: Self-Learning Telemetry ---

@router.post("/runtime-results")
def api_runtime_results(payload: RuntimeResultRequest) -> Dict[str, Any]:
    """Ingests execution results, updates strategy weights, and records repair memory."""
    return _runtime_learning.record_runtime_result(payload)


# --- Phase 7: Explainability Engine ---

@router.post("/explain-cleaning", response_model=ExplainCleaningResponse)
def api_explain_cleaning(payload: ExplainCleaningRequest) -> ExplainCleaningResponse:
    """Provides natural language explanation for why a specific cleaning action was recommended."""
    return _explanation_agent.explain(payload)


# --- Phase 8: Analytics Dashboard ---

@router.get("/analytics")
def api_analytics() -> Dict[str, Any]:
    """Returns platform KPIs, total rows processed/cleaned, top strategies, and domain pattern stats."""
    return _analytics_service.get_analytics()


# --- Phase 9: Download Cleaning Package ---

@router.get("/package/download")
def api_package_download(
    session_id: str = Query("default", description="Session ID for artifact bundling"),
    dataset_name: str = Query("dataset", description="Base dataset name"),
):
    """Generates and streams a standalone zip containing cleaning.py, tests.py, contract.yaml, lineage.json, report.html, manifest.json."""
    zip_buffer = _package_builder.build_package(session_id=session_id, dataset_name=dataset_name)
    filename = f"{dataset_name}_cleaning_package.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
