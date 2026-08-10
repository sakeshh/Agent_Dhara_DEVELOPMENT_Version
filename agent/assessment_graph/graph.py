"""
LangGraph definition and compiler for the assessment sub-graph.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
from langgraph.graph import StateGraph, START, END
from langgraph.constants import Send

from agent.assessment_graph.state import AssessmentState, DatasetState
from agent.assessment_graph.nodes import (
    source_loader,
    profiler,
    db_pushdown_enricher,
    gx_validator,
    custom_rules_runner,
    relationship_analyzer,
    llm_enricher,
    report_assembler,
)


def route_to_profiler(state: AssessmentState) -> List[Send]:
    """Fans out to profiler node for each dataset in datasets."""
    datasets = state.get("datasets") or {}
    sends = []
    for name, df in datasets.items():
        sends.append(
            Send(
                "profiler",
                {
                    "dataset_name": name,
                    "df": df,
                    "thresholds": state.get("thresholds"),
                    "business_rules": state.get("business_rules"),
                    "job_id": state.get("job_id"),
                },
            )
        )
    return sends


def route_to_validators(state: AssessmentState) -> List[Send]:
    """Fans out to gx_validator and custom_rules_runner nodes for each dataset."""
    datasets = state.get("datasets") or {}
    sends = []
    for name, df in datasets.items():
        payload = {
            "dataset_name": name,
            "df": df,
            "thresholds": state.get("thresholds"),
            "business_rules": state.get("business_rules"),
            "job_id": state.get("job_id"),
        }
        sends.append(Send("gx_validator", payload))
        sends.append(Send("custom_rules_runner", payload))
    return sends


# Build state graph
builder = StateGraph(AssessmentState)

builder.add_node("source_loader", source_loader)
builder.add_node("profiler", profiler)
builder.add_node("db_pushdown_enricher", db_pushdown_enricher)
builder.add_node("gx_validator", gx_validator)
builder.add_node("custom_rules_runner", custom_rules_runner)
builder.add_node("relationship_analyzer", relationship_analyzer)
builder.add_node("llm_enricher", llm_enricher)
builder.add_node("report_assembler", report_assembler)

# Define transitions
builder.add_edge(START, "source_loader")
builder.add_conditional_edges("source_loader", route_to_profiler, ["profiler"])
builder.add_edge("profiler", "db_pushdown_enricher")
builder.add_conditional_edges("db_pushdown_enricher", route_to_validators, ["gx_validator", "custom_rules_runner"])
builder.add_edge("gx_validator", "relationship_analyzer")
builder.add_edge("custom_rules_runner", "relationship_analyzer")
builder.add_edge("relationship_analyzer", "llm_enricher")
builder.add_edge("llm_enricher", "report_assembler")
builder.add_edge("report_assembler", END)

# Compile graph
assessment_graph = builder.compile()


def run_assessment_subgraph(
    source_cfg: Dict[str, Any],
    additional_data: Optional[Dict[str, pd.DataFrame]] = None,
    *,
    job_id: Optional[str] = None,
    max_rows: Optional[int] = None,
    db_connectors: Optional[Dict[str, Any]] = None,
    approved_semantics: Optional[Dict[str, str]] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
    run_gx: Optional[bool] = None,
) -> Dict[str, Any]:
    """Wraps sub-graph execution behind a single function invocation."""
    initial_state: AssessmentState = {
        "source_cfg": source_cfg,
        "additional_data": additional_data or {},
        "max_rows": max_rows,
        "db_connectors": db_connectors or {},
        "approved_semantics": approved_semantics or {},
        "business_rules": business_rules or {},
        "thresholds": thresholds or {},
        "job_id": job_id,
        "run_gx": run_gx,
        "datasets": {},
        "metadata": {},
        "per_dataset_dq": {},
        "relationships": [],
        "global_issues": {},
        "gx_results": {},
        "progress_pct": 0,
        "error": None,
        "result": {},
    }

    final_state = assessment_graph.invoke(initial_state)
    return final_state.get("result") or {}
