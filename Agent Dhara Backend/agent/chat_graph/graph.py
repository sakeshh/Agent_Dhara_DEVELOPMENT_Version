# Chat workflow graph construction and entry points.
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    from langgraph.graph import END, StateGraph
    from langgraph.types import Command
except Exception:
    END = None
    StateGraph = None
    Command = None

from agent.chat_graph.state import ChatState

def build_chat_graph(checkpointer=None):
    import agent.chat_graph
    for _n in dir(agent.chat_graph):
        if _n.startswith('_node_'):
            globals()[_n] = getattr(agent.chat_graph, _n)
    if StateGraph is None or END is None:
        raise ImportError("LangGraph not available")
    g = StateGraph(ChatState)
    g.add_node("load_session", _node_load_session)
    g.add_node("route", _node_route)
    g.add_node("help", _node_help)
    g.add_node("reset_flow", _node_reset_flow)
    g.add_node("back_flow", _node_back_flow)
    g.add_node("set_action", _node_set_action)
    g.add_node("list_sources", _node_list_sources)
    g.add_node("select_source", _node_select_source)
    g.add_node("list_tables", _node_list_tables)
    g.add_node("select_table", _node_select_table)
    g.add_node("select_tables", _node_select_tables)
    g.add_node("assess_selected_tables", _node_assess_selected_tables)
    g.add_node("list_blob_files", _node_list_blob_files)
    g.add_node("select_blob_files", _node_select_blob_files)
    g.add_node("assess_selected_files", _node_assess_selected_files)
    g.add_node("list_local_files", _node_list_local_files)
    g.add_node("select_local_files", _node_select_local_files)
    g.add_node("assess_selected_local_files", _node_assess_selected_local_files)
    g.add_node("preview_local_file", _node_preview_local_file)
    g.add_node("preview_blob_file", _node_preview_blob_file)
    g.add_node("preview_selected_file", _node_preview_selected_file)
    g.add_node("show_file_schema", _node_show_file_schema)
    g.add_node("show_file_metadata", _node_show_file_metadata)
    g.add_node("generate_report_selected_files", _node_generate_report_selected_files)
    g.add_node("show_schema", _node_show_schema)
    g.add_node("preview_table", _node_preview_table)
    g.add_node("show_metadata", _node_show_metadata)
    g.add_node("generate_report_selected", _node_generate_report_selected)
    g.add_node("show_cleaning_recommendations", _node_show_cleaning_recommendations)
    g.add_node("show_transform_suggestions", _node_show_transform_suggestions)
    g.add_node("dq_table", _node_dq_table)
    g.add_node("nl_query", _node_nl_query)
    g.add_node("show_null_columns", _node_show_null_columns)
    g.add_node("dq_overview", _node_dq_overview)
    g.add_node("summarize_report", _node_summarize_report)
    g.add_node("relationships_overview", _node_relationships_overview)
    g.add_node("show_selection_status", _node_show_selection_status)
    g.add_node("dq_duplicates", _node_dq_duplicates)
    g.add_node("extract_columns", _node_extract_columns)
    g.add_node("convo_full_report", _node_summarize_report)
    g.add_node("convo_top_issues", _node_convo_top_issues)
    g.add_node("convo_issue_filter", _node_convo_issue_filter)
    g.add_node("convo_triage", _node_convo_triage)
    g.add_node("convo_cross_dataset", _node_convo_cross_dataset)
    g.add_node("convo_clarify", _node_convo_clarify)
    g.add_node("convo_boundary_ood", _node_convo_boundary_ood)
    g.add_node("convo_boundary_adv", _node_convo_boundary_adv)
    g.add_node("convo_etl_guidance", _node_convo_etl_guidance)
    g.add_node("build_etl_plan", _node_build_etl_plan)
    g.add_node("generate_etl_code", _node_generate_etl_code)
    g.add_node("show_etl_plan", _node_show_etl_plan)
    g.add_node("confirm_etl_plan", _node_confirm_etl_plan)
    g.add_node("capture_business_rules", _node_capture_business_rules)
    g.add_node("download_etl_code", _node_download_etl_code)
    g.add_node("discover_semantic_rules", _node_discover_semantic_rules)
    g.add_node("save_session", _node_save_session)

    g.set_entry_point("load_session")
    g.add_edge("load_session", "route")

    def _branch(state: ChatState) -> str:
        return state.get("action") or "help"

    g.add_conditional_edges(
        "route",
        _branch,
        {
            "help": "help",
            "reset_flow": "reset_flow",
            "back_flow": "back_flow",
            "set_action": "set_action",
            "list_sources": "list_sources",
            "select_source": "select_source",
            "list_tables": "list_tables",
            "select_table": "select_table",
            "select_tables": "select_tables",
            "assess_selected_tables": "assess_selected_tables",
            "list_blob_files": "list_blob_files",
            "select_blob_files": "select_blob_files",
            "assess_selected_files": "assess_selected_files",
            "list_local_files": "list_local_files",
            "select_local_files": "select_local_files",
            "assess_selected_local_files": "assess_selected_local_files",
            "preview_local_file": "preview_local_file",
            "preview_blob_file": "preview_blob_file",
            "preview_selected_file": "preview_selected_file",
            "show_file_schema": "show_file_schema",
            "show_file_metadata": "show_file_metadata",
            "generate_report_selected_files": "generate_report_selected_files",
            "show_schema": "show_schema",
            "preview_table": "preview_table",
            "show_metadata": "show_metadata",
            "generate_report_selected": "generate_report_selected",
            "show_cleaning_recommendations": "show_cleaning_recommendations",
            "show_transform_suggestions": "show_transform_suggestions",
            "dq_table": "dq_table",
            "nl_query": "nl_query",
            "show_null_columns": "show_null_columns",
            "dq_overview": "dq_overview",
            "summarize_report": "summarize_report",
            "relationships_overview": "relationships_overview",
            "show_selection_status": "show_selection_status",
            "dq_duplicates": "dq_duplicates",
            "extract_columns": "extract_columns",
            "convo_full_report": "convo_full_report",
            "convo_top_issues": "convo_top_issues",
            "convo_issue_filter": "convo_issue_filter",
            "convo_triage": "convo_triage",
            "convo_cross_dataset": "convo_cross_dataset",
            "convo_clarify": "convo_clarify",
            "convo_boundary_ood": "convo_boundary_ood",
            "convo_boundary_adv": "convo_boundary_adv",
            "convo_etl_guidance": "convo_etl_guidance",
            "build_etl_plan": "build_etl_plan",
            "generate_etl_code": "generate_etl_code",
            "show_etl_plan": "show_etl_plan",
            "confirm_etl_plan": "confirm_etl_plan",
            "capture_business_rules": "capture_business_rules",
            "download_etl_code": "download_etl_code",
            "discover_semantic_rules": "discover_semantic_rules",
        },
    )

    for n in (
        "help",
        "reset_flow",
        "back_flow",
        "set_action",
        "list_sources",
        "select_source",
        "list_tables",
        "select_table",
        "select_tables",
        "assess_selected_tables",
        "list_blob_files",
        "select_blob_files",
        "assess_selected_files",
        "list_local_files",
        "select_local_files",
        "assess_selected_local_files",
        "preview_local_file",
        "preview_blob_file",
        "preview_selected_file",
        "show_file_schema",
        "show_file_metadata",
        "generate_report_selected_files",
        "show_schema",
        "preview_table",
        "show_metadata",
        "generate_report_selected",
        "show_cleaning_recommendations",
        "show_transform_suggestions",
        "dq_table",
        "nl_query",
        "show_null_columns",
        "dq_overview",
        "summarize_report",
        "relationships_overview",
        "show_selection_status",
        "dq_duplicates",
        "extract_columns",
        "convo_full_report",
        "convo_top_issues",
        "convo_issue_filter",
        "convo_triage",
        "convo_cross_dataset",
        "convo_boundary_ood",
        "convo_boundary_adv",
        "convo_etl_guidance",
        "build_etl_plan",
        "generate_etl_code",
        "show_etl_plan",
        "confirm_etl_plan",
        "capture_business_rules",
        "download_etl_code",
        "discover_semantic_rules",
    ):
        g.add_edge(n, "save_session")
    g.add_edge("convo_clarify", "route")
    g.add_edge("save_session", END)
    if checkpointer is None:
        try:
            from agent.memory import get_zep_checkpointer
            checkpointer = get_zep_checkpointer()
        except Exception:
            pass
            
    if checkpointer is None:
        try:
            import sqlite3
            import os
            from langgraph.checkpoint.sqlite import SqliteSaver
            here = os.path.dirname(os.path.abspath(__file__))
            db_dir = os.path.join(here, "data")
            os.makedirs(db_dir, exist_ok=True)
            db_path = os.path.join(db_dir, "checkpointer.db")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            checkpointer = SqliteSaver(conn)
            checkpointer.setup()
        except Exception as e:
            try:
                from langgraph.checkpoint.memory import MemorySaver
                checkpointer = MemorySaver()
            except Exception:
                import logging
                logging.getLogger(__name__).warning(f"Could not initialize SqliteSaver or MemorySaver: {e}")
            
    return g.compile(checkpointer=checkpointer)


def run_chat(*, session_id: str, message: str, job_id: Optional[str] = None, thread_id: Optional[str] = None, resume_value: Optional[str] = None, checkpointer=None) -> Dict[str, Any]:
    graph = build_chat_graph(checkpointer=checkpointer)
    
    tid = thread_id or session_id or "default"
    config = {"configurable": {"thread_id": tid}}
    
    if resume_value is not None:
        raw = dict(graph.invoke(Command(resume=resume_value), config))
    else:
        raw = dict(graph.invoke({"session_id": session_id, "message": message, "job_id": job_id}, config))
        
    # Merge LangGraph-side LLM usage into API payload for the frontend footer.
    pl = dict(raw.get("payload") or {})
    lum = dict(pl.get("llm_usage") or {})
    ru = raw.get("router_llm_usage")
    if isinstance(ru, dict) and ru:
        lum["router"] = ru
    nlu = raw.get("nl_sql_llm_usage")
    if isinstance(nlu, dict) and nlu:
        lum["nl_sql"] = nlu
    if lum:
        pl["llm_usage"] = lum
        
    # Check if we were interrupted
    interrupts = raw.get("__interrupt__")
    if interrupts:
        first_interrupt = interrupts[0]
        val = first_interrupt.value
        if isinstance(val, dict):
            question = val.get("question")
            options = val.get("options") or []
            flow_options = [{"id": o, "text": o, "send": o} for o in options]
            return {
                "reply": question,
                "payload": {
                    "step": "convo_clarify",
                    "status": "paused",
                    "clarification_card": {
                        "question": question,
                        "options": options
                    },
                    "options": flow_options,
                    "thread_id": tid
                }
            }
            
    # Do not return the full graph state (especially `session` + last_assessment_result) as the
    # async job result: it can be megabytes and breaks polling via Next.js proxy (timeouts / aborts).
    out: Dict[str, Any] = {
        "reply": raw.get("reply") if isinstance(raw.get("reply"), str) else "",
        "payload": pl,
    }
    tid_out = raw.get("threadId") or tid
    if tid_out is not None:
        out["threadId"] = tid_out
    return out

