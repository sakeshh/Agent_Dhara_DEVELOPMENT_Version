# LLM intent detection and plan generation.
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, Tuple, Literal
from pydantic import BaseModel, Field
import numpy as np
import os

try:
    from langgraph.types import interrupt, Command
except Exception:
    interrupt = None
    Command = None

import agent.chat_graph
from agent.chat_graph.state import ChatState
from agent.master_agent import load_sources_config
from agent.model_config import load_llm_config
from agent.openai_usage import usage_dict_from_response
from agent.session_store import add_experience, list_recent_experiences, SessionJSONEncoder

def load_session(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "load_session", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != load_session.__code__):
        return func(*args, **kwargs)
    from agent.session_store import load_session as real_load
    return real_load(*args, **kwargs)

def save_session(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "save_session", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != save_session.__code__):
        return func(*args, **kwargs)
    from agent.session_store import save_session as real_save
    return real_save(*args, **kwargs)

def _get_embeddings(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_get_embeddings", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _get_embeddings.__code__):
        return func(*args, **kwargs)
    return _real_get_embeddings(*args, **kwargs)

def _classify_intent_structured(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_classify_intent_structured", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _classify_intent_structured.__code__):
        return func(*args, **kwargs)
    return _real_classify_intent_structured(*args, **kwargs)

def _llm_plan(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_llm_plan", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _llm_plan.__code__):
        return func(*args, **kwargs)
    return _real_llm_plan(*args, **kwargs)

# Import helpers needed by LLM components
from agent.chat_graph.helpers import (
    _flow_options, _prompt_choose_action, _ensure_latest_assessment
)

_MASTER_SYSTEM = """You are Agent Dhara’s Master (Supervisor) router for **data exploration + data quality only**.
You MUST return ONLY valid JSON and nothing else.

Your job:
- Understand the user request in natural language.
- Decide what action to take next (route to the right "agent": extraction vs data quality vs navigation).
- Provide the minimal arguments needed to execute it.

CORE PRODUCT RULES (must obey when choosing actions):
- Answer the user’s **stated intent** with the **smallest** action that satisfies it. Prefer `summarize_report` for narrative “explain the report”, and DQ slice actions (`show_null_columns`, `dq_duplicates`, `dq_overview`) for narrow checks — do **not** default to huge prose unless the user clearly wants a full narrative.
- Vague deictics (“this”, “too”, “fix this”) without a clear object → `show_selection_status` or `help` (ask what to operate on), not `summarize_report`.
- Stocks / general coding / sports / trivia are **out of scope** → `help` with a short refusal tone is acceptable if no better action exists.
- Never instruct downstream agents to invent data-quality issues or contradict a saved assessment verdict.

Allowed actions (exact strings):
help
reset_flow
back_flow
set_action
list_sources
select_source
list_tables
select_tables
select_table
show_schema
preview_table
nl_query
dq_table
show_null_columns
extract_columns
dq_overview
dq_duplicates
summarize_report
relationships_overview
list_blob_files
select_blob_files
assess_selected_files
list_local_files
select_local_files
assess_selected_local_files
assess_selected_tables
preview_local_file
preview_blob_file
show_selection_status
convo_etl_guidance
build_etl_plan
generate_etl_code
show_etl_plan
confirm_etl_plan
capture_business_rules
download_etl_code
discover_semantic_rules

Output schema:
{
  "action": "<one allowed action>",
  "args": { ... }
}

Argument rules:
- For selections, prefer numeric indices when available lists are provided.
- If the user references a specific name (table/blob/file), you may pass it directly by name.
- Never invent sources/tables/files that are not listed in the provided context.

Behavior rules:
- If the user says "restart", choose action=reset_flow.
- If the user says "back", choose action=back_flow.
- If the user picks an action ("view data" or "generate report"), choose action=set_action with {"action":"view"} or {"action":"report"}.
- If the user asks to "run data quality assessment" or "check data quality issues" for the *currently selected blob files*,
  choose action=assess_selected_files.
- If the user asks to assess the *currently selected local files*, choose action=assess_selected_local_files.
- If the user asks to assess the *currently selected tables*, choose action=assess_selected_tables.
- If the user ONLY asks how many / which items are *selected*, or what the current selection is (with no DQ/report ask),
  choose show_selection_status.
- If the user asks you to summarize, explain in plain English, or give an executive summary of THE REPORT / assessment / findings,
  choose summarize_report (not dq_overview).
- If the user asks about relationships between datasets/files, cardinality (one-to-many, many-to-one, etc.), how tables link or join,
  foreign keys, overlaps between keys, or orphan / dangling key hints, choose relationships_overview (not dq_overview).
- If the user says "build ETL plan", "create transformation plan", or "plan the ETL", choose action=build_etl_plan.
- If the user says "generate ETL code", "generate transformations", "create cleaning script", or similar,
  choose action=generate_etl_code.
- If the user says "show ETL plan" or "what transformations are planned", choose action=show_etl_plan.
- If the user says "approve" or "approve the plan", choose action=confirm_etl_plan.
- If the user says "modify the plan", choose action=confirm_etl_plan with plan overrides in args when possible.
- If the user says "download ETL code" or "download the script", choose action=download_etl_code.
- If the user asks how to fix data in SQL without generating a full pipeline, choose convo_etl_guidance.
- If the user asks a data-quality question (nulls, duplicates, outliers, per-dataset issue totals) AFTER a report was generated,
  choose a DQ action (dq_overview / show_null_columns / dq_duplicates) and answer from the latest assessment.
- If the user asks for extraction (show columns, show top rows, preview data) for selected datasets, choose an extraction action.
- If the user asks to discover semantic rules, discover rules, or generate semantic rules, choose action=discover_semantic_rules.

Examples (JSON only):
{"action":"list_sources","args":{}}
{"action":"select_source","args":{"index":0}}
{"action":"list_tables","args":{}}
{"action":"select_tables","args":{"indices":[1,3,4]}}
{"action":"assess_selected_tables","args":{}}
{"action":"list_blob_files","args":{}}
{"action":"select_blob_files","args":{"all":true}}
{"action":"assess_selected_files","args":{}}
{"action":"dq_overview","args":{}}
{"action":"summarize_report","args":{}}
{"action":"relationships_overview","args":{}}
{"action":"show_selection_status","args":{}}
{"action":"extract_columns","args":{}}
{"action":"convo_etl_guidance","args":{}}
{"action":"build_etl_plan","args":{"engine":"python"}}
{"action":"generate_etl_code","args":{"engine":"python"}}
{"action":"show_etl_plan","args":{}}
{"action":"confirm_etl_plan","args":{}}
{"action":"download_etl_code","args":{}}
{"action":"discover_semantic_rules","args":{}}
"""


class UserIntent(BaseModel):
    category: Literal["profile", "clean", "etl_gen", "explain", "status", "chat", "unclear"] = Field(
        description="The primary classified intent of the user message"
    )
    target_table: Optional[str] = Field(description="Name of the table or file referenced, if any")
    confidence: float = Field(description="Confidence score between 0.0 and 1.0")
    clarification_needed: bool = Field(description="True if the intent is ambiguous or needs user clarification")
    clarification_question: Optional[str] = Field(description="A friendly clarifying question to ask the user")
    suggested_options: Optional[List[str]] = Field(description="List of choice buttons (options) to present to the user")

SEMANTIC_UTTERANCES = {
    "view data": ("set_action", {"action": "view"}),
    "preview table data": ("preview_table", {"n": 10}),
    "generate data quality report": ("generate_report_selected", {}),
    "restart session": ("reset_flow", {}),
    "go back one step": ("back_flow", {}),
    "show column schemas": ("show_schema", {}),
    "show table metadata and rows": ("show_metadata", {}),
    "show nulls and missing values": ("show_null_columns", {}),
    "find duplicate records": ("dq_duplicates", {}),
    "cleaning recommendations": ("show_cleaning_recommendations", {}),
    "suggested transformations": ("show_transform_suggestions", {}),
    "show relationships and foreign keys": ("relationships_overview", {}),
    "show current table selection": ("show_selection_status", {}),
    "build etl transformation plan": ("build_etl_plan", {}),
    "generate cleaning etl code": ("generate_etl_code", {}),
    "download generated etl code": ("download_etl_code", {}),
    "what does this column mean": ("discover_semantic_rules", {}),
    "discover semantic rules": ("discover_semantic_rules", {}),
}

_EMBEDDING_CACHE = {}

def _real_get_embeddings():
    cfg = load_llm_config(purpose="router")
    if not cfg:
        return None
    try:
        if cfg.provider == "azure_openai":
            from langchain_openai import AzureOpenAIEmbeddings
            dep = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT") or "text-embedding-ada-002"
            return AzureOpenAIEmbeddings(
                azure_endpoint=cfg.endpoint,
                azure_deployment=dep,
                api_version=cfg.api_version or "2024-02-01",
                api_key=cfg.api_key,
            )
        else:
            from langchain_openai import OpenAIEmbeddings
            model = os.getenv("OPENAI_EMBEDDINGS_MODEL") or "text-embedding-ada-002"
            return OpenAIEmbeddings(
                api_key=cfg.api_key,
                model=model,
            )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not initialize embeddings: {e}")
        return None

def get_semantic_match(user_message: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    global _EMBEDDING_CACHE
    try:
        embeddings = _get_embeddings()
        if not embeddings:
            return None
            
        if not _EMBEDDING_CACHE:
            phrases = list(SEMANTIC_UTTERANCES.keys())
            phrase_vectors = embeddings.embed_documents(phrases)
            for phrase, vec in zip(phrases, phrase_vectors):
                _EMBEDDING_CACHE[phrase] = np.array(vec)
                
        user_vec = np.array(embeddings.embed_query(user_message))
        best_phrase = None
        best_score = -1.0
        
        for phrase, vec in _EMBEDDING_CACHE.items():
            score = np.dot(user_vec, vec) / (np.linalg.norm(user_vec) * np.linalg.norm(vec))
            if score > best_score:
                best_score = score
                best_phrase = phrase
                
        import logging
        logging.getLogger(__name__).info(f"Semantic router: best match phrase='{best_phrase}' with score={best_score:.4f}")
        
        if best_score > 0.88:
            return SEMANTIC_UTTERANCES[best_phrase]
            
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Semantic pre-routing failed: {e}")
        
    return None

def _get_llm_client_langchain():
    cfg = load_llm_config(purpose="router")
    if cfg is None:
        return None
    try:
        if cfg.provider == "azure_openai":
            from langchain_openai import AzureChatOpenAI
            return AzureChatOpenAI(
                azure_endpoint=cfg.endpoint,
                azure_deployment=cfg.model,
                api_version=cfg.api_version or "2024-02-01",
                api_key=cfg.api_key,
                temperature=0.0,
                max_tokens=300,
            )
        else:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                api_key=cfg.api_key,
                model=cfg.model,
                temperature=0.0,
                max_tokens=300,
            )
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("LangChain LLM client unavailable: %s", exc)
        return None

def _real_classify_intent_structured(user_message: str, session: Dict[str, Any]) -> Optional[UserIntent]:
    llm = _get_llm_client_langchain()
    if llm is None:
        return None
    try:
        structured_llm = llm.with_structured_output(UserIntent)
        
        ctx = (session or {}).get("context", {}) if isinstance(session, dict) else {}
        selected_tables = ctx.get("selected_tables") or []
        available_tables = ctx.get("last_table_list") or []
        selected_locals = ctx.get("selected_local_files") or []
        available_locals = ctx.get("last_local_file_list") or []
        selected_blobs = ctx.get("selected_blob_files") or []
        available_blobs = ctx.get("last_blob_list") or []
        
        options_list = list(set(available_tables + available_locals + available_blobs))
        
        system_prompt = f"""You are Agent Dhara's conversational intent routing supervisor.
Based on the user's message and the session context, classify their intent and identify target table/file.

Categories:
- "profile": user wants to explore/run assessment/generate a report on datasets/tables/files.
- "clean": user wants to clean data or get cleaning/transformation recommendations.
- "etl_gen": user wants to build ETL plans, generate code, download code, or plan pipelines.
- "explain": user asks to explain a report, column meanings, business rules, or relationships between datasets.
- "status": user wants to see what's currently selected or selection status.
- "chat": general chatter, greetings, hello, help menu.
- "unclear": anything ambiguous, out-of-domain, or needing clarification.

Context:
- Selected tables: {selected_tables}
- Available tables: {available_tables}
- Selected local files: {selected_locals}
- Available local files: {available_locals}
- Selected blob files: {selected_blobs}
- Available blob files: {available_blobs}

Instructions:
1. If the user message references an action on a dataset/table/file, check if they specified which one.
2. If multiple datasets/tables/files are available but none or too many are selected, or if the target is ambiguous, set:
   - `clarification_needed = True`
   - `clarification_question = "Which table or dataset do you want to work on?"`
   - `suggested_options = {options_list}`
3. If they specified a table/file (or it's already selected/unambiguous), set target_table and set clarification_needed = False.
4. Keep the target_table name matching exactly the name in the available list.
5. Focus only on data validation, profiling, cleaning, and ETL.
"""
        from langchain_core.messages import SystemMessage, HumanMessage
        return structured_llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message)
        ])
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Structured intent routing failed: {e}")
        return None

def map_intent_to_action(intent: UserIntent, msg: str, ctx: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    if intent.clarification_needed:
        orig = "help"
        if intent.category == "clean":
            orig = "show_cleaning_recommendations"
        elif intent.category == "profile":
            orig = "generate_report_selected" if not ctx.get("selected_blob_files") else "generate_report_selected_files"
        elif intent.category == "explain":
            orig = "summarize_report"
        
        return "convo_clarify", {
            "question": intent.clarification_question or "Please clarify which table you want to work on.",
            "options": intent.suggested_options or [],
            "original_action": orig
        }
        
    low = msg.lower()
    
    if intent.category == "status":
        return "show_selection_status", {}
        
    if intent.category == "chat":
        return "help", {}
        
    if intent.target_table:
        tables = ctx.get("last_table_list") or []
        blobs = ctx.get("last_blob_list") or []
        locals_list = ctx.get("last_local_file_list") or []
        
        matched_name = None
        for t in tables:
            if t.lower() == intent.target_table.lower():
                matched_name = t
                ctx["selected_table"] = t
                ctx["selected_tables"] = [t]
                break
        if not matched_name:
            for b in blobs:
                if b.lower() == intent.target_table.lower():
                    matched_name = b
                    ctx["selected_blob_files"] = [b]
                    break
        if not matched_name:
            for l in locals_list:
                if l.lower() == intent.target_table.lower():
                    matched_name = l
                    ctx["selected_local_files"] = [l]
                    break
            
    if intent.category == "profile":
        has_files = bool(ctx.get("selected_blob_files") or ctx.get("selected_local_files"))
        if "schema" in low:
            return "show_file_schema" if has_files else "show_schema", {}
        if "meta" in low:
            return "show_file_metadata" if has_files else "show_metadata", {}
        if "preview" in low or "row" in low or "view" in low:
            return "preview_selected_file" if has_files else "preview_table", {"n": 10}
        return "generate_report_selected_files" if has_files else "generate_report_selected", {}
        
    if intent.category == "clean":
        if "suggest" in low or "transform" in low or "fix" in low:
            return "show_transform_suggestions", {}
        return "show_cleaning_recommendations", {}
        
    if intent.category == "explain":
        if "relationship" in low or "join" in low or "cardinality" in low or "foreign" in low:
            return "relationships_overview", {}
        if "rule" in low or "discover" in low or "semantic" in low:
            return "discover_semantic_rules", {}
        return "summarize_report", {}
        
    if intent.category == "etl_gen":
        if "download" in low or "script" in low:
            return "download_etl_code", {}
        if "approve" in low or "confirm" in low or "yes" in low:
            return "confirm_etl_plan", {}
        if "show" in low or "plan" in low:
            return "show_etl_plan", {}
        if "generate" in low or "code" in low or "clean" in low:
            return "generate_etl_code", {}
        return "build_etl_plan", {}
        
    return "help", {}

def _real_llm_plan(*, user_text: str, session: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load_llm_config(purpose="router")
    if not cfg:
        raise RuntimeError(
            "LLM routing is required but not configured. Set AZURE_OPENAI_ENDPOINT/AZURE_OPENAI_API_KEY/"
            "AZURE_OPENAI_DEPLOYMENT (and optional AZURE_OPENAI_API_VERSION) for Foundry/Azure OpenAI."
        )
    user = (user_text or "").strip()
    if not user:
        return {"action": "help", "args": {}}

    ctx = (session or {}).get("context", {}) if isinstance(session, dict) else {}
    sid = str((session or {}).get("session_id") or "default")
    # Keep context compact but useful for the model.
    context_summary = {
        "selected_source_index": ctx.get("selected_source_index"),
        "selected_db_location_index": ctx.get("selected_db_location_index"),
        "selected_blob_location_index": ctx.get("selected_blob_location_index"),
        "selected_fs_location_index": ctx.get("selected_fs_location_index"),
        "selected_table": ctx.get("selected_table"),
        "selected_tables_count": len(ctx.get("selected_tables") or []),
        "selected_blob_files_count": len(ctx.get("selected_blob_files") or []),
        "selected_local_files_count": len(ctx.get("selected_local_files") or []),
    }

    def _head(lst: Any, n: int = 30) -> Any:
        if not isinstance(lst, list):
            return None
        return lst[:n]

    available_lists = {
        "last_table_list_head": _head(ctx.get("last_table_list"), 40),
        "last_blob_list_head": _head(ctx.get("last_blob_list"), 40),
        "last_local_file_list_head": _head(ctx.get("last_local_file_list"), 40),
    }

    memory = {
        "memory_summary": ctx.get("memory_summary"),
        "recent_experiences": list(reversed(list_recent_experiences(session_id=sid, limit=10))),
    }

    assessment_hints = _router_assessment_hints(ctx)

    payload_for_router: Dict[str, Any] = {
        "user_message": user,
        "context": context_summary,
        "available": available_lists,
        "memory": memory,
    }
    if assessment_hints:
        payload_for_router["last_assessment_hints"] = assessment_hints

    prompt = json.dumps(payload_for_router, ensure_ascii=False, cls=SessionJSONEncoder)
    try:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI  # type: ignore

            client = AzureOpenAI(api_key=cfg.api_key, api_version=cfg.api_version or "2024-02-01", azure_endpoint=cfg.endpoint)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _MASTER_SYSTEM}, {"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=220,
            )
        else:
            from openai import OpenAI  # type: ignore

            client = OpenAI(api_key=cfg.api_key)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _MASTER_SYSTEM}, {"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=220,
            )
        raw = (resp.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        action = str(obj.get("action") or "").strip()
        args = obj.get("args")
        if not isinstance(args, dict):
            args = {}
        if not action:
            return {"action": "help", "args": {}, "usage": usage_dict_from_response(resp)}
        return {"action": action, "args": args, "usage": usage_dict_from_response(resp)}
    except Exception as e:
        # If the model fails to produce JSON, return a helpful message via 'help'
        return {"action": "help", "args": {"error": str(e)}}


