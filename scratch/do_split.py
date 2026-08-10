import os

here = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.dirname(here)
chat_graph_path = os.path.join(workspace_root, "agent", "chat_graph.py.bak")
target_dir = os.path.join(workspace_root, "agent", "chat_graph")
os.makedirs(target_dir, exist_ok=True)

with open(chat_graph_path, "r", encoding="utf-8") as f:
    lines = f.read().splitlines()

def extract_lines(start_1indexed, end_1indexed):
    return "\n".join(lines[start_1indexed-1 : end_1indexed])

# 1. state.py
state_content = f"""# State definition for the chat workflow.
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, TypedDict, Tuple

from agent.master_agent import load_sources_config
from agent.model_config import load_llm_config
from agent.openai_usage import usage_dict_from_response
from agent.session_store import add_experience, list_recent_experiences, load_session, save_session, SessionJSONEncoder

try:
    from langgraph.graph import END, StateGraph
except Exception:  # pragma: no cover
    END = None  # type: ignore
    StateGraph = None  # type: ignore

{extract_lines(28, 39)}
"""

with open(os.path.join(target_dir, "state.py"), "w", encoding="utf-8") as f:
    f.write(state_content)

# 2. helpers.py
helpers_content = f"""# Helper utilities for formatting and rendering responses.
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, Tuple

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

{extract_lines(41, 71)}
{extract_lines(72, 79).replace("def _first_location_index", "def _real_first_location_index")}

def _first_location_index(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_first_location_index", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _first_location_index.__code__):
        return func(*args, **kwargs)
    return _real_first_location_index(*args, **kwargs)

{extract_lines(195, 989)}
{extract_lines(1266, 1564)}
{extract_lines(4429, 4510).replace("def _load_sample_dfs_for_discovery", "def _real_load_sample_dfs_for_discovery")}

def _load_sample_dfs_for_discovery(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_load_sample_dfs_for_discovery", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _load_sample_dfs_for_discovery.__code__):
        return func(*args, **kwargs)
    return _real_load_sample_dfs_for_discovery(*args, **kwargs)
"""

with open(os.path.join(target_dir, "helpers.py"), "w", encoding="utf-8") as f:
    f.write(helpers_content)

# 3. llm.py
llm_content = f"""# LLM intent detection and plan generation.
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

{extract_lines(80, 174).replace("def _get_embeddings", "def _real_get_embeddings")}
{extract_lines(175, 194).replace("def _get_embeddings", "def _real_get_embeddings")}
{extract_lines(1741, 2091).replace("def _classify_intent_structured", "def _real_classify_intent_structured").replace("def _llm_plan", "def _real_llm_plan").replace("def _get_embeddings", "def _real_get_embeddings")}
"""

with open(os.path.join(target_dir, "llm.py"), "w", encoding="utf-8") as f:
    f.write(llm_content)

# 4. nodes.py
nodes_content = f"""# LangGraph node implementations.
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, Tuple

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

def _classify_intent_structured(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_classify_intent_structured", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _classify_intent_structured.__code__):
        return func(*args, **kwargs)
    from agent.chat_graph.llm import _classify_intent_structured as wrapper_func
    return wrapper_func(*args, **kwargs)

def _load_sample_dfs_for_discovery(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_load_sample_dfs_for_discovery", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _load_sample_dfs_for_discovery.__code__):
        return func(*args, **kwargs)
    from agent.chat_graph.helpers import _load_sample_dfs_for_discovery as wrapper_func
    return wrapper_func(*args, **kwargs)

def _llm_plan(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_llm_plan", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _llm_plan.__code__):
        return func(*args, **kwargs)
    from agent.chat_graph.llm import _llm_plan as wrapper_func
    return wrapper_func(*args, **kwargs)

# Import helper functions
from agent.chat_graph.helpers import (
    _flow_options, _prompt_choose_action, _first_location_index,
    _render_report_markdown, _render_report_html, _override_source_root_for_datasets,
    _theme_wrap_html, _html_table, _md_escape, _make_validation,
    _validate_schema_markdown, _validate_metadata_markdown, _validate_report_payload,
    _build_report_tables_markdown, _write_report_artifacts, _pick_single_active_dataset,
    _dataset_null_percent_rank, _assessment_signature, _router_assessment_hints,
    _ensure_latest_assessment, _user_asks_selection_status, _user_wants_narrative_report_summary,
    _user_asks_relationships_focus, _truncate_summary_text, _markdown_narrative_assessment_summary,
    _cardinality_glossary_line
)

# Import LLM components
from agent.chat_graph.llm import (
    _MASTER_SYSTEM, UserIntent, SEMANTIC_UTTERANCES, _EMBEDDING_CACHE,
    _get_embeddings, get_semantic_match, _get_llm_client_langchain,
    map_intent_to_action
)

{extract_lines(991, 1263)}
{extract_lines(1567, 1730)}
{extract_lines(2092, 4428)}
{extract_lines(4513, 4558)}
"""

with open(os.path.join(target_dir, "nodes.py"), "w", encoding="utf-8") as f:
    f.write(nodes_content)

# 5. graph.py
graph_raw = extract_lines(4559, 4829)
graph_raw = graph_raw.replace(
    "def build_chat_graph(checkpointer=None):",
    "def build_chat_graph(checkpointer=None):\n    import agent.chat_graph\n    for _n in dir(agent.chat_graph):\n        if _n.startswith('_node_'):\n            globals()[_n] = getattr(agent.chat_graph, _n)"
)

graph_content = f"""# Chat workflow graph construction and entry points.
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

{graph_raw}
"""

with open(os.path.join(target_dir, "graph.py"), "w", encoding="utf-8") as f:
    f.write(graph_content)

# 6. __init__.py
init_content = """# Re-export key components of chat_graph to preserve backward compatibility.
from __future__ import annotations

# Re-export session store and other functions for patching in tests
from agent.session_store import load_session, save_session, add_experience, list_recent_experiences, SessionJSONEncoder
from agent.model_config import load_llm_config

from agent.chat_graph.state import ChatState
from agent.chat_graph.helpers import (
    _flow_options,
    _prompt_choose_action,
    _first_location_index,
    _render_report_markdown,
    _render_report_html,
    _override_source_root_for_datasets,
    _theme_wrap_html,
    _html_table,
    _md_escape,
    _make_validation,
    _validate_schema_markdown,
    _validate_metadata_markdown,
    _validate_report_payload,
    _build_report_tables_markdown,
    _write_report_artifacts,
    _pick_single_active_dataset,
    _dataset_null_percent_rank,
    _assessment_signature,
    _router_assessment_hints,
    _ensure_latest_assessment,
    _user_asks_selection_status,
    _user_wants_narrative_report_summary,
    _user_asks_relationships_focus,
    _truncate_summary_text,
    _markdown_narrative_assessment_summary,
    _cardinality_glossary_line,
    _load_sample_dfs_for_discovery,
)
from agent.chat_graph.llm import (
    _MASTER_SYSTEM,
    UserIntent,
    SEMANTIC_UTTERANCES,
    _EMBEDDING_CACHE,
    _get_embeddings,
    get_semantic_match,
    _get_llm_client_langchain,
    _classify_intent_structured,
    map_intent_to_action,
    _llm_plan,
)
from agent.chat_graph.nodes import (
    _node_show_cleaning_recommendations,
    _node_show_transform_suggestions,
    _node_show_null_columns,
    _node_dq_overview,
    _node_dq_duplicates,
    _node_relationships_overview,
    _node_summarize_report,
    _node_extract_columns,
    _node_load_session,
    _node_route,
    _node_show_selection_status,
    _node_help,
    _node_reset_flow,
    _node_back_flow,
    _node_list_sources,
    _node_select_source,
    _node_set_action,
    _azure_blob_locations,
    _node_list_blob_files,
    _node_select_blob_files,
    _filesystem_locations,
    _node_list_local_files,
    _node_select_local_files,
    _selected_file_mode_and_names,
    _reset_file_preview_paging,
    _node_preview_selected_file,
    _node_show_file_schema,
    _node_show_file_metadata,
    _node_generate_report_selected_files,
    _node_assess_selected_local_files,
    _node_assess_selected_files,
    _parse_view_mode,
    _node_preview_local_file,
    _node_preview_blob_file,
    _node_list_tables,
    _node_select_tables,
    _node_assess_selected_tables,
    _node_generate_report_selected,
    _node_select_table,
    _node_show_schema,
    _node_preview_table,
    _node_show_metadata,
    _node_dq_table,
    _node_nl_query,
    _node_save_session,
    _convo_followup_options,
    _apply_formatter,
    _node_convo_top_issues,
    _node_convo_issue_filter,
    _node_convo_triage,
    _node_convo_cross_dataset,
    _node_convo_clarify,
    _node_convo_boundary_ood,
    _node_convo_boundary_adv,
    _node_convo_etl_guidance,
    _node_build_etl_plan,
    _node_generate_etl_code,
    _node_show_etl_plan,
    _node_confirm_etl_plan,
    _node_capture_business_rules,
    _node_download_etl_code,
    _node_discover_semantic_rules,
)
from agent.chat_graph.graph import build_chat_graph, run_chat

__all__ = [
    "ChatState",
    "build_chat_graph",
    "run_chat",
    "load_session",
    "save_session",
    "add_experience",
    "list_recent_experiences",
    "SessionJSONEncoder",
    "load_llm_config",
]
"""

with open(os.path.join(target_dir, "__init__.py"), "w", encoding="utf-8") as f:
    f.write(init_content)

print("Split completed successfully!")


with open(os.path.join(target_dir, "__init__.py"), "w", encoding="utf-8") as f:
    f.write(init_content)

print("Split completed successfully!")
