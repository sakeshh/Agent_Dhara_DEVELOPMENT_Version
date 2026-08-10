"""
agent.chat_engine.assistant - Independent Agentic Chat Engine Service
Handles conversational AI interactions, live pipeline context awareness, and autonomous action execution.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("agent.chat_engine.assistant")

PIPELINE_STEPS_ORDER = [
  {"id": "database", "name": "Select Data Source"},
  {"id": "files", "name": "Select Tables / Files"},
  {"id": "fabric-setup", "name": "Fabric OneLake Landings"},
  {"id": "business-requirements", "name": "Business Requirements"},
  {"id": "assessment", "name": "Data Assessment & Profiling"},
  {"id": "report", "name": "DQ Gate & Assessment Report"},
  {"id": "semantics", "name": "Column Semantics Review"},
  {"id": "requirements", "name": "ETL Transformation Rules"},
  {"id": "etl", "name": "ETL Code Generation"},
  {"id": "cleaning", "name": "Execution & Fabric Mirroring"},
  {"id": "complete", "name": "Pipeline Complete"},
]

def format_pipeline_context_prompt(pipeline_context: Optional[Dict[str, Any]]) -> str:
    if not pipeline_context:
        return "No active data pipeline context loaded."

    current_step = pipeline_context.get("currentStep", "database")
    selected_db = pipeline_context.get("selectedDatabase")
    selected_files = pipeline_context.get("selectedFiles", [])
    dq_gate = pipeline_context.get("dqGate")
    has_etl = bool(pipeline_context.get("etlCode"))
    has_exec = bool(pipeline_context.get("execResult"))

    curr_idx = 0
    for idx, s in enumerate(PIPELINE_STEPS_ORDER):
        if s["id"] == current_step:
            curr_idx = idx
            break

    completed = [s["name"] for s in PIPELINE_STEPS_ORDER[:curr_idx]]
    remaining = [s["name"] for s in PIPELINE_STEPS_ORDER[curr_idx + 1:]]
    active = PIPELINE_STEPS_ORDER[curr_idx]["name"]

    prompt_lines = [
        "=== LIVE DATA PIPELINE WORKSPACE CONTEXT ===",
        f"Active Step: Step {curr_idx + 1} of {len(PIPELINE_STEPS_ORDER)} - {active}",
        f"Selected Data Source: {selected_db or 'None'}",
        f"Selected Tables/Files: {', '.join(selected_files) if selected_files else 'None'}",
        f"Completed Steps: {', '.join(completed) if completed else 'None'}",
        f"Remaining Steps: {', '.join(remaining) if remaining else 'None'}",
    ]

    if dq_gate:
        prompt_lines.append(f"Data Quality Score: {dq_gate.get('score', 'N/A')}% (Passed: {dq_gate.get('passed', False)})")
    if has_etl:
        prompt_lines.append("ETL Code: Generated and ready for execution.")
    if has_exec:
        prompt_lines.append("Remediation Status: Executed and mirrored to Microsoft Fabric OneLake Lakehouse.")

    prompt_lines.append("==========================================")
    return "\n".join(prompt_lines)


def detect_action_intent(user_input: str, current_step: str) -> Optional[Dict[str, Any]]:
    q = user_input.lower()
    if any(k in q for k in ["select sql", "choose sql", "use sql database", "sql source"]):
        return {"action": "select_source", "source": "src:sql:0", "label": "Select SQL Data Source"}
    if any(k in q for k in ["select blob", "choose blob", "use blob source"]):
        return {"action": "select_source", "source": "src:blob:0", "label": "Select Blob Data Source"}
    if any(k in q for k in ["run assessment", "start assessment", "profile data", "analyze quality"]):
        return {"action": "run_assessment", "label": "Run Data Assessment & Profiling"}
    if any(k in q for k in ["generate etl", "create sql procedure", "generate code", "build etl"]):
        return {"action": "generate_etl", "label": "Generate Non-Destructive T-SQL Stored Procedure"}
    if any(k in q for k in ["execute", "run remediation", "upload to fabric", "mirror to fabric", "clean data"]):
        return {"action": "execute_remediation", "label": "Execute SQL Remediation & Mirror to Fabric Lakehouse"}

    return None


def handle_chat_message(
    user_input: str,
    session_id: str = "default",
    user_id: str = "default_user",
    history: Optional[List[Dict[str, Any]]] = None,
    pipeline_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Process incoming chat assistant message with full pipeline context awareness and autonomous action execution intents.
    """
    logger.info(f"Processing agentic chat message for session '{session_id}': {user_input[:50]}")
    ctx_text = format_pipeline_context_prompt(pipeline_context)
    curr_step = pipeline_context.get("currentStep", "database") if pipeline_context else "database"
    detected_action = detect_action_intent(user_input, curr_step)

    try:
        from agent.master_agent import process_chat_message
        res = process_chat_message(user_input=user_input, session_id=session_id, user_id=user_id, history=history)
        if isinstance(res, dict):
            res["pipeline_context_summary"] = ctx_text
            if detected_action:
                res["action_intent"] = detected_action
            return res
    except Exception as e:
        logger.error(f"Chat assistant LLM error, using contextual fallback: {e}")

    # Rich Agentic Response with contextual step guidance & detected action
    response_lines = [
        f"I am your Agentic Copilot. I am monitoring your Data Pipeline state.\n\n{ctx_text}\n"
    ]

    if detected_action:
        response_lines.append(
            f"⚡ **Action Prepared**: I can execute **{detected_action['label']}** for you immediately. Click below to approve and run!"
        )
    else:
        response_lines.append(
            "How can I assist you with your pipeline? I can autonomously set up data sources, run data quality profiling, generate non-destructive T-SQL code, or execute remediation and upload clean data to Microsoft Fabric OneLake!"
        )

    return {
        "ok": True,
        "response": "\n".join(response_lines),
        "session_id": session_id,
        "conversation_id": session_id,
        "pipeline_context_summary": ctx_text,
        "action_intent": detected_action
    }
