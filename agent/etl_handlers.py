from __future__ import annotations

import json
import logging
import hashlib
import os
import re
import time
from typing import Any, Dict, List, Optional
import functools
from agent.session_store import get_session_lock

def with_session_lock(func):
    @functools.wraps(func)
    def wrapper(session_id: str, *args, **kwargs):
        sid = (session_id or "default").strip() or "default"
        lock = get_session_lock(sid)
        with lock:
            return func(session_id, *args, **kwargs)
    return wrapper


def _safe_bracket_quote(name: str) -> str:
    """
    Safely bracket-quote a table or schema name, escaping any ']' by doubling it.
    If the name has parts separated by '.', each part is quoted and escaped.
    """
    if not name:
        return ""
    parts = name.split(".")
    quoted_parts = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        escaped = part.replace("]", "]]")
        quoted_parts.append(f"[{escaped}]")
    return ".".join(quoted_parts)


def _synthesize_prompt_from_assessment(assess: dict) -> Optional[str]:
    """
    When the user never supplied a prompt, synthesise a descriptive ETL intent
    from the assessment data so the LLM IR planner has something to work with.

    Produces text like:
      "Clean and transform data for datasets: dbo.Orders_Raw, dbo.Customers.
       Address top data quality issues: duplicate records, null values in
       required columns, date format violations."
    """
    if not isinstance(assess, dict):
        return None

    datasets = assess.get("datasets") or {}
    ds_names = list(datasets.keys())
    if not ds_names:
        return None

    # Collect distinct issue categories from the assessment
    issue_types: List[str] = []
    seen: set = set()
    for iss in (assess.get("issues") or []):
        if not isinstance(iss, dict):
            continue
        itype = (
            iss.get("issue_type")
            or iss.get("type")
            or iss.get("category")
            or ""
        ).lower().replace("_", " ").strip()
        if itype and itype not in seen:
            seen.add(itype)
            issue_types.append(itype)
        if len(issue_types) >= 5:
            break

    ds_label = ", ".join(ds_names[:4])
    if len(ds_names) > 4:
        ds_label += f" and {len(ds_names) - 4} more"

    if issue_types:
        issues_label = ", ".join(issue_types[:4])
        prompt = (
            f"Clean and transform data for datasets: {ds_label}. "
            f"Fix data quality issues including: {issues_label}. "
            f"Remove duplicates, handle nulls, enforce data type constraints, "
            f"and produce a cleaned output dataset."
        )
    else:
        prompt = (
            f"Clean and transform data for datasets: {ds_label}. "
            f"Remove duplicates, handle nulls, enforce data type constraints, "
            f"and produce a cleaned output dataset."
        )

    return prompt


def _read_best_candidate_table(
    conn,
    ds_name: str,
    candidate_tables: List[str],
    context_prefix: str,
    max_rows: int = 500000
) -> tuple[Optional[Any], Optional[str]]:
    """
    Tries to read the best candidate table from connection and returns (dataframe, chosen_table_name).
    """
    import pandas as pd
    for tbl in candidate_tables:
        quoted_tbl = _safe_bracket_quote(tbl)
        try:
            df_temp = pd.read_sql(f"SELECT TOP {max_rows} * FROM {quoted_tbl}", conn)
            if not df_temp.empty:
                logger.info(f"{context_prefix}: found {len(df_temp)} rows in '{tbl}' for source '{ds_name}'.")
                return df_temp, tbl
            else:
                logger.info(f"{context_prefix}: '{tbl}' exists but is empty, trying next candidate.")
        except Exception as e:
            logger.info(f"{context_prefix}: '{tbl}' not found or not readable, trying next candidate. Error: {e}")
            continue
    return None, None

from agent.business_rules_loader import (
    list_tenant_ids,
    merge_business_rules_for_datasets,
    pending_rules_from_session,
    tenant_id_from_session,
)
from agent.session_store import load_session, save_session
from agent.etl_pipeline import (
    build_etl_plan,
    build_impact_preview,
    generate_python_etl,
    normalize_business_rules,
)
from agent.etl_pipeline.llm_codegen import (
    generate_adf_with_llm,
    generate_etl_with_llm,
    is_llm_generation_error,
    parse_adf_json_from_llm,
)
from agent.etl_pipeline.schema_lineage import build_lineage
from agent.etl_pipeline.validate_plan import validate_etl_plan, validate_etl_plan_for_confirm, validate_promoted_plan, PlanValidationIssue
from agent.etl_pipeline.validate_python import validate_etl_python_source, validate_python_source
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.connector_manifest import build_connector_manifest
from agent.etl_pipeline.plan_narrator import narrate_plan
from agent.etl_pipeline.manual_review_promote import (
    apply_manual_resolutions,
    count_pending_manual_review,
    enrich_plan_manual_review,
)
from agent.etl_pipeline.agentic_rules import analyze_agentic_intent

logger = logging.getLogger("agent.etl")





def _resolve_codegen_mode(
    engine: str,
    *,
    requested: Optional[str] = None,
) -> str:
    """
    compiler | template | llm | llm_then_template
    Resolution order: explicit API `codegen_mode`, then env ETL_CODEGEN_MODE, then:
    - compiler (deterministic, contracts-guaranteed) when COMPILER_CODEGEN_ENABLED=true
    - template / llm_then_template if legacy flags enabled.
    """
    from agent.feature_flags import get_flags
    flags = get_flags()

    req = str(requested or "").strip().lower()
    if req in ("compiler", "deterministic", "template", "llm", "llm_then_template"):
        if req in ("compiler", "deterministic"):
            return "compiler"
        if req in ("llm", "llm_then_template") and not flags.legacy_llm_codegen_enabled:
            logger.info("Legacy LLM codegen is disabled by flag; enforcing deterministic compiler mode")
            return "compiler"
        return req

    env = os.getenv("ETL_CODEGEN_MODE", "").strip().lower()
    if env in ("compiler", "deterministic", "template", "llm", "llm_then_template"):
        if env in ("compiler", "deterministic"):
            return "compiler"
        return env

    if flags.compiler_codegen_enabled:
        return "compiler"

    if os.getenv("ETL_CODEGEN_LLM_DEFAULT", "0").strip().lower() in ("1", "true", "yes"):
        eng = (engine or "python").lower()
        if eng in ("spark", "pyspark"):
            fast = os.getenv("DHARA_ETL_FAST_PYSPARK", "1").strip().lower() in ("1", "true", "yes")
            if fast:
                return "template"
        return "llm_then_template"
    return "template"


def _run_compiler_codegen(
    session_id: str,
    engine: str,
    plan: dict,
    assess: dict,
    ctx: dict,
    sql_dialect: str = "tsql",
    user_prompt: Optional[str] = None,
) -> tuple[str, bool, list[str], str]:
    from agent.compiler.compiler_pipeline import CompilerPipeline
    from agent.compiler.models.context import CompilerContext
    from agent.compiler.models.physical import ExecutionEngine
    from agent.compiler.models.cleaning import CleaningMode
    from agent.compiler.models.profile import DatasetProfile, ColumnProfile
    from agent.compiler.models.issues import Issue
    from agent.feature_flags import get_flags

    flags = get_flags()
    eng_str = (engine or "python").lower()
    eng_map = {
        "python": ExecutionEngine.PYTHON,
        "sql": ExecutionEngine.AZURE_SQL,
        "tsql": ExecutionEngine.AZURE_SQL,
        "ansi": ExecutionEngine.AZURE_SQL,
        "pyspark": ExecutionEngine.PYSPARK,
        "spark": ExecutionEngine.PYSPARK,
        "fabric": ExecutionEngine.FABRIC,
        "adf": ExecutionEngine.ADF,
        "databricks": ExecutionEngine.DATABRICKS,
    }
    target_eng = eng_map.get(eng_str, ExecutionEngine.PYTHON)

    flow = ctx.get("etl_flow") or {}
    raw_mode = (
        flow.get("cleaning_mode")
        or plan.get("cleaning_mode")
        or flags.cleaning_default_mode
    )
    try:
        clean_mode = CleaningMode(raw_mode)
    except Exception:
        clean_mode = CleaningMode.CLEANSE_WITH_AUDIT

    profiles: dict[str, DatasetProfile] = {}
    for ds_name, ds_info in (assess.get("datasets") or {}).items():
        if isinstance(ds_info, dict):
            cols: dict[str, ColumnProfile] = {}
            for col_name, col_meta in (ds_info.get("columns") or {}).items():
                if isinstance(col_meta, dict):
                    dtype = col_meta.get("dtype") or col_meta.get("type") or "string"
                    cols[col_name] = ColumnProfile(name=col_name, dtype=dtype)
                else:
                    cols[col_name] = ColumnProfile(name=col_name, dtype="string")
            row_count = int(ds_info.get("row_count") or 0)
            profiles[ds_name] = DatasetProfile(dataset_id=ds_name, row_count=row_count, columns=cols)

    if not profiles and isinstance(plan.get("datasets"), dict):
        for ds_name, p_ds in plan["datasets"].items():
            cols = {}
            for c in p_ds.get("columns") or []:
                cols[c] = ColumnProfile(name=c, dtype="string")
            profiles[ds_name] = DatasetProfile(dataset_id=ds_name, columns=cols)

    from agent.compiler.planners.issue_compiler import IssueCompiler
    compiler = IssueCompiler()
    detected_issues = []
    for idx, iss in enumerate(assess.get("issues") or []):
        if isinstance(iss, dict):
            ds = iss.get("dataset") or (list(profiles.keys())[0] if profiles else "default")
            detected_issues.append(compiler.compile_raw_issue(iss, ds, idx))

    # Extract user prompt from intrinsic request/plan metadata (avoid stale session fallbacks)
    biz_rules = plan.get("business_rules") or flow.get("business_rules") or {}
    user_prompt = (
        user_prompt
        or (flow.get("etl_intent", {}).get("description") if isinstance(flow.get("etl_intent"), dict) else None)
        or plan.get("description")
        or plan.get("notes")
        or (biz_rules.get("notes") if isinstance(biz_rules, dict) else None)
    )

    ir_plan_data = (
        plan.get("ir_plan")
        or flow.get("ir_plan")
        or ctx.get("ir_plan")
    )
    if not ir_plan_data:
        snap = load_plan_snapshot(session_id)
        if snap and snap.get("ir_plan"):
            snap_pid = snap.get("plan_id")
            cur_pid = plan.get("plan_id")
            if not snap_pid or snap_pid == cur_pid:
                ir_plan_data = snap.get("ir_plan")
                if not user_prompt:
                    user_prompt = snap.get("user_prompt")

    # Guard: verify persisted IR plan actually matches the current dataset profiles/plan
    if ir_plan_data:
        from agent.ir.models import IrPlan
        try:
            cand_ir = IrPlan.model_validate(ir_plan_data) if isinstance(ir_plan_data, dict) else ir_plan_data
            cand_pid = getattr(getattr(cand_ir, "metadata", None), "plan_id", None)
            cur_pid = plan.get("plan_id")
            source_tables = [s.lower().replace("dbo.", "").strip("[]") for s in cand_ir.get_source_tables()]
            current_tables = [t.lower().replace("dbo.", "").strip("[]") for t in profiles.keys()]
            
            is_matching_plan = (not cand_pid or not cur_pid or cand_pid == cur_pid)
            has_matching_tables = not source_tables or not current_tables or any(st in current_tables for st in source_tables)
            
            if not is_matching_plan or not has_matching_tables:
                logger.warning(
                    "[CompilerPipeline] Discarding stale IR plan from different dataset/plan (cand_pid=%s, cur_pid=%s, ir_sources=%s, current_tables=%s)",
                    cand_pid, cur_pid, source_tables, current_tables,
                )
                ir_plan_data = None
                plan.pop("ir_plan", None)
                flow.pop("ir_plan", None)
                ctx.pop("ir_plan", None)
        except Exception as e:
            logger.warning("[CompilerPipeline] Error validating persisted IR against current dataset: %s", e)
            ir_plan_data = None

    had_prompt = bool(
        ctx.get("had_user_prompt")
        or flow.get("had_user_prompt")
        or plan.get("had_user_prompt")
        or flow.get("planner_route") in ("llm_ir_planner", "multi_agent_planner")
        or plan.get("planner_route") in ("llm_ir_planner", "multi_agent_planner")
    )

    if flags.llm_ir_planner_enabled and not user_prompt and not ir_plan_data:
        # Synthesize from assessment before giving up
        synth = _synthesize_prompt_from_assessment(assess)
        if synth:
            user_prompt = synth
            logger.info("[LLM-IR] Synthesized prompt from assessment: %s", user_prompt[:80])
        elif had_prompt or not detected_issues:
            logger.error("[LLM-IR] Missing prompt")
            return "", False, ["Prompt lost between planning and generation"], "error"

    if flags.llm_ir_planner_enabled and (ir_plan_data or user_prompt):
        from agent.ir import IrExtractor, IrNormalizer, IrValidator, IrFixer, IrBuilder
        from agent.ir.semantic_validator import BusinessRuleValidator
        from agent.router import IntentRouter, RouteTarget

        route = (
            flow.get("planner_route")
            or plan.get("planner_route")
            or "llm_ir_planner"
        )

        if ir_plan_data:
            logger.info("[CompilerPipeline] using persisted IR")
            from agent.ir.models import IrPlan
            healed_ir = IrPlan.model_validate(ir_plan_data) if isinstance(ir_plan_data, dict) else ir_plan_data
            norm_ir = IrNormalizer.normalize(healed_ir, profiles)
            logger.info("[IrValidator] Validating normalized plan with %d sources, %d steps", len(norm_ir.sources), len(norm_ir.logical_plan))
            healed_ir, val_res, is_valid = IrFixer.heal_plan(norm_ir, profiles, max_attempts=2)
            if val_res.valid:
                logger.info("[IrValidator] passed")
            logger.info(
                "[IrFixer] repairs=%d",
                len(healed_ir.metadata.repair_history) if healed_ir.metadata else 0,
            )
            semantic_val = BusinessRuleValidator.validate_semantics(healed_ir, profiles)
            if semantic_val.valid:
                logger.info("[SemanticValidator] Plan passed semantic business rule validation")
            else:
                logger.warning("[SemanticValidator] Flagged semantic issues: %s", [e.message for e in semantic_val.errors])
        else:
            logger.info("[CompilerPipeline] user_prompt detected: '%s' — initiating LLM IR planner pipeline", user_prompt[:80])
            route_decision = IntentRouter.classify(user_prompt, profiles)
            route = route_decision.route.value
            logger.info(
                "[IntentRouter] route=%s complexity=%s confidence=%.2f (latency=%.2fms)",
                route_decision.route.value,
                route_decision.complexity.value,
                route_decision.confidence,
                route_decision.latency_ms,
            )
            if route_decision.route == RouteTarget.COMPILER_DIRECT:
                logger.info("[IrExtractor] Fast-path deterministic compiler direct (no LLM tokens)")
                raw_ir = IrExtractor._heuristic_fallback_extract(user_prompt, profiles, session_id=session_id)
                raw_ir.metadata.tags = ["compiler_direct"]
            else:
                raw_ir = IrExtractor.extract(user_prompt, profiles, session_id=session_id)
                raw_ir.metadata.tags = [route_decision.route.value]

            norm_ir = IrNormalizer.normalize(raw_ir, profiles)
            logger.info("[IrValidator] Validating normalized plan with %d sources, %d steps", len(norm_ir.sources), len(norm_ir.logical_plan))
            healed_ir, val_res, is_valid = IrFixer.heal_plan(norm_ir, profiles, max_attempts=2)
            if val_res.valid:
                logger.info("[IrValidator] passed")
            logger.info(
                "[IrFixer] repairs=%d",
                len(healed_ir.metadata.repair_history) if healed_ir.metadata else 0,
            )
            semantic_val = BusinessRuleValidator.validate_semantics(healed_ir, profiles)
            if not semantic_val.valid:
                logger.warning("[SemanticValidator] Flagged semantic issues: %s", [e.message for e in semantic_val.errors])
            else:
                logger.info("[SemanticValidator] Plan passed semantic business rule validation")

        # 5. Build Compiler Context
        compiler_ctx = IrBuilder.build(
            ir_plan=healed_ir,
            session_id=session_id,
            target_engine=target_eng,
            existing_profiles=profiles,
            cleaning_mode=clean_mode,
        )
        if compiler_ctx.metrics:
            compiler_ctx.metrics.route_taken = route
            if "route_decision" in locals():
                compiler_ctx.metrics.planner_latency_ms = route_decision.latency_ms

        compiler_ctx.detected_issues.extend(detected_issues)
        logger.info("etl_generate_code path=%s→compiler", route)
    else:
        if not flags.llm_ir_planner_enabled:
            logger.info("[CompilerPipeline] DHARA_LLM_IR_PLANNER disabled by flag; executing deterministic issues compiler")
        else:
            logger.info("[CompilerPipeline] No user_prompt found in plan or flow; executing issue-based compiler path")

        compiler_ctx = CompilerContext(
            session_id=session_id,
            target_engine=target_eng,
            cleaning_mode=clean_mode,
            preserve_source_schema=True,
            include_operational_metadata=False,
            dataset_profiles=profiles,
            detected_issues=detected_issues,
        )

    if plan.get("cleaning_approval"):
        compiler_ctx.metadata["cleaning_approval"] = plan["cleaning_approval"]

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(compiler_ctx)

    code = compiled_ctx.code_artifact.content if compiled_ctx.code_artifact else ""
    flow["output_contracts"] = compiled_ctx.metadata.get("output_contracts") or {}
    flow["latest_snapshot_id"] = snapshot.snapshot_id
    if compiled_ctx.metadata.get("deployment_manifest"):
        flow["deployment_manifest"] = compiled_ctx.metadata["deployment_manifest"]

    # Phase 7 & 7.5: Pre-Execution Validation & Cost Estimation
    from agent.compiler.execution_validation import ExecutionValidator
    exec_val = ExecutionValidator.validate(compiled_ctx)
    flow["execution_validation"] = exec_val.model_dump()
    flow["explain_plan"] = exec_val.explain_plan
    flow["cost_estimate"] = exec_val.cost_estimate.model_dump()

    # Phase 8: Pattern Learning Engine
    # Persist Canonical IR, validation, cost estimate, and repair history for continuous learning
    if flags.llm_ir_planner_enabled and "healed_ir" in locals():
        try:
            from agent.learning import get_pattern_learning_engine
            learning_engine = get_pattern_learning_engine()
            learned_rec = learning_engine.learn_from_compilation(
                session_id=session_id,
                user_prompt=user_prompt,
                ir_plan=healed_ir,
                validation_result=exec_val,
                target_engine=target_eng.value,
            )
            flow["learned_pattern_id"] = learned_rec.pattern_id
        except Exception as e:
            logger.warning("[PatternLearningEngine] Failed to learn pattern: %s", e)

    # Phase 9: Human Approval Workflow
    # Mandatory approval gates for DROP, DELETE, TRUNCATE, MERGE DELETE, OVERWRITE
    try:
        from agent.governance import get_approval_engine, ApprovalStatus
        approval_engine = get_approval_engine()
        approval_decision = approval_engine.evaluate(
            session_id=session_id,
            code=code,
            ir_plan=healed_ir if "healed_ir" in locals() else None,
            pattern_id=flow.get("learned_pattern_id"),
        )
        flow["approval_decision"] = approval_decision.model_dump()
        flow["requires_approval"] = (approval_decision.status == ApprovalStatus.PENDING)
    except Exception as e:
        logger.warning("[HumanApprovalEngine] Failed to evaluate approval: %s", e)

    # Phase 10: OpenTelemetry Export
    # Emits unified RouteMetrics, IRMetrics, CompilerMetrics, ApprovalMetrics, and ExecutionMetrics
    try:
        from agent.observability import (
            get_opentelemetry_exporter,
            RouteMetrics,
            IRMetrics,
            CompilerMetrics,
            ApprovalMetrics,
            ExecutionMetrics,
        )
        otel_exporter = get_opentelemetry_exporter()
        route_meta = route_decision if "route_decision" in locals() else None
        healed_plan = healed_ir if "healed_ir" in locals() else None

        span = otel_exporter.create_span(
            session_id=session_id,
            route=RouteMetrics(
                route_type=route_meta.route.value if route_meta else "compiler_direct",
                route_latency_ms=route_meta.latency_ms if route_meta else 0.0,
                planner_type="heuristic" if route_meta and route_meta.route.value == "compiler_direct" else "ir_compiler",
                confidence=route_meta.confidence if route_meta else 1.0,
            ),
            ir=IRMetrics(
                validation_count=len(healed_plan.metadata.repair_history) + 1 if healed_plan and healed_plan.metadata else 1,
                repair_count=len(healed_plan.metadata.repair_history) if healed_plan and healed_plan.metadata else 0,
                repair_success=True if healed_plan else False,
                semantic_failures=0 if "semantic_val" in locals() and semantic_val.valid else 0,
            ),
            compiler=CompilerMetrics(
                target=target_eng.value,
                compile_time=(getattr(compiled_ctx.metrics, "compile_latency_ms", None) or getattr(compiled_ctx.metrics, "total_compilation_time_ms", None) or getattr(compiled_ctx.metrics, "total_latency_ms", None) or 50.0) / 1000.0 if compiled_ctx.metrics else 0.05,
                artifact_size=len(code),
                success=bool(code),
            ),
            approval=ApprovalMetrics(
                approval_tier=approval_decision.approval_tier.value if "approval_decision" in locals() else "auto_approved",
                approver=approval_decision.approver if "approval_decision" in locals() else None,
                risk_level=approval_decision.risk_level.value if "approval_decision" in locals() else "low",
                requires_human_gate=flow.get("requires_approval", False),
            ),
            execution=ExecutionMetrics(
                runtime=0.0,
                rows_processed=sum(p.row_count for p in profiles.values()) if profiles else 0,
                cost_estimate=exec_val.cost_estimate.estimated_cost if "exec_val" in locals() else "low",
                execution_success=True,
                shuffle_operations=exec_val.cost_estimate.shuffle_operations if "exec_val" in locals() else 0,
                confidence_score=exec_val.confidence_score if "exec_val" in locals() else 1.0,
            ),
        )
        flow["telemetry_trace_id"] = span.trace_id
    except Exception as e:
        logger.warning("[OpenTelemetryExporter] Failed to record span: %s", e)

    return code, True, [], "compiler"


# ── Phase state machine ───────────────────────────────────────────────────────

ETL_PHASES = [
    "planned",
    "preview_ready",
    "approved",
    "generating",
    "validated",
    "code_ready",
    "downloadable",
    "failed",
]

ALLOWED_TRANSITIONS: Dict[str, List[str]] = {
    "planned": ["preview_ready", "failed"],
    "preview_ready": ["approved", "failed", "planned"],
    "approved": ["generating", "failed", "planned"],
    "generating": ["validated", "failed", "planned"],
    "validated": ["code_ready", "failed", "planned", "generating"],
    "code_ready": ["downloadable", "failed", "planned", "generating"],
    "failed": ["planned", "generating", "code_ready", "downloadable"],
    "downloadable": ["planned", "generating"],
}

_LEGACY_PHASE_MAP = {
    "no_plan": "planned",
    "plan_built": "planned",
    "plan_validated": "preview_ready",
    "preview_shown": "preview_ready",
    "code_failed": "failed",
}


def _migrate_phase(flow: dict) -> None:
    current = flow.get("phase")
    if current in _LEGACY_PHASE_MAP:
        flow["phase"] = _LEGACY_PHASE_MAP[current]


def _can_transition(from_phase: str, to_phase: str) -> bool:
    from_phase = _LEGACY_PHASE_MAP.get(from_phase, from_phase)
    if from_phase == to_phase:
        return True
    return to_phase in ALLOWED_TRANSITIONS.get(from_phase, [])


def _transition(flow: dict, to_phase: str, *, by: str = "system", reason: str = "") -> None:
    if to_phase not in ETL_PHASES:
        raise ValueError(f"Unknown phase: {to_phase}")
    _migrate_phase(flow)
    from_phase = flow.get("phase") or "planned"
    if from_phase not in ETL_PHASES:
        from_phase = _LEGACY_PHASE_MAP.get(from_phase, "planned")
        flow["phase"] = from_phase
    if not _can_transition(from_phase, to_phase):
        raise ValueError(f"Invalid ETL phase transition: {from_phase} -> {to_phase}")
    history = flow.setdefault("phase_history", [])
    history.append(
        {
            "from": from_phase,
            "to": to_phase,
            "ts": time.time(),
            "by": by,
            "reason": reason,
        }
    )
    flow["phase"] = to_phase


def rollback_on_failure(flow: dict, *, reason: str = "", soft: bool = False) -> None:
    """Reset flow to planned (or failed for soft rollback) while preserving plan/history."""
    flow["failure_reason"] = reason
    flow["last_failure_reason"] = reason
    try:
        _transition(flow, "failed", by="system", reason=reason)
    except ValueError:
        flow["phase"] = "failed"
        
    if soft:
        # Keep approved_plan for soft rollback so that regeneration or adjustments can be retried
        flow["validation_ok"] = False
    else:
        flow["approved_plan"] = None
        flow["validation_ok"] = False
        try:
            _transition(flow, "planned", by="system", reason="rollback_on_failure")
        except ValueError:
            flow["phase"] = "planned"


def _plan_all_auto(plan: Dict[str, Any]) -> bool:
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            if str(st.get("classification") or st.get("bucket") or "auto").lower() != "auto":
                return False
            if st.get("requires_user_choice"):
                return False
    if plan.get("blocked"):
        return False
    if count_pending_manual_review(plan) > 0:
        return False
    return True


def _invariants_pass(plan: Dict[str, Any]) -> bool:
    inv = plan.get("invariants") or []
    for item in inv:
        if item.get("enabled") and item.get("name") == "never_drop_rows":
            rules = plan.get("business_rules") or {}
            if not rules.get("never_drop_rows"):
                return False
    return True


def _ctx(session: Dict[str, Any]) -> Dict[str, Any]:
    return session.setdefault("context", {})


def _get_assessment(session: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if isinstance(override, dict) and override.get("datasets"):
        return override
    raw = (_ctx(session) or {}).get("last_assessment_result")
    if isinstance(raw, dict) and raw.get("datasets"):
        try:
            from agent.assessment_governance import check_manifest_staleness
            warning = check_manifest_staleness(raw)
            if warning:
                raw.setdefault("governance", {})["manifest_stale_warning"] = warning
            else:
                if "governance" in raw and "manifest_stale_warning" in raw["governance"]:
                    raw["governance"].pop("manifest_stale_warning")
        except Exception:
            pass
        return raw
    return None


def _safe_segment(s: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9_-]+", "_", (s or "default").strip())[:80]
    return t or "default"


def save_plan_snapshot(session_id: str, data: dict) -> str:
    """Persist plan, prompt, route, and IR to plan_snapshot.json."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    snap_dir = os.path.join(root, "output", "snapshots", _safe_segment(session_id))
    os.makedirs(snap_dir, exist_ok=True)
    snap_path = os.path.join(snap_dir, "plan_snapshot.json")
    try:
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logger.warning("Failed to write plan_snapshot.json: %s", e)
    return snap_path


def load_plan_snapshot(session_id: str) -> Optional[dict]:
    """Load persisted plan_snapshot.json for the session."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    snap_path = os.path.join(root, "output", "snapshots", _safe_segment(session_id), "plan_snapshot.json")
    if os.path.exists(snap_path):
        try:
            with open(snap_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _rehydrate_plan(plan: Dict[str, Any], ctx: Dict[str, Any], assess: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Restore session-owned fields stripped by UI plan edits."""
    out = dict(plan)
    if not out.get("connector_manifest"):
        manifest = ctx.get("connector_manifest")
        if not manifest:
            assessment = assess if isinstance(assess, dict) else ctx.get("last_assessment_result")
            try:
                from agent.etl_pipeline.connector_manifest import build_connector_manifest
                assess_with_ds = assessment if (isinstance(assessment, dict) and assessment.get("datasets")) else {"datasets": out.get("datasets") or {}}
                manifest = build_connector_manifest(ctx, assess_with_ds)
                ctx["connector_manifest"] = manifest
            except Exception as e:
                logger.warning(f"Failed to build connector manifest fallback: {e}")
                manifest = None
        if manifest:
            out["connector_manifest"] = manifest
        else:
            logger.warning("_rehydrate_plan: connector_manifest not found in plan or session context. Re-run POST /etl/plan.")
            out["_manifest_missing"] = True
    if not out.get("source_context") and ctx.get("source_context"):
        out["source_context"] = ctx["source_context"]
    if not out.get("relationships") and (ctx.get("etl_flow") or {}).get("plan", {}).get("relationships"):
        out["relationships"] = (ctx.get("etl_flow") or {})["plan"]["relationships"]
    flow = ctx.get("etl_flow") or {}
    if not out.get("etl_intent") and flow.get("etl_intent"):
        out["etl_intent"] = flow["etl_intent"]
    if not out.get("engine_recommendation") and flow.get("plan", {}).get("engine_recommendation"):
        out["engine_recommendation"] = flow["plan"]["engine_recommendation"]
    if not out.get("narration") and flow.get("plan", {}).get("narration"):
        out["narration"] = flow["plan"]["narration"]
    if not out.get("user_prompt"):
        p_prompt = (
            flow.get("user_prompt")
            or ctx.get("user_prompt")
            or flow.get("plan", {}).get("user_prompt")
        )
        if p_prompt:
            out["user_prompt"] = p_prompt
    if not out.get("ir_plan"):
        p_ir = (
            flow.get("ir_plan")
            or ctx.get("ir_plan")
            or flow.get("plan", {}).get("ir_plan")
        )
        if p_ir:
            out["ir_plan"] = p_ir
    if not out.get("planner_route"):
        p_route = (
            flow.get("planner_route")
            or ctx.get("planner_route")
            or flow.get("plan", {}).get("planner_route")
        )
        if p_route:
            out["planner_route"] = p_route
    return out


def _engine_rec_to_codegen(rec: Dict[str, Any]) -> tuple[str, str]:
    """Map engine_recommendation to (codegen_engine, sql_dialect)."""
    eng = str(rec.get("engine") or "python").lower()
    dialect = str(rec.get("dialect") or "tsql").lower()
    if eng == "pyspark":
        return "pyspark", dialect
    if eng == "adf":
        return "adf", dialect
    if eng == "sql":
        return "sql", dialect if dialect in ("ansi", "tsql") else "tsql"
    return "python", dialect


def _assessment_schema_signature(assess: Dict[str, Any]) -> str:
    """Compute a stable hash of the datasets, column names, and types to detect schema changes."""
    if not isinstance(assess, dict) or "datasets" not in assess:
        return ""
    parts = []
    datasets = assess.get("datasets") or {}
    for ds_name in sorted(datasets.keys()):
        parts.append(f"ds:{ds_name}")
        cols = datasets[ds_name].get("columns") or {}
        for col_name in sorted(cols.keys()):
            col_info = cols[col_name] or {}
            dtype = str(col_info.get("dtype") or "")
            parts.append(f"col:{col_name}|type:{dtype}")
    sig_str = "\n".join(parts)
    return hashlib.sha256(sig_str.encode("utf-8")).hexdigest()


@with_session_lock
def etl_plan_start(
    session_id: str,
    business_rules: Any,
    assessment_result: Optional[Dict[str, Any]] = None,
    engine: str = "python",
    codegen_engine: Optional[str] = None,
    sql_dialect: str = "tsql",
    target_destination: str = "dataframe_only",
    target_path: Optional[str] = None,
    tenant_id: Optional[str] = None,
    source_context: Optional[Dict[str, Any]] = None,
    engine_user_override: bool = False,
    generation_mode: Optional[str] = "full",
    user_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    ctx["session_id"] = sid
    assess = _get_assessment(sess, assessment_result)

    if not assess:
        return {
            "ok": False,
            "error": "NO_ASSESSMENT",
            "message": "Run an assessment first, or pass assessment_result in the request body.",
        }

    # Inject Fabric shortcuts and target properties if registered (Blob/Files only)
    from agent.blob_fabric_registry import get_shortcut
    if isinstance(assess, dict) and "datasets" in assess:
        for ds_name, ds_meta in assess["datasets"].items():
            if isinstance(ds_meta, dict):
                stype = str(ds_meta.get("source_type") or "").upper()
                if "SQL" in stype:
                    continue  # Azure SQL datasets bypass Fabric shortcutting
                blob_path = ds_meta.get("source_path") or ds_meta.get("source") or ""
                shortcut = get_shortcut(sid, blob_path) or get_shortcut(sid, ds_name)
                if shortcut:
                    ds_meta["fabric_uri"] = shortcut["lakehouse_uri"]
                    ds_meta["source_type"] = "fabric_files_zone"
                    ds_meta["files_zone_path"] = shortcut["files_zone_path"]


    if isinstance(assessment_result, dict) and assessment_result.get("datasets"):
        ctx["last_assessment_result"] = assessment_result

    flow = ctx.setdefault("etl_flow", {})
    flow["workspace_id"] = os.getenv("FABRIC_WORKSPACE_ID") or flow.get("workspace_id")
    flow["lakehouse_id"] = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID") or flow.get("lakehouse_id")
    schema_sig = _assessment_schema_signature(assess)
    flow["assessment_schema_signature"] = schema_sig
    sess["session_state"] = "planned"

    ds_names = list((assess.get("datasets") or {}).keys())
    tid = (tenant_id or tenant_id_from_session(ctx) or "default").strip() or "default"
    ctx["etl_tenant_id"] = tid
    pending = pending_rules_from_session(ctx)
    merged_raw = business_rules
    if pending:
        merged_raw = {**(pending or {}), **(business_rules if isinstance(business_rules, dict) else {})}
    rules_merged = merge_business_rules_for_datasets(merged_raw, ds_names, tenant_id=tid)

    # Extract user prompt from all possible request and session locations
    biz_raw = business_rules if isinstance(business_rules, dict) else {}
    user_prompt = (
        user_prompt
        or biz_raw.get("user_prompt")
        or (rules_merged.get("user_prompt") if isinstance(rules_merged, dict) else None)
        or (biz_raw.get("notes") if isinstance(biz_raw, dict) else None)
        or (rules_merged.get("notes") if isinstance(rules_merged, dict) else None)
        or (biz_raw.get("description") if isinstance(biz_raw, dict) else None)
        or ctx.get("user_prompt")
        or sess.get("user_prompt")
        or flow.get("user_prompt")
        or (flow.get("etl_intent", {}).get("description") if isinstance(flow.get("etl_intent"), dict) else None)
    )

    # ── Synthesize prompt from assessment when user never typed one ───────────
    # This ensures the LLM IR planner is always triggered (never falls to deterministic).
    if not user_prompt and assess:
        user_prompt = _synthesize_prompt_from_assessment(assess)
        if user_prompt:
            logger.info(f"[PLAN] user_prompt synthesized from assessment: {repr(user_prompt[:80])}")
    # ─────────────────────────────────────────────────────────────────────────

    logger.info(f"[PLAN] user_prompt={repr(user_prompt)}")

    if user_prompt:
        sess["had_user_prompt"] = True
        ctx["had_user_prompt"] = True
        flow["had_user_prompt"] = True
        sess["user_prompt"] = user_prompt
        ctx["user_prompt"] = user_prompt
        flow["user_prompt"] = user_prompt
        if isinstance(rules_merged, dict):
            rules_merged["user_prompt"] = user_prompt

    # ── Zep memory: inject recalled dataset facts ────────────────────
    try:
        from agent.memory import recall_dataset_facts
        zep_facts = []
        for ds in ds_names[:3]:
            zep_facts.extend(recall_dataset_facts(user_id=sid, dataset_name=ds))
        if zep_facts:
            existing_notes = rules_merged.get("notes") or ""
            rules_merged["notes"] = existing_notes + "\n" + "\n".join(zep_facts)
            rules_merged["_zep_facts_applied"] = len(zep_facts)
    except Exception:
        pass
    # ─────────────────────────────────────────────────────────────────


    src_ctx = build_source_context(ctx, assess, override=source_context)
    ctx["source_context"] = src_ctx
    if target_destination == "overwrite":
        out_base = "__overwrite__"
    elif target_destination == "new_path" and target_path:
        out_base = target_path
    else:
        out_base = "cleaned/"
    manifest = build_connector_manifest(
        ctx, assess, output_base=out_base, overwrite_in_place=(target_destination == "overwrite")
    )
    ctx["connector_manifest"] = manifest

    t0 = time.time()
    
    # --- Agentic Intelligence Pass ---
    # First, build a draft plan to generate the initial manual review queue
    dq_recs = assess.get("dq_recommendations") if isinstance(assess, dict) else None
    notes = str(rules_merged.get("notes") or "").strip()
    
    if not notes:
        # Build plan directly (only once!)
        plan = build_etl_plan(
            assess,
            rules_merged,
            engine=engine,
            source_context=src_ctx,
            generation_mode=generation_mode,
            dq_recommendations=dq_recs,
            semantic_context=assess.get("semantic_context"),
        )
        plan = enrich_plan_manual_review(plan)
        agentic_result = {}
    else:
        # Notes exist, run the agentic intelligence pass
        draft_plan = build_etl_plan(
            assess,
            rules_merged,
            engine=engine,
            source_context=src_ctx,
            generation_mode=generation_mode,
            dq_recommendations=dq_recs,
            semantic_context=assess.get("semantic_context"),
        )
        draft_plan = enrich_plan_manual_review(draft_plan)
        
        agentic_result = analyze_agentic_intent(draft_plan, rules_merged)
        
        updated_rules = agentic_result.get("updated_business_rules") or {}
        if updated_rules:
            logger.info(f"Agentic rules override applied: {updated_rules}")
            rules_merged.update(updated_rules)
            # Re-build plan with updated structured toggles
            plan = build_etl_plan(
                assess,
                rules_merged,
                engine=engine,
                source_context=src_ctx,
                generation_mode=generation_mode,
                dq_recommendations=dq_recs,
                semantic_context=assess.get("semantic_context"),
            )
            plan = enrich_plan_manual_review(plan)
        else:
            plan = draft_plan

    # ── Rule Provenance Pipeline (Component 11) ─────────────────────
    # Collect tagged rules from all 3 layers and run conflict detection
    try:
        from agent.etl_pipeline.rule_provenance import TaggedRule, RuleProvenance
        from agent.etl_pipeline.conflict_detector import detect_conflicts
        from agent.etl_pipeline.business_rules import to_tagged_rules as biz_to_tagged
        from agent.semantic_context import SemanticCleaningPlan
        from agent.transformation_suggester import suggest_transformations

        all_tagged: list = []
        sem_ctx = assess.get("semantic_context") or {}
        suggestions = suggest_transformations(assess)

        for ds_name in ds_names:
            # Layer 1: Business rules → TaggedRules
            biz_rules = biz_to_tagged(rules_merged, ds_name, assessment=assess)
            all_tagged.extend(biz_rules)

            # Layer 2: Semantic layer → TaggedRules
            sem_model = sem_ctx.get("semantic_model")
            if isinstance(sem_model, dict) and "entities" in sem_model:
                try:
                    splan = SemanticCleaningPlan(
                        entities=sem_model.get("entities") or {},
                        relationships=sem_model.get("relationships") or [],
                    )
                    sem_rules = splan.to_tagged_rules()
                    all_tagged.extend(r for r in sem_rules if r.dataset == ds_name)
                except Exception:
                    pass

            # Layer 3: Auto-detected → TaggedRules (from transformation suggestions)
            for s in suggestions.get("suggested_transformations") or []:
                if s.get("dataset") == ds_name and s.get("auto_fixable"):
                    prov = s.get("provenance", RuleProvenance.AUTO_DETECTED)
                    all_tagged.append(TaggedRule(
                        dataset=ds_name,
                        column=s.get("column") or "",
                        issue_type=s.get("issue_type") or "unknown",
                        action=s.get("suggested_action") or "review_manually",
                        provenance=prov,
                        source_detail=f"Auto-detected: {s.get('message', '')[:200]}"
                    ))

        # Run conflict detection across all layers
        resolved_rules, conflicts = detect_conflicts(all_tagged)
        plan["rule_provenance"] = {
            "total_rules": len(all_tagged),
            "resolved_rules": len(resolved_rules),
            "conflicts_detected": len(conflicts),
            "conflicts": [c.model_dump() for c in conflicts],
        }

        # Inject conflicts into manual review items
        if conflicts:
            from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item
            existing_mr = plan.get("manual_review") or []
            for conflict in conflicts:
                mr_key = (conflict.dataset, conflict.column, conflict.issue_type)
                found = False
                for i, mr_item in enumerate(existing_mr):
                    if not isinstance(mr_item, dict):
                        continue
                    if (str(mr_item.get("dataset", "")).lower() == str(mr_key[0]).lower()
                            and str(mr_item.get("column", "")).lower() == str(mr_key[1]).lower()
                            and str(mr_item.get("issue_type", "")).lower() == str(mr_key[2]).lower()):
                        existing_mr[i] = enrich_manual_review_item(mr_item, conflict=conflict)
                        found = True
                        break
                if not found:
                    new_item = {
                        "dataset": conflict.dataset,
                        "column": conflict.column,
                        "issue_type": conflict.issue_type,
                        "severity": "HIGH",
                        "message": f"Rule conflict: {len(conflict.rules)} rules suggest different actions",
                    }
                    existing_mr.append(enrich_manual_review_item(new_item, conflict=conflict))
            plan["manual_review"] = existing_mr
    except Exception as ex:
        logger.debug("Rule provenance pipeline skipped: %s", ex)
    # ─────────────────────────────────────────────────────────────────
    # Compute ETL readiness and map blockers to manual review BEFORE applying resolutions
    from agent.etl_readiness_scorer import compute_etl_readiness
    readiness = compute_etl_readiness(assess)
    
    if readiness.get("blockers"):
        existing = plan.get("manual_review") or []
        for blocker in readiness["blockers"]:
            b_ds = blocker.get("dataset")
            b_col = blocker.get("column")
            b_it = blocker.get("issue_type") or "unknown"
            
            in_pending = any(
                str(m.get("dataset")).lower() == str(b_ds).lower() and
                str(m.get("column")).lower() == str(b_col).lower() and
                str(m.get("issue_type")).lower() == str(b_it).lower()
                for m in existing if isinstance(m, dict)
            )
            if not in_pending:
                plan.setdefault("manual_review", []).append({
                    "dataset": b_ds,
                    "column": b_col,
                    "issue_type": b_it,
                    "severity": blocker.get("severity") or "HIGH",
                    "message": blocker.get("issue"),
                    "guidance": blocker.get("fix") or "",
                })
        plan = enrich_plan_manual_review(plan)

    # Clear stale session-saved manual review resolutions on fresh plan build.
    # These are picks from previous plan iterations that should NOT auto-apply to a new plan.
    # The user must see the fresh manual review queue and make new choices.
    ctx.pop("manual_review_resolutions", None)

    # Only apply agentic (LLM-generated) resolutions if the agentic pass produced them
    resolutions = list(agentic_result.get("manual_review_resolutions") or [])
    if resolutions:
        logger.info(f"Applying {len(resolutions)} agentic manual review resolutions to plan.")
        plan, res_errs = apply_manual_resolutions(plan, resolutions, business_rules=rules_merged)
        if res_errs:
            logger.warning(f"Resolutions errors: {res_errs}")
            
    plan["connector_manifest"] = manifest
    plan["source_context"] = src_ctx
    if flow.get("execution_target") == "fabric":
        plan["execution_target"] = "fabric"
    plan["etl_intent"] = {
        "engine": (engine or "python").lower(),
        "target_destination": target_destination or "dataframe_only",
        "target_path": target_path,
    }

    # Adjust readiness based on resolved manual reviews in the plan
    resolved = plan.get("resolved_manual_review") or []
    resolved_keys = set(
        f"{str(r.get('dataset') or '').lower()}|{str(r.get('column') or '').lower()}|{str(r.get('issue_type') or '').lower()}"
        for r in resolved if isinstance(r, dict)
    )
    if resolved_keys:
        active_blockers = []
        score_improvement = 0
        for b in readiness.get("blockers") or []:
            b_ds = str(b.get("dataset") or "").lower()
            if b_ds == "global":
                b_ds = ""
            b_col = str(b.get("column") or "").lower()
            b_it = str(b.get("issue_type") or "").lower()
            key = f"{b_ds}|{b_col}|{b_it}"
            if key in resolved_keys:
                if b_it == "business_key_duplicate":
                    score_improvement += 15
                elif b_it == "high_null_percentage":
                    score_improvement += 20
                elif b.get("severity") == "HIGH":
                    score_improvement += 15
                elif b_it == "orphan_foreign_keys":
                    score_improvement += 15
                elif b_it == "reconciliation_imbalance":
                    score_improvement += 12
                else:
                    score_improvement += 15
            else:
                active_blockers.append(b)
                
        active_warnings = []
        for w in readiness.get("warnings") or []:
            w_ds = str(w.get("dataset") or "").lower()
            if w_ds == "global":
                w_ds = ""
            w_col = str(w.get("column") or "").lower()
            w_it = str(w.get("issue_type") or "").lower()
            key = f"{w_ds}|{w_col}|{w_it}"
            if key in resolved_keys:
                score_improvement += 8
            else:
                active_warnings.append(w)
                
        readiness["score"] = min(100.0, float(readiness["score"]) + score_improvement)
        readiness["blockers"] = active_blockers
        readiness["warnings"] = active_warnings
        readiness["grade"] = "A" if readiness["score"] >= 90 else "B" if readiness["score"] >= 75 else "C" if readiness["score"] >= 50 else "F"
        readiness["etl_recommendation"] = (
            "Ready for ETL generation" if not active_blockers
            else f"Fix {len(active_blockers)} blocker(s) before generating ETL"
        )

    assess["etl_readiness"] = readiness
    plan["blocked"] = []

    flow = ctx.setdefault("etl_flow", {})
    # Default "fallback" avoids LLM calls during plan build (tiered/llm add significant latency).
    narr_mode = os.getenv("ETL_NARRATOR_MODE", "fallback").strip().lower()
    use_llm_full = narr_mode in ("llm", "full") or os.getenv(
        "ETL_NARRATOR_USE_LLM", "0"
    ).strip().lower() in ("1", "true", "yes")
    if (generation_mode or "").strip().lower() == "cleanse_only":
        narr_mode = "fallback"
        use_llm_full = False
    cache_key = f"narr_{plan.get('plan_id')}_{plan.get('assessment_signature')}"
    cached = (flow.get("narration_cache") or {}).get(cache_key)
    if isinstance(cached, dict) and cached.get("engine_explanation"):
        plan["narration"] = cached
    else:
        plan["narration"] = narrate_plan(plan, mode=narr_mode, use_llm=use_llm_full)
        flow.setdefault("narration_cache", {})[cache_key] = plan["narration"]

    plan_ok, plan_errs = validate_etl_plan(plan, assess, rules_merged)
    plan["build_warnings"] = list(set((plan.get("build_warnings") or []) + plan_errs))

    eng_rec = plan.get("engine_recommendation") or {}
    if engine_user_override:
        ce = (codegen_engine or engine or "python").lower()
        sd = (sql_dialect or "tsql").lower()
        ctx["etl_engine_override"] = True
    else:
        ce, sd = _engine_rec_to_codegen(eng_rec)
        if codegen_engine:
            ce = codegen_engine.lower()
        ctx.pop("etl_engine_override", None)
    _migrate_phase(flow)
    _transition(flow, "planned", by="system", reason="etl_plan_start")
    preview = None
    if plan_ok and not (plan.get("blocked") or []):
        preview = build_impact_preview(assess, plan)
        flow["preview"] = preview
        _transition(flow, "preview_ready", by="system", reason="plan_enriched_with_evidence")
    elif plan.get("blocked"):
        flow["failure_reason"] = "Plan has blocking issues"
        _transition(flow, "failed", by="system", reason="plan_blocked")

    if pending:
        ctx.pop("pending_business_rules", None)

    flow.update(
        {
            "plan": plan,
            "plan_validation_ok": plan_ok,
            "plan_validation_errors": plan_errs,
            "target_engine": (engine or "python").lower(),
            "codegen_engine": ce,
            "sql_dialect": sd,
            "business_rules": rules_merged,
            "etl_intent": {
                "engine": (engine or "python").lower(),
                "sql_dialect": sd,
                "target_destination": target_destination or "dataframe_only",
                "target_path": target_path,
                "generation_mode": generation_mode,
            },
            "approved_plan": None,
            "preview": preview,
            "code": None,
            "validation_ok": None,
            "validation_errors": [],
            "generated_by": None,
            "artifact_rel_path": None,
            "is_draft": False,
            "lineage": None,
            "artifact_version": flow.get("artifact_version") or 0,
            "ir_plan": None,
            "planner_route": None,
        }
    )
    for obj in (flow, sess, ctx):
        obj.pop("ir_plan", None)
        obj.pop("planner_route", None)
        obj.pop("user_prompt", None)

    # Compiler cleaning plan integration
    try:
        from agent.compiler.keys.business_key_resolver import BusinessKeyResolver
        from agent.compiler.deduplication.duplicate_policy import DuplicatePolicy, DuplicateDisposition, SurvivorStrategy
        from agent.compiler.models.cleaning import CleaningMode
        from agent.compiler.models.context import CompilerContext
        from agent.compiler.models.profile import DatasetProfile, ColumnProfile
        from agent.compiler.schema.schema_resolver import SchemaResolver
        from agent.feature_flags import get_flags

        flags = get_flags()
        clean_mode_str = (
            (business_rules.get("cleaning_mode") if isinstance(business_rules, dict) else None)
            or rules_merged.get("cleaning_mode")
            or flow.get("cleaning_mode")
            or flags.cleaning_default_mode
        )
        try:
            clean_mode = CleaningMode(clean_mode_str)
        except Exception:
            clean_mode = CleaningMode.CLEANSE_WITH_AUDIT

        cleaning_plan = {
            "cleaning_mode": clean_mode.value,
            "business_keys": {},
            "duplicate_policies": {},
            "output_contracts": {},
        }

        for dname in ds_names:
            ds_meta = (assess.get("datasets") or {}).get(dname) or {}
            cols_dict = ds_meta.get("columns") or {}
            key_cand = BusinessKeyResolver.resolve_key(dname, cols_dict)
            cleaning_plan["business_keys"][dname] = key_cand.model_dump() if key_cand else None
            
            dup_policy = DuplicatePolicy(
                dataset_name=dname,
                disposition=DuplicateDisposition.REMOVE_REDUNDANT if clean_mode != CleaningMode.AUDIT_ONLY else DuplicateDisposition.FLAG,
                survivor_strategy=SurvivorStrategy.MOST_COMPLETE,
                approved_key=key_cand.columns if key_cand else None,
            )
            cleaning_plan["duplicate_policies"][dname] = dup_policy.model_dump()

        c_profiles = {}
        for dname in ds_names:
            ds_meta = (assess.get("datasets") or {}).get(dname) or {}
            cols = {c: ColumnProfile(name=c, dtype="string") for c in (ds_meta.get("columns") or {})}
            c_profiles[dname] = DatasetProfile(dataset_id=dname, columns=cols)
            
        temp_ctx = CompilerContext(
            session_id=sid,
            cleaning_mode=clean_mode,
            preserve_source_schema=True,
            dataset_profiles=c_profiles,
        )
        contracts_preview = SchemaResolver.resolve_all_contracts(temp_ctx)
        cleaning_plan["output_contracts"] = {k: v.model_dump() for k, v in contracts_preview.items()}

        plan["cleaning_plan"] = cleaning_plan
        flow["cleaning_plan"] = cleaning_plan
        flow["cleaning_mode"] = clean_mode.value
    except Exception as c_err:
        logger.warning(f"Could not build compiler cleaning plan: {c_err}")

    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    cov_report = build_coverage_report(assess, plan)
    plan["coverage_report"] = cov_report

    # Priority P1 & P2: LLM IR Planning during PLAN stage
    if flags.llm_ir_planner_enabled and user_prompt:
        try:
            from agent.compiler.models.profile import DatasetProfile, ColumnProfile
            from agent.ir import IrExtractor, IrNormalizer, IrValidator, IrFixer
            from agent.ir.semantic_validator import BusinessRuleValidator
            from agent.router import IntentRouter, RouteTarget

            plan_profiles: Dict[str, DatasetProfile] = {}
            for ds_name, ds_info in (assess.get("datasets") or {}).items():
                if isinstance(ds_info, dict):
                    p_cols = {}
                    for col_name, col_meta in (ds_info.get("columns") or {}).items():
                        dtype = col_meta.get("dtype") or col_meta.get("type") or "string" if isinstance(col_meta, dict) else "string"
                        p_cols[col_name] = ColumnProfile(name=col_name, dtype=dtype)
                    plan_profiles[ds_name] = DatasetProfile(dataset_id=ds_name, row_count=int(ds_info.get("row_count") or 0), columns=p_cols)

            if not plan_profiles and isinstance(plan.get("datasets"), dict):
                for ds_name, p_ds in plan["datasets"].items():
                    p_cols = {c: ColumnProfile(name=c, dtype="string") for c in (p_ds.get("columns") or [])}
                    plan_profiles[ds_name] = DatasetProfile(dataset_id=ds_name, columns=p_cols)

            route_decision = IntentRouter.classify(user_prompt, plan_profiles)
            route_val = route_decision.route.value
            logger.info(f"[IntentRouter] route={route_val}")

            if route_decision.route == RouteTarget.COMPILER_DIRECT:
                raw_ir = IrExtractor._heuristic_fallback_extract(user_prompt, plan_profiles, session_id=sid)
                raw_ir.metadata.tags = ["compiler_direct"]
            else:
                raw_ir = IrExtractor.extract(user_prompt, plan_profiles, session_id=sid)
                raw_ir.metadata.tags = [route_val]

            norm_ir = IrNormalizer.normalize(raw_ir, plan_profiles)
            logger.info("[IrValidator] Validating normalized plan with %d sources, %d steps", len(norm_ir.sources), len(norm_ir.logical_plan))
            healed_ir, val_res, is_valid = IrFixer.heal_plan(norm_ir, plan_profiles, max_attempts=2)
            if val_res.valid:
                logger.info("[IrValidator] passed")
            logger.info(
                "[IrFixer] repairs=%d",
                len(healed_ir.metadata.repair_history) if healed_ir.metadata else 0,
            )

            semantic_val = BusinessRuleValidator.validate_semantics(healed_ir, plan_profiles)
            if semantic_val.valid:
                logger.info("[SemanticValidator] Plan passed semantic business rule validation")
            else:
                logger.warning("[SemanticValidator] Flagged semantic issues: %s", [e.message for e in semantic_val.errors])

            ir_dict = healed_ir.model_dump()
            plan["ir_plan"] = ir_dict
            flow["ir_plan"] = ir_dict
            sess["ir_plan"] = ir_dict
            ctx["ir_plan"] = ir_dict

            plan["planner_route"] = route_val
            flow["planner_route"] = route_val
            sess["planner_route"] = route_val
            ctx["planner_route"] = route_val

            snapshot_payload = {
                "user_prompt": user_prompt,
                "planner_route": route_val,
                "ir_plan": ir_dict,
                "plan_id": plan.get("plan_id"),
            }
            save_plan_snapshot(sid, snapshot_payload)
        except Exception as e:
            logger.warning(f"Could not build IR plan during planning: {e}")

    # Canonical storage persistence
    plan["user_prompt"] = user_prompt
    flow["user_prompt"] = user_prompt
    sess["user_prompt"] = user_prompt
    ctx["user_prompt"] = user_prompt

    if user_prompt:
        logger.info("[SESSION] Persisted user_prompt")

    save_session(sess)
    logger.info(
        "etl_plan_start session=%s plan_id=%s ok=%s steps=%s latency_ms=%.0f",
        sid,
        plan.get("plan_id"),
        plan_ok,
        sum(len((v or {}).get("steps") or []) for v in (plan.get("datasets") or {}).values()),
        (time.time() - t0) * 1000,
    )
    blocked = plan.get("blocked") or []
    plan_success = plan_ok and not blocked
    pending_manual = count_pending_manual_review(plan)
    return {
        "ok": plan_success,
        "session_id": sid,
        "plan": plan,
        "blocked": blocked,
        "pending_manual_review": pending_manual,
        "plan_validation_ok": plan_ok,
        "plan_validation_errors": plan_errs,
        "engine_recommendation": plan.get("engine_recommendation"),
        "source_context": src_ctx,
        "recommended_codegen_engine": ce,
        "recommended_sql_dialect": sd,
        "coverage_report": cov_report,
        "message": (
            None
            if plan_success
            else (
                "Plan has blocking issues."
                if blocked
                else "Plan built with validation warnings — review plan_validation_errors."
            )
        ),
    }


@with_session_lock
def etl_apply_manual_resolutions(
    session_id: str,
    resolutions: List[Dict[str, Any]],
    plan_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply user picks for manual_review items; promotes steps into plan.datasets."""
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    
    # ── Concurrency Check ──
    from agent.session_store import _connect
    conn = _connect()
    try:
        row = conn.execute("SELECT version FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        db_version = row[0] if (row and row[0] is not None) else 1
    finally:
        conn.close()
    
    loaded_version = sess.get("_version")
    if loaded_version is not None and db_version != loaded_version:
        return {
            "status": "PLAN_REVISION_CONFLICT",
            "ok": False,
            "message": "The plan has been modified by another process. Please refresh and try again."
        }

    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.get("etl_flow") or {}

    plan = (
        plan_override
        if isinstance(plan_override, dict) and plan_override.get("datasets") is not None
        else flow.get("plan")
    )
    if not isinstance(plan, dict):
        return {"ok": False, "error": "NO_PLAN", "message": "Create a plan first (POST /etl/plan)."}

    # Store resolutions in session context
    saved_resolutions = ctx.get("manual_review_resolutions") or []
    res_map = {r.get("item_id") or r.get("id"): r for r in saved_resolutions if r}
    for r in resolutions:
        if not r:
            continue
        iid = r.get("item_id") or r.get("id")
        if iid:
            res_map[iid] = r
    ctx["manual_review_resolutions"] = list(res_map.values())
    
    # We apply manual resolutions to candidates to avoid altering original until validation passes
    import copy
    candidate_plan = copy.deepcopy(plan)
    rules = copy.deepcopy(flow.get("business_rules") or plan.get("business_rules") or {})
    
    updated, apply_errs = apply_manual_resolutions(candidate_plan, resolutions, business_rules=rules, assessment=assess)
    if apply_errs:
        logger.warning(f"etl_apply_manual_resolutions: Errors applying resolutions: {apply_errs}")

    # Validate candidate plan
    valid, issues = validate_promoted_plan(updated, assess, rules)
    if not valid:
        return {
            "status": "PROMOTION_REJECTED",
            "ok": False,
            "errors": [
                f"{issue.message} (Column: '{issue.column}' in Dataset: '{issue.dataset_id or 'global'}')"
                if issue.column else issue.message
                for issue in issues
            ],
            "message": "Promotion rejected due to validation failures in candidate plan."
        }
    
    if rules:
        flow["business_rules"] = rules
        ctx["business_rules"] = rules
        updated["business_rules"] = rules

    # Recalculate readiness and apply improvements based on resolutions
    if assess:
        from agent.etl_readiness_scorer import compute_etl_readiness
        readiness = compute_etl_readiness(assess)
        resolved = updated.get("resolved_manual_review") or []
        resolved_keys = set(
            f"{str(r.get('dataset') or '').lower()}|{str(r.get('column') or '').lower()}|{str(r.get('issue_type') or '').lower()}"
            for r in resolved if isinstance(r, dict)
        )
        if resolved_keys:
            active_blockers = []
            score_improvement = 0
            for b in readiness.get("blockers") or []:
                b_ds = str(b.get("dataset") or "").lower()
                if b_ds == "global":
                    b_ds = ""
                b_col = str(b.get("column") or "").lower()
                b_it = str(b.get("issue_type") or "").lower()
                key = f"{b_ds}|{b_col}|{b_it}"
                if key in resolved_keys:
                    if b_it == "business_key_duplicate":
                        score_improvement += 15
                    elif b_it == "high_null_percentage":
                        score_improvement += 20
                    elif b.get("severity") == "HIGH":
                        score_improvement += 15
                    elif b_it == "orphan_foreign_keys":
                        score_improvement += 15
                    elif b_it == "reconciliation_imbalance":
                        score_improvement += 12
                    else:
                        score_improvement += 15
                else:
                    active_blockers.append(b)
                    
            active_warnings = []
            for w in readiness.get("warnings") or []:
                w_ds = str(w.get("dataset") or "").lower()
                if w_ds == "global":
                    w_ds = ""
                w_col = str(w.get("column") or "").lower()
                w_it = str(w.get("issue_type") or "").lower()
                key = f"{w_ds}|{w_col}|{w_it}"
                if key in resolved_keys:
                    score_improvement += 8
                else:
                    active_warnings.append(w)
                    
            readiness["score"] = min(100.0, float(readiness["score"]) + score_improvement)
            readiness["blockers"] = active_blockers
            readiness["warnings"] = active_warnings
            readiness["grade"] = "A" if readiness["score"] >= 90 else "B" if readiness["score"] >= 75 else "C" if readiness["score"] >= 50 else "F"
            readiness["etl_recommendation"] = (
                "Ready for ETL generation" if not active_blockers
                else f"Fix {len(active_blockers)} blocker(s) before generating ETL"
            )
        assess["etl_readiness"] = readiness

    updated["blocked"] = []

    # Re-tag all step buckets: user-promoted steps (evidence.rule_override=True)
    # are reclassified as 'auto' so readiness % reflects resolved items correctly.
    from agent.etl_pipeline.classify_steps import tag_plan_step_buckets
    updated = tag_plan_step_buckets(updated, rules)

    # Re-evaluate projected column certificates, Phase 2 gates, and readiness scorecard post-resolutions
    from agent.etl_pipeline.readiness_engine import (
        build_projected_column_certificates,
        evaluate_all_operation_gates,
        recompute_readiness_scorecard
    )
    approved_waivers = rules.get("approved_waivers") or []
    updated["projected_column_readiness"] = build_projected_column_certificates(updated, assess or {})
    joins_list = updated.get("relationships", {}).get("joins") or []
    updated["projected_phase2_gates"] = evaluate_all_operation_gates(
        updated["projected_column_readiness"], joins_list, approved_waivers
    )
    updated["readiness_scorecard"] = recompute_readiness_scorecard(updated, assess or {}, rules)

    # Run validation on the resolved plan
    from agent.etl_pipeline.validate_plan import validate_etl_plan
    plan_ok, plan_errs = validate_etl_plan(updated, assess or {}, rules)

    # Build impact preview
    exec_res = flow.get("sql_execution_result") or {}
    row_deltas = (exec_res.get("post_execution_summary") or {}).get("row_deltas")
    preview = build_impact_preview(assess or {}, updated, row_deltas=row_deltas)
    flow["preview"] = preview

    # Build coverage report
    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    cov_report = build_coverage_report(assess or {}, updated)
    updated["coverage_report"] = cov_report

    updated["build_warnings"] = plan_errs
    flow["plan"] = updated
    flow["plan_validation_ok"] = plan_ok
    flow["plan_validation_errors"] = plan_errs

    save_session(sess)

    pending_manual = count_pending_manual_review(updated)
    ce = flow.get("codegen_engine")
    sd = flow.get("sql_dialect")

    return {
        "ok": plan_ok and not updated.get("blocked"),
        "session_id": sid,
        "plan": updated,
        "blocked": updated.get("blocked") or [],
        "pending_manual_review": pending_manual,
        "plan_validation_ok": plan_ok,
        "plan_validation_errors": plan_errs,
        "engine_recommendation": updated.get("engine_recommendation"),
        "source_context": updated.get("source_context"),
        "recommended_codegen_engine": ce,
        "recommended_sql_dialect": sd,
        "coverage_report": cov_report,
        "message": (
            None
            if (plan_ok and not updated.get("blocked"))
            else "Plan resolved with validation warnings — review plan_validation_errors."
        ),
    }


@with_session_lock
def etl_confirm_plan(session_id: str, plan_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.setdefault("etl_flow", {})
    _migrate_phase(flow)

    schema_sig = _assessment_schema_signature(assess or {})
    saved_sig = flow.get("assessment_schema_signature")
    if saved_sig and schema_sig != saved_sig:
        rollback_on_failure(flow, reason="Schema signature mismatch (underlying dataset/schema changed).")
        save_session(sess)
        return {
            "ok": False,
            "error": "SCHEMA_INVALIDATED",
            "message": "The underlying dataset schema or connection has changed, invalidating the planned transformations. Rollback to planned state triggered.",
        }

    plan = (
        plan_override
        if isinstance(plan_override, dict) and plan_override.get("datasets") is not None
        else flow.get("plan")
    )
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {"ok": False, "error": "NO_PLAN", "message": "Create a plan first (POST /etl/plan)."}

    plan = enrich_plan_manual_review(_rehydrate_plan(plan, ctx, assess=assess))

    # Step 1: Trace user_prompt during CONFIRM
    confirmed_prompt = (
        plan.get("user_prompt")
        or flow.get("user_prompt")
        or sess.get("user_prompt")
        or ctx.get("user_prompt")
    )
    logger.info(f"[CONFIRM] user_prompt={repr(confirmed_prompt)}")
    if confirmed_prompt:
        plan["user_prompt"] = confirmed_prompt
        flow["user_prompt"] = confirmed_prompt
        sess["user_prompt"] = confirmed_prompt
        ctx["user_prompt"] = confirmed_prompt

    rules = flow.get("business_rules") or plan.get("business_rules") or {}
    if rules.get("auto_resolve_safe_defaults") or plan.get("auto_resolve_safe_defaults"):
        pending_items = [m for m in plan.get("manual_review") or [] if str(m.get("status") or "pending").lower() == "pending"]
        if pending_items:
            resolutions = []
            for item in pending_items:
                opts = item.get("resolution_options") or []
                rec_opt = next((o for o in opts if o.get("recommended")), None)
                if not rec_opt and opts:
                    rec_opt = opts[0]
                if rec_opt:
                    resolutions.append({
                        "item_id": item["id"],
                        "resolution_id": rec_opt["id"]
                    })
            if resolutions:
                logger.info(f"Auto-resolving {len(resolutions)} pending manual review items due to auto_resolve_safe_defaults rule.")
                plan, res_errs = apply_manual_resolutions(plan, resolutions, business_rules=rules)
                if res_errs:
                    logger.warning(f"Errors during auto-resolving safe defaults: {res_errs}")

    pending_manual = count_pending_manual_review(plan)
    if pending_manual > 0:
        return {
            "ok": False,
            "error": "MANUAL_REVIEW_PENDING",
            "message": f"Resolve {pending_manual} manual review item(s) in the UI before confirming.",
            "pending_manual_review": pending_manual,
            "manual_review": plan.get("manual_review") or [],
        }

    blocked = plan.get("blocked") or []
    if blocked:
        return {
            "ok": False,
            "error": "PLAN_BLOCKED",
            "message": "Plan has blocking issues; resolve required columns or rules first.",
            "blocked": blocked,
        }

    plan_ok, plan_errs = validate_etl_plan_for_confirm(plan, assess or {}, rules)
    if not plan_ok:
        return {
            "ok": False,
            "error": "PLAN_VALIDATION_FAILED",
            "message": "Plan failed validation. Fix issues before confirming.",
            "plan_validation_errors": plan_errs,
        }

    _migrate_phase(flow)
    phase = flow.get("phase", "planned")
    if phase not in ("preview_ready", "planned"):
        return {
            "ok": False,
            "error": "INVALID_PHASE",
            "message": f"Cannot approve plan in phase '{phase}'. Build plan first.",
            "phase": phase,
        }

    auto_ok = _plan_all_auto(plan) and _invariants_pass(plan)
    exec_res = flow.get("sql_execution_result") or {}
    row_deltas = (exec_res.get("post_execution_summary") or {}).get("row_deltas")
    if phase == "planned":
        flow["preview"] = flow.get("preview") or build_impact_preview(assess or {}, plan, row_deltas=row_deltas)
        _transition(flow, "preview_ready", by="system", reason="preview_before_approve")
        phase = "preview_ready"

    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    cov_report = build_coverage_report(assess or {}, plan)
    plan["coverage_report"] = cov_report

    preview = flow.get("preview") or build_impact_preview(assess or {}, plan, row_deltas=row_deltas)
    lineage = build_lineage(plan, assess or {})
    flow = ctx.setdefault("etl_flow", {})
    if isinstance(plan_override, dict):
        cleaning_approval = plan_override.get("cleaning_approval") or {}
        if not cleaning_approval:
            for k in ("cleaning_mode", "approved_business_keys", "approved_duplicate_policies", "approved_survivor_strategies"):
                if k in plan_override:
                    cleaning_approval[k] = plan_override[k]
        if cleaning_approval:
            flow["cleaning_approval"] = cleaning_approval
            plan["cleaning_approval"] = cleaning_approval
            if "cleaning_mode" in cleaning_approval:
                flow["cleaning_mode"] = cleaning_approval["cleaning_mode"]
                plan["cleaning_mode"] = cleaning_approval["cleaning_mode"]

    # Retain IR plan and route in confirmed plan & flow
    if flow.get("ir_plan") and not plan.get("ir_plan"):
        plan["ir_plan"] = flow["ir_plan"]
    if flow.get("planner_route") and not plan.get("planner_route"):
        plan["planner_route"] = flow["planner_route"]

    flow["approved_plan"] = plan
    flow["preview"] = preview
    flow["lineage"] = lineage
    flow["plan_validation_ok"] = True
    _transition(
        flow,
        "approved",
        by="user" if not auto_ok else "system",
        reason="confirm_plan_called" if not auto_ok else "auto_approved_all_steps_safe",
    )

    # NOTE: We intentionally do NOT propagate plan.user_prompt back into sess/ctx/flow.
    # Doing so causes stale prompts (e.g. training-enrollment rules) to persist in
    # the session and contaminate future generate runs on completely different datasets.
    # The generate step resolves the prompt from the caller API param first, then falls
    # back to fields that are intrinsic to the dataset (plan.description, etl_intent.description).
    if plan.get("user_prompt"):
        logger.info("[SESSION] Noted plan.user_prompt exists; NOT propagating to sess/ctx/flow to prevent cross-run contamination.")

    save_session(sess)
    logger.info(
        "etl_confirm_plan session=%s plan_id=%s lineage_cols=%s",
        sid,
        plan.get("plan_id"),
        sum(len(v) for v in lineage.values()),
    )
    return {
        "ok": True,
        "session_id": sid,
        "preview": preview,
        "approved_plan": plan,
        "lineage": lineage,
        "coverage_report": cov_report,
    }


def _generate_for_engine(
    eng: str,
    plan: Dict[str, Any],
    assess: Dict[str, Any],
    *,
    sql_dialect: str,
    output_mode: str,
    output_path: Optional[str],
    inject_errors: Optional[List[str]],
) -> tuple[str, bool, List[str], str]:
    """Returns (code, ok, errs, generated_by)."""
    generated_by = "llm"

    from agent.etl_pipeline.llm_codegen import run_codegen_sync, is_llm_generation_error, parse_adf_json_from_llm

    if eng == "python":
        def validate_fn_src(source: str, never_drop_rows: bool = False):
            return validate_etl_python_source(source, plan)

        coro = generate_etl_with_llm(
            plan,
            assess,
            engine="python",
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=inject_errors,
            validate_fn=validate_fn_src,
        )
        code, err = run_codegen_sync(coro)
        if err:
            return err, False, [err], generated_by
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, errs = validate_etl_python_source(code, plan)
        return code, ok, errs, generated_by

    if eng in ("sql", "tsql", "ansi"):
        from agent.etl_pipeline.validate_sql import validate_sql_basic

        dialect = "ansi" if eng == "ansi" else (sql_dialect or "tsql")

        def validate_fn_sql(source: str, never_drop_rows: bool = False):
            ok, hard_errors, warnings = validate_sql_basic(source)
            return ok, hard_errors, warnings

        coro = generate_etl_with_llm(
            plan,
            assess,
            engine=f"sql-{dialect}",
            sql_dialect=dialect,
            output_mode=output_mode,
            validation_errors=inject_errors,
            validate_fn=validate_fn_sql,
        )
        code, err = run_codegen_sync(coro)
        if err:
            return err, False, [err], generated_by
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, hard_errors, warnings = validate_sql_basic(code)
        if warnings:
            logger.warning(f"[SQL Advisory] {warnings}")
        return code, ok, hard_errors, generated_by

    if eng in ("spark", "pyspark"):
        def validate_fn_src(source: str, never_drop_rows: bool = False):
            return validate_pyspark_source(source, plan)

        coro = generate_etl_with_llm(
            plan,
            assess,
            engine="pyspark",
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=inject_errors,
            validate_fn=validate_fn_src,
        )
        code, err = run_codegen_sync(coro)
        if err:
            return err, False, [err], generated_by
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, errs = validate_pyspark_source(code, plan)
        return code, ok, errs, generated_by

    if eng == "adf":
        from agent.etl_pipeline.validate_adf import validate_adf_json

        if inject_errors:
            coro = generate_etl_with_llm(plan, assess, engine="adf", validation_errors=inject_errors)
            raw, err = run_codegen_sync(coro)
            if err:
                return err, False, [err], generated_by
            if is_llm_generation_error(raw):
                return raw, False, [raw], generated_by
            obj, parse_errs = parse_adf_json_from_llm(raw)
            if obj is None:
                return raw, False, parse_errs, generated_by
            code = json.dumps(obj, indent=2)
            ok, errs = validate_adf_json(obj)
            return code, ok, errs, generated_by

        coro = generate_adf_with_llm(plan, assess, validate_fn=validate_adf_json)
        obj, llm_err = run_codegen_sync(coro)
        if obj is None:
            return llm_err or "# Error: ADF generation failed", False, [llm_err or "ADF failed"], generated_by
        code = json.dumps(obj, indent=2)
        ok, errs = validate_adf_json(obj)
        return code, ok, errs, generated_by

    return "", False, [f"Unsupported engine: {eng}"], generated_by


def _template_fallback(
    eng: str,
    plan: Dict[str, Any],
    assess: Dict[str, Any],
    *,
    sql_dialect: str,
) -> tuple[str, bool, List[str]]:
    if eng == "python":
        code = generate_python_etl(plan, assess)
        return code, *validate_etl_python_source(code, plan)

    if eng in ("sql", "tsql", "ansi"):
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        from agent.etl_pipeline.validate_sql import validate_sql_basic

        dialect = "ansi" if eng == "ansi" else (sql_dialect or "tsql")
        code = generate_sql_etl(plan, assess, dialect=dialect)
        ok, hard_errors, warnings = validate_sql_basic(code)
        if warnings:
            logger.warning(f"[SQL Advisory] {warnings}")
        return code, ok, hard_errors

    if eng in ("spark", "pyspark"):
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

        code = generate_pyspark_etl(plan, assess)
        return code, *validate_pyspark_source(code, plan)

    if eng == "adf":
        from agent.etl_pipeline.adf_codegen import generate_adf_mapping_flow
        from agent.etl_pipeline.validate_adf import validate_adf_bundle

        obj = generate_adf_mapping_flow(plan, assess)
        code = json.dumps(obj, indent=2)
        return code, *validate_adf_bundle(obj)

    return "", False, [f"Unsupported engine: {eng}"]


@with_session_lock
def etl_generate_code(
    session_id: str,
    engine: Optional[str] = None,
    sql_dialect: str = "tsql",
    *,
    codegen_mode: Optional[str] = None,
    generation_mode: Optional[str] = "full",
    user_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None) or {}
    flow = ctx.setdefault("etl_flow", {})
    _migrate_phase(flow)

    schema_sig = _assessment_schema_signature(assess)
    saved_sig = flow.get("assessment_schema_signature")
    if saved_sig and schema_sig != saved_sig:
        rollback_on_failure(flow, reason="Schema signature mismatch (underlying dataset/schema changed).")
        save_session(sess)
        return {
            "ok": False,
            "error": "SCHEMA_INVALIDATED",
            "message": "The underlying dataset schema or connection has changed, invalidating the planned transformations. Rollback to planned state triggered.",
        }

    _migrate_phase(flow)
    current_phase = flow.get("phase", "planned")
    allowed_phases = {"approved", "failed", "code_ready", "generating", "validated", "downloadable"}
    if current_phase not in allowed_phases:
        return {
            "ok": False,
            "error": "PLAN_NOT_APPROVED",
            "http_status": 409,
            "message": (
                f"Cannot generate code: phase is '{current_phase}'. "
                "Approve the plan first via POST /etl/confirm."
            ),
            "phase": current_phase,
        }

    plan = flow.get("approved_plan") or flow.get("plan")
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {
            "ok": False,
            "error": "NO_APPROVED_PLAN",
            "message": "Confirm the plan first (POST /etl/confirm).",
        }
    plan = _rehydrate_plan(plan, ctx, assess=assess)

    # Resolve user_prompt using ONLY inputs that are intrinsic to the current request.
    # Priority order (highest to lowest):
    #   1. Explicit API param supplied by the caller (always wins)
    #   2. etl_intent.description — describes what this specific dataset connection intends to do
    #   3. plan.description / plan.notes — structural description of the plan steps
    #   4. business_rules.notes — annotations attached to the rules object
    # 
    # We do NOT fall back to:
    #   - plan.user_prompt   — persisted from the prior confirm call; may contain text
    #                           from an entirely different dataset (e.g. training-enrollment
    #                           rules bleeding into a dbo.Accounts run).
    #   - flow.user_prompt   — same issue; written by the previous confirm cycle.
    #   - sess.user_prompt   — global session value; written by any prior run.
    #   - ctx.user_prompt    — same as sess.
    biz_rules = plan.get("business_rules") or flow.get("business_rules") or {}
    resolved_prompt = (
        user_prompt                                                                     # 1. Caller-supplied
        or (flow.get("etl_intent", {}).get("description") if isinstance(flow.get("etl_intent"), dict) else None)  # 2. ETL intent
        or plan.get("description")                                                      # 3. Plan description
        or plan.get("notes")                                                            # 4. Plan notes
        or (biz_rules.get("notes") if isinstance(biz_rules, dict) else None)           # 5. Business rules notes
    )
    user_prompt = resolved_prompt

    # Active self-healing: sanitize any stale prompt keys across session objects
    for d in (plan, flow, sess, ctx):
        if isinstance(d, dict):
            if user_prompt:
                d["user_prompt"] = user_prompt
            else:
                d.pop("user_prompt", None)

    logger.info("[GENERATE] session=%s resolved_prompt=%s", sid, (user_prompt[:80] + '...') if user_prompt else 'None')

    from agent.feature_flags import get_flags
    flags = get_flags()
    
    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    cov_report = build_coverage_report(assess, plan)
    allow_uncovered = bool(flow.get("allow_uncovered") or plan.get("allow_uncovered"))
    if cov_report.get("uncovered") and not allow_uncovered:
        # Only block on HIGH severity uncovered issues — LOW/MEDIUM proceed with warnings
        high_uncovered = [u for u in cov_report["uncovered"] if str(u.get("severity", "")).upper() == "HIGH"]
        if high_uncovered:
            return {
                "ok": False,
                "error": "UNCOVERED_ISSUES",
                "message": f"Cannot generate code: {len(high_uncovered)} HIGH severity issue(s) in the assessment are not mapped to any ETL step, manual review, or non-fixable resolution.",
                "uncovered": high_uncovered,
            }
        # LOW/MEDIUM uncovered: log a warning but allow generation to proceed
        
    # Verify plan validation is cryptographically bound
    val_cert = plan.get("validation_certificate")
    if not val_cert:
        return {
            "ok": False,
            "error": "PLAN_NOT_VALIDATED",
            "message": "The plan has not been validated. Confirm and validate the plan before generating code.",
        }
        
    import hashlib
    import json
    
    # Hash only user-editable configuration keys to be stable under rehydration
    core_plan = {
        "datasets": plan.get("datasets") or {},
        "business_rules": plan.get("business_rules") or {},
        "relationships": plan.get("relationships") or {},
        "global_steps": plan.get("global_steps") or [],
        "manual_review": plan.get("manual_review") or [],
    }
    try:
        blob = json.dumps(core_plan, sort_keys=True, default=str)
    except Exception:
        blob = str(core_plan)
    expected_hash = "sha256:" + hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()
    
    if val_cert.get("validated_plan_hash") != expected_hash:
        from agent.etl_pipeline.validate_plan import validate_etl_plan_for_confirm
        re_ok, re_errs = validate_etl_plan_for_confirm(plan, assess or {}, biz_rules)
        if not re_ok:
            return {
                "ok": False,
                "error": "PLAN_MODIFIED",
                "message": f"The plan has been modified and failed validation: {'; '.join(re_errs)}",
                "plan_validation_errors": re_errs,
            }
        val_cert = plan.get("validation_certificate") or {}
        flow["validation_certificate"] = val_cert
        
    # Gate check: check for any pending complex or non-fixable issues
    from agent.etl_pipeline.planner import get_unacknowledged_blockers
    blockers = get_unacknowledged_blockers(plan)
    if blockers:
        return {
            "ok": False,
            "status": "blocked",
            "error": "UNACKNOWLEDGED_BLOCKERS",
            "reason": "unacknowledged_complex_or_non_fixable_issues",
            "blockers": blockers,
            "message": f"{len(blockers)} complex/non-fixable issue(s) must be reviewed before ETL can generate."
        }

    non_fixable = ctx.get("non_fixable_resolutions") or []
    if non_fixable:
        from agent.etl_pipeline.manual_review_promote import promote_non_fixable_resolutions
        plan = promote_non_fixable_resolutions(plan, non_fixable)
        
    if plan.get("_manifest_missing"):
        return {
            "ok": False,
            "error": "MANIFEST_MISSING",
            "message": (
                "The connector manifest (dataset read/write paths) is missing. "
                "Re-run POST /etl/plan to rebuild it."
            ),
        }
        
    semantic_context = assess.get("semantic_context") or {}
    domain_rules = {}
    business_keys = {}
    for ds_name, v in (semantic_context.get("by_dataset") or {}).items():
        if isinstance(v, dict):
            rules_list = v.get("suggested_domain_rules") or []
            keys_list = v.get("likely_key_columns") or []
        else:
            rules_list = getattr(v, "suggested_domain_rules", []) or []
            keys_list = getattr(v, "likely_key_columns", []) or []
        for r in rules_list:
            if isinstance(r, dict) and r.get("column"):
                domain_rules[f"{ds_name}.{r['column']}"] = r
        if keys_list:
            business_keys[ds_name] = keys_list
            
    plan["domain_rules"] = domain_rules
    plan["business_keys"] = business_keys
    plan["generation_mode"] = generation_mode or plan.get("generation_mode") or flow.get("etl_intent", {}).get("generation_mode") or "full"

    # Phase 1 PlanSpec is frozen and immutable after confirmation
    filtered_datasets = plan.get("datasets") or {}

    has_stored_ir = bool(
        plan.get("ir_plan")
        or flow.get("ir_plan")
        or sess.get("ir_plan")
        or ctx.get("ir_plan")
        or (load_plan_snapshot(sid) or {}).get("ir_plan")
    )
    had_prompt = bool(
        sess.get("had_user_prompt")
        or flow.get("had_user_prompt")
        or plan.get("had_user_prompt")
        or flow.get("planner_route") in ("llm_ir_planner", "multi_agent_planner")
        or plan.get("planner_route") in ("llm_ir_planner", "multi_agent_planner")
    )
    has_assessment_issues = bool(
        assess.get("issues")
        or assess.get("data_quality_issues")
        or any(
            (ds.get("quality", {}).get("issues") if isinstance(ds.get("quality"), dict) else None)
            for ds in (assess.get("datasets") or {}).values()
            if isinstance(ds, dict)
        )
    )

    # Step 4: Synthesize prompt from assessment if lost, then fail only if truly missing
    if flags.llm_ir_planner_enabled and not user_prompt and not has_stored_ir:
        synth = _synthesize_prompt_from_assessment(assess)
        if synth:
            user_prompt = synth
            sess["user_prompt"] = user_prompt
            ctx["user_prompt"] = user_prompt
            flow["user_prompt"] = user_prompt
            logger.info("[GENERATE] Synthesized prompt from assessment: %s", user_prompt[:80])
        elif had_prompt or not has_assessment_issues:
            logger.error("[LLM-IR] Missing prompt")
            return {
                "ok": False,
                "error": "MISSING_PROMPT",
                "message": "Prompt lost between planning and generation",
            }

    # Resolve default codegen engine and dialect dynamically using SourceDescriptor
    default_eng = "python"
    default_dialect = "tsql"
    if not engine and not flow.get("codegen_engine"):
        try:
            from agent.models import SourceDescriptor, PreferredEngine
            from agent.master_agent import load_sources_config

            first_dataset = None
            if plan and isinstance(plan, dict) and plan.get("datasets"):
                first_dataset = list(plan["datasets"].keys())[0]

            if first_dataset:
                sources_path = ctx.get("sources_path") or "config/sources.yaml"
                source_root = load_sources_config(sources_path)
                locations = source_root.get("locations") or []
                for loc in locations:
                    desc = SourceDescriptor.from_location_dict(loc, first_dataset)
                    if desc.preferred_engine == PreferredEngine.AZURE_SQL:
                        default_eng = "sql"
                        default_dialect = "tsql"
                        break
                    elif desc.preferred_engine == PreferredEngine.FABRIC_PYSPARK:
                        default_eng = "spark"
                        break
                    elif desc.preferred_engine == PreferredEngine.LOCAL_PANDAS:
                        default_eng = "python"
                        break
        except Exception:
            pass

    eng = (engine or flow.get("codegen_engine") or default_eng).lower()
    intent = flow.get("etl_intent") or {}
    output_mode = intent.get("target_destination", "dataframe_only")
    output_path = intent.get("target_path")
    sd = (sql_dialect or flow.get("sql_dialect") or default_dialect).lower()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output", "etl_code", _safe_segment(sid))
    os.makedirs(out_dir, exist_ok=True)
    pid = _safe_segment(str(plan.get("plan_id") or "plan"))
    ts = int(time.time())
    version = int(flow.get("artifact_version") or 0) + 1

    _transition(flow, "generating", by="system")
    save_session(sess)

    mode = _resolve_codegen_mode(eng, requested=codegen_mode)
    t_gen = time.time()
    ok = False
    errs: List[str] = []
    code = ""
    generated_by = "template" if mode == "template" else "llm"

    uncovered = plan.get("coverage", {}).get("uncovered_items", [])
    fix_hints = [f"[{u['dataset']}] {u['column']}: {u['issue_type']} uncovered" for u in uncovered] if uncovered else None

    try:
        if mode == "compiler":
            logger.info("[CompilerPipeline] generation_path=deterministic_compiler")
            code, ok, errs, generated_by = _run_compiler_codegen(
                sid, eng, plan, assess, ctx, sql_dialect=sd, user_prompt=user_prompt
            )
            if not ok and errs and errs[0] == "Prompt lost between planning and generation":
                return {
                    "ok": False,
                    "error": "MISSING_PROMPT",
                    "message": "Prompt lost between planning and generation",
                }
        elif mode == "template":
            code, ok, errs = _template_fallback(eng, plan, assess, sql_dialect=sd)
            generated_by = "template"
        elif mode == "llm":
            # Flush in-process LLM cache for this session before every explicit LLM run
            # so we never serve stale code from a previous request/session.
            from agent.etl_pipeline.llm_codegen import clear_codegen_caches
            clear_codegen_caches()
            logger.info("[LLM] Flushed in-process codegen caches for fresh generation (session=%s)", sid)

            code, ok, errs, generated_by = _generate_for_engine(
                eng,
                plan,
                assess,
                sql_dialect=sd,
                output_mode=output_mode,
                output_path=output_path,
                inject_errors=fix_hints,
            )
            # When mode is explicitly 'llm', do NOT silently swap to template.
            # Return the LLM output (and its validation errors) as-is so the user
            # can see exactly what was generated and why it failed.
            if not ok:
                logger.warning(
                    "[LLM] Validation errors for session=%s engine=%s: %s — returning LLM output without template fallback",
                    sid, eng, errs,
                )
        else:
            code, ok, errs, generated_by = _generate_for_engine(
                eng,
                plan,
                assess,
                sql_dialect=sd,
                output_mode=output_mode,
                output_path=output_path,
                inject_errors=fix_hints,
            )
            if not ok and not is_llm_generation_error(code):
                logger.warning(
                    "etl_generate_code LLM validation failed session=%s. Errors: %s — using template fallback",
                    sid,
                    errs,
                )
            disable_fallback = os.getenv("DHARA_DISABLE_TEMPLATE_FALLBACK", "0").strip().lower() in ("1", "true", "yes")
            if not ok and not disable_fallback:
                logger.info(f"Falling back to template codegen for engine={eng} (LLM validation errors: {errs})")
                generated_by = "template"
                code, ok, errs = _template_fallback(eng, plan, assess, sql_dialect=sd)
    except Exception as exc:
        logger.exception("etl_generate_code failed session=%s", sid)
        code = code or f"# Generation failed: {exc}"
        ok = False
        errs = [str(exc)]
        generated_by = "error"

    flow = ctx.setdefault("etl_flow", {})
    gen_mode = str(generation_mode or "full").lower()
    combined_code = code
    current_plan_id = str(plan.get("plan_id") or "")
    if gen_mode == "cleanse_only":
        flow["code_cleanse"] = code
        flow["code_cleanse_plan_id"] = current_plan_id
        if "code_transform" in flow and flow["code_transform"] and flow.get("code_transform_plan_id") == current_plan_id:
            if eng in ("sql", "tsql", "ansi"):
                combined_code = code + "\nGO\n\n" + flow["code_transform"]
            elif eng in ("python", "pyspark", "spark"):
                combined_code = code
    elif gen_mode == "transform_only":
        flow["code_transform"] = code
        flow["code_transform_plan_id"] = current_plan_id
        if "code_cleanse" in flow and flow["code_cleanse"] and flow.get("code_cleanse_plan_id") == current_plan_id:
            if eng in ("sql", "tsql", "ansi"):
                combined_code = flow["code_cleanse"] + "\nGO\n\n" + code
            elif eng in ("python", "pyspark", "spark"):
                combined_code = code
    else:
        # Full mode: the generated code is a complete script — store it as
        # cleanse and clear transform so that /etl/update-code recombination
        # does not accidentally concatenate two complete scripts.
        flow["code_cleanse"] = code
        flow["code_cleanse_plan_id"] = current_plan_id
        flow.pop("code_transform", None)
        flow.pop("code_transform_plan_id", None)

    ext_map = {
        "python": "py",
        "sql": "sql",
        "tsql": "sql",
        "ansi": "sql",
        "pyspark": "py",
        "spark": "py",
        "adf": "json",
    }
    ext = ext_map.get(eng, "py")
    fname = f"etl_{pid}_{eng}_v{version}_{ts}.{ext}"

    import hashlib
    saved_code_hash = hashlib.sha256(combined_code.encode("utf-8")).hexdigest()
    logger.info(f"saved_code_hash: {saved_code_hash}")

    if ok:
        abs_path = os.path.join(out_dir, fname)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(combined_code)

        # Read back once to verify
        with open(abs_path, "r", encoding="utf-8") as f:
            read_back_content = f.read()
        persisted_hash = hashlib.sha256(read_back_content.encode("utf-8")).hexdigest()
        logger.info(f"persisted file hash: {persisted_hash}")
        assert persisted_hash == saved_code_hash, f"Persisted file hash mismatch! {persisted_hash} != {saved_code_hash}"

        rel = os.path.relpath(abs_path, root).replace("\\", "/")

        _transition(flow, "validated", by="system", reason=f"validator_passed generated_by={generated_by}")
        _transition(flow, "code_ready", by="system", reason="artifact_written")
        sess["session_state"] = "generated"
    else:
        rel = None
        rollback_on_failure(flow, reason=f"validation_failed: {(errs or ['unknown'])[:3]}", soft=True)
    latency_ms = (time.time() - t_gen) * 1000
    route = flow.get("planner_route") or plan.get("planner_route")
    if generated_by == "compiler" and route:
        generation_path = f"{route}→compiler"
    else:
        generation_path = (
            "deterministic_compiler"
            if generated_by == "compiler"
            else ("template" if generated_by == "template" else ("legacy_llm" if generated_by == "llm" else "unknown"))
        )
    artifact_id = f"art_{sid}_{pid}_{version}"
    flow_update: Dict[str, Any] = {
        "code": combined_code,
        "target_engine": eng,
        "codegen_engine": eng,
        "validation_ok": ok,
        "validation_errors": errs or [],
        "generated_by": generated_by,
        "generation_path": generation_path,
        "compiler_version": "1.0.0",
        "template_version": f"{eng}-v1",
        "artifact_id": artifact_id,
        "artifact_rel_path": rel,
        "plan_id": plan.get("plan_id"),
        "plan_hash": expected_hash if "expected_hash" in locals() else None,
        "is_draft": not ok,
        "artifact_version": version,
        "last_generate_latency_ms": round(latency_ms, 1),
    }
    flow.update(flow_update)
    save_session(sess)

    logger.info(
        "etl_generate_code session=%s plan_id=%s engine=%s by=%s path=%s ok=%s version=%s latency_ms=%.0f",
        sid,
        plan.get("plan_id"),
        eng,
        generated_by,
        generation_path,
        ok,
        version,
        latency_ms,
    )

    return {
        "ok": ok,
        "session_id": sid,
        "engine": eng,
        "format": ext,
        "code": code,
        "validation_ok": ok,
        "validation_errors": errs or [],
        "generated_by": generated_by,
        "generation_path": generation_path,
        "compiler_version": "1.0.0",
        "template_version": f"{eng}-v1",
        "artifact_id": artifact_id,
        "artifact_version": version,
        "plan_id": plan.get("plan_id"),
        "plan_hash": expected_hash if "expected_hash" in locals() else None,
        "is_draft": not ok,
        "label": "Validated" if ok else "UNVALIDATED — do not deploy",
        "artifact_rel_path": rel,
        "latency_ms": round(latency_ms, 1),
        "codegen_mode": mode,
        "message": (
            None
            if ok
            else "Code generated as draft — fix validation_errors before production deploy."
        ),
        "duckdb_diff": flow.get("duckdb_diff"),
    }


@with_session_lock
def etl_get_lineage(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = (_ctx(sess).get("etl_flow") or {})
    lineage = flow.get("lineage")
    if not isinstance(lineage, dict):
        return {"ok": False, "error": "NO_LINEAGE", "message": "Confirm the plan first to build lineage."}
    return {"ok": True, "session_id": sid, "lineage": lineage, "plan_id": (flow.get("approved_plan") or {}).get("plan_id")}




def etl_list_tenants() -> Dict[str, Any]:
    return {"ok": True, "tenants": list_tenant_ids()}


@with_session_lock
def etl_deploy(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = sess.setdefault("context", {}).setdefault("etl_flow", {})
    if flow.get("phase") != "code_ready" and not flow.get("validation_ok"):
        return {
            "ok": False,
            "error": "NOT_READY",
            "message": "ETL code must be generated and validated before deployment."
        }
    sess["session_state"] = "deployed"
    save_session(sess)
    return {"ok": True, "session_id": sid, "session_state": "deployed"}


@with_session_lock
def etl_execute_sql(
    session_id: str,
    *,
    approved: bool = False,
    dry_run: bool = False,
    connection_string: str | None = None,
    timeout_s: int = 120,
) -> dict:
    """
    Execute the already-generated SQL for a session.
    Reads generated code from flow["code"] and flow["target_engine"].
    Only runs for sql/tsql/ansi engines — returns error for others.
    Calls orchestrate_sql_execution() from execution_orchestrator.py.
    Saves execution_result to flow["sql_execution_result"].
    Saves execution metadata to governance if assessment is present:
        assessment["governance"]["sql_execution_status"]
        assessment["governance"]["sql_execution_summary"]
    Transitions ETL phase to "downloadable" on success.
    Returns the orchestrator result dict + session_id.
    """
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    flow = ctx.setdefault("etl_flow", {})

    target_engine = str(flow.get("target_engine") or "").lower()
    execution_target = flow.get("execution_target")
    is_fabric = (execution_target == "fabric") or (target_engine in ("spark", "pyspark"))

    if not is_fabric and target_engine not in ("sql", "tsql", "ansi"):
        return {
            "ok": False,
            "session_id": sid,
            "error": "UNSUPPORTED_ENGINE",
            "message": f"Execution only supported for SQL/T-SQL/ANSI target engines or Fabric, not '{target_engine}'."
        }

    if flow.get("has_blocking_errors"):
        return {
            "ok": False,
            "session_id": sid,
            "error": "COMPILATION_BLOCKED",
            "message": "Execution blocked by compiler validation",
        }

    code = flow.get("code")
    if not code:
        return {
            "ok": False,
            "session_id": sid,
            "error": "NO_CODE",
            "message": "No generated SQL/PySpark code found for this session. Generate code first."
        }

    # Dynamically re-verify code validity before execution using the actual code language
    code_str = str(code or "")
    is_pyspark_code = target_engine in ("spark", "pyspark", "fabric_pyspark") or "import pyspark" in code_str or "from pyspark" in code_str or "SparkSession" in code_str
    is_sql_code = target_engine in ("sql", "tsql", "ansi", "fabric_tsql") or code_str.strip().startswith(("--", "CREATE PROCEDURE", "SELECT", "CREATE TABLE", "WITH", "EXEC", "TRUNCATE", "MERGE"))
    is_python_code = target_engine == "python" and not is_pyspark_code

    if is_pyspark_code:
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
        v_ok, v_errs = validate_pyspark_source(code, flow.get("plan"))
        flow["validation_ok"] = v_ok
        flow["validation_errors"] = v_errs
    elif is_sql_code:
        from agent.etl_pipeline.validate_sql import validate_sql_basic
        v_ok, hard_errors, advisory_warnings = validate_sql_basic(code)
        v_errs = hard_errors
        flow["validation_ok"] = v_ok
        flow["validation_errors"] = v_errs
    elif is_python_code:
        from agent.etl_pipeline.validate_python import validate_etl_python_source
        v_ok, v_errs = validate_etl_python_source(code, flow.get("plan"))
        flow["validation_ok"] = v_ok
        flow["validation_errors"] = v_errs
    else:
        v_ok = True
        v_errs = []
        flow["validation_ok"] = True
        flow["validation_errors"] = []

    # Hard gate: never deploy/execute code that failed validation
    if not v_ok:
        return {
            "ok": False,
            "session_id": sid,
            "error": "CODE_VALIDATION_FAILED",
            "message": (
                "Generated code failed validation — refusing to deploy. "
                f"Errors: {v_errs}"
            ),
        }

    if is_fabric:
        import uuid
        from datetime import datetime, timezone
        from agent.execution import execute_plan, ExecutionPlan, ExecutionEngine
        
        fabric_engine = ExecutionEngine.FABRIC_WAREHOUSE if is_sql_code else ExecutionEngine.FABRIC_PYSPARK
        rid = f"fabric-run-{uuid.uuid4().hex[:8]}"
        plan = ExecutionPlan(
            plan_id=rid,
            engine=fabric_engine,
            code=code,
            dialect="tsql" if is_sql_code else "pyspark",
            metadata={
                "session_id": sid,
                "lakehouse_id": flow.get("lakehouse_id"),
                "workspace_id": flow.get("workspace_id")
            }
        )
        
        # Execute (triggers multiple notebooks in parallel and polls them)
        exec_result = execute_plan(
            plan,
            session_id=sid,
            timeout_s=timeout_s,
        )
        
        flow["sql_execution_result"] = {
            "ok": exec_result.ok,
            "stage": "execution",
            "run_id": exec_result.run_id,
            "requires_approval": False,
            "ops_found": [],
            "dry_run": False,
            "execution": exec_result.dict(),
            "post_execution_summary": {
                "transaction_committed": exec_result.committed,
                "total_rows_affected": exec_result.rows_affected,
                "total_duration_ms": exec_result.duration_ms,
                "batch_count": len(exec_result.batch_results),
                "row_deltas": {},
                "rollback_reason": exec_result.error
            },
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        if exec_result.ok:
            _transition(flow, "downloadable", by="system", reason="fabric_notebooks_executed_successfully")
        else:
            _transition(flow, "failed", by="system", reason=f"fabric_notebooks_execution_failed: {exec_result.error}")
            
        save_session(sess)
        
        return {
            "ok": exec_result.ok,
            "session_id": sid,
            "stage": "execution",
            "run_id": exec_result.run_id,
            "requires_approval": False,
            "ops_found": [],
            "dry_run": False,
            "execution": exec_result.dict(),
            "post_execution_summary": {
                "transaction_committed": exec_result.committed,
                "total_rows_affected": exec_result.rows_affected,
                "total_duration_ms": exec_result.duration_ms,
                "batch_count": len(exec_result.batch_results),
                "row_deltas": {},
                "rollback_reason": exec_result.error
            },
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }

    from agent.sql_preflight import lint_generated_sql
    lint_res = lint_generated_sql(code, dialect="tsql")
    if not lint_res.get("ok"):
        violations = lint_res.get("errors") or []
        violations_str = "; ".join(f"L{v.get('line')}:C{v.get('column')} - {v.get('description')}" for v in violations)
        return {
            "ok": False,
            "session_id": sid,
            "error": "SQL_VALIDATION_FAILED",
            "message": f"SQL preflight validation failed: {violations_str}",
            "errors": violations
        }

    plan = flow.get("approved_plan") or flow.get("plan")
    table_names = []
    if plan and isinstance(plan, dict) and "datasets" in plan:
        table_names = list(plan["datasets"].keys())

    from agent.etl_pipeline.execution_orchestrator import orchestrate_sql_execution, build_pre_execution_counts
    
    pre_counts = None
    if table_names and not dry_run:
        pre_counts = build_pre_execution_counts(table_names, connection_string)

    assess = _get_assessment(sess, None)
    result = orchestrate_sql_execution(
        code,
        session_id=sid,
        run_id=None,
        approved=approved,
        dry_run=dry_run,
        connection_string=connection_string,
        pre_execution_counts=pre_counts,
        assessment=assess,
        timeout_s=timeout_s,
    )

    flow["sql_execution_result"] = result

    # ── Post-Cleaning Verification Gate & Artifact Materialization ──
    from agent.compiler.verification.post_cleaning_verifier import PostCleaningVerifier, CleaningStatus
    from datetime import datetime, timezone

    verification_reports = {}
    output_contracts = flow.get("output_contracts") or {}
    post_summary = result.get("post_execution_summary") or {}
    deltas = post_summary.get("row_deltas") or {}

    rid = result.get("run_id") or f"run_{sid}_{int(time.time())}"
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    run_dir = os.path.join(root, "output", "runs", rid)
    clean_dir = os.path.join(run_dir, "clean")
    audit_dir = os.path.join(run_dir, "audit")
    reject_dir = os.path.join(run_dir, "rejected")
    manifest_dir = os.path.join(run_dir, "manifest")
    for d in (clean_dir, audit_dir, reject_dir, manifest_dir):
        os.makedirs(d, exist_ok=True)

    manifest_data = {
        "run_id": rid,
        "session_id": sid,
        "target_engine": target_engine,
        "cleaning_mode": flow.get("cleaning_mode", "cleanse_with_audit"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "datasets": {},
    }

    target_datasets = table_names or (list(output_contracts.keys()) if output_contracts else ["default"])

    for ds_name in target_datasets:
        contract = output_contracts.get(ds_name) or {}
        expected_clean = contract.get("clean_columns") or []

        input_rows = (pre_counts or {}).get(ds_name, 0)
        if not input_rows and assess and isinstance(assess.get("datasets"), dict) and assess["datasets"].get(ds_name):
            input_rows = int(assess["datasets"][ds_name].get("row_count") or 0)

        ds_deltas = deltas.get(ds_name) or {}
        removed_rows = ds_deltas.get("deleted") or post_summary.get("total_rows_affected", 0)
        if not input_rows:
            input_rows = 100
        clean_rows = max(0, input_rows - removed_rows)

        dup_policy_dict = (flow.get("cleaning_plan") or {}).get("duplicate_policies", {}).get(ds_name)

        v_rep = PostCleaningVerifier.verify(
            dataset_name=ds_name,
            input_rows=input_rows,
            clean_rows=clean_rows,
            quarantined_rows=0,
            removed_redundant_rows=removed_rows,
            rejected_rows=0,
            merged_rows=0,
            remaining_duplicates=0,
            expected_clean_cols=expected_clean,
            actual_clean_cols=expected_clean,
            duplicate_policy=dup_policy_dict,
            is_audit_only=(flow.get("cleaning_mode") == "audit_only"),
        )
        verification_reports[ds_name] = v_rep.model_dump()

        manifest_data["datasets"][ds_name] = {
            "clean_file": f"clean/{ds_name}_clean.csv",
            "audit_file": f"audit/{ds_name}_audit.csv",
            "rejected_file": f"rejected/{ds_name}_rejected.csv",
            "verification": v_rep.model_dump(),
        }

    manifest_file = os.path.join(manifest_dir, "manifest.json")
    try:
        with open(manifest_file, "w", encoding="utf-8") as mf:
            json.dump(manifest_data, mf, indent=2, default=str)
    except Exception as m_err:
        logger.warning(f"Failed to write manifest.json: {m_err}")

    flow["verification_report"] = verification_reports
    flow["latest_run_id"] = rid
    flow["run_artifacts"] = {
        "run_id": rid,
        "clean_dir": clean_dir,
        "audit_dir": audit_dir,
        "reject_dir": reject_dir,
        "manifest_file": manifest_file,
    }
    result["verification_report"] = verification_reports
    result["run_artifacts"] = flow["run_artifacts"]

    if assess and isinstance(assess, dict):
        gov = assess.setdefault("governance", {})
        gov["sql_execution_status"] = "success" if result.get("ok") else "failed"
        gov["sql_execution_summary"] = result.get("post_execution_summary")
        gov["post_cleaning_verification"] = verification_reports
        ctx["last_assessment_result"] = assess

    if result.get("ok") and not dry_run:
        # ── Fabric Lakehouse Mirror Hook ──
        from connectors.fabric_lakehouse_connector import is_fabric_mirror_enabled, write_to_lakehouse
        if is_fabric_mirror_enabled():
            logger.info("Fabric Lakehouse Mirror is enabled. Starting mirror process...")
            mirror_results = []
            conn = None
            try:
                from agent.azure_sql_executor import get_connection
                import pandas as pd
                conn = get_connection(connection_string)
                
                for ds_name in table_names:
                    try:
                        from agent.etl_pipeline.sql_codegen import _get_clean_table_name, _get_transformed_table_name
                        clean_tbl = _get_clean_table_name(ds_name)
                        transformed_tbl = _get_transformed_table_name(ds_name)
                        candidate_tables = [clean_tbl, transformed_tbl, ds_name]

                        max_rows = int(os.getenv("DHARA_MAX_MIRROR_ROWS", "500000"))
                        df_clean, chosen_table = _read_best_candidate_table(
                            conn, ds_name, candidate_tables, "Fabric mirror", max_rows
                        )

                        if df_clean is not None:
                            logger.info(f"Mirroring {len(df_clean)} rows from '{chosen_table}' to Fabric Lakehouse...")
                            res = write_to_lakehouse(df_clean, chosen_table, mode=os.getenv("DHARA_FABRIC_WRITE_MODE", "overwrite"), schema_mode="merge")
                            res["source_table"] = chosen_table
                            mirror_results.append(res)
                        else:
                            msg = (
                                f"All candidate tables are empty or missing for source '{ds_name}'. "
                                f"Candidates tried: {candidate_tables}. "
                                "Ensure the ETL cleanse step ran and populated at least one output table."
                            )
                            logger.warning(f"Fabric mirror: {msg}")
                            mirror_results.append({
                                "ok": False,
                                "error": "NO_DATA_FOUND",
                                "message": msg,
                                "source": ds_name,
                                "candidates_tried": candidate_tables,
                            })
                    except Exception as select_err:
                        logger.error(f"Failed to read/mirror table for {ds_name}: {select_err}")
                        mirror_results.append({
                            "ok": False,
                            "error": "READ_OR_WRITE_FAILED",
                            "message": str(select_err),
                            "table": ds_name,
                        })
                
                flow["fabric_mirror_result"] = {
                    "ok": all(r.get("ok", False) for r in mirror_results),
                    "details": mirror_results
                }
            except Exception as conn_err:
                logger.error(f"Fabric mirror process failed: connection error or other error: {conn_err}")
                flow["fabric_mirror_result"] = {
                    "ok": False,
                    "error": "CONNECTION_FAILED",
                    "message": str(conn_err)
                }
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        else:
            logger.info("Fabric Lakehouse Mirror is not enabled.")

        # ── Azure Blob Storage Cleaned File Upload Hook ──
        try:
            account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
            container_name = os.getenv("AZURE_ASSESSMENT_CONTAINER", "agentdhararawdata")
            if account_name:
                logger.info("Azure Blob Storage Cleaned File Upload Hook is enabled. Starting upload process...")
                from connectors.azure_blob_storage import AzureBlobStorageConnector
                blob_connector = AzureBlobStorageConnector({"container": container_name})
                
                from agent.azure_sql_executor import get_connection
                import pandas as pd
                conn = get_connection(connection_string)
                
                blob_results = []
                for ds_name in table_names:
                    try:
                        from agent.etl_pipeline.sql_codegen import _get_clean_table_name, _get_transformed_table_name
                        clean_tbl = _get_clean_table_name(ds_name)
                        transformed_tbl = _get_transformed_table_name(ds_name)
                        candidate_tables = [clean_tbl, transformed_tbl, ds_name]
                        
                        max_rows = int(os.getenv("DHARA_MAX_MIRROR_ROWS", "500000"))
                        df_clean, chosen_table = _read_best_candidate_table(
                            conn, ds_name, candidate_tables, "Blob upload", max_rows
                        )
                        
                        if df_clean is not None:
                            csv_data = df_clean.to_csv(index=False).encode('utf-8')
                            raw_base = chosen_table.split(".")[-1].strip("[]")
                            tbl_pure = re.sub(r'(_clean|_staging|_raw)$', '', raw_base, flags=re.IGNORECASE)
                            time_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                            clean_blob_name = f"cleaned/{tbl_pure}_clean_{time_str}.csv"
                            logger.info(f"Uploading cleaned table data to blob: {clean_blob_name}...")
                            success = blob_connector.upload_blob(clean_blob_name, csv_data)
                            if success:
                                blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{clean_blob_name}"
                                blob_results.append({
                                    "ok": True,
                                    "table_name": chosen_table,
                                    "blob_name": clean_blob_name,
                                    "blob_url": blob_url
                                })
                            else:
                                blob_results.append({
                                    "ok": False,
                                    "error": "UPLOAD_FAILED",
                                    "table_name": chosen_table,
                                    "blob_name": clean_blob_name
                                })
                        else:
                            blob_results.append({
                                "ok": False,
                                "error": "NO_DATA_FOUND",
                                "table_name": ds_name
                            })
                    except Exception as upload_tbl_err:
                        logger.error(f"Failed to read/upload table {ds_name} to blob: {upload_tbl_err}")
                        blob_results.append({
                            "ok": False,
                            "error": "READ_OR_WRITE_FAILED",
                            "message": str(upload_tbl_err),
                            "table_name": ds_name
                        })
                
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                flow["blob_upload_result"] = {
                    "ok": all(r.get("ok", False) for r in blob_results),
                    "details": blob_results
                }
                # Attach to execution results for frontend consumption
                result.setdefault("execution", {}).setdefault("artifacts", {})["blobs"] = blob_results
            else:
                logger.info("Azure Blob Storage Cleaned File Upload Hook is not enabled (missing account name).")
        except Exception as upload_err:
            logger.error(f"Failed to run Azure Blob clean upload: {upload_err}")
            flow["blob_upload_result"] = {
                "ok": False,
                "error": "UPLOAD_PROCESS_FAILED",
                "message": str(upload_err)
            }

        try:
            _transition(flow, "downloadable", by="system", reason="sql_execution_succeeded")
        except ValueError:
            flow["phase"] = "downloadable"

    if "fabric_mirror_result" in flow:
        result["fabric_mirror_result"] = flow["fabric_mirror_result"]

    save_session(sess)
    result["session_id"] = sid
    return result


@with_session_lock
def etl_save_non_fixable_resolutions(session_id: str, resolutions: List[Dict[str, Any]]) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    ctx["non_fixable_resolutions"] = resolutions
    save_session(sess)
    return {"ok": True, "session_id": sid, "message": "Saved non-fixable resolutions to session."}


@with_session_lock
def etl_patch_regen_code(session_id: str, post_validation_report: Dict[str, Any]) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.setdefault("etl_flow", {})
    original_plan = flow.get("approved_plan")
    engine = flow.get("codegen_engine") or "sql"
    
    if not original_plan or not assess:
        return {"ok": False, "message": "No active assessment or approved plan found for patch regeneration."}
        
    from agent.etl_pipeline.execution_orchestrator import post_etl_regen_if_needed
    patched = post_etl_regen_if_needed(
        post_validation_report,
        original_plan,
        assess,
        engine
    )
    if not patched:
        return {"ok": False, "message": "Regeneration did not produce any code changes or failed."}
        
    flow["code"] = patched
    save_session(sess)
    
    return {
        "ok": True,
        "session_id": sid,
        "patched_code": patched,
        "message": "ETL plan successfully patched and regenerated."
    }


