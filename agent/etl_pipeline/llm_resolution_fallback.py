"""
llm_resolution_fallback.py — Dynamic LLM-driven resolution fallback for unmapped issue types.

Features:
1. Strict per-run call budget (DHARA_MAX_LLM_RESOLUTION_CALLS, default 5).
2. Deduplication by unique resolution signature (issue_type, semantic_type, message_pattern).
3. Concurrent execution with asyncio.Semaphore (DHARA_LLM_RESOLUTION_CONCURRENCY, default 3).
4. Strict schema validation: action MUST exist in ACTION_REGISTRY allowlist; destructive actions require review.
5. In-memory + persistent cache.
6. Safe non-blocking execution: NO asyncio.run() in library code.
"""
from __future__ import annotations

from enum import Enum
import asyncio
import json
import logging
import os
import threading
from typing import Any, Dict, List, Literal, Optional, Set, Tuple
from pydantic import BaseModel, model_validator

from agent.etl_pipeline.action_registry import ACTION_REGISTRY


class ResolutionStatus(str, Enum):
    SUPPORTED = "supported"
    UNSUPPORTED_ACTION = "unsupported_action"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"
    INVALID_RECOMMENDATION = "invalid_recommendation"


class AdvisoryExtensionProposal(BaseModel):
    proposal_id: str
    issue_id: str
    raw_proposed_action: str
    normalized_action_label: Optional[str] = None
    proposed_parameters: Dict[str, Any] = {}
    rationale: Optional[str] = None
    resolution_status: ResolutionStatus = ResolutionStatus.UNSUPPORTED_ACTION
    execution_eligible: Literal[False] = False
    requires_human_approval: Literal[True] = True
    source: str = "llm_resolution_fallback"
    source_model: Optional[str] = None
    interpretation_id: Optional[str] = None
    reason_code: str = "unmapped_action"

    @model_validator(mode="after")
    def validate_proposal(self) -> "AdvisoryExtensionProposal":
        if self.execution_eligible is not False:
            raise ValueError("Advisory proposals must never be execution eligible")
        if self.resolution_status == ResolutionStatus.SUPPORTED:
            raise ValueError("Supported actions must use typed action models, not AdvisoryExtensionProposal")
        return self
from agent.etl_pipeline.issue_identity import (
    make_resolution_signature,
    normalize_action,
    normalize_issue_type,
    normalize_message_pattern,
)
from agent.model_config import load_llm_config

logger = logging.getLogger("agent.etl_pipeline.llm_resolution_fallback")

# Configuration via environment variables
DHARA_LLM_RESOLUTION_FALLBACK = os.getenv("DHARA_LLM_RESOLUTION_FALLBACK", "1").strip().lower() in ("1", "true", "yes")
DHARA_MAX_LLM_RESOLUTION_CALLS = int(os.getenv("DHARA_MAX_LLM_RESOLUTION_CALLS", "5"))
DHARA_LLM_RESOLUTION_TIMEOUT_SECONDS = float(os.getenv("DHARA_LLM_RESOLUTION_TIMEOUT_SECONDS", "20"))
DHARA_LLM_RESOLUTION_CONCURRENCY = int(os.getenv("DHARA_LLM_RESOLUTION_CONCURRENCY", "3"))
DHARA_RESOLUTION_CACHE_PATH = os.getenv(
    "DHARA_RESOLUTION_CACHE_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config",
        "llm_resolution_cache.json"
    )
)

_CACHE_LOCK = threading.Lock()
_IN_MEMORY_CACHE: Dict[str, List[Dict[str, Any]]] = {}


def _load_disk_cache() -> Dict[str, List[Dict[str, Any]]]:
    if not os.path.exists(DHARA_RESOLUTION_CACHE_PATH):
        return {}
    try:
        with open(DHARA_RESOLUTION_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        logger.debug("Could not read resolution cache: %s", e)
    return {}


def _save_disk_cache(cache: Dict[str, List[Dict[str, Any]]]) -> None:
    try:
        os.makedirs(os.path.dirname(DHARA_RESOLUTION_CACHE_PATH), exist_ok=True)
        with open(DHARA_RESOLUTION_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        logger.debug("Could not write resolution cache: %s", e)


def init_cache() -> None:
    global _IN_MEMORY_CACHE
    with _CACHE_LOCK:
        if not _IN_MEMORY_CACHE:
            _IN_MEMORY_CACHE = _load_disk_cache()


def get_cached_resolution(signature: str) -> Optional[List[Dict[str, Any]]]:
    init_cache()
    with _CACHE_LOCK:
        return _IN_MEMORY_CACHE.get(signature)


def put_cached_resolution(signature: str, options: List[Dict[str, Any]]) -> None:
    init_cache()
    with _CACHE_LOCK:
        _IN_MEMORY_CACHE[signature] = options
        _save_disk_cache(_IN_MEMORY_CACHE)


class LLMResolutionBudget:
    """Tracks budget per planning run."""
    def __init__(self, max_calls: int = DHARA_MAX_LLM_RESOLUTION_CALLS):
        self.max_calls = max_calls
        self.calls_used = 0
        self.seen_signatures: Set[str] = set()
        self._lock = threading.Lock()

    def can_call(self, signature: str) -> bool:
        with self._lock:
            if signature in self.seen_signatures:
                return True  # Deduplicated within run, does not spend extra budget
            return self.calls_used < self.max_calls

    def record_call(self, signature: str) -> None:
        with self._lock:
            if signature not in self.seen_signatures:
                self.seen_signatures.add(signature)
                self.calls_used += 1


_VALID_ACTION_NAMES = set(ACTION_REGISTRY.keys())


def validate_resolution_options(raw_options: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validates and sanitizes options returned by LLM.
    1. action MUST be in ACTION_REGISTRY. Unmapped actions are preserved as UNSUPPORTED_ACTION
       advisory proposals with execution_eligible=False (never silently overwritten to noop).
    2. confidence must be float between 0 and 1.
    3. Destructive actions set requires_human_approval = True.
    """
    valid_options: List[Dict[str, Any]] = []
    has_recommended = False

    for opt in raw_options:
        if not isinstance(opt, dict):
            continue
        opt_id = str(opt.get("id") or "").strip()
        label = str(opt.get("label") or opt_id).strip()
        raw_action_val = opt.get("action")
        action = normalize_action(raw_action_val)
        if not opt_id or not label:
            continue

        confidence = opt.get("confidence")
        try:
            confidence = float(confidence) if confidence is not None else 0.8
            confidence = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            confidence = 0.8

        if action not in _VALID_ACTION_NAMES:
            logger.warning(
                "LLM resolution action '%s' not in ACTION_REGISTRY; recording as unsupported advisory proposal (never noop)",
                action,
            )
            valid_options.append({
                "id": opt_id,
                "label": f"[Advisory] {label}",
                "action": "unsupported_action",
                "raw_action": str(raw_action_val or action),
                "recommended": False,
                "description": str(opt.get("description") or f"Proposed action '{raw_action_val}' is outside the supported action catalog."),
                "confidence": confidence,
                "requires_human_approval": True,
                "resolution_source": "llm_fallback",
                "resolution_status": ResolutionStatus.UNSUPPORTED_ACTION.value,
                "execution_eligible": False,
                "proposed_parameters": opt.get("parameters") or opt.get("params") or {},
                "rationale": opt.get("rationale") or opt.get("description"),
            })
            continue

        act_def = ACTION_REGISTRY.get(action, {})
        is_destructive = bool(act_def.get("destructive"))
        requires_review = bool(act_def.get("requires_review") or is_destructive)

        recommended = bool(opt.get("recommended"))
        if recommended and not has_recommended:
            has_recommended = True
        elif recommended and has_recommended:
            recommended = False

        valid_options.append({
            "id": opt_id,
            "label": label,
            "action": action,
            "raw_action": str(raw_action_val or action),
            "recommended": recommended,
            "description": str(opt.get("description") or ""),
            "confidence": confidence,
            "requires_human_approval": requires_review,
            "resolution_source": "llm_fallback",
            "resolution_status": ResolutionStatus.SUPPORTED.value,
            "execution_eligible": True,
        })

    # Ensure at least one keep_as_is or noop option exists
    if not any(o["action"] in ("noop", "keep_as_is") for o in valid_options):
        valid_options.append({
            "id": "keep_as_is",
            "label": "Keep as-is (skip in ETL)",
            "action": "noop",
            "raw_action": "noop",
            "recommended": not has_recommended,
            "description": "No transformation applied; accept data as-is.",
            "confidence": 1.0,
            "requires_human_approval": False,
            "resolution_source": "default_safe",
            "resolution_status": ResolutionStatus.SUPPORTED.value,
            "execution_eligible": True,
        })

    if valid_options and not any(o.get("recommended") for o in valid_options):
        eligible = [o for o in valid_options if o.get("execution_eligible") is True]
        if eligible:
            eligible[0]["recommended"] = True
        else:
            valid_options[0]["recommended"] = True

    return valid_options


async def resolve_unmapped_issue_async(
    issue_type: str,
    column: Optional[str],
    col_semantic_type: Optional[str],
    message: Optional[str],
    budget: LLMResolutionBudget,
    semaphore: asyncio.Semaphore,
) -> List[Dict[str, Any]]:
    """
    Asynchronously query LLM for resolution options for an unmapped issue type,
    respecting budget, semaphore concurrency, and timeout.
    """
    sig = make_resolution_signature(issue_type, col_semantic_type, message)

    # 1. Check cache
    cached = get_cached_resolution(sig)
    if cached:
        return cached

    # 2. Check fallback enabled and budget
    if not DHARA_LLM_RESOLUTION_FALLBACK:
        return _make_budget_exhausted_fallback(issue_type, reason="llm_fallback_disabled")

    if not budget.can_call(sig):
        logger.warning("LLM resolution fallback budget (%d) exhausted for signature %s", budget.max_calls, sig)
        return _make_budget_exhausted_fallback(issue_type, reason="llm_budget_exhausted")

    # 3. Call LLM under concurrency limit
    cfg = load_llm_config(purpose="etl_codegen")
    if not cfg:
        return _make_budget_exhausted_fallback(issue_type, reason="llm_not_configured")

    budget.record_call(sig)

    prompt = (
        f"You are a Senior Data Quality and ETL Specialist.\n"
        f"An unmapped data defect was detected:\n"
        f"- Issue Type: {issue_type}\n"
        f"- Column: {column or '*'}\n"
        f"- Column Semantic Type: {col_semantic_type or 'unknown'}\n"
        f"- Message / Context: {message or 'N/A'}\n\n"
        f"Propose 2 or 3 standard remediation options. The 'action' field MUST be one of the registered actions:\n"
        f"{', '.join(sorted(list(_VALID_ACTION_NAMES)[:40]))}...\n"
        f"Format your response as a valid JSON object with key 'options':\n"
        f"{{\n"
        f"  \"options\": [\n"
        f"    {{\"id\": \"remediation_id\", \"label\": \"User-friendly label\", \"action\": \"registered_action_name\", \"recommended\": true, \"description\": \"Why this action fixes it\", \"confidence\": 0.85}}\n"
        f"  ]\n"
        f"}}"
    )

    async with semaphore:
        try:
            options = await asyncio.wait_for(
                _call_llm_for_options(cfg, prompt),
                timeout=DHARA_LLM_RESOLUTION_TIMEOUT_SECONDS,
            )
            validated = validate_resolution_options(options)
            put_cached_resolution(sig, validated)
            return validated
        except asyncio.TimeoutError:
            logger.warning("LLM resolution fallback timed out (%ss) for %s", DHARA_LLM_RESOLUTION_TIMEOUT_SECONDS, issue_type)
            return _make_budget_exhausted_fallback(issue_type, reason="llm_timeout")
        except Exception as e:
            logger.error("LLM resolution fallback failed for %s: %s", issue_type, e)
            return _make_budget_exhausted_fallback(issue_type, reason=f"llm_error: {e}")


async def _call_llm_for_options(cfg: Any, prompt: str) -> List[Dict[str, Any]]:
    """Helper to call OpenAI / AzureOpenAI chat completion."""
    def _invoke() -> List[Dict[str, Any]]:
        client = None
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI
            client = AzureOpenAI(
                azure_endpoint=cfg.endpoint,
                api_key=cfg.api_key,
                api_version=cfg.api_version or "2024-02-01",
            )
        elif cfg.provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=cfg.api_key)

        if not client:
            return []

        resp = client.chat.completions.create(
            model=cfg.model,
            messages=[
                {"role": "system", "content": "You are an expert ETL data quality engineer. Respond in JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
        return data.get("options") or []

    return await asyncio.to_thread(_invoke)


def _make_budget_exhausted_fallback(issue_type: str, reason: str = "llm_budget_exhausted") -> List[Dict[str, Any]]:
    return [
        {
            "id": "keep_as_is",
            "label": "Keep as-is (review manually)",
            "action": "noop",
            "recommended": True,
            "description": f"Unmapped issue '{issue_type}' ({reason}) — review with data steward.",
            "confidence": 0.5,
            "requires_human_approval": True,
            "resolution_source": reason,
        }
    ]
