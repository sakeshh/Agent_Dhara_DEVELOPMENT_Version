"""
Feature Flags for Agent Dhara Engine 3.x and GX Integration.
Controls progressive rollout of Engine 3.0 components and Great Expectations provider.
All flags default to safe (legacy-compatible) values and can be overridden via environment variables.
"""
from __future__ import annotations

import os
import logging
from typing import Any, Dict

logger = logging.getLogger("agent.feature_flags")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class FeatureFlags:
    """
    Central feature-flag registry.  Read once per process start; all flags are read-only after
    initialization to prevent mid-run mutation.
    """

    def __init__(self) -> None:
        # ── Engine 3.0 core pipeline ────────────────────────────────────────────
        self.dq_engine_v3: bool = _env_bool("DHARA_DQ_ENGINE_V3", True)
        self.semantic_inference_v2: bool = _env_bool("DHARA_SEMANTIC_INFERENCE_V2", True)
        self.key_engine_v2: bool = _env_bool("DHARA_KEY_ENGINE_V2", True)
        self.relationship_engine_v2: bool = _env_bool("DHARA_RELATIONSHIP_ENGINE_V2", True)
        self.finding_deduplicator_v2: bool = _env_bool("DHARA_FINDING_DEDUPLICATOR_V2", True)
        self.dimension_scoring_v2: bool = _env_bool("DHARA_DIMENSION_SCORING_V2", True)
        self.privacy_score_separation: bool = _env_bool("DHARA_PRIVACY_SEPARATION", True)

        # ── Great Expectations provider ──────────────────────────────────────────
        self.gx_validation_provider: bool = _env_bool("DHARA_GX_PROVIDER", True)
        self.gx_semantic_aware_planning: bool = _env_bool("DHARA_GX_SEMANTIC_PLANNING", True)
        self.gx_result_translator: bool = _env_bool("DHARA_GX_RESULT_TRANSLATOR", True)

        # ── Legacy components (explicit disable = use Engine 3) ─────────────────
        self.legacy_rule_engine: bool = _env_bool("DHARA_LEGACY_RULE_ENGINE", False)
        self.legacy_flat_penalty_scorer: bool = _env_bool("DHARA_LEGACY_FLAT_PENALTY", False)
        self.legacy_pii_quality_findings: bool = _env_bool("DHARA_LEGACY_PII_IN_DQ", False)
        self.legacy_semantic_detection: bool = _env_bool("DHARA_LEGACY_SEMANTIC", False)

        # ── Reporting ────────────────────────────────────────────────────────────
        self.component_manifest_in_report: bool = _env_bool("DHARA_COMPONENT_MANIFEST", True)
        self.reproducibility_hashes: bool = _env_bool("DHARA_REPRO_HASHES", True)

        # ── Business Rules Pipeline ──────────────────────────────────────────────
        self.notes_assessment_mode: str = os.environ.get("DHARA_NOTES_ASSESSMENT_MODE", "pre_assessment").strip().lower()

        # ── Compiler & Universal Cleaning Runtime Pipeline ─────────────────────
        self.compiler_codegen_enabled: bool = _env_bool("COMPILER_CODEGEN_ENABLED", True)
        self.legacy_llm_codegen_enabled: bool = _env_bool("LEGACY_LLM_CODEGEN_ENABLED", False)
        self.allow_llm_direct_execution: bool = _env_bool("ALLOW_LLM_DIRECT_EXECUTION", False)
        self.cleaning_default_mode: str = os.environ.get("CLEANING_DEFAULT_MODE", "cleanse_with_audit").strip().lower()
        self.llm_ir_planner_enabled: bool = _env_bool("DHARA_LLM_IR_PLANNER", True)

        self._log_startup()

    def _log_startup(self) -> None:
        enabled = [k for k, v in self.as_dict().items() if v is True]
        disabled = [k for k, v in self.as_dict().items() if v is False]
        logger.info("[DQ3] FeatureFlags initialized — enabled=%s disabled=%s", enabled, disabled)

    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def to_manifest_entry(self) -> Dict[str, Any]:
        return {
            "features": self.as_dict(),
            "legacy_components_enabled": [k for k, v in self.as_dict().items()
                                          if k.startswith("legacy_") and v is True],
        }


# ── Singleton ────────────────────────────────────────────────────────────────────
_FLAGS: FeatureFlags | None = None


def get_flags() -> FeatureFlags:
    """Returns the singleton FeatureFlags instance (created once per process)."""
    global _FLAGS
    if _FLAGS is None:
        _FLAGS = FeatureFlags()
    return _FLAGS


def reset_flags() -> None:
    """For testing only: reset the singleton so flags are re-read from env."""
    global _FLAGS
    _FLAGS = None
