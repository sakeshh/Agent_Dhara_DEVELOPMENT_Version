"""
Runtime Component Manifest for Agent Dhara DQ Engine.
Emitted in every assessment result and API response so the UI, operator logs,
and regression tests can verify exactly which pipeline ran.
"""
from __future__ import annotations

import logging
import importlib
from typing import Any, Dict, List, Optional

logger = logging.getLogger("agent.component_manifest")

# ── Canonical engine versions ────────────────────────────────────────────────────
_ENGINE_FAMILY = "AGENT_DHARA_DQ"
_ENGINE_VERSION = "3.1.0"

_COMPONENT_VERSIONS: Dict[str, str] = {
    "orchestrator":                "3.1.0",
    "semantic_inference":          "3.0.0",
    "key_engine":                  "3.0.0",
    "relationship_engine":         "3.0.0",
    "privacy_engine":              "3.0.0",
    "finding_deduplicator":        "3.0.0",
    "dimension_scorer":            "3.0.0",
    "remediation_engine":          "3.0.0",
    "great_expectations_provider": "3.1.0",
    "dhara_native_provider":       "3.1.0",
    "gx_result_translator":        "3.1.0",
    "gx_suite_builder":            "3.1.0",
    "gx_capability_registry":      "3.1.0",
}


def _gx_version() -> str:
    """Safely resolve the installed Great Expectations version string."""
    try:
        import great_expectations as gx  # type: ignore
        return getattr(gx, "__version__", "unknown")
    except ImportError:
        return "not-installed"


def build_component_manifest(
    *,
    active_components: Optional[List[str]] = None,
    legacy_components: Optional[List[str]] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Returns the standard component manifest dict that must appear in every assessment output.
    """
    from agent.feature_flags import get_flags
    flags = get_flags()

    gx_ver = _gx_version()
    gx_enabled = flags.gx_validation_provider and gx_ver != "not-installed"

    components: Dict[str, str] = {}
    # Always include the versions for active Engine 3.0 components
    for name, ver in _COMPONENT_VERSIONS.items():
        components[name] = ver

    # Resolve which legacy components remain active based on flags
    legacy_active: List[str] = list(legacy_components or [])
    if flags.legacy_flat_penalty_scorer:
        legacy_active.append("legacy_flat_penalty_scorer")
    if flags.legacy_pii_quality_findings:
        legacy_active.append("legacy_pii_quality_findings")
    if flags.legacy_semantic_detection:
        legacy_active.append("legacy_semantic_detection")
    if flags.legacy_rule_engine:
        legacy_active.append("legacy_rule_engine")

    manifest: Dict[str, Any] = {
        "engine_family":   _ENGINE_FAMILY,
        "engine_version":  _ENGINE_VERSION,
        "components":      components,
        "validation_providers": {
            "great_expectations": {
                "enabled":       gx_enabled,
                "version":       gx_ver,
                "context_mode":  "ephemeral",
                "semantic_aware_planning": flags.gx_semantic_aware_planning,
                "result_translator_active": flags.gx_result_translator,
            },
            "dhara_native": {
                "enabled": True,
                "version": _COMPONENT_VERSIONS["dhara_native_provider"],
            },
        },
        "feature_flags":          flags.as_dict(),
        "legacy_components_enabled": legacy_active,
        "privacy_separation":     flags.privacy_score_separation,
        "dimension_scorer_v3":    flags.dimension_scoring_v2,
        "semantic_inference_v3":  flags.semantic_inference_v2,
        "key_engine_v3":          flags.key_engine_v2,
        "relationship_engine_v3": flags.relationship_engine_v2,
    }

    if extra_metadata:
        manifest.update(extra_metadata)

    return manifest


def log_component_startup(component_name: str, version: str = "3.0.0") -> None:
    """
    Emit the standardized startup log line for a component.
    Called at initialization time from each Engine 3.0 module.
    """
    logger.info("[DQ3] %s initialized (v%s)", component_name, version)
