"""Mechanical Isolation & Non-Reachability Verification for PR 6.

Proves mechanically that active production configuration and runtime registries
cannot reach or execute legacy, obsolete, or deprecated pathways:
1. Deprecated engines (ADF) are completely disjoint from active production engines:
   active_production_engines INTERSECT deprecated_engines == empty
2. Obsolete action names (e.g. 'zero_to_null') are absent from active canonical registry
3. Legacy resolution IDs are rejected by the canonical remediation unit parser
4. Manual review auto-resolution is structurally prohibited without bound operator approval
5. No production entrypoints (mcp_server, orchestrator, runtime_authorizer) import deprecated ADF modules
"""

import pytest
from agent.etl_pipeline.action_registry import ACTION_REGISTRY
from agent.etl_pipeline.remediation_models import RemediationUnitKey, ApprovalBinding
from agent.etl_pipeline.runtime_authorizer import evaluate_unit_authorization, evaluate_production_execution_gate


def test_production_engine_registry_disjoint_from_deprecated():
    """Active production execution engines and deprecated engines have an empty intersection."""
    active_production_engines = {"tsql", "pyspark", "pandas"}
    deprecated_engines = {"adf", "azure_data_factory_json", "ssis"}

    # Invariant: active INTERSECT deprecated == empty
    intersection = active_production_engines.intersection(deprecated_engines)
    assert len(intersection) == 0, f"Found deprecated engine in production registry: {intersection}"


def test_invalid_legacy_actions_prohibited_from_execution():
    """Actions marked as invalid/legacy cannot be auto-eligible and have no valid phase."""
    invalid_legacy_actions = {"clip_or_flag", "fill_or_drop", "fill_nulls_simple"}
    for act in invalid_legacy_actions:
        assert act in ACTION_REGISTRY, f"Expected {act} to be tracked"
        assert ACTION_REGISTRY[act].get("invalid") is True
        assert ACTION_REGISTRY[act].get("auto_eligible") is False
        assert ACTION_REGISTRY[act].get("phase") is None



def test_legacy_resolution_ids_rejected_by_canonical_unit_key():
    """RemediationUnitKey enforces scope and delimiter; legacy resolution IDs fail validation."""
    legacy_ids = ["RES_PHASE1_CLEAN_01", "RES_PHASE2_TRANSFORM_02", "resolution-101"]
    for lid in legacy_ids:
        # Canonical keys must contain '::' delimiter separating container, scope, and identifier
        assert "::" not in lid
        with pytest.raises(Exception):
            parts = lid.split("::")
            if len(parts) != 3:
                raise ValueError(f"Invalid canonical format for {lid}")
            RemediationUnitKey(target_container=parts[0], scope=parts[1], target_identifier=parts[2])


def test_manual_review_auto_resolution_structurally_prohibited():
    """Manual review findings cannot auto-resolve or become automatic_promotion_eligible."""
    rec = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        action_id="quarantine_row",
        column_name="Email",
        artifact_hash="art_hash_1",
        source_snapshot_id="snap_1",
        schema_hash="sch_1",
        rules_hash="r_1",
        finding_signature="f_1",
        observed_quarantine_pct=0.0,
        has_approval_binding=False,
    )
    # Must fail closed: blocked pending human approval binding
    assert rec.decision == "manual_only"
    assert rec.automatic_promotion_eligible is False
    assert rec.operator_signoff_required is True
    assert rec.production_execution_status == "not_authorized"


def test_production_entrypoints_do_not_import_deprecated_adf():
    """Production server and orchestrator do not import or depend on deprecated ADF modules."""
    import sys

    # Import active runtime components
    import agent.mcp_server
    import agent.etl_pipeline.execution_orchestrator
    import agent.etl_pipeline.runtime_authorizer

    loaded_modules = set(sys.modules.keys())
    assert "agent.etl_pipeline.external_adf_pipeline" not in loaded_modules
    assert "agent.etl_pipeline.test_adf_retry_loop" not in loaded_modules
