"""Structured Configuration Patch Engine for Agent Dhara.

Governs configuration actions (e.g. metadata_manifest_mismatch -> update_manifest)
through a formal patch lifecycle with strict safety controls:
- Allowlisted configuration paths only
- No arbitrary YAML tags or code execution
- In-memory schema validation
- Idempotency and conflict detection
- Round-trip YAML revalidation
- Rollback patch generation
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger("agent.configuration_patch")

ALLOWLISTED_ACTIONS = {
    "remove_or_map_missing",
    "add_field_mapping",
    "update_field_type",
    "reconcile_schema",
}

ALLOWLISTED_TARGET_CONFIGS = {
    "config/metadata_manifest.yaml",
    "config/dataset_inventory.yaml",
    "config/schema_definitions.yaml",
}


@dataclass(frozen=True)
class StructuredConfigPatch:
    """Structured, cryptographic patch proposal for metadata configuration."""
    patch_id: str
    target_config_id: str
    target_config_version: str
    dataset_id: str
    field_name: str
    action_type: str
    before_hash: str
    patch_payload: Dict[str, Any]
    patch_hash: str
    lifecycle_status: str = "proposed"  # proposed | structurally_validated | reviewed | approved | applied | verified | rejected | superseded | failed | rolled_back
    requested_by: str = "system_profiler"
    approved_by: Optional[str] = None
    created_at: str = "2026-09-07T13:00:00Z"
    applied_at: Optional[str] = None
    rollback_patch_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "target_config_id": self.target_config_id,
            "target_config_version": self.target_config_version,
            "dataset_id": self.dataset_id,
            "field_name": self.field_name,
            "action_type": self.action_type,
            "before_hash": self.before_hash,
            "patch_payload": self.patch_payload,
            "patch_hash": self.patch_hash,
            "lifecycle_status": self.lifecycle_status,
            "requested_by": self.requested_by,
            "approved_by": self.approved_by,
            "created_at": self.created_at,
            "applied_at": self.applied_at,
            "rollback_patch_id": self.rollback_patch_id,
        }

    def render_yaml_preview(self) -> str:
        """Renders clean, safe YAML string of the patch proposal."""
        return yaml.safe_dump(self.patch_payload, sort_keys=False)


def compute_content_hash(text: str) -> str:
    """Computes standard sha256 checksum over utf-8 encoded text."""
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def create_config_patch_proposal(
    dataset_id: str,
    field_name: str,
    action_type: str = "remove_or_map_missing",
    target_config_id: str = "config/metadata_manifest.yaml",
    target_config_version: str = "v2.0.0",
    base_config_content: str = "",
    justification: str = "Field not found in physical database schema",
    requested_by: str = "system_profiler",
) -> StructuredConfigPatch:
    """Factory creating an immutable StructuredConfigPatch proposal."""
    if action_type not in ALLOWLISTED_ACTIONS:
        raise ValueError(f"Action '{action_type}' is not allowlisted for configuration patches")
    if target_config_id not in ALLOWLISTED_TARGET_CONFIGS:
        raise ValueError(f"Target config '{target_config_id}' is not in allowlisted configuration targets")

    before_hash = compute_content_hash(base_config_content) if base_config_content else "sha256:empty"
    patch_id = f"patch-{str(uuid.uuid4())[:8]}"

    patch_payload: Dict[str, Any] = {
        "metadata_manifest": {
            "datasets": {
                dataset_id: {
                    "reconcile_fields": [
                        {
                            "action": action_type,
                            "field_name": field_name,
                            "justification": justification,
                        }
                    ]
                }
            }
        }
    }

    # Deterministic canonical patch hash
    canon_bytes = json.dumps(patch_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    patch_hash = "sha256:" + hashlib.sha256(canon_bytes).hexdigest()

    return StructuredConfigPatch(
        patch_id=patch_id,
        target_config_id=target_config_id,
        target_config_version=target_config_version,
        dataset_id=dataset_id,
        field_name=field_name,
        action_type=action_type,
        before_hash=before_hash,
        patch_payload=patch_payload,
        patch_hash=patch_hash,
        lifecycle_status="proposed",
        requested_by=requested_by,
    )


def validate_patch_against_schema(
    patch: StructuredConfigPatch,
    current_config_content: str,
    physical_columns: Optional[List[str]] = None,
) -> Tuple[bool, List[str]]:
    """Strictly validates a configuration patch before approval or execution."""
    errors: List[str] = []

    # 1. Target config allowlist check
    if patch.target_config_id not in ALLOWLISTED_TARGET_CONFIGS:
        errors.append(f"Target config '{patch.target_config_id}' not allowed.")

    # 2. Action type allowlist check
    if patch.action_type not in ALLOWLISTED_ACTIONS:
        errors.append(f"Action '{patch.action_type}' not allowed.")

    # 3. Path traversal & security checks
    if ".." in patch.target_config_id or patch.target_config_id.startswith("/") or "\\" in patch.target_config_id:
        errors.append("Invalid or path-traversal detected in target_config_id.")

    # 4. Before hash matching
    if current_config_content:
        cur_hash = compute_content_hash(current_config_content)
        if patch.before_hash and patch.before_hash != "sha256:empty" and cur_hash != patch.before_hash:
            errors.append(f"Stale configuration base: expected {patch.before_hash}, found {cur_hash}.")

    # 5. Verify physical field absence
    if physical_columns is not None:
        if patch.field_name in physical_columns:
            errors.append(
                f"Field '{patch.field_name}' is physically present in {patch.dataset_id}; configuration mismatch does not exist."
            )

    # 6. YAML parsing & safety (no custom tags)
    raw_yaml = patch.render_yaml_preview()
    if "!!" in raw_yaml:
        errors.append("Arbitrary YAML custom tags are forbidden.")

    try:
        parsed = yaml.safe_load(raw_yaml)
        if not isinstance(parsed, dict):
            errors.append("Parsed YAML patch payload must be a dictionary.")
    except Exception as e:
        errors.append(f"YAML parsing error: {e}")

    return len(errors) == 0, errors


def apply_config_patch(
    patch: StructuredConfigPatch,
    base_config_content: str,
) -> Tuple[str, str]:
    """Applies the structured patch to base configuration text and returns (new_yaml, new_hash)."""
    base_dict: Dict[str, Any] = yaml.safe_load(base_config_content) or {}
    updated_dict = copy.deepcopy(base_dict)

    datasets = updated_dict.setdefault("metadata_manifest", {}).setdefault("datasets", {})
    ds_entry = datasets.setdefault(patch.dataset_id, {})
    reconcile_list = ds_entry.setdefault("reconcile_fields", [])

    # Idempotent insertion
    existing = False
    for r in reconcile_list:
        if r.get("field_name") == patch.field_name and r.get("action") == patch.action_type:
            existing = True
            break
    if not existing:
        reconcile_list.append({
            "action": patch.action_type,
            "field_name": patch.field_name,
            "justification": patch.patch_payload.get("metadata_manifest", {})
            .get("datasets", {})
            .get(patch.dataset_id, {})
            .get("reconcile_fields", [{}])[0]
            .get("justification", "Reconciled missing manifest field"),
        })

    new_yaml = yaml.safe_dump(updated_dict, sort_keys=False)
    new_hash = compute_content_hash(new_yaml)
    return new_yaml, new_hash


def generate_rollback_patch(
    patch: StructuredConfigPatch,
    applied_config_content: str,
) -> StructuredConfigPatch:
    """Generates an inverted rollback patch restoring previous state."""
    applied_hash = compute_content_hash(applied_config_content)
    rb_payload = {
        "metadata_manifest": {
            "datasets": {
                patch.dataset_id: {
                    "reconcile_fields": [
                        {
                            "action": "revert_reconciliation",
                            "field_name": patch.field_name,
                            "original_patch_id": patch.patch_id,
                        }
                    ]
                }
            }
        }
    }
    canon_bytes = json.dumps(rb_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    rb_hash = "sha256:" + hashlib.sha256(canon_bytes).hexdigest()

    return StructuredConfigPatch(
        patch_id=f"rollback-{patch.patch_id}",
        target_config_id=patch.target_config_id,
        target_config_version=f"{patch.target_config_version}-rollback",
        dataset_id=patch.dataset_id,
        field_name=patch.field_name,
        action_type="revert_reconciliation",
        before_hash=applied_hash,
        patch_payload=rb_payload,
        patch_hash=rb_hash,
        lifecycle_status="proposed",
        requested_by="rollback_subsystem",
        rollback_patch_id=patch.patch_id,
    )
