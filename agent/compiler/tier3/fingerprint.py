from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal


def compute_proposal_fingerprint(
    proposal: Tier3ResolutionProposal,
    target_engine: str = "AZURE_SQL",
    compiler_version: str = "1.0",
    schema_fingerprint: Optional[str] = None,
    rule_fingerprint: Optional[str] = None,
) -> str:
    """
    Computes a deterministic, invariant SHA-256 fingerprint from validated semantic inputs.
    Excludes non-deterministic or ephemeral values (timestamps, proposal IDs, free-form text, confidence).
    """
    semantic_payload: Dict[str, Any] = {
        "compiler_version": compiler_version,
        "contract_version": proposal.contract_version,
        "target_engine": target_engine.upper().strip(),
        "dataset_name": proposal.dataset_name.strip(),
        "issue_id": proposal.issue_id.strip(),
        "proposed_action_type": proposal.proposed_action_type.upper().strip(),
        "target_columns": sorted([c.strip() for c in proposal.target_columns]),
        "disposition": proposal.disposition.value,
        "risk_level": proposal.risk_level.value,
        "evidence_authorities": sorted([a.value for a in proposal.evidence_authorities]),
        "action_parameters": proposal.action_parameters,
        "condition_ast": proposal.condition_ast,
        "schema_fingerprint": schema_fingerprint or "",
        "rule_fingerprint": rule_fingerprint or "",
    }

    canonical_json = json.dumps(
        semantic_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
