from __future__ import annotations

import hashlib
import json
from typing import Dict, Optional

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3.model_client import Tier3ResolutionRequest
from agent.compiler.tier3.validator import Tier3ValidationOrchestrator


class Tier3ReplayCache:
    """
    Deterministic replay cache for Tier 3 proposals.

    Guarantees:
      - Uses canonical SHA-256 request hashing.
      - Cached entries MUST be re-validated against the current active schema & policies
        before reuse, ensuring schema drift or policy changes invalidate stale cached proposals.
    """

    def __init__(self, orchestrator: Optional[Tier3ValidationOrchestrator] = None):
        self._cache: Dict[str, Tier3ResolutionProposal] = {}
        self.orchestrator = orchestrator or Tier3ValidationOrchestrator()
        self.hits: int = 0
        self.misses: int = 0

    def compute_request_key(self, request: Tier3ResolutionRequest) -> str:
        """Computes deterministic hash from invariant request inputs."""
        user_feedback = request.metadata.get("user_feedback") if request.metadata else None
        payload = {
            "contract_version": request.contract_version,
            "issue_type": request.issue_type.strip().upper(),
            "issue_description": request.issue_description.strip(),
            "dataset_name": request.dataset_name.strip(),
            "target_columns": sorted(request.target_columns),
            "dataset_schema": {k: str(v).lower() for k, v in sorted(request.dataset_schema.items())},
            "available_rules": sorted([json.dumps(r, sort_keys=True) for r in request.available_rules]),
            "target_engine": request.target_engine.upper().strip(),
            "user_feedback": str(user_feedback).strip() if user_feedback else None,
        }
        canonical_str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

    def get(
        self,
        request: Tier3ResolutionRequest,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Optional[Tier3ResolutionProposal]:
        """
        Retrieves a cached proposal and re-validates it against the active schema.
        Returns None on cache miss or if current validation fails.
        """
        key = self.compute_request_key(request)
        cached_proposal = self._cache.get(key)
        if not cached_proposal:
            self.misses += 1
            return None

        # Always re-validate cached proposals against the live active schema
        try:
            val_res = self.orchestrator.validate_proposal(cached_proposal, dataset_profiles)
            if not val_res.is_valid:
                # Active schema or safety policy changed, invalidate stale cached item
                del self._cache[key]
                self.misses += 1
                return None
        except Exception:
            # Temporary validation infrastructure failure (e.g. transient schema lookup error)
            # Retain cached proposal for future validation, treat current attempt as miss
            self.misses += 1
            return None

        self.hits += 1
        return cached_proposal

    def put(self, request: Tier3ResolutionRequest, proposal: Tier3ResolutionProposal) -> None:
        """Stores a validated candidate proposal in the replay cache."""
        key = self.compute_request_key(request)
        self._cache[key] = proposal

    def clear(self) -> None:
        self._cache.clear()
        self.hits = 0
        self.misses = 0
