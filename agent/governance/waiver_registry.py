from __future__ import annotations
import os
import yaml
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

class WaiverRegistry:
    def __init__(self, waivers: Optional[List[Dict[str, Any]]] = None):
        self.waivers = waivers or []

    @classmethod
    def load_from_rules_yaml(cls, path: str = "config/business_rules.yaml") -> WaiverRegistry:
        """Load waivers from business_rules.yaml if it exists."""
        if not os.path.exists(path):
            return cls()
        try:
            with open(path, "r") as f:
                rules = yaml.safe_load(f) or {}
                return cls(rules.get("waivers") or [])
        except Exception:
            return cls()

    def get_active_waivers(self, current_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get list of currently valid, non-expired waivers."""
        now = current_time or datetime.now(timezone.utc)
        active = []
        for w in self.waivers:
            start_str = w.get("effective_from")
            expiry_str = w.get("expires_at")
            
            # Parsing ISO timestamps
            try:
                start = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else None
                expiry = datetime.fromisoformat(expiry_str.replace("Z", "+00:00")) if expiry_str else None
            except Exception:
                # Fallback to general parsing
                start = None
                expiry = None
                
            if start and now < start:
                continue
            if expiry and now > expiry:
                continue
            active.append(w)
        return active
