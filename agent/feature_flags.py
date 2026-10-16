"""Feature Flags - Centralized feature flag management."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable
from threading import Lock
import os


@dataclass
class FeatureFlag:
    """A single feature flag."""
    name: str
    enabled: bool
    description: str = ""
    rollout_percentage: int = 100  # 0-100
    conditions: Dict[str, Any] = field(default_factory=dict)  # e.g., {"environment": ["staging", "production"]}
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureFlags:
    """Thread-safe feature flag manager."""
    
    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = Lock()
        self._evaluators: Dict[str, Callable[[Dict[str, Any]], bool]] = {}
        self._context: Dict[str, Any] = {}
    
    def register(
        self, 
        name: str, 
        enabled: bool = False, 
        description: str = "",
        rollout_percentage: int = 100,
        conditions: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> FeatureFlag:
        """Register a new feature flag."""
        with self._lock:
            flag = FeatureFlag(
                name=name,
                enabled=enabled,
                description=description,
                rollout_percentage=rollout_percentage,
                conditions=conditions or {},
                metadata=metadata or {}
            )
            self._flags[name] = flag
            return flag
    
    def register_many(self, flags: Dict[str, Dict[str, Any]]) -> None:
        """Register multiple flags at once."""
        for name, config in flags.items():
            self.register(
                name=name,
                enabled=config.get("enabled", False),
                description=config.get("description", ""),
                rollout_percentage=config.get("rollout_percentage", 100),
                conditions=config.get("conditions", {}),
                metadata=config.get("metadata", {})
            )
    
    def is_enabled(self, name: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Check if a feature is enabled for the given context."""
        with self._lock:
            flag = self._flags.get(name)
            if not flag:
                return False
            
            if not flag.enabled:
                return False
            
            # Check conditions
            if flag.conditions:
                eval_context = {**self._context, **(context or {})}
                for condition_key, condition_value in flag.conditions.items():
                    context_value = eval_context.get(condition_key)
                    if isinstance(condition_value, list):
                        if context_value not in condition_value:
                            return False
                    elif context_value != condition_value:
                        return False
            
            # Check rollout percentage
            if flag.rollout_percentage < 100:
                # Use consistent hashing for stable rollout
                import hashlib
                hash_input = f"{name}:{context.get('user_id', 'anonymous')}" if context else name
                hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
                if (hash_value % 100) >= flag.rollout_percentage:
                    return False
            
            return True
    
    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get flag definition."""
        with self._lock:
            return self._flags.get(name)
    
    def list_flags(self) -> Dict[str, FeatureFlag]:
        """List all flags."""
        with self._lock:
            return dict(self._flags)
    
    def set_context(self, context: Dict[str, Any]) -> None:
        """Set global context for condition evaluation."""
        with self._lock:
            self._context.update(context)
    
    def clear_context(self) -> None:
        """Clear global context."""
        with self._lock:
            self._context.clear()
    
    def override(self, name: str, enabled: bool) -> None:
        """Force enable/disable a flag (for testing)."""
        with self._lock:
            if name in self._flags:
                self._flags[name].enabled = enabled
    
    def register_evaluator(self, name: str, evaluator: Callable[[Dict[str, Any]], bool]) -> None:
        """Register a custom evaluator for complex conditions."""
        with self._lock:
            self._evaluators[name] = evaluator


# Default flags - matching config.py
DEFAULT_FLAGS = {
    # Core features
    "assessment_swarm": {
        "enabled": True,
        "description": "Enable parallel assessment swarm (Profiler, Semantic, KeyFinder, DQ, Relationship, Drift)",
    },
    "business_logic_discovery": {
        "enabled": True,
        "description": "Enable automatic business logic discovery (FDs, conditional constraints, aggregations)",
    },
    "incremental_cdc": {
        "enabled": True,
        "description": "Enable CDC-based incremental processing",
        "conditions": {"environment": ["staging", "production"]},
    },
    "delta_cdf": {
        "enabled": True,
        "description": "Enable Delta Lake Change Data Feed support",
        "conditions": {"environment": ["staging", "production"]},
    },
    "multi_target_deployment": {
        "enabled": True,
        "description": "Enable multi-target deployment (SQL, Fabric, ADF, Snowflake, Databricks)",
    },
    "human_gates": {
        "enabled": True,
        "description": "Enable human-in-the-loop gates (Plan Review, Code Review, Execution Approval)",
    },
    "scheduler": {
        "enabled": True,
        "description": "Enable pipeline scheduler with cron and event triggers",
    },
    "data_contracts": {
        "enabled": True,
        "description": "Enable data contract validation and governance",
    },
    "governance": {
        "enabled": True,
        "description": "Enable governance features (lineage, PII, audit)",
    },
    "learning": {
        "enabled": True,
        "description": "Enable continuous learning from execution outcomes",
    },
    
    # Advanced features (off by default)
    "reverse_etl": {
        "enabled": False,
        "description": "Enable reverse ETL (warehouse to SaaS)",
        "conditions": {"environment": ["production"]},
    },
    "streaming": {
        "enabled": False,
        "description": "Enable real-time streaming pipelines",
        "conditions": {"environment": ["production"]},
    },
    "reverse_etl": {
        "enabled": False,
        "description": "Enable reverse ETL (warehouse to SaaS)",
    },
    "ml_feature_store": {
        "enabled": False,
        "description": "Enable ML feature store integration",
    },
    "time_travel": {
        "enabled": False,
        "description": "Enable time travel queries",
    },
    
    # Experimental
    "experimental_llm_semantic": {
        "enabled": True,
        "description": "Use LLM for semantic refinement",
        "rollout_percentage": 50,
    },
    "experimental_auto_fix": {
        "enabled": True,
        "description": "Enable auto-fix for validation errors",
    },
    "experimental_parallel_assessment": {
        "enabled": True,
        "description": "Enable parallel assessment execution",
    },
}


# Global instance
_flags_instance: Optional["FeatureFlags"] = None
_init_lock = Lock()


def get_flags(config: Optional[Dict[str, Any]] = None) -> FeatureFlags:
    """Get global feature flags instance."""
    global _flags_instance
    if _flags_instance is None:
        with _init_lock:
            if _flags_instance is None:
                _flags_instance = FeatureFlags()
                # Load from config if provided
                if config and "features" in config:
                    for name, value in config["features"].items():
                        if isinstance(value, bool):
                            _flags_instance.register(name, enabled=value)
                        elif isinstance(value, dict):
                            _flags_instance.register(
                                name=name,
                                enabled=value.get("enabled", False),
                                description=value.get("description", ""),
                                rollout_percentage=value.get("rollout_percentage", 100),
                                conditions=value.get("conditions", {}),
                            )
                else:
                    # Register defaults
                    for name, config in DEFAULT_FLAGS.items():
                        _flags_instance.register(
                            name=name,
                            enabled=config.get("enabled", False),
                            description=config.get("description", ""),
                            rollout_percentage=config.get("rollout_percentage", 100),
                            conditions=config.get("conditions", {}),
                        )
    return _flags_instance


def reset_flags() -> None:
    """Reset global flags (for testing)."""
    global _flags_instance
    with _init_lock:
        _flags_instance = None


# Convenience function
def is_enabled(flag_name: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Quick check if a feature is enabled."""
    return get_flags().is_enabled(flag_name, context)


# Decorator for feature-gated functions
def feature_gate(flag_name: str, fallback: Any = None):
    """Decorator to gate a function behind a feature flag."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            if is_enabled(flag_name):
                return func(*args, **kwargs)
            return fallback
        return wrapper
    return decorator