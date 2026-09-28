from __future__ import annotations

import logging
from typing import Any, Callable, Dict
from agent.ir.errors import IrErrorCode, IrError
from agent.ir.models import IrPlan

logger = logging.getLogger("agent.ir.migrations")

CURRENT_IR_VERSION = "1.0"

_MIGRATIONS: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}


def register_migration(from_version: str, to_version: str):
    def decorator(fn: Callable[[Dict[str, Any]], Dict[str, Any]]):
        _MIGRATIONS[f"{from_version}->{to_version}"] = fn
        return fn
    return decorator


class IrMigrator:
    """Manages version evolution and seamless upgrades of IrPlan payloads."""

    @staticmethod
    def upgrade(plan_dict: Dict[str, Any], target_version: str = CURRENT_IR_VERSION) -> Dict[str, Any]:
        # If older than 1.0 or unversioned, apply defaults to bring to 1.0
        if "ir_version" not in plan_dict:
            plan_dict["ir_version"] = "1.0"
            if "intent" in plan_dict and isinstance(plan_dict["intent"], str):
                plan_dict["intent"] = {"type": plan_dict["intent"], "complexity": "simple", "confidence": 1.0}
            if "lineage" not in plan_dict:
                plan_dict["lineage"] = {"source_tables": [], "target_tables": []}
            if "validation" not in plan_dict:
                plan_dict["validation"] = {"strict_schema": True, "allow_missing_columns": False, "max_repair_attempts": 3}
            if "physical_hints" not in plan_dict:
                plan_dict["physical_hints"] = {}

        curr = plan_dict.get("ir_version", "1.0")
        if curr == target_version:
            return plan_dict

        # Sequentially apply registered version migrations if defined
        curr = plan_dict.get("ir_version", "1.0")
        while curr != target_version:
            # Look for step migration
            step_found = False
            for key, migration_fn in _MIGRATIONS.items():
                src, dest = key.split("->")
                if src == curr:
                    logger.info(f"Applying IR migration from {src} to {dest}")
                    plan_dict = migration_fn(plan_dict)
                    plan_dict["ir_version"] = dest
                    curr = dest
                    step_found = True
                    break
            if not step_found:
                raise ValueError(f"No migration path available from IR version {curr} to {target_version}")

        return plan_dict
