from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
import yaml
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext


class ContractAgent(BaseAgent):
    """Generates and validates declarative Data Contracts for pre-execution drift and schema enforcement."""

    def generate_contract_yaml(self, context: CompilerContext) -> str:
        """Generates OpenDataContract-compliant YAML schema contract."""
        datasets_contract: Dict[str, Any] = {}

        for ds_name, ds_profile in context.dataset_profiles.items():
            col_specs: Dict[str, Any] = {}
            for col_name, col_profile in ds_profile.columns.items():
                spec: Dict[str, Any] = {
                    "type": col_profile.dtype,
                    "nullable": col_profile.null_percentage > 0,
                }
                if col_profile.semantic_type and col_profile.semantic_type != "unknown":
                    spec["semantic_type"] = col_profile.semantic_type
                if col_profile.is_primary_key_candidate:
                    spec["unique"] = True
                    spec["primary_key"] = True
                col_specs[col_name] = spec

            datasets_contract[ds_name] = {
                "dataset_name": ds_name,
                "expected_columns": len(ds_profile.columns),
                "columns": col_specs,
            }

        contract_payload = {
            "version": "1.0.0",
            "contract_id": f"contract_{context.session_id}",
            "invariants": {
                "never_drop_rows": context.options.get("never_drop_rows", False),
            },
            "datasets": datasets_contract,
        }

        return yaml.dump(contract_payload, sort_keys=False, default_flow_style=False)

    def execute(self, context: CompilerContext) -> CompilerContext:
        contract_yaml = self.generate_contract_yaml(context)
        context.metadata["data_contract_yaml"] = contract_yaml

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CONTRACT_GENERATED,
            session_id=context.session_id,
            payload={"contract_id": f"contract_{context.session_id}"},
            source_agent=self.__class__.__name__,
        )

        return context
