from __future__ import annotations

from typing import Dict, List, Optional
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.schema.column_registry import ColumnKind, ColumnLifecycleRegistry
from agent.compiler.schema.output_contract import OutputSchemaContract
from agent.compiler.models.cleaning import CleaningMode


class SchemaResolver:
    """
    Constructs and registers column lifecycles from DatasetProfiles and LogicalSteps.
    Guarantees deterministic, clean projections before physical code generation.
    """

    @classmethod
    def resolve_dataset_contract(
        cls,
        dataset_name: str,
        profile: Optional[DatasetProfile],
        context: CompilerContext,
    ) -> OutputSchemaContract:
        registry = ColumnLifecycleRegistry(
            dataset_id=dataset_name,
            mode=context.cleaning_mode,
        )

        # 1. Register verified source columns
        if profile and profile.columns:
            for col_name, col_meta in profile.columns.items():
                col_type = getattr(col_meta, "dtype", getattr(col_meta, "inferred_type", "string"))
                registry.register_source(col_name=col_name, dtype=col_type)

        # 2. Register generated steps from logical plan
        if context.logical_plan:
            for step in context.logical_plan.steps:
                if step.dataset_name != dataset_name:
                    continue

                target_cols = getattr(step, "target_columns", getattr(step, "columns", []))
                for col in target_cols:
                    # If this step adds an audit flag
                    action_val = step.action.value if hasattr(step.action, "value") else str(step.action)
                    if action_val in ("flag_business_key_duplicate", "flag_outliers", "preserve_null_and_flag"):
                        registry.register_generated(
                            col_name=f"flag_{col}",
                            kind=ColumnKind.AUDIT,
                            producer_step_id=step.step_id,
                            dtype="boolean",
                        )
                    elif action_val == "deduplicate":
                        registry.register_generated(
                            col_name="__row_number",
                            kind=ColumnKind.INTERNAL,
                            producer_step_id=step.step_id,
                            dtype="integer",
                        )
                        registry.register_generated(
                            col_name="__duplicate_rank",
                            kind=ColumnKind.INTERNAL,
                            producer_step_id=step.step_id,
                            dtype="integer",
                        )

        # 3. Add system operational metadata if explicitly requested
        if context.include_operational_metadata and context.cleaning_mode != CleaningMode.CLEANSE_ONLY:
            registry.register_generated(
                col_name="etl_batch_id",
                kind=ColumnKind.SYSTEM,
                producer_step_id="system_runtime",
                dtype="string",
            )
            registry.register_generated(
                col_name="etl_created_at",
                kind=ColumnKind.SYSTEM,
                producer_step_id="system_runtime",
                dtype="timestamp",
            )

        return registry.build_contract(preserve_source_schema=context.preserve_source_schema)

    @classmethod
    def resolve_all_contracts(cls, context: CompilerContext) -> Dict[str, OutputSchemaContract]:
        contracts: Dict[str, OutputSchemaContract] = {}
        for ds_name, profile in context.dataset_profiles.items():
            contracts[ds_name] = cls.resolve_dataset_contract(ds_name, profile, context)
        return contracts
