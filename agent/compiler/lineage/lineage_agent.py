from __future__ import annotations

from typing import Any, Dict, List
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import LogicalPlan


class LineageAgent(BaseAgent):
    """Builds column-level DAG lineage graphs and visual Mermaid flowcharts from LogicalPlans."""

    def build_column_lineage(self, plan: LogicalPlan) -> List[Dict[str, Any]]:
        lineage_entries: List[Dict[str, Any]] = []

        for ds_name in plan.dataset_names:
            steps = plan.get_steps_for_dataset(ds_name)
            col_ops: Dict[str, List[str]] = {}

            for step in steps:
                for col in step.target_columns:
                    ops = col_ops.setdefault(col, [])
                    if step.fused_operations:
                        ops.extend([op.value for op in step.fused_operations])
                    else:
                        ops.append(step.action.value)

            for col, ops in col_ops.items():
                lineage_entries.append({
                    "dataset": ds_name,
                    "source_column": f"{ds_name}.{col}",
                    "transformations": ops,
                    "target_column": f"{ds_name}_clean.{col}",
                })

        return lineage_entries

    def build_mermaid_dag(self, lineage_entries: List[Dict[str, Any]]) -> str:
        """Generates a Mermaid graph visualization of the transformation lineage."""
        lines = ["flowchart LR"]
        for idx, entry in enumerate(lineage_entries):
            src_node = f"src_{idx}[\"{entry['source_column']}\"]"
            tgt_node = f"tgt_{idx}[\"{entry['target_column']}\"]"
            
            if entry["transformations"]:
                prev_node = src_node
                for t_idx, op in enumerate(entry["transformations"]):
                    op_node = f"op_{idx}_{t_idx}[\"{op}\"]"
                    lines.append(f"    {prev_node} --> {op_node}")
                    prev_node = op_node
                lines.append(f"    {prev_node} --> {tgt_node}")
            else:
                lines.append(f"    {src_node} --> {tgt_node}")

        return "\n".join(lines)

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.logical_plan:
            return context

        entries = self.build_column_lineage(context.logical_plan)
        mermaid_dag = self.build_mermaid_dag(entries)

        context.metadata["lineage_entries"] = entries
        context.metadata["lineage_mermaid"] = mermaid_dag

        EventBus.get_instance().emit(
            event_type=CompilerEventType.LINEAGE_GENERATED,
            session_id=context.session_id,
            payload={"entries_count": len(entries)},
            source_agent=self.__class__.__name__,
        )

        return context
