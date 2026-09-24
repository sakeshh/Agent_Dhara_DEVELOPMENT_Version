from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan
from agent.compiler.templates.engine import TemplateEngine


class PythonCodegenAgent(BaseCodegenAgent):
    """Generates hardened, modular Python / Pandas ETL pipelines using Jinja2 templates."""

    def __init__(self, template_engine: TemplateEngine | None = None):
        self.template_engine = template_engine or TemplateEngine.get_instance()

    def generate(self, physical_plan: PhysicalPlan, context: CompilerContext) -> CodeArtifact:
        dataset_transforms: Dict[str, str] = {}

        # 1. Group and render steps per dataset
        for ds_name in physical_plan.dataset_names:
            steps = physical_plan.get_steps_for_dataset(ds_name)
            rendered_steps: List[str] = []

            for step in steps:
                template_path = f"python/{step.template_name}"
                params = dict(step.template_parameters)
                params["step_id"] = step.step_id
                params["description"] = step.description or step.strategy_name
                params["columns"] = step.target_columns
                params["never_drop_rows"] = context.options.get("never_drop_rows", False)

                try:
                    step_code = self.template_engine.render(template_path, **params)
                    rendered_steps.append(step_code)
                except Exception:
                    # Fallback generic inline comment if template is not found
                    rendered_steps.append(f"# Step: {step.step_id} ({step.strategy_name}) - custom")

            dataset_transforms[ds_name] = "\n\n".join(rendered_steps) if rendered_steps else "# No transformations needed"

        # 2. Render master pipeline
        pipeline_code = self.template_engine.render(
            "python/pipeline.j2",
            pipeline_id=physical_plan.plan_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            dataset_names=physical_plan.dataset_names,
            dataset_transforms=dataset_transforms,
        )

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_python",
            name="etl_pipeline.py",
            language="python",
            content=pipeline_code,
            file_extension=".py",
            is_entrypoint=True,
            metadata={
                "plan_id": physical_plan.plan_id,
                "step_count": len(physical_plan.steps),
                "engine": "python",
            },
        )

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "language": artifact.language,
                "bytes": len(pipeline_code),
            },
            source_agent=self.__class__.__name__,
        )

        return artifact
