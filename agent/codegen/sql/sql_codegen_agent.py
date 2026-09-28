import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.compilation_result import (
    CodeGenerationError,
    CompilationTelemetry,
    StepCompilationResult,
)
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan
from agent.compiler.schema.sql_identifier import parse_table_identifier
from agent.compiler.templates.engine import TemplateEngine

logger = logging.getLogger("agent.codegen.sql")

STRATEGY_PARAMETER_CONTRACTS: Dict[str, Set[str]] = {
    "tsql_left_anti_join": {"parent_dataset", "parent_key", "child_key"},
    "tsql_prefix_add": {"prefix"},
    "tsql_suffix_add": {"suffix"},
    "tsql_pattern_replace": {"pattern"},
    "tsql_value_map": {"mapping"},
    "tsql_pad": {"length"},
    "tsql_conditional_update": {"sql_condition", "rendered_new_value"},
    "tsql_regex_email": {"source_table", "dataset_name", "pipeline_id"},
    "sanitize_email": {"source_table", "dataset_name", "pipeline_id"},
    "tsql_clean_digits": {"source_table", "dataset_name", "pipeline_id"},
    "normalize_phone": {"source_table", "dataset_name", "pipeline_id"},
    "tsql_mask_phone": {"source_table", "dataset_name", "pipeline_id"},
    "mask_phone": {"source_table", "dataset_name", "pipeline_id"},
    "tsql_case_boolean": {"source_table", "dataset_name", "pipeline_id"},
    "standardize_boolean": {"source_table", "dataset_name", "pipeline_id"},
}


def is_executable_sql(sql_code: Optional[str]) -> bool:
    """Checks if rendered SQL contains actual SQL statements beyond comments and blank lines."""
    if not sql_code:
        return False
    lines = [line.strip() for line in sql_code.splitlines()]
    non_comment = [l for l in lines if l and not l.startswith("--") and not l.startswith("/*")]
    return len(non_comment) > 0


class SQLCodegenAgent(BaseCodegenAgent):
    """Generates production-grade T-SQL stored procedures with fail-closed validation gates."""

    def __init__(self, template_engine: TemplateEngine | None = None):
        self.template_engine = template_engine or TemplateEngine.get_instance()

    def generate(self, physical_plan: PhysicalPlan, context: CompilerContext) -> CodeArtifact:
        dataset_transforms: Dict[str, str] = {}
        never_drop = context.options.get("never_drop_rows", False)
        compilation_results: List[StepCompilationResult] = []
        infrastructure_statement_count = 2  # Rejection schema + Rejection_Log DDL
        projection_statement_count = 0

        for ds_name in physical_plan.dataset_names:
            table_ident = parse_table_identifier(ds_name)
            steps = physical_plan.get_steps_for_dataset(ds_name)
            rendered_steps: List[str] = []

            # Add staging table preparation header (infrastructure)
            staging_table = table_ident.staging_name
            source_table = table_ident.qualified_name
            prep_ddl = (
                f"-- Ensure Staging table exists\n"
                f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE {staging_table};\n"
                f"SELECT * INTO {staging_table} FROM {source_table};"
            )
            rendered_steps.append(prep_ddl)
            infrastructure_statement_count += 1

            for step in steps:
                template_path = f"sql/{step.template_name}"
                params = dict(step.template_parameters)
                params["step_id"] = step.step_id
                params["description"] = step.description or step.strategy_name
                params["columns"] = step.target_columns
                params["dataset_name"] = table_ident.table
                params["schema_name"] = table_ident.schema
                params["staging_table"] = table_ident.staging_name
                params["source_table"] = table_ident.qualified_name
                params["clean_table"] = table_ident.clean_name
                params["run_id"] = "@RunId"
                step_issue_ids = list(getattr(step, "source_issue_ids", []) or [])
                params["issue_id"] = step_issue_ids[0] if step_issue_ids else ""
                if "parent_dataset" in params and params["parent_dataset"]:
                    try:
                        p_ident = parse_table_identifier(params["parent_dataset"])
                        params["parent_dataset"] = p_ident.table
                        params["parent_qualified_name"] = p_ident.qualified_name
                    except Exception:
                        pass
                params["never_drop_rows"] = never_drop
                params["pipeline_id"] = physical_plan.plan_id

                # Normalize parameter aliases across planners and test fixtures
                if "mapping" in params and "mappings" not in params:
                    params["mappings"] = params["mapping"]
                elif "mappings" in params and "mapping" not in params:
                    params["mapping"] = params["mappings"]

                if "width" in params and "length" not in params:
                    params["length"] = params["width"]
                elif "length" in params and "width" not in params:
                    params["width"] = params["length"]

                if "side" in params and "direction" not in params:
                    params["direction"] = params["side"]
                elif "direction" in params and "side" not in params:
                    params["side"] = params["direction"]

                if "fill_character" in params and "pad_char" not in params:
                    params["pad_char"] = params["fill_character"]
                elif "pad_char" in params and "fill_character" not in params:
                    params["fill_character"] = params["pad_char"]

                if "disposition" not in params:
                    params["disposition"] = "CORRECT"

                if "target_column" not in params and step.target_columns:
                    params["target_column"] = step.target_columns[0]

                # 1. Parameter contract validation
                required_params = STRATEGY_PARAMETER_CONTRACTS.get(step.strategy_name, set())
                missing_params = required_params - set(params.keys())
                if missing_params:
                    err_msg = f"Missing required parameter(s) for strategy {step.strategy_name}: {sorted(missing_params)}"
                    logger.error("[SQLCodegenAgent] %s (step=%s)", err_msg, step.step_id)
                    compilation_results.append(
                        StepCompilationResult(
                            step_id=step.step_id,
                            dataset_name=step.dataset_name,
                            action_type=getattr(step, "action_type", None) or step.strategy_name,
                            strategy_name=step.strategy_name,
                            template_name=template_path,
                            status="PARAMETER_MISSING",
                            error=err_msg,
                            source_logical_step_id=step.logical_step_id,
                            source_action_id=getattr(step, "source_action_id", None),
                            source_issue_ids=step_issue_ids,
                            is_executable_transformation=False,
                        )
                    )
                    continue

                # 2. Template existence validation
                if not self.template_engine.template_exists(template_path):
                    err_msg = f"Template not found: '{template_path}' for strategy '{step.strategy_name}'"
                    logger.error("[SQLCodegenAgent] %s (step=%s)", err_msg, step.step_id)
                    compilation_results.append(
                        StepCompilationResult(
                            step_id=step.step_id,
                            dataset_name=step.dataset_name,
                            action_type=getattr(step, "action_type", None) or step.strategy_name,
                            strategy_name=step.strategy_name,
                            template_name=template_path,
                            status="TEMPLATE_MISSING",
                            error=err_msg,
                            source_logical_step_id=step.logical_step_id,
                            source_action_id=getattr(step, "source_action_id", None),
                            source_issue_ids=step_issue_ids,
                            is_executable_transformation=False,
                        )
                    )
                    continue

                # 3. Render step
                try:
                    step_code = self.template_engine.render(template_path, **params)
                    if not is_executable_sql(step_code):
                        err_msg = f"Template '{template_path}' rendered empty or comment-only SQL"
                        logger.error("[SQLCodegenAgent] %s (step=%s)", err_msg, step.step_id)
                        compilation_results.append(
                            StepCompilationResult(
                                step_id=step.step_id,
                                dataset_name=step.dataset_name,
                                action_type=getattr(step, "action_type", None) or step.strategy_name,
                                strategy_name=step.strategy_name,
                                template_name=template_path,
                                status="RENDER_FAILED",
                                error=err_msg,
                                source_logical_step_id=step.logical_step_id,
                                source_action_id=getattr(step, "source_action_id", None),
                                source_issue_ids=step_issue_ids,
                                is_executable_transformation=False,
                            )
                        )
                    else:
                        rendered_steps.append(step_code)
                        compilation_results.append(
                            StepCompilationResult(
                                step_id=step.step_id,
                                dataset_name=step.dataset_name,
                                action_type=getattr(step, "action_type", None) or step.strategy_name,
                                strategy_name=step.strategy_name,
                                template_name=template_path,
                                status="RENDERED",
                                sql=step_code,
                                source_logical_step_id=step.logical_step_id,
                                source_action_id=getattr(step, "source_action_id", None),
                                source_issue_ids=step_issue_ids,
                                is_executable_transformation=True,
                            )
                        )
                except Exception as exc:
                    err_msg = f"Rendering error in '{template_path}': {exc}"
                    logger.exception("[SQLCodegenAgent] %s (step=%s)", err_msg, step.step_id)
                    compilation_results.append(
                        StepCompilationResult(
                            step_id=step.step_id,
                            dataset_name=step.dataset_name,
                            action_type=getattr(step, "action_type", None) or step.strategy_name,
                            strategy_name=step.strategy_name,
                            template_name=template_path,
                            status="RENDER_FAILED",
                            error=err_msg,
                            source_logical_step_id=step.logical_step_id,
                            source_action_id=getattr(step, "source_action_id", None),
                            source_issue_ids=step_issue_ids,
                            is_executable_transformation=False,
                        )
                    )

            # Retrieve output contract for this dataset (projection)
            output_contracts = context.metadata.get("output_contracts") or {}
            contract_data = output_contracts.get(ds_name) or {}
            clean_columns = contract_data.get("clean_columns") or []

            clean_table = table_ident.clean_name
            if clean_columns:
                cols_quoted = ",\n    ".join(f"[{c}]" for c in clean_columns)
                projection_sql = (
                    f"-- Strict Final Clean Projection (Output Contract Conformance)\n"
                    f"IF OBJECT_ID('{clean_table}', 'U') IS NOT NULL DROP TABLE {clean_table};\n"
                    f"SELECT\n    {cols_quoted}\nINTO {clean_table}\nFROM {staging_table};"
                )
                rendered_steps.append(projection_sql)
                projection_statement_count += 1

            dataset_transforms[ds_name] = "\n\n".join(rendered_steps) if rendered_steps else "-- No transformations"

        # ── Fail-Closed Validation Gates ──────────────────────────────────────
        ing_telem = context.metadata.get("ingestion_telemetry") or {}
        assessment_issue_count = ing_telem.get("assessment_discovered_count", len(context.detected_issues))
        default_mapped = len(context.structured_actions) if context.structured_actions else (len(context.logical_plan.steps) if context.logical_plan else 0)
        mapped_issue_count = ing_telem.get("mapped_issue_count", ing_telem.get("mapped_action_count", default_mapped))
        unsupported_issue_count = ing_telem.get("unsupported_issue_count", 0)
        manual_review_issue_count = ing_telem.get("manual_review_issue_count", 0)

        structured_action_count = len(context.structured_actions)
        logical_step_count = len(context.logical_plan.steps) if context.logical_plan else 0
        physical_step_count = len(physical_plan.steps)
        mapped_action_count = mapped_issue_count
        planned_step_count = physical_step_count

        rendered_transformation_count = sum(
            r.status == "RENDERED"
            and r.is_executable_transformation
            for r in compilation_results
        )
        rendered_step_count = rendered_transformation_count
        template_missing_count = sum(r.status == "TEMPLATE_MISSING" for r in compilation_results)
        parameter_missing_count = sum(r.status == "PARAMETER_MISSING" for r in compilation_results)
        render_failed_count = sum(r.status == "RENDER_FAILED" for r in compilation_results)
        skipped_step_count = sum(r.status == "SKIPPED" for r in compilation_results)
        unsupported_step_count = sum(r.status == "UNSUPPORTED" for r in compilation_results)
        failed_transformation_count = template_missing_count + parameter_missing_count + render_failed_count + unsupported_step_count

        strict_mode = context.options.get("strict_execution", True)
        if context.options.get("preview_mode", False) or context.options.get("preview_partial", False):
            strict_mode = False

        telemetry = CompilationTelemetry(
            ok=True,
            executable=True,
            partial=not strict_mode and (unsupported_issue_count > 0 or failed_transformation_count > 0),
            assessment_issue_count=assessment_issue_count,
            mapped_issue_count=mapped_issue_count,
            unsupported_issue_count=unsupported_issue_count,
            manual_review_issue_count=manual_review_issue_count,
            structured_action_count=structured_action_count,
            logical_step_count=logical_step_count,
            physical_step_count=physical_step_count,
            rendered_transformation_count=rendered_transformation_count,
            failed_transformation_count=failed_transformation_count,
            template_missing_count=template_missing_count,
            parameter_missing_count=parameter_missing_count,
            render_failed_count=render_failed_count,
            skipped_step_count=skipped_step_count,
            unsupported_step_count=unsupported_step_count,
            infrastructure_statement_count=infrastructure_statement_count,
            projection_statement_count=projection_statement_count,
            fallback_projection_used=bool(clean_columns and rendered_transformation_count == 0),
            step_results=compilation_results,
        )

        # Gate Evaluation
        if assessment_issue_count == 0 and planned_step_count == 0:
            # Scenario E: Zero-issue assessment pass-through
            telemetry.pipeline_type = "pass_through"
            telemetry.message = "No cleaning transformations were required."
        else:
            # Gate 1: Issues detected but none mapped
            if assessment_issue_count > 0 and mapped_action_count == 0:
                telemetry.ok = False
                telemetry.executable = False
                telemetry.failure_stage = "capability_validation"
                raise CodeGenerationError(
                    f"Assessment detected {assessment_issue_count} issue(s), but none could be mapped to compiler actions.",
                    telemetry=telemetry.to_dict(),
                    failure_stage="capability_validation",
                )

            # Gate 2: Unsupported issues (enforced in strict mode)
            if unsupported_issue_count > 0:
                if strict_mode:
                    telemetry.ok = False
                    telemetry.executable = False
                    telemetry.failure_stage = "capability_validation"
                    raise CodeGenerationError(
                        f"Compilation stopped because {unsupported_issue_count} assessment issue(s) have no supported remediation.",
                        telemetry=telemetry.to_dict(),
                        failure_stage="capability_validation",
                    )
                else:
                    telemetry.executable = False
                    telemetry.partial = True
                    telemetry.warnings.append("Generated SQL is incomplete and must not be executed as a complete cleaning pipeline.")

            # Gate 3: Actions mapped but planners produced 0 steps
            if mapped_action_count > 0 and planned_step_count == 0:
                telemetry.ok = False
                telemetry.executable = False
                telemetry.failure_stage = "planning_validation"
                raise CodeGenerationError(
                    f"{mapped_action_count} issue action(s) were mapped, but the logical and physical planners generated no steps.",
                    telemetry=telemetry.to_dict(),
                    failure_stage="planning_validation",
                )

            # Gate 4: Missing templates, missing params, or render failures
            if template_missing_count > 0 or render_failed_count > 0 or parameter_missing_count > 0:
                if strict_mode:
                    telemetry.ok = False
                    telemetry.executable = False
                    telemetry.failure_stage = "template_validation"
                    raise CodeGenerationError(
                        "Compilation stopped because one or more planned transformations could not be rendered.",
                        telemetry=telemetry.to_dict(),
                        failure_stage="template_validation",
                    )
                else:
                    telemetry.executable = False
                    telemetry.partial = True
                    telemetry.warnings.append("One or more transformations failed to render.")

            # Gate 5: Physical plan has steps, but 0 executable steps rendered
            if planned_step_count > 0 and rendered_step_count == 0:
                telemetry.ok = False
                telemetry.executable = False
                telemetry.failure_stage = "rendering_validation"
                raise CodeGenerationError(
                    f"The physical plan contained {planned_step_count} step(s), but SQL code generation rendered no executable transformations.",
                    telemetry=telemetry.to_dict(),
                    failure_stage="rendering_validation",
                )

        context.metadata["compilation_telemetry"] = telemetry.to_dict()

        # Sanitize proc id
        clean_plan_id = re.sub(r"[^a-zA-Z0-9_]", "_", physical_plan.plan_id)

        sql_code = self.template_engine.render(
            "sql/stored_procedure.j2",
            pipeline_id=clean_plan_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            dataset_transforms=dataset_transforms,
        )

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_sql",
            name=f"sp_etl_{clean_plan_id}.sql",
            language="sql",
            content=sql_code,
            file_extension=".sql",
            is_entrypoint=True,
            metadata={
                "plan_id": physical_plan.plan_id,
                "step_count": len(physical_plan.steps),
                "engine": "sql",
                "telemetry": telemetry.to_dict(),
            },
        )

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "language": artifact.language,
                "bytes": len(sql_code),
                "rendered_steps": rendered_step_count,
            },
            source_agent=self.__class__.__name__,
        )

        return artifact
