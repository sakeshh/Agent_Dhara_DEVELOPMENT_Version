"""CodegenAgent - Generates executable ETL code from approved plans."""

from __future__ import annotations
import asyncio
import logging
import uuid
import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.models import (
    ETLPlan, DatasetPlan, PlanStep, CrossDatasetStep,
    ManualReviewItem, CleaningPlan, CoverageReport,
    ReconciliationReport, PlanMetadata, PlanStatus,
    GenerationMode, ReviewResult, GeneratedCode, ExecutionResult
)
from agent.core.interfaces import MemoryStore
from agent.core.interfaces.connector import SourceType, TableInfo, ColumnInfo, FileFormat
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.codegen")


@dataclass
class GeneratedCode:
    """Generated ETL code artifact."""
    id: str = field(default_factory=lambda: f"code_{uuid.uuid4().hex[:8]}")
    language: str = "python"  # python, sql, spark, etc.
    code: str = ""
    dialect: str = "tsql"  # target database/warehouse dialect
    plan_id: str = ""
    step_ids: List[str] = field(default_factory=list)
    evidence_ids: List[str] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "language": self.language,
            "code_len": len(self.code),
            "dialect": self.dialect,
            "plan_id": self.plan_id,
            "step_count": len(self.step_ids),
            "generated_at": self.generated_at.isoformat(),
        }


class CodegenAgent:
    """Generates executable ETL code from approved plans.
    
    Transforms plan steps into runnable code (Python, SQL, Spark) targeting
    the configured data platform. Supports multiple code dialects.
    """
    
    def __init__(self, config: Any, code_dialect: Optional[str] = None):
        self.config = config
        self.code_dialect = code_dialect or self._detect_dialect()
    
    def _detect_dialect(self) -> Any:
        """Detect the target code dialect from config."""
        if hasattr(self.config, 'target_platform', None):
            return self.config.target_platform.value if hasattr(self.config.target_platform, 'value') else self.config.target_platform
        return "python"
    
    async def generate_code(self, plan: Any) -> Any:
        """Generate code from an approved ETL plan."""
        
        with get_performance_logger("code_generation") as perf:
            
            dialect = self.code_dialect
            
            # Generate per-dataset code
            dataset_codes: Dict[str, GeneratedCode] = {}
            all_step_ids: List[str] = []
            all_evidence_ids: List[str] = []
            
            for ds_name, ds_plan in plan.datasets.items():
                ds_code = await self._generate_dataset_code(ds_name, ds_plan, plan, dialect)
                dataset_codes[ds_name] = ds_code
                all_step_ids.extend(step.step_id for step in ds_plan.steps)
                if ds_code.evidence_ids:
                    all_evidence_ids.extend(ds_code.evidence_ids)
            
            # Generate cross-dataset code
            cross_code = await self._generate_cross_dataset_code(plan, dialect)
            
            # Combine all evidence IDs
            all_evidence_ids = list(set(all_evidence_ids))
            
            # Combine codes
            combined_code = self._combine_codes(dataset_codes, cross_code, dialect)
            
            code_artifact = GeneratedCode(
                language="python",
                code=combined_code,
                dialect=dialect,
                plan_id=plan.plan_id,
                step_ids=list(set(all_step_ids)),
                evidence_ids=all_evidence_ids,
            )
            
            perf.log_metric("plan_id", plan.plan_id)
            perf.log_metric("dialect", dialect)
            perf.log_metric("dataset_count", len(dataset_codes))
            perf.log_metric("step_count", len(all_step_ids))
            
            return code_artifact
    
    async def _generate_dataset_code(self, ds_name: str, ds_plan: Any, plan: Any, dialect: Any) -> Any:
        """Generate code for a single dataset."""
        
        steps = ds_plan.steps
        
        if not steps:
            # No steps - generate no-op
            return GeneratedCode(
                language="python",
                code="pass\n",
                dialect=dialect,
                plan_id=plan.plan_id,
                step_ids=[],
                evidence_ids=[],
            )
        
        # Group steps by action type
        action_groups: Dict[str, List[Any]] = {}
        for step in steps:
            action = step.action
            if action not in action_groups:
                action_groups[action] = []
            action_groups[action].append(step)
        
        # Generate code for each action group
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for action, action_steps in action_groups.items():
            part = await self._generate_action_code(action, action_steps, ds_name, ds_plan, plan)
            code_parts.append(part["code"])
            evidence_ids.extend(part.get("evidence_ids", []))
        
        # Build dataset-specific code
        dataset_code = self._build_dataset_code(ds_name, code_parts, dialect, ds_plan)
        
        return GeneratedCode(
            language="python",
            code=dataset_code,
            dialect=dialect,
            plan_id=plan.plan_id,
            step_ids=[step.step_id for step in ds_plan.steps],
            evidence_ids=list(set(evidence_ids)),
        )
    
    async def _generate_action_code(self, action: str, steps: List[Any], ds_name: str, ds_plan: Any, plan: Any) -> Any:
        """Generate code for a specific action type."""
        
        if not steps:
            return {"code": "\n", "evidence_ids": []}
        
        # Get first step's params as template
        first_step = steps[0]
        params = first_step.params
        
        if action == "fill_nulls":
            return await self._generate_fill_null_code(steps, params)
        if action == "standardize_format":
            return await self._generate_standardize_format_code(steps, params)
        if action == "clip_outliers":
            return await self._generate_clip_outliers_code(steps, params)
        if action == "deduplicate":
            return await self._generate_deduplicate_code(steps, params)
        if action == "validate_value":
            return await self._generate_validate_value_code(steps, params)
        if action == "refine_semantic_type":
            return await self._generate_refine_semantic_code(steps, params)
        if action == "incremental_read":
            return await self._generate_incremental_read_code(steps, params, ds_name)
        if action == "noop":
            return {"code": "\n", "evidence_ids": []}
        
        # Default - generic step
        return {
            "code": self._generate_generic_step_code(step, ds_name),
            "evidence_ids": [],
        }
    
    async def _generate_fill_null_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to fill null values."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                continue
            
            # Determine fill strategy from params
            strategy = step.params.get("fill_strategy", "mean") if hasattr(step, 'params') else "mean"
            fill_value = step.params.get("fill_value") if step.params else None
            
            if fill_value:
                part = f"df['{col}'].fillna({fill_value!r}, inplace=True)"
            elif strategy == "mean":
                part = f"df['{col}'].fillna(df['{col}'].mean(), inplace=True)"
            elif strategy == "median":
                part = f"df['{col}'].fillna(df['{col}'].median(), inplace=True)"
            elif strategy == "mode":
                part = f"df['{col}'].fillna(df['{col}'].mode()[0], inplace=True)"
            elif strategy == "ffill":
                part = f"df['{col}'].ffill(inplace=True)"
            elif strategy == "bfill":
                part = f"df['{col}'].fillna(method='bfill', inplace=True)"
            else:
                part = f"df['{col}'].fillna({strategy}, inplace=True)"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_standardize_format_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to standardize format."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                continue
            
            target_format = step.params.get("target_format", "canonical") if step.params else "canonical"
            strict = step.params.get("strict", True) if step.params else True
            
            if strict:
                part = f"df['{col}'] = standardize_to_canonical(df['{col}'], format='{target_format}')"
            else:
                part = f"df['{col}'] = lenient_standardize(df['{col}'], format='{target_format}')"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_clip_outliers_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to clip outliers."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                continue
            
            method = step.params.get("method", "iqr") if step.params else "iqr"
            threshold = step.params.get("threshold", 1.5) if step.params else 1.5
            
            if method == "iqr":
                part = f"df['{col}'] = clip_iqr(df['{col}'], k={threshold})"
            elif method == "zscore":
                part = f"df['{col}'] = clip_zscore(df['{col}'], k={threshold})"
            else:
                part = f"df['{col}'] = clip_value(df['{col}'], min={threshold}, max={threshold})"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_deduplicate_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to deduplicate."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                # Dedup on entire row
                part = "df = df.drop_duplicates(inplace=False)"
                code_parts.append(part)
                continue
            
            strategy = step.params.get("strategy", "keep_last") if step.params else "keep_last"
            
            if strategy == "keep_first":
                part = f"df = df.drop_duplicates(subset=['{col}'], keep='first')"
            elif strategy == "keep_last":
                part = f"df = df.drop_duplicates(subset=['{col}'], keep='last')"
            else:
                part = f"df = df.drop_duplicates()"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_validate_value_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to validate values."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                continue
            
            # Get validation rules
            rules = step.params.get("validation_rules", []) if step.params else []
            
            if rules:
                rule_parts = []
                for rule in rules:
                    rule_type = rule.get("type", "unknown")
                    if rule_type == "range":
                        min_val = rule.get("min")
                        max_val = rule.get("max")
                        rule_parts.append(f"df['{col}'] = df['{col}'].between({min_val}, {max_val})")
                    elif rule_type == "pattern":
                        pattern = rule.get("pattern")
                        rule_parts.append(f"df['{col}'] = df['{col}'].str.match('{pattern}')")
                    elif rule_type == "nullable":
                        rule_parts.append(f"null_mask = df['{col}'].isna(); df['{col}'] = df['{col}'].where(~null_mask)")
                
                # Combine rules
                combined = "\n".join(rule_parts)
                part = f"df = df[{combined}]" if rule_parts else "\n"
            else:
                # Default numeric validation
                part = f"df = df[df['{col}'].notna() & (df['{col}'].between(df['{col}'].min(), df['{col}'].max()))]"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_refine_semantic_code(self, steps: List[Any], params: Any) -> Any:
        """Generate code to refine semantic types."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        for step in steps:
            col = params.get("column")
            if not col:
                continue
            
            target_type = step.params.get("target_semantic_type", "string") if step.params else "string"
            
            part = f"df['{col}'] = infer_semantic_type(df['{col}'], target='{target_type}')"
            
            code_parts.append(part)
            evidence_ids.append(step.evidence_id) if step.evidence_id else None
        
        code = "\n".join(code_parts) if code_parts else "\n"
        
        return {"code": code, "evidence_ids": evidence_ids}
    
    async def _generate_incremental_read_code(self, steps: List[Any], params: Any, ds_name: Any) -> Any:
        """Generate code for incremental reading."""
        
        code_parts: List[str] = []
        evidence_ids: List[str] = []
        
        # Get watermark column
        watermark_col = params.get("watermark_column") if params else None
        
        # Get incremental config
        if hasattr(self.config, 'incremental_config', None):
            inc_config = self.config.incremental_config
            
            if watermark_col:
                if inc_config and inc_config.get("watermark_strategy") == "incremental_cdf":
                    part = f"df = incremental_cdf_read('{ds_name}', watermark='{watermark_col}', config={inc_config})"
                elif inc_config and inc_config.get("watermark_strategy") == "watermark":
                    part = f"df = watermark_read('{ds_name}', watermark_col='{watermark_col}')"
                else:
                    part = f"df = watermark_read('{ds_name}', watermark_col='{watermark_col}')"
            else:
                part = f"df = watermark_read('{ds_name}')"
            
            code_parts.append(part)
        else:
            # Default incremental
            part = f"df = incremental_read('{ds_name}', watermark_col='{watermark_col}')"
            code_parts.append(part)
        
        evidence_ids.append(step.evidence_id) if steps and steps[0].evidence_id else None
        
        return {"code": "\n".join(code_parts), "evidence_ids": evidence_ids}
    
    def _build_dataset_code(self, ds_name: str, code_parts: List[str], dialect: Any, ds_plan: Any) -> Any:
        """Build full dataset code from parts."""
        
        combined = "\n\n".join(code_parts) if code_parts else "pass\n"
        
        # Add dataset metadata
        code = f"\n# Dataset: {ds_name}\n{combined}\n"
        
        return code
    
    async def _generate_cross_dataset_code(self, plan: Any, dialect: Any) -> Any:
        """Generate code for cross-dataset steps."""
        
        steps = plan.cross_dataset_steps
        
        if not steps:
            return "\n"
        
        code_parts: List[str] = []
        
        for step in steps:
            if step.action == "join":
                part = self._generate_join_code(step)
            elif step.action == "union":
                part = self._generate_union_code(step)
            elif step.action == "cross_dataset_agg":
                part = self._generate_aggregation_code(step)
            else:
                part = f"# {step.action}\n"
            code_parts.append(part)
        
        return "\n\n".join(code_parts) if code_parts else "\n"
    
    def _generate_join_code(self, step: Any) -> Any:
        """Generate join code."""
        
        datasets = step.datasets
        join_keys = step.join_keys
        
        if len(datasets) == 2:
            ds1, ds2 = datasets
            col1, col2 = join_keys.get(ds1), join_keys.get(ds2)
            
            if col1 and col2:
                return f"df = df.merge({ds2}, left_on='{col1}', right_on='{col2}', how='left')"
        
        # Generic fallback
        return f"df = df.join({str(datasets)}, **{str(join_keys)})"
    
    def _generate_union_code(self, step: Any) -> Any:
        """Generate union code."""
        
        datasets = step.datasets
        
        if len(datasets) >= 2:
            parts = [f"df_{i}" for i in range(len(datasets))]
            part = "\n".join([f"{p} = read_{ds}({ds})" for p, ds in zip(parts, datasets)])
            part += "\ndf = pandas.concat([\n" + ",\n".join(parts) + "])"
            return part
        
        return "\n"
    
    def _generate_aggregation_code(self, step: Any) -> Any:
        """Generate aggregation code."""
        
        datasets = step.datasets
        
        if datasets:
            ds = datasets[0]
            return f"df = aggregate_{ds}({ds})"
        
        return "\n"
    
    def _combine_codes(self, dataset_codes: Dict[str, Any], cross_code: Any, dialect: Any) -> Any:
        """Combine per-dataset and cross-dataset code."""
        
        code_parts: List[str] = []
        
        # Per-dataset code
        for ds_name, ds_code in dataset_codes.items():
            code_parts.append(f"\n# Dataset: {ds_name}\n{ds_code.code}")
        
        # Cross-dataset code
        if cross_code:
            code_parts.append(f"\n# Cross-dataset operations\n{cross_code}")
        
        combined = "\n\n".join(code_parts) if code_parts else "pass\n"
        
        return combined
    
    def _generate_generic_step_code(self, step: Any, ds_name: Any) -> Any:
        """Generate generic step code."""
        
        action = step.action
        col = step.params.get("column") if step.params else None
        
        if col:
            return f"# Step {step.step_id}: {action} on {col}\npass\n"
        return f"# Step {step.step_id}: {action}\npass\n"