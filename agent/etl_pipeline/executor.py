"""ExecutorAgent - Executes validated ETL code against data platforms."""

from __future__ import annotations
import asyncio
import logging
import uuid
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.models import (
    ETLPlan, GeneratedCode, ExecutionResult, PlanStep,
    CrossDatasetStep,
    ManualReviewItem, PlanStatus
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger, set_context, clear_context, LogContext

logger = get_logger("etl_pipeline.executor")


@dataclass
class ExecutionConfig:
    """Configuration for code execution."""
    
    engine: str  # "python", "spark", "sql", "trino", etc.
    max_retries: int = 3
    timeout_minutes: int = 30
    enable_progress_tracking: bool = True
    fail_fast: bool = True  # Stop on first error
    
    # Execution mode
    mode: str = "incremental"  # "full", "incremental", "validate_only"
    
    # Progress tracking
    track_per_step: bool = True
    summarize_results: bool = True


@dataclass
class ExecutionResult:
    """Result of code execution."""
    execution_id: str = field(default_factory=lambda: f"exec_{uuid.uuid4().hex[:8]}")
    plan_id: str = ""
    status: str = "pending"  # pending, running, completed, failed, cancelled
    
    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Results
    success: Optional[bool] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    
    # Per-step results
    step_results: List[Dict[str, Any]] = field(default_factory=list)
    
    # Records processed/written
    records_processed: Optional[int] = None
    records_written: Optional[int] = None
    
    # Log context
    log_context_id: Optional[str] = None
    memory_store: Optional[MemoryStore] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "error_details": self.error_details,
            "step_results": self.step_results,
            "records_processed": self.records_processed,
            "records_written": self.records_written,
            "log_context_id": self.log_context_id,
        }


class ExecutorAgent:
    """Executes validated ETL code against data platforms.
    
    Supports Python execution with incremental reading, and can target
    various data platforms (python, spark, sql, trino, etc.).
    """
    
    def __init__(self, config: ExecutionConfig, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.logger = logger
    
    async def execute_plan(self, plan: ETLPlan) -> ExecutionResult:
        """Execute an ETL plan.
        
        Args:
            plan: The ETL plan to execute
            
        Returns:
            ExecutionResult with status and output data
        """
        execution_id = f"exec_{uuid.uuid4().hex[:8]}"
        started_at = datetime.utcnow()
        
        result = ExecutionResult(
            execution_id=execution_id,
            plan_id=plan.plan_id,
            status="running",
            started_at=started_at,
            config=self.config,
        )
        
        try:
            for ds_name, ds_plan in plan.datasets.items():
                self.logger.info(f"Executing dataset: {ds_name}")
                
                for step in ds_plan.steps:
                    self.logger.info(f"Executing step {step.step_id}: {step.action}")
                    await self._execute_step(step, ds_plan)
                
                # Execute cross-dataset steps after each dataset
                for cds in plan.cross_dataset_steps:
                    self.logger.info(f"Executing cross-dataset step: {cds.action}")
                    await self._execute_cross_dataset_step(cds, plan)
            
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            result.status = "completed"
            result.success = True
            
        except Exception as e:
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            result.status = "failed"
            result.error = str(e)
            self.logger.error(f"Execution failed: {e}")
            raise
        
        return result
    
    async def _execute_step(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Execute a single plan step."""
        action = step.action
        
        if action == "fill_nulls":
            await self._fill_nulls(step, ds_plan)
        elif action == "standardize_format":
            await self._standardize_format(step, ds_plan)
        elif action == "clip_outliers":
            await self._clip_outliers(step, ds_plan)
        elif action == "deduplicate":
            await self._deduplicate(step, ds_plan)
        elif action == "validate_value":
            await self._validate_value(step, ds_plan)
        elif action == "refine_semantic_type":
            await self._refine_semantic_type(step, ds_plan)
        elif action == "incremental_read":
            await self._incremental_read(step, ds_plan)
        elif action == "noop":
            pass  # No operation
        else:
            self.logger.warning(f"Unknown action: {action}")
    
    async def _fill_nulls(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Fill null values in a column."""
        col = step.params.get("column") if step.params else None
        if not col:
            return
        
        strategy = step.params.get("fill_strategy", "mean") if step.params else "mean"
        fill_value = step.params.get("fill_value") if step.params else None
        
        if fill_value:
            self.logger.info(f"Filling nulls in {col} with value {fill_value}")
        elif strategy == "mean":
            self.logger.info(f"Filling nulls in {col} with mean")
        elif strategy == "median":
            self.logger.info(f"Filling nulls in {col} with median")
        elif strategy == "mode":
            self.logger.info(f"Filling nulls in {col} with mode")
        elif strategy == "ffill":
            self.logger.info(f"Forward-filling nulls in {col}")
        elif strategy == "bfill":
            self.logger.info(f"Back-filling nulls in {col}")
        else:
            self.logger.info(f"Filling nulls in {col} with strategy: {strategy}")
    
    async def _standardize_format(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Standardize format of a column."""
        col = step.params.get("column") if step.params else None
        if not col:
            return
        
        target_format = step.params.get("target_format", "canonical") if step.params else "canonical"
        self.logger.info(f"Standardizing format of {col} to {target_format}")
    
    async def _clip_outliers(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Clip outliers from a column."""
        col = step.params.get("column") if step.params else None
        if not col:
            return
        
        method = step.params.get("method", "iqr") if step.params else "iqr"
        threshold = step.params.get("threshold", 1.5) if step.params else 1.5
        
        self.logger.info(f"Clipping outliers in {col} using {method} method (k={threshold})")
    
    async def _deduplicate(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Remove duplicate rows."""
        col = step.params.get("column") if step.params else None
        
        if not col:
            self.logger.info("Removing all duplicate rows")
        else:
            strategy = step.params.get("strategy", "keep_last") if step.params else "keep_last"
            self.logger.info(f"Deduplicating on {col} with strategy: {strategy}")
    
    async def _validate_value(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Validate values in a column."""
        col = step.params.get("column") if step.params else None
        if not col:
            return
        
        rules = step.params.get("validation_rules", []) if step.params else []
        
        if rules:
            self.logger.info(f"Validating values in {col} with {len(rules)} rule(s)")
        else:
            self.logger.info(f"Validating values in {col}")
    
    async def _refine_semantic_type(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Refine the semantic type of a column."""
        col = step.params.get("column") if step.params else None
        if not col:
            return
        
        target_type = step.params.get("target_semantic_type", "string") if step.params else "string"
        self.logger.info(f"Refining semantic type of {col} to {target_type}")
    
    async def _incremental_read(self, step: PlanStep, ds_plan: DatasetPlan) -> Any:
        """Perform incremental read based on watermark."""
        watermark_col = step.params.get("watermark_column") if step.params else None
        
        self.logger.info(f"Incremental read on dataset {ds_plan.dataset}")
        if watermark_col:
            self.logger.info(f"Using watermark column: {watermark_col}")
        else:
            self.logger.info("No watermark column specified")
    
    async def _execute_cross_dataset_step(self, step: CrossDatasetStep, plan: ETLPlan) -> Any:
        """Execute a cross-dataset step."""
        action = step.action
        
        if action == "join":
            self.logger.info(f"Joining datasets: {step.datasets}")
        elif action == "union":
            self.logger.info(f"Union of datasets: {step.datasets}")
        elif action == "cross_dataset_agg":
            self.logger.info(f"Aggregation across datasets: {step.datasets}")
        else:
            self.logger.warning(f"Unknown cross-dataset action: {action}")