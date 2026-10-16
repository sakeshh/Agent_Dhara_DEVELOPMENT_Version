"""SchedulerAgent - Pipeline scheduling and orchestration."""

from __future__ import annotations
import asyncio
import logging
import uuid
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.models import (
    ETLPlan, PlanStatus, GenerationMode, ManualReviewItem,
    PipelineSchedule, PlanStep, CrossDatasetStep,
    CoverageReport, ReconciliationReport, PlanMetadata
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.scheduler")


@dataclass
class PipelineSchedule:
    """A pipeline schedule configuration."""
    plan_id: str
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    schedule_name: str = ""
    cron_expression: Optional[str] = None
    trigger: str = "one_time"  # "cron", "interval", "one_time", "event_based"
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_completed: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    configured: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "plan_id": self.plan_id,
            "schedule_name": self.schedule_name,
            "trigger": self.trigger,
            "status": self.status,
            "cron_expression": self.cron_expression.isoformat() if self.cron_expression else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_completed": self.last_completed.isoformat() if self.last_completed else None,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "configured": self.configured,
        }


@dataclass
class PlanStatusSnapshot:
    """Snapshot of plan status at scheduling time."""
    plan_id: str
    status: PlanStatus
    captured_at: datetime = field(default_factory=datetime.utcnow)
    reason: str = ""
    blocking_items: List[str] = field(default_factory=list)


class SchedulerAgent:
    """SchedulerAgent - Pipeline scheduling and orchestration.
    
    Supports cron-based scheduling, interval triggers, and one-time execution.
    Coordinates with the pipeline orchestrator to manage plan execution timing.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logger
        self.timeout_minutes = self.config.get('timeout_minutes', 30)
    
    async def schedule_run(self, plan: ETLPlan, trigger: str = "one_time") -> PipelineSchedule:
        """Schedule an ETL plan run.
        
        Args:
            plan: The ETL plan to schedule
            trigger: Trigger type ("cron", "interval", "one_time", "event_based")
            
        Returns:
            PipelineSchedule with scheduling details
        """
        schedule = PipelineSchedule(
            plan_id=plan.plan_id,
            schedule_name=self._generate_schedule_name(plan),
            trigger=trigger,
            status="pending",
        )
        
        if trigger == "cron" and self.config.get('cron_expression'):
            schedule.cron_expression = self.config['cron_expression']
        elif trigger == "interval":
            schedule.trigger = "interval"
        
        self.logger.info(f"Scheduled plan {plan.plan_id} with trigger: {trigger}")
        return schedule
    
    async def check_due_runs(self, schedules: List[PipelineSchedule]) -> List[PipelineSchedule]:
        """Check which scheduled runs are due for execution.
        
        Args:
            schedules: List of schedules to check
            
        Returns:
            List of schedules that are due
        """
        due: List[PipelineSchedule] = []
        
        for schedule in schedules:
            if schedule.trigger == "one_time" or self._is_cron_due(schedule):
                if schedule.status == "pending":
                    schedule.status = "ready"
                    due.append(schedule)
        
        return due
    
    def _is_cron_due(self, schedule: PipelineSchedule) -> bool:
        """Check if a cron schedule is due.
        
        Args:
            schedule: The schedule to check
            
        Returns:
            True if the cron schedule is due
        """
        # Simplified cron check - in production would use a proper cron library
        if not schedule.cron_expression:
            return False
        
        now = datetime.utcnow()
        # Basic check - if next_run is set and in the past, it's due
        if schedule.next_run and schedule.next_run <= now:
            return True
        if not schedule.next_run:
            # First run - schedule for immediate execution
            schedule.next_run = now + timedelta(minutes=1)
            return True
        
        return False
    
    async def update_schedule_status(self, schedule: PipelineSchedule, status: str, error: Optional[str] = None) -> PipelineSchedule:
        """Update the status of a scheduled run.
        
        Args:
            schedule: The schedule to update
            status: New status
            error: Optional error message if failed
            
        Returns:
            Updated schedule
        """
        schedule.status = status
        
        if status == "completed":
            schedule.success_count = getattr(schedule, 'success_count', 0) + 1
            schedule.last_completed = datetime.utcnow()
        elif status == "failed":
            schedule.failure_count = getattr(schedule, 'failure_count', 0) + 1
            schedule.last_run = datetime.utcnow()
            if error:
                schedule.metadata["last_error"] = error
        elif status == "running":
            schedule.last_run = datetime.utcnow()
        
        self.logger.info(f"Updated schedule {schedule.id} status to: {status}")
        return schedule
    
    def _generate_schedule_name(self, plan: ETLPlan) -> str:
        """Generate a schedule name from the plan."""
        return f"{plan.plan_id}_{uuid.uuid4().hex[:4]}"