#!/usr/bin/env python
"""Add PipelineSchedule to models.py"""

with open('agent/etl_pipeline/models.py', 'r') as f:
    content = f.read()

pipeline_schedule = '''

@dataclass
class PipelineSchedule:
    """A pipeline schedule configuration."""
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    plan_id: str
    schedule_name: str
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
        }
'''

# Add PipelineSchedule before the ValidationResult class
marker = "class ValidationResult"
if marker in content:
    content = content.replace(marker, pipeline_schedule + marker)
    with open('agent/etl_pipeline/models.py', 'w') as f:
        f.write(content)
    print("Successfully added PipelineSchedule to models.py")
else:
    print("Marker not found in models.py")