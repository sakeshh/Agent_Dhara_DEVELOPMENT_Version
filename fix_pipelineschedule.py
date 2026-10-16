#!/usr/bin/env python
"""Fix PipelineSchedule dataclass field order in models.py"""

with open('agent/etl_pipeline/models.py', 'r') as f:
    content = f.read()

new_class = """@dataclass
class PipelineSchedule:
    """A pipeline schedule configuration."""
    plan_id: str
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    schedule_name: str = ""
    cron_expression: Optional[str] = None
    trigger: str = "one_time"
    status: str = "pending"
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
        }"""

# Replace the PipelineSchedule class
old_class_pattern = """@dataclass
class PipelineSchedule:
    """A pipeline schedule configuration."""
    plan_id: str
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}"""
    schedule_name: str
    cron_expression: Optional[str] = None
    trigger: str = "one_time"
    status: str = "pending"
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
        }"""

# Actually, let me just find and replace using the known content
# The current PipelineSchedule has: id first (with default), plan_id second (no default)
# I need to swap so plan_id is first (no default), id second (with default)

# Find the exact current content and replace
old = """    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    plan_id: str
    schedule_name: str"""

new = """    plan_id: str
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    schedule_name: str = """""

if old in content:
    content = content.replace(old, new)
    with open('agent/etl_pipeline/models.py', 'w') as f:
        f.write(content)
    print("Fixed PipelineSchedule field order")
else:
    print("Pattern not found, trying alternate...")
    # Try alternate pattern
    old2 = """    id: str = field(default_factory=lambda: f"schedule_
    plan_id: str"""
    if old2 in content:
        content = content.replace(old2, """    plan_id: str\n    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}".""")
        with open('agent/etl_pipeline/models.py', 'w') as f:
            f.write(content)
        print("Fixed PipelineSchedule field order (alternate)")
    else:
        print("Neither pattern found")