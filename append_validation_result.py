with open('agent/etl_pipeline/models.py', 'a') as f:
    f.write('''


@dataclass
class ValidationResult:
    """Result of code validation."""
    plan_id: str
    step_id: Optional[str] = None
    checks_passed: int = 0
    checks_total: int = 0
    severity: str = "info"
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "checks_passed": self.checks_passed,
            "checks_total": self.checks_total,
            "severity": self.severity,
            "details": self.details,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
        }
''')