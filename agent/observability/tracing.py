from __future__ import annotations
import uuid
import contextlib
import time
from typing import Generator
from agent.observability.structured_logging import log_event
from agent.observability.metrics import metrics_registry

class ExecutionTracker:
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or f"RUN-{uuid.uuid4().hex[:12].upper()}"
        self.start_time = time.time()

    @contextlib.contextmanager
    def trace_stage(self, stage_name: str, dataset_id: Optional[str] = None) -> Generator[None, None, None]:
        t0 = time.time()
        log_event(
            event_type=f"{stage_name}_started",
            run_id=self.run_id,
            dataset_id=dataset_id,
            assessment_stage=stage_name,
            status="RUNNING"
        )
        try:
            yield
            duration = (time.time() - t0) * 1000.0
            log_event(
                event_type=f"{stage_name}_completed",
                run_id=self.run_id,
                dataset_id=dataset_id,
                assessment_stage=stage_name,
                status="COMPLETE",
                duration_ms=duration
            )
        except Exception as e:
            duration = (time.time() - t0) * 1000.0
            log_event(
                event_type=f"{stage_name}_failed",
                run_id=self.run_id,
                dataset_id=dataset_id,
                assessment_stage=stage_name,
                status="FAILED",
                duration_ms=duration,
                extra_fields={"error": str(e)}
            )
            raise
