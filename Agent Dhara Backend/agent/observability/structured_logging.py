from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("agent.observability.structured")

def log_event(
    event_type: str,
    run_id: str,
    dataset_id: Optional[str] = None,
    relationship_id: Optional[str] = None,
    assessment_stage: Optional[str] = None,
    status: str = "COMPLETE",
    duration_ms: Optional[float] = None,
    extra_fields: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log structured event payloads in standard JSON formats for analytics ingestion.
    """
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "run_id": run_id,
        "status": status,
    }
    if dataset_id:
        event["dataset_id"] = dataset_id
    if relationship_id:
        event["relationship_id"] = relationship_id
    if assessment_stage:
        event["assessment_stage"] = assessment_stage
    if duration_ms is not None:
        event["duration_ms"] = round(duration_ms, 2)

    if extra_fields:
        # Exclude high-cardinality values from metric mappings; log them cleanly
        event.update(extra_fields)

    logger.info(json.dumps(event))
