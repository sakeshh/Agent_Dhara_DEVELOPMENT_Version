from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Any, Callable, Dict, List, Optional
import logging

logger = logging.getLogger("agent.compiler.event_bus")


class CompilerEventType(str, Enum):
    ASSESSMENT_INGESTED = "ASSESSMENT_INGESTED"
    PATTERN_MATCHED = "PATTERN_MATCHED"
    LOGICAL_PLAN_CREATED = "LOGICAL_PLAN_CREATED"
    AST_OPTIMIZED = "AST_OPTIMIZED"
    PHYSICAL_PLAN_COMPILED = "PHYSICAL_PLAN_COMPILED"
    LANGUAGE_ROUTED = "LANGUAGE_ROUTED"
    CODE_GENERATED = "CODE_GENERATED"
    VALIDATION_PASSED = "VALIDATION_PASSED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    REPAIR_ATTEMPTED = "REPAIR_ATTEMPTED"
    REPAIR_SUCCEEDED = "REPAIR_SUCCEEDED"
    TESTS_GENERATED = "TESTS_GENERATED"
    CONTRACT_GENERATED = "CONTRACT_GENERATED"
    LINEAGE_GENERATED = "LINEAGE_GENERATED"
    ARTIFACT_MANIFEST_PUBLISHED = "ARTIFACT_MANIFEST_PUBLISHED"


@dataclass(frozen=True)
class CompilerEvent:
    event_type: CompilerEventType
    session_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    source_agent: Optional[str] = None


class EventBus:
    """Thread-safe event bus for cross-agent lifecycle events and decoupled observability."""

    _instance: Optional[EventBus] = None

    def __init__(self):
        self._subscribers: Dict[CompilerEventType, List[Callable[[CompilerEvent], None]]] = {}
        self._history: List[CompilerEvent] = []

    @classmethod
    def get_instance(cls) -> EventBus:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def subscribe(
        self,
        event_type: CompilerEventType,
        handler: Callable[[CompilerEvent], None],
    ) -> None:
        self._subscribers.setdefault(event_type, []).append(handler)

    def publish(self, event: CompilerEvent) -> None:
        self._history.append(event)
        handlers = self._subscribers.get(event.event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.warning(f"Error in EventBus handler for {event.event_type}: {e}")

    def emit(
        self,
        event_type: CompilerEventType,
        session_id: str,
        payload: Optional[Dict[str, Any]] = None,
        source_agent: Optional[str] = None,
    ) -> CompilerEvent:
        event = CompilerEvent(
            event_type=event_type,
            session_id=session_id,
            payload=payload or {},
            source_agent=source_agent,
        )
        self.publish(event)
        return event

    def get_events(self, session_id: Optional[str] = None) -> List[CompilerEvent]:
        if session_id is None:
            return list(self._history)
        return [e for e in self._history if e.session_id == session_id]

    def clear(self) -> None:
        self._history.clear()
        self._subscribers.clear()
