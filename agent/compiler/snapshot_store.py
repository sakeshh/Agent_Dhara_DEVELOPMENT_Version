from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

from agent.compiler.models.context import CompilerContext


@dataclass
class CompilerSnapshot:
    snapshot_id: str
    session_id: str
    pipeline_id: str
    target_engine: str
    created_at: str
    detected_issues: List[Dict[str, Any]]
    logical_plan: Optional[Dict[str, Any]] = None
    physical_plan: Optional[Dict[str, Any]] = None
    generated_code: Optional[str] = None
    validation_result: Optional[Dict[str, Any]] = None
    agent_metrics: List[Dict[str, Any]] = field(default_factory=list)
    data_contract_yaml: Optional[str] = None
    lineage_entries: Optional[List[Dict[str, Any]]] = None
    companion_test: Optional[Dict[str, Any]] = None
    repair_analytics: Optional[Dict[str, Any]] = None
    deployment_manifest: Optional[Dict[str, Any]] = None
    semantic_plan: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class SnapshotStore:
    """Thread-safe snapshot store for compiler auditability, debugging, and rollback."""

    _instance: Optional[SnapshotStore] = None

    def __init__(self, storage_dir: Optional[Path] = None):
        self._snapshots: Dict[str, CompilerSnapshot] = {}
        self.storage_dir = storage_dir

    @classmethod
    def get_instance(cls) -> SnapshotStore:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def create_snapshot(self, context: CompilerContext) -> CompilerSnapshot:
        plan_id = context.logical_plan.plan_id if context.logical_plan else f"plan_{int(time.time())}"
        snapshot_id = f"snap_{context.session_id}_{int(time.time() * 1000)}"

        snapshot = CompilerSnapshot(
            snapshot_id=snapshot_id,
            session_id=context.session_id,
            pipeline_id=plan_id,
            target_engine=context.target_engine.value,
            created_at=datetime.now(timezone.utc).isoformat(),
            detected_issues=[i.model_dump() for i in context.detected_issues],
            logical_plan=context.logical_plan.model_dump() if context.logical_plan else None,
            physical_plan=context.physical_plan.model_dump() if context.physical_plan else None,
            generated_code=context.code_artifact.content if context.code_artifact else None,
            validation_result=context.validation_result.model_dump() if context.validation_result else None,
            agent_metrics=list(context.metadata.get("agent_metrics_history") or []),
            data_contract_yaml=context.metadata.get("data_contract_yaml"),
            lineage_entries=context.metadata.get("lineage_entries"),
            companion_test=context.metadata.get("companion_test_artifact"),
            repair_analytics=context.metadata.get("repair_analytics"),
            deployment_manifest=context.metadata.get("deployment_manifest"),
            semantic_plan=context.semantic_plan.model_dump() if hasattr(context.semantic_plan, "model_dump") else (dict(context.semantic_plan) if isinstance(context.semantic_plan, dict) else None),
            metadata=dict(context.metadata),
        )

        self._snapshots[snapshot_id] = snapshot
        return snapshot

    def get_snapshot(self, snapshot_id: str) -> Optional[CompilerSnapshot]:
        return self._snapshots.get(snapshot_id)

    def list_snapshots(self, session_id: Optional[str] = None) -> List[CompilerSnapshot]:
        if session_id is None:
            return list(self._snapshots.values())
        return [s for s in self._snapshots.values() if s.session_id == session_id]
