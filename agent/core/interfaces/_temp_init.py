"""Core interfaces for Agent Dhara - Base contracts all components implement."""
from .base_agent import BaseAgent, AgentResult, AgentInput, AgentOutput
from .incremental import IncrementalStrategy, IncrementalConfig, IncrementalStrategyBase, IncrementalState, WriteResult, IncrementalInput, IncrementalOutput
from .memory import MemoryStore, MemoryInterface, MemoryRecord, MemoryQuery, MemoryType, MemoryTier, MemoryStats
from .connector import ConnectorBase, ConnectorConfig, SourceType, FileFormat, TableInfo, ColumnInfo, FileInfo, ConnectorFactory
from .orchestration import PipelineState, PhaseGate, GateContext, AgentMetadata, PipelineOrchestratorInterface, PipelineDefinition, ScheduledRun


__all__ = [
    "BaseAgent",
    "AgentResult", 
    "AgentInput",
    "AgentOutput",
    "IncrementalStrategy",
    "IncrementalConfig", 
    "IncrementalStrategyBase",
    "IncrementalState",
    "WriteResult",
    "IncrementalInput",
    "IncrementalOutput",
    "MemoryStore",
    "MemoryInterface",
    "MemoryRecord",
    "MemoryQuery",
    "MemoryType",
    "MemoryTier",
    "MemoryStats",
    "SourceType",
    "FileFormat",
    "TableInfo",
    "ColumnInfo",
    "FileInfo",
    "ConnectorFactory",
    "ConnectorBase",
    "ConnectorConfig",
    "PipelineState",
    "PhaseGate",
    "GateContext",
    "AgentMetadata",
    "PipelineOrchestratorInterface",
    "PipelineDefinition",
    "ScheduledRun",
]