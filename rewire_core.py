#!/usr/bin/env python
"""Rewrite core __init__.py with only essential exports"""

with open('agent/core/__init__.py', 'r') as f:
    content = f.read()

# Keep only the essential exports needed for the pipeline to work
# Remove all the interface class imports and just keep the core types
new_content = '''"""Agent Dhara Core - Main exports."\"\"\"\\n\\nfrom .interfaces import (\\n    # Base agent\\n    BaseAgent,\\n    AgentResult,\\n    AgentInput,\\n    AgentOutput,\\n    \\n    # Incremental\\n    IncrementalStrategy,\\n    IncrementalConfig,\\n    IncrementalState,\\n    WriteResult,\\n    IncrementalStrategyBase,\\n    IncrementalInput,\\n    IncrementalOutput,\\n    \\n    # Memory\\n    MemoryStore,\\n    MemoryInterface,\\n    MemoryRecord,\\n    MemoryQuery,\\n    MemoryType,\\n    MemoryTier,\\n    MemoryStats,\\n    \\n    # Connector\\n    ConnectorBase,\\n    ConnectorConfig,\\n    SourceType,\\n    FileFormat,\\n    TableInfo,\\n    ColumnInfo,\\n    FileInfo,\\n    ConnectorFactory,\\n    \\n    # Essential orchestration\\n    PipelineState,\\n    PipelineStatus,\\n    PhaseGate,\\n    GateContext,\\n    GateDecision,\\n    GateDecisionRecord,\\n    PipelineDefinition,\\n    ScheduledRun,\\n)\\n\\n# Backwards compatibility\\nPipelineStateType = PipelinePhase\\nPipelineStatusType = PipelineStatus\\n'''

# Simple replacement - just rewrite the whole file
with open('agent/core/__init__.py', 'w') as f:
    f.write(new_content)
print("Rewrote core __init__.py with essential exports only")