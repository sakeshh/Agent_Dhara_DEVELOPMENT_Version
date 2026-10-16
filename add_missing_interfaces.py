#!/usr/bin/env python
"""Add missing interface classes to interfaces __init__.py"""

with open('agent/core/interfaces/__init__.py', 'r') as f:
    content = f.read()

# Add missing interface classes before the backwards compatibility section
new_classes = """

# Pipeline definition
class PipelineDefinition:
    """Definition of an ETL pipeline."""
    pipeline_id: str
    name: str
    description: str
    phases: list
    config: dict
    metadata: dict


# Gate manager interface
class GateManagerInterface:
    """Interface for gate manager implementations."""
    async def evaluate_gate(self, gate_context, pipeline_state):
        raise NotImplementedError


# State store interface
class StateStoreInterface:
    """Interface for state store implementations."""
    async def save_state(self, state_id, state_data):
        raise NotImplementedError
    async def load_state(self, state_id):
        raise NotImplementedError


# Code generator interface
class CodeGeneratorInterface:
    """Interface for code generator implementations."""
    async def generate(self, plan, dialect):
        raise NotImplementedError


# Code validator interface
class CodeValidatorInterface:
    """Interface for code validator implementations."""
    async def validate(self, code, plan):
        raise NotImplementedError


# Deployment executor interface
class DeploymentExecutorInterface:
    """Interface for deployment executor implementations."""
    async def execute(self, code, environment):
        raise NotImplementedError


# Scheduler interface
class SchedulerInterface:
    """Interface for scheduler implementations."""
    async def schedule(self, plan, trigger):
        raise NotImplementedError
"""

# Add the new classes before the backwards compatibility marker
marker = "# Backwards compatibility exports"
if marker in content:
    content = content.replace(marker, new_classes + marker)
    # Add the new interface names to __all__
    if '"PipelineOrchestratorInterface"' in content:
        # PipelineOrchestratorInterface is already there, add the others
        for iface in ['PipelineDefinition', 'GateManagerInterface', 'StateStoreInterface', 
                      'CodeGeneratorInterface', 'CodeValidatorInterface', 'DeploymentExecutorInterface', 'SchedulerInterface']:
            if f'"{iface}"' not in content:
                # Insert after ConnectorBase
                content = content.replace('"ConnectorBase",', f'"{iface}",\n    "ConnectorBase",')
    
    with open('agent/core/interfaces/__init__.py', 'w') as f:
        f.write(content)
    print("Added missing interfaces to interfaces __init__.py")
else:
    print("Marker not found in file")