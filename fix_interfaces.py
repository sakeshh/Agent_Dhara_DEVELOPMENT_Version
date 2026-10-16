#!/usr/bin/env python
"""Fix interfaces __init__.py to export PipelineOrchestratorInterface"""

with open('agent/core/interfaces/__init__.py', 'r') as f:
    content = f.read()

# Add the import from orchestration
old_import = 'from .orchestration import PipelineState, PhaseGate, GateContext, AgentMetadata'
new_import = 'from .orchestration import PipelineState, PhaseGate, GateContext, AgentMetadata, PipelineOrchestratorInterface'
content = content.replace(old_import, new_import)

# Add to __all__ if not already there
if 'PipelineOrchestratorInterface' not in content.split('__all__')[1].split(']')[0]:
    # Find the __all__ list and add PipelineOrchestratorInterface
    all_start = content.find('__all__ = [')
    if all_start >= 0:
        # Find where to insert - after ConnectorFactory
        insert_pos = content.find('ConnectorFactory,', all_start)
        if insert_pos >= 0:
            # Add PipelineOrchestratorInterface after ConnectorFactory
            insertion = ',\n    "PipelineOrchestratorInterface"'
            content = content[:insert_pos + len('ConnectorFactory,')] + insertion + content[insert_pos + len('ConnectorFactory,'):]
        
        with open('agent/core/interfaces/__init__.py', 'w') as f:
            f.write(content)
        print("Updated interfaces __init__.py with PipelineOrchestratorInterface")
    else:
        print("Could not find __all__ start")
else:
    print("PipelineOrchestratorInterface already in __init__.py")