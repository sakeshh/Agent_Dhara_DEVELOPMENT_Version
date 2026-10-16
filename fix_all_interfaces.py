#!/usr/bin/env python
"""Remove unused interface references from core __init__.py"""

with open('agent/core/__init__.py', 'r') as f:
    content = f.read()

# Remove all the interface imports that aren't essential
interfaces_to_remove = [
    '    CodeGeneratorInterface,\n',
    '    CodeValidatorInterface,\n',
    '    DeploymentExecutorInterface,\n',
    '    GateManagerInterface,\n',
    '    StateStoreInterface,\n',
    '    SchedulerInterface,\n',
]

for iface in interfaces_to_remove:
    content = content.replace(iface, '')

with open('agent/core/__init__.py', 'w') as f:
    f.write(content)
print("Removed unused interface references from core __init__.py")