#!/usr/bin/env python
"""Remove SchedulerInterface from core __init__.py"""

with open('agent/core/__init__.py', 'r') as f:
    content = f.read()

# Remove SchedulerInterface import
content = content.replace('    SchedulerInterface,\n', '')

# Remove SchedulerInterface from __all__
content = content.replace('    "SchedulerInterface",\n', '')

with open('agent/core/__init__.py', 'w') as f:
    f.write(content)
print("Removed SchedulerInterface from core __init__.py")