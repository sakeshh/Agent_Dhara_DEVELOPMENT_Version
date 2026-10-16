#!/usr/bin/env python
with open('agent/core/interfaces/orchestration.py', 'r') as f:
    lines = f.readlines()

# Find the line with "# Backwards compatibility exports"
insert_idx = None
for i, line in enumerate(lines):
    if '# Backwards compatibility exports' in line:
        insert_idx = i
        break

if insert_idx is None:
    print("ERROR: Marker not found")
    sys.exit(1)

# Insert the new class before the marker
new_class_lines = [
    "class PipelineOrchestratorInterface:\n",
    '    """Interface for pipeline orchestrator implementations."""\n',
    "    async def run_assessment(self, context: Any) -> Any:\n",
    '        """Run assessment on dataset."""\n',
    "        raise NotImplementedError\n",
    "    async def generate_plan(self, assessment: Any) -> Any:\n",
    '        """Generate ETL plan from assessment."""\n',
    "        raise NotImplementedError\n",
    "    async def generate_code(self, plan: Any) -> Any:\n",
    '        """Generate code from ETL plan."""\n',
    "        raise NotImplementedError\n",
    "    async def validate_code(self, code: Any, plan: Any) -> Any:\n",
    '        """Validate generated code."""\n',
    "        raise NotImplementedError\n",
    "    async def execute_plan(self, plan: Any) -> Any:\n",
    '        """Execute the ETL plan."""\n',
    "        raise NotImplementedError\n",
    "\n",
]

# Insert the new class lines
new_lines = lines[:insert_idx] + new_class_lines + lines[insert_idx:]

with open('agent/core/interfaces/orchestration.py', 'w') as f:
    f.writelines(new_lines)

print("Successfully added PipelineOrchestratorInterface")