with open('agent/core/interfaces/orchestration.py', 'r') as f:
    content = f.read()

new_class = """

class PipelineOrchestratorInterface:
    """Interface for pipeline orchestrator implementations."""

    async def run_assessment(self, context: Any) -> Any:
        """Run assessment on dataset."""
        raise NotImplementedError

    async def generate_plan(self, assessment: Any) -> Any:
        """Generate ETL plan from assessment."""
        raise NotImplementedError

    async def generate_code(self, plan: Any) -> Any:
        """Generate code from ETL plan."""
        raise NotImplementedError

    async def validate_code(self, code: Any, plan: Any) -> Any:
        """Validate generated code."""
        raise NotImplementedError

    async def execute_plan(self, plan: Any) -> Any:
        """Execute the ETL plan."""
        raise NotImplementedError
"""

# Add the new class before the backwards compatibility exports
content = content.replace(
    "# Backwards compatibility exports",
    new_class + "# Backwards compatibility exports"
)

with open('agent/core/interfaces/orchestration.py', 'w') as f:
    f.write(content)

print("File updated successfully")