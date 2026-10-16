#!/usr/bin/env python
"""Add PipelineConfig to orchestrator.py"""

with open('agent/etl_pipeline/orchestrator.py', 'r') as f:
    content = f.read()

new_class = """@dataclass
class PipelineConfig:
    """Configuration for the ETL pipeline orchestrator."""
    # Feature flags
    enable_incremental: bool = True
    enable_semantic_refinement: bool = True
    enable_cross_dataset: bool = True
    auto_approve_low_risk: bool = True
    validation_timeout_seconds: int = 60
    
    # Platform settings
    target_platform: str = "python"  # python, spark, sql, trino, etc.
    dialect: str = "tsql"
    
    # Execution settings
    max_retries: int = 3
    timeout_minutes: int = 30
    fail_fast: bool = True
    
    # Quality thresholds
    min_issue_coverage: float = 0.7
    min_semantic_coverage: float = 0.6
"""

# Insert before the ETLPipelineOrchestrator class
insert_pos = content.find("class ETLPipelineOrchestrator")
if insert_pos >= 0:
    content = content[:insert_pos] + new_class + content[insert_pos:]
    with open("agent/etl_pipeline/orchestrator.py", "w") as f:
        f.write(content)
    print("Added PipelineConfig to orchestrator.py")
else:
    print("Could not find ETLPipelineOrchestrator class")