"""Deployment Interfaces - Multi-target code generation and execution."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from datetime import datetime


class DeploymentTarget(Enum):
    """Supported deployment targets."""
    AZURE_SQL = "azure_sql"
    FABRIC_PYSPARK = "fabric_pyspark"
    FABRIC_SQL = "fabric_sql"
    AZURE_DATA_FACTORY = "azure_data_factory"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    LOCAL_PYTHON = "local_python"
    LOCAL_SQL = "local_sql"


class CodeDialect(Enum):
    """Code generation dialects."""
    TSQL = "tsql"
    PYSPARK = "pyspark"
    ADF_JSON = "adf_json"
    SNOWFLAKE_SQL = "snowflake_sql"
    DATABRICKS_SQL = "databricks_sql"
    POSTGRESQL_SQL = "postgresql_sql"
    MYSQL_SQL = "mysql_sql"
    ANSISQL = "ansi_sql"
    PYTHON_PANDAS = "python_pandas"


@dataclass
class TargetCapabilities:
    """Capabilities of a deployment target."""
    target: DeploymentTarget
    dialect: CodeDialect
    
    # SQL features
    supports_merge: bool = True
    supports_try_cast: bool = True
    supports_cte: bool = True
    supports_window_functions: bool = True
    supports_pivot: bool = False
    
    # Transaction
    supports_transactions: bool = True
    supports_savepoints: bool = False
    
    # DDL
    supports_partitioning: bool = True
    supports_clustering: bool = False
    supports_identity: bool = True
    supports_sequence: bool = False
    
    # Incremental
    supports_cdc: bool = False
    supports_delta_cdf: bool = False
    supports_scd_type2: bool = True
    
    # Performance
    supports_bulk_insert: bool = True
    supports_parallel_execution: bool = False
    max_batch_size: int = 10000
    
    # Features
    supports_temporal_tables: bool = False
    supports_json: bool = True
    supports_arrays: bool = False
    supports_structs: bool = False
    
    # Limits
    max_identifier_length: int = 128
    max_query_length: int = 65536


@dataclass
class DeploymentConfig:
    """Configuration for a deployment target."""
    target: DeploymentTarget
    connection_config: Dict[str, Any] = field(default_factory=dict)
    
    # Code generation options
    include_comments: bool = True
    include_logging: bool = True
    include_error_handling: bool = True
    batch_size: int = 10000
    
    # Naming conventions
    schema_prefix: str = ""
    table_prefix: str = ""
    temp_table_prefix: str = "tmp_"
    staging_schema: str = "staging"
    
    # Partitioning
    default_partition_strategy: str = "by_day"
    partition_columns: List[str] = field(default_factory=list)
    
    # Incremental
    default_scd_type: int = 2
    watermark_column_default: str = "updated_at"
    
    # Performance
    parallelism: int = 4
    memory_per_executor: str = "4g"
    executor_cores: int = 2
    
    # Options
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CodeArtifact:
    """Generated code artifact."""
    artifact_id: str
    target: DeploymentTarget
    dialect: CodeDialect
    
    # Content
    code: str
    files: Dict[str, str] = field(default_factory=dict)  # For multi-file outputs (ADF)
    
    # Metadata
    generated_at: datetime = field(default_factory=datetime.utcnow)
    generated_by: str = "agent_dhara"
    version: str = "1.0.0"
    
    # Validation
    syntax_valid: bool = False
    semantic_valid: bool = False
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    # Lineage
    plan_id: Optional[str] = None
    plan_version: Optional[str] = None
    source_columns: Dict[str, List[str]] = field(default_factory=dict)  # target_col -> source_cols
    
    # Metadata
    lines_of_code: int = 0
    estimated_runtime_seconds: Optional[float] = None
    estimated_cost_usd: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result


@dataclass
class ValidationResult:
    """Result of code validation."""
    passed: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    fixed_code: Optional[str] = None
    validation_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentResult:
    """Result of deployment execution."""
    success: bool
    target: DeploymentTarget
    run_id: str
    
    # Execution
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    
    # Results
    rows_affected: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    partitions_processed: List[str] = field(default_factory=list)
    
    # Errors
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Artifacts
    output_files: List[str] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    
    # Metrics
    metrics: Dict[str, float] = field(default_factory=dict)
    
    # Rollback
    rollback_available: bool = False
    rollback_id: Optional[str] = None


class CodeGeneratorInterface(ABC):
    """Interface for code generation per target."""
    
    @property
    @abstractmethod
    def target(self) -> DeploymentTarget:
        pass
    
    @property
    @abstractmethod
    def dialect(self) -> CodeDialect:
        pass
    
    @property
    @abstractmethod
    def capabilities(self) -> TargetCapabilities:
        pass
    
    @abstractmethod
    async def generate(
        self, 
        plan: Any, 
        config: DeploymentConfig
    ) -> CodeArtifact:
        """Generate code artifact from plan."""
        pass
    
    @abstractmethod
    async def validate(self, artifact: CodeArtifact) -> ValidationResult:
        """Validate generated code."""
        pass
    
    @abstractmethod
    async def auto_fix(
        self, 
        artifact: CodeArtifact, 
        validation: ValidationResult
    ) -> Optional[CodeArtifact]:
        """Attempt to auto-fix validation errors."""
        pass
    
    @abstractmethod
    def get_template(self, template_name: str) -> str:
        """Get a template by name."""
        pass
    
    @abstractmethod
    def register_template(self, name: str, template: str) -> None:
        """Register a custom template."""
        pass


class CodeValidatorInterface(ABC):
    """Interface for code validation."""
    
    @abstractmethod
    async def validate_syntax(self, code: str, dialect: CodeDialect) -> ValidationResult:
        pass
    
    @abstractmethod
    async def validate_semantics(
        self, 
        code: str, 
        plan: Any, 
        dialect: CodeDialect
    ) -> ValidationResult:
        pass
    
    @abstractmethod
    async def validate_references(
        self, 
        code: str, 
        plan: Any, 
        dialect: CodeDialect
    ) -> ValidationResult:
        pass
    
    @abstractmethod
    async def dry_run(
        self, 
        code: str, 
        plan: Any, 
        dialect: CodeDialect,
        connection_config: Dict[str, Any]
    ) -> ValidationResult:
        pass


class DeploymentExecutorInterface(ABC):
    """Interface for executing deployed code."""
    
    @property
    @abstractmethod
    def target(self) -> DeploymentTarget:
        pass
    
    @abstractmethod
    async def deploy(
        self, 
        artifact: CodeArtifact, 
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Execute the code artifact."""
        pass
    
    @abstractmethod
    async def dry_run(
        self, 
        artifact: CodeArtifact, 
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Simulate execution without committing."""
        pass
    
    @abstractmethod
    async def rollback(
        self, 
        run_id: str, 
        config: DeploymentConfig
    ) -> DeploymentResult:
        """Rollback a deployment."""
        pass
    
    @abstractmethod
    async def get_deployment_status(self, run_id: str) -> Optional[DeploymentResult]:
        pass
    
    @abstractmethod
    async def cancel_deployment(self, run_id: str) -> bool:
        pass


class DeploymentManager:
    """Manages multi-target deployments."""
    
    def __init__(self):
        self.generators: Dict[DeploymentTarget, CodeGeneratorInterface] = {}
        self.executors: Dict[DeploymentTarget, DeploymentExecutorInterface] = {}
        self.validator: Optional[CodeValidatorInterface] = None
        self.configs: Dict[DeploymentTarget, DeploymentConfig] = {}
    
    def register_generator(self, generator: CodeGeneratorInterface):
        self.generators[generator.target] = generator
    
    def register_executor(self, executor: DeploymentExecutorInterface):
        self.executors[executor.target] = executor
    
    def set_validator(self, validator: CodeValidatorInterface):
        self.validator = validator
    
    def set_config(self, target: DeploymentTarget, config: DeploymentConfig):
        self.configs[target] = config
    
    async def generate_for_target(
        self, 
        target: DeploymentTarget, 
        plan: Any, 
        config: Optional[DeploymentConfig] = None
    ) -> CodeArtifact:
        generator = self.generators.get(target)
        if not generator:
            raise ValueError(f"No generator registered for {target}")
        
        cfg = config or self.configs.get(target, DeploymentConfig(target=target))
        artifact = await generator.generate(plan, cfg)
        
        # Validate if validator available
        if self.validator:
            validation = await self.validator.validate_syntax(artifact.code, artifact.dialect)
            artifact.syntax_valid = validation.passed
            artifact.validation_errors = validation.errors
            artifact.validation_warnings = validation.warnings
            
            if not validation.passed and validation.fixed_code:
                # Try auto-fix
                fixed_artifact = await generator.auto_fix(artifact, validation)
                if fixed_artifact:
                    artifact = fixed_artifact
        
        return artifact
    
    async def generate_all(
        self, 
        plan: Any, 
        targets: List[DeploymentTarget],
        configs: Optional[Dict[DeploymentTarget, DeploymentConfig]] = None
    ) -> Dict[DeploymentTarget, CodeArtifact]:
        """Generate code for multiple targets."""
        results = {}
        for target in targets:
            cfg = configs.get(target) if configs else self.configs.get(target)
            results[target] = await self.generate_for_target(target, plan, cfg)
        return results
    
    async def deploy(
        self, 
        target: DeploymentTarget, 
        artifact: CodeArtifact, 
        config: Optional[DeploymentConfig] = None
    ) -> DeploymentResult:
        executor = self.executors.get(target)
        if not executor:
            raise ValueError(f"No executor registered for {target}")
        
        cfg = config or self.configs.get(target, DeploymentConfig(target=target))
        return await executor.deploy(artifact, cfg)
    
    async def deploy_all(
        self, 
        artifacts: Dict[DeploymentTarget, CodeArtifact],
        configs: Optional[Dict[DeploymentTarget, DeploymentConfig]] = None
    ) -> Dict[DeploymentTarget, DeploymentResult]:
        results = {}
        for target, artifact in artifacts.items():
            cfg = configs.get(target) if configs else self.configs.get(target)
            results[target] = await self.deploy(target, artifact, cfg)
        return results


# Target capability registry
TARGET_CAPABILITIES: Dict[DeploymentTarget, TargetCapabilities] = {
    DeploymentTarget.AZURE_SQL: TargetCapabilities(
        target=DeploymentTarget.AZURE_SQL,
        dialect=CodeDialect.TSQL,
        supports_merge=True,
        supports_try_cast=True,
        supports_cte=True,
        supports_window_functions=True,
        supports_pivot=True,
        supports_transactions=True,
        supports_savepoints=True,
        supports_partitioning=True,
        supports_identity=True,
        supports_sequence=True,
        supports_cdc=True,
        supports_scd_type2=True,
        supports_bulk_insert=True,
        supports_temporal_tables=True,
        supports_json=True,
        max_identifier_length=128,
    ),
    DeploymentTarget.FABRIC_PYSPARK: TargetCapabilities(
        target=DeploymentTarget.FABRIC_PYSPARK,
        dialect=CodeDialect.PYSPARK,
        supports_merge=True,
        supports_try_cast=True,
        supports_cte=True,
        supports_window_functions=True,
        supports_transactions=False,  # Spark doesn't support traditional transactions
        supports_partitioning=True,
        supports_clustering=True,
        supports_delta_cdf=True,
        supports_scd_type2=True,
        supports_bulk_insert=True,
        supports_parallel_execution=True,
        supports_json=True,
        supports_arrays=True,
        supports_structs=True,
    ),
    DeploymentTarget.AZURE_DATA_FACTORY: TargetCapabilities(
        target=DeploymentTarget.AZURE_DATA_FACTORY,
        dialect=CodeDialect.ADF_JSON,
        supports_merge=False,  # Uses Mapping Data Flows
        supports_transactions=False,
        supports_partitioning=True,
        supports_scd_type2=True,
        max_query_length=1000000,
    ),
    DeploymentTarget.SNOWFLAKE: TargetCapabilities(
        target=DeploymentTarget.SNOWFLAKE,
        dialect=CodeDialect.SNOWFLAKE_SQL,
        supports_merge=True,
        supports_try_cast=True,  # TRY_CAST
        supports_cte=True,
        supports_window_functions=True,
        supports_pivot=True,
        supports_transactions=True,
        supports_partitioning=True,
        supports_clustering=True,
        supports_scd_type2=True,
        supports_bulk_insert=True,
        supports_json=True,
        supports_arrays=True,
        supports_structs=True,
    ),
    DeploymentTarget.DATABRICKS: TargetCapabilities(
        target=DeploymentTarget.DATABRICKS,
        dialect=CodeDialect.DATABRICKS_SQL,
        supports_merge=True,
        supports_try_cast=True,
        supports_cte=True,
        supports_window_functions=True,
        supports_partitioning=True,
        supports_clustering=True,
        supports_delta_cdf=True,
        supports_scd_type2=True,
        supports_parallel_execution=True,
    ),
    DeploymentTarget.POSTGRESQL: TargetCapabilities(
        target=DeploymentTarget.POSTGRESQL,
        dialect=CodeDialect.POSTGRESQL_SQL,
        supports_merge=True,  # INSERT ... ON CONFLICT
        supports_try_cast=False,  # Use CAST with exception handling
        supports_cte=True,
        supports_window_functions=True,
        supports_transactions=True,
        supports_savepoints=True,
        supports_partitioning=True,
        supports_sequence=True,
        supports_scd_type2=True,
    ),
}