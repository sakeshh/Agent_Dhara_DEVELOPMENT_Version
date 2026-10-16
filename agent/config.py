"""Configuration management for Agent Dhara."""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from pathlib import Path
import yaml


@dataclass
class AppConfig:
    """Main application configuration."""
    
    # Environment
    environment: str = "development"  # development, staging, production
    debug: bool = False
    log_level: str = "INFO"
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_timeout: int = 300
    
    # Authentication
    auth_enabled: bool = True
    auth_provider: str = "azure_ad"  # azure_ad, jwt, api_key
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 60
    
    # Database
    database_url: str = "postgresql://localhost/agent_dhara"
    database_pool_size: int = 20
    database_max_overflow: int = 10
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50
    
    # Object Storage
    storage_type: str = "minio"  # minio, s3, azure_blob
    storage_endpoint: str = "http://localhost:9000"
    storage_access_key: Optional[str] = None
    storage_secret_key: Optional[str] = None
    storage_bucket: str = "agent-dhara"
    storage_region: str = "us-east-1"
    
    # Azure
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_subscription_id: Optional[str] = None
    
    # Fabric
    fabric_workspace_id: Optional[str] = None
    fabric_lakehouse_name: Optional[str] = None
    
    # LLM
    llm_provider: str = "azure_openai"  # azure_openai, openai, anthropic, local
    llm_api_key: Optional[str] = None
    llm_endpoint: Optional[str] = None
    llm_model: str = "gpt-4"
    llm_temperature: float = 0.1
    llm_max_tokens: int = 4000
    llm_timeout: int = 120
    
    # Feature Flags
    features: Dict[str, bool] = field(default_factory=lambda: {
        "assessment_swarm": True,
        "business_logic_discovery": True,
        "incremental_cdc": True,
        "delta_cdf": True,
        "multi_target_deployment": True,
        "human_gates": True,
        "scheduler": True,
        "data_contracts": True,
        "governance": True,
        "learning": True,
        "reverse_etl": False,
        "streaming": False,
    })
    
    # Assessment
    assessment_max_rows: int = 100000
    assessment_sample_size: int = 10000
    assessment_parallelism: int = 4
    assessment_timeout: int = 600
    
    # ETL Pipeline
    pipeline_max_parallel: int = 8
    pipeline_timeout: int = 3600
    pipeline_retry_attempts: int = 3
    pipeline_checkpoint_interval: int = 100
    
    # Incremental
    incremental_default_lookback: str = "7d"
    incremental_max_lookback: str = "90d"
    incremental_batch_size: int = 10000
    
    # Scheduler
    scheduler_enabled: bool = True
    scheduler_poll_interval: int = 60
    scheduler_max_concurrent: int = 50
    
    # Human Gates
    gate_timeout_hours: int = 24
    gate_notification_webhook: Optional[str] = None
    gate_default_assignee: Optional[str] = None
    
    # Memory
    memory_hot_ttl_hours: int = 24
    memory_warm_ttl_days: int = 30
    memory_cold_ttl_days: int = 365
    memory_max_records: int = 100000
    
    # Governance
    pii_detection_enabled: bool = True
    lineage_enabled: bool = True
    audit_enabled: bool = True
    data_contracts_enabled: bool = True
    
    # Deployment
    default_target: str = "azure_sql"
    supported_targets: List[str] = field(default_factory=lambda: [
        "azure_sql", "fabric_pyspark", "azure_data_factory", "snowflake", "databricks"
    ])
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9090
    tracing_enabled: bool = True
    tracing_sample_rate: float = 0.1
    
    # Cost
    cost_tracking_enabled: bool = True
    cost_alert_threshold_usd: float = 100.0


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """Load configuration from file and environment variables."""
    config = AppConfig()
    
    # Load from YAML file if provided
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            file_config = yaml.safe_load(f)
            if file_config:
                _apply_config(config, file_config)
    
    # Override with environment variables
    _apply_env_overrides(config)
    
    return config


def _apply_config(config: AppConfig, data: Dict[str, Any]) -> None:
    """Apply configuration from dictionary."""
    for key, value in data.items():
        if hasattr(config, key):
            if isinstance(getattr(config, key), dict) and isinstance(value, dict):
                getattr(config, key).update(value)
            elif isinstance(getattr(config, key), list) and isinstance(value, list):
                setattr(config, key, value)
            else:
                setattr(config, key, value)


def _apply_env_overrides(config: AppConfig) -> None:
    """Apply environment variable overrides."""
    env_mapping = {
        "AGENT_DHARA_ENV": "environment",
        "AGENT_DHARA_DEBUG": ("debug", lambda x: x.lower() == "true"),
        "AGENT_DHARA_LOG_LEVEL": "log_level",
        "AGENT_DHARA_API_HOST": "api_host",
        "AGENT_DHARA_API_PORT": ("api_port", int),
        "AGENT_DHARA_DATABASE_URL": "database_url",
        "AGENT_DHARA_REDIS_URL": "redis_url",
        "AGENT_DHARA_STORAGE_TYPE": "storage_type",
        "AGENT_DHARA_STORAGE_ENDPOINT": "storage_endpoint",
        "AGENT_DHARA_STORAGE_ACCESS_KEY": "storage_access_key",
        "AGENT_DHARA_STORAGE_SECRET_KEY": "storage_secret_key",
        "AGENT_DHARA_STORAGE_BUCKET": "storage_bucket",
        "AGENT_DHARA_AZURE_TENANT_ID": "azure_tenant_id",
        "AGENT_DHARA_AZURE_CLIENT_ID": "azure_client_id",
        "AGENT_DHARA_AZURE_CLIENT_SECRET": "azure_client_secret",
        "AGENT_DHARA_LLM_PROVIDER": "llm_provider",
        "AGENT_DHARA_LLM_API_KEY": "llm_api_key",
        "AGENT_DHARA_LLM_ENDPOINT": "llm_endpoint",
        "AGENT_DHARA_LLM_MODEL": "llm_model",
        "AGENT_DHARA_DEFAULT_TARGET": "default_target",
    }
    
    for env_var, config_attr in env_mapping.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            if isinstance(config_attr, tuple):
                attr_name, converter = config_attr
                setattr(config, attr_name, converter(env_value))
            else:
                setattr(config, config_attr, env_value)
    
    # Feature flags from env
    for feature in config.features:
        env_var = f"AGENT_DHARA_FEATURE_{feature.upper()}"
        env_value = os.getenv(env_var)
        if env_value is not None:
            config.features[feature] = env_value.lower() == "true"


def get_config() -> AppConfig:
    """Get global config instance (singleton)."""
    global _config
    if _config is None:
        config_path = os.getenv("AGENT_DHARA_CONFIG_PATH")
        _config = load_config(config_path)
    return _config


_config: Optional[AppConfig] = None


# Convenience functions
def is_feature_enabled(feature: str) -> bool:
    return get_config().features.get(feature, False)


def get_target_config(target: str) -> Dict[str, Any]:
    """Get target-specific configuration."""
    config = get_config()
    return {
        "target": target,
        "connection": {},  # Would be loaded from secrets
        "options": config.features,
    }


# Configuration validation
def validate_config(config: AppConfig) -> List[str]:
    """Validate configuration. Returns list of errors."""
    errors = []
    
    if config.environment == "production":
        if not config.auth_enabled:
            errors.append("Authentication must be enabled in production")
        if config.debug:
            errors.append("Debug mode should be disabled in production")
        if not config.jwt_secret:
            errors.append("JWT secret required in production")
    
    if config.features.get("incremental_cdc") and not config.features.get("scheduler_enabled"):
        errors.append("CDC requires scheduler to be enabled")
    
    if config.features.get("multi_target_deployment"):
        if not config.supported_targets:
            errors.append("At least one target must be specified for multi-target deployment")
    
    return errors