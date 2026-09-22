"""
SQL Templates package for Agent Dhara ETL pipeline.
Provides canonical, reusable SQL schemas and fragments.
"""

from agent.etl_pipeline.sql_templates.infrastructure import (
    ETL_DEFAULT_VALUES_SCHEMA,
    ETL_INVALID_VALUES_SCHEMA,
    ETL_LOG_SCHEMA,
    ETL_REJECTS_SCHEMA,
    ETL_REJECT_SCHEMA,
    ETL_WATERMARK_SCHEMA,
    get_infrastructure_ddl,
)

__all__ = [
    "ETL_LOG_SCHEMA",
    "ETL_REJECT_SCHEMA",
    "ETL_REJECTS_SCHEMA",
    "ETL_WATERMARK_SCHEMA",
    "ETL_DEFAULT_VALUES_SCHEMA",
    "ETL_INVALID_VALUES_SCHEMA",
    "get_infrastructure_ddl",
]
