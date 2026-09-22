"""
Canonical infrastructure schemas for Agent Dhara ETL SQL generation.
Single source of truth for all operational/telemetry tables.
"""

from __future__ import annotations

from typing import List, Optional, Set

# Canonical DDL for dbo.etl_log
ETL_LOG_SCHEMA = """IF OBJECT_ID('dbo.etl_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME NULL,
        status VARCHAR(20) NOT NULL,
        error_message VARCHAR(MAX) NULL
    );
END;"""

# Canonical DDL for dbo.etl_rejects
ETL_REJECT_SCHEMA = """IF OBJECT_ID('dbo.etl_rejects', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_rejects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        row_data VARCHAR(MAX) NOT NULL,
        error_reason VARCHAR(MAX) NOT NULL,
        rejected_at DATETIME DEFAULT GETDATE()
    );
END;"""

# Alias for compatibility
ETL_REJECTS_SCHEMA = ETL_REJECT_SCHEMA

# Canonical DDL for dbo.etl_watermark
ETL_WATERMARK_SCHEMA = """IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_watermark (
        process_name VARCHAR(256) PRIMARY KEY,
        last_run_time DATETIME NOT NULL
    );
END;"""

# Canonical DDL for dbo.etl_default_values
ETL_DEFAULT_VALUES_SCHEMA = """IF OBJECT_ID('dbo.etl_default_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_default_values (
        column_name VARCHAR(256) PRIMARY KEY,
        default_value VARCHAR(256) NOT NULL,
        data_type VARCHAR(50) NOT NULL
    );
END;"""

# Canonical DDL for dbo.etl_invalid_values
ETL_INVALID_VALUES_SCHEMA = """IF OBJECT_ID('dbo.etl_invalid_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_invalid_values (
        column_name VARCHAR(256),
        invalid_value VARCHAR(256),
        PRIMARY KEY (column_name, invalid_value)
    );
END;"""


def get_infrastructure_ddl(
    *,
    include_rejects: bool = True,
    include_watermark: bool = True,
    include_defaults: bool = False,
    include_invalid: bool = True,
    dialect: str = "tsql",
    include_go: bool = True,
    emitted_objects: Optional[Set[str]] = None,
) -> str:
    """
    Generate canonical DDL statements for operational infrastructure tables.
    Tracks emitted objects to prevent duplicate table generation.
    """
    if dialect.lower() != "tsql":
        return ""

    if emitted_objects is None:
        emitted_objects = set()

    blocks: List[str] = []
    separator = "\nGO\n\n" if include_go else "\n\n"

    if "dbo.etl_log" not in emitted_objects:
        blocks.append(ETL_LOG_SCHEMA)
        emitted_objects.add("dbo.etl_log")

    if include_defaults and "dbo.etl_default_values" not in emitted_objects:
        blocks.append(ETL_DEFAULT_VALUES_SCHEMA)
        emitted_objects.add("dbo.etl_default_values")

    if include_invalid and "dbo.etl_invalid_values" not in emitted_objects:
        blocks.append(ETL_INVALID_VALUES_SCHEMA)
        emitted_objects.add("dbo.etl_invalid_values")

    if include_rejects and "dbo.etl_rejects" not in emitted_objects:
        blocks.append(ETL_REJECT_SCHEMA)
        emitted_objects.add("dbo.etl_rejects")

    if include_watermark and "dbo.etl_watermark" not in emitted_objects:
        blocks.append(ETL_WATERMARK_SCHEMA)
        emitted_objects.add("dbo.etl_watermark")

    if not blocks:
        return ""

    result = separator.join(blocks)
    if include_go:
        result += "\nGO\n"
    return result
