"""
SourceDescriptor Pydantic model for standardizing dataset location type and preferred execution engine.
"""
from __future__ import annotations

import os
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel


class SourceType(str, Enum):
    AZURE_SQL = "AZURE_SQL"
    SQL_SERVER = "SQL_SERVER"
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    BLOB_CSV = "BLOB_CSV"
    BLOB_PARQUET = "BLOB_PARQUET"
    BLOB_JSON = "BLOB_JSON"
    LOCAL_CSV = "LOCAL_CSV"
    LOCAL_PARQUET = "LOCAL_PARQUET"
    LOCAL_JSON = "LOCAL_JSON"
    LOCAL_EXCEL = "LOCAL_EXCEL"
    FABRIC_DELTA = "FABRIC_DELTA"
    STREAM = "STREAM"
    UNKNOWN = "UNKNOWN"


class PreferredEngine(str, Enum):
    AZURE_SQL = "AZURE_SQL"
    FABRIC_PYSPARK = "FABRIC_PYSPARK"
    FABRIC_WAREHOUSE = "FABRIC_WAREHOUSE"
    LOCAL_PANDAS = "LOCAL_PANDAS"


class SourceDescriptor(BaseModel):
    dataset_name: str
    source_type: SourceType
    physical_location: str
    preferred_engine: PreferredEngine
    extension: str
    row_count: int = 0
    size_mb: float = 0.0
    connection_config: Optional[Dict[str, Any]] = None

    @classmethod
    def from_location_dict(cls, loc: Dict[str, Any], dataset_name: str) -> SourceDescriptor:
        raw_type = str(loc.get("type") or "").lower().strip()
        conn = loc.get("connection") or {}
        path = loc.get("path") or dataset_name
        ext = os.path.splitext(dataset_name)[1].lower()

        # Defaults
        stype = SourceType.UNKNOWN
        pengine = PreferredEngine.LOCAL_PANDAS
        phys_loc = path

        if raw_type == "database":
            driver = str(conn.get("driver") or "").lower()
            server = str(conn.get("server") or "").lower()
            if "azure" in server or "database.windows.net" in server:
                stype = SourceType.AZURE_SQL
                pengine = PreferredEngine.AZURE_SQL
            elif "postgres" in driver:
                stype = SourceType.POSTGRES
                pengine = PreferredEngine.LOCAL_PANDAS
            elif "mysql" in driver:
                stype = SourceType.MYSQL
                pengine = PreferredEngine.LOCAL_PANDAS
            else:
                stype = SourceType.SQL_SERVER
                pengine = PreferredEngine.AZURE_SQL
            phys_loc = f"{conn.get('server', '')}.{conn.get('database', '')}.{dataset_name}"

        elif raw_type == "azure_blob":
            if ext in (".csv", ".tsv"):
                stype = SourceType.BLOB_CSV
            elif ext == ".parquet":
                stype = SourceType.BLOB_PARQUET
            elif ext in (".json", ".jsonl"):
                stype = SourceType.BLOB_JSON
            else:
                stype = SourceType.BLOB_CSV
            
            if "fabric" in path or "abfss://" in path:
                pengine = PreferredEngine.FABRIC_PYSPARK
            else:
                pengine = PreferredEngine.LOCAL_PANDAS
            phys_loc = path

        elif raw_type in ("filesystem", "local_fs"):
            if ext in (".csv", ".tsv"):
                stype = SourceType.LOCAL_CSV
            elif ext == ".parquet":
                stype = SourceType.LOCAL_PARQUET
            elif ext in (".json", ".jsonl"):
                stype = SourceType.LOCAL_JSON
            elif ext in (".xlsx", ".xls"):
                stype = SourceType.LOCAL_EXCEL
            else:
                stype = SourceType.LOCAL_CSV
            pengine = PreferredEngine.LOCAL_PANDAS
            phys_loc = os.path.join(path, dataset_name) if path and path != dataset_name else dataset_name

        elif raw_type == "stream":
            stype = SourceType.STREAM
            pengine = PreferredEngine.LOCAL_PANDAS
            phys_loc = "stream"

        else:
            if ext in (".csv", ".tsv"):
                stype = SourceType.LOCAL_CSV
            elif ext == ".parquet":
                stype = SourceType.LOCAL_PARQUET
            elif ext in (".json", ".jsonl"):
                stype = SourceType.LOCAL_JSON
            elif ext in (".xlsx", ".xls"):
                stype = SourceType.LOCAL_EXCEL
            else:
                stype = SourceType.UNKNOWN
            pengine = PreferredEngine.LOCAL_PANDAS
            phys_loc = dataset_name

        return cls(
            dataset_name=dataset_name,
            source_type=stype,
            physical_location=phys_loc,
            preferred_engine=pengine,
            extension=ext,
            connection_config=conn or None
        )

    @property
    def requires_fabric_mirroring(self) -> bool:
        """Returns True if pre-assessment Fabric mirroring/shortcutting is required (Blob/Files)."""
        return self.source_type in (
            SourceType.BLOB_CSV,
            SourceType.BLOB_PARQUET,
            SourceType.BLOB_JSON,
        ) or self.preferred_engine in (
            PreferredEngine.FABRIC_PYSPARK,
            PreferredEngine.FABRIC_WAREHOUSE,
        )

