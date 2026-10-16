"""Connector Interfaces - Source-agnostic data access."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, AsyncIterator, Union
from datetime import datetime
import pandas as pd


class SourceType(Enum):
    """Supported data source types."""
    AZURE_SQL = "azure_sql"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    AZURE_BLOB = "azure_blob"
    ADLS_GEN2 = "adls_gen2"
    S3 = "s3"
    LOCAL_FILESYSTEM = "local_filesystem"
    DELTA_LAKE = "delta_lake"
    FABRIC_LAKEHOUSE = "fabric_lakehouse"
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    KAFKA = "kafka"
    EVENT_HUBS = "event_hubs"
    COSMOS_DB = "cosmos_db"
    DYNAMODB = "dynamodb"
    SALESFORCE = "salesforce"
    DYNAMICS = "dynamics"


class FileFormat(Enum):
    """Supported file formats."""
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    EXCEL = "excel"
    XML = "xml"
    DELTA = "delta"


@dataclass
class ConnectorConfig:
    """Configuration for a data connector."""
    source_type: SourceType
    connection_string: Optional[str] = None
    
    # Common
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Cloud storage
    account_name: Optional[str] = None
    account_key: Optional[str] = None
    container: Optional[str] = None
    bucket: Optional[str] = None
    region: Optional[str] = None
    
    # API
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    auth_type: Optional[str] = None  # "bearer", "basic", "oauth", "api_key"
    oauth_config: Dict[str, Any] = field(default_factory=dict)
    
    # Kafka/Streaming
    bootstrap_servers: Optional[str] = None
    consumer_group: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    # Delta Lake
    delta_table_path: Optional[str] = None
    
    # Options
    options: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class TableInfo:
    """Metadata about a table/dataset."""
    name: str
    schema: Optional[str] = None
    catalog: Optional[str] = None
    table_type: str = "TABLE"  # TABLE, VIEW, EXTERNAL_TABLE, DELTA_TABLE
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    columns: List["ColumnInfo"] = field(default_factory=list)
    partitions: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    last_modified: Optional[datetime] = None
    
    @property
    def full_name(self) -> str:
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.name)
        return ".".join(parts)


@dataclass
class ColumnInfo:
    """Metadata about a column."""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    comment: Optional[str] = None
    # Statistics
    null_count: Optional[int] = None
    distinct_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class FileInfo:
    """Metadata about a file."""
    path: str
    name: str
    size_bytes: int
    format: Optional[FileFormat] = None
    modified_at: Optional[datetime] = None
    hash: Optional[str] = None
    schema: Optional[List[ColumnInfo]] = None
    row_count: Optional[int] = None
    partitions: Dict[str, str] = field(default_factory=dict)  # Hive-style partition keys


class ConnectorBase(ABC):
    """Base class for all data connectors."""
    
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._connected = False
    
    @property
    @abstractmethod
    def source_type(self) -> SourceType:
        pass
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """Close connection."""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test if connection is alive."""
        pass
    
    @abstractmethod
    async def list_datasets(self, schema: Optional[str] = None) -> List[TableInfo]:
        """List available tables/datasets."""
        pass
    
    @abstractmethod
    async def get_dataset_info(self, name: str, schema: Optional[str] = None) -> TableInfo:
        """Get detailed metadata for a dataset."""
        pass
    
    @abstractmethod
    async def read_data(
        self, 
        dataset: str, 
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        incremental_config: Optional[Any] = None
    ) -> pd.DataFrame:
        """Read data from dataset. Supports incremental via config."""
        pass
    
    @abstractmethod
    async def write_data(
        self, 
        dataset: str, 
        data: pd.DataFrame,
        schema: Optional[str] = None,
        mode: str = "append",  # append, overwrite, merge
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Write data to dataset."""
        pass
    
    @abstractmethod
    async def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute arbitrary SQL (for SQL sources)."""
        pass
    
    # Incremental methods
    async def get_incremental_state(self, dataset: str) -> Any:
        """Get last incremental state."""
        return None
    
    async def fetch_changes(self, dataset: str, since: Any) -> pd.DataFrame:
        """Fetch incremental changes."""
        return pd.DataFrame()
    
    async def write_changes(self, dataset: str, changes: pd.DataFrame, mode: str = "merge") -> bool:
        """Write incremental changes."""
        return await self.write_data(dataset, changes, mode=mode)
    
    async def get_current_state(self, dataset: str) -> Any:
        """Get current state after write."""
        return None


# File-based connectors
class FileConnectorBase(ConnectorBase):
    """Base for file-based connectors (Blob, ADLS, S3, Local)."""
    
    @abstractmethod
    async def list_files(
        self, 
        prefix: str = "", 
        recursive: bool = True,
        file_format: Optional[FileFormat] = None
    ) -> List[FileInfo]:
        pass
    
    @abstractmethod
    async def read_file(
        self, 
        path: str, 
        format: Optional[FileFormat] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        pass
    
    @abstractmethod
    async def write_file(
        self, 
        path: str, 
        data: pd.DataFrame,
        format: FileFormat = FileFormat.PARQUET,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> bool:
        pass
    
    @abstractmethod
    async def delete_file(self, path: str) -> bool:
        pass
    
    @abstractmethod
    async def file_exists(self, path: str) -> bool:
        pass
    
    @abstractmethod
    async def get_file_hash(self, path: str, algorithm: str = "md5") -> str:
        pass


# Streaming connectors
class StreamingConnectorBase(ConnectorBase):
    """Base for streaming connectors (Kafka, Event Hubs, etc.)."""
    
    @abstractmethod
    async def subscribe(self, topics: List[str], from_beginning: bool = False) -> bool:
        pass
    
    @abstractmethod
    async def poll(self, timeout_ms: int = 1000, max_records: int = 1000) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def commit_offsets(self) -> bool:
        pass
    
    @abstractmethod
    async def seek_to_offset(self, topic: str, partition: int, offset: int) -> bool:
        pass
    
    @abstractmethod
    async def get_current_offsets(self) -> Dict[str, int]:
        pass


# API connectors
class APIConnectorBase(ConnectorBase):
    """Base for API connectors (REST, GraphQL)."""
    
    @abstractmethod
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def put(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def delete(self, endpoint: str) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def paginate(
        self, 
        endpoint: str, 
        cursor: Optional[str] = None,
        page_size: int = 100,
        params: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def upsert(self, endpoint: str, key: str, data: Dict[str, Any]) -> bool:
        pass


# Connector factory
class ConnectorFactory:
    """Factory for creating connectors from config."""
    
    _connectors: Dict[SourceType, type] = {}
    
    @classmethod
    def register(cls, source_type: SourceType, connector_class: type):
        cls._connectors[source_type] = connector_class
    
    @classmethod
    def create(cls, config: ConnectorConfig) -> ConnectorBase:
        connector_class = cls._connectors.get(config.source_type)
        if not connector_class:
            raise ValueError(f"No connector registered for {config.source_type}")
        return connector_class(config)
    
    @classmethod
    def create_from_dict(cls, config_dict: Dict[str, Any]) -> ConnectorBase:
        config = ConnectorConfig(**config_dict)
        return cls.create(config)


# Register built-in connectors (imports would be here in real implementation)
# from agent.connectors.azure_sql import AzureSQLConnector
# ConnectorFactory.register(SourceType.AZURE_SQL, AzureSQLConnector)
# etc.