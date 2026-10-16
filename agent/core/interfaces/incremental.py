"""Incremental Processing Interfaces - Source-agnostic change detection and processing."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime


class IncrementalStrategy(Enum):
    """Supported incremental processing strategies."""
    WATERMARK = "watermark"           # Timestamp column tracking
    CDC = "cdc"                       # Change Data Capture (Debezium, SQL CDC)
    FILE_MANIFEST = "file_manifest"   # File listing + hash comparison
    DELTA_CDF = "delta_cdf"           # Delta Lake Change Data Feed
    CURSOR = "cursor"                 # API pagination token
    OFFSET = "offset"                 # Kafka/Event Hubs offset
    CHANGE_FEED = "change_feed"       # Cosmos DB, DynamoDB streams
    FULL_REFRESH = "full_refresh"     # No incremental possible


class WriteMode(Enum):
    """Write modes for incremental processing."""
    APPEND = "append"
    MERGE = "merge"                   # Upsert with business key
    OVERWRITE_PARTITION = "overwrite_partition"
    REPLACE = "replace"


@dataclass
class IncrementalConfig:
    """Configuration for incremental processing - source agnostic."""
    strategy: IncrementalStrategy
    
    # Watermark strategy
    watermark_column: Optional[str] = None
    lookback_window: str = "7d"       # Late arrival handling (ISO duration)
    watermark_format: str = "iso"     # "iso", "unix_ms", "unix_s", "sql_datetime"
    
    # CDC strategy
    cdc_source: Optional[str] = None  # "debezium", "sql_cdc", "native"
    cdc_table: Optional[str] = None
    
    # File-based strategy
    file_pattern: Optional[str] = None
    manifest_path: Optional[str] = None
    file_hash_algorithm: str = "md5"
    
    # Delta Lake strategy
    delta_table_path: Optional[str] = None
    starting_version: Optional[int] = None
    
    # Cursor/API strategy
    cursor_field: Optional[str] = None
    page_size: int = 1000
    rate_limit_rps: Optional[float] = None
    
    # Kafka/Streaming strategy
    consumer_group: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    checkpoint_interval: int = 1000
    
    # General
    partition_strategy: str = "by_day"  # "by_day", "by_month", "by_hour", "by_partition", "none"
    idempotency_key: Optional[str] = None  # Business key for exactly-once
    scd_type: int = 1                   # 1=overwrite, 2=history, 3=current+prev
    write_mode: WriteMode = WriteMode.MERGE
    
    # Schema evolution
    schema_evolution: str = "merge"     # "merge", "overwrite", "fail"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncrementalConfig":
        """Deserialize from dictionary."""
        if "strategy" in data and isinstance(data["strategy"], str):
            data["strategy"] = IncrementalStrategy(data["strategy"])
        if "write_mode" in data and isinstance(data["write_mode"], str):
            data["write_mode"] = WriteMode(data["write_mode"])
        return cls(**data)


@dataclass
class IncrementalState:
    """State checkpoint for incremental processing."""
    dataset: str
    strategy: IncrementalStrategy
    
    # Watermark
    watermark: Optional[Union[str, int, float, datetime]] = None
    
    # CDC
    cdc_lsn: Optional[str] = None
    cdc_timestamp: Optional[datetime] = None
    
    # File manifest
    manifest: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Delta Lake
    version: Optional[int] = None
    
    # Cursor/Offset
    cursor: Optional[str] = None
    offsets: Dict[str, int] = field(default_factory=dict)
    
    # Generic
    last_processed: Optional[datetime] = None
    records_processed: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in value.items()}
            else:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncrementalState":
        if "strategy" in data and isinstance(data["strategy"], str):
            data["strategy"] = IncrementalStrategy(data["strategy"])
        if "last_processed" in data and isinstance(data["last_processed"], str):
            data["last_processed"] = datetime.fromisoformat(data["last_processed"])
        if "watermark" in data and isinstance(data["watermark"], str):
            try:
                data["watermark"] = datetime.fromisoformat(data["watermark"])
            except:
                pass
        if "cdc_timestamp" in data and isinstance(data["cdc_timestamp"], str):
            data["cdc_timestamp"] = datetime.fromisoformat(data["cdc_timestamp"])
        return cls(**data)


@dataclass
class WriteResult:
    """Result of incremental write operation."""
    success: bool
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    partitions_affected: List[str] = field(default_factory=list)
    new_state: Optional[IncrementalState] = None
    error: Optional[str] = None
    duration_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class IncrementalStrategyBase(ABC):
    """Base class for all incremental strategy implementations."""
    
    def __init__(self, config: IncrementalConfig, connector: Any = None):
        self.config = config
        self.connector = connector
    
    @property
    @abstractmethod
    def strategy_type(self) -> IncrementalStrategy:
        pass
    
    @abstractmethod
    async def get_last_state(self, dataset: str) -> IncrementalState:
        """Retrieve last checkpoint state for dataset."""
        pass
    
    @abstractmethod
    async def get_changes(self, dataset: str, since: IncrementalState) -> Any:
        """Fetch incremental changes since last state. Returns DataFrame or equivalent."""
        pass
    
    @abstractmethod
    async def write_changes(self, dataset: str, changes: Any, mode: WriteMode = WriteMode.MERGE) -> WriteResult:
        """Write changes with idempotency. Returns new state."""
        pass
    
    @abstractmethod
    async def get_current_state(self, dataset: str) -> IncrementalState:
        """Get current state after write (for next checkpoint)."""
        pass
    
    @abstractmethod
    async def validate_config(self) -> List[str]:
        """Validate configuration. Returns list of errors (empty if valid)."""
        pass
    
    async def get_state_store_key(self, dataset: str) -> str:
        """Generate storage key for state."""
        return f"incremental_state:{self.config.strategy.value}:{dataset}"


# Input/Output types for incremental agents
@dataclass
class IncrementalInput:
    """Input for incremental processing agents."""
    dataset: str
    config: IncrementalConfig
    last_state: Optional[IncrementalState] = None
    force_full_refresh: bool = False
    session_id: Optional[str] = None


@dataclass
class IncrementalOutput:
    """Output from incremental processing."""
    dataset: str
    changes_processed: int
    write_result: WriteResult
    new_state: IncrementalState
    duration_ms: int
    warnings: List[str] = field(default_factory=list)