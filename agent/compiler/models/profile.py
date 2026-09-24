from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    name: str
    dtype: str
    semantic_type: Optional[str] = "unknown"
    null_count: int = 0
    null_percentage: float = 0.0
    unique_count: int = 0
    unique_percentage: float = 0.0
    is_primary_key_candidate: bool = False
    is_foreign_key_candidate: bool = False
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    sample_values: List[Any] = Field(default_factory=list)
    format_anomalies: List[str] = Field(default_factory=list)


class DatasetProfile(BaseModel):
    dataset_name: str
    row_count: int = 0
    column_count: int = 0
    source_type: str = "database"  # database | blob | filesystem
    source_uri: Optional[str] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = "dbo"
    columns: Dict[str, ColumnProfile] = Field(default_factory=dict)
    primary_key_columns: List[str] = Field(default_factory=list)

    def get_column(self, col_name: str) -> Optional[ColumnProfile]:
        return self.columns.get(col_name)
