from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    name: str
    dtype: str
    inferred_type: Optional[str] = None
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
    dataset_name: str = ""
    dataset_id: Optional[str] = None
    row_count: int = 0
    column_count: int = 0
    source_type: str = "database"  # database | blob | filesystem
    source_uri: Optional[str] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = "dbo"
    columns: Dict[str, ColumnProfile] = Field(default_factory=dict)
    primary_key_columns: List[str] = Field(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        if not self.dataset_name and self.dataset_id:
            self.dataset_name = self.dataset_id
        elif not self.dataset_id and self.dataset_name:
            self.dataset_id = self.dataset_name

    def get_column(self, col_name: str) -> Optional[ColumnProfile]:
        return self.columns.get(col_name)
