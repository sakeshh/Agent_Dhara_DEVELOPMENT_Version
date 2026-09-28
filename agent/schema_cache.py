from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.profile import ColumnProfile, DatasetProfile

logger = logging.getLogger("agent.schema_cache")


class CachedColumnSummary(BaseModel):
    name: str
    dtype: str = "string"
    semantic_type: Optional[str] = "unknown"
    null_count: int = 0
    null_percentage: float = 0.0
    distinct_count: int = 0
    distinct_percentage: float = 0.0
    is_primary_key_candidate: bool = False
    sample_values: List[Any] = Field(default_factory=list)


class CachedDatasetSchema(BaseModel):
    dataset_name: str
    schema_name: str = "dbo"
    row_count: int = 0
    column_count: int = 0
    columns: Dict[str, CachedColumnSummary] = Field(default_factory=dict)
    primary_key_columns: List[str] = Field(default_factory=list)
    schema_hash: str = ""
    cached_at: float = Field(default_factory=time.time)
    ttl_seconds: int = 300

    def is_expired(self) -> bool:
        return (time.time() - self.cached_at) > self.ttl_seconds


class SchemaCache:
    """
    In-memory thread-safe schema cache.
    Caches column profiles and statistics so LLM prompt construction and IR validation
    do not incur repeated expensive profiling or DB round-trips.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Dict[str, CachedDatasetSchema]] = {}  # session_id -> {dataset_name: CachedDatasetSchema}
        self._lock = threading.RLock()

    def get(self, session_id: str, dataset_name: str) -> Optional[CachedDatasetSchema]:
        with self._lock:
            session_schemas = self._cache.get(session_id, {})
            cached = session_schemas.get(dataset_name)
            if not cached:
                return None
            if cached.is_expired():
                logger.debug(f"Schema cache entry for {session_id}:{dataset_name} has expired; invalidating")
                del session_schemas[dataset_name]
                return None
            return cached

    def put(
        self,
        session_id: str,
        dataset_name: str,
        schema: CachedDatasetSchema,
        ttl_seconds: int = 300,
    ) -> None:
        with self._lock:
            if session_id not in self._cache:
                self._cache[session_id] = {}
            schema.ttl_seconds = ttl_seconds
            schema.cached_at = time.time()
            self._cache[session_id][dataset_name] = schema

    def put_from_dataset_profile(
        self,
        session_id: str,
        profile: DatasetProfile,
        ttl_seconds: int = 300,
    ) -> CachedDatasetSchema:
        cols: Dict[str, CachedColumnSummary] = {}
        for col_name, cp in profile.columns.items():
            cols[col_name] = CachedColumnSummary(
                name=col_name,
                dtype=cp.dtype or "string",
                semantic_type=cp.semantic_type or "unknown",
                null_count=cp.null_count,
                null_percentage=cp.null_percentage,
                distinct_count=cp.unique_count,
                distinct_percentage=cp.unique_percentage,
                is_primary_key_candidate=cp.is_primary_key_candidate,
                sample_values=cp.sample_values[:5],
            )

        col_sig = sorted([(k, v.dtype) for k, v in cols.items()])
        schema_hash = hashlib.sha256(json.dumps(col_sig).encode("utf-8")).hexdigest()[:16]

        cached = CachedDatasetSchema(
            dataset_name=profile.dataset_name or profile.dataset_id or "default",
            schema_name=profile.schema_name or "dbo",
            row_count=profile.row_count,
            column_count=len(cols),
            columns=cols,
            primary_key_columns=profile.primary_key_columns,
            schema_hash=schema_hash,
            ttl_seconds=ttl_seconds,
        )
        self.put(session_id, cached.dataset_name, cached, ttl_seconds=ttl_seconds)
        return cached

    def get_all_for_session(self, session_id: str) -> Dict[str, CachedDatasetSchema]:
        with self._lock:
            res: Dict[str, CachedDatasetSchema] = {}
            session_schemas = self._cache.get(session_id, {})
            expired_keys = []
            for ds_name, cached in session_schemas.items():
                if cached.is_expired():
                    expired_keys.append(ds_name)
                else:
                    res[ds_name] = cached
            for k in expired_keys:
                del session_schemas[k]
            return res

    def to_llm_schema_prompt(self, session_id: str, table_names: Optional[List[str]] = None) -> str:
        """
        Formats a compact, token-efficient schema description suitable for LLM prompt context.
        """
        schemas = self.get_all_for_session(session_id)
        if not schemas:
            return "No schema catalog currently cached for this session."

        lines = ["### Available Schemas & Tables:"]
        for tbl, s in schemas.items():
            if table_names and tbl not in table_names:
                continue
            lines.append(f"- Table: `{s.schema_name}.{tbl}` (Rows: ~{s.row_count})")
            col_descs = []
            for c_name, c_info in s.columns.items():
                pk_tag = " [PK]" if c_name in s.primary_key_columns or c_info.is_primary_key_candidate else ""
                col_descs.append(f"  * `{c_name}` ({c_info.dtype}{pk_tag})")
            lines.extend(col_descs)
        return "\n".join(lines)

    def invalidate(self, session_id: str, dataset_name: Optional[str] = None) -> None:
        with self._lock:
            if dataset_name and session_id in self._cache:
                self._cache[session_id].pop(dataset_name, None)
            elif not dataset_name:
                self._cache.pop(session_id, None)


_GLOBAL_SCHEMA_CACHE = SchemaCache()


def get_schema_cache() -> SchemaCache:
    return _GLOBAL_SCHEMA_CACHE
