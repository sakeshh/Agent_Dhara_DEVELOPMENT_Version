"""Memory Store Interfaces - Persistent learning and context across runs."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from typing import Generic, TypeVar

T = TypeVar('T')


class MemoryTier(Enum):
    """Memory storage tiers with different TTL and persistence."""
    HOT = "hot"           # Redis - sub-ms, TTL: hours-days
    WARM = "warm"         # PostgreSQL - ms, TTL: months-years
    COLD = "cold"         # Object storage - seconds, TTL: years
    PERMANENT = "permanent"  # Never expires


class MemoryType(Enum):
    """Types of memories stored."""
    SCHEMA_PATTERN = "schema_pattern"        # "Customer tables have customer_id PK"
    FIX_PATTERN = "fix_pattern"              # "negative price -> range_clip min=0"
    BUSINESS_RULE = "business_rule"          # "Finance: amount>0 when type=sale"
    EXECUTION_HISTORY = "execution_history"  # "Azure SQL: batch 10K optimal"
    USER_PREFERENCE = "user_preference"      # "Prefers quarantine over drop"
    FIX_OUTCOME = "fix_outcome"              # "pattern X worked 92% of time"
    DATA_PROFILE = "data_profile"            # Column statistics, distributions
    PIPELINE_PATTERN = "pipeline_pattern"    # Reusable pipeline structures


@dataclass
class MemoryRecord(Generic[T]):
    """A single memory record with metadata."""
    id: str
    type: MemoryType
    tier: MemoryTier
    
    # Content
    key: str                          # Lookup key (e.g., "fix:negative_values:price")
    value: T                          # The actual memory content
    
    # Context
    context: Dict[str, Any] = field(default_factory=dict)  # Dataset type, source, etc.
    confidence: float = 1.0           # 0.0 - 1.0
    
    # Provenance
    source: str = "observation"       # "observation", "user_feedback", "llm_inference", "execution_result"
    created_by: Optional[str] = None  # Agent name or user ID
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Usage tracking
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    success_count: int = 0
    failure_count: int = 0
    
    # Lifecycle
    expires_at: Optional[datetime] = None
    version: int = 1
    
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
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MemoryRecord":
        if "type" in data and isinstance(data["type"], str):
            data["type"] = MemoryType(data["type"])
        if "tier" in data and isinstance(data["tier"], str):
            data["tier"] = MemoryTier(data["tier"])
        for dt_field in ["created_at", "updated_at", "last_accessed", "expires_at"]:
            if dt_field in data and isinstance(data[dt_field], str):
                data[dt_field] = datetime.fromisoformat(data[dt_field])
        return cls(**data)


@dataclass
class MemoryQuery:
    """Query for retrieving memories."""
    types: Optional[List[MemoryType]] = None
    tiers: Optional[List[MemoryTier]] = None
    keys: Optional[List[str]] = None
    key_pattern: Optional[str] = None  # Glob pattern
    context_filters: Dict[str, Any] = field(default_factory=dict)
    min_confidence: float = 0.0
    max_age_days: Optional[int] = None
    limit: int = 100
    sort_by: str = "confidence"  # "confidence", "recency", "success_rate"
    sort_desc: bool = True


@dataclass
class MemoryStats:
    """Statistics about memory store."""
    total_records: int = 0
    by_type: Dict[str, int] = field(default_factory=dict)
    by_tier: Dict[str, int] = field(default_factory=dict)
    avg_confidence: float = 0.0
    hit_rate: float = 0.0
    storage_size_mb: float = 0.0


class MemoryInterface(ABC):
    """Abstract interface for memory storage backends."""
    
    @abstractmethod
    async def store(self, record: MemoryRecord) -> bool:
        """Store a memory record. Returns True if new, False if updated."""
        pass
    
    @abstractmethod
    async def get(self, key: str) -> Optional[MemoryRecord]:
        """Retrieve a single memory by exact key."""
        pass
    
    @abstractmethod
    async def query(self, query: MemoryQuery) -> List[MemoryRecord]:
        """Query memories with filters."""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete a memory record."""
        pass
    
    @abstractmethod
    async def increment_access(self, key: str, success: bool = True) -> bool:
        """Track access for learning."""
        pass
    
    @abstractmethod
    async def get_stats(self) -> MemoryStats:
        """Get store statistics."""
        pass
    
    @abstractmethod
    async def cleanup_expired(self) -> int:
        """Remove expired records. Returns count removed."""
        pass


class MemoryStore:
    """
    High-level memory store facade - routes to appropriate tier.
    """
    
    def __init__(self, hot_store: MemoryInterface, warm_store: MemoryInterface, cold_store: MemoryInterface):
        self.stores = {
            MemoryTier.HOT: hot_store,
            MemoryTier.WARM: warm_store,
            MemoryTier.COLD: cold_store,
            MemoryTier.PERMANENT: warm_store,  # Use warm for permanent
        }
        self.default_tier = MemoryTier.WARM
    
    def _get_store(self, tier: MemoryTier) -> MemoryInterface:
        return self.stores.get(tier, self.stores[self.default_tier])
    
    async def remember(
        self, 
        key: str, 
        value: Any, 
        type: MemoryType,
        tier: Optional[MemoryTier] = None,
        confidence: float = 1.0,
        context: Optional[Dict[str, Any]] = None,
        source: str = "observation",
        expires_at: Optional[datetime] = None
    ) -> MemoryRecord:
        """Store a memory with automatic tier selection."""
        tier = tier or self._select_tier(type, confidence)
        
        record = MemoryRecord(
            id=f"{type.value}:{key}",
            type=type,
            tier=tier,
            key=key,
            value=value,
            context=context or {},
            confidence=confidence,
            source=source,
            expires_at=expires_at
        )
        
        store = self._get_store(tier)
        await store.store(record)
        return record
    
    async def recall(
        self, 
        key: str, 
        tier: Optional[MemoryTier] = None
    ) -> Optional[MemoryRecord]:
        """Recall a specific memory."""
        if tier:
            return await self._get_store(tier).get(key)
        
        # Try all tiers in order
        for t in [MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD, MemoryTier.PERMANENT]:
            record = await self._get_store(t).get(key)
            if record:
                return record
        return None
    
    async def search(
        self, 
        query: MemoryQuery
    ) -> List[MemoryRecord]:
        """Search memories across tiers."""
        all_results = []
        tiers = query.tiers or [MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD, MemoryTier.PERMANENT]
        
        for tier in tiers:
            store = self._get_store(tier)
            results = await store.query(query)
            all_results.extend(results)
        
        # Sort combined results
        if query.sort_by == "confidence":
            all_results.sort(key=lambda r: r.confidence, reverse=query.sort_desc)
        elif query.sort_by == "recency":
            all_results.sort(key=lambda r: r.created_at or datetime.min, reverse=query.sort_desc)
        elif query.sort_by == "success_rate":
            all_results.sort(
                key=lambda r: r.success_count / max(1, r.success_count + r.failure_count), 
                reverse=query.sort_desc
            )
        
        return all_results[:query.limit]
    
    async def learn_from_execution(
        self,
        dataset_type: str,
        issue_type: str,
        action: str,
        params: Dict[str, Any],
        success: bool,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Learn from execution outcome - core learning loop."""
        key = f"{issue_type}:{action}"
        full_context = {"dataset_type": dataset_type, **(context or {})}
        
        existing = await self.recall(key)
        
        if existing:
            # Update existing pattern
            existing.value["success_count"] = existing.value.get("success_count", 0) + (1 if success else 0)
            existing.value["failure_count"] = existing.value.get("failure_count", 0) + (0 if success else 1)
            existing.value["last_params"] = params
            existing.value["last_outcome"] = "success" if success else "failure"
            existing.updated_at = datetime.utcnow()
            existing.confidence = self._calculate_confidence(existing.value)
            await self._get_store(existing.tier).store(existing)
        else:
            # Create new pattern
            pattern = {
                "issue_type": issue_type,
                "action": action,
                "params": params,
                "success_count": 1 if success else 0,
                "failure_count": 0 if success else 1,
                "dataset_types": [dataset_type],
                "last_outcome": "success" if success else "failure"
            }
            await self.remember(
                key=key,
                value=pattern,
                type=MemoryType.FIX_PATTERN,
                tier=MemoryTier.WARM,
                confidence=0.5,
                context=full_context,
                source="execution_result"
            )
    
    def _select_tier(self, type: MemoryType, confidence: float) -> MemoryTier:
        """Automatically select storage tier based on type and confidence."""
        # High-confidence patterns -> permanent
        if confidence > 0.9 and type in (MemoryType.FIX_PATTERN, MemoryType.BUSINESS_RULE):
            return MemoryTier.PERMANENT
        # Frequently accessed -> hot
        if type in (MemoryType.USER_PREFERENCE, MemoryType.SCHEMA_PATTERN):
            return MemoryTier.HOT
        # Default warm
        return MemoryTier.WARM
    
    def _calculate_confidence(self, pattern: Dict[str, Any]) -> float:
        """Calculate confidence from success/failure counts."""
        success = pattern.get("success_count", 0)
        failure = pattern.get("failure_count", 0)
        total = success + failure
        if total == 0:
            return 0.5
        return success / total
    
    async def get_schema_patterns(self, dataset_names: List[str]) -> Dict[str, Any]:
        """Get learned schema patterns for dataset types."""
        query = MemoryQuery(
            types=[MemoryType.SCHEMA_PATTERN],
            context_filters={"dataset_type": {"$in": dataset_names}},
            min_confidence=0.5,
            limit=50
        )
        results = await self.search(query)
        patterns = {}
        for r in results:
            patterns[r.key] = r.value
        return patterns
    
    async def get_fix_patterns(self, issue_types: List[str]) -> Dict[str, Any]:
        """Get learned fix patterns for issue types."""
        query = MemoryQuery(
            types=[MemoryType.FIX_PATTERN],
            key_pattern="*",
            context_filters={"issue_type": {"$in": issue_types}},
            min_confidence=0.6,
            sort_by="success_rate",
            limit=100
        )
        results = await self.search(query)
        patterns = {}
        for r in results:
            patterns[r.key] = r.value
        return patterns
    
    async def get_user_preferences(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Get user preferences."""
        query = MemoryQuery(
            types=[MemoryType.USER_PREFERENCE],
            context_filters={"user_id": user_id} if user_id else {},
            limit=50
        )
        results = await self.search(query)
        return {r.key: r.value for r in results}
    
    async def set_user_preference(self, key: str, value: Any, user_id: Optional[str] = None) -> None:
        """Set a user preference."""
        context = {"user_id": user_id} if user_id else {}
        await self.remember(
            key=key,
            value=value,
            type=MemoryType.USER_PREFERENCE,
            tier=MemoryTier.HOT,
            confidence=1.0,
            context=context
        )