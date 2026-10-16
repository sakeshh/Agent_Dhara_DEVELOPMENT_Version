"""Semantic Analysis - Pattern-based and LLM-enhanced semantic type inference."""
from __future__ import annotations
import asyncio
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter

from agent.assessment.models import SemanticType, ColumnProfile, DatasetProfile, SemanticProfile
from agent.core.interfaces import MemoryStore
from agent.feature_flags import get_flags
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.semantic")


# Pattern-based semantic inference rules
SEMANTIC_PATTERNS = {
    SemanticType.EMAIL: {
        "regex": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "name_keywords": ["email", "e_mail", "mail", "contact_email"],
        "confidence": 0.95
    },
    SemanticType.PHONE: {
        "regex": r'^[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$',
        "name_keywords": ["phone", "telephone", "mobile", "cell", "fax", "contact_number"],
        "confidence": 0.9
    },
    SemanticType.URL: {
        "regex": r'^https?://[^\s/$.?#].[^\s]*$',
        "name_keywords": ["url", "link", "website", "homepage", "uri"],
        "confidence": 0.95
    },
    SemanticType.UUID: {
        "regex": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        "name_keywords": ["uuid", "guid", "unique_id"],
        "confidence": 0.99
    },
    SemanticType.CURRENCY: {
        "regex": r'^[\$\£\€]?\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})?$',
        "name_keywords": ["price", "cost", "amount", "fee", "charge", "payment", "salary", "wage", "revenue", "sales", "total", "subtotal", "tax", "discount"],
        "confidence": 0.85
    },
    SemanticType.PERCENTAGE: {
        "regex": r'^\d{1,3}(?:\.\d+)?%?$',
        "name_keywords": ["pct", "percent", "percentage", "rate", "ratio"],
        "confidence": 0.8
    },
    SemanticType.DATE: {
        "regex": r'^\d{4}-\d{2}-\d{2}$',
        "name_keywords": ["date", "created", "updated", "modified", "birth", "dob", "start", "end", "expire", "due", "deadline"],
        "confidence": 0.9
    },
    SemanticType.DATETIME: {
        "regex": r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}',
        "name_keywords": ["timestamp", "created_at", "updated_at", "created_on", "modified_at", "logged_at"],
        "confidence": 0.95
    },
    SemanticType.PRIMARY_KEY: {
        "name_keywords": ["id", "_id", "pk", "primary_key", "key"],
        "confidence": 0.8
    },
    SemanticType.FOREIGN_KEY: {
        "name_keywords": ["_id", "_fk", "ref_id", "reference_id", "parent_id", "customer_id", "user_id", "order_id", "product_id"],
        "confidence": 0.75
    },
    SemanticType.NAME: {
        "name_keywords": ["name", "first_name", "last_name", "full_name", "username", "display_name", "first", "last", "given", "surname"],
        "confidence": 0.7
    },
    SemanticType.ADDRESS: {
        "name_keywords": ["address", "street", "city", "state", "zip", "postal", "country", "region", "location"],
        "confidence": 0.7
    },
    SemanticType.CATEGORY: {
        "name_keywords": ["category", "type", "kind", "class", "group", "segment", "tier", "level", "status", "status_id"],
        "confidence": 0.6
    },
    SemanticType.STATUS: {
        "name_keywords": ["status", "state", "condition", "flag", "is_active", "is_enabled", "is_deleted"],
        "confidence": 0.7
    },
    SemanticType.QUANTITY: {
        "name_keywords": ["qty", "quantity", "count", "amount", "number", "num", "units", "volume"],
        "confidence": 0.7
    },
    SemanticType.AGE: {
        "name_keywords": ["age", "years_old", "yrs"],
        "confidence": 0.9
    },
}


@dataclass
class SemanticInferenceResult:
    """Result of semantic inference for a column."""
    semantic_type: SemanticType
    confidence: float
    source: str  # "pattern", "name", "llm", "combined"
    evidence: Dict[str, Any] = field(default_factory=dict)


class SemanticAnalyzer:
    """Pattern-based semantic type inference."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.flags = get_flags()
        self._compiled_patterns = self._compile_patterns()
    
    def _compile_patterns(self) -> Dict[SemanticType, re.Pattern]:
        """Pre-compile regex patterns."""
        compiled = {}
        for sem_type, config in SEMANTIC_PATTERNS.items():
            if "regex" in config:
                try:
                    compiled[sem_type] = re.compile(config["regex"], re.IGNORECASE)
                except re.error:
                    pass
        return compiled
    
    async def rough_inference(self, profile: DatasetProfile) -> Dict[str, SemanticInferenceResult]:
        """Fast pattern-based semantic inference for all columns."""
        
        results = {}
        
        for col_name, col_profile in profile.columns.items():
            result = self._infer_column_semantics(col_name, col_profile, profile)
            results[col_name] = result
        
        return results
    
    def _infer_column_semantics(
        self, 
        col_name: str, 
        col_profile: ColumnProfile, 
        dataset_profile: DatasetProfile
    ) -> SemanticInferenceResult:
        """Infer semantic type for a single column using patterns."""
        
        scores: Dict[SemanticType, float] = {}
        evidence: Dict[str, Any] = {}
        
        # 1. Name-based matching
        col_lower = col_name.lower()
        for sem_type, config in SEMANTIC_PATTERNS.items():
            if "name_keywords" in config:
                for keyword in config["name_keywords"]:
                    if keyword in col_lower:
                        scores[sem_type] = scores.get(sem_type, 0) + config["confidence"]
                        evidence[f"name_match_{sem_type.value}"] = keyword
        
        # 2. Pattern matching on sample values
        sample_values = col_profile.sample_values[:20]
        if sample_values:
            for sem_type, pattern in self._compiled_patterns.items():
                matches = sum(1 for v in sample_values if pattern.match(str(v)))
                if matches > 0:
                    match_pct = matches / len(sample_values)
                    scores[sem_type] = scores.get(sem_type, 0) + match_pct * 0.9
                    evidence[f"pattern_match_{sem_type.value}"] = f"{matches}/{len(sample_values)}"
        
        # 3. Data type + statistics heuristics
        dtype = col_profile.data_type.lower()
        null_pct = col_profile.null_percentage
        distinct_pct = col_profile.distinct_percentage
        
        # High cardinality + low nulls -> likely identifier
        if distinct_pct > 95 and null_pct < 5:
            scores[SemanticType.PRIMARY_KEY] = scores.get(SemanticType.PRIMARY_KEY, 0) + 0.6
            scores[SemanticType.UNIQUE_ID] = scores.get(SemanticType.UNIQUE_ID, 0) + 0.6
            evidence["high_cardinality"] = f"{distinct_pct:.1f}% distinct"
        
        # FK pattern: medium cardinality, _id suffix
        if col_name.lower().endswith("_id") or col_name.lower().endswith("_fk"):
            scores[SemanticType.FOREIGN_KEY] = scores.get(SemanticType.FOREIGN_KEY, 0) + 0.8
            evidence["name_suffix"] = "_id/_fk"
        
        # Numeric with currency-like stats
        if "int" in str(col_profile.data_type).lower() or "float" in str(col_profile.data_type).lower():
            if col_profile.min_value is not None and col_profile.min_value >= 0:
                scores[SemanticType.CURRENCY] = scores.get(SemanticType.CURRENCY, 0) + 0.3
                evidence["non_negative_numeric"] = True
        
        # Date-like columns
        if "date" in str(col_profile.data_type).lower() or "time" in str(col_profile.data_type).lower():
            scores[SemanticType.DATE] = scores.get(SemanticType.DATE, 0) + 0.7
            scores[SemanticType.DATETIME] = scores.get(SemanticType.DATETIME, 0) + 0.7
            evidence["datetime_dtype"] = True
        
        # Pick best match
        if scores:
            best_type = max(scores, key=scores.get)
            confidence = min(scores[best_type], 1.0)
            
            # Determine source
            if best_type in self._compiled_patterns and any("pattern_match" in k for k in evidence):
                source = "pattern"
            elif any("name_match" in k for k in evidence):
                source = "name"
            else:
                source = "heuristic"
            
            return SemanticInferenceResult(
                semantic_type=best_type,
                confidence=confidence,
                source=source,
                evidence=evidence
            )
        
        return SemanticInferenceResult(
            semantic_type=SemanticType.UNKNOWN,
            confidence=0.0,
            source="none",
            evidence={}
        )
    
    async def infer(self, profile: DatasetProfile) -> Dict[str, SemanticInferenceResult]:
        """Public interface for semantic inference."""
        return self.rough_inference(profile)


class SemanticRefiner:
    """LLM-enhanced semantic refinement for ambiguous columns."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.flags = get_flags()
    
    async def refine(
        self, 
        dataset_name: str, 
        profile: DatasetProfile, 
        ambiguous_columns: List[ColumnProfile]
    ) -> SemanticProfile:
        """Refine semantic understanding using LLM for ambiguous columns."""
        
        # For now, return enhanced profile with combined logic
        # Real implementation would call LLM here
        
        column_roles = {}
        entities = {}
        inferred_constraints = []
        
        for col in ambiguous_columns:
            # Enhanced inference combining patterns + context
            refined = self._enhanced_inference(col, profile)
            col.semantic_type = refined.semantic_type
            col.semantic_confidence = refined.confidence
            
            # Assign column role
            column_roles[col.name] = self._infer_column_role(col, profile)
        
        # Identify entities
        entities = self._identify_entities(profile)
        
        return SemanticProfile(
            dataset_name=profile.name,
            entities=entities,
            column_roles=column_roles,
            inferred_constraints=inferred_constraints,
            confidence=0.8
        )
    
    def _enhanced_inference(self, col: ColumnProfile, profile: DatasetProfile) -> SemanticInferenceResult:
        """Enhanced inference combining multiple signals."""
        
        # Start with pattern-based
        base = SemanticAnalyzer(None, None)
        base._compiled_patterns = base._compile_patterns()
        result = base._infer_column_semantics(col.name, col, profile)
        
        # Boost confidence if multiple signals agree
        if result.confidence > 0.5:
            result.confidence = min(result.confidence + 0.15, 0.95)
        
        return result
    
    def _infer_column_role(self, col: ColumnProfile, profile: DatasetProfile) -> str:
        """Infer the role of a column in the dataset."""
        sem = col.semantic_type
        
        if sem in [SemanticType.PRIMARY_KEY, SemanticType.UNIQUE_ID, SemanticType.UUID]:
            return "identifier"
        elif sem in [SemanticType.FOREIGN_KEY]:
            return "key"
        elif sem in [SemanticType.CURRENCY, SemanticType.PERCENTAGE, SemanticType.QUANTITY, SemanticType.AMOUNT]:
            return "measure"
        elif sem in [SemanticType.DATE, SemanticType.DATETIME, SemanticType.TIMESTAMP]:
            return "timestamp"
        elif sem in [SemanticType.CATEGORY, SemanticType.STATUS, SemanticType.NAME]:
            return "attribute"
        else:
            return "attribute"
    
    def _identify_entities(self, profile: DatasetProfile) -> Dict[str, Dict[str, Any]]:
        """Identify business entities from column patterns."""
        
        entities = {}
        
        # Group columns by entity prefix
        entity_columns = {}
        for col_name in profile.columns.keys():
            # Simple heuristic: split on underscore
            parts = col_name.split('_')
            if len(parts) > 1:
                entity = parts[0]
                if entity not in entity_columns:
                    entity_columns[entity] = []
                entity_columns[entity].append(col_name)
        
        # Create entities
        for entity, cols in entity_columns.items():
            if len(cols) >= 2:  # At least 2 columns to form an entity
                entities[entity] = {
                    "columns": cols,
                    "type": "business_entity",
                    "inferred": True
                }
        
        return entities


class LLMEnricher:
    """LLM-based semantic enrichment (placeholder for future implementation)."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
    
    async def enrich_column(self, col_profile: ColumnProfile, context: Dict[str, Any]) -> SemanticInferenceResult:
        """Use LLM to enrich semantic understanding."""
        # Placeholder for LLM integration
        # Would send column info + sample values to LLM
        # Return enhanced semantic type with reasoning
        return SemanticInferenceResult(
            semantic_type=SemanticType.UNKNOWN,
            confidence=0.0,
            source="llm",
            evidence={"status": "not_implemented"}
        )
    
    async def enrich_dataset(self, profile: DatasetProfile) -> SemanticProfile:
        """Enrich entire dataset semantics."""
        return SemanticProfile(
            dataset_name=profile.name,
            entities={},
            column_roles={},
            inferred_constraints=[],
        )