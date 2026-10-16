"""Relationship Finder - Cross-dataset relationship discovery and referential integrity."""
from __future__ import annotations
import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

import pandas as pd
import numpy as np

from agent.assessment.models import (
    Relationship, ColumnProfile, DatasetProfile, QualityIssue,
    IssueSeverity, IssueCategory, FixabilityTier
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.relationships")


@dataclass
class RelationshipConfig:
    """Configuration for relationship discovery."""
    min_overlap_pct: float = 0.1  # Minimum overlap to consider relationship
    min_cardinality_ratio: float = 0.01  # Min cardinality ratio for FK detection
    sample_size: int = 10000  # Sample size for overlap computation
    enable_fk_detection: bool = True
    enable_cardinality_inference: bool = True
    enable_orphan_detection: bool = True
    fuzzy_match_threshold: float = 0.8  # For fuzzy column matching


@dataclass
class RelationshipCandidate:
    """A potential relationship between two datasets."""
    parent_dataset: str
    parent_column: str
    child_dataset: str
    child_column: str
    overlap_count: int
    overlap_pct: float
    parent_cardinality: int
    child_cardinality: int
    inferred_type: str  # "one_to_one", "one_to_many", "many_to_one", "many_to_many"
    confidence: float
    match_type: str  # "exact", "fuzzy", "semantic"


class RelationshipFinder:
    """Discovers relationships between datasets including FK detection and cardinality inference."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.rel_config = RelationshipConfig(
            min_overlap_pct=getattr(config, 'rel_min_overlap', 0.1),
            sample_size=getattr(config, 'rel_sample_size', 10000),
            enable_fk_detection=getattr(config, 'enable_fk_detection', True),
            enable_cardinality_inference=getattr(config, 'enable_cardinality', True),
            enable_orphan_detection=getattr(config, 'enable_orphan_detection', True),
        )
    
    async def find(self, profiles: Dict[str, DatasetProfile], datasets: Dict[str, Any]) -> List[Relationship]:
        """Find relationships between all dataset pairs."""
        
        with get_performance_logger("relationship_discovery") as perf:
            
            relationships = []
            dataset_names = list(profiles.keys())
            
            # Compare each pair of datasets
            for i, ds1_name in enumerate(dataset_names):
                for ds2_name in dataset_names[i+1:]:
                    ds1_profile = profiles[ds1_name]
                    ds2_profile = profiles[ds2_name]
                    ds1_data = datasets.get(ds1_name)
                    ds2_data = datasets.get(ds2_name)
                    
                    if ds1_data is None or ds2_data is None:
                        continue
                    
                    # Find relationships in both directions
                    candidates = await self._find_candidates(
                        ds1_name, ds1_profile, ds1_data,
                        ds2_name, ds2_profile, ds2_data
                    )
                    
                    for candidate in candidates:
                        # Validate with data if available
                        validated = await self._validate_candidate(candidate, datasets)
                        if validated:
                            relationship = self._candidate_to_relationship(validated)
                            relationships.append(relationship)
            
            # Also check for declared relationships from config
            declared = self._get_declared_relationships()
            relationships.extend(declared)
            
            # Deduplicate
            relationships = self._deduplicate_relationships(relationships)
            
            perf.log_metric("relationships_found", len(relationships))
            
            return relationships
    
    async def _find_candidates(
        self, 
        ds1_name: str, ds1_profile: DatasetProfile, ds1_data: Any,
        ds2_name: str, ds2_profile: DatasetProfile, ds2_data: Any
    ) -> List[RelationshipCandidate]:
        """Find relationship candidates between two datasets."""
        
        candidates = []
        
        # Get column pairs to compare
        col_pairs = self._get_column_pairs(ds1_profile, ds2_profile)
        
        for col1_name, col2_name in col_pairs:
            col1 = ds1_profile.columns[col1_name]
            col2 = ds2_profile.columns[col2_name]
            
            # Quick filter: compatible semantic types
            if not self._compatible_types(col1, col2):
                continue
            
            # Compute overlap if data available
            if ds1_data is not None and ds2_data is not None:
                candidate = await self._compute_overlap(
                    ds1_name, col1_name, ds1_data,
                    ds2_name, col2_name, ds2_data,
                    col1, col2
                )
                if candidate and candidate.overlap_pct >= self.rel_config.min_overlap_pct:
                    candidates.append(candidate)
            else:
                # Schema-only matching
                candidate = self._schema_only_match(
                    ds1_name, col1_name, col1,
                    ds2_name, col2_name, col2
                )
                if candidate:
                    candidates.append(candidate)
        
        return candidates
    
    def _get_column_pairs(self, profile1: DatasetProfile, profile2: DatasetProfile) -> List[Tuple[str, str]]:
        """Get column pairs to compare between two datasets."""
        
        pairs = []
        
        for name1, col1 in profile1.columns.items():
            for name2, col2 in profile2.columns.items():
                # Quick name similarity
                if self._names_similar(name1, name2):
                    pairs.append((name1, name2))
                # Same semantic type
                elif col1.semantic_type == col2.semantic_type and col1.semantic_type != "unknown":
                    pairs.append((name1, name2))
                # FK pattern
                elif name1.lower().endswith("_id") and name2.lower().endswith("_id"):
                    pairs.append((name1, name2))
        
        return pairs
    
    def _names_similar(self, name1: str, name2: str) -> bool:
        """Check if column names are similar."""
        n1 = name1.lower().replace("_", "")
        n2 = name2.lower().replace("_", "")
        
        # Exact match after normalization
        if n1 == n2:
            return True
        
        # One contains the other
        if n1 in n2 or n2 in n1:
            return True
        
        # Fuzzy match (simplified)
        return False
    
    def _compatible_types(self, col1: ColumnProfile, col2: ColumnProfile) -> bool:
        """Check if two columns have compatible types for relationship."""
        
        # Same semantic type
        if col1.semantic_type == col2.semantic_type and col1.semantic_type != "unknown":
            return True
        
        # Both numeric
        if self._is_numeric(col1) and self._is_numeric(col2):
            return True
        
        # Both text
        if self._is_text(col1) and self._is_text(col2):
            return True
        
        # Both datetime
        if self._is_datetime(col1) and self._is_datetime(col2):
            return True
        
        # Identifier types
        if col1.semantic_type in ["primary_key", "foreign_key", "unique_id", "uuid"] and \
           col2.semantic_type in ["primary_key", "foreign_key", "unique_id", "uuid"]:
            return True
        
        return False
    
    def _is_numeric(self, col: ColumnProfile) -> bool:
        return "int" in str(col.data_type).lower() or "float" in str(col.data_type).lower() or "decimal" in str(col.data_type).lower()
    
    def _is_text(self, col: ColumnProfile) -> bool:
        return "object" in str(col.data_type).lower() or "string" in str(col.data_type).lower() or "text" in str(col.data_type).lower()
    
    def _is_datetime(self, col: ColumnProfile) -> bool:
        return "datetime" in str(col.data_type).lower() or "date" in str(col.data_type).lower() or "time" in str(col.data_type).lower()
    
    async def _compute_overlap(
        self, 
        ds1_name: str, col1_name: str, ds1_data: Any,
        ds2_name: str, col2_name: str, ds2_data: Any,
        col1: ColumnProfile, col2: ColumnProfile
    ) -> Optional[RelationshipCandidate]:
        """Compute actual overlap between two columns using data."""
        
        try:
            # Convert to DataFrames if needed
            df1 = self._to_dataframe(ds1_data)
            df2 = self._to_dataframe(ds2_data)
            
            if df1 is None or df2 is None:
                return None
            
            if col1_name not in df1.columns or col2_name not in df2.columns:
                return None
            
            # Sample if too large
            sample_size = min(self.rel_config.sample_size, len(df1), len(df2))
            if len(df1) > sample_size:
                df1 = df1.sample(n=sample_size, random_state=42)
            if len(df2) > sample_size:
                df2 = df2.sample(n=sample_size, random_state=42)
            
            # Get unique values
            vals1 = set(df1[col1_name].dropna().astype(str).unique())
            vals2 = set(df2[col2_name].dropna().astype(str).unique())
            
            if not vals1 or not vals2:
                return None
            
            # Compute overlap
            overlap = vals1 & vals2
            overlap_count = len(overlap)
            
            # Overlap percentage (relative to smaller set)
            min_cardinality = min(len(vals1), len(vals2))
            overlap_pct = overlap_count / min_cardinality if min_cardinality > 0 else 0
            
            if overlap_count == 0:
                return None
            
            # Infer cardinality
            card1 = len(vals1)
            card2 = len(vals2)
            
            # Determine relationship type
            # If vals1 is subset of vals2 -> ds1 is child, ds2 is parent (many_to_one from ds1 to ds2)
            # If vals2 is subset of vals1 -> ds2 is child, ds1 is parent
            # If neither is subset -> many_to_many or partial overlap
            
            vals1_is_subset = vals1.issubset(vals2)
            vals2_is_subset = vals2.issubset(vals1)
            
            if vals1_is_subset and vals2_is_subset:
                inferred_type = "one_to_one"
            elif vals1_is_subset:
                inferred_type = "many_to_one"  # ds1 (child) -> ds2 (parent)
            elif vals2_is_subset:
                inferred_type = "one_to_many"  # ds1 (parent) -> ds2 (child)
            else:
                # Partial overlap
                if overlap_pct > 0.5:
                    inferred_type = "many_to_many"
                else:
                    inferred_type = "partial_overlap"
            
            # Determine which is parent/child based on cardinality and PK/FK patterns
            col1_is_pk = col1.is_primary_key_candidate
            col2_is_pk = col2.is_primary_key_candidate
            col1_is_fk = col1.is_foreign_key_candidate
            col2_is_fk = col2.is_foreign_key_candidate
            
            confidence = self._calculate_confidence(
                overlap_pct, card1, card2, col1, col2,
                col1_is_pk, col2_is_pk, col1_is_fk, col2_is_fk
            )
            
            if confidence < 0.3:
                return None
            
            return RelationshipCandidate(
                parent_dataset=ds2_name if inferred_type in ["many_to_one", "one_to_one"] else ds1_name,
                parent_column=col2_name if inferred_type in ["many_to_one", "one_to_one"] else col1_name,
                child_dataset=ds1_name if inferred_type in ["many_to_one", "one_to_one"] else ds2_name,
                child_column=col1_name if inferred_type in ["many_to_one", "one_to_one"] else col2_name,
                overlap_count=overlap_count,
                overlap_pct=overlap_pct,
                parent_cardinality=card2 if inferred_type in ["many_to_one", "one_to_one"] else card1,
                child_cardinality=card1 if inferred_type in ["many_to_one", "one_to_one"] else card2,
                inferred_type=inferred_type,
                confidence=confidence,
                match_type="exact"
            )
            
        except Exception as e:
            logger.warning(f"Overlap computation failed: {e}")
            return None
    
    def _calculate_confidence(
        self, 
        overlap_pct: float, 
        card1: int, 
        card2: int,
        col1: ColumnProfile, col2: ColumnProfile,
        col1_is_pk: bool, col2_is_pk: bool,
        col1_is_fk: bool, col2_is_fk: bool
    ) -> float:
        """Calculate confidence score for a relationship."""
        
        confidence = 0.0
        
        # Base confidence from overlap
        confidence += overlap_pct * 0.4
        
        # PK-FK pattern boost
        if (col1_is_pk and col2_is_fk) or (col2_is_pk and col1_is_fk):
            confidence += 0.3
        elif col1_is_pk and col2_is_pk:
            confidence += 0.2
        
        # Cardinality ratio
        if card1 > 0 and card2 > 0:
            ratio = min(card1, card2) / max(card1, card2)
            confidence += ratio * 0.2
        
        # Semantic type match
        if col1.semantic_type == col2.semantic_type and col1.semantic_type != "unknown":
            confidence += 0.1
        
        # Name similarity boost
        n1 = col1.name.lower().replace("_", "")
        n2 = col2.name.lower().replace("_", "")
        if n1 == n2:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _schema_only_match(
        self, 
        ds1_name: str, col1_name: str, col1: ColumnProfile,
        ds2_name: str, col2_name: str, col2: ColumnProfile
    ) -> Optional[RelationshipCandidate]:
        """Schema-only relationship matching when data not available."""
        
        if not self._compatible_types(col1, col2):
            return None
        
        # Name similarity
        if not self._names_similar(col1_name, col2_name):
            return None
        
        # Semantic type match
        if col1.semantic_type != col2.semantic_type and col1.semantic_type != "unknown":
            return None
        
        # PK/FK pattern
        col1_is_pk = col1.is_primary_key_candidate
        col2_is_pk = col2.is_primary_key_candidate
        col1_is_fk = col1.is_foreign_key_candidate
        col2_is_fk = col2.is_foreign_key_candidate
        
        inferred_type = "potential"
        confidence = 0.3
        
        if (col1_is_pk and col2_is_fk) or (col2_is_pk and col1_is_fk):
            inferred_type = "likely_fk"
            confidence = 0.7
        elif col1_is_pk and col2_is_pk:
            inferred_type = "potential_pk_overlap"
            confidence = 0.4
        
        return RelationshipCandidate(
            parent_dataset=ds2_name if col2_is_pk else ds1_name,
            parent_column=col2_name if col2_is_pk else col1_name,
            child_dataset=ds1_name if col2_is_pk else ds2_name,
            child_column=col1_name if col2_is_pk else col2_name,
            overlap_count=0,
            overlap_pct=0.0,
            parent_cardinality=0,
            child_cardinality=0,
            inferred_type=inferred_type,
            confidence=confidence,
            match_type="schema"
        )
    
    def _to_dataframe(self, data: Any) -> Optional[pd.DataFrame]:
        if data is None:
            return None
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, dict):
            return pd.DataFrame(data)
        if isinstance(data, list):
            return pd.DataFrame(data)
        return None
    
    async def _validate_candidate(self, candidate: RelationshipCandidate, datasets: Dict[str, Any]) -> Optional[RelationshipCandidate]:
        """Validate a relationship candidate with full data."""
        
        # For now, return as-is if confidence is decent
        if candidate.confidence >= 0.5:
            return candidate
        
        # Could do deeper validation here with full data
        return candidate
    
    def _candidate_to_relationship(self, candidate: RelationshipCandidate) -> Relationship:
        """Convert candidate to Relationship model."""
        
        # Determine severity based on relationship type
        if candidate.inferred_type in ["many_to_one", "one_to_many"]:
            severity = IssueSeverity.MEDIUM
        elif candidate.inferred_type == "one_to_one":
            severity = IssueSeverity.LOW
        else:
            severity = IssueSeverity.LOW
        
        return Relationship(
            id=f"rel_{candidate.parent_dataset}_{candidate.parent_column}_{candidate.child_dataset}_{candidate.child_column}",
            parent=f"{candidate.parent_dataset}.{candidate.parent_column}",
            child=f"{candidate.child_dataset}.{candidate.child_column}",
            parent_dataset=candidate.parent_dataset,
            parent_column=candidate.parent_column,
            child_dataset=candidate.child_dataset,
            child_column=candidate.child_column,
            overlap_count=candidate.overlap_count,
            overlap_pct=candidate.overlap_pct,
            parent_cardinality=candidate.parent_cardinality,
            child_cardinality=candidate.child_cardinality,
            inferred_cardinality=candidate.inferred_type,
            confidence=candidate.confidence,
            severity=severity,
            detected_at=datetime.utcnow(),
        )
    
    def _get_declared_relationships(self) -> List[Relationship]:
        """Get relationships declared in config."""
        relationships = []
        
        declared = self.config.get("relationships", [])
        for rel in declared:
            relationships.append(Relationship(
                id=f"declared_{rel.get('name', str(uuid.uuid4())[:8])}",
                parent=f"{rel['parent']['dataset']}.{rel['parent']['columns'][0]}",
                child=f"{rel['child']['dataset']}.{rel['child']['columns'][0]}",
                parent_dataset=rel['parent']['dataset'],
                parent_column=rel['parent']['columns'][0],
                child_dataset=rel['child']['dataset'],
                child_column=rel['child']['columns'][0],
                overlap_count=0,
                overlap_pct=0.0,
                parent_cardinality=0,
                child_cardinality=0,
                inferred_cardinality=rel.get("cardinality", "one_to_many"),
                confidence=1.0,
                severity=IssueSeverity.HIGH,
                detected_at=datetime.utcnow(),
            ))
        
        return relationships
    
    def _deduplicate_relationships(self, relationships: List[Relationship]) -> List[Relationship]:
        """Remove duplicate relationships."""
        
        seen = set()
        unique = []
        
        for rel in relationships:
            # Normalize: order by dataset name
            key_parts = sorted([rel.parent, rel.child])
            key = f"{key_parts[0]}|{key_parts[1]}"
            
            if key not in seen:
                seen.add(key)
                unique.append(rel)
        
        return unique