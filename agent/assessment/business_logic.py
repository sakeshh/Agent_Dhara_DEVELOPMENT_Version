"""Business Logic Engine - Discovers cross-column rules, functional dependencies, and business constraints."""
from __future__ import annotations
import asyncio
import logging
import uuid
import itertools
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict, Counter

import pandas as pd
import numpy as np

from agent.assessment.models import (
    QualityIssue, IssueSeverity, IssueCategory, FixabilityTier,
    ColumnProfile, DatasetProfile
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.business_logic")


@dataclass
class DiscoveredRule:
    """A discovered business rule."""
    id: str
    name: str
    rule_type: str  # "functional_dependency", "conditional", "aggregation", "range", "format", "state_machine"
    description: str
    columns: List[str]
    datasets: List[str]
    condition: str  # Human-readable condition
    sql_expression: Optional[str] = None
    confidence: float = 0.0
    support: int = 0  # Number of rows supporting
    exceptions: int = 0  # Number of violations
    severity: IssueSeverity = IssueSeverity.MEDIUM
    suggested_action: str = "review"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class BusinessLogicConfig:
    """Configuration for business logic discovery."""
    min_confidence: float = 0.7
    min_support: int = 10
    max_exception_rate: float = 0.05
    enable_fds: bool = True  # Functional dependencies
    enable_conditionals: bool = True
    enable_aggregations: bool = True
    enable_state_machines: bool = True
    enable_range_rules: bool = True
    max_columns_per_rule: int = 4


class BusinessLogicEngine:
    """Discovers business logic rules from data patterns."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.bl_config = BusinessLogicConfig(
            min_confidence=getattr(config, 'bl_min_confidence', 0.7),
            min_support=getattr(config, 'bl_min_support', 10),
        )
    
    async def discover(
        self, 
        profiles: Dict[str, DatasetProfile], 
        issues: Dict[str, List[Any]], 
        relationships: List[Any],
        business_rules: Dict[str, Any]
    ) -> List[DiscoveredRule]:
        """Discover business logic rules from data patterns."""
        
        with get_performance_logger("business_logic_discovery") as perf:
            
            all_rules = []
            
            # Merge business rules from config
            declared_rules = self._parse_declared_rules(business_rules)
            perf.log_metric("declared_rules", len(declared_rules))
            
            for ds_name, profile in profiles.items():
                # We need the actual data for pattern discovery
                # In practice, this would be passed or fetched
                # For now, we discover from profile statistics
                ds_rules = await self._discover_dataset_rules(ds_name, profile)
                all_rules.extend(ds_rules)
            
            # Cross-dataset rules
            if len(profiles) > 1:
                cross_rules = await self._discover_cross_dataset_rules(profiles)
                all_rules.extend(cross_rules)
            
            # Merge with declared rules
            all_rules = self._merge_with_declared(all_rules, declared_rules)
            
            # Filter by confidence
            filtered = [r for r in all_rules if r.confidence >= self.bl_config.min_confidence]
            
            perf.log_metric("total_rules_discovered", len(all_rules))
            perf.log_metric("rules_after_filter", len(filtered))
            
            return filtered
    
    async def _discover_dataset_rules(self, ds_name: str, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Discover rules within a single dataset."""
        
        rules = []
        
        # 1. Functional Dependencies (A -> B)
        if self.bl_config.enable_fds:
            fds = await self._discover_functional_dependencies(profile)
            rules.extend(fds)
        
        # 2. Conditional Constraints (IF A THEN B)
        if self.bl_config.enable_conditionals:
            conditionals = await self._discover_conditional_constraints(profile)
            rules.extend(conditionals)
        
        # 3. Range/Domain Rules
        if self.bl_config.enable_range_rules:
            ranges = await self._discover_range_rules(profile)
            rules.extend(ranges)
        
        # 4. Value Dependencies (A determines values of B)
        value_deps = await self._discover_value_dependencies(profile)
        rules.extend(value_deps)
        
        # 5. State Machine Detection (for status columns)
        state_machines = await self._discover_state_machines(profile)
        rules.extend(state_machines)
        
        return rules
    
    async def _discover_functional_dependencies(self, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Discover functional dependencies A -> B where A uniquely determines B."""
        
        rules = []
        
        # Need actual data to verify - this is a statistical approximation
        # Real implementation would check: for each value of A, B has single value
        
        # Candidate: High cardinality column -> other columns
        pk_candidates = [name for name, col in profile.columns.items() 
                        if col.is_primary_key_candidate]
        
        for pk in pk_candidates:
            pk_col = profile.columns[pk]
            
            # Check other columns for dependency
            for other_name, other_col in profile.columns.items():
                if other_name == pk:
                    continue
                
                # Heuristic: if pk is unique and other has low distinct count relative to pk
                if pk_col.distinct_count > 0:
                    dep_ratio = other_col.distinct_count / pk_col.distinct_count
                    
                    # If other has much lower cardinality, likely dependent
                    if dep_ratio < 0.1 and other_col.distinct_count < 50:
                        rule = DiscoveredRule(
                            id=str(uuid.uuid4())[:8],
                            name=f"fd_{pk}_determines_{other_name}",
                            rule_type="functional_dependency",
                            description=f"{pk} functionally determines {other_name}",
                            columns=[pk, other_name],
                            datasets=[profile.name],
                            condition=f"{pk} -> {other_name}",
                            confidence=0.7,
                            support=profile.row_count,
                            metadata={"dep_ratio": dep_ratio},
                        )
                        rules.append(rule)
        
        return rules
    
    async def _discover_conditional_constraints(self, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Discover IF-THEN constraints: IF condition THEN constraint."""
        
        rules = []
        
        # Find categorical columns that might control other columns
        cat_cols = [name for name, col in profile.columns.items() 
                   if col.semantic_type in ['category', 'status', 'category']]
        
        for cat_col in cat_cols:
            # For each value in categorical column, check if it implies constraints on other columns
            # This is statistical - real implementation needs actual data
            
            # Heuristic: status columns often control date columns
            if 'status' in cat_col.lower() or 'state' in cat_col.lower():
                date_cols = [name for name, col in profile.columns.items() 
                           if col.semantic_type in ['date', 'datetime', 'timestamp']]
                
                for date_col in date_cols:
                    # Example: IF status='shipped' THEN ship_date NOT NULL
                    rule = DiscoveredRule(
                        id=str(uuid.uuid4())[:8],
                        name=f"conditional_{cat_col}_requires_{date_col}",
                        rule_type="conditional",
                        description=f"When {cat_col} indicates completion, {date_col} must be present",
                        columns=[cat_col, date_col],
                        datasets=[profile.name],
                        condition=f"IF {cat_col} in ('shipped','delivered','completed') THEN {date_col} IS NOT NULL",
                        confidence=0.75,
                        suggested_action="validate_conditional",
                    )
                    rules.append(rule)
        
        return rules
    
    async def _discover_range_rules(self, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Discover valid value ranges for numeric columns."""
        
        rules = []
        
        for col_name, col in profile.columns.items():
            if col.mean is not None and col.std_dev is not None and col.std_dev > 0:
                # Statistical range (mean ± 3 std)
                lower = col.mean - 3 * col.std_dev
                upper = col.mean + 3 * col.std_dev
                
                # Semantic-aware bounds
                if col.semantic_type in ['currency', 'price', 'amount'] and col.min_value is not None:
                    if col.min_value < 0:
                        # Negative prices might be invalid
                        rule = DiscoveredRule(
                            id=str(uuid.uuid4())[:8],
                            name=f"range_{col_name}_non_negative",
                            rule_type="range",
                            description=f"{col_name} should not be negative",
                            columns=[col_name],
                            datasets=[profile.name],
                            condition=f"{col_name} >= 0",
                            confidence=0.85,
                            severity=IssueSeverity.HIGH,
                            suggested_action="range_clip",
                            suggested_parameters={"min": 0},
                        )
                        rules.append(rule)
                
                # Percentage bounds
                if col.semantic_type == 'percentage' or 'pct' in col_name.lower():
                    rule = DiscoveredRule(
                        id=str(uuid.uuid4())[:8],
                        name=f"range_{col_name}_percentage",
                        rule_type="range",
                        description=f"{col_name} should be between 0 and 100",
                        columns=[col_name],
                        datasets=[profile.name],
                        condition=f"{col_name} BETWEEN 0 AND 100",
                        confidence=0.9,
                        severity=IssueSeverity.HIGH,
                        suggested_action="range_clip",
                        suggested_parameters={"min": 0, "max": 100},
                    )
                    rules.append(rule)
                
                # Age bounds
                if col.semantic_type == 'age' or 'age' in col_name.lower():
                    rule = DiscoveredRule(
                        id=str(uuid.uuid4())[:8],
                        name=f"range_{col_name}_age",
                        rule_type="range",
                        description=f"Age should be between 0 and 150",
                        columns=[col_name],
                        datasets=[profile.name],
                        condition=f"{col_name} BETWEEN 0 AND 150",
                        confidence=0.95,
                        severity=IssueSeverity.HIGH,
                        suggested_action="range_clip",
                        suggested_parameters={"min": 0, "max": 150},
                    )
                    rules.append(rule)
        
        return rules
    
    async def _discover_value_dependencies(self, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Discover value-level dependencies (A=value1 -> B=value2)."""
        
        rules = []
        
        # Find boolean/flag columns that might control other columns
        flag_cols = [name for name, col in profile.columns.items() 
                    if col.semantic_type in ['boolean', 'flag'] or 
                       'is_' in name.lower() or 'has_' in name.lower() or
                       'flag' in name.lower()]
        
        for flag_col in flag_cols:
            # Look for columns that might be controlled by this flag
            for other_name, other_col in profile.columns.items():
                if other_name == flag_col:
                    continue
                
                # Heuristic: if flag column has low cardinality (mostly 0/1)
                # and other column has meaningful variance
                pass
        
        return rules
    
    async def _discover_state_machines(self, profile: DatasetProfile) -> List[DiscoveredRule]:
        """Detect state machine patterns in status columns."""
        
        rules = []
        
        status_cols = [name for name, col in profile.columns.items() 
                      if col.semantic_type == 'status' or 
                         'status' in name.lower() or 'state' in name.lower()]
        
        for status_col in status_cols:
            col = profile.columns[status_col]
            
            # Known state machines
            known_machines = {
                'order': ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled'],
                'payment': ['pending', 'authorized', 'captured', 'refunded', 'failed'],
                'shipment': ['pending', 'picked', 'packed', 'shipped', 'in_transit', 'delivered', 'returned'],
                'ticket': ['open', 'in_progress', 'resolved', 'closed'],
            }
            
            # Try to match
            col_lower = status_col.lower()
            for machine_name, states in known_machines.items():
                if machine_name in col_lower or any(s in col_lower for s in states):
                    # Check if values match
                    # Would need actual data
                    rule = DiscoveredRule(
                        id=str(uuid.uuid4())[:8],
                        name=f"state_machine_{status_col}",
                        rule_type="state_machine",
                        description=f"{status_col} follows {machine_name} state machine",
                        columns=[status_col],
                        datasets=[profile.name],
                        condition=f"{status_col} must follow valid transitions: {' -> '.join(states)}",
                        confidence=0.8,
                        suggested_action="validate_state_transitions",
                        metadata={"states": states, "machine_type": machine_name},
                    )
                    rules.append(rule)
                    break
        
        return rules
    
    async def _discover_cross_dataset_rules(self, profiles: Dict[str, DatasetProfile]) -> List[DiscoveredRule]:
        """Discover rules spanning multiple datasets."""
        
        rules = []
        
        # 1. Referential integrity rules (from relationships)
        # 2. Aggregation consistency (detail -> header)
        # 3. Duplicate detection across datasets
        
        ds_names = list(profiles.keys())
        
        # Check for aggregation patterns (e.g., order_header vs order_lines)
        for i, ds1 in enumerate(ds_names):
            for ds2 in ds_names[i+1:]:
                p1, p2 = profiles[ds1], profiles[ds2]
                
                # Check if one might be header, other detail
                # Look for shared key columns
                common_keys = set(p1.columns.keys()) & set(p2.columns.keys())
                pk_candidates_1 = set(p1.primary_keys)
                pk_candidates_2 = set(p2.primary_keys)
                
                # If ds1 key is in ds2 as FK, might be header-detail
                for pk in pk_candidates_1:
                    if pk in p2.columns and not pk in pk_candidates_2:
                        # Likely header-detail
                        rule = DiscoveredRule(
                            id=str(uuid.uuid4())[:8],
                            name=f"aggregation_{ds1}_to_{ds2}",
                            rule_type="aggregation",
                            description=f"{ds1} aggregates to {ds2} via {pk}",
                            columns=[pk],
                            datasets=[ds1, ds2],
                            condition=f"SUM({ds2}.amount) = {ds1}.total_amount WHERE {ds1}.{pk} = {ds2}.{pk}",
                            confidence=0.7,
                            suggested_action="validate_aggregation",
                        )
                        rules.append(rule)
        
        return rules
    
    def _parse_declared_rules(self, business_rules: Dict[str, Any]) -> List[DiscoveredRule]:
        """Parse declared business rules from config."""
        
        rules = []
        
        # Valid values
        for col, values in business_rules.get("valid_values", {}).items():
            rule = DiscoveredRule(
                id=f"declared_enum_{col}",
                name=f"enum_{col}",
                rule_type="enum",
                description=f"{col} must be one of: {values}",
                columns=[col],
                datasets=[],
                condition=f"{col} IN ({','.join(repr(v) for v in values)})",
                confidence=1.0,
                severity=IssueSeverity.HIGH,
            )
            rules.append(rule)
        
        # Formula rules
        for formula in business_rules.get("formula_rules", []):
            rule = DiscoveredRule(
                id=f"declared_formula_{formula.get('id', str(uuid.uuid4())[:8])}",
                name=f"formula_{formula.get('assertion', 'custom')[:20]}",
                rule_type="formula",
                description=formula.get("message", "Custom formula rule"),
                columns=[],
                datasets=[],
                condition=formula.get("assertion", ""),
                confidence=1.0,
                severity=IssueSeverity(formula.get("severity", "medium")),
            )
            rules.append(rule)
        
        return rules
    
    def _merge_with_declared(self, discovered: List[DiscoveredRule], declared: List[DiscoveredRule]) -> List[DiscoveredRule]:
        """Merge discovered and declared rules, handling conflicts."""
        
        # For now, just combine
        all_rules = discovered + declared
        
        # Deduplicate by name
        seen = set()
        unique = []
        for rule in all_rules:
            key = (rule.name, tuple(rule.columns))
            if key not in seen:
                seen.add(key)
                unique.append(rule)
        
        return unique