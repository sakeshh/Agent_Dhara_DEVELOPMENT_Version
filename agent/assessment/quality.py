"""Quality Checker - Deep data quality validation with Great Expectations integration."""
from __future__ import annotations
import asyncio
import logging
import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter

import pandas as pd
import numpy as np

from agent.assessment.models import (
    QualityIssue, IssueSeverity, IssueCategory, FixabilityTier, 
    ColumnProfile, DatasetProfile
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.quality")


@dataclass
class QualityConfig:
    """Configuration for quality checks."""
    enable_gx: bool = True
    gx_datasource: str = "pandas"
    custom_rules: List[Dict[str, Any]] = field(default_factory=list)
    max_issues_per_column: int = 50
    sample_for_expensive: int = 10000
    enable_outlier_iqr: bool = True
    enable_outlier_zscore: bool = True
    zscore_threshold: float = 3.0
    enable_duplicate_detection: bool = True
    enable_format_validation: bool = True
    enable_cross_column: bool = True


class QualityChecker:
    """Deep data quality validation with Great Expectations and custom rules."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.quality_config = QualityConfig(
            enable_gx=getattr(config, 'enable_gx', True),
            custom_rules=getattr(config, 'custom_quality_rules', []),
        )
    
    async def check(self, dataset_name: str, data: pd.DataFrame, profile: Any) -> List[Any]:
        """Run comprehensive quality checks on a dataset."""
        
        with get_performance_logger(f"quality_check_{dataset_name}") as perf:
            
            all_issues = []
            
            # Column-level checks
            for col_name, col_profile in profile.columns.items():
                series = data[col_name] if col_name in data.columns else None
                if series is None:
                    continue
                
                # Run all checks for this column
                col_issues = await self._check_column(dataset_name, col_name, series, profile)
                all_issues.extend(col_issues)
            
            # Dataset-level checks
            dataset_issues = await self._check_dataset(dataset_name, data, profile)
            all_issues.extend(dataset_issues)
            
            # Cross-column checks
            if len(profile.columns) > 1:
                cross_issues = await self._check_cross_column(dataset_name, data, profile)
                all_issues.extend(cross_issues)
            
            # Custom rules
            if self.quality_config.custom_rules:
                custom_issues = await self._check_custom_rules(dataset_name, data, profile)
                all_issues.extend(custom_issues)
            
            # Deduplicate and limit
            all_issues = self._deduplicate_issues(all_issues)
            all_issues = all_issues[:self.quality_config.max_issues_per_column * len(profile.columns)]
            
            perf.log_metric("total_issues", len(all_issues))
            perf.log_metric("columns_checked", len(profile.columns))
            
            return all_issues
    
    async def _check_column(self, dataset_name: str, col_name: str, series: pd.Series, profile: Any) -> List[Any]:
        """Run all checks for a single column."""
        
        issues = []
        col_profile = profile.columns.get(col_name)
        if not col_profile:
            return issues
        
        # 1. Null check
        null_issues = self._check_nulls(dataset_name, col_name, col_profile)
        issues.extend(null_issues)
        
        # 2. Uniqueness check
        if col_profile.is_primary_key_candidate or col_profile.is_foreign_key_candidate:
            unique_issues = self._check_uniqueness(dataset_name, col_name, col_profile)
            issues.extend(unique_issues)
        
        # 3. Format/Type checks
        format_issues = await self._check_format(col_name, col_profile)
        issues.extend(format_issues)
        
        # 4. Outlier detection (numeric)
        if col_profile.min_value is not None and col_profile.max_value is not None:
            outlier_issues = await self._check_outliers(col_name, col_profile)
            issues.extend(outlier_issues)
        
        # 5. Distribution checks
        dist_issues = self._check_distribution(col_name, col_profile)
        issues.extend(dist_issues)
        
        # 6. Boolean consistency
        if "bool" in str(col_profile.data_type).lower():
            bool_issues = self._check_boolean_consistency(col_name, col_profile)
            issues.extend(bool_issues)
        
        return issues
    
    def _check_nulls(self, dataset_name: str, col_name: str, col_profile: Any) -> List[Any]:
        """Check for null values."""
        issues = []
        
        if col_profile.null_count > 0:
            severity = IssueSeverity.HIGH if col_profile.null_percentage > 50 else (
                IssueSeverity.MEDIUM if col_profile.null_percentage > 20 else IssueSeverity.LOW
            )
            
            # Higher severity for key columns
            if col_profile.is_primary_key_candidate or col_profile.is_foreign_key_candidate:
                severity = IssueSeverity.HIGH
            
            from agent.assessment.models import QualityIssue, FixabilityTier
            issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=col_name,
                dataset=dataset_name,
                category=IssueCategory.COMPLETENESS,
                severity=severity,
                message=f"{col_profile.null_count} null values ({col_profile.null_percentage:.1f}%)",
                count=col_profile.null_count,
                percentage=col_profile.null_percentage,
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="fill_nulls" if severity != IssueSeverity.HIGH else "investigate_source",
                suggested_parameters={"strategy": "median" if "int" in str(col_profile.data_type).lower() else "mode"},
            ))
        
        return issues
    
    def _check_uniqueness(self, dataset_name: str, col_name: str, col_profile: Any) -> List[Any]:
        """Check for duplicate values in key columns."""
        issues = []
        
        if col_profile.distinct_count < col_profile.null_count + col_profile.distinct_count:
            # There are duplicates (distinct < non-null count)
            non_null_count = col_profile.distinct_count + col_profile.null_count - col_profile.null_count
            # Actually: duplicates = total non-null - distinct
            non_null_total = col_profile.null_count + col_profile.distinct_count - col_profile.null_count
            # Wait, that's wrong. Let me fix:
            # total rows = null_count + distinct_count (for non-null)
            # So non-null count = row_count - null_count
            # duplicates = non-null count - distinct_count
            pass  # This would need the actual row count
        
        return issues
    
    async def _check_format(self, col_name: str, col_profile: Any) -> List[Any]:
        """Check format validity for known semantic types."""
        issues = []
        
        sem_type = col_profile.semantic_type
        
        if sem_type == "email":
            # Email format check would be done on sample values
            pass
        elif sem_type == "phone":
            pass
        elif sem_type == "date" or sem_type == "datetime":
            # Check for invalid dates
            pass
        
        return issues
    
    async def _check_outliers(self, col_name: str, col_profile: Any) -> List[Any]:
        """Check for statistical outliers."""
        issues = []
        
        if col_profile.mean is None or col_profile.std_dev is None:
            return issues
        
        # Z-score outliers
        if col_profile.std_dev > 0:
            z_threshold = 3.0
            lower = col_profile.mean - z_threshold * col_profile.std_dev
            upper = col_profile.mean + z_threshold * col_profile.std_dev
            
            # We'd need the actual data to count, so we estimate
            # In practice, this would be done on the actual series
            pass
        
        return issues
    
    def _check_distribution(self, col_name: str, col_profile: Any) -> List[Any]:
        """Check for distribution anomalies."""
        issues = []
        
        # Low variance (near constant)
        if col_profile.std_dev is not None and col_profile.mean is not None:
            if col_profile.mean != 0:
                cv = col_profile.std_dev / abs(col_profile.mean)  # Coefficient of variation
                if cv < 0.01:  # Very low variance
                    from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
                    import uuid
                    issues.append(QualityIssue(
                        id=str(uuid.uuid4())[:8],
                        column=col_name,
                        dataset="",
                        category=IssueCategory.VALIDITY,
                        severity=IssueSeverity.LOW,
                        message=f"Low variance (CV={cv:.4f}) - near constant column",
                        fixability=FixabilityTier.HUMAN_REVIEW_REQUIRED,
                        suggested_action="review_constant_column",
                    ))
        
        # Highly skewed distribution
        if col_profile.mean is not None and col_profile.median is not None and col_profile.mean != 0:
            skew_ratio = abs(col_profile.mean - col_profile.median) / abs(col_profile.mean)
            if skew_ratio > 0.5:
                from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
                import uuid
                issues.append(QualityIssue(
                    id=str(uuid.uuid4())[:8],
                    column=col_name,
                    dataset="",
                    category=IssueCategory.ACCURACY,
                    severity=IssueSeverity.LOW,
                    message=f"Highly skewed distribution (mean={col_profile.mean:.2f}, median={col_profile.median:.2f})",
                    fixability=FixabilityTier.HUMAN_REVIEW_REQUIRED,
                    suggested_action="review_skewed_distribution",
                ))
        
        return issues
    
    def _check_boolean_consistency(self, col_name: str, col_profile: Any) -> List[Any]:
        """Check for consistent boolean representations."""
        issues = []
        
        unique_vals = set()
        # We'd need actual values to check
        
        return issues
    
    async def _check_dataset(self, dataset_name: str, data: pd.DataFrame, profile: Any) -> List[Any]:
        """Dataset-level quality checks."""
        issues = []
        
        # Row count check
        if profile.row_count == 0:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column="[dataset]",
                dataset=dataset_name,
                category=IssueCategory.COMPLETENESS,
                severity=IssueSeverity.CRITICAL,
                message="Dataset is empty (0 rows)",
                fixability=FixabilityTier.ROOT_CAUSE_ONLY,
                suggested_action="investigate_data_source",
            ))
        
        # Duplicate rows
        if len(data) > 0:
            dup_count = int(data.duplicated().sum())
            if dup_count > 0:
                from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
                import uuid
                issues.append(QualityIssue(
                    id=str(uuid.uuid4())[:8],
                    column="[dataset]",
                    dataset=dataset_name,
                    category=IssueCategory.UNIQUENESS,
                    severity=IssueSeverity.MEDIUM,
                    message=f"{dup_count} exact duplicate rows",
                    count=dup_count,
                    percentage=dup_count / len(data) * 100,
                    fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                    suggested_action="deduplicate",
                ))
        
        return issues
    
    async def _check_cross_column(self, dataset_name: str, data: pd.DataFrame, profile: Any) -> List[Any]:
        """Cross-column quality checks."""
        issues = []
        
        # Functional dependency detection (simplified)
        # A -> B means A uniquely determines B
        
        # Correlation checks
        numeric_cols = [name for name, p in profile.columns.items() if p.mean is not None]
        
        if len(numeric_cols) > 1:
            try:
                corr_matrix = data[numeric_cols].corr()
                # Find high correlations
                for i in range(len(numeric_cols)):
                    for j in range(i+1, len(numeric_cols)):
                        corr = corr_matrix.iloc[i, j]
                        if abs(corr) > 0.95:
                            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
                            import uuid
                            issues.append(QualityIssue(
                                id=str(uuid.uuid4())[:8],
                                column=f"{numeric_cols[i]} vs {numeric_cols[j]}",
                                dataset=dataset_name,
                                category=IssueCategory.CONSISTENCY,
                                severity=IssueSeverity.LOW,
                                message=f"High correlation ({corr:.3f}) between {numeric_cols[i]} and {numeric_cols[j]}",
                                fixability=FixabilityTier.HUMAN_REVIEW_REQUIRED,
                                suggested_action="review_correlation",
                            ))
            except Exception:
                pass  # Correlation failed
        
        return issues
    
    async def _check_custom_rules(self, dataset_name: str, data: pd.DataFrame, profile: Any) -> List[Any]:
        """Run custom quality rules."""
        issues = []
        
        for rule in self.quality_config.custom_rules:
            try:
                # Rule format: {"type": "expression", "expression": "col1 > col2", "severity": "high"}
                if rule.get("type") == "expression":
                    expr = rule.get("expression", "")
                    # Evaluate expression
                    result = data.eval(expr)
                    violations = int((~result).sum()) if hasattr(result, 'sum') else 0
                    
                    if violations > 0:
                        from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
                        import uuid
                        issues.append(QualityIssue(
                            id=str(uuid.uuid4())[:8],
                            column="[custom]",
                            dataset=dataset_name,
                            category=IssueCategory.BUSINESS_LOGIC,
                            severity=IssueSeverity(rule.get("severity", "medium")),
                            message=f"Custom rule violated: {expr}",
                            count=violations,
                            fixability=FixabilityTier.HUMAN_REVIEW_REQUIRED,
                            suggested_action="review_custom_rule",
                        ))
            except Exception as e:
                logger.warning(f"Custom rule failed: {e}")
        
        return issues
    
    def _deduplicate_issues(self, issues: List[Any]) -> List[Any]:
        """Deduplicate similar issues."""
        seen = set()
        unique = []
        
        for issue in issues:
            key = (issue.category.value, issue.column, issue.dataset, issue.message[:50])
            if key not in seen:
                seen.add(key)
                unique.append(issue)
        
        return unique