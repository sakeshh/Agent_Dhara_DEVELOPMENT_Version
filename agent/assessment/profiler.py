"""Data Profiler - Fast statistical profiling with SQL pushdown support."""
from __future__ import annotations
import asyncio
import logging
import hashlib
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import Counter

from agent.assessment.models import ColumnProfile, DatasetProfile, SemanticType
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.profiler")


@dataclass
class ProfilerConfig:
    max_rows: int = 100000
    sample_size: int = 10000
    compute_quantiles: bool = True
    compute_correlations: bool = False
    correlation_threshold: float = 0.8
    pattern_sample_size: int = 1000
    quantiles: List[float] = field(default_factory=lambda: [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])


class DataProfiler:
    """Fast statistical profiler with SQL pushdown support."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.profiler_config = ProfilerConfig(
            max_rows=getattr(config, 'max_rows_per_dataset', 100000),
            sample_size=getattr(config, 'assessment_sample_size', 10000),
        )
    
    async def profile(self, data: Any, name: str) -> DatasetProfile:
        """Profile a dataset and return DatasetProfile."""
        
        with get_performance_logger(f"profile_{name}") as perf:
            
            # Convert to pandas if needed
            df = await self._to_dataframe(data)
            
            if df is None or len(df) == 0:
                return DatasetProfile(name=name, row_count=0, column_count=0)
            
            # Sample if needed
            if len(df) > self.profiler_config.max_rows:
                df = df.sample(n=self.profiler_config.max_rows, random_state=42)
                sampled = True
            else:
                sampled = False
            
            # Profile each column (parallel)
            column_profiles = {}
            tasks = []
            
            for col_name in df.columns:
                task = self._profile_column(df[col_name], col_name)
                tasks.append((col_name, task))
            
            # Execute profiling tasks
            for col_name, task in tasks:
                try:
                    col_profile = await task
                    column_profiles[col_name] = col_profile
                except Exception as e:
                    logger.error(f"Failed to profile column {col_name}: {e}")
                    column_profiles[col_name] = ColumnProfile(
                        name=col_name,
                        data_type=str(df[col_name].dtype),
                        semantic_type=SemanticType.UNKNOWN
                    )
            
            # Detect primary key candidates
            pk_candidates = self._detect_pk_candidates(column_profiles, len(df))
            
            # Build dataset profile
            profile = DatasetProfile(
                name=name,
                row_count=len(df),
                column_count=len(df.columns),
                size_bytes=df.memory_usage(deep=True).sum() if hasattr(df, 'memory_usage') else 0,
                columns=column_profiles,
                primary_keys=pk_candidates,
                profiled_at=datetime.utcnow(),
                sample_size=self.profiler_config.sample_size if sampled else None,
            )
            
            # Calculate quality summary
            profile.total_issues = sum(len(c.issues) for c in column_profiles.values())
            profile.issues_by_severity = self._aggregate_issues_by_severity(column_profiles)
            profile.issues_by_category = self._aggregate_issues_by_category(column_profiles)
            
            perf.log_metric("columns_profiled", len(column_profiles))
            perf.log_metric("pk_candidates", len(pk_candidates))
            
            return profile
    
    async def _to_dataframe(self, data: Any) -> Optional[pd.DataFrame]:
        """Convert various data formats to pandas DataFrame."""
        if data is None:
            return None
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, dict):
            return pd.DataFrame(data)
        if isinstance(data, list):
            return pd.DataFrame(data)
        # Could add pyarrow, polars, etc.
        return None
    
    async def _profile_column(self, series: pd.Series, name: str) -> ColumnProfile:
        """Profile a single column."""
        
        dtype = str(series.dtype)
        null_count = int(series.isnull().sum())
        total_count = len(series)
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0
        
        # Non-null series for stats
        non_null = series.dropna()
        distinct_count = int(non_null.nunique())
        distinct_pct = (distinct_count / total_count * 100) if total_count > 0 else 0
        
        profile = ColumnProfile(
            name=name,
            data_type=dtype,
            null_count=null_count,
            null_percentage=null_pct,
            distinct_count=distinct_count,
            distinct_percentage=distinct_pct,
        )
        
        # Numeric profiling
        if self._is_numeric(series):
            await self._profile_numeric(profile, non_null)
        
        # Text profiling
        elif self._is_text(series):
            await self._profile_text(profile, non_null)
        
        # Date profiling
        elif self._is_datetime(series):
            await self._profile_datetime(profile, non_null)
        
        # Boolean
        elif self._is_boolean(series):
            await self._profile_boolean(profile, non_null)
        
        # PK candidate detection
        profile.is_primary_key_candidate = (
            distinct_count == total_count and null_count == 0
        )
        
        return profile
    
    def _is_numeric(self, series: pd.Series) -> bool:
        return pd.api.types.is_numeric_dtype(series)
    
    def _is_text(self, series: pd.Series) -> bool:
        return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)
    
    def _is_datetime(self, series: pd.Series) -> bool:
        return pd.api.types.is_datetime64_any_dtype(series)
    
    def _is_boolean(self, series: pd.Series) -> bool:
        return pd.api.types.is_bool_dtype(series)
    
    async def _profile_numeric(self, profile: ColumnProfile, series: pd.Series) -> None:
        """Profile numeric column statistics."""
        if len(series) == 0:
            return
        
        profile.min_value = float(series.min())
        profile.max_value = float(series.max())
        profile.mean = float(series.mean())
        profile.median = float(series.median())
        profile.std_dev = float(series.std()) if len(series) > 1 else 0.0
        
        # Quantiles
        if self.profiler_config.compute_quantiles:
            for q in self.profiler_config.quantiles:
                profile.quantiles[f"q{int(q*100)}"] = float(series.quantile(q))
        
        # Outlier detection (IQR)
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        
        outliers = series[(series < lower) | (series > upper)]
        if len(outliers) > 0:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",  # Filled later
                category=IssueCategory.ACCURACY,
                severity=IssueSeverity.MEDIUM,
                message=f"{len(outliers)} outliers detected (IQR method)",
                count=len(outliers),
                percentage=len(outliers) / len(series) * 100,
                sample_values=outliers.head(5).tolist(),
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="range_clip",
                suggested_parameters={"lower_bound": float(lower), "upper_bound": float(upper)},
            ))
    
    async def _profile_text(self, profile: ColumnProfile, series: pd.Series) -> None:
        """Profile text column statistics."""
        if len(series) == 0:
            return
        
        # Length stats
        lengths = series.str.len()
        profile.min_length = int(lengths.min())
        profile.max_length = int(lengths.max())
        profile.avg_length = float(lengths.mean())
        
        # Pattern detection (sample for performance)
        sample = series.head(self.profiler_config.pattern_sample_size)
        
        patterns = Counter()
        for val in sample:
            if isinstance(val, str):
                # Detect common patterns
                if '@' in val and '.' in val:
                    patterns['email'] += 1
                elif val.replace('-', '').replace(' ', '').isdigit():
                    patterns['phone_like'] += 1
                elif val.startswith('http'):
                    patterns['url'] += 1
                elif val.isdigit():
                    patterns['numeric_string'] += 1
                elif val.isupper():
                    patterns['uppercase'] += 1
                elif val.islower():
                    patterns['lowercase'] += 1
        
        profile.pattern_distribution = dict(patterns)
        
        # Top values
        top = series.value_counts().head(10)
        profile.top_values = [{"value": str(k), "count": int(v)} for k, v in top.items()]
        
        # Whitespace check
        has_leading_trailing = series.str.match(r'^\s|\s$').any()
        has_internal = series.str.contains(r'\s{2,}').any()
        
        if has_leading_trailing:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",
                category=IssueCategory.VALIDITY,
                severity=IssueSeverity.LOW,
                message="Leading/trailing whitespace detected",
                fixability=FixabilityTier.AUTO_FIXABLE,
                suggested_action="trim",
            ))
        
        if has_internal:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",
                category=IssueCategory.VALIDITY,
                severity=IssueSeverity.LOW,
                message="Internal whitespace (multiple spaces) detected",
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="trim_internal",
            ))
    
    async def _profile_datetime(self, profile: ColumnProfile, series: pd.Series) -> None:
        """Profile datetime column statistics."""
        if len(series) == 0:
            return
        
        profile.min_value = series.min().isoformat() if not pd.isna(series.min()) else None
        profile.max_value = series.max().isoformat() if not pd.isna(series.max()) else None
        
        # Check for future dates
        now = pd.Timestamp.now()
        future_count = int((series > now).sum())
        if future_count > 0:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",
                category=IssueCategory.ACCURACY,
                severity=IssueSeverity.MEDIUM,
                message=f"{future_count} future dates detected",
                count=future_count,
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="validate_date_range",
            ))
        
        # Check for ancient dates
        ancient = pd.Timestamp('1900-01-01')
        ancient_count = int((series < ancient).sum())
        if ancient_count > 0:
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",
                category=IssueCategory.ACCURACY,
                severity=IssueSeverity.MEDIUM,
                message=f"{ancient_count} potentially invalid ancient dates",
                count=ancient_count,
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="validate_date_range",
            ))
    
    async def _profile_boolean(self, profile: ColumnProfile, series: pd.Series) -> None:
        """Profile boolean column."""
        if len(series) == 0:
            return
        
        # Value distribution
        vc = series.value_counts()
        profile.top_values = [{"value": str(k), "count": int(v)} for k, v in vc.head(10).items()]
        
        # Check for inconsistent representations
        unique_vals = set(str(v).lower() for v in series.dropna().unique())
        bool_vals = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 't', 'f'}
        if not unique_vals.issubset(bool_vals):
            from agent.assessment.models import QualityIssue, IssueSeverity, IssueCategory, FixabilityTier
            import uuid
            profile.issues.append(QualityIssue(
                id=str(uuid.uuid4())[:8],
                column=profile.name,
                dataset="",
                category=IssueCategory.CONFORMITY,
                severity=IssueSeverity.LOW,
                message=f"Inconsistent boolean representations: {unique_vals}",
                fixability=FixabilityTier.CONDITIONALLY_FIXABLE,
                suggested_action="standardize_boolean",
            ))
    
    def _detect_pk_candidates(self, profiles: Dict[str, ColumnProfile], row_count: int) -> List[str]:
        """Detect primary key candidates."""
        candidates = []
        for name, profile in profiles.items():
            if (profile.is_primary_key_candidate and 
                profile.null_count == 0 and 
                profile.distinct_count == row_count):
                candidates.append(name)
        return candidates
    
    def _aggregate_issues_by_severity(self, profiles: Dict[str, ColumnProfile]) -> Dict[str, int]:
        counts = {}
        for profile in profiles.values():
            for issue in profile.issues:
                counts[issue.severity.value] = counts.get(issue.severity.value, 0) + 1
        return counts
    
    def _aggregate_issues_by_category(self, profiles: Dict[str, ColumnProfile]) -> Dict[str, int]:
        counts = {}
        for profile in profiles.values():
            for issue in profile.issues:
                counts[issue.category.value] = counts.get(issue.category.value, 0) + 1
        return counts