"""Drift Detector - Schema, distribution, and quality drift detection across runs."""
from __future__ import annotations
import asyncio
import logging
import uuid
import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from enum import Enum

import pandas as pd
import numpy as np
from scipy import stats

from agent.assessment.models import (
    DatasetProfile, ColumnProfile, QualityIssue, IssueSeverity, 
    IssueCategory, FixabilityTier
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("assessment.drift")


class DriftType(Enum):
    SCHEMA = "schema"
    DISTRIBUTION = "distribution"
    QUALITY = "quality"
    SEMANTIC = "semantic"
    VOLUME = "volume"
    RELATIONSHIP = "relationship"


class DriftSeverity(Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DriftSignal:
    """A single drift detection signal."""
    drift_type: DriftType
    severity: DriftSeverity
    dataset: str
    column: Optional[str]
    message: str
    metric_name: str
    current_value: Any
    baseline_value: Any
    drift_score: float  # 0-1
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.utcnow)
    
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


@dataclass
class DriftReport:
    """Complete drift detection report."""
    run_id: str
    baseline_run_id: Optional[str]
    detected_at: datetime
    signals: List[DriftSignal] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    datasets_checked: List[str] = field(default_factory=list)
    overall_drift_score: float = 0.0
    recommendation: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, list):
                result[key] = [v.to_dict() if hasattr(v, 'to_dict') else v for v in value]
            elif hasattr(value, 'to_dict'):
                result[key] = value.to_dict()
            elif isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class DriftConfig:
    """Configuration for drift detection."""
    # Distribution drift
    psi_threshold: float = 0.2  # Population Stability Index
    ks_threshold: float = 0.05  # Kolmogorov-Smirnov p-value
    chi2_threshold: float = 0.05  # Chi-square p-value for categorical
    
    # Schema drift
    schema_change_threshold: float = 0.0  # Any schema change
    
    # Quality drift
    quality_drift_threshold: float = 0.15  # 15% change in issue count
    
    # Volume drift
    volume_drift_threshold: float = 0.5  # 50% volume change
    
    # Semantic drift
    semantic_drift_threshold: float = 0.3  # Semantic type change
    
    # Baselines
    baseline_lookback_days: int = 30
    min_baseline_runs: int = 3
    
    # Sampling
    sample_for_drift: int = 10000


class DriftDetector:
    """Detects drift across assessment runs."""
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.drift_config = DriftConfig(
            psi_threshold=getattr(config, 'drift_psi_threshold', 0.2),
            ks_threshold=getattr(config, 'drift_ks_threshold', 0.05),
            volume_drift_threshold=getattr(config, 'drift_volume_threshold', 0.5),
        )
    
    async def detect(self, current_profiles: Dict[str, DatasetProfile], run_id: str) -> Any:
        """Detect drift by comparing current profiles with historical baselines."""
        
        with get_performance_logger("drift_detection", run_id=run_id) as perf:
            
            if not self.memory:
                logger.warning("No memory store available for drift detection")
                return self._empty_report(run_id)
            
            # Load historical baselines
            baselines = await self._load_baselines(current_profiles)
            
            if not baselines:
                logger.info("No baselines available for drift detection")
                return self._empty_report(run_id, "No historical baselines available")
            
            signals = []
            datasets_checked = list(current_profiles.keys())
            
            for ds_name, current_profile in current_profiles.items():
                baseline = baselines.get(ds_name)
                if not baseline:
                    continue
                
                # Check each drift type
                ds_signals = await self._check_dataset_drift(
                    current_profile, baseline, ds_name
                )
                signals.extend(ds_signals)
            
            # Cross-dataset drift
            if len(current_profiles) > 1:
                cross_signals = await self._check_cross_dataset_drift(
                    current_profiles, baselines
                )
                signals.extend(cross_signals)
            
            # Build report
            report = self._build_report(run_id, signals, datasets_checked, baselines)
            
            # Store current as new baseline
            await self._store_baseline(current_profiles, run_id)
            
            perf.log_metric("signals_detected", len(signals))
            perf.log_metric("datasets_checked", len(datasets_checked))
            perf.log_metric("overall_drift_score", report.overall_drift_score)
            
            return report
    
    async def _load_baselines(self, current_profiles: Dict[str, DatasetProfile]) -> Dict[str, Any]:
        """Load historical baselines from memory."""
        
        baselines = {}
        dataset_names = list(current_profiles.keys())
        
        # Try to get from memory store
        if self.memory:
            try:
                for ds_name in dataset_names:
                    # Try to get baseline from memory
                    query = {
                        "types": ["DATA_PROFILE"],
                        "context_filters": {"dataset_name": ds_name},
                        "limit": self.drift_config.min_baseline_runs,
                        "sort_by": "recency"
                    }
                    # This would query the memory store
                    # For now, return empty - real implementation would query
                    pass
            except Exception as e:
                logger.warning(f"Failed to load baselines: {e}")
        
        return baselines
    
    async def _check_dataset_drift(
        self, 
        current: DatasetProfile, 
        baseline: Any, 
        ds_name: str
    ) -> List[Any]:
        """Check all drift types for a single dataset."""
        
        signals = []
        
        # 1. Schema drift
        schema_signals = self._check_schema_drift(current, baseline, ds_name)
        signals.extend(schema_signals)
        
        # 2. Volume drift
        volume_signals = self._check_volume_drift(current, baseline, ds_name)
        signals.extend(volume_signals)
        
        # 3. Distribution drift (per column)
        for col_name, current_col in current.columns.items():
            baseline_col = baseline.get("columns", {}).get(col_name)
            if not baseline_col:
                continue
            
            # Distribution drift
            dist_signals = self._check_distribution_drift(
                current_col, baseline_col, ds_name, col_name
            )
            signals.extend(dist_signals)
            
            # Quality drift
            quality_signals = self._check_quality_drift(
                current_col, baseline_col, ds_name, col_name
            )
            signals.extend(quality_signals)
            
            # Semantic drift
            semantic_signals = self._check_semantic_drift(
                current_col, baseline_col, ds_name, col_name
            )
            signals.extend(semantic_signals)
        
        return signals
    
    def _check_schema_drift(self, current: DatasetProfile, baseline: Any, ds_name: str) -> List[Any]:
        """Detect schema changes."""
        
        signals = []
        baseline_cols = baseline.get("columns", {})
        
        current_cols = set(current.columns.keys())
        baseline_cols_set = set(baseline_cols.keys())
        
        # Added columns
        added = current_cols - baseline_cols_set
        for col in added:
            signals.append(self._create_signal(
                DriftType.SCHEMA, DriftSeverity.HIGH, ds_name, col,
                f"New column added: {col}",
                "schema_change", "added", "not_existed",
                {"action": "added"}
            ))
        
        # Removed columns
        removed = baseline_cols_set - current_cols
        for col in removed:
            signals.append(self._create_signal(
                DriftType.SCHEMA, DriftSeverity.HIGH, ds_name, col,
                f"Column removed: {col}",
                "schema_change", "removed", "existed",
                {"action": "removed"}
            ))
        
        # Type changes
        for col in current_cols & baseline_cols_set:
            curr_col = current.columns[col]
            base_col = baseline_cols[col]
            
            if str(curr_col.data_type) != str(base_col.get("data_type", "")):
                signals.append(self._create_signal(
                    DriftType.SCHEMA, DriftSeverity.MEDIUM, ds_name, col,
                    f"Data type changed: {base_col.get('data_type')} -> {curr_col.data_type}",
                    "data_type", str(curr_col.data_type), str(base_col.get("data_type", "")),
                    {"old_type": str(base_col.get("data_type")), "new_type": str(curr_col.data_type)}
                ))
            
            # Nullability change
            curr_nullable = curr_col.null_percentage > 0
            base_nullable = base_col.get("null_percentage", 0) > 0
            if curr_nullable != base_nullable:
                signals.append(self._create_signal(
                    DriftType.SCHEMA, DriftSeverity.MEDIUM, ds_name, col,
                    f"Nullability changed: was_nullable={base_nullable}, now_nullable={curr_nullable}",
                    "nullability", str(curr_nullable), str(base_nullable),
                    {"action": "nullability_changed"}
                ))
        
        return signals
    
    def _check_volume_drift(self, current: DatasetProfile, baseline: Any, ds_name: str) -> List[Any]:
        """Detect significant volume changes."""
        
        signals = []
        current_rows = current.row_count
        baseline_rows = baseline.get("row_count", 0)
        
        if baseline_rows > 0:
            change_pct = (current_rows - baseline_rows) / baseline_rows
            
            if abs(change_pct) >= self.drift_config.volume_drift_threshold:
                severity = DriftSeverity.HIGH if abs(change_pct) > 1.0 else DriftSeverity.MEDIUM
                
                signals.append(self._create_signal(
                    DriftType.VOLUME, severity, ds_name, "[dataset]",
                    f"Row count changed by {change_pct:.1%}: {baseline_rows:,} -> {current_rows:,}",
                    "row_count", current_rows, baseline_rows,
                    {"change_pct": change_pct}
                ))
        
        return signals
    
    def _check_distribution_drift(
        self, 
        current: ColumnProfile, 
        baseline: Dict[str, Any], 
        ds_name: str, 
        col_name: str
    ) -> List[Any]:
        """Detect distribution drift using PSI, KS test, etc."""
        
        signals = []
        
        # PSI (Population Stability Index) for numeric
        if current.mean is not None and current.std_dev is not None and current.std_dev > 0:
            base_mean = baseline.get("mean")
            base_std = baseline.get("std_dev")
            
            if base_mean is not None and base_std is not None and base_std > 0:
                # Approximate PSI using mean/std shift
                mean_shift = abs(current.mean - base_mean) / base_std
                std_shift = abs(current.std_dev - base_std) / base_std
                
                # Approximate PSI
                psi = (mean_shift + std_shift) / 2
                
                if psi >= self.drift_config.psi_threshold:
                    severity = DriftSeverity.HIGH if psi > 0.5 else DriftSeverity.MEDIUM
                    signals.append(self._create_signal(
                        DriftType.DISTRIBUTION, severity, ds_name, col_name,
                        f"Distribution shift detected (PSI={psi:.3f}): mean shifted by {mean_shift:.2f}σ",
                        "psi", round(psi, 3), 0.0,
                        {"mean_shift": round(mean_shift, 3), "std_shift": round(std_shift, 3)}
                    ))
        
        # Quantile drift
        if current.quantiles and baseline.get("quantiles"):
            for q_name, current_q in current.quantiles.items():
                base_q = baseline["quantiles"].get(q_name)
                if base_q is not None and base_q != 0:
                    q_shift = abs(current_q - base_q) / abs(base_q)
                    if q_shift > 0.2:  # 20% shift in quantile
                        signals.append(self._create_signal(
                            DriftType.DISTRIBUTION, DriftSeverity.MEDIUM, ds_name, col_name,
                            f"Quantile {q_name} shifted by {q_shift:.1%}: {base_q:.2f} -> {current_q:.2f}",
                            f"quantile_{q_name}", round(current_q, 3), round(base_q, 3),
                            {"shift_pct": round(q_shift, 3)}
                        ))
        
        # Categorical distribution drift (chi-square)
        if current.pattern_distribution and baseline.get("pattern_distribution"):
            # Chi-square test for categorical distribution
            current_patterns = current.pattern_distribution
            base_patterns = baseline["pattern_distribution"]
            
            all_patterns = set(current_patterns.keys()) | set(base_patterns.keys())
            if all_patterns:
                chi2_stat = 0
                total_current = sum(current_patterns.values())
                total_base = sum(base_patterns.values())
                
                for pattern in all_patterns:
                    obs = current_patterns.get(pattern, 0)
                    exp = base_patterns.get(pattern, 0) * (total_current / total_base) if total_base > 0 else 0
                    if exp > 0:
                        chi2_stat += (obs - exp) ** 2 / exp
                
                if chi2_stat > 10:  # Significant
                    signals.append(self._create_signal(
                        DriftType.DISTRIBUTION, DriftSeverity.MEDIUM, ds_name, col_name,
                        f"Categorical distribution shift (χ²={chi2_stat:.1f})",
                        "chi2", round(chi2_stat, 1), 0.0,
                        {"chi2_statistic": round(chi2_stat, 1)}
                    ))
        
        return signals
    
    def _check_quality_drift(
        self, 
        current: ColumnProfile, 
        baseline: Dict[str, Any], 
        ds_name: str, 
        col_name: str
    ) -> List[Any]:
        """Detect quality metric drift."""
        
        signals = []
        
        # Null percentage drift
        base_null = baseline.get("null_percentage", 0)
        current_null = current.null_percentage
        
        if base_null > 0:
            null_change = (current_null - base_null) / base_null
        else:
            null_change = current_null / 100 if current_null > 0 else 0
        
        if abs(null_change) >= self.drift_config.quality_drift_threshold:
            severity = DriftSeverity.HIGH if abs(null_change) > 0.5 else DriftSeverity.MEDIUM
            signals.append(self._create_signal(
                DriftType.QUALITY, severity, ds_name, col_name,
                f"Null percentage changed by {null_change:.1%}: {base_null:.1f}% -> {current_null:.1f}%",
                "null_percentage", round(current_null, 1), round(base_null, 1),
                {"change_pct": round(null_change, 3)}
            ))
        
        # Distinct count drift
        base_distinct = baseline.get("distinct_count", 0)
        current_distinct = current.distinct_count
        
        if base_distinct > 0:
            distinct_change = (current_distinct - base_distinct) / base_distinct
            if abs(distinct_change) >= self.drift_config.quality_drift_threshold:
                signals.append(self._create_signal(
                    DriftType.QUALITY, DriftSeverity.MEDIUM, ds_name, col_name,
                    f"Distinct count changed by {distinct_change:.1%}: {base_distinct} -> {current_distinct}",
                    "distinct_count", current_distinct, base_distinct,
                    {"change_pct": round(distinct_change, 3)}
                ))
        
        return signals
    
    def _check_semantic_drift(
        self, 
        current: ColumnProfile, 
        baseline: Dict[str, Any], 
        ds_name: str, 
        col_name: str
    ) -> List[Any]:
        """Detect semantic type changes."""
        
        signals = []
        
        base_semantic = baseline.get("semantic_type", "unknown")
        current_semantic = current.semantic_type
        
        if current_semantic != base_semantic:
            # Check if it's a significant change
            if base_semantic != "unknown" and current_semantic != "unknown":
                signals.append(self._create_signal(
                    DriftType.SEMANTIC, DriftSeverity.MEDIUM, ds_name, col_name,
                    f"Semantic type changed: {base_semantic} -> {current_semantic}",
                    "semantic_type", current_semantic, base_semantic,
                    {"confidence": current.semantic_confidence}
                ))
        
        # Confidence drop
        base_conf = baseline.get("semantic_confidence", 1.0)
        current_conf = current.semantic_confidence
        
        if current_conf < base_conf - 0.3:
            signals.append(self._create_signal(
                DriftType.SEMANTIC, DriftSeverity.LOW, ds_name, col_name,
                f"Semantic confidence dropped: {base_conf:.2f} -> {current_conf:.2f}",
                "semantic_confidence", round(current_conf, 2), round(base_conf, 2),
                {}
            ))
        
        return signals
    
    async def _check_cross_dataset_drift(
        self, 
        current_profiles: Dict[str, DatasetProfile], 
        baselines: Dict[str, Any]
    ) -> List[Any]:
        """Check drift in cross-dataset relationships."""
        
        signals = []
        
        # Check if relationship patterns changed
        # This would compare current relationships with baseline
        # For now, placeholder
        
        return signals
    
    def _create_signal(
        self, 
        drift_type: DriftType, 
        severity: DriftSeverity, 
        dataset: str, 
        column: Optional[str],
        message: str,
        metric_name: str,
        current_value: Any,
        baseline_value: Any,
        details: Dict[str, Any]
    ) -> Any:
        """Create a drift signal."""
        
        # Calculate drift score based on severity
        severity_scores = {
            DriftSeverity.INFO: 0.1,
            DriftSeverity.LOW: 0.25,
            DriftSeverity.MEDIUM: 0.5,
            DriftSeverity.HIGH: 0.75,
            DriftSeverity.CRITICAL: 1.0,
        }
        
        return DriftSignal(
            drift_type=drift_type,
            severity=severity,
            dataset=dataset,
            column=column,
            message=message,
            metric_name=metric_name,
            current_value=current_value,
            baseline_value=baseline_value,
            drift_score=severity_scores[severity],
            threshold=self._get_threshold(drift_type),
            details=details,
        )
    
    def _get_threshold(self, drift_type: DriftType) -> float:
        """Get threshold for drift type."""
        thresholds = {
            DriftType.SCHEMA: 0.0,
            DriftType.DISTRIBUTION: self.drift_config.psi_threshold,
            DriftType.QUALITY: self.drift_config.quality_drift_threshold,
            DriftType.SEMANTIC: self.drift_config.semantic_drift_threshold,
            DriftType.VOLUME: self.drift_config.volume_drift_threshold,
            DriftType.RELATIONSHIP: 0.0,
        }
        return thresholds.get(drift_type, 0.0)
    
    def _build_report(
        self, 
        run_id: str, 
        signals: List[Any], 
        datasets_checked: List[str],
        baselines: Dict[str, Any]
    ) -> Any:
        """Build final drift report."""
        
        # Calculate overall drift score
        if signals:
            overall_score = sum(s.drift_score for s in signals) / len(signals)
        else:
            overall_score = 0.0
        
        # Summary by type
        by_type = defaultdict(int)
        by_severity = defaultdict(int)
        for s in signals:
            by_type[s.drift_type.value] += 1
            by_severity[s.severity.value] += 1
        
        # Generate recommendation
        if overall_score > 0.7:
            recommendation = "Critical drift detected. Review schema changes and data pipeline immediately."
        elif overall_score > 0.4:
            recommendation = "Significant drift detected. Schedule data quality review."
        elif overall_score > 0.1:
            recommendation = "Moderate drift detected. Monitor trends."
        else:
            recommendation = "No significant drift detected."
        
        return DriftReport(
            run_id=run_id,
            baseline_run_id=None,  # Would be set from baselines
            detected_at=datetime.utcnow(),
            signals=signals,
            summary={
                "total_signals": len(signals),
                "by_type": dict(by_type),
                "by_severity": dict(by_severity),
            },
            datasets_checked=datasets_checked,
            overall_drift_score=round(overall_score, 3),
            recommendation=recommendation,
        )
    
    def _empty_report(self, run_id: str, message: str = "No drift detection performed") -> Any:
        """Return empty drift report."""
        return DriftReport(
            run_id=run_id,
            baseline_run_id=None,
            detected_at=datetime.utcnow(),
            signals=[],
            summary={"message": message},
            datasets_checked=[],
            overall_drift_score=0.0,
            recommendation=message,
        )
    
    async def _store_baseline(self, profiles: Dict[str, DatasetProfile], run_id: str) -> None:
        """Store current profiles as new baseline."""
        
        if not self.memory:
            return
        
        try:
            for ds_name, profile in profiles.items():
                baseline_data = {
                    "run_id": run_id,
                    "dataset_name": ds_name,
                    "timestamp": datetime.utcnow().isoformat(),
                    "row_count": profile.row_count,
                    "column_count": profile.column_count,
                    "columns": {
                        name: {
                            "data_type": str(col.data_type),
                            "null_percentage": col.null_percentage,
                            "distinct_count": col.distinct_count,
                            "distinct_percentage": col.distinct_percentage,
                            "mean": col.mean,
                            "std_dev": col.std_dev,
                            "quantiles": col.quantiles,
                            "pattern_distribution": col.pattern_distribution,
                            "semantic_type": col.semantic_type,
                            "semantic_confidence": col.semantic_confidence,
                            "min_value": str(col.min_value) if col.min_value else None,
                            "max_value": str(col.max_value) if col.max_value else None,
                        }
                        for name, col in profile.columns.items()
                    }
                }
                
                # Store in memory
                key = f"baseline:{ds_name}:{run_id}"
                await self.memory.remember(
                    key=key,
                    value=baseline_data,
                    type=MemoryType.DATA_PROFILE,
                    tier=MemoryTier.WARM,
                )
        except Exception as e:
            logger.warning(f"Failed to store baseline: {e}")