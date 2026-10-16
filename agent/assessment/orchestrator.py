"""Assessment Orchestrator - Main entry point for the assessment engine."""
from __future__ import annotations
import asyncio
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Callable, Awaitable
from concurrent.futures import ThreadPoolExecutor

from agent.core.interfaces import (
    BaseAgent, AgentInput, AgentResult, IncrementalConfig, 
    IncrementalState, IncrementalStrategy, MemoryStore
)
from agent.assessment.models import (
    AssessmentResult, DatasetProfile, ColumnProfile, QualityIssue,
    Relationship, SemanticProfile, IssueSeverity, IssueCategory,
    FixabilityTier, SemanticType
)
from agent.feature_flags import get_flags
from agent.logging_setup import get_logger, get_performance_logger, set_context


logger = get_logger("assessment.orchestrator")


@dataclass
class AssessmentConfig:
    """Configuration for assessment run."""
    datasets: Dict[str, Any]  # dataset_name -> data source config
    max_rows_per_dataset: int = 100000
    sample_size: int = 10000
    parallelism: int = 4
    enable_semantic: bool = True
    enable_business_logic: bool = True
    enable_drift: bool = True
    enable_relationships: bool = True
    incremental_configs: Dict[str, IncrementalConfig] = field(default_factory=dict)
    business_rules: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 600
    session_id: Optional[str] = None


@dataclass
class AssessmentContext:
    """Context passed through assessment pipeline."""
    config: AssessmentConfig
    session_id: str
    request_id: str
    started_at: datetime
    datasets: Dict[str, Any] = field(default_factory=dict)
    profiles: Dict[str, Any] = field(default_factory=dict)
    semantic_profiles: Dict[str, Any] = field(default_factory=dict)
    issues: Dict[str, List[Any]] = field(default_factory=dict)
    relationships: List[Any] = field(default_factory=list)
    drift_report: Optional[Any] = None
    business_rules: List[Any] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class AssessmentOrchestrator:
    """
    Main orchestrator for the assessment engine.
    
    Coordinates the three-pass assessment pipeline:
    1. Pass 1: Fast profiling + rough semantics (parallel)
    2. Pass 2: Deep quality checks + relationship discovery
    3. Pass 3: Semantic refinement + business logic discovery + drift detection
    """
    
    def __init__(
        self,
        memory_store: Optional[Any] = None,
        config: Optional[AssessmentConfig] = None,
    ):
        self.config = config or AssessmentConfig(datasets={})
        self.memory = memory_store
        self.flags = get_flags()
        self.executor = ThreadPoolExecutor(max_workers=self.config.parallelism)
        
        # Initialize sub-agents (lazy loaded)
        self._profiler = None
        self._semantic_analyzer = None
        self._quality_checker = None
        self._relationship_finder = None
        self._business_logic_engine = None
        self._drift_detector = None
        self._semantic_refiner = None
    
    @property
    def profiler(self):
        if self._profiler is None:
            from agent.assessment.profiler import DataProfiler
            self._profiler = DataProfiler(self.config, self.memory)
        return self._profiler
    
    @property
    def semantic_analyzer(self):
        if self._semantic_analyzer is None:
            from agent.assessment.semantic import SemanticAnalyzer
            self._semantic_analyzer = SemanticAnalyzer(self.config, self.memory)
        return self._semantic_analyzer
    
    @property
    def quality_checker(self):
        if self._quality_checker is None:
            from agent.assessment.quality import QualityChecker
            self._quality_checker = QualityChecker(self.config, self.memory)
        return self._quality_checker
    
    @property
    def relationship_finder(self):
        if self._relationship_finder is None:
            from agent.assessment.relationships import RelationshipFinder
            self._relationship_finder = RelationshipFinder(self.config, self.memory)
        return self._relationship_finder
    
    @property
    def business_logic_engine(self):
        if self._business_logic_engine is None:
            from agent.assessment.business_logic import BusinessLogicEngine
            self._business_logic_engine = BusinessLogicEngine(self.config, self.memory)
        return self._business_logic_engine
    
    @property
    def drift_detector(self):
        if self._drift_detector is None:
            from agent.assessment.drift import DriftDetector
            self._drift_detector = DriftDetector(self.config, self.memory)
        return self._drift_detector
    
    @property
    def semantic_refiner(self):
        if self._semantic_refiner is None:
            from agent.assessment.semantic import SemanticRefiner
            self._semantic_refiner = SemanticRefiner(self.config, self.memory)
        return self._semantic_refiner
    
    async def run_assessment(self, config: Optional[AssessmentConfig] = None) -> AssessmentResult:
        """
        Run complete assessment pipeline.
        
        This is the main entry point for running assessments.
        """
        cfg = config or self.config
        session_id = cfg.session_id or str(uuid.uuid4())
        request_id = str(uuid.uuid4())[:8]
        
        # Set up logging context
        prev_context = set_context(
            request_id=request_id,
            session_id=session_id,
        )
        
        logger.info(f"Starting assessment session {session_id} with {len(cfg.datasets)} datasets")
        
        perf = get_performance_logger("full_assessment", session_id=session_id)
        
        try:
            # Create context
            ctx = AssessmentContext(
                config=cfg,
                session_id=session_id,
                request_id=request_id,
                started_at=datetime.utcnow(),
            )
            
            # PASS 1: Fast Profiling + Rough Semantics (Parallel)
            perf.log_metric("pass", 1)
            await self._pass1_fast_profiling(ctx)
            perf.log_metric("pass1_duration_ms", (datetime.utcnow() - ctx.started_at).total_seconds() * 1000)
            
            # PASS 2: Deep Quality + Relationships (Parallel per dataset)
            perf.log_metric("pass", 2)
            await self._pass2_deep_analysis(ctx)
            perf.log_metric("pass2_duration_ms", (datetime.utcnow() - ctx.started_at).total_seconds() * 1000)
            
            # PASS 3: Semantic Refinement + Business Logic + Drift
            perf.log_metric("pass", 3)
            await self._pass3_refinement_and_synthesis(ctx)
            perf.log_metric("pass3_duration_ms", (datetime.utcnow() - ctx.started_at).total_seconds() * 1000)
            
            # Build final result
            result = self._build_result(ctx)
            
            # Store learnings
            await self._store_learnings(ctx)
            
            logger.info(
                f"Assessment complete: {result.total_issues} issues found, "
                f"{len(result.datasets)} datasets profiled in {result.duration_ms}ms"
            )
            
            return result
            
        except Exception as e:
            logger.exception(f"Assessment failed: {e}")
            raise
        finally:
            # Restore logging context
            set_context(**prev_context)
    
    # ============================================================
    # PASS 1: Fast Profiling + Rough Semantics
    # ============================================================
    
    async def _pass1_fast_profiling(self, ctx: AssessmentContext) -> None:
        """Pass 1: Fast profiling with rough semantic tags."""
        
        with get_performance_logger("pass1_profiling", session_id=ctx.session_id) as perf:
            
            # Load data for all datasets (parallel)
            load_tasks = []
            for ds_name, ds_config in ctx.config.datasets.items():
                task = self._load_dataset(ds_name, ds_config)
                load_tasks.append((ds_name, task))
            
            # Wait for all loads
            for ds_name, task in load_tasks:
                try:
                    data = await task
                    ctx.datasets[ds_name] = data
                    perf.log_metric(f"loaded_{ds_name}_rows", len(data) if hasattr(data, '__len__') else 0)
                except Exception as e:
                    logger.error(f"Failed to load dataset {ds_name}: {e}")
                    ctx.errors.append(f"Load failed for {ds_name}: {e}")
            
            # Fast profiling + rough semantics in parallel
            profile_tasks = []
            for ds_name, data in ctx.datasets.items():
                task = self._profile_dataset_pass1(ds_name, data)
                profile_tasks.append((ds_name, task))
            
            for ds_name, task in profile_tasks:
                try:
                    profile, rough_semantics = await task
                    ctx.profiles[ds_name] = profile
                    # Store rough semantics for pass 2
                    ctx.profiles[ds_name].rough_semantics = rough_semantics
                    perf.log_metric(f"profiled_{ds_name}_cols", len(profile.columns))
                except Exception as e:
                    logger.error(f"Profiling failed for {ds_name}: {e}")
                    ctx.errors.append(f"Profiling failed for {ds_name}: {e}")
            
            perf.log_metric("datasets_profiled", len(ctx.profiles))
    
    async def _load_dataset(self, name: str, config: Dict[str, Any]) -> Any:
        """Load dataset from source."""
        # This would use the connector abstraction
        # For now, return mock data structure
        logger.debug(f"Loading dataset {name} from {config.get('source_type', 'unknown')}")
        # Real implementation would use connector factory
        return None
    
    async def _profile_dataset_pass1(self, name: str, data: Any) -> tuple:
        """Fast profiling + rough semantic tags."""
        with get_performance_logger(f"profile_pass1_{name}") as perf:
            # Fast statistical profiling
            profile = await self.profiler.profile(data, name)
            
            # Rough semantic inference (pattern-based, no LLM)
            rough_semantics = await self.semantic_analyzer.rough_inference(profile)
            
            perf.log_metric("columns", len(profile.columns))
            perf.log_metric("semantic_tags", len([c for c in profile.columns.values() if c.semantic_type != 'unknown']))
            
            return profile, rough_semantics
    
    # ============================================================
    # PASS 2: Deep Quality + Relationships
    # ============================================================
    
    async def _pass2_deep_analysis(self, ctx: AssessmentContext) -> None:
        """Pass 2: Deep quality checks + relationship discovery."""
        
        with get_performance_logger("pass2_deep", session_id=ctx.session_id) as perf:
            
            # Deep quality checks per dataset (parallel)
            quality_tasks = []
            for ds_name, profile in ctx.profiles.items():
                data = ctx.datasets.get(ds_name)
                if data is not None:
                    task = self.quality_checker.check(ds_name, data, profile)
                    quality_tasks.append((ds_name, task))
            
            for ds_name, task in quality_tasks:
                try:
                    issues = await task
                    ctx.issues[ds_name] = issues
                    perf.log_metric(f"issues_{ds_name}", len(issues))
                except Exception as e:
                    logger.error(f"Quality check failed for {ds_name}: {e}")
                    ctx.errors.append(f"Quality check failed for {ds_name}: {e}")
            
            # Relationship discovery (cross-dataset)
            if ctx.config.enable_relationships and len(ctx.profiles) > 1:
                try:
                    relationships = await self.relationship_finder.find(ctx.profiles, ctx.datasets)
                    ctx.relationships = relationships
                    perf.log_metric("relationships_found", len(relationships))
                except Exception as e:
                    logger.error(f"Relationship discovery failed: {e}")
                    ctx.warnings.append(f"Relationship discovery failed: {e}")
            
            perf.log_metric("total_issues", sum(len(v) for v in ctx.issues.values()))
    
    # ============================================================
    # PASS 3: Semantic Refinement + Business Logic + Drift
    # ============================================================
    
    async def _pass3_refinement_and_synthesis(self, ctx: AssessmentContext) -> None:
        """Pass 3: LLM semantic refinement + business logic discovery + drift."""
        
        with get_performance_logger("pass3_refinement", session_id=ctx.session_id) as perf:
            
            # Semantic refinement (LLM for ambiguous columns)
            if ctx.config.enable_semantic:
                refine_tasks = []
                for ds_name, profile in ctx.profiles.items():
                    # Only refine ambiguous columns
                    ambiguous = [c for c in profile.columns.values() 
                               if c.semantic_confidence < 0.7 or c.semantic_type == 'unknown']
                    if ambiguous:
                        task = self.semantic_refiner.refine(ds_name, profile, ambiguous)
                        refine_tasks.append((ds_name, task))
                
                for ds_name, task in refine_tasks:
                    try:
                        refined = await task
                        ctx.semantic_profiles[ds_name] = refined
                        perf.log_metric(f"refined_{ds_name}", len(refined.column_roles))
                    except Exception as e:
                        logger.error(f"Semantic refinement failed for {ds_name}: {e}")
                        ctx.warnings.append(f"Semantic refinement failed for {ds_name}: {e}")
            
            # Business logic discovery
            if ctx.config.enable_business_logic:
                try:
                    rules = await self.business_logic_engine.discover(
                        ctx.profiles, ctx.issues, ctx.relationships, ctx.config.business_rules
                    )
                    ctx.business_rules = rules
                    perf.log_metric("business_rules_discovered", len(rules))
                except Exception as e:
                    logger.error(f"Business logic discovery failed: {e}")
                    ctx.warnings.append(f"Business logic discovery failed: {e}")
            
            # Drift detection
            if ctx.config.enable_drift and self.memory:
                try:
                    drift = await self.drift_detector.detect(ctx.profiles, ctx.session_id)
                    ctx.drift_report = drift
                    perf.log_metric("drift_signals", len(drift.distribution_shifts) if drift else 0)
                except Exception as e:
                    logger.error(f"Drift detection failed: {e}")
                    ctx.warnings.append(f"Drift detection failed: {e}")
            
            # Cross-dataset consistency checks
            if len(ctx.profiles) > 1:
                try:
                    cross_issues = await self._check_cross_dataset_consistency(ctx)
                    ctx.cross_issues = cross_issues
                except Exception as e:
                    logger.error(f"Cross-dataset consistency check failed: {e}")
    
    async def _check_cross_dataset_consistency(self, ctx: AssessmentContext) -> List[Any]:
        """Check for cross-dataset consistency issues."""
        issues = []
        
        # ID type drift
        id_columns = {}
        for ds_name, profile in ctx.profiles.items():
            for col_name, col_profile in profile.columns.items():
                if col_profile.semantic_type in ['unique_id', 'foreign_key', 'primary_key']:
                    key = f"{ds_name}.{col_name}"
                    id_columns[key] = col_profile
        
        # Compare ID columns across datasets
        # This is a simplified check - real implementation would be more sophisticated
        return issues
    
    # ============================================================
    # Result Building
    # ============================================================
    
    def _build_result(self, ctx: AssessmentContext) -> AssessmentResult:
        """Build final assessment result from context."""
        
        # Aggregate issues
        all_issues = []
        for ds_issues in ctx.issues.values():
            all_issues.extend(ds_issues)
        
        total_issues = len(all_issues)
        issues_by_severity = {}
        issues_by_category = {}
        
        for issue in all_issues:
            issues_by_severity[issue.severity.value] = issues_by_severity.get(issue.severity.value, 0) + 1
            issues_by_category[issue.category.value] = issues_by_category.get(issue.category.value, 0) + 1
        
        # Build dataset profiles for result
        result_datasets = {}
        for ds_name, profile in ctx.profiles.items():
            # Add semantic info
            if ds_name in ctx.semantic_profiles:
                profile.semantic_profile = ctx.semantic_profiles[ds_name]
            result_datasets[ds_name] = profile
        
        # Build relationships
        relationships = []
        for rel in ctx.relationships:
            relationships.append(rel)
        
        # Calculate totals
        total_rows = sum(p.row_count for p in ctx.profiles.values())
        total_columns = sum(p.column_count for p in ctx.profiles.values())
        
        duration_ms = int((datetime.utcnow() - ctx.started_at).total_seconds() * 1000)
        
        result = AssessmentResult(
            assessment_id=ctx.session_id,
            datasets=result_datasets,
            relationships=relationships,
            total_rows=total_rows,
            total_columns=total_columns,
            total_issues=total_issues,
            issues_by_severity=issues_by_severity,
            issues_by_category=issues_by_category,
            discovered_rules=ctx.business_rules,
            business_logic_summary=self._summarize_business_logic(ctx.business_rules),
            drift_report=ctx.drift_report,
            duration_ms=duration_ms,
            completed_at=datetime.utcnow(),
        )
        
        return result
    
    def _summarize_business_logic(self, rules: List[Any]) -> Dict[str, Any]:
        """Summarize discovered business rules."""
        return {
            "total_rules": len(rules),
            "by_type": {},
            "high_confidence": len([r for r in rules if getattr(r, 'confidence', 0) > 0.8]),
        }
    
    async def _store_learnings(self, ctx: AssessmentContext) -> None:
        """Store learnings in memory for future runs."""
        if not self.memory:
            return
        
        try:
            # Store fix patterns from issues
            for ds_name, issues in ctx.issues.items():
                for issue in issues:
                    if issue.suggested_action:
                        await self.memory.learn_from_execution(
                            dataset_type=ctx.profiles[ds_name].source_type,
                            issue_type=issue.category.value,
                            action=issue.suggested_action,
                            params=issue.suggested_parameters or {},
                            success=True,  # We don't know yet, but pattern is recorded
                            context={"dataset": ds_name, "column": issue.column}
                        )
            
            # Store schema patterns
            for ds_name, profile in ctx.profiles.items():
                pattern_key = f"schema:{profile.source_type}"
                await self.memory.remember(
                    key=pattern_key,
                    value={
                        "columns": {name: c.to_dict() for name, c in profile.columns.items()},
                        "row_count": profile.row_count,
                    },
                    type=MemoryType.SCHEMA_PATTERN,
                    confidence=0.8,
                )
            
        except Exception as e:
            logger.warning(f"Failed to store learnings: {e}")
    
    # ============================================================
    # Public API
    # ============================================================
    
    async def assess_dataset(self, name: str, data: Any, config: Optional[Dict] = None) -> DatasetProfile:
        """Quick assessment of a single dataset."""
        profile = await self.profiler.profile(data, name)
        return profile
    
    async def check_quality(self, name: str, data: Any, profile: DatasetProfile) -> List[QualityIssue]:
        """Run quality checks on a dataset."""
        return await self.quality_checker.check(name, data, profile)
    
    async def infer_semantics(self, profile: DatasetProfile) -> Dict[str, Any]:
        """Infer semantic types for a profile."""
        return await self.semantic_analyzer.infer(profile)
    
    async def discover_relationships(self, profiles: Dict[str, DatasetProfile]) -> List[Relationship]:
        """Discover relationships between datasets."""
        return await self.relationship_finder.find(profiles, {})
    
    async def discover_business_logic(
        self, 
        profiles: Dict[str, DatasetProfile], 
        issues: Dict[str, List[QualityIssue]]
    ) -> List[Any]:
        """Discover business logic rules."""
        return await self.business_logic_engine.discover(profiles, issues, [], {})
    
    async def detect_drift(self, profiles: Dict[str, DatasetProfile]) -> Any:
        """Detect drift from historical baselines."""
        return await self.drift_detector.detect(profiles, str(uuid.uuid4()))