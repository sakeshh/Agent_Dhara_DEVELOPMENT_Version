from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional
from agent.compiler.execution_validation.models import CostEstimate, ExecutionValidationResult
from agent.ir.models import IrPlan, RepairRecord
from agent.learning.models import LearningSummary, PatternExecutionRecord
from agent.rag.models import RagEntityType, RagItem
from agent.rag.store import RagStore, get_rag_store

logger = logging.getLogger("agent.learning.engine")


class PatternLearningEngine:
    """
    Phase 8 Pattern Learning Engine for Agent Dhara.
    Closes the enterprise feedback loop by:
    1. Persisting canonical IR plans, validation, repair history, and cost estimates (Zero generated code stored).
    2. Ingesting runtime execution outcomes (success rate, latency, failures).
    3. Ingesting human user feedback (ratings, reviews, approval actions).
    4. Dynamically re-weighting pattern selection scores over time.
    5. Persisting patterns to disk for durability across restarts.
    """

    def __init__(
        self,
        rag_store: Optional[RagStore] = None,
        persistence_path: Optional[str] = None,
    ) -> None:
        self.rag_store = rag_store or get_rag_store()
        self.persistence_path = persistence_path
        self._records: Dict[str, PatternExecutionRecord] = {}
        self._session_to_pattern: Dict[str, str] = {}
        self._lock = threading.RLock()

        if self.persistence_path and os.path.exists(self.persistence_path):
            self.load_patterns(self.persistence_path)

    def learn_from_compilation(
        self,
        session_id: str,
        user_prompt: str,
        ir_plan: IrPlan,
        validation_result: Optional[ExecutionValidationResult] = None,
        cost_estimate: Optional[CostEstimate] = None,
        repair_history: Optional[List[RepairRecord]] = None,
        target_engine: str = "sql",
        pattern_id: Optional[str] = None,
    ) -> PatternExecutionRecord:
        with self._lock:
            pid = pattern_id or f"pat_{int(time.time() * 1000)}"
            val_dict = validation_result.model_dump() if validation_result else None
            cost = cost_estimate or (validation_result.cost_estimate if validation_result else CostEstimate())
            repairs = repair_history or (ir_plan.metadata.repair_history if ir_plan.metadata else [])

            record = PatternExecutionRecord(
                pattern_id=pid,
                session_id=session_id,
                prompt=user_prompt,
                ir_plan=ir_plan,
                repair_history=repairs,
                cost_estimate=cost,
                validation_result=val_dict,
                target_engine=target_engine,
                compilation_success=validation_result.valid if validation_result else True,
            )

            self._records[pid] = record
            self._session_to_pattern[session_id] = pid

            # Mirror into hybrid RagStore with composite ranking metadata
            self.rag_store.add_ir_pattern(
                pattern_id=pid,
                user_prompt=user_prompt,
                ir_plan=ir_plan,
                target_engine=target_engine,
                success_score=1.0 if record.compilation_success else 0.5,
                usage_count=1,
                execution_success=1.0,
                user_rating=5.0,
            )

            logger.info(f"[PatternLearningEngine] Learned new IR pattern '{pid}' for prompt: '{user_prompt[:60]}'")
            self._auto_persist()
            return record

    def record_execution_outcome(
        self,
        pattern_id_or_session_id: str,
        execution_success: bool,
        runtime_ms: float = 0.0,
        error_message: Optional[str] = None,
    ) -> Optional[PatternExecutionRecord]:
        with self._lock:
            pid = self._resolve_pattern_id(pattern_id_or_session_id)
            if not pid or pid not in self._records:
                logger.warning(f"[PatternLearningEngine] Pattern not found for ID: {pattern_id_or_session_id}")
                return None

            record = self._records[pid]
            record.total_executions += 1
            if execution_success:
                record.successful_executions += 1
            record.execution_success = execution_success
            record.execution_runtime_ms = runtime_ms
            record.execution_error = error_message
            record.updated_at = time.time()

            exec_rate = record.successful_executions / max(record.total_executions, 1)

            # Re-weight in RagStore
            rag_item = self.rag_store._items.get(pid)
            if rag_item:
                rag_item.metadata["execution_success"] = exec_rate
                rag_item.metadata["usage_count"] = record.total_executions
                rag_item.metadata["last_runtime_ms"] = runtime_ms
                if not execution_success:
                    rag_item.metadata["last_error"] = error_message

            self._update_rank_score(record)
            logger.info(
                f"[PatternLearningEngine] Recorded execution for '{pid}': success={execution_success} (Rate: {exec_rate:.2f})"
            )
            self._auto_persist()
            return record

    def record_user_feedback(
        self,
        pattern_id_or_session_id: str,
        user_rating: float,
        feedback_comment: Optional[str] = None,
        user_action: Optional[str] = None,
    ) -> Optional[PatternExecutionRecord]:
        with self._lock:
            pid = self._resolve_pattern_id(pattern_id_or_session_id)
            if not pid or pid not in self._records:
                return None

            record = self._records[pid]
            bounded_rating = max(1.0, min(5.0, user_rating))
            total = record.total_ratings + 1
            new_avg = ((record.average_rating * record.total_ratings) + bounded_rating) / total

            record.user_rating = bounded_rating
            record.total_ratings = total
            record.average_rating = round(new_avg, 2)
            if feedback_comment:
                record.feedback_comments.append(feedback_comment)
            if user_action:
                record.user_action = user_action
            record.updated_at = time.time()

            # Re-weight in RagStore
            rag_item = self.rag_store._items.get(pid)
            if rag_item:
                rag_item.metadata["user_rating"] = record.average_rating

            self._update_rank_score(record)
            logger.info(
                f"[PatternLearningEngine] Recorded user rating {bounded_rating} for '{pid}' (New Avg: {new_avg:.2f})"
            )
            self._auto_persist()
            return record

    def get_best_pattern(self, user_prompt: str, min_score: float = 0.65) -> Optional[IrPlan]:
        """Retrieves best matching learned IrPlan ranked by similarity + historical execution + ratings + recency."""
        return self.rag_store.get_top_ir_pattern(user_prompt, min_score=min_score)

    def get_pattern(self, pattern_id_or_session_id: str) -> Optional[PatternExecutionRecord]:
        with self._lock:
            pid = self._resolve_pattern_id(pattern_id_or_session_id)
            return self._records.get(pid) if pid else None

    def get_learning_summary(self) -> LearningSummary:
        with self._lock:
            total_patterns = len(self._records)
            total_execs = sum(r.total_executions for r in self._records.values())
            total_successes = sum(r.successful_executions for r in self._records.values())
            overall_exec_rate = (total_successes / total_execs) if total_execs > 0 else 1.0

            rated_records = [r for r in self._records.values() if r.total_ratings > 0]
            avg_rating = (
                sum(r.average_rating for r in rated_records) / len(rated_records) if rated_records else 5.0
            )

            domains = set()
            for r in self._records.values():
                domains.update(r.ir_plan.get_source_tables())

            high_conf = sum(1 for r in self._records.values() if r.rank_score >= 0.85)

            return LearningSummary(
                total_patterns_learned=total_patterns,
                total_executions_recorded=total_execs,
                overall_execution_success_rate=round(overall_exec_rate, 2),
                average_user_rating=round(avg_rating, 2),
                top_domains=list(domains)[:10],
                high_confidence_patterns=high_conf,
            )

    def export_patterns(self, file_path: Optional[str] = None) -> str:
        target = file_path or self.persistence_path or "data/learned_ir_patterns.json"
        with self._lock:
            os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
            payload = {
                pid: rec.model_dump()
                for pid, rec in self._records.items()
            }
            with open(target, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
            logger.info(f"[PatternLearningEngine] Exported {len(payload)} patterns to {target}")
            return target

    def load_patterns(self, file_path: str) -> int:
        if not os.path.exists(file_path):
            return 0
        with self._lock:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                loaded = 0
                for pid, data in payload.items():
                    record = PatternExecutionRecord.model_validate(data)
                    self._records[pid] = record
                    self._session_to_pattern[record.session_id] = pid
                    # Mirror into RAG store
                    self.rag_store.add_ir_pattern(
                        pattern_id=pid,
                        user_prompt=record.prompt,
                        ir_plan=record.ir_plan,
                        target_engine=record.target_engine,
                        success_score=1.0 if record.compilation_success else 0.5,
                        usage_count=max(record.total_executions, 1),
                        execution_success=(record.successful_executions / record.total_executions) if record.total_executions > 0 else 1.0,
                        user_rating=record.average_rating,
                    )
                    loaded += 1
                logger.info(f"[PatternLearningEngine] Loaded {loaded} patterns from {file_path}")
                return loaded
            except Exception as e:
                logger.warning(f"[PatternLearningEngine] Failed to load patterns from {file_path}: {e}")
                return 0

    def _resolve_pattern_id(self, id_or_session: str) -> Optional[str]:
        if id_or_session in self._records:
            return id_or_session
        return self._session_to_pattern.get(id_or_session)

    def _update_rank_score(self, record: PatternExecutionRecord) -> None:
        comp_score = 1.0 if record.compilation_success else 0.4
        exec_score = (record.successful_executions / record.total_executions) if record.total_executions > 0 else 1.0
        rating_score = record.average_rating / 5.0
        age_days = (time.time() - record.created_at) / 86400.0
        recency = max(0.0, 1.0 - (age_days / 365.0))

        # 40% Similarity (computed at query time) + 25% Comp + 20% Exec + 10% User + 5% Recency
        record.rank_score = round(
            (comp_score * 0.45) + (exec_score * 0.35) + (rating_score * 0.15) + (recency * 0.05),
            3,
        )

        # Pattern Stability Score & Tier:
        # Measures operational dependability over time.
        # High volume of runs with consistent 100% success and minimal repairs yields ~1.0
        from agent.learning.models import StabilityTier
        if record.total_executions > 0:
            repair_penalty = min(0.15, 0.03 * len(record.repair_history))
            error_penalty = 0.1 if record.execution_error else 0.0
            raw_stability = (exec_score * (1.0 - repair_penalty)) - error_penalty
            record.stability_score = round(max(0.0, min(1.0, raw_stability)), 2)

            if record.stability_score >= 0.95 and record.total_executions >= 3:
                record.stability_tier = StabilityTier.GOLD
            elif record.stability_score >= 0.90:
                record.stability_tier = StabilityTier.SILVER
            elif record.stability_score >= 0.80:
                record.stability_tier = StabilityTier.BRONZE
            else:
                record.stability_tier = StabilityTier.PROVISIONAL
        else:
            record.stability_score = 1.0
            record.stability_tier = StabilityTier.PROVISIONAL

    def _auto_persist(self) -> None:
        if self.persistence_path:
            try:
                self.export_patterns(self.persistence_path)
            except Exception as e:
                logger.warning(f"[PatternLearningEngine] Auto-persist error: {e}")


# Global Singleton
_GLOBAL_LEARNING_ENGINE = PatternLearningEngine()


def get_pattern_learning_engine() -> PatternLearningEngine:
    return _GLOBAL_LEARNING_ENGINE
