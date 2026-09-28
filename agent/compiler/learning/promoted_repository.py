from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from agent.compiler.learning.promoted_models import (
    ExtensionCandidate,
    PromotedRule,
    PromotionEvidence,
    RuleLifecycleState,
    SchemaSignature,
)
from agent.compiler.learning.repository import get_db_path
from agent.compiler.models.logical import ActionType
from agent.compiler.models.profile import DatasetProfile

logger = logging.getLogger("agent.compiler.learning.promoted_repository")
_REPO_LOCK = threading.Lock()


class PromotedRuleRepository:
    """
    Persistence and retrieval engine for governed promoted rules, promotion evidence,
    and unsupported extension candidates. Backed by SQLite.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS promoted_rules (
                    rule_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    action_type TEXT NOT NULL,
                    parameters_json TEXT NOT NULL,
                    target_columns_json TEXT NOT NULL,
                    dataset_scope TEXT,
                    schema_signature_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    rule_intent_pattern TEXT,
                    created_by TEXT NOT NULL,
                    approved_by TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (rule_id, version)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS promotion_evidence (
                    evidence_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    rule_version INTEGER NOT NULL,
                    run_id TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    schema_signature_json TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    rows_processed INTEGER NOT NULL,
                    rows_failed INTEGER DEFAULT 0,
                    rows_quarantined INTEGER DEFAULT 0,
                    reconciliation_passed INTEGER DEFAULT 1,
                    safety_status TEXT NOT NULL,
                    governance_decision TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS extension_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    original_rule TEXT NOT NULL UNIQUE,
                    dataset_name TEXT NOT NULL,
                    suggested_action_name TEXT,
                    frequency INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'PENDING_DEVELOPER_REVIEW',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.commit()

    def save_candidate(self, rule: PromotedRule) -> PromotedRule:
        """Saves a candidate rule in STAGED state."""
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO promoted_rules (
                    rule_id, version, name, description, action_type, parameters_json,
                    target_columns_json, dataset_scope, schema_signature_json, status,
                    rule_intent_pattern, created_by, approved_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.rule_id,
                rule.version,
                rule.name,
                rule.description,
                rule.action_type.value,
                json.dumps(rule.parameters),
                json.dumps(rule.target_columns),
                rule.dataset_scope,
                json.dumps(rule.schema_signature.model_dump()),
                rule.status.value,
                rule.rule_intent_pattern,
                rule.created_by,
                rule.approved_by,
                rule.created_at,
                rule.updated_at,
            ))
            conn.commit()
        return rule

    def record_evidence(self, evidence: PromotionEvidence) -> PromotionEvidence:
        """Records an execution evidence record linking back to a rule."""
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO promotion_evidence (
                    evidence_id, rule_id, rule_version, run_id, dataset, schema_signature_json,
                    success, rows_processed, rows_failed, rows_quarantined, reconciliation_passed,
                    safety_status, governance_decision, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                evidence.evidence_id,
                evidence.rule_id,
                evidence.rule_version,
                evidence.run_id,
                evidence.dataset,
                json.dumps(evidence.schema_signature),
                1 if evidence.success else 0,
                evidence.rows_processed,
                evidence.rows_failed,
                evidence.rows_quarantined,
                1 if evidence.reconciliation_passed else 0,
                evidence.safety_status,
                evidence.governance_decision,
                evidence.timestamp,
            ))
            conn.commit()
        return evidence

    def find_applicable(
        self,
        dataset_name: str,
        rule_text: Optional[str] = None,
        action_type: Optional[ActionType] = None,
        target_columns: Optional[List[str]] = None,
        schema_profile: Optional[DatasetProfile] = None,
    ) -> Optional[PromotedRule]:
        """
        Finds a reusable promoted rule for the given dataset and schema context.
        Enforces strict lifecycle and schema compatibility:
          - Only DATASET_SCOPED or GLOBAL_PROMOTED rules can be selected.
          - Never returns STAGED, SUSPENDED, or RETIRED rules.
          - DATASET_SCOPED rules must match dataset_name exactly.
          - Target columns must exist and match schema signature.
        """
        norm_rule = rule_text.strip().lower() if rule_text else None

        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            # Order by version DESC to get the latest version
            cur.execute("""
                SELECT * FROM promoted_rules
                WHERE status IN ('dataset_scoped', 'global_promoted')
                ORDER BY version DESC
            """)
            rows = cur.fetchall()

        for row in rows:
            rule_status = RuleLifecycleState(row["status"])
            rule_scope = row["dataset_scope"]
            rule_act = ActionType(row["action_type"])
            rule_cols = json.loads(row["target_columns_json"])
            rule_params = json.loads(row["parameters_json"])
            rule_sig = SchemaSignature.model_validate(json.loads(row["schema_signature_json"]))
            intent_pat = row["rule_intent_pattern"]

            # 1. Dataset Scope Check
            if rule_status == RuleLifecycleState.DATASET_SCOPED:
                if rule_scope != dataset_name:
                    continue  # Does not match unrelated dataset!
            # If GLOBAL_PROMOTED, dataset_scope can be None or any dataset

            # 2. Rule intent or action match
            if norm_rule and intent_pat:
                if norm_rule != intent_pat.strip().lower():
                    continue
            elif action_type and rule_act != action_type:
                continue

            # 3. Target columns check
            if target_columns is not None:
                if set(rule_cols) != set(target_columns):
                    continue

            # 4. Schema Compatibility Guard
            # Checks if target column exists in profile and datatype has not drifted
            if schema_profile:
                is_compat, reason = rule_sig.is_compatible(schema_profile, rule_cols)
                if not is_compat:
                    logger.warning(f"Promoted rule '{row['rule_id']}' rejected for dataset '{dataset_name}': {reason}")
                    continue

            return PromotedRule(
                rule_id=row["rule_id"],
                version=row["version"],
                name=row["name"],
                description=row["description"],
                action_type=rule_act,
                parameters=rule_params,
                target_columns=rule_cols,
                dataset_scope=rule_scope,
                schema_signature=rule_sig,
                status=rule_status,
                rule_intent_pattern=intent_pat,
                created_by=row["created_by"],
                approved_by=row["approved_by"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

        return None

    def promote(
        self,
        rule_id: str,
        new_status: RuleLifecycleState,
        approved_by: Optional[str] = None,
    ) -> PromotedRule:
        """Promotes a rule to DATASET_SCOPED or GLOBAL_PROMOTED."""
        now_ts = datetime.now(timezone.utc).isoformat()
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE promoted_rules
                SET status = ?, approved_by = ?, updated_at = ?
                WHERE rule_id = ? AND version = (
                    SELECT MAX(version) FROM promoted_rules WHERE rule_id = ?
                )
            """, (new_status.value, approved_by, now_ts, rule_id, rule_id))
            conn.commit()

        rule = self.get_rule(rule_id)
        if not rule:
            raise ValueError(f"Rule with id '{rule_id}' not found.")
        return rule

    def demote(
        self,
        rule_id: str,
        new_status: RuleLifecycleState = RuleLifecycleState.SUSPENDED,
        reason: str = "",
    ) -> PromotedRule:
        """Demotes a rule to SUSPENDED or RETIRED upon regression detection."""
        now_ts = datetime.now(timezone.utc).isoformat()
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE promoted_rules
                SET status = ?, description = description || ' [Demoted: ' || ? || ']', updated_at = ?
                WHERE rule_id = ? AND version = (
                    SELECT MAX(version) FROM promoted_rules WHERE rule_id = ?
                )
            """, (new_status.value, reason, now_ts, rule_id, rule_id))
            conn.commit()

        rule = self.get_rule(rule_id)
        if not rule:
            raise ValueError(f"Rule with id '{rule_id}' not found.")
        return rule

    def create_new_version(
        self,
        rule_id: str,
        updated_parameters: Dict[str, Any],
        created_by: str = "compiler_learning",
    ) -> PromotedRule:
        """Creates a new version of an existing rule, deprecating/retiring the previous version."""
        current = self.get_rule(rule_id)
        if not current:
            raise ValueError(f"Rule with id '{rule_id}' not found.")

        # Demote/retire previous version
        now_ts = datetime.now(timezone.utc).isoformat()
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE promoted_rules
                SET status = 'retired', updated_at = ?
                WHERE rule_id = ? AND version = ?
            """, (now_ts, rule_id, current.version))
            conn.commit()

        new_rule = PromotedRule(
            rule_id=rule_id,
            version=current.version + 1,
            name=current.name,
            description=f"Updated version of {rule_id}",
            action_type=current.action_type,
            parameters=updated_parameters,
            target_columns=list(current.target_columns),
            dataset_scope=current.dataset_scope,
            schema_signature=current.schema_signature,
            status=RuleLifecycleState.DATASET_SCOPED,
            rule_intent_pattern=current.rule_intent_pattern,
            created_by=created_by,
        )
        return self.save_candidate(new_rule)

    def record_extension_candidate(
        self,
        original_rule: str,
        dataset_name: str,
        suggested_action_name: Optional[str] = None,
    ) -> ExtensionCandidate:
        """Records or increments frequency of an unsupported business rule."""
        now_ts = datetime.now(timezone.utc).isoformat()
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM extension_candidates WHERE original_rule = ?", (original_rule,))
            row = cur.fetchone()
            if row:
                new_freq = row["frequency"] + 1
                cur.execute("""
                    UPDATE extension_candidates
                    SET frequency = ?, updated_at = ?
                    WHERE original_rule = ?
                """, (new_freq, now_ts, original_rule))
                candidate_id = row["candidate_id"]
                created_at = row["created_at"]
                status = row["status"]
            else:
                candidate_id = f"ext_{abs(hash(original_rule)) % 1000000}"
                new_freq = 1
                created_at = now_ts
                status = "PENDING_DEVELOPER_REVIEW"
                cur.execute("""
                    INSERT INTO extension_candidates (
                        candidate_id, original_rule, dataset_name, suggested_action_name,
                        frequency, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    candidate_id,
                    original_rule,
                    dataset_name,
                    suggested_action_name,
                    new_freq,
                    status,
                    created_at,
                    now_ts,
                ))
            conn.commit()

        return ExtensionCandidate(
            candidate_id=candidate_id,
            original_rule=original_rule,
            dataset_name=dataset_name,
            suggested_action_name=suggested_action_name,
            frequency=new_freq,
            status=status,
            created_at=created_at,
            updated_at=now_ts,
        )

    def get_extension_candidates(self) -> List[ExtensionCandidate]:
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM extension_candidates ORDER BY frequency DESC")
            rows = cur.fetchall()
        return [
            ExtensionCandidate(
                candidate_id=r["candidate_id"],
                original_rule=r["original_rule"],
                dataset_name=r["dataset_name"],
                suggested_action_name=r["suggested_action_name"],
                frequency=r["frequency"],
                status=r["status"],
                created_at=r["created_at"],
                updated_at=r["updated_at"],
            ) for r in rows
        ]

    def get_rule(self, rule_id: str, version: Optional[int] = None) -> Optional[PromotedRule]:
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            if version is not None:
                cur.execute("SELECT * FROM promoted_rules WHERE rule_id = ? AND version = ?", (rule_id, version))
            else:
                cur.execute("SELECT * FROM promoted_rules WHERE rule_id = ? ORDER BY version DESC LIMIT 1", (rule_id,))
            row = cur.fetchone()

        if not row:
            return None

        return PromotedRule(
            rule_id=row["rule_id"],
            version=row["version"],
            name=row["name"],
            description=row["description"],
            action_type=ActionType(row["action_type"]),
            parameters=json.loads(row["parameters_json"]),
            target_columns=json.loads(row["target_columns_json"]),
            dataset_scope=row["dataset_scope"],
            schema_signature=SchemaSignature.model_validate(json.loads(row["schema_signature_json"])),
            status=RuleLifecycleState(row["status"]),
            rule_intent_pattern=row["rule_intent_pattern"],
            created_by=row["created_by"],
            approved_by=row["approved_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_history(self, rule_id: str) -> Dict[str, Any]:
        with _REPO_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM promoted_rules WHERE rule_id = ? ORDER BY version ASC", (rule_id,))
            rule_rows = cur.fetchall()
            cur.execute("SELECT * FROM promotion_evidence WHERE rule_id = ? ORDER BY timestamp ASC", (rule_id,))
            ev_rows = cur.fetchall()

        return {
            "rule_id": rule_id,
            "versions": [dict(r) for r in rule_rows],
            "evidence": [dict(r) for r in ev_rows],
        }
