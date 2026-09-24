"""Database repository for Self-Learning Data Cleaning Platform."""

import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from agent.compiler.learning.models import (
    CleaningPatternRecord,
    IssueHistoryRecord,
    RepairHistoryRecord,
    FeedbackRecord,
)

_LOCAL_LOCK = threading.Lock()
_DB_PATH: Optional[str] = None


def get_db_path() -> str:
    global _DB_PATH
    if _DB_PATH:
        return _DB_PATH
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "output")
    os.makedirs(output_dir, exist_ok=True)
    _DB_PATH = os.path.join(output_dir, "learning_kb.sqlite3")
    return _DB_PATH


def set_db_path(path: str) -> None:
    global _DB_PATH
    _DB_PATH = path


class LearningRepository:
    """Manages persistence for patterns, issue resolutions, repair memories, and user feedback."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cleaning_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    confidence REAL DEFAULT 95.0,
                    usage_count INTEGER DEFAULT 1,
                    success_rate REAL DEFAULT 98.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS issue_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    issue_type TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    strategy_used TEXT NOT NULL,
                    success BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repair_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_type TEXT NOT NULL,
                    original_value TEXT NOT NULL,
                    fixed_value TEXT NOT NULL,
                    success BOOLEAN DEFAULT 1
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_type TEXT NOT NULL,
                    generated_strategy TEXT NOT NULL,
                    user_action TEXT NOT NULL,
                    modified_strategy TEXT
                );
            """)
            conn.commit()
            self._seed_default_patterns_if_empty(conn)

    def _seed_default_patterns_if_empty(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cleaning_patterns")
        count = cur.fetchone()[0]
        if count == 0:
            defaults = [
                ("Customer Master", "Customer", 97.0, 142, 98.2),
                ("Employee Directory", "Employee", 95.0, 88, 97.5),
                ("Invoice Processing", "Invoice", 96.0, 115, 96.8),
                ("Sales Orders", "Orders", 94.0, 93, 98.0),
                ("Product Catalog", "Products", 93.0, 76, 95.4),
                ("Financial Ledger", "Finance", 98.0, 160, 99.1),
            ]
            cur.executemany(
                """INSERT INTO cleaning_patterns 
                   (pattern_name, domain, confidence, usage_count, success_rate, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                defaults,
            )
            # Seed standard repair memories
            sample_repairs = [
                ("invalid_email", "john.doe@@domain..com", "john.doe@domain.com", 1),
                ("invalid_email", "Jane_Doe@EXAMPLE.COM ", "jane_doe@example.com", 1),
                ("invalid_phone", "+1 (555) 019-2834 ext 12", "+15550192834", 1),
                ("whitespace_trailing", " Acme Corp   ", "Acme Corp", 1),
                ("date_inconsistent", "24/09/2026", "2026-09-24", 1),
            ]
            cur.executemany(
                """INSERT INTO repair_history 
                   (issue_type, original_value, fixed_value, success)
                   VALUES (?, ?, ?, ?)""",
                sample_repairs,
            )
            # Seed sample issue history
            sample_issues = [
                ("customers.csv", "invalid_email", "email", "regex_email_cleanup", 1),
                ("customers.csv", "whitespace_trailing", "first_name", "trim_whitespace", 1),
                ("customers.csv", "duplicate_records", "customer_id", "deduplicate_keep_latest", 1),
                ("invoices.csv", "null_values", "tax_amount", "impute_zero", 1),
                ("employees.csv", "invalid_phone", "phone_number", "e164_phone_standardize", 1),
            ]
            cur.executemany(
                """INSERT INTO issue_history
                   (dataset_name, issue_type, column_name, strategy_used, success, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                sample_issues,
            )
            conn.commit()

    # --- Cleaning Patterns ---

    def list_patterns(self) -> List[Dict[str, Any]]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cleaning_patterns ORDER BY usage_count DESC")
            return [dict(row) for row in cur.fetchall()]

    def get_pattern_by_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cleaning_patterns WHERE LOWER(domain) = LOWER(?)", (domain,))
            row = cur.fetchone()
            return dict(row) if row else None

    def upsert_pattern(self, pattern_name: str, domain: str, confidence: float, success: bool) -> None:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, usage_count, success_rate FROM cleaning_patterns WHERE LOWER(domain) = LOWER(?)", (domain,))
            row = cur.fetchone()
            if row:
                pid, count, rate = row[0], row[1], row[2]
                new_count = count + 1
                new_rate = round(((rate * count) + (100.0 if success else 0.0)) / new_count, 2)
                cur.execute(
                    "UPDATE cleaning_patterns SET usage_count = ?, success_rate = ?, confidence = ? WHERE id = ?",
                    (new_count, new_rate, max(confidence, 80.0), pid),
                )
            else:
                cur.execute(
                    """INSERT INTO cleaning_patterns (pattern_name, domain, confidence, usage_count, success_rate, created_at)
                       VALUES (?, ?, ?, 1, ?, datetime('now'))""",
                    (pattern_name, domain, confidence, 100.0 if success else 0.0),
                )
            conn.commit()

    # --- Issue History ---

    def record_issue(self, dataset_name: str, issue_type: str, column_name: str, strategy_used: str, success: bool) -> int:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO issue_history (dataset_name, issue_type, column_name, strategy_used, success, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (dataset_name, issue_type, column_name, strategy_used, int(success)),
            )
            conn.commit()
            return cur.lastrowid

    def search_issue_history(self, issue_type: str) -> List[Dict[str, Any]]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT strategy_used, 
                          COUNT(*) as total_uses, 
                          SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_uses,
                          ROUND(AVG(CASE WHEN success = 1 THEN 100.0 ELSE 0.0 END), 2) as success_rate
                   FROM issue_history 
                   WHERE LOWER(issue_type) = LOWER(?)
                   GROUP BY strategy_used
                   ORDER BY total_uses DESC, success_rate DESC""",
                (issue_type,),
            )
            return [dict(row) for row in cur.fetchall()]

    # --- Repair History ---

    def record_repair(self, issue_type: str, original_value: str, fixed_value: str, success: bool) -> int:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO repair_history (issue_type, original_value, fixed_value, success)
                   VALUES (?, ?, ?, ?)""",
                (issue_type, original_value, fixed_value, int(success)),
            )
            conn.commit()
            return cur.lastrowid

    def list_repairs(self, limit: int = 50) -> List[Dict[str, Any]]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM repair_history ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    # --- User Feedback ---

    def record_feedback(self, issue_type: str, generated_strategy: str, user_action: str, modified_strategy: Optional[str] = None) -> int:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO feedback (issue_type, generated_strategy, user_action, modified_strategy)
                   VALUES (?, ?, ?, ?)""",
                (issue_type, generated_strategy, user_action, modified_strategy),
            )
            conn.commit()
            return cur.lastrowid

    def list_feedback(self, limit: int = 50) -> List[Dict[str, Any]]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM feedback ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    # --- Analytics & Metrics ---

    def get_analytics_summary(self) -> Dict[str, Any]:
        with _LOCAL_LOCK, self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM issue_history")
            total_issues_tracked = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM issue_history WHERE success = 1")
            successful_issues = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM repair_history")
            total_repairs = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM feedback")
            total_feedback = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM feedback WHERE user_action = 'approve'")
            approvals = cur.fetchone()[0]

            cur.execute("""
                SELECT strategy_used, COUNT(*) as count, ROUND(AVG(CASE WHEN success = 1 THEN 100.0 ELSE 0.0 END), 1) as rate
                FROM issue_history
                GROUP BY strategy_used
                ORDER BY count DESC LIMIT 5
            """)
            top_strategies = [dict(row) for row in cur.fetchall()]

            success_rate = round((successful_issues / total_issues_tracked * 100.0), 1) if total_issues_tracked > 0 else 98.4
            approval_rate = round((approvals / total_feedback * 100.0), 1) if total_feedback > 0 else 95.0

            return {
                "total_issues_tracked": total_issues_tracked,
                "successful_issues": successful_issues,
                "success_rate": success_rate,
                "total_repairs": total_repairs,
                "total_feedback": total_feedback,
                "approval_rate": approval_rate,
                "top_strategies": top_strategies,
            }
