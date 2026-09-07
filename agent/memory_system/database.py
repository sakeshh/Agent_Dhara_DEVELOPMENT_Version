"""
Database layer for the Enterprise Memory System.
Provides thread-safe SQLite connection management and migration scripts.
"""
from __future__ import annotations
import os
import sqlite3
import threading
import logging
from typing import Any, List, Dict, Optional
from contextlib import contextmanager

from agent.memory_system.config import get_config

logger = logging.getLogger("agent.memory_system.database")

_thread_local = threading.local()
_db_lock = threading.Lock()


def get_db_path() -> str:
    path = get_config().db_path
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def get_db_connection() -> sqlite3.Connection:
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None:
        conn = sqlite3.connect(get_db_path(), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            try:
                conn.execute("PRAGMA journal_mode=DELETE")
            except Exception:
                pass
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        _thread_local.conn = conn
    return _thread_local.conn


@contextmanager
def get_db():
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database transaction error: {e}")
        raise e
def ensure_user_exists(conn: sqlite3.Connection, user_id: str, display_name: str = "User"):
    if not user_id:
        return
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, display_name) VALUES (?, ?)",
        (user_id, display_name),
    )


def init_db():
    """Create all tables, indexes, and initial migrations if missing."""
    with _db_lock:
        conn = sqlite3.connect(get_db_path(), timeout=30.0)
        conn.execute("PRAGMA foreign_keys=ON")
        cursor = conn.cursor()

        # 1. Users Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            email TEXT UNIQUE,
            display_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert default user if not exists
        default_uid = get_config().default_user_id
        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id, display_name) VALUES (?, ?)",
            (default_uid, "Default User")
        )

        # 2. Conversations Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            is_deleted INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_conversations_user_updated ON conversations(user_id, is_deleted, updated_at DESC);")

        # 3. Messages Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('system', 'user', 'assistant')),
            content TEXT NOT NULL,
            metadata_json TEXT DEFAULT '{}',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_conv_timestamp ON messages(conversation_id, timestamp ASC);")

        # 4. Conversation Summaries Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversation_summaries (
            summary_id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            last_summarized_message_id TEXT,
            version INTEGER DEFAULT 1,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_summaries_conv_ver ON conversation_summaries(conversation_id, version DESC);")

        # 5. User Memories Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_memories (
            memory_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            memory_type TEXT NOT NULL CHECK(memory_type IN ('Profile', 'Preference', 'Skill', 'Goal', 'Project', 'Interest')),
            memory_text TEXT NOT NULL,
            confidence_score REAL DEFAULT 1.0,
            source_conversation_id TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_user_type ON user_memories(user_id, memory_type, is_active);")

        # 6. User Preferences Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            preference_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL UNIQUE,
            preferred_language TEXT DEFAULT 'English',
            preferred_coding_language TEXT DEFAULT 'Python',
            preferred_response_length TEXT DEFAULT 'detailed',
            preferred_tone TEXT DEFAULT 'professional',
            preferred_depth TEXT DEFAULT 'deep_dive',
            custom_instructions TEXT DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """)

        # Ensure default user preferences exist
        cursor.execute(
            "INSERT OR IGNORE INTO user_preferences (preference_id, user_id) VALUES (?, ?)",
            (f"pref_{default_uid}", default_uid)
        )

        # 7. Response Feedback Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS message_feedback (
            feedback_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            rating INTEGER CHECK(rating IN (-1, 1)),
            comment TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(message_id) REFERENCES messages(message_id) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_msg ON message_feedback(message_id);")

        # 8. Text Embeddings Table (Local Vector Storage fallback)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS text_embeddings (
            embedding_id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL CHECK(entity_type IN ('message', 'summary', 'memory')),
            entity_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            vector_json TEXT NOT NULL,
            content_chunk TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_user_entity ON text_embeddings(user_id, entity_type, entity_id);")

        conn.commit()
        conn.close()
        logger.info("Enterprise Memory System database initialized successfully.")
