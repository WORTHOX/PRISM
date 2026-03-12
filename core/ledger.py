"""
Prism - Semantic Ledger
========================
The append-only audit trail. Every AI decision and every human override
is written here permanently. This is the "Maker-Checker" log.

Supports multiple backend adapters (DuckDB for local, PostgreSQL for enterprise production).
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum
import duckdb
# psycopg2 imported dynamically for Postgres driver support

# --- Data Directory ---
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
LEDGER_DB_PATH = str(DATA_DIR / "prism_ledger.duckdb")

class AIDecision(str, Enum):
    PASS = "PASS"
    HOLD = "HOLD"
    BLOCK = "BLOCK"

class HumanDecision(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"


class BaseLedger:
    """Abstract interface for all underlying storage adapaters."""
    
    def log_ai_decision(self, pipeline_name: str, data_asset: str, ai_decision: AIDecision, ai_reason: str, ai_confidence: float = 1.0, ai_fix_suggestion: str = None, fingerprint_delta: float = 0.0, rows_affected: int = 0, snapshot_used: bool = False) -> str:
        raise NotImplementedError

    def log_human_override(self, event_id: str, human_name: str, human_email: str, human_decision: HumanDecision, human_note: str = "", override_impact: str = "") -> str:
        raise NotImplementedError

    def register_contract(self, data_asset: str, plain_english: str, compiled_rules: str, created_by: str) -> str:
        raise NotImplementedError

    def save_fingerprint(self, data_asset: str, fingerprint_vec: str, row_count: int, column_stats: str, status: str = "good") -> str:
        raise NotImplementedError

    def get_recent_decisions(self, limit: int = 50) -> list[dict]:
        raise NotImplementedError

    def get_pending_reviews(self) -> list[dict]:
        raise NotImplementedError

    def get_contracts(self) -> list[dict]:
        raise NotImplementedError

    def get_last_good_fingerprint(self, data_asset: str) -> dict | None:
        raise NotImplementedError

    def get_dashboard_stats(self) -> dict:
        raise NotImplementedError


class DuckDBLedger(BaseLedger):
    def __init__(self, db_path: str = LEDGER_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return duckdb.connect(self.db_path)
        
    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_ledger (
                event_id         TEXT PRIMARY KEY,
                timestamp        TIMESTAMP NOT NULL,
                pipeline_name    TEXT NOT NULL,
                data_asset       TEXT NOT NULL,
                ai_decision      TEXT NOT NULL,
                ai_confidence    REAL,
                ai_reason        TEXT,
                ai_fix_suggestion TEXT,
                fingerprint_delta REAL,
                rows_affected    INTEGER,
                snapshot_used    BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS human_override_log (
                override_id      TEXT PRIMARY KEY,
                event_id         TEXT NOT NULL,
                timestamp        TIMESTAMP NOT NULL,
                human_name       TEXT NOT NULL,
                human_email      TEXT NOT NULL,
                human_decision   TEXT NOT NULL,
                human_note       TEXT,
                override_impact  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contract_registry (
                contract_id      TEXT PRIMARY KEY,
                data_asset       TEXT NOT NULL UNIQUE,
                plain_english    TEXT NOT NULL,
                compiled_rules   TEXT,
                created_at       TIMESTAMP NOT NULL,
                updated_at       TIMESTAMP NOT NULL,
                created_by       TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fingerprint_snapshots (
                snapshot_id      TEXT PRIMARY KEY,
                data_asset       TEXT NOT NULL,
                timestamp        TIMESTAMP NOT NULL,
                fingerprint_vec  TEXT NOT NULL,
                row_count        INTEGER,
                column_stats     TEXT,
                status           TEXT DEFAULT 'good'
            )
        """)
        conn.close()

    def log_ai_decision(self, pipeline_name: str, data_asset: str, ai_decision: AIDecision, ai_reason: str, ai_confidence: float = 1.0, ai_fix_suggestion: str = None, fingerprint_delta: float = 0.0, rows_affected: int = 0, snapshot_used: bool = False) -> str:
        event_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO audit_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [event_id, ts, pipeline_name, data_asset, ai_decision.value, ai_confidence, ai_reason, ai_fix_suggestion, fingerprint_delta, rows_affected, snapshot_used]
        )
        conn.close()
        return event_id

    def log_human_override(self, event_id: str, human_name: str, human_email: str, human_decision: HumanDecision, human_note: str = "", override_impact: str = "") -> str:
        override_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO human_override_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [override_id, event_id, ts, human_name, human_email, human_decision.value, human_note, override_impact]
        )
        conn.close()
        return override_id

    def register_contract(self, data_asset: str, plain_english: str, compiled_rules: str, created_by: str) -> str:
        contract_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO contract_registry VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (data_asset) DO UPDATE SET
                plain_english = excluded.plain_english,
                compiled_rules = excluded.compiled_rules,
                updated_at = excluded.updated_at
        """, [contract_id, data_asset, plain_english, compiled_rules, ts, ts, created_by])
        conn.close()
        return contract_id

    def save_fingerprint(self, data_asset: str, fingerprint_vec: str, row_count: int, column_stats: str, status: str = "good") -> str:
        snapshot_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO fingerprint_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)",
            [snapshot_id, data_asset, ts, fingerprint_vec, row_count, column_stats, status]
        )
        conn.close()
        return snapshot_id

    def get_recent_decisions(self, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT
                a.event_id, a.timestamp, a.pipeline_name, a.data_asset,
                a.ai_decision, a.ai_confidence, a.ai_reason, a.ai_fix_suggestion,
                a.fingerprint_delta, a.rows_affected, a.snapshot_used,
                h.human_name, h.human_email, h.human_decision, h.human_note, h.timestamp AS override_timestamp
            FROM audit_ledger a
            LEFT JOIN human_override_log h ON a.event_id = h.event_id
            ORDER BY a.timestamp DESC
            LIMIT ?
        """, [limit]).fetchall()
        conn.close()
        columns = ["event_id", "timestamp", "pipeline_name", "data_asset", "ai_decision", "ai_confidence", "ai_reason", "ai_fix_suggestion", "fingerprint_delta", "rows_affected", "snapshot_used", "human_name", "human_email", "human_decision", "human_note", "override_timestamp"]
        return [dict(zip(columns, row)) for row in rows]

    def get_pending_reviews(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT
                a.event_id, a.timestamp, a.pipeline_name, a.data_asset,
                a.ai_decision, a.ai_confidence, a.ai_reason, a.ai_fix_suggestion, a.fingerprint_delta
            FROM audit_ledger a
            WHERE a.ai_decision IN ('HOLD', 'BLOCK')
              AND a.event_id NOT IN (SELECT event_id FROM human_override_log)
            ORDER BY a.timestamp DESC
        """).fetchall()
        conn.close()
        columns = ["event_id", "timestamp", "pipeline_name", "data_asset", "ai_decision", "ai_confidence", "ai_reason", "ai_fix_suggestion", "fingerprint_delta"]
        return [dict(zip(columns, row)) for row in rows]

    def get_contracts(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT contract_id, data_asset, plain_english, compiled_rules,
                   created_at, updated_at, created_by
            FROM contract_registry
            ORDER BY updated_at DESC
        """).fetchall()
        conn.close()
        columns = ["contract_id", "data_asset", "plain_english", "compiled_rules", "created_at", "updated_at", "created_by"]
        return [dict(zip(columns, row)) for row in rows]

    def get_last_good_fingerprint(self, data_asset: str) -> dict | None:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT snapshot_id, fingerprint_vec, row_count, column_stats, timestamp
            FROM fingerprint_snapshots
            WHERE data_asset = ? AND status = 'good'
            ORDER BY timestamp DESC
            LIMIT 1
        """, [data_asset]).fetchone()
        conn.close()
        if not row:
            return None
        return {
            "snapshot_id": row[0],
            "fingerprint_vec": row[1],
            "row_count": row[2],
            "column_stats": row[3],
            "timestamp": row[4],
        }

    def get_dashboard_stats(self) -> dict:
        conn = self._get_conn()
        stats = conn.execute("""
            SELECT
                COUNT(*) AS total_events,
                SUM(CASE WHEN ai_decision = 'PASS' THEN 1 ELSE 0 END) AS total_pass,
                SUM(CASE WHEN ai_decision = 'HOLD' THEN 1 ELSE 0 END) AS total_hold,
                SUM(CASE WHEN ai_decision = 'BLOCK' THEN 1 ELSE 0 END) AS total_block
            FROM audit_ledger
        """).fetchone()

        pending = conn.execute("""
            SELECT COUNT(*) FROM audit_ledger a
            WHERE a.ai_decision IN ('HOLD', 'BLOCK')
              AND a.event_id NOT IN (SELECT event_id FROM human_override_log)
        """).fetchone()[0]

        overrides = conn.execute("""
            SELECT COUNT(*) FROM human_override_log
            WHERE human_decision = 'APPROVED'
        """).fetchone()[0]
        conn.close()

        return {
            "total_events": stats[0] or 0,
            "total_pass": stats[1] or 0,
            "total_hold": stats[2] or 0,
            "total_block": stats[3] or 0,
            "pending_review": pending or 0,
            "human_overrides": overrides or 0,
        }

class PostgresLedger(BaseLedger):
    """
    Enterprise adapter for PostgreSQL using psycopg2.
    Translates the DuckDB schema and dialect into standard Postgres format.
    """
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._init_db()

    def _get_conn(self):
        import psycopg2
        return psycopg2.connect(self.db_url)

    def _init_db(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_ledger (
                event_id         TEXT PRIMARY KEY,
                timestamp        TIMESTAMP WITH TIME ZONE NOT NULL,
                pipeline_name    TEXT NOT NULL,
                data_asset       TEXT NOT NULL,
                ai_decision      TEXT NOT NULL,
                ai_confidence    REAL,
                ai_reason        TEXT,
                ai_fix_suggestion TEXT,
                fingerprint_delta REAL,
                rows_affected    INTEGER,
                snapshot_used    BOOLEAN DEFAULT FALSE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS human_override_log (
                override_id      TEXT PRIMARY KEY,
                event_id         TEXT NOT NULL,
                timestamp        TIMESTAMP WITH TIME ZONE NOT NULL,
                human_name       TEXT NOT NULL,
                human_email      TEXT NOT NULL,
                human_decision   TEXT NOT NULL,
                human_note       TEXT,
                override_impact  TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contract_registry (
                contract_id      TEXT PRIMARY KEY,
                data_asset       TEXT NOT NULL UNIQUE,
                plain_english    TEXT NOT NULL,
                compiled_rules   TEXT,
                created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
                created_by       TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fingerprint_snapshots (
                snapshot_id      TEXT PRIMARY KEY,
                data_asset       TEXT NOT NULL,
                timestamp        TIMESTAMP WITH TIME ZONE NOT NULL,
                fingerprint_vec  TEXT NOT NULL,
                row_count        INTEGER,
                column_stats     TEXT,
                status           TEXT DEFAULT 'good'
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

    def log_ai_decision(self, pipeline_name: str, data_asset: str, ai_decision: AIDecision, ai_reason: str, ai_confidence: float = 1.0, ai_fix_suggestion: str = None, fingerprint_delta: float = 0.0, rows_affected: int = 0, snapshot_used: bool = False) -> str:
        event_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO audit_ledger VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [event_id, ts, pipeline_name, data_asset, ai_decision.value, ai_confidence, ai_reason, ai_fix_suggestion, fingerprint_delta, rows_affected, snapshot_used]
        )
        conn.commit()
        cursor.close()
        conn.close()
        return event_id

    def log_human_override(self, event_id: str, human_name: str, human_email: str, human_decision: HumanDecision, human_note: str = "", override_impact: str = "") -> str:
        override_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO human_override_log VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [override_id, event_id, ts, human_name, human_email, human_decision.value, human_note, override_impact]
        )
        conn.commit()
        cursor.close()
        conn.close()
        return override_id

    def register_contract(self, data_asset: str, plain_english: str, compiled_rules: str, created_by: str) -> str:
        contract_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO contract_registry VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (data_asset) DO UPDATE SET
                plain_english = EXCLUDED.plain_english,
                compiled_rules = EXCLUDED.compiled_rules,
                updated_at = EXCLUDED.updated_at
        """, [contract_id, data_asset, plain_english, compiled_rules, ts, ts, created_by])
        conn.commit()
        cursor.close()
        conn.close()
        return contract_id

    def save_fingerprint(self, data_asset: str, fingerprint_vec: str, row_count: int, column_stats: str, status: str = "good") -> str:
        snapshot_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc)
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO fingerprint_snapshots VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [snapshot_id, data_asset, ts, fingerprint_vec, row_count, column_stats, status]
        )
        conn.commit()
        cursor.close()
        conn.close()
        return snapshot_id

    def get_recent_decisions(self, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                a.event_id, a.timestamp, a.pipeline_name, a.data_asset,
                a.ai_decision, a.ai_confidence, a.ai_reason, a.ai_fix_suggestion,
                a.fingerprint_delta, a.rows_affected, a.snapshot_used,
                h.human_name, h.human_email, h.human_decision, h.human_note, h.timestamp AS override_timestamp
            FROM audit_ledger a
            LEFT JOIN human_override_log h ON a.event_id = h.event_id
            ORDER BY a.timestamp DESC
            LIMIT %s
        """, [limit])
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        columns = ["event_id", "timestamp", "pipeline_name", "data_asset", "ai_decision", "ai_confidence", "ai_reason", "ai_fix_suggestion", "fingerprint_delta", "rows_affected", "snapshot_used", "human_name", "human_email", "human_decision", "human_note", "override_timestamp"]
        return [dict(zip(columns, row)) for row in rows]

    def get_pending_reviews(self) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                a.event_id, a.timestamp, a.pipeline_name, a.data_asset,
                a.ai_decision, a.ai_confidence, a.ai_reason, a.ai_fix_suggestion, a.fingerprint_delta
            FROM audit_ledger a
            WHERE a.ai_decision IN ('HOLD', 'BLOCK')
              AND a.event_id NOT IN (SELECT event_id FROM human_override_log)
            ORDER BY a.timestamp DESC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        columns = ["event_id", "timestamp", "pipeline_name", "data_asset", "ai_decision", "ai_confidence", "ai_reason", "ai_fix_suggestion", "fingerprint_delta"]
        return [dict(zip(columns, row)) for row in rows]

    def get_contracts(self) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT contract_id, data_asset, plain_english, compiled_rules,
                   created_at, updated_at, created_by
            FROM contract_registry
            ORDER BY updated_at DESC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        columns = ["contract_id", "data_asset", "plain_english", "compiled_rules", "created_at", "updated_at", "created_by"]
        return [dict(zip(columns, row)) for row in rows]

    def get_last_good_fingerprint(self, data_asset: str) -> dict | None:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_id, fingerprint_vec, row_count, column_stats, timestamp
            FROM fingerprint_snapshots
            WHERE data_asset = %s AND status = 'good'
            ORDER BY timestamp DESC
            LIMIT 1
        """, [data_asset])
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return None
        return {
            "snapshot_id": row[0],
            "fingerprint_vec": row[1],
            "row_count": row[2],
            "column_stats": row[3],
            "timestamp": row[4],
        }

    def get_dashboard_stats(self) -> dict:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*) AS total_events,
                SUM(CASE WHEN ai_decision = 'PASS' THEN 1 ELSE 0 END) AS total_pass,
                SUM(CASE WHEN ai_decision = 'HOLD' THEN 1 ELSE 0 END) AS total_hold,
                SUM(CASE WHEN ai_decision = 'BLOCK' THEN 1 ELSE 0 END) AS total_block
            FROM audit_ledger
        """)
        stats = cursor.fetchone()

        cursor.execute("""
            SELECT COUNT(*) FROM audit_ledger a
            WHERE a.ai_decision IN ('HOLD', 'BLOCK')
              AND a.event_id NOT IN (SELECT event_id FROM human_override_log)
        """)
        pending = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM human_override_log
            WHERE human_decision = 'APPROVED'
        """)
        overrides = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return {
            "total_events": stats[0] or 0,
            "total_pass": stats[1] or 0,
            "total_hold": stats[2] or 0,
            "total_block": stats[3] or 0,
            "pending_review": pending or 0,
            "human_overrides": overrides or 0,
        }

# --- FACTORY PATTERN FOR SINGLETON ACCESS ---

def _get_ledger_instance() -> BaseLedger:
    ledger_type = os.environ.get("PRISM_LEDGER_TYPE", "duckdb").lower()
    if ledger_type == "postgres":
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL must be set if PRISM_LEDGER_TYPE=postgres")
        return PostgresLedger(db_url)
    return DuckDBLedger()

_ledger = _get_ledger_instance()

# --- MODULE LEVEL WRAPPERS (Preserving existing API) ---

def log_ai_decision(*args, **kwargs):
    return _ledger.log_ai_decision(*args, **kwargs)

def log_human_override(*args, **kwargs):
    return _ledger.log_human_override(*args, **kwargs)

def register_contract(*args, **kwargs):
    return _ledger.register_contract(*args, **kwargs)

def save_fingerprint(*args, **kwargs):
    return _ledger.save_fingerprint(*args, **kwargs)

def get_recent_decisions(*args, **kwargs):
    return _ledger.get_recent_decisions(*args, **kwargs)

def get_pending_reviews(*args, **kwargs):
    return _ledger.get_pending_reviews(*args, **kwargs)

def get_contracts(*args, **kwargs):
    return _ledger.get_contracts(*args, **kwargs)

def get_last_good_fingerprint(*args, **kwargs):
    return _ledger.get_last_good_fingerprint(*args, **kwargs)

def get_dashboard_stats(*args, **kwargs):
    return _ledger.get_dashboard_stats(*args, **kwargs)
