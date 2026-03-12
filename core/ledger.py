"""
Prism - Semantic Ledger
========================
The append-only audit trail. Every AI decision and every human override
is written here permanently. This is the "Maker-Checker" log.

Think of it like a bank's transaction ledger — immutable, tamper-proof,
and auditable by anyone with access.
"""

import duckdb
import uuid
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum

# --- Data Directory ---
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
LEDGER_DB_PATH = str(DATA_DIR / "prism_ledger.duckdb")


class AIDecision(str, Enum):
    PASS = "PASS"
    HOLD = "HOLD"
    BLOCK = "BLOCK"


class HumanDecision(str, Enum):
    APPROVED = "APPROVED"      # Human overrode AI (AI said BLOCK/HOLD but human said it's fine)
    REJECTED = "REJECTED"      # Human confirmed AI was right
    PENDING = "PENDING"        # Waiting for human review


def _get_conn():
    """Get a DuckDB connection and ensure tables exist."""
    conn = duckdb.connect(LEDGER_DB_PATH)

    # Main audit ledger — every AI decision
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_ledger (
            event_id         TEXT PRIMARY KEY,
            timestamp        TIMESTAMP NOT NULL,
            pipeline_name    TEXT NOT NULL,
            data_asset       TEXT NOT NULL,
            ai_decision      TEXT NOT NULL,         -- PASS / HOLD / BLOCK
            ai_confidence    REAL,                  -- 0.0 to 1.0
            ai_reason        TEXT,                  -- Plain English explanation
            ai_fix_suggestion TEXT,                 -- Auto-generated SQL / code fix
            fingerprint_delta REAL,                 -- How much did semantics drift? 0.0 = no drift
            rows_affected    INTEGER,
            snapshot_used    BOOLEAN DEFAULT FALSE  -- Did we serve last-known-good?
        )
    """)

    # Human override log — every time a human intervenes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS human_override_log (
            override_id      TEXT PRIMARY KEY,
            event_id         TEXT NOT NULL,         -- References audit_ledger
            timestamp        TIMESTAMP NOT NULL,
            human_name       TEXT NOT NULL,
            human_email      TEXT NOT NULL,
            human_decision   TEXT NOT NULL,         -- APPROVED / REJECTED
            human_note       TEXT,                  -- Why they approved/rejected
            override_impact  TEXT                   -- Downstream effect acknowledged
        )
    """)

    # Contract registry — the "rules" each data asset must follow
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contract_registry (
            contract_id      TEXT PRIMARY KEY,
            data_asset       TEXT NOT NULL UNIQUE,
            plain_english    TEXT NOT NULL,          -- Human-written contract
            compiled_rules   TEXT,                   -- JSON rules parsed by Gemini
            created_at       TIMESTAMP NOT NULL,
            updated_at       TIMESTAMP NOT NULL,
            created_by       TEXT NOT NULL
        )
    """)

    # Fingerprint snapshots — the "last known good" baseline per asset
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fingerprint_snapshots (
            snapshot_id      TEXT PRIMARY KEY,
            data_asset       TEXT NOT NULL,
            timestamp        TIMESTAMP NOT NULL,
            fingerprint_vec  TEXT NOT NULL,          -- JSON array (embedding)
            row_count        INTEGER,
            column_stats     TEXT,                   -- JSON summary stats
            status           TEXT DEFAULT 'good'     -- 'good' | 'quarantined'
        )
    """)

    return conn


# ─────────────────────────────────────────────
#  WRITE FUNCTIONS
# ─────────────────────────────────────────────

def log_ai_decision(
    pipeline_name: str,
    data_asset: str,
    ai_decision: AIDecision,
    ai_reason: str,
    ai_confidence: float = 1.0,
    ai_fix_suggestion: str = None,
    fingerprint_delta: float = 0.0,
    rows_affected: int = 0,
    snapshot_used: bool = False,
) -> str:
    """
    Write an AI decision to the audit ledger.
    Returns the event_id for reference.
    """
    event_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)

    conn = _get_conn()
    conn.execute("""
        INSERT INTO audit_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        event_id, ts, pipeline_name, data_asset,
        ai_decision.value, ai_confidence, ai_reason,
        ai_fix_suggestion, fingerprint_delta,
        rows_affected, snapshot_used
    ])
    conn.close()

    return event_id


def log_human_override(
    event_id: str,
    human_name: str,
    human_email: str,
    human_decision: HumanDecision,
    human_note: str = "",
    override_impact: str = "",
) -> str:
    """
    Write a human override to the override log.
    This is the "Maker-Checker" record — who sanctioned what.
    Returns the override_id.
    """
    override_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)

    conn = _get_conn()
    conn.execute("""
        INSERT INTO human_override_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        override_id, event_id, ts,
        human_name, human_email,
        human_decision.value, human_note, override_impact
    ])
    conn.close()

    return override_id


def register_contract(
    data_asset: str,
    plain_english: str,
    compiled_rules: str,
    created_by: str,
) -> str:
    """Register or update a data contract for an asset."""
    contract_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)

    conn = _get_conn()
    # Upsert: update if exists, insert if new
    conn.execute("""
        INSERT INTO contract_registry VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (data_asset) DO UPDATE SET
            plain_english = excluded.plain_english,
            compiled_rules = excluded.compiled_rules,
            updated_at = excluded.updated_at
    """, [contract_id, data_asset, plain_english, compiled_rules, ts, ts, created_by])
    conn.close()

    return contract_id


def save_fingerprint(
    data_asset: str,
    fingerprint_vec: str,
    row_count: int,
    column_stats: str,
    status: str = "good",
) -> str:
    """Save a fingerprint snapshot for an asset."""
    snapshot_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)

    conn = _get_conn()
    conn.execute("""
        INSERT INTO fingerprint_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [snapshot_id, data_asset, ts, fingerprint_vec, row_count, column_stats, status])
    conn.close()

    return snapshot_id


# ─────────────────────────────────────────────
#  READ FUNCTIONS
# ─────────────────────────────────────────────

def get_recent_decisions(limit: int = 50) -> list[dict]:
    """Get recent AI decisions with their human overrides joined."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT
            a.event_id,
            a.timestamp,
            a.pipeline_name,
            a.data_asset,
            a.ai_decision,
            a.ai_confidence,
            a.ai_reason,
            a.ai_fix_suggestion,
            a.fingerprint_delta,
            a.rows_affected,
            a.snapshot_used,
            h.human_name,
            h.human_email,
            h.human_decision,
            h.human_note,
            h.timestamp AS override_timestamp
        FROM audit_ledger a
        LEFT JOIN human_override_log h ON a.event_id = h.event_id
        ORDER BY a.timestamp DESC
        LIMIT ?
    """, [limit]).fetchall()
    conn.close()

    columns = [
        "event_id", "timestamp", "pipeline_name", "data_asset",
        "ai_decision", "ai_confidence", "ai_reason", "ai_fix_suggestion",
        "fingerprint_delta", "rows_affected", "snapshot_used",
        "human_name", "human_email", "human_decision",
        "human_note", "override_timestamp"
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_pending_reviews() -> list[dict]:
    """Get all HOLD/BLOCK decisions that have no human override yet."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT
            a.event_id,
            a.timestamp,
            a.pipeline_name,
            a.data_asset,
            a.ai_decision,
            a.ai_confidence,
            a.ai_reason,
            a.ai_fix_suggestion,
            a.fingerprint_delta
        FROM audit_ledger a
        WHERE a.ai_decision IN ('HOLD', 'BLOCK')
          AND a.event_id NOT IN (SELECT event_id FROM human_override_log)
        ORDER BY a.timestamp DESC
    """).fetchall()
    conn.close()

    columns = [
        "event_id", "timestamp", "pipeline_name", "data_asset",
        "ai_decision", "ai_confidence", "ai_reason",
        "ai_fix_suggestion", "fingerprint_delta"
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_contracts() -> list[dict]:
    """Get all registered contracts."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT contract_id, data_asset, plain_english, compiled_rules,
               created_at, updated_at, created_by
        FROM contract_registry
        ORDER BY updated_at DESC
    """).fetchall()
    conn.close()

    columns = [
        "contract_id", "data_asset", "plain_english", "compiled_rules",
        "created_at", "updated_at", "created_by"
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_last_good_fingerprint(data_asset: str) -> dict | None:
    """Get the most recent 'good' fingerprint snapshot for an asset."""
    conn = _get_conn()
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


def get_dashboard_stats() -> dict:
    """Get summary stats for the dashboard."""
    conn = _get_conn()
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
