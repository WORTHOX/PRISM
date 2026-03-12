"""
Prism - Human-in-the-Loop (HITL) Review Module
================================================
The "Maker-Checker" layer. When AI says HOLD or BLOCK,
a human data steward can review and either:

  APPROVE — "AI was wrong, this data is fine. Let it through."
  REJECT  — "AI was right. This data stays blocked."

Every human decision is logged permanently: who approved,
when, and why. Full accountability trail.
"""

from core.ledger import (
    HumanDecision,
    AIDecision,
    log_human_override,
    get_pending_reviews,
    get_recent_decisions,
    _get_conn,
)


def approve_decision(
    event_id: str,
    human_name: str,
    human_email: str,
    human_note: str = "",
) -> dict:
    """
    Human overrides the AI — approves data that was HELD or BLOCKED.

    This is the "Maker-Checker" sanction. From this point forward, this
    data is considered valid and the AI learns from this correction.

    Returns a summary of what was sanctioned.
    """
    # Get the original AI decision
    conn = _get_conn()
    row = conn.execute("""
        SELECT pipeline_name, data_asset, ai_decision, ai_reason, ai_confidence
        FROM audit_ledger WHERE event_id = ?
    """, [event_id]).fetchone()
    conn.close()

    if not row:
        return {"error": f"Event {event_id} not found in ledger."}

    pipeline_name, data_asset, ai_decision, ai_reason, ai_confidence = row

    override_id = log_human_override(
        event_id=event_id,
        human_name=human_name,
        human_email=human_email,
        human_decision=HumanDecision.APPROVED,
        human_note=human_note,
        override_impact=(
            f"Human overrode AI {ai_decision} decision on '{data_asset}'. "
            f"Data will now proceed to consumers."
        ),
    )

    return {
        "override_id": override_id,
        "event_id": event_id,
        "action": "APPROVED",
        "sanctioned_by": f"{human_name} ({human_email})",
        "original_ai_decision": ai_decision,
        "data_asset": data_asset,
        "pipeline_name": pipeline_name,
        "note": human_note,
        "message": (
            f"✅ Override recorded. '{data_asset}' data approved by {human_name}. "
            f"This override is permanently logged in the audit ledger."
        ),
    }


def reject_decision(
    event_id: str,
    human_name: str,
    human_email: str,
    human_note: str = "",
) -> dict:
    """
    Human confirms the AI was correct — the HOLD/BLOCK stands.
    This reinforces the AI's decision in the audit trail.
    """
    conn = _get_conn()
    row = conn.execute("""
        SELECT pipeline_name, data_asset, ai_decision
        FROM audit_ledger WHERE event_id = ?
    """, [event_id]).fetchone()
    conn.close()

    if not row:
        return {"error": f"Event {event_id} not found in ledger."}

    pipeline_name, data_asset, ai_decision = row

    override_id = log_human_override(
        event_id=event_id,
        human_name=human_name,
        human_email=human_email,
        human_decision=HumanDecision.REJECTED,
        human_note=human_note,
        override_impact=(
            f"Human confirmed AI {ai_decision} on '{data_asset}'. "
            f"Data remains blocked."
        ),
    )

    return {
        "override_id": override_id,
        "event_id": event_id,
        "action": "REJECTED",
        "confirmed_by": f"{human_name} ({human_email})",
        "original_ai_decision": ai_decision,
        "data_asset": data_asset,
        "message": (
            f"🚫 AI {ai_decision} confirmed by {human_name}. "
            f"'{data_asset}' data remains blocked."
        ),
    }


def get_review_queue() -> list[dict]:
    """Get all decisions pending human review."""
    return get_pending_reviews()


def get_full_audit_trail(limit: int = 100) -> list[dict]:
    """Get the complete audit trail with human overrides merged."""
    return get_recent_decisions(limit=limit)


def get_accountability_report() -> list[dict]:
    """
    Generate an accountability report: who approved/rejected what.
    Perfect for compliance audits.
    """
    conn = _get_conn()
    rows = conn.execute("""
        SELECT
            h.human_name,
            h.human_email,
            h.human_decision,
            h.timestamp,
            h.human_note,
            a.data_asset,
            a.ai_decision AS original_ai_decision,
            a.ai_reason,
            a.pipeline_name
        FROM human_override_log h
        JOIN audit_ledger a ON h.event_id = a.event_id
        ORDER BY h.timestamp DESC
    """).fetchall()
    conn.close()

    columns = [
        "human_name", "human_email", "human_decision", "timestamp",
        "human_note", "data_asset", "original_ai_decision",
        "ai_reason", "pipeline_name"
    ]
    return [dict(zip(columns, row)) for row in rows]
