"""
Prism - Decision Engine
=========================
The heart of Prism. Takes incoming data, runs it through the
semantic contract, compares fingerprints, and makes the call:

  ✅ PASS  — Data is clean. Forward it to consumers.
  ⚠️ HOLD  — Something looks off. Serve last-known-good, queue for human review.
  ❌ BLOCK — Clear violation. Stop the data. Explain why. Suggest fix.

Every decision is logged to the Semantic Ledger — always.
"""

import json
import pandas as pd
from dataclasses import dataclass

from core.ledger import (
    AIDecision,
    log_ai_decision,
    save_fingerprint,
    get_last_good_fingerprint,
    get_dashboard_stats,
)
from core.fingerprinter import (
    fingerprint_dataframe,
    compute_drift_score,
    explain_drift,
)
from core.contracts import (
    get_contract_for_asset,
    generate_fix_suggestion,
)


# ─── Drift thresholds ────────────────────────────────────────────────
DRIFT_PASS_THRESHOLD  = 0.15   # Below this: PASS  (normal variation)
DRIFT_HOLD_THRESHOLD  = 0.40   # Below this: HOLD  (investigate)
# Above DRIFT_HOLD_THRESHOLD: BLOCK (clear semantic violation)


@dataclass
class PrismResult:
    """The result of running data through the Prism engine."""
    decision: AIDecision
    confidence: float
    reason: str
    fix_suggestion: str | None
    drift_score: float
    drift_explanation: str
    event_id: str            # Ledger reference
    snapshot_served: bool


def inspect(
    df: pd.DataFrame,
    pipeline_name: str,
    data_asset: str,
) -> PrismResult:
    """
    Run a DataFrame through the Prism engine.

    This is the main entry point — the "clearinghouse gate".
    Every piece of data that wants to reach a consumer must pass through here.

    Args:
        df:            The data being checked (e.g., a dbt model output)
        pipeline_name: Name of the pipeline (e.g., "nightly_revenue_pipeline")
        data_asset:    Name of the data asset (e.g., "fct_monthly_revenue")

    Returns:
        PrismResult with the decision, reason, and any fix suggestions.
    """

    # ── Step 1: Compute current fingerprint ──────────────────────────
    current_fp = fingerprint_dataframe(df, asset_description=data_asset)
    current_vec = current_fp["vector"]
    current_stats = current_fp["column_stats"]

    # ── Step 2: Get baseline fingerprint ─────────────────────────────
    baseline_record = get_last_good_fingerprint(data_asset)

    # ── Step 3: Compute drift score ───────────────────────────────────
    drift_score = 0.0
    drift_explanation = "No baseline fingerprint found — this is the first run."

    if baseline_record:
        baseline_vec = json.loads(baseline_record["fingerprint_vec"])
        baseline_stats = json.loads(baseline_record.get("column_stats") or "{}")
        drift_score = compute_drift_score(
            current_fingerprint=current_fp["vector"],
            baseline_fingerprint=baseline_vec,
            current_stats=current_fp["column_stats"],
            baseline_stats=baseline_stats,
        )
        drift_explanation = explain_drift(current_stats, baseline_stats, drift_score)
    else:
        # First run — save as baseline and PASS
        _save_good_snapshot(data_asset, current_fp)
        event_id = log_ai_decision(
            pipeline_name=pipeline_name,
            data_asset=data_asset,
            ai_decision=AIDecision.PASS,
            ai_reason="First run — baseline fingerprint established.",
            ai_confidence=1.0,
            fingerprint_delta=0.0,
            rows_affected=len(df),
        )
        return PrismResult(
            decision=AIDecision.PASS,
            confidence=1.0,
            reason="First run — baseline fingerprint established.",
            fix_suggestion=None,
            drift_score=0.0, # This will be 0.0 for the first run, as there's no baseline to compare against.
            drift_explanation="Baseline created.",
            event_id=event_id,
            snapshot_served=False,
        )

    # ── Step 4: Contract rule checks ─────────────────────────────────
    contract = get_contract_for_asset(data_asset)
    contract_violations = []

    if contract and contract.get("compiled"):
        rules = contract["compiled"].get("rules", [])
        for rule in rules:
            violation = _check_rule(df, rule)
            if violation:
                contract_violations.append(violation)

    # ── Step 5: Make the decision ─────────────────────────────────────
    has_contract_violation = len(contract_violations) > 0

    if has_contract_violation or drift_score > DRIFT_HOLD_THRESHOLD:
        # ── BLOCK ──────────────────────────────────────────────────────
        decision = AIDecision.BLOCK
        confidence = min(0.95, 0.6 + drift_score * 0.5)

        if contract_violations:
            reason = f"Contract violation(s): {'; '.join(contract_violations)}. " \
                     f"Semantic drift score: {drift_score:.2f}."
        else:
            reason = f"Severe semantic drift detected (score: {drift_score:.2f}). " \
                     f"{drift_explanation[:200]}"

        fix_suggestion = generate_fix_suggestion(
            data_asset=data_asset,
            contract_plain_english=contract["plain_english"] if contract else "No contract defined.",
            violation_reason=reason,
            drift_explanation=drift_explanation,
        )
        snapshot_served = True   # Consumers will get last-known-good

    elif drift_score > DRIFT_PASS_THRESHOLD:
        # ── HOLD ──────────────────────────────────────────────────────
        decision = AIDecision.HOLD
        confidence = 0.6
        reason = (
            f"Moderate semantic drift detected (score: {drift_score:.2f}). "
            f"Flagged for human review. {drift_explanation[:150]}"
        )
        fix_suggestion = None
        snapshot_served = True   # Consumers get last-known-good while human reviews

    else:
        # ── PASS ──────────────────────────────────────────────────────
        decision = AIDecision.PASS
        confidence = 1.0 - drift_score
        reason = f"Data passed all checks. Semantic drift score: {drift_score:.2f} (within threshold)."
        fix_suggestion = None
        snapshot_served = False

        # Save this as the new good baseline
        _save_good_snapshot(data_asset, current_fp)

    # ── Step 6: Write to Ledger ───────────────────────────────────────
    event_id = log_ai_decision(
        pipeline_name=pipeline_name,
        data_asset=data_asset,
        ai_decision=decision,
        ai_reason=reason,
        ai_confidence=confidence,
        ai_fix_suggestion=fix_suggestion,
        fingerprint_delta=drift_score,
        rows_affected=len(df),
        snapshot_used=snapshot_served,
    )

    return PrismResult(
        decision=decision,
        confidence=confidence,
        reason=reason,
        fix_suggestion=fix_suggestion,
        drift_score=drift_score,
        drift_explanation=drift_explanation,
        event_id=event_id,
        snapshot_served=snapshot_served,
    )


# ─── Internal Helpers ─────────────────────────────────────────────────

def _save_good_snapshot(data_asset: str, fingerprint: dict):
    """Persist a fingerprint as a 'good' baseline snapshot."""
    save_fingerprint(
        data_asset=data_asset,
        fingerprint_vec=json.dumps(fingerprint["vector"]),
        row_count=fingerprint["meta"]["row_count"],
        column_stats=json.dumps(fingerprint["column_stats"]),
        status="good",
    )


def _check_rule(df: pd.DataFrame, rule: dict) -> str | None:
    """
    Check a single compiled contract rule against the DataFrame.
    Returns a violation message string, or None if the rule passes.
    """
    rule_type = rule.get("type", "")
    col = rule.get("column", "all")
    params = rule.get("parameters", {})

    try:
        if rule_type == "null_check":
            target_cols = df.columns if col == "all" else ([col] if col in df.columns else [])
            for c in target_cols:
                null_pct = df[c].isna().mean() * 100
                max_null = params.get("max_null_pct", 0)
                if null_pct > max_null:
                    return f"Column '{c}' has {null_pct:.1f}% nulls (limit: {max_null}%)"

        elif rule_type == "range_check":
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                min_val = params.get("min")
                max_val = params.get("max")
                if min_val is not None and df[col].min() < min_val:
                    return (f"Column '{col}' has values below minimum {min_val} "
                            f"(found min: {df[col].min():.2f})")
                if max_val is not None and df[col].max() > max_val:
                    return (f"Column '{col}' has values above maximum {max_val} "
                            f"(found max: {df[col].max():.2f})")

    except Exception as e:
        return f"Rule check failed for '{rule.get('rule_id', '?')}': {e}"

    return None
