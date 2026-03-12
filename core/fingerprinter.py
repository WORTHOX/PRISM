"""
Prism - Semantic Fingerprinter
================================
Converts a DataFrame into a "semantic fingerprint" — a vector that
captures the MEANING of the data, not just its bytes.

Analogy: Like a fingerprint for a person — even if you change your
clothes (column names), your fingerprint (semantic meaning) stays the same.
When the fingerprint changes unexpectedly, Prism raises an alert.
"""

import json
import hashlib
import numpy as np
import pandas as pd
from pathlib import Path


def _stats_for_column(series: pd.Series) -> dict:
    """Compute statistical summary for a single column."""
    stats = {
        "name": series.name,
        "dtype": str(series.dtype),
        "null_pct": round(series.isna().mean() * 100, 2),
        "unique_pct": round(series.nunique() / max(len(series), 1) * 100, 2),
    }

    if pd.api.types.is_bool_dtype(series):
        # Cast booleans to int so numeric ops work cleanly on numpy 2.x
        series = series.astype("int64")

    if pd.api.types.is_numeric_dtype(series):
        non_null = series.dropna().astype(float)
        if len(non_null) > 0:
            stats.update({
                "mean": round(float(non_null.mean()), 4),
                "std": round(float(non_null.std()), 4),
                "min": round(float(non_null.min()), 4),
                "max": round(float(non_null.max()), 4),
                "negative_pct": round((non_null < 0).mean() * 100, 2),
                # Key signal: what fraction of values are in various ranges
                "p25": round(float(non_null.quantile(0.25)), 4),
                "p50": round(float(non_null.quantile(0.50)), 4),
                "p75": round(float(non_null.quantile(0.75)), 4),
            })
    elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        non_null = series.dropna().astype(str)
        if len(non_null) > 0:
            stats.update({
                "avg_length": round(non_null.str.len().mean(), 2),
                "sample_values": non_null.head(5).tolist(),
            })

    return stats


def compute_column_stats(df: pd.DataFrame) -> dict:
    """
    Compute per-column statistics for a DataFrame.
    Returns a dict of {column_name: stats_dict}.
    """
    return {col: _stats_for_column(df[col]) for col in df.columns}


def fingerprint_dataframe(df: pd.DataFrame, asset_description: str = "") -> dict:
    """
    Generate a semantic fingerprint for a DataFrame.

    The fingerprint is a deterministic numeric vector that captures:
    1. Column-level statistical distributions
    2. Row count and column count
    3. Basic structural properties

    Instead of heavyweight ML embeddings (which require GPU),
    we use a carefully designed numeric feature vector that is:
    - Fast to compute (runs in milliseconds on a laptop)
    - Sensitive to semantic changes (catches unit changes, drift, etc.)
    - Comparable across runs

    Returns:
        {
            "vector": [...],         # The fingerprint vector as a list of floats
            "hash": "abc123...",     # SHA256 hash of the vector (for quick comparison)
            "column_stats": {...},   # Full per-column statistics
            "meta": {...}            # Metadata about the fingerprint
        }
    """
    col_stats = compute_column_stats(df)
    vector_parts = []

    # Global features
    vector_parts.extend([
        float(len(df)),          # Row count
        float(len(df.columns)),  # Column count
        float(df.isna().mean().mean()),  # Overall null rate
    ])

    # Per-column features (sorted by column name for stability)
    for col in sorted(df.columns):
        stats = col_stats[col]
        is_numeric = pd.api.types.is_numeric_dtype(df[col])

        if is_numeric:
            vector_parts.extend([
                stats.get("mean", 0.0) or 0.0,
                stats.get("std", 0.0) or 0.0,
                stats.get("min", 0.0) or 0.0,
                stats.get("max", 0.0) or 0.0,
                stats.get("null_pct", 0.0) or 0.0,
                stats.get("unique_pct", 0.0) or 0.0,
                stats.get("negative_pct", 0.0) or 0.0,
                stats.get("p50", 0.0) or 0.0,
            ])
        else:
            vector_parts.extend([
                0.0, 0.0, 0.0, 0.0,
                stats.get("null_pct", 0.0) or 0.0,
                stats.get("unique_pct", 0.0) or 0.0,
                0.0,
                float(stats.get("avg_length", 0.0) or 0.0),
            ])

    # Normalize the vector
    vec = np.array(vector_parts, dtype=np.float64)
    # Replace inf/nan with 0
    vec = np.nan_to_num(vec, nan=0.0, posinf=0.0, neginf=0.0)

    vec_list = vec.tolist()
    vec_str = json.dumps(vec_list)
    fingerprint_hash = hashlib.sha256(vec_str.encode()).hexdigest()[:16]

    return {
        "vector": vec_list,
        "hash": fingerprint_hash,
        "column_stats": col_stats,
        "meta": {
            "row_count": len(df),
            "col_count": len(df.columns),
            "columns": sorted(df.columns.tolist()),
            "asset_description": asset_description,
        }
    }


def compute_drift_score(
    current_fingerprint: list[float],
    baseline_fingerprint: list[float],
) -> float:
    """
    Compute semantic drift between two fingerprints.

    Returns a score from 0.0 to 1.0:
    - 0.0 = identical (no drift at all)
    - 0.1 to 0.3 = minor drift (normal data variation)
    - 0.3 to 0.6 = moderate drift (investigate)
    - 0.6+ = severe drift (likely semantic change)
    """
    if not current_fingerprint or not baseline_fingerprint:
        return 0.0

    current = np.array(current_fingerprint, dtype=np.float64)
    baseline = np.array(baseline_fingerprint, dtype=np.float64)

    # Pad shorter vector with zeros
    max_len = max(len(current), len(baseline))
    current = np.pad(current, (0, max_len - len(current)))
    baseline = np.pad(baseline, (0, max_len - len(baseline)))

    # Replace inf/nan
    current = np.nan_to_num(current)
    baseline = np.nan_to_num(baseline)

    # Cosine distance (0 = identical direction, 1 = completely different)
    norm_curr = np.linalg.norm(current)
    norm_base = np.linalg.norm(baseline)

    if norm_curr == 0 or norm_base == 0:
        # If one vector is all zeros, use L2 norm ratio instead
        return float(np.linalg.norm(current - baseline) / max(norm_curr, norm_base, 1.0))

    cosine_similarity = np.dot(current, baseline) / (norm_curr * norm_base)
    cosine_distance = 1.0 - float(np.clip(cosine_similarity, -1.0, 1.0))

    return round(cosine_distance, 4)


def explain_drift(
    current_stats: dict,
    baseline_stats: dict,
    drift_score: float,
) -> str:
    """
    Generate a plain-English explanation of what changed between
    the current fingerprint and the baseline.
    """
    if drift_score < 0.05:
        return "No significant semantic drift detected."

    explanations = []

    all_cols = set(current_stats.keys()) | set(baseline_stats.keys())

    for col in sorted(all_cols):
        curr = current_stats.get(col, {})
        base = baseline_stats.get(col, {})

        if col not in current_stats:
            explanations.append(f"⚠️  Column '{col}' was present before but is now MISSING.")
            continue
        if col not in baseline_stats:
            explanations.append(f"⚠️  Column '{col}' is NEW (was not in baseline).")
            continue

        # Check for numeric drift
        for stat in ["mean", "std", "max", "min"]:
            curr_val = curr.get(stat)
            base_val = base.get(stat)
            if curr_val is not None and base_val is not None and base_val != 0:
                pct_change = abs((curr_val - base_val) / base_val) * 100
                if pct_change > 50:
                    explanations.append(
                        f"🔴 Column '{col}' {stat} changed by {pct_change:.0f}% "
                        f"(was {base_val:.2f}, now {curr_val:.2f}). "
                        f"Possible unit change or aggregation change."
                    )
                elif pct_change > 20:
                    explanations.append(
                        f"🟡 Column '{col}' {stat} changed by {pct_change:.0f}% "
                        f"(was {base_val:.2f}, now {curr_val:.2f})."
                    )

        # Check null rate change
        curr_null = curr.get("null_pct", 0)
        base_null = base.get("null_pct", 0)
        if abs(curr_null - base_null) > 10:
            explanations.append(
                f"⚠️  Column '{col}' null rate changed from {base_null:.1f}% to {curr_null:.1f}%."
            )

    if not explanations:
        return (
            f"Overall semantic drift score is {drift_score:.2f}. "
            "Column-level changes are subtle — manual review recommended."
        )

    header = f"Semantic drift score: {drift_score:.2f}. Changes detected:\n"
    return header + "\n".join(explanations)
