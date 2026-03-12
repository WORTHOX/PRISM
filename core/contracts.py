"""
Prism - Contract Engine
=========================
Handles the "data contracts" — the plain-English rules that define
what each data asset is supposed to mean.

Example contract:
  "Revenue is always in USD, non-negative, represents monthly recurring
   revenue from active paid subscribers only, and should not grow by
   more than 30% week-over-week."

The Gemini API parses this into executable rules.
"""

import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv
from core.ledger import register_contract, get_contracts

load_dotenv()

# Configure Gemini
_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
_gemini_client = genai.Client(api_key=_GEMINI_KEY) if _GEMINI_KEY else None


# ──────────────────────────────────────────
#  CONTRACT PARSING
# ──────────────────────────────────────────

_PARSE_PROMPT = """You are a data contract compiler. Convert the following plain-English
data contract into a structured JSON object with executable rules.

Plain-English Contract:
"{plain_english}"

Return ONLY a valid JSON object with this exact structure:
{{
  "asset_description": "Brief description of what this data asset is",
  "unit": "currency unit or measurement unit if mentioned",
  "rules": [
    {{
      "rule_id": "rule_1",
      "type": "range_check | null_check | growth_limit | unit_check | custom",
      "column": "column_name_if_applicable",
      "description": "plain English description of this rule",
      "parameters": {{
        "min": null_or_number,
        "max": null_or_number,
        "max_growth_pct": null_or_number,
        "allowed_values": null_or_list
      }}
    }}
  ],
  "critical_columns": ["list", "of", "must-have", "columns"],
  "business_context": "Brief business context from the contract"
}}

Be precise. Extract every constraint mentioned."""


def parse_contract_with_ai(plain_english: str) -> dict:
    """
    Use Gemini to parse a plain-English contract into structured rules.
    Falls back to a basic rule set if Gemini is unavailable.
    """
    if not _GEMINI_KEY or not _gemini_client:
        return _fallback_parse(plain_english)

    try:
        prompt = _PARSE_PROMPT.format(plain_english=plain_english)
        response = _gemini_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        text = response.text.strip()
        # Remove markdown code blocks if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()
        return json.loads(text)
    except Exception as e:
        print(f"[Contract Engine] Gemini parse failed: {e}. Using fallback.")
        return _fallback_parse(plain_english)


def _fallback_parse(plain_english: str) -> dict:
    """Basic rule extraction without AI — keyword matching."""
    rules = []
    contract_lower = plain_english.lower()

    # Detect null checks
    if "non-null" in contract_lower or "not null" in contract_lower or "required" in contract_lower:
        rules.append({
            "rule_id": "null_check_1",
            "type": "null_check",
            "column": "all",
            "description": "No null values allowed in critical columns",
            "parameters": {"max_null_pct": 0}
        })

    # Detect non-negative
    if "non-negative" in contract_lower or "positive" in contract_lower:
        rules.append({
            "rule_id": "range_check_1",
            "type": "range_check",
            "column": "revenue",
            "description": "Values must be non-negative",
            "parameters": {"min": 0, "max": None, "max_growth_pct": None, "allowed_values": None}
        })

    # Detect growth limits
    import re
    growth_match = re.search(r"(\d+)%?\s*(week|day|month)", contract_lower)
    if growth_match:
        rules.append({
            "rule_id": "growth_limit_1",
            "type": "growth_limit",
            "column": "revenue",
            "description": f"Growth should not exceed {growth_match.group(1)}% per {growth_match.group(2)}",
            "parameters": {
                "min": None, "max": None,
                "max_growth_pct": float(growth_match.group(1)),
                "allowed_values": None
            }
        })

    return {
        "asset_description": plain_english[:100] + "...",
        "unit": "USD" if "usd" in contract_lower else "unknown",
        "rules": rules,
        "critical_columns": [],
        "business_context": plain_english,
    }


# ──────────────────────────────────────────
#  FIX SUGGESTION GENERATION
# ──────────────────────────────────────────

_FIX_PROMPT = """You are a senior data engineer. A data contract violation has occurred.

Data Asset: {data_asset}
Contract: {contract_plain_english}
Violation: {violation_reason}
Drift Explanation: {drift_explanation}

Generate a specific, actionable fix suggestion. Include:
1. Most likely root cause (in 1 sentence)
2. SQL or Python code to investigate the issue (1-3 lines)
3. How to fix it (1-3 sentences)

Format as plain text. Be concise."""


def generate_fix_suggestion(
    data_asset: str,
    contract_plain_english: str,
    violation_reason: str,
    drift_explanation: str,
) -> str:
    """Use Gemini to generate a specific fix suggestion for a violation."""
    if not _GEMINI_KEY or not _gemini_client:
        return (
            f"[Auto-diagnosis] Semantic drift detected in '{data_asset}'.\n"
            f"Violation: {violation_reason}\n"
            f"Recommended action: Review recent pipeline changes, especially schema migrations "
            f"and aggregation logic changes. Compare with last known-good snapshot."
        )

    try:
        prompt = _FIX_PROMPT.format(
            data_asset=data_asset,
            contract_plain_english=contract_plain_english,
            violation_reason=violation_reason,
            drift_explanation=drift_explanation,
        )
        response = _gemini_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        return f"Fix suggestion unavailable (Gemini error: {e}). Manual investigation required."


# ──────────────────────────────────────────
#  CONTRACT CRUD
# ──────────────────────────────────────────

def create_contract(
    data_asset: str,
    plain_english: str,
    created_by: str,
) -> dict:
    """
    Create a new data contract.
    Parses the plain-English definition with Gemini and stores it.
    Returns the full contract object.
    """
    compiled = parse_contract_with_ai(plain_english)
    compiled_json = json.dumps(compiled)

    contract_id = register_contract(
        data_asset=data_asset,
        plain_english=plain_english,
        compiled_rules=compiled_json,
        created_by=created_by,
    )

    return {
        "contract_id": contract_id,
        "data_asset": data_asset,
        "plain_english": plain_english,
        "compiled": compiled,
    }


def get_contract_for_asset(data_asset: str) -> dict | None:
    """Get the compiled contract for a specific data asset."""
    contracts = get_contracts()
    for c in contracts:
        if c["data_asset"] == data_asset:
            c["compiled"] = json.loads(c.get("compiled_rules") or "{}")
            return c
    return None
