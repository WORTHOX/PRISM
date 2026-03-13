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


from pydantic import BaseModel
from typing import List, Optional

class RuleParameter(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None
    max_growth_pct: Optional[float] = None
    allowed_values: Optional[List[str]] = None

class ContractRule(BaseModel):
    rule_id: str
    type: str
    column: str
    description: str
    parameters: RuleParameter

class ContractSchema(BaseModel):
    asset_description: str
    unit: str
    rules: List[ContractRule]
    critical_columns: List[str]
    business_context: str

# ──────────────────────────────────────────
#  CONTRACT PARSING
# ──────────────────────────────────────────

_PARSE_PROMPT = """You are a data contract compiler. Convert the following plain-English
data contract into a structured JSON object with executable rules.

Plain-English Contract:
"{plain_english}"

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
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ContractSchema,
                temperature=0.1,
            )
        )
        text = response.text.strip()
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
    """Generate a deterministic, specific fix suggestion for a violation without incurring LLM latency."""
    return (
        f"[Auto-diagnosis] Semantic drift or violation detected in '{data_asset}'.\n"
        f"Violation Details: {violation_reason}\n"
        f"Drift Context: {drift_explanation}\n\n"
        f"Recommended Action:\n"
        f"1. Query the latest corrupted batch: `SELECT * FROM {data_asset} LIMIT 100`\n"
        f"2. Review recent upstream pipeline or schema migrations affecting the column mentioned.\n"
        f"3. Compare the output to the last known-good snapshot to verify if this is an organic business shift or a software bug."
    )


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
    Returns the full contract object with a deterministic version hash.
    """
    import hashlib
    
    compiled = parse_contract_with_ai(plain_english)
    
    # Deterministic Schema Pinning: Ensure the JSON can't silently drift
    schema_str = json.dumps(compiled, sort_keys=True)
    version_hash = hashlib.sha256(schema_str.encode("utf-8")).hexdigest()[:16]
    compiled["version_hash"] = f"v_{version_hash}"
    
    compiled_json = json.dumps(compiled)

    contract_id = register_contract(
        data_asset=data_asset,
        plain_english=plain_english,
        compiled_rules=compiled_json,
        created_by=created_by,
    )

    return {
        "contract_id": contract_id,
        "version_hash": compiled["version_hash"],
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
