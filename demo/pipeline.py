"""
Prism - Demo "Chaos Kitchen" Pipeline
======================================
This simulates a real company's data pipeline with Prism plugged in
as the enforcement checkpoint.

The pipeline:
  1. Generates "clean" revenue data (the baseline)
  2. Optionally injects one of 3 "chaos" breaks
  3. Runs the data through Prism
  4. Shows what Prism caught

Think of it as the school lunch canteen:
  - Normal run: cook makes the right dal → inspector: PASS
  - Chaos run: cook accidentally ruins it → inspector: BLOCK

Chaos modes:
  A) "unit_flip"      — Revenue silently changed from monthly to daily (÷30)
  B) "null_injection" — 40% of revenue values go NULL
  C) "sign_flip"      — Revenue becomes negative (returns masquerading as revenue)
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from core.engine import inspect
from core.contracts import create_contract
from core.ledger import AIDecision

console = Console()


# ─── Data Generator ──────────────────────────────────────────────────

def generate_revenue_data(n_rows: int = 500, seed: int = 42) -> pd.DataFrame:
    """Generate clean monthly revenue data (the 'good kitchen')."""
    np.random.seed(seed)
    random.seed(seed)

    customer_ids = [f"CUST_{i:04d}" for i in range(1, n_rows + 1)]
    base_date = datetime(2025, 1, 1)

    return pd.DataFrame({
        "customer_id": customer_ids,
        "month": [(base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m")
                  for _ in range(n_rows)],
        "monthly_revenue_usd": np.random.lognormal(mean=7.5, sigma=0.8, size=n_rows).round(2),
        "subscription_plan": np.random.choice(["basic", "pro", "enterprise"], n_rows, p=[0.5, 0.35, 0.15]),
        "is_active": np.random.choice([True, False], n_rows, p=[0.95, 0.05]),
    })


def apply_chaos(df: pd.DataFrame, chaos_mode: str) -> pd.DataFrame:
    """Apply a silent data corruption to the clean DataFrame."""
    df = df.copy()

    if chaos_mode == "unit_flip":
        # Revenue changed from monthly to daily — looks fine, semantics broken
        df["monthly_revenue_usd"] = (df["monthly_revenue_usd"] / 30).round(2)
        description = "💉 CHAOS: Revenue divided by 30 (monthly → daily). Values look valid. Meaning is wrong."

    elif chaos_mode == "null_injection":
        # 40% of revenue goes NULL
        mask = np.random.random(len(df)) < 0.40
        df.loc[mask, "monthly_revenue_usd"] = np.nan
        description = "💉 CHAOS: 40% of revenue values set to NULL."

    elif chaos_mode == "sign_flip":
        # Revenue becomes negative (returns mixed in)
        df["monthly_revenue_usd"] = df["monthly_revenue_usd"] * -1
        description = "💉 CHAOS: Revenue values flipped negative."

    else:
        description = "No chaos applied."

    console.print(f"\n[bold red]{description}[/bold red]")
    return df


# ─── Contract Setup ────────────────────────────────────────────────────

def setup_contract(created_by: str = "demo@prism.dev"):
    """Register the revenue data contract."""
    return create_contract(
        data_asset="fct_monthly_revenue",
        plain_english=(
            "Monthly revenue is always in USD. "
            "Values must be non-negative and non-null. "
            "Represents monthly recurring revenue from active subscribers only. "
            "Revenue per customer should be between 50 and 100,000 USD per month."
        ),
        created_by=created_by,
    )


# ─── Main Demo Runner ─────────────────────────────────────────────────

def run_demo(chaos_mode: str = None):
    """
    Run the full Prism demo.
    chaos_mode: None | 'unit_flip' | 'null_injection' | 'sign_flip'
    """
    console.print(Panel.fit(
        "[bold cyan]🔷 PRISM — Semantic Data Clearinghouse[/bold cyan]\n"
        "The food inspector between your kitchen and your students.",
        border_style="cyan"
    ))

    # Step 1: Set up contract
    console.print("\n[bold]Step 1: Registering data contract...[/bold]")
    contract = setup_contract()
    console.print(f"[green]✅ Contract registered for 'fct_monthly_revenue'[/green]")
    console.print(f"   [dim]{contract['plain_english']}[/dim]")

    # Step 2: Generate baseline (first run → always PASS)
    console.print("\n[bold]Step 2: Running clean baseline data through Prism...[/bold]")
    clean_df = generate_revenue_data(seed=42)
    result = inspect(clean_df, pipeline_name="nightly_revenue", data_asset="fct_monthly_revenue", api_key="demo-key")
    _print_result(result, "Baseline Run")

    # Step 3: Run with chaos if specified
    if chaos_mode:
        console.print(f"\n[bold]Step 3: Injecting chaos ({chaos_mode}) and running again...[/bold]")
        chaos_df = generate_revenue_data(seed=99)   # Slightly different seed
        chaos_df = apply_chaos(chaos_df, chaos_mode)
        result = inspect(chaos_df, pipeline_name="nightly_revenue", data_asset="fct_monthly_revenue", api_key="demo-key")
        _print_result(result, f"Chaos Run ({chaos_mode})")

        if result.decision != AIDecision.PASS:
            console.print(Panel(
                f"[bold yellow]📋 AI ANALYSIS[/bold yellow]\n\n"
                f"{result.drift_explanation}\n\n"
                f"[bold]🔧 Suggested Fix:[/bold]\n{result.fix_suggestion or 'Manual investigation required.'}",
                border_style="yellow",
                title="Why Prism Blocked This"
            ))

    console.print("\n[bold green]✅ Demo complete. Audit trail written to data/prism_ledger.duckdb[/bold green]")
    console.print("[dim]Run `streamlit run ui/dashboard.py` to see the Command Center.[/dim]\n")


def _print_result(result, label: str):
    """Pretty-print a PrismResult to the terminal."""
    colors = {AIDecision.PASS: "green", AIDecision.HOLD: "yellow", AIDecision.BLOCK: "red"}
    icons  = {AIDecision.PASS: "✅", AIDecision.HOLD: "⚠️", AIDecision.BLOCK: "❌"}
    color  = colors.get(result.decision, "white")
    icon   = icons.get(result.decision, "?")

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    table.add_column("Field", style="dim", width=18)
    table.add_column("Value")

    table.add_row("Decision", f"[bold {color}]{icon} {result.decision.value}[/bold {color}]")
    table.add_row("Confidence", f"{result.confidence * 100:.0f}%")
    table.add_row("Drift Score", f"{result.drift_score:.3f} (threshold: 0.40)")
    table.add_row("Snapshot Served", "Yes" if result.snapshot_served else "No")
    table.add_row("Reason", result.reason[:100] + "..." if len(result.reason) > 100 else result.reason)
    table.add_row("Event ID", f"[dim]{result.event_id}[/dim]")

    console.print(Panel(table, title=f"[bold]{label}[/bold]", border_style=color))


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    if mode not in [None, "unit_flip", "null_injection", "sign_flip"]:
        console.print("[red]Usage: python demo/pipeline.py [unit_flip|null_injection|sign_flip][/red]")
        sys.exit(1)
    run_demo(chaos_mode=mode)
