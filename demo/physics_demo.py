"""
Prism - Physics Guardrail Demo (Shannon Entropy)
==================================================
This demo uses real-world data (NYC Taxi dataset) to show how 
applying Thermodynamics (Shannon Entropy) catches silent failures 
that standard SQL tests completely miss.

The Scenario:
1. Baseline: We read 10,000 real taxi trips. The `payment_type` column 
   has natural diversity (Credit Card, Cash, Dispute, etc.). The 
   Shannon Entropy is healthy (around 1.5).
2. Chaos: A bug in the payment gateway silently defaults all failed 
   or missing lookups to "Credit Card". 
   - Row count is perfect.
   - Null count is 0%.
   - Data types are correct.
   - Traditional tests PASS.
3. Prism: Catches the massive drop in Shannon Entropy. The "temperature"
   of the system flatlined. Prism blocks the data.
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
import numpy as np
from rich.console import Console
from rich.panel import Panel

from core.engine import inspect
from core.contracts import create_contract
from demo.pipeline import _print_result

console = Console()

def load_real_data(n_rows=10000):
    """Load a chunk of the real NYC Taxi dataset."""
    try:
        df = pd.read_parquet("data/ny_taxi/yellow_tripdata_2024-01.parquet")
        # Keep it small for the demo and select interesting columns
        df = df.head(n_rows)[['VendorID', 'tpep_pickup_datetime', 'passenger_count', 
                              'trip_distance', 'payment_type', 'total_amount']]
        
        # Map payment type to strings to make it categorical
        payment_map = {1.0: "Credit Card", 2.0: "Cash", 3.0: "No Charge", 
                       4.0: "Dispute", 5.0: "Unknown", 6.0: "Voided"}
        df['payment_type'] = df['payment_type'].map(payment_map).fillna("Unknown")
        
        return df
    except Exception as e:
        console.print(f"[red]Error loading data: {e}[/red]")
        console.print("Please ensure you ran the curl command to download the parquet file.")
        sys.exit(1)

def apply_entropy_chaos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate a silent 'default value' bug.
    Every non-credit-card payment silently defaults to 'Credit Card'.
    No nulls are created. The data looks perfectly clean to standard tools.
    """
    df = df.copy()
    # The Bug: The gateway accidentally defaults everything to Credit Card
    df['payment_type'] = "Credit Card"
    
    console.print(Panel(
        "[bold red]💉 CHAOS INJECTED: Payment Gateway Bug[/bold red]\n"
        "All diverse payment types silently defaulted to 'Credit Card'.\n"
        "Total rows: Unchanged. Nulls: 0. SQL tests will definitely pass.",
        border_style="red"
    ))
    return df

def run_entropy_demo():
    console.print(Panel.fit(
        "[bold cyan]🔷 PRISM — Physics Guardrail Demo (Entropy)[/bold cyan]\n"
        "Testing advanced Thermodynamics metrics on real NYC Taxi data.",
        border_style="cyan"
    ))

    # 1. Contract Setup
    console.print("\n[bold]Step 1: Setting basic contract...[/bold]")
    contract = create_contract(
        data_asset="nyc_taxi_trips",
        plain_english="Payment type must be non-null. Total amount must be positive.",
        created_by="demo@prism.dev"
    )
    console.print("[green]✅ Contract registered.[/green]")

    # 2. Baseline
    console.print("\n[bold]Step 2: Running real NYC Taxi data (Baseline)...[/bold]")
    df_clean = load_real_data()
    # Print the natural distribution
    dist = df_clean['payment_type'].value_counts(normalize=True) * 100
    console.print("Natural Payment Distribution:")
    for k, v in dist.items():
        console.print(f"  - {k}: {v:.1f}%")
        
    result_clean = inspect(df_clean, "taxi_ingest", "nyc_taxi_trips")
    _print_result(result_clean, "Baseline Run")

    # 3. Chaos (Entropy Collapse)
    console.print("\n[bold]Step 3: Injecting silent failure (Entropy Collapse)...[/bold]")
    df_chaos = apply_entropy_chaos(df_clean)
    result_chaos = inspect(df_chaos, "taxi_ingest", "nyc_taxi_trips")
    _print_result(result_chaos, "Chaos Run (Diversity Loss)")

    if result_chaos.decision != "PASS":
        console.print(Panel(
            f"[bold yellow]📋 PRISM DRIFT ANALYSIS[/bold yellow]\n\n"
            f"{result_chaos.drift_explanation}",
            border_style="yellow",
            title="How Prism Caught It"
        ))

if __name__ == "__main__":
    run_entropy_demo()
