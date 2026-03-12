import pandas as pd
import numpy as np
from core.fingerprinter import fingerprint_dataframe, compute_drift_score, explain_drift

def run_benford_test():
    print("--- BENFORD'S LAW GUARDRAIL TEST ---")
    
    # 1. Create a baseline dataset with natural numbers (follows Benford)
    # Easiest way to simulate Benford naturally is exponential growth or taking 10^uniform
    np.random.seed(42)
    natural_prices = 10 ** np.random.uniform(1, 5, 1000)
    df_base = pd.DataFrame({"transaction_amount": natural_prices})
    
    print("Extracting baseline fingerprint...")
    fp_base = fingerprint_dataframe(df_base, "Financial Transactions")
    base_dev = fp_base["column_stats"]["transaction_amount"]["benford_dev"]
    print(f"Baseline Benford Deviation: {base_dev:.4f} (Should be very close to 0.0)")
    
    # 2. Inject synthetic data (Uniformly distributed, violates Benford)
    synthetic_prices = np.random.uniform(10, 99999, 1000)
    df_chaos = pd.DataFrame({"transaction_amount": synthetic_prices})
    
    print("\nExtracting chaos fingerprint...")
    fp_chaos = fingerprint_dataframe(df_chaos, "Financial Transactions")
    chaos_dev = fp_chaos["column_stats"]["transaction_amount"]["benford_dev"]
    print(f"Chaos Benford Deviation: {chaos_dev:.4f} (Should be high, e.g. > 0.15)")
    
    # 3. Compute drift
    score = compute_drift_score(
        current_fingerprint=fp_chaos["vector"],
        baseline_fingerprint=fp_base["vector"],
        current_stats=fp_chaos["column_stats"],
        baseline_stats=fp_base["column_stats"]
    )
    
    print(f"\nFinal Drift Score: {score:.4f} (Should be > 0.45 due to penalty)")
    
    # 4. Explain drift
    explanation = explain_drift(fp_chaos["column_stats"], fp_base["column_stats"], score)
    print("\nDrift Explanation:")
    print(explanation)
    
    if score > 0.40 and "Benford's Law" in explanation:
        print("\n✅ TEST PASSED: Benford's guardrail successfully intercepted synthetic data.")
    else:
        print("\n❌ TEST FAILED.")

if __name__ == "__main__":
    run_benford_test()
