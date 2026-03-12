import pandas as pd
from core.fingerprinter import fingerprint_dataframe, compute_drift_score
from demo.physics_demo import load_real_data, apply_entropy_chaos

df_clean = load_real_data()
df_chaos = apply_entropy_chaos(df_clean)

base = fingerprint_dataframe(df_clean)
chaos = fingerprint_dataframe(df_chaos)

print(f"Base hash: {base['hash']}")
print(f"Chaos hash: {chaos['hash']}")

for col in df_clean.columns:
    if "entropy" in base["column_stats"][col] or "entropy" in chaos["column_stats"][col]:
        print(f"Col {col} Base Entropy: {base['column_stats'][col].get('entropy')}")
        print(f"Col {col} Chaos Entropy: {chaos['column_stats'][col].get('entropy')}")

# Find where vectors differ
diff_count = 0
for i, (b, c) in enumerate(zip(base["vector"], chaos["vector"])):
    if b != c:
        print(f"Vector Index {i}: Base={b}, Chaos={c}")
        diff_count += 1

print(f"Total vector indices changed: {diff_count}")

score = compute_drift_score(chaos["vector"], base["vector"])
print(f"Computed Drift Score: {score}")
