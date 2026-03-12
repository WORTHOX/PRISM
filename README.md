<div align="center">
  <h1>🔷 PRISM</h1>
  <p><b>Deterministic Semantic Data Routing for Autonomous Systems</b></p>

  <p>
    <a href="https://python.org"><img src="https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python"></a>
    <a href="https://duckdb.org"><img src="https://img.shields.io/badge/Ledger-DuckDB-yellow?style=flat-square&logo=duckdb"></a>
    <a href="https://streamlit.io"><img src="https://img.shields.io/badge/UI-Streamlit-red?style=flat-square&logo=streamlit"></a>
    <img src="https://img.shields.io/badge/Status-MVP-success?style=flat-square">
  </p>
</div>

---

## 🛑 The Silent Failure Problem

Modern data pipelines rarely crash. Instead, they fail silently. 
An upstream firmware update changes a temperature reading from Fahrenheit to Celsius. A payment gateway bug defaults all failed lookups to "Credit Card." A timezone shift drops twelve hours of weekend data.

To traditional pipeline monitoring, **nothing is wrong**. The row counts match. The schemas are identical. Null values are at 0%.

But the *semantic meaning* of the data has mutated. When this drifted data is fed into an autonomous AI agent or a dynamic pricing engine, the system confidently executes disastrous, millions-of-dollars decisions based on a hallucinated reality.

**Prism is a Layer 3 Semantic Gateway.** It sits between your raw data streams and your execution models, mathematically proving that data means exactly what it meant yesterday.

---

## � How It Works: The Physics of Data

Prism abandons fragile, slow-to-train ML anomaly detection models in favor of rigid **Statistical Mechanics** and **Thermodynamics**. 

When data passes through Prism, it does not scan for simple nulls. It generates a high-dimensional mathematical vector (the "Fingerprint") of the data's true shape and evaluates incoming streams using **Cosine Distance**.

If the fundamental geometry of the data warps, Prism severs the pipeline.

### Core Mathematical Constraints
* 🌡️ **Information Entropy (Thermodynamics):** Prism measures the Shannon Entropy of categorical columns. If a bug forces diverse transaction types to default to a single value, Prism detects the system's sudden "temperature drop" and blocks the execution, even if the data is 100% syntactically valid.
* 📈 **Kinematics (Velocity & Acceleration):** Prism monitors the first and second derivatives of core numerical streams. Organic shifts create smooth curves; software bugs create mathematically impossible acceleration.
* 🤖 **LLM-Compiled Data Contracts:** Data stewards define limits in plain English (*"Revenue must be in USD and non-negative"*). An embedded LLM compiles this into abstract JSON constraints executed at runtime.

---

## 🚦 System Architecture

If data violates physical constraints, Prism routes it based on severity:

1. ✅ **PASS:** Data geometry aligns perfectly with the baseline. Routed to the AI model.
2. ⚠️ **HOLD:** Moderate semantic drift detected. Data is quarantined. The downstream model is served a safe historical snapshot.
3. ❌ **BLOCK:** Contract violation or catastrophic entropy collapse. Pipeline halted.

### The Immutable Review Ledger (HITL)
Machine learning models struggle with sudden, legitimate market shifts (e.g., launching a Free tier). Prism solves this via **Zero-Shot Adaptation**. 

When data is placed on HOLD, it enters a secure Human-in-the-Loop review queue. A human analyzes the drift and clicks `APPROVE`. The exact millisecond the data is approved, Prism recalculates the mathematical vector and adopts it as the new geometric baseline. **The system adapts to new realities instantly, with zero retraining.** Every decision is logged to an append-only DuckDB ledger for compliance auditing.

---

## 🚀 The Chaos Demo

The best way to understand Prism is to watch it catch a bug that SQL tests miss. The included demo generates 10,000 trips from the real NYC Taxi dataset and injects a silent diversity collapse.

### 1. Installation
```bash
git clone https://github.com/WORTHOX/PRISM.git
cd PRISM

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Entropy Demo
This demo simulates a payment gateway bug. All diverse payment types (Cash, Dispute, Void) are silently defaulted to "Credit Card". Row counts remain perfect. Nulls are 0%.
```bash
python demo/physics_demo.py
```
*Watch Prism's semantic engine detect the Shannon Entropy collapse and instantly sever the pipeline.*

To see traditional unit/sign-flip defenses:
```bash
python demo/pipeline.py sign_flip
```

### 3. Launch the Command Center
```bash
streamlit run ui/dashboard.py
```
Open `http://localhost:8501` to view the Live Telemetry, the Quarantine Queue, and the Cryptographic Audit Ledger.

---

## 🛠 Tech Stack
* **Engine / Telemetry:** Python 3.10+, Pandas, NumPy
* **Immutable Audit Ledger:** DuckDB
* **Data Steward Interface:** Streamlit
* **Contract Compilation:** Google Gemini 2.0 Flash API

---

*Prism provides the necessary mathematical infrastructure to operate autonomous systems with full confidence, ensuring the underlying data feeds are actively verified for semantic truth.*
