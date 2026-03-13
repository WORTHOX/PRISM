<div align="center">
  <h1>PRISM</h1>
  <p><b>Deterministic Semantic Data Routing for Autonomous Systems</b></p>

  <p>
    <a href="https://python.org"><img src="https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python"></a>
    <a href="https://duckdb.org"><img src="https://img.shields.io/badge/Ledger-DuckDB%20%7C%20PostgreSQL-yellow?style=flat-square&logo=postgresql"></a>
    <a href="https://streamlit.io"><img src="https://img.shields.io/badge/UI-Streamlit-red?style=flat-square&logo=streamlit"></a>
    <img src="https://img.shields.io/badge/Status-Enterprise_Beta-success?style=flat-square">
  </p>
</div>

---

## The Silent Failure Problem

Modern data pipelines rarely crash. Instead, they fail silently. 
An upstream firmware update changes a temperature reading from Fahrenheit to Celsius. A payment gateway bug defaults all failed lookups to "Credit Card." A timezone shift drops twelve hours of weekend data.

To traditional pipeline monitoring, **nothing is wrong**. The row counts match. The schemas are identical. Null values are at 0%.

But the *semantic meaning* of the data has mutated. When this drifted data is fed into an autonomous AI agent or a dynamic pricing engine, the system confidently executes disastrous, millions-of-dollars decisions based on a hallucinated reality.

**Prism is a Layer 3 Semantic Gateway.** It sits between your raw data streams and your execution models, mathematically proving that data means exactly what it meant yesterday.

---

## How It Works: The Physics of Data

Prism abandons fragile, slow-to-train ML anomaly detection models in favor of rigid **Statistical Mechanics** and **Thermodynamics**. 

When data passes through Prism, it does not scan for simple nulls. It generates a high-dimensional mathematical vector (the "Fingerprint") of the data's true shape and evaluates incoming streams using **Cosine Distance**.

If the fundamental geometry of the data warps, Prism severs the pipeline.

### Core Mathematical Constraints
* **Information Entropy (Thermodynamics):** Prism measures the Shannon Entropy of categorical columns. If a bug forces diverse transaction types to default to a single value, Prism detects the system's sudden "temperature drop" and blocks the execution, even if the data is 100% syntactically valid.
* **Benford's Law (Logarithmic Integrity):** Prism evaluates the leading-digit distribution of financial columns against the Benford curve. If synthetic data is injected, or a bug generates artificially uniform numeric data, the deviation spikes and Prism flags the injection as fraudulent/corrupted.
* **Kinematics (Velocity & Acceleration):** Prism monitors the first and second derivatives of core numerical streams. Organic shifts create smooth curves; software bugs create mathematically impossible acceleration.
* **LLM-Compiled Data Contracts:** Data stewards define limits in plain English (*"Revenue must be in USD and non-negative"*). An embedded LLM compiles this into abstract JSON constraints (using strict Pydantic structured schemas) executed at runtime.

---

## System Architecture

If data violates physical constraints, Prism routes it based on severity:

1. **PASS:** Data geometry aligns perfectly with the baseline. Routed to the AI model.
2. **HOLD:** Moderate semantic drift detected. Data is quarantined. The downstream model is served a safe historical snapshot.
3. **BLOCK:** Contract violation or catastrophic entropy/Benford collapse. Pipeline halted.

### The Immutable Review Ledger & IAM
Machine learning models struggle with sudden, legitimate market shifts (e.g., launching a Free tier). Prism solves this via **Zero-Shot Adaptation**. 

When data is placed on HOLD, it enters a secure Human-in-the-Loop review queue. Authorized data stewards authenticate via the **Identity & Access Management (IAM)** layer within the Command Center. If the steward analyzes the drift and clicks `Approve`, Prism recalculates the mathematical vector and adopts it as the new geometric baseline. **The system adapts to new realities instantly, with zero retraining.** Every decision (and the authenticated steward who made it) is logged to the backend storage.

### Storage Abstraction
Prism ships with a highly extensible `BaseLedger` interface. By default, it operates on a local, cryptographically secure **DuckDB** instance for rapid development. For enterprise deployments, passing `PRISM_LEDGER_TYPE=postgres` instantly abstracts all I/O to a concurrent **PostgreSQL** cluster.

---

## System Demonstrations

The best way to understand Prism is to watch it catch a bug that unit tests miss. 

### 1. Installation
```bash
git clone https://github.com/WORTHOX/PRISM.git
cd PRISM

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Physics Demos
**Chaos Pipeline Simulator (`demo/pipeline.py`):**
Simulates a classic daily ETL job. You can pass in chaos arguments (`unit_flip`, `null_injection`, `sign_flip`) to watch the deterministic engine block severe data drift while logging semantic hashes.
```bash
python demo/pipeline.py unit_flip
```

**Entropy Collapse (`demo/physics_demo.py`):**
This demo simulates a payment gateway bug using real NYC Taxi data. All diverse payment types (Cash, Dispute, Void) are silently defaulted to "Credit Card". Row counts remain perfect. Nulls are 0%. Watch Prism's semantic engine detect the Shannon Entropy collapse and instantly sever the pipeline.
```bash
python demo/physics_demo.py
```

**Benford's Law Validation (`test_benford.py`):**
This verifies Prism's capability to detect artificially uniform synthetic data injections within financial ledgers that bypass standard bounds checks.
```bash
python test_benford.py
```

### 3. Launch the Command Center
```bash
streamlit run ui/dashboard.py
```
Open `http://localhost:8501` to view the Live Telemetry, the IAM-secured Quarantine Queue, and the Cryptographic Audit Ledger.

---

## Enterprise Upgrades (v1 Beta)
* **API Edge Securitization**: Headless ingestion endpoints (`engine.inspect()`) are protected via the `PRISM_API_KEYS` environmental registry, enforcing machine-to-machine trust.
* **Deterministic Contract Pinning**: LLM-compiled Pydantic schemas are hashed via SHA-256 (`v_XXXXX`) upon creation. The execution engine permanently pins and logs this exact schema hash against the audit ledger to prevent silent drift.
* **Zero-Copy Big Data Ingestion**: The fingerprinter natively accepts Apache Arrow (`pyarrow.Table`), preparing the pipeline for high-throughput Parquet/Iceberg Lakehouse evaluation without memory fragmentation.
* **HITL Blast-Radius Control**: The Streamlit interface integrates dynamic 'Anti-Rubber-Stamping' mechanisms. If incoming drift exceeds 0.60, the UI forcefully challenges the steward to type `CONFIRM` before allowing the override.
* **Baseline Caching**: An in-memory TTL dictionary intercepts redundant Postgres DB lookups on high-concurrency micro-batch streams, vastly improving throughput.

---

## Tech Stack
* **Engine / Telemetry:** Python 3.10+, Pandas, NumPy, PyArrow
* **Immutable Storage:** DuckDB, PostgreSQL, psycopg2
* **Command Center UI:** Streamlit (Custom Enterprise Dark Theme)
* **Contract Compiler:** Gemini 2.0 API (Isolated to generation only; runtime execution is deterministically isolated).

---

*Prism provides the necessary mathematical infrastructure to operate autonomous systems with full confidence, ensuring the underlying data feeds are actively verified for semantic truth.*
