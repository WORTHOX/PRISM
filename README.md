# Prism — Data Trust Layer for AI Agents

AI agents making pricing, demand, or supply-chain decisions are only as reliable as the data they consume. In production, data pipelines rarely crash — instead, they *silently succeed with the wrong information*. 

- A `revenue` column silently changes from dollars to cents.
- An upstream engineer accidentally drops a currency conversion.
- Demand signals start including cancelled orders.

The SQL tests pass. The pipeline stays green. But the **semantic meaning** has drifted. An AI agent acting on this data will confidently hallucinate a disastrous pricing maneuver.

**Prism is the enforcement layer that sits between your data pipelines and your AI agents.** 

It verifies semantic correctness in real-time, blocks corrupted data before it reaches a decision model, and logs every event with a human-reviewable, append-only audit trail.

---

## What It Does
When data crosses the boundary from producer to consumer, Prism:

1. **Reads the Data Contract:** Constraints written in plain English (e.g., *"Revenue is non-negative and in USD"*).
2. **Computes the Semantic Fingerprint:** A lightweight, deterministic statistical vector of the data's true meaning (mean, variance, null rates, percentiles).
3. **Makes a Routing Decision:**
   - ✅ **PASS:** Data matches the contract and baseline. Forwarded to the agent.
   - ⚠️ **HOLD:** Semantic drift detected. Agent receives last-known-good snapshot. Flagged for human review.
   - ❌ **BLOCK:** Contract violated. Data stopped. Auto-generated root-cause analysis provided.
4. **Appends to the Ledger:** Every decision is cryptographically logged for compliance. Human reviewers (HITL) can override decisions, leaving a permanent accountability trail.

---

## 🏗 Architecture

Prism is built lightweight and dependency-light:
- **Ledger:** DuckDB (append-only)
- **Engine:** Python
- **Contract Parser:** Gemini 2.0 API (compiles plain-English into executable constraints)
- **Command Center:** Streamlit

---

## 🚀 Quick Start (Demo "Chaos Kitchen")

Want to see Prism catch silent semantic drift? Run the demo pipeline. 
The demo generates perfectly clean revenue data, and then optionally injects 3 types of silent failure.

### 1. Setup
```bash
# Clone the repo
git clone https://github.com/WORTHOX/PRISM.git
cd PRISM

# Set up environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# (Optional) Add Gemini API key for AI-powered fix suggestions
cp .env.example .env
nano .env  # Add GEMINI_API_KEY
```

### 2. Run the Chaos Pipeline
```bash
# 1. Baseline: Run clean data (Prism learns the fingerprint)
python demo/pipeline.py

# 2. Chaos Mode A: Revenue is silently divided by 30 (monthly -> daily ARR)
python demo/pipeline.py unit_flip

# 3. Chaos Mode B: Inject 40% NULL values
python demo/pipeline.py null_injection

# 4. Chaos Mode C: Revenue flips negative
python demo/pipeline.py sign_flip
```

### 3. Launch the Command Center
```bash
streamlit run ui/dashboard.py
```
Open `localhost:8501` to view the **Audit Ledger**, manage **Data Contracts**, and clear the **Review Queue**.

---

## Designed for Output-Driven Engineering
Prism is built for environments where engineering speed matters, but accountability cannot be sacrificed. It is the system that allows teams to ship high-stakes revenue agents confidently, knowing the underlying data feed is actively verified for semantic truth.
