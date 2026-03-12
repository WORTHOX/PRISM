# PRISM: Architecture & Operations Overview

**Project Name:** Prism (Semantic Data Clearinghouse)
**Core Concept:** A zero-training, deterministic semantic enforcement gate that intercepts corrupted data before it reaches autonomous AI models or downstream pipelines.
**Target Application:** Securing data flow for high-stakes modeling pipelines by enforcing semantic guardrails and providing IAM-secured human-in-the-loop accountability.

---

## 1. System Architecture

Prism operates as a standalone data evaluation engine that utilizes statistical mechanics rather than predictive machine learning, allowing it to function effectively without historical training data sets.

### Core Modules
* **The Fingerprinter (`core/fingerprinter.py`)**: An engine that parses datasets to capture their semantic properties as multidimensional mathematical vectors (mean, standard deviation, percentiles, cardinality). It leverages Cosine Distance measurements to calculate "semantic drift."
* **Thermodynamics Guardrail (Shannon Entropy)**: Detects "diversity collapse" (e.g., if categorical data defaults to a single value due to an upstream bug) by monitoring entropy drops, even when baseline row and null counts remain stable.
* **Logarithmic Guardrail (Benford's Law)**: Calculates the Mean Absolute Deviation of numerical leading digits against the Benford curve to instantly detect and halt synthetic data injection or fraudulent financial scaling.
* **The Contract Engine (`core/contracts.py`)**: Uses a Large Language Model (Gemini API) combined with `Pydantic` JSON Structured Outputs to mathematically enforce plain-English constraints (*e.g., "Revenue must be in USD and non-negative"*) into executable code without hallucination risks.
* **The Decision Engine (`core/engine.py`)**: Evaluates incoming data against the pre-established Semantic Fingerprint Baseline and the Contract Rules. It issues routing decisions: **PASS**, **HOLD** (moderate drift), or **BLOCK** (severe anomaly).
* **The Immutable Ledger (`core/ledger.py`)**: Utilizes an abstracted Repository Pattern (`BaseLedger`) to simultaneously support local **DuckDB** instances and enterprise **PostgreSQL** clusters. All engine decisions are permanently appended.
* **The Command Center (`ui/dashboard.py`)**: A Streamlit interface providing telemetry via Live Stats. It utilizes an Identity & Access Management (IAM) authentication layer to secure the UI.
* **The HITL Queue (`core/hitl.py`)**: A Human-in-the-Loop review mechanism enabling authenticated data stewards to manually Approve or Reject held data, creating a cryptographic signature linked to the logged-in user.

### Example Workflows
* **Standard Chaos Simulation (`demo/pipeline.py`)**: Simulates a revenue data pipeline to demonstrate constraint enforcement against null injections, negative value propagation, and unit dimension errors.
* **Entropy Anomaly Simulation (`demo/physics_demo.py`)**: Analyzes a sample of NYC Taxi trip records to demonstrate the Shannon Entropy guardrails detecting a silent categorical collapse.
* **Logarithmic Integrity Validation (`test_benford.py`)**: Proves the mathematical interception of uniformly generic synthetic financial injections.

---

## 2. Core Value Proposition

1. **Third-Party Accountability Layer**
   Prism provides an impartial, cryptographically secure audit trail. By running on a highly-available Postgres database, it prevents the conflict of interest inherent in internally managed data validation pipelines.

2. **Deterministic Mathematics over ML Models**
   By utilizing deterministic statistical mechanics — such as tracking Information Entropy and Benford's Law deviations — Prism detects corrupted data instantaneously, bypassing the retraining periods and drift vulnerabilities inherent to predictive anomaly detection ML models.

3. **Continuous Baseline Adaptation**
   During fundamental, intentional business shifts (e.g., the launch of a new product segment), Prism will quarantine the anomalous data flow. Upon Human-in-the-Loop approval via the authenticated Command Center, the system immediately recalculates the new Semantic Vector, adopting it as the baseline for subsequent evaluations without any retraining delay.

---

## 3. Technology Stack & Deployment

* **Deployment State:** Enterprise Beta (Abstracted scaling architecture complete).
* **Dependencies:** Python 3.10+, Pandas, NumPy, DuckDB, PostgreSQL, Pydantic, Streamlit, google-generativeai.
* **Design Philosophy:** Codebase is designed to be lightweight, un-opinionated regarding the host environment, and mathematically rigorous.

---

## 4. Operational Checklist / Completed Upgrades

* `[x]` **Storage Abstraction (P0)**: Refactored DuckDB into an abstract Interface supporting concurrent Postgres clustering.
* `[x]` **HITL Authentication (P0)**: Secured the Command Center via IAM, linking all Ledger overrides to specific human identities.
* `[x]` **Benford's Law Integration (P1)**: Implemented logarithmic physical constraints to halt synthetic/fraudulent numerical injections.
* `[x]` **LLM Structured Validation (P1)**: Deprecated raw string prompting in favor of strict Pydantic JSON schema generation via the Gemini API.
* `[x]` **UI Standardization**: Extracted casual conversational elements and emojis from the Command Center, replacing them with a formal Dark Mode CSS enterprise design language.
