# 🔷 PRISM: Architecture & Operations Overview

**Project Name:** Prism (Semantic Data Clearinghouse)
**Core Concept:** A zero-training, deterministic semantic enforcement gate that intercepts corrupted data before it reaches autonomous AI models or downstream pipelines.
**Target Application:** Securing data flow for high-stakes modeling pipelines by enforcing semantic guardrails and providing human-in-the-loop accountability.

---

## 1. System Architecture

Prism operates as a standalone data evaluation engine that utilizes statistical mechanics rather than predictive machine learning, allowing it to function effectively without historical training data sets.

### 🧩 Core Modules
* **The Fingerprinter (`core/fingerprinter.py`)**: An engine that parses datasets to capture their semantic properties as multidimensional mathematical vectors (mean, standard deviation, percentiles, cardinality). It leverages Cosine Distance measurements to calculate "semantic drift."
* **Physics-Informed Guardrails**: Incorporates Thermodynamics (Shannon Entropy) within the fingerprinter module. This detects "diversity collapse" (e.g., if categorical data defaults to a single value due to an upstream bug) by monitoring entropy drops, even when baseline row and null counts remain stable.
* **The Contract Engine (`core/contracts.py`)**: Uses a Large Language Model (Gemini API) to parse plain-English constraints (*e.g., "Revenue must be in USD and non-negative"*) into executable JSON rules, abstractions business logic from application code.
* **The Decision Engine (`core/engine.py`)**: Evaluates incoming data against the pre-established Semantic Fingerprint Baseline and the Contract Rules. It issues routing decisions: **PASS**, **HOLD** (moderate drift), or **BLOCK** (severe drift or rule violation).
* **The Immutable Ledger (`core/ledger.py`)**: An append-only DuckDB instance. All engine decisions and AI-generated root cause analyses are permanently appended to support compliance auditing.
* **The Command Center (`ui/dashboard.py`)**: A Streamlit interface providing telemetry via Live Stats, an Audit Ledger viewer, and Queues for quarantined data.
* **The HITL Queue (`core/hitl.py`)**: A Human-in-the-Loop review mechanism enabling data stewards to manually Approve or Reject held data, creating a cryptographic signature for compliance tracking.

### 🎢 Example Workflows
* **Standard Chaos Simulation (`demo/pipeline.py`)**: Simulates a revenue data pipeline to demonstrate constraint enforcement against null injections, negative value propagation, and unit dimension errors.
* **Entropy Anomaly Simulation (`demo/physics_demo.py`)**: Analyzes a sample of NYC Taxi trip records to demonstrate the Shannon Entropy guardrails detecting a silent categorical collapse that traditional pipeline tests miss.

---

## 2. Core Value Proposition

1. **Third-Party Accountability Layer**
   Prism provides an impartial, cryptographically secure audit trail. Operating as an external infrastructure layer prevents the conflict of interest inherent in internally managed data validation pipelines.

2. **Deterministic Mathematics over ML Models**
   By utilizing deterministic statistical mechanics — such as tracking Information Entropy and multidimensional variance — Prism detects statistically impossible geometric shifts in data distribution instantaneously, bypassing the retraining periods required by predictive anomaly detection models.

3. **Continuous Baseline Adaptation**
   During fundamental, intentional business shifts (e.g., the launch of a new product segment), Prism will quarantine the anomalous data flow. Upon Human-in-the-Loop approval, the system immediately recalculates the new Semantic Vector, adopting it as the baseline for subsequent evaluations without any retraining delay.

---

## 3. Technology Stack & Deployment

* **Deployment State:** Remote repository contains MVP.
* **Dependencies:** Python 3.10+, Pandas, NumPy, DuckDB, Streamlit, google-generativeai. Codebase is designed to be lightweight with minimal infrastructure requirements.

---

## 4. Expansion Vectors

Identified areas for further system capability expansion:

1. **Statistical Enhancements:** Implementing Benford's Law distribution checks for financial transaction columns.
2. **Test Infrastructure:** Developing a standardized `pytest` suite for core modules.
3. **Cloud Accessibility:** Deploying the Streamlit interface via fully managed cloud services.
