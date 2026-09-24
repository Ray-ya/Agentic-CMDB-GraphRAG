# Agentic-CMDB-GraphRAG: Topology-Grounded SRE Agent with Multi-Modal Telemetry & Temporal Change Correlation

[![Test Status](https://img.shields.io/badge/tests-12%20pillars%20passing-brightgreen.svg)](test_suite.py)
[![Architecture](https://img.shields.io/badge/architecture-Dual--Level%20GraphRAG-blue.svg)](dual_level_graphrag.py)
[![FastAPI](https://img.shields.io/badge/FastAPI-v1.4.0-009688.svg)](server.py)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

An Enterprise SRE Root-Cause Analysis (RCA) and Autonomous Remediation Engine powered by **CMDB Directed Dependency Graphs**, **Temporal CI/CD Change Correlator**, **Multi-Modal Distributed Telemetry Ingestion**, and **Dual-Level GraphRAG** (inspired by LightRAG, HKUDS 2025).

---

## 🌟 Architectural Overview

```
                          [ Prometheus / Datadog / K8s Events / OTel Spans ]
                                                  │
                                                  ▼
                                ┌───────────────────────────────────┐
                                │ Multi-Modal Telemetry Ingestion   │ (multimodal_ingestion.py)
                                └─────────────────┬─────────────────┘
                                                  │
                                                  ▼
 ┌─────────────────────────┐            ┌───────────────────────────────────┐
 │ Enterprise CMDB Topology│            │ Temporal Change Correlator (CI/CD)│
 │ (Directed Graph Network)├───────────►│ (Half-life exponential decay)    │
 └───────────┬─────────────┘            └─────────────────┬─────────────────┘
             │                                            │
             └──────────────────────┬─────────────────────┘
                                    ▼
                     ┌─────────────────────────────┐
                     │ Dual-Level GraphRAG Engine  │
                     │  - Low-Level (CI Entities)  │
                     │  - High-Level (Topologies)  │
                     │  - Telemetry + Mutations    │
                     └──────────────┬──────────────┘
                                    ▼
                     ┌─────────────────────────────┐
                     │    Agentic RCA Resolver     │
                     │ (Graph-guided Path Search)  │
                     └──────────────┬──────────────┘
                                    │
           ┌────────────────────────┴────────────────────────┐
           ▼                                                 ▼
┌─────────────────────────────┐               ┌─────────────────────────────┐
│  Human Safety Gate Approval │               │ Closed-Loop Learning Engine │
│  (Rollbacks / Restarts)     │               │ (Auto-Postmortem to SRE KB) │
└─────────────────────────────┘               └─────────────────────────────┘
```

---

## 🚀 Key Modules & Components

1. **CMDB Topology Modeling (`cmdb_topology.py`)**:
   - Directed dependency graph modeling CIs (`APPLICATION`, `SERVICE`, `POD`, `DATABASE`, `MESSAGE_BROKER`, `HOST`).
   - Computes **Downstream Blast Radius** (impact scope & severity) and **Upstream Dependency Path Traversal** to isolate failing root-cause components.

2. **Temporal Change Event Correlator (`change_correlator.py`)**:
   - Correlates CI/CD deployments, configuration changes, schema migrations, and secret rotations occurring within a customizable temporal window (e.g. 1-hour pre-incident).
   - Utilizes exponential time-decay scoring based on half-life algorithms.

3. **Multi-Modal Telemetry Ingestion (`multimodal_ingestion.py`)**:
   - **Kubernetes Cluster Events**: Ingests `OOMKilled`, `PodEvicted`, `CrashLoopBackOff`, and `FailedScheduling` events.
   - **OpenTelemetry Distributed Spans**: Captures span latency degradation, HTTP error status codes, and trace context.
   - **Prometheus Metric Anomalies**: Maps CPU/memory thresholds directly to CI topology nodes.
   - **Audit Trails**: Integrates AWS CloudTrail / GCP Audit operational actions.

4. **Dual-Level GraphRAG Engine (`dual_level_graphrag.py`)**:
   - Synthesizes low-level entity metrics, live telemetry, and matching runbooks with high-level architectural blast radius and mutation events.

5. **Autonomous RCA Agent & Safety Gate (`rca_agent.py`)**:
   - Isolates culprit nodes, synthesizes human-auditable evidence chains, and formulates remediation actions with mandatory human approval gates for destructive commands.

6. **Closed-Loop SRE Feedback**:
   - Generates structured postmortems upon incident resolution and automatically re-indexes them into `HybridKnowledgeBase` for future retrieval.

7. **FastAPI RESTful Gateway (`server.py`)**:
   - Exposes clean REST endpoints for alert webhooks, telemetry ingestion, topology graph queries, human-in-the-loop approvals, and postmortem generation.

---

## 🧪 Comprehensive Verification & Test Suite

Run the full end-to-end test suite:

```bash
python test_suite.py
```

### Verification Pillars Covered:
- **Pillar 1**: CMDB Topology Graph & Blast Radius Calculation
- **Pillar 2**: Upstream Root-Cause Traversal from Degraded Nodes
- **Pillar 3**: Hybrid Vector + Keyword SRE Knowledge Base Retrieval
- **Pillar 4**: Temporal Change Event Correlation Engine
- **Pillar 5**: Multi-Modal Telemetry Ingestion (K8s, Prometheus, OTel)
- **Pillar 6**: Dual-Level GraphRAG Context Synthesis
- **Pillar 7**: Autonomous RCA Agent with Safe Action Formation & Human Gate
- **Pillar 8**: Closed-Loop Postmortem Synthesis & KB Re-injection
- **Pillar 9**: FastAPI Server RESTful Endpoints (`/health`, `/api/v1/telemetry/*`, `/api/v1/investigate`, etc.)
- **Pillar 10**: Multi-Stage Remediation DAG, Blast Radius Pre-flight Barrier & Contingency Rollback Orchestrator (`remediation_orchestrator.py`)
- **Pillar 11**: Alert Storm Deduplication, Noise Reduction & Topological Flapping Suppressor (`alert_storm_dedup.py`)
- **Pillar 12**: Continuous Topology Drift Detection & Shadow Dependency Reconciler (`topology_drift_reconciler.py`)
- **Pillar 13**: Automated Canary SLI Drift Evaluator & Smart Rollback Gate (`canary_evaluator.py`)
- **Pillar 14**: Dynamic Adaptive Rate-Limiting & Dependency Backpressure Governor (`backpressure_governor.py`)
