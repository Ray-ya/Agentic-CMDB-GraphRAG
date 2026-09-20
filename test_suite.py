"""
End-to-End Automated Test Suite for Agentic CMDB GraphRAG (test_suite.py)
Validates all core pillars:
1. CMDB Graph Topology Modeling & Dependency Navigation
2. Blast Radius Calculation (Downstream Impact)
3. Upstream Root Cause Candidate Traversal
4. Hybrid Runbook & Postmortem Retrieval
5. Temporal Change Correlation (CI/CD Deployments, Migrations, Config)
6. Multi-Modal Distributed Telemetry Ingestion (K8s, Prometheus, OpenTelemetry, CloudTrail)
7. Dual-Level GraphRAG Synthesis (Low-level CI + High-level Topology & Changes & Telemetry)
8. Autonomous RCA Agent Reasoning with Human-Gate Approval
9. Closed-Loop Self-Learning Postmortem Synthesis & KB Re-injection
10. FastAPI RESTful Endpoints (Health, Graph, Blast-Radius, Changes, Telemetry K8s/OTel, Investigate, Approve, Postmortem)
"""

import sys
import os
import time

sys.stdout.reconfigure(encoding='utf-8')

from cmdb_topology import build_sample_enterprise_topology, CINode, CMDBTopologyGraph
from knowledge_base import build_sample_sre_knowledge_base, KnowledgeDocument
from change_correlator import TemporalChangeCorrelator, ChangeEvent
from multimodal_ingestion import MultiModalTelemetryIngestionEngine
from dual_level_graphrag import DualLevelGraphRAGEngine
from rca_agent import AgenticRCAResolver
from server import app
from fastapi.testclient import TestClient


def run_tests():
    print("=" * 70)
    print(">> [Antigravity] Starting Agentic-CMDB-GraphRAG Comprehensive Test Suite v1.2.0")
    print("=" * 70)

    client = TestClient(app)

    # ----------------------------------------------------
    # TEST 1: Topology Construction & Blast Radius
    # ----------------------------------------------------
    print("\n[TEST 1] Testing CMDB Topology Graph & Blast Radius Calculation...")
    topo = build_sample_enterprise_topology()
    assert len(topo.nodes) == 11, f"Expected 11 nodes, got {len(topo.nodes)}"
    
    # Blast radius of database failure
    db_blast = topo.compute_blast_radius("db-postgres-pay", max_depth=3)
    impacted_ids = [c["id"] for c in db_blast["impacted_components"]]
    assert "pod-pay-01" in impacted_ids or "svc-payment" in impacted_ids
    print(f"   Origin: {db_blast['origin_node']}, Impact count: {db_blast['impact_count']}")
    print(f"   Severity: {db_blast['severity_assessment']}")
    print("   [PASS] Topology Graph & Blast Radius logic verified.")

    # ----------------------------------------------------
    # TEST 2: Upstream Root-Cause Candidate Traversal
    # ----------------------------------------------------
    print("\n[TEST 2] Testing Upstream Root-Cause Traversal from degraded order-api-service...")
    candidates = topo.trace_upstream_root_cause_candidates("svc-order", max_depth=3)
    candidate_ids = [c["id"] for c in candidates]
    assert "svc-payment" in candidate_ids, "Should detect svc-payment as upstream dependency"
    assert "db-postgres-pay" in candidate_ids, "Should detect db-postgres-pay as underlying dependency"
    print(f"   Found {len(candidates)} upstream candidates in path from svc-order")
    print(f"   Top culprit isolated: {candidates[0]['name']} (Status: {candidates[0]['status']})")
    print("   [PASS] Upstream root cause candidate traversal verified.")

    # ----------------------------------------------------
    # TEST 3: Hybrid Knowledge Retrieval
    # ----------------------------------------------------
    print("\n[TEST 3] Testing Hybrid Vector+Keyword SRE Knowledge Base Retrieval...")
    kb = build_sample_sre_knowledge_base()
    hits = kb.search("postgres connection pool exhaustion timeout", target_component="db-postgres-pay", top_k=2)
    assert len(hits) > 0, "Expected at least 1 matched document"
    top_doc = hits[0]
    assert top_doc["doc_id"] == "RB-PG-001", f"Expected RB-PG-001, got {top_doc['doc_id']}"
    print(f"   Top Match: [{top_doc['doc_id']}] {top_doc['title']} (Score: {top_doc['score']})")
    print("   [PASS] SRE Hybrid retrieval correctly grounded.")

    # ----------------------------------------------------
    # TEST 4: Temporal Change Event Correlation
    # ----------------------------------------------------
    print("\n[TEST 4] Testing Temporal Change Event Correlation Engine...")
    correlator = TemporalChangeCorrelator(time_window_seconds=3600.0, half_life_seconds=900.0)
    now_ts = time.time()
    
    # Simulate a recent config update on the database 10 minutes ago
    correlator.record_change(ChangeEvent(
        change_id="CHG-2026-001",
        node_id="db-postgres-pay",
        change_type="CONFIG_UPDATE",
        timestamp=now_ts - 600.0,  # 10 mins ago
        author="dba-alex",
        description="Updated max_connections to 500 and enabled aggressive idle connection logging."
    ))
    
    # Simulate a deployment on payment pod 8 minutes ago
    correlator.record_change(ChangeEvent(
        change_id="CHG-2026-002",
        node_id="pod-pay-01",
        change_type="DEPLOYMENT",
        timestamp=now_ts - 480.0,  # 8 mins ago
        author="ci-bot",
        description="Deployed release v2.4.1-rc3 with payment gateway retry logic."
    ))

    # Correlate for alert on svc-payment
    correlated = correlator.correlate_incident(
        alert_timestamp=now_ts,
        upstream_candidates=candidates,
        symptom_node_id="svc-payment"
    )
    assert len(correlated) >= 1, "Should correlate changes to upstream dependencies"
    print(f"   Correlated {len(correlated)} mutation events within 1-hour window.")
    for c in correlated:
        print(f"   -> Match: {c.change.change_type} on {c.change.node_id} (Score: {c.correlation_score}, {c.minutes_before_alert}m ago)")
    print("   [PASS] Temporal Change Correlation successfully grounded.")

    # ----------------------------------------------------
    # TEST 5: Multi-Modal Distributed Telemetry Ingestion
    # ----------------------------------------------------
    print("\n[TEST 5] Testing Multi-Modal Telemetry Ingestion (K8s, Prometheus, OTel)...")
    telemetry_eng = MultiModalTelemetryIngestionEngine()
    
    # Ingest K8s OOMKilled
    k8s_ev = telemetry_eng.ingest_k8s_event({
        "reason": "OOMKilled",
        "involvedObject": {"name": "pod-pay-01", "namespace": "prod"},
        "message": "Container payment-app was terminated due to memory limit reached."
    })
    assert k8s_ev.severity == "CRITICAL"
    assert k8s_ev.target_node_id == "pod-pay-01"

    # Ingest OpenTelemetry span with error
    otel_ev = telemetry_eng.ingest_opentelemetry_span({
        "service_name": "svc-payment",
        "span_id": "span-9988",
        "trace_id": "trace-1122",
        "status_code": "ERROR",
        "duration_ms": 5230.5,
        "http_status_code": 504,
        "error_message": "Gateway Timeout contacting database"
    })
    assert otel_ev.severity == "CRITICAL"
    assert otel_ev.target_node_id == "svc-payment"

    # Verify query for CI telemetry
    pod_telemetry = telemetry_eng.get_recent_telemetry_for_ci("pod-pay-01")
    assert len(pod_telemetry) >= 1
    assert pod_telemetry[0]["event_name"] == "K8s_OOMKilled"
    print(f"   Ingested {len(telemetry_eng.event_store)} multi-modal events across K8s and OTel.")
    print("   [PASS] Multi-Modal Telemetry Ingestion verified.")

    # ----------------------------------------------------
    # TEST 6: Dual-Level GraphRAG Synthesis with Temporal Data & Telemetry
    # ----------------------------------------------------
    print("\n[TEST 6] Testing Dual-Level GraphRAG (Low-level CI + Telemetry + High-level Topology + Changes)...")
    engine = DualLevelGraphRAGEngine(topo, kb, correlator, telemetry_eng)
    alert_event = {
        "alert_name": "HTTP504_Payment_Timeout",
        "severity": "CRITICAL",
        "node_id": "svc-payment",
        "timestamp": now_ts,
        "details": {"error_rate": "84.2%", "p99_latency": "14.2s"}
    }
    context = engine.synthesize_incident_context(alert_event)
    assert "low_level_inspection" in context["dual_level_graphrag"]
    assert "high_level_topology" in context["dual_level_graphrag"]
    assert len(context["dual_level_graphrag"]["high_level_topology"]["correlated_changes"]) >= 1
    assert "recent_telemetry" in context["dual_level_graphrag"]["low_level_inspection"]
    print("   [PASS] Dual-Level GraphRAG successfully synthesized entity, topological, temporal & telemetry context.")

    # ----------------------------------------------------
    # TEST 7: Autonomous RCA Agent & Rollback Remediation
    # ----------------------------------------------------
    print("\n[TEST 7] Testing Autonomous RCA Agent with Change-Aware Rollback Actions...")
    resolver = AgenticRCAResolver(engine)
    report = resolver.investigate(alert_event)
    
    assert report.root_cause_node == "rds-postgres-payment-primary", f"Root cause node should be DB, got {report.root_cause_node}"
    assert report.confidence_score >= 0.90, f"Expected high confidence, got {report.confidence_score}"
    assert report.correlated_change_count >= 1, "Should reflect correlated changes"
    print(f"   Incident ID: {report.incident_id}")
    print(f"   Root Cause: {report.root_cause_node}")
    print(f"   Confidence Score: {report.confidence_score * 100:.1f}%")
    print(f"   Actions Generated: {len(report.recommended_actions)}")
    for act in report.recommended_actions:
        print(f"   -> [{act.action_type}] on {act.target_ci_name} (Destructive: {act.is_destructive}): {act.command[:60]}...")

    # Verify Human Gate: Actions must be in PENDING_APPROVAL
    first_action = report.recommended_actions[0]
    assert first_action.status == "PENDING_APPROVAL", "Destructive actions must await human approval"
    
    # Approve Action
    appr_res = resolver.approve_action(report.incident_id, first_action.action_id)
    assert appr_res["success"] is True
    assert appr_res["status"] == "EXECUTED"
    print(f"   [PASS] Human Safety Gate successfully verified and executed: {first_action.action_id}")

    # ----------------------------------------------------
    # TEST 8: Closed-Loop Postmortem Generation & Auto-KB Injection
    # ----------------------------------------------------
    print("\n[TEST 8] Testing Closed-Loop Postmortem Synthesis & KB Re-injection...")
    pm_res = resolver.generate_and_store_postmortem(report.incident_id)
    assert pm_res["success"] is True
    assert pm_res["postmortem_doc_id"].startswith("PM-")
    # Verify postmortem document is now searchable in KB
    pm_hits = kb.search("payment gateway timeout postmortem", target_component="svc-payment", top_k=3)
    pm_ids = [h["doc_id"] for h in pm_hits]
    assert pm_res["postmortem_doc_id"] in pm_ids, "Newly created postmortem must be immediately searchable in KB"
    print(f"   Generated Postmortem Doc: [{pm_res['postmortem_doc_id']}] {pm_res['title']}")
    print("   [PASS] Closed-loop continuous learning verified: Postmortem injected back into Knowledge Base.")

    # ----------------------------------------------------
    # TEST 9: FastAPI Server Endpoints
    # ----------------------------------------------------
    print("\n[TEST 9] Testing FastAPI RESTful Endpoints (including Telemetry APIs)...")
    # GET /health
    r_health = client.get("/health")
    assert r_health.status_code == 200
    assert r_health.json()["status"] == "HEALTHY"

    # GET /api/v1/topology/graph
    r_graph = client.get("/api/v1/topology/graph")
    assert r_graph.status_code == 200
    assert len(r_graph.json()["nodes"]) == 11

    # POST /api/v1/changes
    r_chg = client.post("/api/v1/changes", json={
        "change_id": "CHG-API-001",
        "node_id": "svc-payment",
        "change_type": "DEPLOYMENT",
        "timestamp": time.time() - 300.0,
        "author": "sre-lead",
        "description": "Hotfix deployment for payment retries"
    })
    assert r_chg.status_code == 200
    assert r_chg.json()["success"] is True

    # POST /api/v1/telemetry/k8s
    r_k8s = client.post("/api/v1/telemetry/k8s", json={
        "reason": "FailedScheduling",
        "involvedObject": {"name": "pod-pay-02", "namespace": "production"},
        "message": "0/3 nodes are available: insufficient cpu."
    })
    assert r_k8s.status_code == 200
    assert r_k8s.json()["success"] is True

    # POST /api/v1/telemetry/otel
    r_otel = client.post("/api/v1/telemetry/otel", json={
        "service_name": "svc-payment",
        "span_id": "span-api-123",
        "trace_id": "trace-api-456",
        "status_code": "ERROR",
        "duration_ms": 12000.0,
        "http_status_code": 504,
        "error_message": "Upstream timeout"
    })
    assert r_otel.status_code == 200
    assert r_otel.json()["success"] is True

    # POST /api/v1/investigate
    r_inv = client.post("/api/v1/investigate", json={
        "alert_name": "K8s_Pod_CrashLoop",
        "severity": "CRITICAL",
        "node_id": "pod-pay-01"
    })
    assert r_inv.status_code == 200
    inv_data = r_inv.json()
    assert inv_data["incident_id"].startswith("INC-")

    # POST /api/v1/actions/approve
    act_to_approve = inv_data["recommended_actions"][0]["action_id"]
    r_app = client.post("/api/v1/actions/approve", json={
        "incident_id": inv_data["incident_id"],
        "action_id": act_to_approve
    })
    assert r_app.status_code == 200
    assert r_app.json()["status"] == "EXECUTED"

    # POST /api/v1/postmortem/generate
    r_pm = client.post("/api/v1/postmortem/generate", json={
        "incident_id": inv_data["incident_id"]
    })
    assert r_pm.status_code == 200
    assert r_pm.json()["postmortem_doc_id"].startswith("PM-")
    print("   [PASS] All RESTful API endpoints (Health, Graph, Changes, Telemetry K8s/OTel, Investigate, Approve, Postmortem) responded HTTP 200 OK.")

    # ---------------------------------------------------------
    # Pillar 10: Multi-Stage Remediation DAG & Automated Rollback
    # ---------------------------------------------------------
    print("\n[Pillar 10] Testing Multi-Stage Remediation DAG & Blast-Radius Pre-flight Audit...")
    from remediation_orchestrator import RemediationOrchestrator, StepExecutionStatus, DAGExecutionStatus

    rem_orch = RemediationOrchestrator(topo)

    # 10.1 Plan Remediation DAG for pod failure
    dag = rem_orch.plan_remediation_dag(
        incident_id="INC-DAG-TEST-001",
        target_node_id="pod-pay-01",
        action_type="RESTART_POD"
    )
    assert dag.status == DAGExecutionStatus.PLANNED
    assert dag.preflight_safety_passed is True
    assert len(dag.steps) == 4
    print(f"   [PASS] DAG Planned successfully with 4 stages (Pre-check, Mitigation, Verify, Rollback).")

    # 10.2 Attempt execution while step 2 is GATED -> Should pause at GATED
    exec_gated = rem_orch.execute_dag(dag.dag_id)
    assert exec_gated["status"] == "GATED"
    assert dag.steps[0].status == StepExecutionStatus.SUCCEEDED  # PRE_CHECK succeeded
    assert dag.steps[1].status == StepExecutionStatus.PENDING    # MITIGATION gated
    print(f"   [PASS] Execution safely paused at Step 2 awaiting Human Approval.")

    # 10.3 Approve Step 2
    app_res = rem_orch.approve_step(dag.dag_id, dag.steps[1].step_id)
    assert app_res["success"] is True
    assert dag.steps[1].approved_by == "sre-oncall-admin"

    # 10.4 Resume execution -> Succeeded without rollback
    exec_success = rem_orch.execute_dag(dag.dag_id)
    assert exec_success["status"] == "SUCCEEDED"
    assert dag.steps[1].status == StepExecutionStatus.SUCCEEDED
    assert dag.steps[2].status == StepExecutionStatus.SUCCEEDED
    assert dag.steps[3].status == StepExecutionStatus.SKIPPED
    print(f"   [PASS] Approved DAG executed smoothly: Pre-check -> Mitigation -> Post-verify (Rollback SKIPPED).")

    # 10.5 Simulated Verification Failure -> Automated Rollback Triggered
    print("\n[Pillar 10.5] Testing Automated Rollback upon Verification Failure...")
    dag_fail = rem_orch.plan_remediation_dag(
        incident_id="INC-DAG-FAIL-002",
        target_node_id="svc-payment",
        action_type="ROLLBACK_DEPLOYMENT",
        rollback_command="helm rollback payment-svc 2"
    )
    # Approve mitigation step
    rem_orch.approve_step(dag_fail.dag_id, dag_fail.steps[1].step_id)
    # Execute with simulated verification failure
    exec_rollback = rem_orch.execute_dag(dag_fail.dag_id, simulate_failure_at_verify=True)
    assert exec_rollback["status"] == "ROLLED_BACK"
    assert dag_fail.steps[2].status == StepExecutionStatus.FAILED   # POST_VERIFY failed
    assert dag_fail.steps[3].status == StepExecutionStatus.SUCCEEDED # ROLLBACK executed
    assert len(exec_rollback["contingency_executed"]) > 0
    print(f"   [PASS] Verification failure safely triggered contingency rollback: {exec_rollback['contingency_executed']}")

    # 10.6 Blast Radius Safety Barrier (Exceeding max allowed downstream impact)
    print("\n[Pillar 10.6] Testing Pre-flight Blast Radius Safety Barrier...")
    # svc-auth has blast radius 3 (svc-order, svc-payment, ing-01 etc.) -> threshold=2 should block preflight
    strict_orch = RemediationOrchestrator(topo, max_blast_radius_threshold=2)
    blocked_dag = strict_orch.plan_remediation_dag(
        incident_id="INC-DAG-BLOCK-003",
        target_node_id="svc-auth",
        action_type="RESTART_POD"
    )
    assert blocked_dag.preflight_safety_passed is False
    assert blocked_dag.status == DAGExecutionStatus.FAILED
    assert "SAFETY BARRIER TRIGGERED" in blocked_dag.preflight_warning
    print(f"   [PASS] Safety barrier blocked dangerous remediation: {blocked_dag.preflight_warning}")

    # 10.7 Test API Endpoints for Remediation DAG
    r_plan = client.post("/api/v1/remediation/plan", json={
        "incident_id": "INC-API-DAG-01",
        "target_node_id": "pod-pay-01",
        "action_type": "RESTART_POD"
    })
    assert r_plan.status_code == 200
    dag_api_id = r_plan.json()["dag_id"]
    step_to_approve = r_plan.json()["steps"][1]["step_id"]

    r_approve = client.post("/api/v1/remediation/approve", json={
        "dag_id": dag_api_id,
        "step_id": step_to_approve
    })
    assert r_approve.status_code == 200

    r_exec = client.post("/api/v1/remediation/execute", json={
        "dag_id": dag_api_id,
        "simulate_failure_at_verify": False
    })
    assert r_exec.status_code == 200
    assert r_exec.json()["status"] == "SUCCEEDED"
    print(f"   [PASS] REST API /api/v1/remediation endpoints (plan, approve, execute) verified.")

    # ---------------------------------------------------------
    # Pillar 11: Alert Storm Deduplication & Topological Flapping Suppressor
    # ---------------------------------------------------------
    print("\n[Pillar 11] Testing Alert Storm Deduplication & Topological Flapping Suppressor...")
    from alert_storm_dedup import AlertStormEngine, AlertItem
    from datetime import datetime, timezone, timedelta

    alert_storm_engine = AlertStormEngine(topo, flapping_window_seconds=60, flapping_threshold=3)

    now = datetime.now(timezone.utc)

    # 11.1 Test Flapping Detection (Rapid oscillating alert state FIRING <-> RESOLVED)
    flapping_alert_template = {
        "alert_id": "ALT-FLAP-01",
        "source_ci": "pod-pay-01",
        "metric_name": "container_cpu_throttle",
        "severity": "WARNING",
        "message": "CPU throttling oscillating"
    }
    # Send 4 oscillating alerts within 30 seconds
    for i, state in enumerate(["FIRING", "RESOLVED", "FIRING", "RESOLVED"]):
        item = AlertItem(
            alert_id=f"ALT-FLAP-{i}",
            source_ci=flapping_alert_template["source_ci"],
            metric_name=flapping_alert_template["metric_name"],
            severity=flapping_alert_template["severity"],
            timestamp=now - timedelta(seconds=40 - i * 10),
            message=flapping_alert_template["message"],
            state=state
        )
        is_flapping = alert_storm_engine.record_alert_state(item)
        if i >= 3:
            assert is_flapping is True, "Oscillating alert should be marked as flapping"
    print("   [PASS] Anti-flapping suppression correctly detected rapid state oscillation.")

    # 11.2 Test Cascading Storm Clustering (1 root DB failure causing downstream cascading alerts)
    storm_alerts = [
        # Upstream root: Database connection pool alert
        AlertItem(
            alert_id="ALT-ROOT-01",
            source_ci="db-postgres-pay",
            metric_name="pg_stat_activity_connections",
            severity="CRITICAL",
            timestamp=now,
            message="Connection pool exhausted (99% active)",
            state="FIRING"
        ),
        # Downstream cascading symptom 1: Payment gateway latency
        AlertItem(
            alert_id="ALT-DOWN-01",
            source_ci="svc-payment",
            metric_name="http_request_duration_seconds",
            severity="CRITICAL",
            timestamp=now,
            message="HTTP 504 Gateway Timeout elevated",
            state="FIRING"
        ),
        # Downstream cascading symptom 2: Order service degradation
        AlertItem(
            alert_id="ALT-DOWN-02",
            source_ci="svc-order",
            metric_name="downstream_call_failed",
            severity="WARNING",
            timestamp=now,
            message="Failed RPC calls to payment service",
            state="FIRING"
        ),
        # Downstream cascading symptom 3: Ingress error rate
        AlertItem(
            alert_id="ALT-DOWN-03",
            source_ci="ing-01",
            metric_name="nginx_ingress_controller_requests",
            severity="WARNING",
            timestamp=now,
            message="Ingress 5xx error rate > 5%",
            state="FIRING"
        ),
        # Flapping alert (should be filtered)
        AlertItem(
            alert_id="ALT-FLAP-TEST",
            source_ci="pod-pay-01",
            metric_name="container_cpu_throttle",
            severity="WARNING",
            timestamp=now,
            message="CPU throttling oscillating",
            state="FIRING"
        )
    ]

    clusters, storm_metrics = alert_storm_engine.cluster_alerts(storm_alerts)
    assert len(clusters) == 1, f"Expected 1 collapsed incident cluster, got {len(clusters)}"
    root_cluster = clusters[0]
    assert root_cluster.root_ci_candidate == "db-postgres-pay"
    assert root_cluster.alerts_count == 4  # 4 valid cascading alerts collapsed, 1 flapping suppressed
    assert "db-postgres-pay" in root_cluster.affected_cis
    assert storm_metrics["suppressed_flapping"] == 1
    assert storm_metrics["clusters_formed"] == 1
    print(f"   [PASS] Cascading storm successfully collapsed into 1 root cluster: {root_cluster.cluster_id}")
    print(f"   [PASS] Storm metrics verified: {storm_metrics['noise_reduction_percentage']} noise reduction.")

    # 11.3 Test REST API /api/v1/alerts/storm-cluster
    r_storm = client.post("/api/v1/alerts/storm-cluster", json={
        "alerts": [
            {
                "alert_id": "API-ALT-01",
                "source_ci": "db-postgres-pay",
                "metric_name": "db_connections",
                "severity": "CRITICAL",
                "message": "DB saturation"
            },
            {
                "alert_id": "API-ALT-02",
                "source_ci": "svc-payment",
                "metric_name": "http_5xx",
                "severity": "CRITICAL",
                "message": "504 from payment"
            }
        ]
    })
    assert r_storm.status_code == 200
    storm_res = r_storm.json()
    assert len(storm_res["clusters"]) == 1
    assert storm_res["clusters"][0]["root_ci_candidate"] == "db-postgres-pay"
    print("   [PASS] REST API /api/v1/alerts/storm-cluster responded HTTP 200 OK.")

    print("\n" + "=" * 70)
    print(">> [SUCCESS] All 11 Verification Pillars Passed with Exit Code 0!")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()
