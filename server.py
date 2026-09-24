"""
RESTful API Server for Agentic CMDB GraphRAG (server.py)
Provides endpoints for:
- Alert Webhook Ingestion (Prometheus / Datadog / Zabbix compatible)
- Multi-Modal Telemetry Ingestion (K8s Events, Prometheus Metrics, OpenTelemetry Spans, CloudTrail)
- On-demand SRE Incident Investigation
- Downstream Blast-Radius Analysis
- Cytoscape / D3 Topology Graph Visualization
- Human-Gate Remediation Approval
- Temporal Change Event Ingestion & Correlation
- Postmortem Synthesis & Closed-Loop KB Feedback
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from dataclasses import asdict
import time

from cmdb_topology import build_sample_enterprise_topology, CMDBTopologyGraph
from knowledge_base import build_sample_sre_knowledge_base, HybridKnowledgeBase
from change_correlator import TemporalChangeCorrelator, ChangeEvent
from multimodal_ingestion import MultiModalTelemetryIngestionEngine, TelemetryEvent
from dual_level_graphrag import DualLevelGraphRAGEngine
from rca_agent import AgenticRCAResolver
from remediation_orchestrator import RemediationOrchestrator
from alert_storm_dedup import AlertStormEngine, AlertItem
from topology_drift_reconciler import TopologyDriftReconciler, DriftType, DriftReconciliationReport
from canary_evaluator import CanarySLIEvaluator, CanaryEvaluationResult, SLIMetricThreshold
from backpressure_governor import DependencyBackpressureGovernor, NodeHealthMetrics, BackpressureDirective, CircuitState
from datetime import datetime, timezone

app = FastAPI(
    title="Agentic-CMDB-GraphRAG API Gateway",
    description="Enterprise SRE Root-Cause Analysis and Topology-Grounded GraphRAG System with Temporal Correlation, Multi-Modal Telemetry & Drift Reconciler",
    version="1.6.0"
)

# Global system state
topology = build_sample_enterprise_topology()
kb = build_sample_sre_knowledge_base()
correlator = TemporalChangeCorrelator()
telemetry_engine = MultiModalTelemetryIngestionEngine()
engine = DualLevelGraphRAGEngine(topology, kb, correlator, telemetry_engine)
resolver = AgenticRCAResolver(engine)
orchestrator = RemediationOrchestrator(topology)
alert_engine = AlertStormEngine(topology)
drift_reconciler = TopologyDriftReconciler(topology, telemetry_engine)
canary_evaluator = CanarySLIEvaluator()
backpressure_governor = DependencyBackpressureGovernor(topology)


class AlertWebhookPayload(BaseModel):
    alert_name: str = Field(..., example="HTTP504_Gateway_Timeout")
    severity: str = Field(..., example="CRITICAL")
    node_id: str = Field(..., example="svc-payment")
    timestamp: Optional[float] = Field(default=None, example=1773547200.0)
    details: Dict[str, Any] = Field(default_factory=dict)


class ActionApprovalPayload(BaseModel):
    incident_id: str
    action_id: str


class ChangeEventPayload(BaseModel):
    change_id: str
    node_id: str
    change_type: str  # DEPLOYMENT, CONFIG_UPDATE, SCHEMA_MIGRATION, SECRET_ROTATION
    timestamp: float
    author: str
    description: str
    details: Dict[str, Any] = Field(default_factory=dict)


class K8sEventPayload(BaseModel):
    reason: str
    involvedObject: Dict[str, Any]
    type: Optional[str] = "Warning"
    message: Optional[str] = ""
    count: Optional[int] = 1
    source: Optional[Dict[str, Any]] = Field(default_factory=dict)
    firstTimestamp: Optional[float] = None


class OTelSpanPayload(BaseModel):
    service_name: str
    span_id: str
    trace_id: str
    status_code: str = "OK"  # OK, ERROR
    duration_ms: float = 0.0
    http_status_code: Optional[int] = 200
    error_message: Optional[str] = ""
    start_time: Optional[float] = None


class PostmortemPayload(BaseModel):
    incident_id: str


class PlanRemediationPayload(BaseModel):
    incident_id: str
    target_node_id: str
    action_type: str  # RECYCLE_DB_POOL, ROLLBACK_DEPLOYMENT, RESTART_POD
    rollback_command: Optional[str] = None


class ApproveStepPayload(BaseModel):
    dag_id: str
    step_id: str


class ExecuteDAGPayload(BaseModel):
    dag_id: str
    simulate_failure_at_verify: Optional[bool] = False


@app.get("/health")
def health_check():
    return {
        "status": "HEALTHY",
        "service": "Agentic-CMDB-GraphRAG",
        "ci_nodes_count": len(topology.nodes),
        "runbooks_indexed": len(kb.documents),
        "recorded_changes_count": len(correlator.events),
        "telemetry_events_ingested": len(telemetry_engine.event_store)
    }


@app.get("/api/v1/topology/graph")
def get_full_topology_graph():
    """Returns the full CMDB topology graph for frontend visualization"""
    return topology.export_graph_json()


@app.get("/api/v1/topology/blast-radius/{node_id}")
def get_blast_radius(node_id: str, max_depth: int = 3):
    """Computes downstream blast radius of a failing CI component"""
    res = topology.compute_blast_radius(node_id, max_depth=max_depth)
    if "error" in res:
        raise HTTPException(status_code=404, detail=res["error"])
    return res


@app.post("/api/v1/changes")
def record_change(payload: ChangeEventPayload):
    """Records a CI/CD deployment, configuration change, or database schema migration"""
    event = ChangeEvent(
        change_id=payload.change_id,
        node_id=payload.node_id,
        change_type=payload.change_type,
        timestamp=payload.timestamp,
        author=payload.author,
        description=payload.description,
        details=payload.details
    )
    correlator.record_change(event)
    return {"success": True, "change_id": event.change_id, "recorded_count": len(correlator.events)}


@app.post("/api/v1/telemetry/k8s")
def ingest_k8s_telemetry(payload: K8sEventPayload):
    """Ingests raw Kubernetes cluster events (OOMKilled, PodEvicted, FailedScheduling)"""
    event = telemetry_engine.ingest_k8s_event(payload.dict())
    return {
        "success": True,
        "event_id": event.event_id,
        "severity": event.severity,
        "target_node_id": event.target_node_id
    }


@app.post("/api/v1/telemetry/otel")
def ingest_otel_span(payload: OTelSpanPayload):
    """Ingests OpenTelemetry tracing spans with error status or high latency"""
    event = telemetry_engine.ingest_opentelemetry_span(payload.dict())
    return {
        "success": True,
        "event_id": event.event_id,
        "severity": event.severity,
        "target_node_id": event.target_node_id
    }


@app.post("/api/v1/investigate")
def trigger_investigation(payload: AlertWebhookPayload):
    """
    Executes autonomous incident investigation:
    1. Dual-level GraphRAG context retrieval with multi-modal telemetry
    2. Upstream root cause candidate isolation & temporal change correlation
    3. Runbook-grounded action plan generation with Human Safety Gates
    """
    data = payload.dict()
    if not data.get("timestamp"):
        data["timestamp"] = time.time()
    report = resolver.investigate(data)
    return asdict(report)


@app.post("/api/v1/actions/approve")
def approve_action(payload: ActionApprovalPayload):
    """Human gate safety approval for executing remediation commands"""
    res = resolver.approve_action(payload.incident_id, payload.action_id)
    if not res.get("success"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@app.post("/api/v1/postmortem/generate")
def generate_postmortem(payload: PostmortemPayload):
    """Closed-loop learning: creates and stores a postmortem document in KB"""
    res = resolver.generate_and_store_postmortem(payload.incident_id)
    if not res.get("success"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@app.post("/api/v1/remediation/plan")
def plan_remediation_workflow(payload: PlanRemediationPayload):
    """
    Synthesizes a 4-stage Remediation DAG (PRE_CHECK -> MITIGATION -> POST_VERIFY -> ROLLBACK_STEP)
    with automated CMDB Blast Radius Pre-Flight Safety Audit.
    """
    dag = orchestrator.plan_remediation_dag(
        incident_id=payload.incident_id,
        target_node_id=payload.target_node_id,
        action_type=payload.action_type,
        rollback_command=payload.rollback_command
    )
    return asdict(dag)


@app.post("/api/v1/remediation/approve")
def approve_remediation_step(payload: ApproveStepPayload):
    """Human safety gate approval for a gated mitigation step in a DAG"""
    res = orchestrator.approve_step(payload.dag_id, payload.step_id)
    if not res.get("success"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@app.post("/api/v1/remediation/execute")
def execute_remediation_dag(payload: ExecuteDAGPayload):
    """
    Executes DAG workflow. If verification fails, automatically triggers safe rollback.
    """
    res = orchestrator.execute_dag(
        dag_id=payload.dag_id,
        simulate_failure_at_verify=payload.simulate_failure_at_verify or False
    )
    return res


class RawAlertPayload(BaseModel):
    alert_id: str
    source_ci: str
    metric_name: str
    severity: str
    message: str
    state: Optional[str] = "FIRING"
    timestamp: Optional[float] = None


class AlertStormBatchPayload(BaseModel):
    alerts: List[RawAlertPayload]


@app.post("/api/v1/alerts/storm-cluster")
def cluster_alert_storm(payload: AlertStormBatchPayload):
    """
    Suppresses flapping alerts and clusters cascading alert storms into 
    unified root-incident clusters using CMDB directed dependency topology.
    """
    alert_items = []
    for a in payload.alerts:
        ts = datetime.fromtimestamp(a.timestamp, tz=timezone.utc) if a.timestamp else datetime.now(timezone.utc)
        item = AlertItem(
            alert_id=a.alert_id,
            source_ci=a.source_ci,
            metric_name=a.metric_name,
            severity=a.severity,
            timestamp=ts,
            message=a.message,
            state=a.state or "FIRING"
        )
        alert_items.append(item)

    clusters, metrics = alert_engine.cluster_alerts(alert_items)
    return {
        "metrics": metrics,
        "clusters": [asdict(c) for c in clusters]
    }


class DriftReconciliationPayload(BaseModel):
    live_k8s_resources: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    observed_spans: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    auto_heal: Optional[bool] = False


@app.post("/api/v1/topology/reconcile-drift")
def reconcile_topology_drift(payload: DriftReconciliationPayload):
    """
    Pillar 12: Reconciles CMDB declared graph against live runtime K8s states
    and distributed OpenTelemetry trace spans to detect shadow CIs, phantom CIs,
    and undocumented dependency edges.
    """
    report = drift_reconciler.reconcile(
        live_k8s_resources=payload.live_k8s_resources,
        observed_spans=payload.observed_spans
    )
    res: Dict[str, Any] = {
        "report_id": report.report_id,
        "generated_at": report.generated_at,
        "total_drifts": report.total_drifts,
        "critical_drifts": report.critical_drifts,
        "warning_drifts": report.warning_drifts,
        "graph_health_score": report.graph_health_score,
        "drifts": [asdict(d) for d in report.drifts],
        "healing_patch": report.healing_patch
    }

    if payload.auto_heal:
        healing_result = drift_reconciler.apply_auto_healing(report)
        res["auto_healing_applied"] = healing_result

    return res


class CanaryEvaluationPayload(BaseModel):
    target_node_id: str = Field(..., example="pod-pay-01")
    observed_metrics: Dict[str, float] = Field(..., example={"http_p99_latency_ms": 420.0, "http_5xx_error_rate_pct": 0.05})


@app.post("/api/v1/canary/evaluate")
def evaluate_canary_sli(payload: CanaryEvaluationPayload):
    """
    Pillar 13: Evaluates real-time canary SLI metrics (p99 latency, 5xx error budget degradation, cpu throttling)
    against enterprise SLO thresholds to recommend PROMOTE or TRIGGER_ROLLBACK.
    """
    result = canary_evaluator.evaluate_canary(
        target_node_id=payload.target_node_id,
        observed_metrics=payload.observed_metrics
    )
    return asdict(result)


class BackpressureGovernPayload(BaseModel):
    target_ci: str = Field(..., example="rds-postgres-payment-primary")
    latency_p99_ms: float = Field(..., example=1450.0)
    error_rate_pct: float = Field(..., example=12.5)
    connection_pool_utilization_pct: float = Field(..., example=92.0)


@app.post("/api/v1/backpressure/govern")
def govern_dependency_backpressure(payload: BackpressureGovernPayload):
    """
    Pillar 14: Dynamic Adaptive Rate-Limiting & Dependency Backpressure Governor
    Assesses downstream CI degradation and dynamically computes AIMD concurrency/rate limits
    and circuit breaking states for all upstream callers to stop cascading outages.
    """
    metrics = NodeHealthMetrics(
        latency_p99_ms=payload.latency_p99_ms,
        error_rate_pct=payload.error_rate_pct,
        connection_pool_utilization_pct=payload.connection_pool_utilization_pct
    )
    directives = backpressure_governor.assess_and_govern(
        target_ci=payload.target_ci,
        metrics=metrics
    )
    return {
        "target_ci": payload.target_ci,
        "total_directives": len(directives),
        "directives": [asdict(d) for d in directives]
    }





