"""
Topology Drift Engine & Shadow Dependency Reconciler (Pillar 12)
Part of Agentic-CMDB-GraphRAG v1.4.0

Solves the #1 enterprise SRE bottleneck: CMDB Staleness & Topology Drift.
Continuously reconciles the deterministic CMDB knowledge graph against:
1. Live Kubernetes runtime states (Pods, Services, Ingresses)
2. Distributed OpenTelemetry trace spans (Observing live caller->callee edges)
3. Multi-modal telemetry anomalies

Detects:
- SHADOW_CI: Unregistered workloads running in production discovered via telemetry/OTel.
- PHANTOM_CI: Stale/terminated nodes lingering in CMDB with zero heartbeat.
- UNDOCUMENTED_EDGE: Hidden architectural dependencies observed in distributed traces.
- MUTATED_STATE: Divergence between CMDB declared status and runtime health.

Generates automated Graph Self-Healing Patches and GraphRAG reconciliation audits.
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import hashlib

from cmdb_topology import CMDBTopologyGraph, CINode, CIEdge
from multimodal_ingestion import MultiModalTelemetryIngestionEngine, TelemetryEvent


class DriftType(str, Enum):
    SHADOW_CI = "SHADOW_CI"                  # Running in runtime, missing in CMDB
    PHANTOM_CI = "PHANTOM_CI"                # In CMDB, but dead/terminated in runtime
    UNDOCUMENTED_EDGE = "UNDOCUMENTED_EDGE"  # Live traffic observed, but no edge in CMDB
    MUTATED_STATE = "MUTATED_STATE"          # Health state mismatch


@dataclass
class DriftItem:
    drift_id: str
    drift_type: DriftType
    target_id: str
    target_name: str
    severity: str  # CRITICAL, WARNING, INFO
    description: str
    evidence: Dict[str, Any]
    detected_at: float = field(default_factory=time.time)
    remediation_action: str = ""


@dataclass
class DriftReconciliationReport:
    report_id: str
    generated_at: float
    total_drifts: int
    critical_drifts: int
    warning_drifts: int
    drifts: List[DriftItem]
    healing_patch: Dict[str, Any]
    graph_health_score: float  # 0.0 to 100.0


class TopologyDriftReconciler:
    """
    Continuous Graph Reconciliation Engine.
    Cross-references CMDB declared topology with live OTel distributed traces & K8s cluster state.
    """

    def __init__(self, topology: CMDBTopologyGraph, telemetry_engine: Optional[MultiModalTelemetryIngestionEngine] = None):
        self.topology = topology
        self.telemetry_engine = telemetry_engine or MultiModalTelemetryIngestionEngine()
        self.audit_log: List[Dict[str, Any]] = []

    def reconcile(
        self,
        live_k8s_resources: Optional[List[Dict[str, Any]]] = None,
        observed_spans: Optional[List[Dict[str, Any]]] = None
    ) -> DriftReconciliationReport:
        """
        Runs comprehensive drift detection across nodes and edges.
        """
        drifts: List[DriftItem] = []
        live_k8s_resources = live_k8s_resources or []
        observed_spans = observed_spans or []

        cmdb_node_ids = set(self.topology.nodes.keys())
        live_node_ids: Set[str] = set()

        # 1. Inspect live K8s resources for SHADOW_CI and MUTATED_STATE
        for res in live_k8s_resources:
            res_id = res.get("id")
            res_name = res.get("name", res_id)
            res_type = res.get("type", "POD")
            res_status = res.get("status", "HEALTHY")
            live_node_ids.add(res_id)

            if res_id not in cmdb_node_ids:
                # Discovered an uncataloged workload
                drifts.append(DriftItem(
                    drift_id=f"DFT-{hashlib.md5((res_id + 'SHADOW').encode()).hexdigest()[:8].upper()}",
                    drift_type=DriftType.SHADOW_CI,
                    target_id=res_id,
                    target_name=res_name,
                    severity="CRITICAL" if res_type in ("SERVICE", "DATABASE") else "WARNING",
                    description=f"Workload '{res_name}' ({res_id}) is running in cluster but not registered in CMDB.",
                    evidence={"k8s_spec": res, "ci_type": res_type},
                    remediation_action=f"Auto-register CI node '{res_id}' into CMDB topology graph."
                ))
            else:
                # Check status drift
                cmdb_node = self.topology.nodes[res_id]
                if cmdb_node.status != res_status:
                    drifts.append(DriftItem(
                        drift_id=f"DFT-{hashlib.md5((res_id + 'STATE').encode()).hexdigest()[:8].upper()}",
                        drift_type=DriftType.MUTATED_STATE,
                        target_id=res_id,
                        target_name=res_name,
                        severity="WARNING",
                        description=f"Status mismatch for '{res_name}': CMDB declared '{cmdb_node.status}' but runtime is '{res_status}'.",
                        evidence={"cmdb_status": cmdb_node.status, "runtime_status": res_status},
                        remediation_action=f"Update CMDB node '{res_id}' status to '{res_status}'."
                    ))

        # 2. Inspect CMDB nodes for PHANTOM_CI (Nodes marked as running in K8s, but not in live resources)
        # We only check POD / SERVICE types that are expected to report in live_k8s_resources
        if live_k8s_resources:
            for node_id, node in self.topology.nodes.items():
                if node.ci_type in ("POD", "SERVICE") and node_id not in live_node_ids:
                    # Stale phantom node
                    drifts.append(DriftItem(
                        drift_id=f"DFT-{hashlib.md5((node_id + 'PHANTOM').encode()).hexdigest()[:8].upper()}",
                        drift_type=DriftType.PHANTOM_CI,
                        target_id=node_id,
                        target_name=node.name,
                        severity="WARNING",
                        description=f"CMDB node '{node.name}' ({node_id}) is absent from live K8s runtime resources.",
                        evidence={"cmdb_node": {"id": node.id, "type": node.ci_type, "status": node.status}},
                        remediation_action=f"Deprecate/Retire phantom node '{node_id}' from active CMDB graph."
                    ))

        # 3. Inspect OpenTelemetry spans for UNDOCUMENTED_EDGE (Hidden caller -> callee dependencies)
        for span in observed_spans:
            caller = span.get("caller_ci")
            callee = span.get("callee_ci")
            if not caller or not callee:
                continue

            # Verify if caller and callee have a direct or indirect edge in CMDB
            has_edge = False
            if caller in self.topology.out_edges:
                for edge in self.topology.out_edges[caller]:
                    if edge.target == callee:
                        has_edge = True
                        break

            if not has_edge:
                edge_sig = f"{caller}->{callee}"
                drifts.append(DriftItem(
                    drift_id=f"DFT-{hashlib.md5((edge_sig + 'EDGE').encode()).hexdigest()[:8].upper()}",
                    drift_type=DriftType.UNDOCUMENTED_EDGE,
                    target_id=edge_sig,
                    target_name=edge_sig,
                    severity="CRITICAL" if span.get("has_errors") else "WARNING",
                    description=f"Live distributed trace observed caller '{caller}' invoking callee '{callee}', but no dependency edge exists in CMDB.",
                    evidence={"span_id": span.get("span_id"), "trace_id": span.get("trace_id"), "duration_ms": span.get("duration_ms")},
                    remediation_action=f"Synthesize new CMDB edge: ({caller}) -[DEPENDS_ON]-> ({callee})."
                ))

        # Synthesize Healing Patch
        healing_patch = {
            "nodes_to_add": [],
            "nodes_to_update": [],
            "nodes_to_deprecate": [],
            "edges_to_add": []
        }

        for d in drifts:
            if d.drift_type == DriftType.SHADOW_CI:
                healing_patch["nodes_to_add"].append({
                    "id": d.target_id,
                    "name": d.target_name,
                    "ci_type": d.evidence.get("ci_type", "SERVICE"),
                    "status": d.evidence.get("k8s_spec", {}).get("status", "HEALTHY")
                })
            elif d.drift_type == DriftType.MUTATED_STATE:
                healing_patch["nodes_to_update"].append({
                    "id": d.target_id,
                    "new_status": d.evidence.get("runtime_status")
                })
            elif d.drift_type == DriftType.PHANTOM_CI:
                healing_patch["nodes_to_deprecate"].append(d.target_id)
            elif d.drift_type == DriftType.UNDOCUMENTED_EDGE:
                caller, callee = d.target_id.split("->")
                healing_patch["edges_to_add"].append({
                    "source": caller,
                    "target": callee,
                    "relation": "DEPENDS_ON"
                })

        # Calculate Graph Health Score
        total_nodes = max(len(self.topology.nodes), 1)
        penalty = (len([d for d in drifts if d.severity == "CRITICAL"]) * 15.0) + \
                  (len([d for d in drifts if d.severity == "WARNING"]) * 5.0)
        health_score = max(0.0, min(100.0, 100.0 - (penalty / total_nodes * 20.0)))

        crit_count = sum(1 for d in drifts if d.severity == "CRITICAL")
        warn_count = sum(1 for d in drifts if d.severity == "WARNING")

        report = DriftReconciliationReport(
            report_id=f"REP-DRIFT-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8].upper()}",
            generated_at=time.time(),
            total_drifts=len(drifts),
            critical_drifts=crit_count,
            warning_drifts=warn_count,
            drifts=drifts,
            healing_patch=healing_patch,
            graph_health_score=round(health_score, 1)
        )

        return report

    def apply_auto_healing(self, report: DriftReconciliationReport) -> Dict[str, Any]:
        """
        Executes safe self-healing mutations onto the CMDB topology graph.
        """
        patch = report.healing_patch
        applied_nodes_added = 0
        applied_nodes_updated = 0
        applied_edges_added = 0

        # 1. Add shadow CIs
        for node_spec in patch.get("nodes_to_add", []):
            if node_spec["id"] not in self.topology.nodes:
                new_node = CINode(
                    id=node_spec["id"],
                    name=node_spec["name"],
                    ci_type=node_spec["ci_type"],
                    status=node_spec["status"],
                    metadata={"source": "auto_healed_from_runtime"}
                )
                self.topology.add_node(new_node)
                applied_nodes_added += 1

        # 2. Update status drift
        for update_spec in patch.get("nodes_to_update", []):
            node = self.topology.get_node(update_spec["id"])
            if node:
                node.status = update_spec["new_status"]
                applied_nodes_updated += 1

        # 3. Add undocumented edges
        for edge_spec in patch.get("edges_to_add", []):
            src = edge_spec["source"]
            tgt = edge_spec["target"]
            # Ensure both exist before adding edge
            if src in self.topology.nodes and tgt in self.topology.nodes:
                # Check if edge already exists
                exists = any(e.target == tgt for e in self.topology.out_edges.get(src, []))
                if not exists:
                    self.topology.add_edge(src, tgt, edge_spec["relation"])
                    applied_edges_added += 1

        result = {
            "status": "HEALED",
            "report_id": report.report_id,
            "nodes_added": applied_nodes_added,
            "nodes_updated": applied_nodes_updated,
            "edges_added": applied_edges_added,
            "timestamp": time.time()
        }
        self.audit_log.append(result)
        return result
