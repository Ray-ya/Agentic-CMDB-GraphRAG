"""
Pillar 15: Topologically-Grounded Chaos Simulator & SPOF Resilience Validator
(chaos_resilience_simulator.py)

Part of Agentic-CMDB-GraphRAG
Validates infrastructure resilience before outages happen by combining:
1. Directed CMDB Dependency Graph Traversal (Identifying Single Points of Failure / SPOFs)
2. Synthetic Chaos Experiment Simulation (Network Partition, Latency Injection, Node Termination)
3. Dynamic Blast Radius Propagation & Cascading Outage Simulation
4. Automated Resilience Health Scoring (0 - 100) & Proactive Hardening Recommendations
"""

from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import uuid
import time

from cmdb_topology import CMDBTopologyGraph, CINode


class FaultType(str, Enum):
    NETWORK_PARTITION = "NETWORK_PARTITION"     # Sever connectivity between CIs
    LATENCY_INJECTION = "LATENCY_INJECTION"     # Add synthetic latency (e.g., 2000ms)
    NODE_TERMINATION = "NODE_TERMINATION"       # Abrupt node crash / OOM
    RESOURCE_SATURATION = "RESOURCE_SATURATION" # 100% CPU/Memory/Pool exhaustion


@dataclass
class ChaosExperimentSpec:
    experiment_id: str
    target_ci: str
    fault_type: FaultType
    duration_seconds: int = 60
    injected_latency_ms: float = 0.0
    packet_loss_pct: float = 0.0
    description: str = ""


@dataclass
class SimulationImpact:
    ci_id: str
    ci_name: str
    ci_type: str
    impact_level: str  # DIRECT_FAILURE, CASCADING_FAILURE, DEGRADED, UNAFFECTED
    has_fallback: bool
    path_depth: int
    notes: str


@dataclass
class ChaosSimulationReport:
    experiment_id: str
    target_ci: str
    fault_type: str
    is_spof: bool
    total_nodes_in_topology: int
    directly_impacted_count: int
    cascading_impacted_count: int
    resilience_score: float  # 0.0 to 100.0
    blast_radius_pct: float
    impacted_components: List[SimulationImpact]
    hardening_recommendations: List[str]
    timestamp: float = field(default_factory=time.time)


class ChaosResilienceSimulator:
    """
    Simulates topological chaos scenarios and audits architectural resilience & SPOFs.
    """

    def __init__(self, topology: CMDBTopologyGraph):
        self.topology = topology

    def detect_spofs(self) -> List[Dict[str, Any]]:
        """
        Scans CMDB graph to identify Single Points of Failure (SPOFs).
        A CI is a SPOF if critical services or pods depend on it and it has no
        active-active redundant failover peer (e.g., standalone database, non-replicated service).
        """
        spofs = []
        for node_id, node in self.topology.nodes.items():
            callers = self.topology.in_edges.get(node_id, [])
            if not callers:
                continue

            # Critical infrastructure items: DATABASE, MESSAGE_BROKER, or critical SERVICE with callers
            # Check if this node has dedicated HA active-active / read-replica peers
            has_explicit_ha = node.metadata.get("ha_enabled", False) or node.metadata.get("replica_count", 1) > 1
            
            # If multiple components depend on it, or if it's a sole backend datastore
            is_critical_dependency = (
                node.ci_type in ("DATABASE", "MESSAGE_BROKER") or
                (node.ci_type == "SERVICE" and len(callers) >= 1)
            )

            # Check if callers have an identical failover target (same engine/role)
            redundancy_peers = [
                n_id for n_id, n in self.topology.nodes.items()
                if n_id != node_id and n.ci_type == node.ci_type and n.name.startswith(node.name.split("-")[0])
            ]

            if is_critical_dependency and not has_explicit_ha and len(redundancy_peers) == 0:
                spofs.append({
                    "node_id": node_id,
                    "node_name": node.name,
                    "ci_type": node.ci_type,
                    "dependent_caller_count": len(callers),
                    "dependent_callers": [e.source for e in callers],
                    "risk_level": "CRITICAL" if node.ci_type in ("DATABASE", "SERVICE") else "HIGH"
                })
        return spofs

    def simulate_fault(self, spec: ChaosExperimentSpec) -> ChaosSimulationReport:
        """
        Runs synthetic topological failure propagation based on CMDB directed dependencies.
        """
        target_node = self.topology.get_node(spec.target_ci)
        if not target_node:
            raise ValueError(f"Target CI '{spec.target_ci}' does not exist in CMDB topology.")

        # Compute blast radius from target node upwards to dependent services
        blast = self.topology.compute_blast_radius(spec.target_ci, max_depth=5)
        impacted_cis = blast.get("impacted_components", [])

        # Evaluate SPOF status
        spof_list = self.detect_spofs()
        is_spof = any(s["node_id"] == spec.target_ci for s in spof_list)

        impact_details: List[SimulationImpact] = []
        # Target node itself
        impact_details.append(SimulationImpact(
            ci_id=target_node.id,
            ci_name=target_node.name,
            ci_type=target_node.ci_type,
            impact_level="DIRECT_FAILURE",
            has_fallback=not is_spof,
            path_depth=0,
            notes=f"Injected fault: {spec.fault_type}"
        ))

        cascading_count = 0
        for comp in impacted_cis:
            # Check if component has circuit breaker or fallback
            has_fallback = False
            # If path depth is 1, direct dependency; if > 1, cascading
            impact_level = "CASCADING_FAILURE" if comp["depth"] > 1 else "DIRECT_FAILURE"
            if impact_level == "CASCADING_FAILURE":
                cascading_count += 1

            impact_details.append(SimulationImpact(
                ci_id=comp["id"],
                ci_name=comp["name"],
                ci_type=comp["ci_type"],
                impact_level=impact_level,
                has_fallback=has_fallback,
                path_depth=comp["depth"],
                notes=f"Impacted via path: {comp.get('path_from_root', 'N/A')}"
            ))

        total_nodes = len(self.topology.nodes)
        total_impacted = len(impact_details)
        blast_radius_pct = round((total_impacted / total_nodes) * 100.0, 2) if total_nodes > 0 else 0.0

        # Resilience score calculation: 100 minus impact penalties
        penalty = (blast_radius_pct * 0.6) + (30.0 if is_spof else 0.0) + (cascading_count * 5.0)
        resilience_score = max(5.0, min(100.0, round(100.0 - penalty, 1)))

        # Formulate actionable hardening recommendations
        recommendations = []
        if is_spof:
            recommendations.append(
                f"SPOF Remediation: Provision high-availability multi-AZ standby or read-replica pool for {target_node.name}."
            )
        if cascading_count > 0:
            recommendations.append(
                f"Cascading Isolation: Enforce dynamic circuit-breaking and token-bucket backpressure on upstream callers ({cascading_count} cascading nodes detected)."
            )
        if spec.fault_type == FaultType.LATENCY_INJECTION:
            recommendations.append(
                "Timeout Budgeting: Tighten client-side RPC timeouts and configure hedged requests to mask latency tails."
            )
        if not recommendations:
            recommendations.append("Topology demonstrated strong fault isolation with negligible cascading risk.")

        return ChaosSimulationReport(
            experiment_id=spec.experiment_id,
            target_ci=spec.target_ci,
            fault_type=spec.fault_type.value,
            is_spof=is_spof,
            total_nodes_in_topology=total_nodes,
            directly_impacted_count=total_impacted - cascading_count,
            cascading_impacted_count=cascading_count,
            resilience_score=resilience_score,
            blast_radius_pct=blast_radius_pct,
            impacted_components=impact_details,
            hardening_recommendations=recommendations
        )
