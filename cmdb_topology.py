"""
CMDB Infrastructure Knowledge Graph (cmdb_topology.py)
Implements deterministic topological graph modeling, upstream root-cause dependency traversal,
and downstream blast-radius calculation for cloud-native infrastructure.
"""

from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field
import json


@dataclass
class CINode:
    id: str
    name: str
    ci_type: str  # HOST, POD, SERVICE, DATABASE, INGRESS, SWITCH
    status: str   # HEALTHY, WARNING, CRITICAL, DEGRADED
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CIEdge:
    source: str
    target: str
    relation: str  # DEPLOYS_ON, DEPENDS_ON, CONNECTS_TO, ROUTED_BY, HOSTED_ON
    weight: float = 1.0


class CMDBTopologyGraph:
    """
    Deterministic Knowledge Graph representing Infrastructure Configuration Items (CIs).
    Unlike pure vector stores, this guarantees zero-hallucination topological dependency tracking.
    """

    def __init__(self):
        self.nodes: Dict[str, CINode] = {}
        self.out_edges: Dict[str, List[CIEdge]] = {}  # source -> list of edges
        self.in_edges: Dict[str, List[CIEdge]] = {}   # target -> list of edges

    def add_node(self, node: CINode) -> None:
        self.nodes[node.id] = node
        if node.id not in self.out_edges:
            self.out_edges[node.id] = []
        if node.id not in self.in_edges:
            self.in_edges[node.id] = []

    def add_edge(self, source: str, target: str, relation: str, weight: float = 1.0) -> None:
        if source not in self.nodes or target not in self.nodes:
            raise ValueError(f"Both source '{source}' and target '{target}' must exist in graph.")
        edge = CIEdge(source=source, target=target, relation=relation, weight=weight)
        self.out_edges[source].append(edge)
        self.in_edges[target].append(edge)

    def get_node(self, node_id: str) -> Optional[CINode]:
        return self.nodes.get(node_id)

    def compute_blast_radius(self, node_id: str, max_depth: int = 3) -> Dict[str, Any]:
        """
        Calculates downstream blast radius: If this node fails, who else will be impacted?
        Traverses edges where other components DEPEND_ON or CONNECT_TO this node.
        """
        if node_id not in self.nodes:
            return {"error": f"Node {node_id} not found", "impacted_components": []}

        impacted: Dict[str, Dict[str, Any]] = {}
        queue: List[Tuple[str, int, str]] = [(node_id, 0, "Self")]
        visited: Set[str] = {node_id}

        while queue:
            curr_id, depth, rel_path = queue.pop(0)
            if depth > 0:
                curr_node = self.nodes[curr_id]
                impacted[curr_id] = {
                    "id": curr_node.id,
                    "name": curr_node.name,
                    "ci_type": curr_node.ci_type,
                    "status": curr_node.status,
                    "depth": depth,
                    "path_from_root": rel_path
                }

            if depth < max_depth:
                # Upstream dependencies that call this node (in_edges with DEPENDS_ON/CONNECTS_TO/DEPLOYS_ON)
                for edge in self.in_edges.get(curr_id, []):
                    if edge.source not in visited and edge.relation in ("DEPENDS_ON", "CONNECTS_TO", "ROUTED_BY", "DEPLOYS_ON"):
                        visited.add(edge.source)
                        queue.append((edge.source, depth + 1, f"{rel_path} <- ({edge.relation}) <- {edge.source}"))

        severity = "HIGH" if len(impacted) >= 4 else "MEDIUM" if len(impacted) >= 2 else "LOW"
        return {
            "origin_node": self.nodes[node_id].name,
            "origin_id": node_id,
            "impact_count": len(impacted),
            "severity_assessment": severity,
            "impacted_components": list(impacted.values())
        }

    def trace_upstream_root_cause_candidates(self, node_id: str, max_depth: int = 3) -> List[Dict[str, Any]]:
        """
        Calculates upstream dependency tree: If this node is alerting/degraded,
        what underlying services, databases, or hosts might be causing it?
        """
        if node_id not in self.nodes:
            return []

        candidates = []
        queue: List[Tuple[str, int, str]] = [(node_id, 0, "Symptom")]
        visited: Set[str] = {node_id}

        while queue:
            curr_id, depth, rel_path = queue.pop(0)
            if depth > 0:
                node = self.nodes[curr_id]
                candidates.append({
                    "id": node.id,
                    "name": node.name,
                    "ci_type": node.ci_type,
                    "status": node.status,
                    "depth": depth,
                    "dependency_chain": rel_path,
                    "is_anomalous": node.status in ("CRITICAL", "WARNING", "DEGRADED")
                })

            if depth < max_depth:
                # Downstream services that this node depends on (out_edges)
                for edge in self.out_edges.get(curr_id, []):
                    if edge.target not in visited:
                        visited.add(edge.target)
                        queue.append((edge.target, depth + 1, f"{rel_path} -> ({edge.relation}) -> {edge.target}"))

        # Sort candidates: anomalous ones first, then by depth
        candidates.sort(key=lambda x: (not x["is_anomalous"], x["depth"]))
        return candidates

    def get_subgraph_context(self, node_id: str) -> Dict[str, Any]:
        """Extracts 1-hop subgraph around a node for GraphRAG prompt ingestion"""
        if node_id not in self.nodes:
            return {}

        sub_nodes = {node_id: self.nodes[node_id]}
        sub_edges = []

        for edge in self.out_edges.get(node_id, []):
            sub_nodes[edge.target] = self.nodes[edge.target]
            sub_edges.append({"source": edge.source, "target": edge.target, "relation": edge.relation})

        for edge in self.in_edges.get(node_id, []):
            sub_nodes[edge.source] = self.nodes[edge.source]
            sub_edges.append({"source": edge.source, "target": edge.target, "relation": edge.relation})

        return {
            "center_node": self.nodes[node_id].name,
            "total_nodes": len(sub_nodes),
            "nodes": [{"id": n.id, "name": n.name, "type": n.ci_type, "status": n.status} for n in sub_nodes.values()],
            "edges": sub_edges
        }

    def export_graph_json(self) -> Dict[str, Any]:
        all_edges = []
        for src, edges in self.out_edges.items():
            for e in edges:
                all_edges.append({"source": e.source, "target": e.target, "relation": e.relation})

        return {
            "nodes": [{"id": n.id, "name": n.name, "type": n.ci_type, "status": n.status, "metadata": n.metadata} for n in self.nodes.values()],
            "edges": all_edges
        }


def build_sample_enterprise_topology() -> CMDBTopologyGraph:
    """
    Constructs a realistic Cloud-Native Microservices & Infrastructure Topology:
    Edge Ingress -> Payment Gateway Service -> Order DB & Redis & Payment Pods -> K8s Nodes
    """
    g = CMDBTopologyGraph()

    # Network / Ingress
    g.add_node(CINode(id="ing-01", name="k8s-ingress-nginx", ci_type="INGRESS", status="HEALTHY", metadata={"cluster": "prod-k8s-01"}))

    # Core Microservices
    g.add_node(CINode(id="svc-order", name="order-api-service", ci_type="SERVICE", status="DEGRADED", metadata={"team": "order-platform"}))
    g.add_node(CINode(id="svc-payment", name="payment-gateway-service", ci_type="SERVICE", status="CRITICAL", metadata={"team": "fintech"}))
    g.add_node(CINode(id="svc-inventory", name="inventory-service", ci_type="SERVICE", status="HEALTHY", metadata={"team": "warehouse"}))
    g.add_node(CINode(id="svc-auth", name="auth-sso-service", ci_type="SERVICE", status="HEALTHY", metadata={"team": "security"}))

    # Pods
    g.add_node(CINode(id="pod-pay-01", name="payment-pod-7df9f8-1", ci_type="POD", status="CRITICAL", metadata={"restart_count": 14, "oom": False}))
    g.add_node(CINode(id="pod-pay-02", name="payment-pod-7df9f8-2", ci_type="POD", status="CRITICAL", metadata={"restart_count": 12, "oom": False}))

    # Databases & Caches
    g.add_node(CINode(id="db-postgres-pay", name="rds-postgres-payment-primary", ci_type="DATABASE", status="CRITICAL", metadata={"max_connections": 500, "current_connections": 500, "engine": "PostgreSQL 15"}))
    g.add_node(CINode(id="cache-redis-01", name="redis-cluster-cache", ci_type="DATABASE", status="HEALTHY", metadata={"hit_rate": "98.2%"}))

    # Infrastructure Hosts
    g.add_node(CINode(id="host-k8s-worker-1", name="node-prod-worker-k8s-01", ci_type="HOST", status="HEALTHY", metadata={"ip": "10.200.1.11", "cpu_usage": "48%"}))
    g.add_node(CINode(id="host-k8s-worker-2", name="node-prod-worker-k8s-02", ci_type="HOST", status="HEALTHY", metadata={"ip": "10.200.1.12", "cpu_usage": "89%"}))

    # Relationships
    g.add_edge("ing-01", "svc-order", "ROUTED_BY")
    g.add_edge("ing-01", "svc-payment", "ROUTED_BY")

    g.add_edge("svc-order", "svc-payment", "DEPENDS_ON")
    g.add_edge("svc-order", "svc-inventory", "DEPENDS_ON")
    g.add_edge("svc-payment", "svc-auth", "DEPENDS_ON")

    g.add_edge("svc-payment", "pod-pay-01", "DEPLOYS_ON")
    g.add_edge("svc-payment", "pod-pay-02", "DEPLOYS_ON")

    g.add_edge("pod-pay-01", "host-k8s-worker-1", "HOSTED_ON")
    g.add_edge("pod-pay-02", "host-k8s-worker-2", "HOSTED_ON")

    g.add_edge("pod-pay-01", "db-postgres-pay", "CONNECTS_TO")
    g.add_edge("pod-pay-02", "db-postgres-pay", "CONNECTS_TO")
    g.add_edge("pod-pay-01", "cache-redis-01", "CONNECTS_TO")

    return g
