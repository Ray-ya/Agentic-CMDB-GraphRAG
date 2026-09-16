"""
Dual-Level GraphRAG Engine (dual_level_graphrag.py)
Inspired by LightRAG (HKUDS, EMNLP 2025):
Implements dual-level retrieval:
1. Low-Level Retrieval: Specific CI entity metrics, error logs, and immediate local edges.
2. High-Level Retrieval: Top-level service clusters, architectural blast radius, and global impact topology.
3. Temporal Correlation: Maps upstream dependency mutations within the event correlation window.
4. Multi-Modal Telemetry Integration: Correlates K8s events, Prometheus metrics, and OpenTelemetry spans.
"""

from typing import Dict, List, Any, Optional
from cmdb_topology import CMDBTopologyGraph
from knowledge_base import HybridKnowledgeBase
from change_correlator import TemporalChangeCorrelator, CorrelatedChange
from multimodal_ingestion import MultiModalTelemetryIngestionEngine
import time


class DualLevelGraphRAGEngine:
    """
    Synthesizes Topological Graph Traversal + Temporal Mutation Logs + Unstructured SRE Knowledge
    + Multi-Modal Distributed Telemetry Streams.
    Guarantees structural, temporal, and empirical telemetry grounding before LLM reasoning.
    """

    def __init__(
        self,
        topology: CMDBTopologyGraph,
        kb: HybridKnowledgeBase,
        correlator: Optional[TemporalChangeCorrelator] = None,
        telemetry_engine: Optional[MultiModalTelemetryIngestionEngine] = None
    ):
        self.topology = topology
        self.kb = kb
        self.correlator = correlator
        self.telemetry_engine = telemetry_engine

    def retrieve_low_level(self, alerting_node_id: str) -> Dict[str, Any]:
        """
        Low-Level Retrieval:
        Inspects the exact CI node, its runtime metrics/metadata, immediate 1-hop connections,
        directly matched runbook snippets, and live multimodal telemetry.
        """
        node = self.topology.get_node(alerting_node_id)
        if not node:
            return {"error": f"Node {alerting_node_id} not found"}

        local_subgraph = self.topology.get_subgraph_context(alerting_node_id)
        matched_runbooks = self.kb.search(
            query=f"{node.name} {node.ci_type} failure diagnostic",
            target_component=node.id,
            top_k=2
        )

        telemetry_events = []
        if self.telemetry_engine:
            telemetry_events = self.telemetry_engine.get_recent_telemetry_for_ci(alerting_node_id, max_events=5)

        return {
            "focus_entity": {
                "id": node.id,
                "name": node.name,
                "type": node.ci_type,
                "status": node.status,
                "runtime_metadata": node.metadata
            },
            "immediate_topology": local_subgraph,
            "entity_runbooks": matched_runbooks,
            "recent_telemetry": telemetry_events
        }

    def retrieve_high_level(self, alerting_node_id: str, alert_timestamp: Optional[float] = None) -> Dict[str, Any]:
        """
        High-Level Retrieval:
        Inspects full architectural blast radius, upstream root-cause candidates,
        temporal CI/CD change correlations, and historical cross-service postmortems.
        """
        blast_radius = self.topology.compute_blast_radius(alerting_node_id, max_depth=3)
        upstream_candidates = self.topology.trace_upstream_root_cause_candidates(alerting_node_id, max_depth=3)

        # Correlate changes across upstream candidates and symptom node
        correlated_changes = []
        if self.correlator:
            ts = alert_timestamp or time.time()
            changes = self.correlator.correlate_incident(
                alert_timestamp=ts,
                upstream_candidates=upstream_candidates,
                symptom_node_id=alerting_node_id
            )
            correlated_changes = [
                {
                    "change_id": c.change.change_id,
                    "node_id": c.change.node_id,
                    "change_type": c.change.change_type,
                    "author": c.change.author,
                    "minutes_before_alert": c.minutes_before_alert,
                    "correlation_score": c.correlation_score,
                    "evidence_statement": c.evidence_statement
                }
                for c in changes
            ]

        # Search for historical cross-service postmortems
        postmortems = self.kb.search(
            query=f"cascading outage timeout failure postmortem",
            target_component=alerting_node_id,
            top_k=2
        )

        return {
            "blast_radius_assessment": blast_radius,
            "upstream_dependency_path": upstream_candidates,
            "correlated_changes": correlated_changes,
            "historical_postmortems": postmortems
        }

    def synthesize_incident_context(self, alert_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Unified GraphRAG Synthesis:
        Combines Low-Level (Entity Precision + Telemetry) + High-Level (Global Topology & Temporal Changes)
        into a coherent prompt context for the RCA Agent.
        """
        node_id = alert_event.get("node_id", "")
        alert_name = alert_event.get("alert_name", "Unknown Alert")
        alert_severity = alert_event.get("severity", "WARNING")
        alert_timestamp = alert_event.get("timestamp", time.time())

        low_level_data = self.retrieve_low_level(node_id)
        high_level_data = self.retrieve_high_level(node_id, alert_timestamp=alert_timestamp)

        return {
            "alert_event": {
                "alert_name": alert_name,
                "severity": alert_severity,
                "target_node": node_id,
                "timestamp": alert_timestamp,
                "details": alert_event.get("details", {})
            },
            "dual_level_graphrag": {
                "low_level_inspection": low_level_data,
                "high_level_topology": high_level_data
            }
        }
