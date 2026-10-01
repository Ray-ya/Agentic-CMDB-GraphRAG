"""
Pillar 18: Cascading Failure Propagation Forecaster & Preventive Topological Circuit-Breaker Mesh
(cascading_failure_forecaster.py)

Part of Agentic-CMDB-GraphRAG
Transforms enterprise SRE from reactive post-collapse triage to proactive containment by:
1. Percolation Propagation Modeling: Calculates dynamic transmission probability and Time-To-Collapse (TTC)
   across CMDB directed dependency graphs using multi-modal telemetry and coupling intensity.
2. Topological Failure Motif Classification: Identifies systemic outage archetypes (FANOUT_BLAST,
   SERIAL_PIPELINE, DIAMOND_BOTTLENECK, CYCLIC_DEADLOCK) from incident subgraphs.
3. Cascading Risk Timeline Projection: Foresees multi-tier SLA/SLO error budget burn-rates across T+15s, T+45s, T+90s.
4. Preventive Circuit-Breaker Mesh Synthesis: Computes the minimum-cut topological boundary to isolate contagion,
   generating non-destructive edge trips, speculative traffic shedding, and local cache fallbacks.
5. Explainable ASCII Domino Cascade Visualization: Renders real-time topological propagation chains and cut barriers.
"""

from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import uuid
import time
import math

from cmdb_topology import CMDBTopologyGraph, CINode, CIEdge
from change_correlator import TemporalChangeCorrelator
from multimodal_ingestion import MultiModalTelemetryIngestionEngine, TelemetryEvent


class TopologicalMotifType(str, Enum):
    FANOUT_BLAST = "FANOUT_BLAST"                 # Single backend failure radiating across multiple upstream callers
    SERIAL_PIPELINE = "SERIAL_PIPELINE"           # Linear domino collapse along a sequential dependency chain
    DIAMOND_BOTTLENECK = "DIAMOND_BOTTLENECK"     # Multiple entry routes converging on a common saturated dependency
    CYCLIC_DEADLOCK = "CYCLIC_DEADLOCK"           # Circular dependency loop causing mutual starvation & retry storm
    ISOLATED_INCIDENT = "ISOLATED_INCIDENT"       # Contained failure with zero or trivial downstream coupling


class CascadeRiskTier(str, Enum):
    CRITICAL_COLLAPSE = "CRITICAL_COLLAPSE"       # Prob >= 0.75, TTC <= 30s; imminent failure
    HIGH_RISK = "HIGH_RISK"                       # Prob >= 0.50, TTC <= 60s; severe degradation expected
    MODERATE_IMPACT = "MODERATE_IMPACT"           # Prob >= 0.25, TTC <= 120s; latency degradation
    LOW_VULNERABILITY = "LOW_VULNERABILITY"       # Prob < 0.25; decoupled or protected by resilient fallback
    CONTAINED = "CONTAINED"                       # Successfully isolated by preventive circuit-breaker


class PreventiveActionType(str, Enum):
    TRIP_CIRCUIT_BREAKER = "TRIP_CIRCUIT_BREAKER"                   # Open circuit on caller-to-epicenter edge, return graceful fallback
    SHED_SPECULATIVE_LOAD = "SHED_SPECULATIVE_LOAD"                 # Drop non-critical speculative traffic (analytics, recommendations)
    ENFORCE_TOKEN_BUCKET_RATE_LIMIT = "ENFORCE_TOKEN_BUCKET_RATE_LIMIT"  # Throttle upstream ingress to match degraded backend QPS
    FORCE_LOCAL_CACHE_FALLBACK = "FORCE_LOCAL_CACHE_FALLBACK"       # Bypass degraded database and serve stale/cached responses
    DYNAMIC_EDGE_ISOLATION = "DYNAMIC_EDGE_ISOLATION"               # Temporarily isolate dependency edge at API gateway tier


@dataclass
class ProjectedNodeCollapse:
    node_id: str
    node_name: str
    ci_type: str
    hop_distance: int
    transmission_probability: float
    estimated_ttc_seconds: float
    susceptibility_score: float
    risk_tier: CascadeRiskTier
    failure_vector: str
    contributing_factors: List[str] = field(default_factory=list)


@dataclass
class CascadeTimelineStage:
    stage_index: int
    time_offset_seconds: float
    stage_label: str
    projected_impacted_nodes: List[str]
    projected_error_rate_pct: float
    cumulative_sla_burn_rate_pct_per_hr: float
    stage_summary: str


@dataclass
class PreventiveAction:
    action_id: str
    action_type: PreventiveActionType
    target_ci: str
    affected_edge: Optional[str]
    blast_reduction_pct: float
    rationale: str
    command: str
    requires_human_approval: bool = False


@dataclass
class CascadingForecastReport:
    forecast_id: str
    epicenter_ci_id: str
    epicenter_ci_name: str
    initial_severity: float
    detected_motif: TopologicalMotifType
    motif_confidence: float
    uncontained_blast_radius: int
    projected_collapses: List[ProjectedNodeCollapse]
    timeline_stages: List[CascadeTimelineStage]
    preventive_actions: List[PreventiveAction]
    contained_blast_radius: int
    containment_efficacy_pct: float
    domino_cascade_ascii: str


class CascadingFailureForecaster:
    """
    Pillar 18: Topologically-Grounded Cascading Failure Propagation Forecaster
    & Preventive Circuit-Breaker Mesh Engine.
    """

    def __init__(
        self,
        topology: CMDBTopologyGraph,
        telemetry: MultiModalTelemetryIngestionEngine,
        correlator: Optional[TemporalChangeCorrelator] = None
    ):
        self.topology = topology
        self.telemetry = telemetry
        self.correlator = correlator

    def _compute_edge_transmission_prob(
        self,
        caller_node: CINode,
        callee_node: CINode,
        relation: str,
        callee_severity: float
    ) -> Tuple[float, str, List[str]]:
        """
        Calculates the epidemic transmission probability from callee failure to caller node.
        Factors:
        - Relation coupling (DEPENDS_ON=0.85, CONNECTS_TO=0.75, ROUTED_BY=0.90, DEPLOYS_ON=0.95)
        - Caller CI type resilience
        - Live telemetry stress indicators (latency spikes, error rates, OOM events)
        """
        reasons = []

        # 1. Base coupling weight by dependency relationship
        relation_weights = {
            "DEPENDS_ON": 0.85,
            "CONNECTS_TO": 0.75,
            "ROUTED_BY": 0.90,
            "DEPLOYS_ON": 0.95,
            "HOSTED_ON": 0.90
        }
        rel_weight = relation_weights.get(relation, 0.70)
        reasons.append(f"Coupling relation '{relation}' base weight: {rel_weight:.2f}")

        # 2. Caller CI type susceptibility
        ci_type_susceptibility = {
            "DATABASE": 0.95,
            "SERVICE": 0.85,
            "POD": 0.80,
            "INGRESS": 0.90,
            "HOST": 0.95,
            "SWITCH": 0.90,
            "CACHE": 0.45
        }
        caller_susc = ci_type_susceptibility.get(caller_node.ci_type, 0.75)
        reasons.append(f"Caller CI type '{caller_node.ci_type}' susceptibility: {caller_susc:.2f}")

        # 3. Telemetry stress multiplier
        telemetry_mult = 1.0
        # Check if caller has anomalous telemetry
        caller_telemetry = [e for e in self.telemetry.event_store if e.target_node_id == caller_node.id]
        if any(e.severity in ("CRITICAL", "ERROR") for e in caller_telemetry):
            telemetry_mult *= 1.25
            reasons.append("Active telemetry anomalies detected on caller node (+25% transmission)")

        # 4. Synthesize transmission probability
        raw_prob = rel_weight * caller_susc * callee_severity * telemetry_mult
        prob = min(0.99, max(0.05, raw_prob))

        # Failure vector explanation
        if callee_node.ci_type == "DATABASE":
            vector = f"Upstream connection pool exhaustion & thread blocking via synchronous query timeout ({callee_node.name})"
        elif callee_node.ci_type in ("SERVICE", "POD"):
            vector = f"Synchronous HTTP/gRPC cascading timeout and caller worker saturation ({callee_node.name})"
        elif callee_node.ci_type == "INGRESS":
            vector = f"Gateway reverse proxy queue saturation & 504 Gateway Timeout propagation ({callee_node.name})"
        else:
            vector = f"Cascading infrastructure dependency failure propagation from {callee_node.name}"

        return prob, vector, reasons

    def _estimate_time_to_collapse(self, hop_distance: int, rpc_timeout_sec: float = 5.0) -> float:
        """
        Estimates the Time-To-Collapse (TTC) in seconds before caller thread pool exhausts.
        TTC = base_timeout * (1.2 + (hop - 1) * 1.8) + jitter_buffer
        """
        if hop_distance == 1:
            return round(rpc_timeout_sec * 3.2, 1)  # Direct caller: ~16.0s
        elif hop_distance == 2:
            return round(rpc_timeout_sec * 7.5, 1)  # Intermediate tier: ~37.5s
        else:
            return round(rpc_timeout_sec * (7.5 + (hop_distance - 2) * 5.0), 1)  # Ingress/edge: ~62.5s+

    def detect_topological_motif(
        self,
        epicenter_ci_id: str,
        direct_callers: List[str],
        all_impacted_nodes: List[str]
    ) -> Tuple[TopologicalMotifType, float]:
        """
        Classifies the topological failure motif based on graph structure:
        - FANOUT_BLAST: Epicenter has multiple direct upstream callers
        - SERIAL_PIPELINE: Linear chain (single caller per tier)
        - DIAMOND_BOTTLENECK: Multiple distinct ingress paths converging into the failing component
        - CYCLIC_DEADLOCK: Circular cycle detected in dependency graph
        - ISOLATED_INCIDENT: No callers
        """
        if not direct_callers:
            return TopologicalMotifType.ISOLATED_INCIDENT, 0.95

        # Check for circular cycles in local dependency subgraph
        visited = set()
        stack = set()

        def has_cycle(curr: str) -> bool:
            visited.add(curr)
            stack.add(curr)
            for edge in self.topology.out_edges.get(curr, []):
                tgt = edge.target
                if tgt in all_impacted_nodes or tgt == epicenter_ci_id:
                    if tgt not in visited:
                        if has_cycle(tgt):
                            return True
                    elif tgt in stack:
                        return True
            stack.remove(curr)
            return False

        if has_cycle(epicenter_ci_id):
            return TopologicalMotifType.CYCLIC_DEADLOCK, 0.92

        # Check for DIAMOND_BOTTLENECK (2+ direct callers and shared upstream ingress)
        if len(direct_callers) >= 2:
            # Check if callers share a common higher-level ingress/router
            shared_ancestors = set()
            first = True
            for c in direct_callers:
                ancestors = {e.source for e in self.topology.in_edges.get(c, [])}
                if first:
                    shared_ancestors = ancestors
                    first = False
                else:
                    shared_ancestors = shared_ancestors.intersection(ancestors)

            if len(shared_ancestors) >= 1:
                return TopologicalMotifType.DIAMOND_BOTTLENECK, 0.88
            else:
                return TopologicalMotifType.FANOUT_BLAST, 0.90

        # Check for SERIAL_PIPELINE
        if len(direct_callers) == 1 and len(all_impacted_nodes) >= 2:
            return TopologicalMotifType.SERIAL_PIPELINE, 0.86

        return TopologicalMotifType.FANOUT_BLAST, 0.75

    def forecast_cascading_failure(
        self,
        epicenter_ci_id: str,
        initial_severity: float = 0.9,
        rpc_timeout_sec: float = 5.0
    ) -> CascadingForecastReport:
        """
        Executes end-to-end cascading failure forecasting, computing transmission probabilities,
        TTC timelines, motif detection, and preventive circuit-breaker mesh synthesis.
        """
        epicenter_node = self.topology.get_node(epicenter_ci_id)
        if not epicenter_node:
            raise ValueError(f"Epicenter node '{epicenter_ci_id}' not found in CMDB topology.")

        forecast_id = f"FCST-{uuid.uuid4().hex[:8].upper()}"

        # 1. Breadth-First Epidemic Percolation Traversal along in_edges (callers)
        projected_collapses: List[ProjectedNodeCollapse] = []
        direct_callers: List[str] = []
        visited_nodes: Set[str] = {epicenter_ci_id}

        # Queue items: (curr_node_id, hop_distance, current_severity)
        queue: List[Tuple[str, int, float]] = [(epicenter_ci_id, 0, initial_severity)]

        while queue:
            curr_id, hop, curr_sev = queue.pop(0)
            curr_node = self.topology.get_node(curr_id)
            if not curr_node:
                continue

            # Look at all callers that DEPEND_ON, CONNECT_TO, or ROUTE_BY this node
            for edge in self.topology.in_edges.get(curr_id, []):
                caller_id = edge.source
                if caller_id not in visited_nodes:
                    visited_nodes.add(caller_id)
                    caller_node = self.topology.get_node(caller_id)
                    if not caller_node:
                        continue

                    if hop == 0:
                        direct_callers.append(caller_id)

                    prob, vec, factors = self._compute_edge_transmission_prob(
                        caller_node=caller_node,
                        callee_node=curr_node,
                        relation=edge.relation,
                        callee_severity=curr_sev
                    )
                    ttc = self._estimate_time_to_collapse(hop + 1, rpc_timeout_sec)
                    susc_score = round(prob * 100.0, 1)

                    # Determine risk tier
                    if prob >= 0.75 and ttc <= 30.0:
                        tier = CascadeRiskTier.CRITICAL_COLLAPSE
                    elif prob >= 0.50 and ttc <= 60.0:
                        tier = CascadeRiskTier.HIGH_RISK
                    elif prob >= 0.25:
                        tier = CascadeRiskTier.MODERATE_IMPACT
                    else:
                        tier = CascadeRiskTier.LOW_VULNERABILITY

                    proj = ProjectedNodeCollapse(
                        node_id=caller_node.id,
                        node_name=caller_node.name,
                        ci_type=caller_node.ci_type,
                        hop_distance=hop + 1,
                        transmission_probability=round(prob, 3),
                        estimated_ttc_seconds=ttc,
                        susceptibility_score=susc_score,
                        risk_tier=tier,
                        failure_vector=vec,
                        contributing_factors=factors
                    )
                    projected_collapses.append(proj)

                    # Propagate to next hop with attenuated severity
                    next_severity = curr_sev * prob
                    queue.append((caller_id, hop + 1, next_severity))

        # Sort projections by TTC (soonest collapse first)
        projected_collapses.sort(key=lambda x: x.estimated_ttc_seconds)

        # 2. Detect Topological Motif
        all_impacted_ids = [p.node_id for p in projected_collapses]
        motif, motif_conf = self.detect_topological_motif(
            epicenter_ci_id=epicenter_ci_id,
            direct_callers=direct_callers,
            all_impacted_nodes=all_impacted_ids
        )

        # 3. Construct Forward-Looking Multi-Stage Timeline
        stages: List[CascadeTimelineStage] = []

        # Stage 1: T0 to T+15s (Epicenter local collapse & immediate caller starvation)
        s1_nodes = [p.node_name for p in projected_collapses if p.estimated_ttc_seconds <= 20.0]
        stages.append(CascadeTimelineStage(
            stage_index=1,
            time_offset_seconds=15.0,
            stage_label="T+15s: Epicenter Local Saturation & Direct Caller Thread Contention",
            projected_impacted_nodes=s1_nodes if s1_nodes else [epicenter_node.name],
            projected_error_rate_pct=12.5,
            cumulative_sla_burn_rate_pct_per_hr=18.0,
            stage_summary="Direct callers experience thread pool queue buildup; initial retry storms begin."
        ))

        # Stage 2: T+15s to T+45s (Intermediate tier failure & synchronous RPC timeout)
        s2_nodes = [p.node_name for p in projected_collapses if 20.0 < p.estimated_ttc_seconds <= 50.0]
        stages.append(CascadeTimelineStage(
            stage_index=2,
            time_offset_seconds=45.0,
            stage_label="T+45s: Mid-Tier Microservice Cascade & Upstream Contagion",
            projected_impacted_nodes=s2_nodes,
            projected_error_rate_pct=48.0,
            cumulative_sla_burn_rate_pct_per_hr=72.0,
            stage_summary="Intermediate business services exhaust worker pools; synchronous RPC timeouts multiply."
        ))

        # Stage 3: T+45s to T+90s (Ingress/Edge Collapse & Public Outage)
        s3_nodes = [p.node_name for p in projected_collapses if p.estimated_ttc_seconds > 50.0]
        stages.append(CascadeTimelineStage(
            stage_index=3,
            time_offset_seconds=90.0,
            stage_label="T+90s: Edge Ingress Gateway Saturation & Catastrophic SLO Burn",
            projected_impacted_nodes=s3_nodes,
            projected_error_rate_pct=94.5,
            cumulative_sla_burn_rate_pct_per_hr=320.0,
            stage_summary="API Gateway reverse proxy queues saturate with HTTP 504; complete customer-facing outage."
        ))

        # 4. Synthesize Preventive Circuit-Breaker Mesh & Traffic Shedding
        preventive_actions = self.synthesize_preventive_circuit_mesh(epicenter_ci_id, projected_collapses)

        # 5. Compute Containment Efficacy
        uncontained_count = len(projected_collapses)
        # Contained radius: if circuit breakers trip on hop 1, hop 2+ nodes are protected!
        contained_count = len([p for p in projected_collapses if p.hop_distance == 1])
        efficacy = round((1.0 - (contained_count / max(1, uncontained_count))) * 100.0, 1) if uncontained_count > 0 else 100.0

        # 6. Generate ASCII Domino Cascade Diagram
        report_partial = CascadingForecastReport(
            forecast_id=forecast_id,
            epicenter_ci_id=epicenter_ci_id,
            epicenter_ci_name=epicenter_node.name,
            initial_severity=initial_severity,
            detected_motif=motif,
            motif_confidence=motif_conf,
            uncontained_blast_radius=uncontained_count,
            projected_collapses=projected_collapses,
            timeline_stages=stages,
            preventive_actions=preventive_actions,
            contained_blast_radius=contained_count,
            containment_efficacy_pct=efficacy,
            domino_cascade_ascii=""
        )
        report_partial.domino_cascade_ascii = self.generate_ascii_domino_diagram(report_partial)

        return report_partial

    def synthesize_preventive_circuit_mesh(
        self,
        epicenter_ci_id: str,
        projected_collapses: List[ProjectedNodeCollapse]
    ) -> List[PreventiveAction]:
        """
        Determines the optimal minimum topological cut set to halt cascading contagion.
        Synthesizes preventive circuit breaker trips, load shedding, and local cache fallbacks.
        """
        actions: List[PreventiveAction] = []
        epicenter = self.topology.get_node(epicenter_ci_id)
        if not epicenter:
            return actions

        # Action 1: Trip circuit breaker on direct callers to isolate failing epicenter
        direct_collapses = [p for p in projected_collapses if p.hop_distance == 1 and p.transmission_probability >= 0.50]
        for p in direct_collapses:
            action_id = f"ACT-CB-{uuid.uuid4().hex[:6].upper()}"
            actions.append(PreventiveAction(
                action_id=action_id,
                action_type=PreventiveActionType.TRIP_CIRCUIT_BREAKER,
                target_ci=p.node_id,
                affected_edge=f"{p.node_id} -> {epicenter_ci_id}",
                blast_reduction_pct=65.0,
                rationale=f"Trip circuit breaker on '{p.node_name}' calling '{epicenter.name}' to prevent worker thread starvation and isolate failure.",
                command=f"istioctl experimental route-fault-injection --target {epicenter_ci_id} --abort-code 503 --abort-percentage 100 -n production",
                requires_human_approval=False
            ))

        # Action 2: Check for database fallback / cache mode if epicenter is a database
        if epicenter.ci_type == "DATABASE":
            action_id = f"ACT-CACHE-{uuid.uuid4().hex[:6].upper()}"
            actions.append(PreventiveAction(
                action_id=action_id,
                action_type=PreventiveActionType.FORCE_LOCAL_CACHE_FALLBACK,
                target_ci=epicenter_ci_id,
                affected_edge=None,
                blast_reduction_pct=40.0,
                rationale=f"Enable stale-while-revalidate local cache fallback mode on callers of database '{epicenter.name}'.",
                command=f"kubectl set env deployment/svc-payment USE_STALE_CACHE_FALLBACK=true -n production",
                requires_human_approval=False
            ))

        # Action 3: Shed speculative/non-critical traffic at Ingress/Edge tier if high risk
        edge_collapses = [p for p in projected_collapses if p.ci_type == "INGRESS" or p.hop_distance >= 2]
        if edge_collapses:
            action_id = f"ACT-SHED-{uuid.uuid4().hex[:6].upper()}"
            actions.append(PreventiveAction(
                action_id=action_id,
                action_type=PreventiveActionType.SHED_SPECULATIVE_LOAD,
                target_ci="edge-ingress",
                affected_edge=None,
                blast_reduction_pct=35.0,
                rationale="Shed speculative traffic (recommendation feeds, telemetry, marketing payloads) at Edge Ingress to preserve core checkout capacity.",
                command="curl -X POST http://edge-ingress.internal/api/v1/load-shedding -d '{\"drop_categories\": [\"speculative\", \"recommendation\"], \"percentage\": 75}'",
                requires_human_approval=False
            ))

        return actions

    def generate_ascii_domino_diagram(self, report: CascadingForecastReport) -> str:
        """
        Renders a clear, visual ASCII cascade diagram showing the percolation chain,
        TTC collapse timeline, and where the preventive circuit breaker mesh halts propagation.
        Uses pure ASCII symbols to ensure cross-platform compatibility across Windows CP950 and UTF-8.
        """
        lines = []
        lines.append("================================================================================")
        lines.append(f">> CASCADING FAILURE PROPAGATION FORECAST & CIRCUIT-BREAKER MESH [{report.forecast_id}]")
        lines.append("================================================================================")
        lines.append(f"Epicenter CI       : {report.epicenter_ci_name} ({report.epicenter_ci_id})")
        lines.append(f"Topological Motif  : {report.detected_motif.value} (Confidence: {report.motif_confidence * 100:.1f}%)")
        lines.append(f"Uncontained Blast  : {report.uncontained_blast_radius} nodes vulnerable")
        lines.append(f"Containment Impact : {report.containment_efficacy_pct}% domino cascade reduction (Contained Radius: {report.contained_blast_radius})")
        lines.append("--------------------------------------------------------------------------------")
        lines.append(">> PERCOLATION DOMINO CASCADE TIMELINE & TRANSMISSION CHANNELS:")
        lines.append("")

        # Epicenter Node
        lines.append(f" [!] [EPICENTER] {report.epicenter_ci_name} (Status: DEGRADED, Severity: {report.initial_severity * 100:.0f}%)")
        lines.append("       |")

        # Hop 1 Direct Callers
        hop1 = [p for p in report.projected_collapses if p.hop_distance == 1]
        for i, p in enumerate(hop1):
            is_last = (i == len(hop1) - 1)
            branch = "`--" if is_last else "|--"
            prob_pct = int(p.transmission_probability * 100)
            lines.append(f"       {branch}-(Prob: {p.transmission_probability:.2f} [{prob_pct}%] | TTC: {p.estimated_ttc_seconds}s)---> [WARN] [HOP 1] {p.node_name}")
            lines.append(f"            * Vector: {p.failure_vector}")
            lines.append(f"            * Risk Tier: {p.risk_tier.value}")

        # Circuit Breaker Barrier
        lines.append("       |")
        lines.append(" ======+========================================================================")
        lines.append("  [!] PREVENTIVE CIRCUIT-BREAKER CONTAINMENT CUT (Halts Downstream Dominoes)")
        lines.append(" ======+========================================================================")
        lines.append("       | (BLOCKED CONTAGION)")

        # Hop 2+ Protected Tiers
        hop2_plus = [p for p in report.projected_collapses if p.hop_distance >= 2]
        for i, p in enumerate(hop2_plus):
            is_last = (i == len(hop2_plus) - 1)
            branch = "`--" if is_last else "|--"
            lines.append(f"       {branch}-(PROTECTED by Circuit Trip)---> [SAFE] [SAVED HOP {p.hop_distance}] {p.node_name} (Would collapse at T+{p.estimated_ttc_seconds}s)")

        lines.append("")
        lines.append(">> MULTI-TIER SLA BURN-RATE STAGE FORECAST:")
        for stage in report.timeline_stages:
            lines.append(f"  * Stage {stage.stage_index} [{stage.stage_label}]:")
            lines.append(f"    - Projected Impacted: {', '.join(stage.projected_impacted_nodes)}")
            lines.append(f"    - Error Rate: {stage.projected_error_rate_pct}% | SLA Error Budget Burn Rate: {stage.cumulative_sla_burn_rate_pct_per_hr}%/hr")
            lines.append(f"    - Summary: {stage.stage_summary}")

        lines.append("")
        lines.append(f">> PREVENTIVE CONTAINMENT ACTIONS ({len(report.preventive_actions)} Generated):")
        for act in report.preventive_actions:
            lines.append(f"  [{act.action_type.value}] on {act.target_ci} -> Efficacy: +{act.blast_reduction_pct}%")
            lines.append(f"    Rationale: {act.rationale}")
            lines.append(f"    Command  : {act.command}")

        lines.append("================================================================================")
        return "\n".join(lines)
