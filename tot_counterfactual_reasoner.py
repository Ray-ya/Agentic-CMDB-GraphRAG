"""
Pillar 17: Multi-Hypothesis Tree-of-Thought (ToT) Counterfactual Reasoner & Bayesian Confidence Estimator
(tot_counterfactual_reasoner.py)

Part of Agentic-CMDB-GraphRAG
Eliminates confirmation bias and single-path premature convergence in autonomous SRE RCAs by:
1. Multi-Branch Hypothesis Space Generation: Explores competing failure modes across topological tiers.
2. Topological & Multi-Modal Corroboration: Verifies against CMDB dependency paths, OTel traces, and Prometheus metrics.
3. Counterfactual Falsification (Refutation Testing): Rigorously tests if observed evidence disproves a hypothesis.
4. Bayesian Posterior Probability Scoring: Updates prior probabilities with telemetry likelihoods.
5. Explainable Tree-of-Thought Decision Audit: Outputs a fully traceable reasoning tree for human review.
"""

from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import uuid
import time
import math

from cmdb_topology import CMDBTopologyGraph, CINode
from change_correlator import TemporalChangeCorrelator
from multimodal_ingestion import MultiModalTelemetryIngestionEngine, TelemetryEvent


class HypothesisCategory(str, Enum):
    DATABASE_EXHAUSTION = "DATABASE_EXHAUSTION"             # Connection pool saturation, locks, slow queries
    DEPLOYMENT_REGRESSION = "DEPLOYMENT_REGRESSION"         # Bad commit, canary failure, config drift
    RESOURCE_STARVATION = "RESOURCE_STARVATION"             # OOMKilled, CPU throttling, Pod eviction
    DEPENDENCY_TIMEOUT = "DEPENDENCY_TIMEOUT"               # Downstream/Upstream RPC failure, network split
    CACHE_DEGRADATION = "CACHE_DEGRADATION"                 # Redis cache miss storm, key eviction, memory limit


class HypothesisStatus(str, Enum):
    PENDING = "PENDING"
    CORROBORATED = "CORROBORATED"
    REFUTED = "REFUTED"
    ACCEPTED = "ACCEPTED"


@dataclass
class EvidenceItem:
    source: str          # "CMDB_TOPOLOGY", "PROMETHEUS", "OPENTELEMETRY", "K8S_EVENT", "CI_CD"
    evidence_type: str   # "CORROBORATING" | "CONTRADICTING"
    description: str
    weight: float = 1.0


@dataclass
class CounterfactualTest:
    test_name: str
    condition_checked: str
    result: str          # "PASSED" (hypothesis supported or neutral) | "REFUTED" (hypothesis contradicted)
    explanation: str


@dataclass
class HypothesisNode:
    hypothesis_id: str
    category: HypothesisCategory
    target_ci_id: str
    target_ci_name: str
    statement: str
    prior_probability: float
    corroborating_evidence: List[EvidenceItem] = field(default_factory=list)
    contradicting_evidence: List[EvidenceItem] = field(default_factory=list)
    counterfactual_tests: List[CounterfactualTest] = field(default_factory=list)
    is_falsified: bool = False
    falsification_reason: Optional[str] = None
    likelihood_score: float = 1.0
    posterior_probability: float = 0.0
    status: HypothesisStatus = HypothesisStatus.PENDING


@dataclass
class ToTReasoningReport:
    incident_id: str
    symptom_node_id: str
    symptom_node_name: str
    alert_name: str
    total_hypotheses_evaluated: int
    pruned_hypotheses_count: int
    winning_hypothesis: Optional[HypothesisNode]
    posterior_confidence_pct: float
    hypotheses: List[HypothesisNode]
    reasoning_tree_ascii: str
    recommended_mitigation: str
    timestamp: float = field(default_factory=time.time)


class ToTCounterfactualReasoner:
    """
    Multi-Hypothesis Tree-of-Thought (ToT) Counterfactual Reasoner.
    Applies scientific method: Formulate competing hypotheses -> Seek disconfirming evidence -> Compute Bayesian posterior.
    """

    def __init__(
        self,
        topology: CMDBTopologyGraph,
        telemetry_engine: MultiModalTelemetryIngestionEngine,
        change_correlator: TemporalChangeCorrelator
    ):
        self.topology = topology
        self.telemetry = telemetry_engine
        self.correlator = change_correlator

    def generate_competing_hypotheses(
        self,
        symptom_node_id: str,
        alert_name: str
    ) -> List[HypothesisNode]:
        """
        Explores the upstream topological dependency graph from the symptom node,
        synthesizes candidate failure nodes, and constructs competing hypotheses across domains.
        """
        symptom_node = self.topology.get_node(symptom_node_id)
        if not symptom_node:
            return []

        upstream_candidates = self.topology.trace_upstream_root_cause_candidates(symptom_node_id)

        hypotheses: List[HypothesisNode] = []
        counter = 1

        # Collect unique CIs along the path
        candidate_cis: List[CINode] = [symptom_node]
        for item in upstream_candidates:
            cid = item.get("id")
            cnode = self.topology.get_node(cid)
            if cnode and cnode.id not in [c.id for c in candidate_cis]:
                candidate_cis.append(cnode)

        # 1. Database Tier Hypotheses
        for ci in candidate_cis:
            if ci.ci_type == "DATABASE":
                hypotheses.append(HypothesisNode(
                    hypothesis_id=f"HYP-{counter:02d}",
                    category=HypothesisCategory.DATABASE_EXHAUSTION,
                    target_ci_id=ci.id,
                    target_ci_name=ci.name,
                    statement=f"Database connection pool exhaustion or query lock contention on {ci.name} ({ci.id})",
                    prior_probability=0.35
                ))
                counter += 1

        # 2. Application & Deployment Regression Hypotheses
        for ci in candidate_cis:
            if ci.ci_type in ["SERVICE", "POD"]:
                hypotheses.append(HypothesisNode(
                    hypothesis_id=f"HYP-{counter:02d}",
                    category=HypothesisCategory.DEPLOYMENT_REGRESSION,
                    target_ci_id=ci.id,
                    target_ci_name=ci.name,
                    statement=f"Recent deployment, configuration change, or code regression on {ci.name} ({ci.id})",
                    prior_probability=0.30
                ))
                counter += 1

        # 3. Infrastructure & Resource Starvation Hypotheses
        for ci in candidate_cis:
            if ci.ci_type in ["POD", "HOST"]:
                hypotheses.append(HypothesisNode(
                    hypothesis_id=f"HYP-{counter:02d}",
                    category=HypothesisCategory.RESOURCE_STARVATION,
                    target_ci_id=ci.id,
                    target_ci_name=ci.name,
                    statement=f"Pod OOMKilled, memory leak, or CPU throttling saturation on {ci.name} ({ci.id})",
                    prior_probability=0.20
                ))
                counter += 1

        # 4. Cache & Memory Layer Hypotheses
        for ci in candidate_cis:
            if "cache" in ci.name.lower() or "redis" in ci.id.lower():
                hypotheses.append(HypothesisNode(
                    hypothesis_id=f"HYP-{counter:02d}",
                    category=HypothesisCategory.CACHE_DEGRADATION,
                    target_ci_id=ci.id,
                    target_ci_name=ci.name,
                    statement=f"Cache stampede, high eviction rate, or key miss surge on {ci.name} ({ci.id})",
                    prior_probability=0.15
                ))
                counter += 1

        return hypotheses

    def evaluate_counterfactuals_and_evidence(
        self,
        hypothesis: HypothesisNode,
        symptom_node_id: str
    ) -> None:
        """
        Applies Karl Popper's falsification test and collects multi-source evidence:
        - Corroborating signals boost likelihood.
        - Hard contradictions immediately FALSIFY the hypothesis (is_falsified = True).
        """
        target_id = hypothesis.target_ci_id
        target_node = self.topology.get_node(target_id)
        if not target_node:
            hypothesis.is_falsified = True
            hypothesis.falsification_reason = f"Target CI '{target_id}' does not exist in CMDB."
            hypothesis.status = HypothesisStatus.REFUTED
            return

        # --- Test 1: Topological Reachability ---
        # If target_id != symptom_node_id, there must be a valid path between them
        if target_id != symptom_node_id:
            upstream_candidates = self.topology.trace_upstream_root_cause_candidates(symptom_node_id)
            upstream_ids = [item.get("id") for item in upstream_candidates]
            if target_id not in upstream_ids:
                hypothesis.is_falsified = True
                hypothesis.falsification_reason = f"Topological Disconnection: '{target_id}' has no dependency path to symptom '{symptom_node_id}'."
                hypothesis.status = HypothesisStatus.REFUTED
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Topological Reachability",
                    condition_checked=f"Exists path from {symptom_node_id} to {target_id}",
                    result="REFUTED",
                    explanation=hypothesis.falsification_reason
                ))
                return
            else:
                hypothesis.corroborating_evidence.append(EvidenceItem(
                    source="CMDB_TOPOLOGY",
                    evidence_type="CORROBORATING",
                    description=f"Direct/transitive dependency path verified from {symptom_node_id} to {target_id}.",
                    weight=1.2
                ))
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Topological Reachability",
                    condition_checked=f"Exists path from {symptom_node_id} to {target_id}",
                    result="PASSED",
                    explanation="Path confirmed via CMDB Directed Graph."
                ))

        # --- Test 2: Category-Specific Telemetry & Mutations ---

        if hypothesis.category == HypothesisCategory.DEPLOYMENT_REGRESSION:
            # Check CI/CD mutations within the last 1 hour
            mutations = self.correlator.correlate_events_for_node(target_id, window_seconds=3600)
            if not mutations:
                # Counterfactual: If deployment regression, a mutation MUST exist!
                hypothesis.is_falsified = True
                hypothesis.falsification_reason = f"No CI/CD deployment or config mutation found on '{target_id}' within the last 60 minutes."
                hypothesis.status = HypothesisStatus.REFUTED
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Mutation Temporality Check",
                    condition_checked="Recent deployment or mutation event in 60m window",
                    result="REFUTED",
                    explanation=hypothesis.falsification_reason
                ))
                return
            else:
                top_mutation = mutations[0]
                decay_score = top_mutation.get("decay_score", 1.0)
                change_event = top_mutation.get("event")
                hypothesis.corroborating_evidence.append(EvidenceItem(
                    source="CI_CD",
                    evidence_type="CORROBORATING",
                    description=f"Correlated {change_event.change_type} on {target_id} (Score: {decay_score:.3f}).",
                    weight=1.5 * decay_score
                ))
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Mutation Temporality Check",
                    condition_checked="Recent deployment or mutation event in 60m window",
                    result="PASSED",
                    explanation=f"Found recent {change_event.change_type} {top_mutation.get('age_minutes', 0):.1f}m ago."
                ))

        elif hypothesis.category == HypothesisCategory.DATABASE_EXHAUSTION:
            # Check multi-modal telemetry for DB errors or connection spikes
            db_telemetry = [e for e in self.telemetry.event_store if e.target_node_id == target_id]
            otel_spans = [e for e in self.telemetry.event_store if e.source == "OPENTELEMETRY" and e.target_node_id == target_id]
            prom_metrics = [e for e in self.telemetry.event_store if e.source == "PROMETHEUS" and e.target_node_id == target_id]

            has_corroborating_telemetry = False
            for span in otel_spans:
                latency = span.payload.get("duration_ms", 0.0)
                if latency > 1000.0 or span.severity in ["WARNING", "CRITICAL"]:
                    hypothesis.corroborating_evidence.append(EvidenceItem(
                        source="OPENTELEMETRY",
                        evidence_type="CORROBORATING",
                        description=f"OTel span latency {latency:.1f}ms exceeds threshold (Status: {span.payload.get('status_code', 'ERR')}).",
                        weight=2.0
                    ))
                    has_corroborating_telemetry = True

            for metric in prom_metrics:
                val = metric.payload.get("metric_value", 0.0)
                hypothesis.corroborating_evidence.append(EvidenceItem(
                    source="PROMETHEUS",
                    evidence_type="CORROBORATING",
                    description=f"Prometheus metric anomaly: {metric.event_name} = {val} ({metric.severity}).",
                    weight=1.8
                ))
                has_corroborating_telemetry = True

            # Check node status in CMDB
            if target_node.status in ["CRITICAL", "DEGRADED"]:
                hypothesis.corroborating_evidence.append(EvidenceItem(
                    source="CMDB_TOPOLOGY",
                    evidence_type="CORROBORATING",
                    description=f"CMDB CI status is {target_node.status}.",
                    weight=1.5
                ))
                has_corroborating_telemetry = True

            # Counterfactual: If DB has healthy status, 0 slow spans, and 0 metrics, downgrade
            if not has_corroborating_telemetry and target_node.status == "HEALTHY":
                hypothesis.is_falsified = True
                hypothesis.falsification_reason = f"Database '{target_id}' reports HEALTHY status with zero anomalous spans or metrics."
                hypothesis.status = HypothesisStatus.REFUTED
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Database Telemetry Degradation Check",
                    condition_checked="Evidence of DB saturation, high latency or errors",
                    result="REFUTED",
                    explanation=hypothesis.falsification_reason
                ))
                return
            else:
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Database Telemetry Degradation Check",
                    condition_checked="Evidence of DB saturation, high latency or errors",
                    result="PASSED",
                    explanation="Telemetry corroborates DB degradation."
                ))

        elif hypothesis.category == HypothesisCategory.RESOURCE_STARVATION:
            # Check for K8s OOMKilled / CrashLoopBackOff events
            k8s_events = [e for e in self.telemetry.event_store if e.source == "K8S_EVENT" and e.target_node_id == target_id]
            oom_events = [e for e in k8s_events if "OOM" in e.event_name or "Crash" in e.event_name]

            if not oom_events:
                hypothesis.is_falsified = True
                hypothesis.falsification_reason = f"No Kubernetes OOMKilled or CrashLoopBackOff events recorded for '{target_id}'."
                hypothesis.status = HypothesisStatus.REFUTED
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="K8s OOM & Eviction Check",
                    condition_checked="OOMKilled or CrashLoop events present in cluster log",
                    result="REFUTED",
                    explanation=hypothesis.falsification_reason
                ))
                return
            else:
                for oom in oom_events:
                    hypothesis.corroborating_evidence.append(EvidenceItem(
                        source="K8S_EVENT",
                        evidence_type="CORROBORATING",
                        description=f"K8s Event: {oom.event_name} ({oom.severity}) - {oom.payload.get('message', '')}",
                        weight=2.5
                    ))
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="K8s OOM & Eviction Check",
                    condition_checked="OOMKilled or CrashLoop events present in cluster log",
                    result="PASSED",
                    explanation=f"Confirmed {len(oom_events)} K8s resource starvation events."
                ))

        elif hypothesis.category == HypothesisCategory.CACHE_DEGRADATION:
            # Check for cache miss or latency
            cache_events = [e for e in self.telemetry.event_store if e.target_node_id == target_id]
            if not cache_events and target_node.status == "HEALTHY":
                hypothesis.is_falsified = True
                hypothesis.falsification_reason = f"Cache CI '{target_id}' has no anomalous telemetry and remains HEALTHY."
                hypothesis.status = HypothesisStatus.REFUTED
                hypothesis.counterfactual_tests.append(CounterfactualTest(
                    test_name="Cache Health & Hit-Ratio Check",
                    condition_checked="Anomalous cache telemetry or degraded state",
                    result="REFUTED",
                    explanation=hypothesis.falsification_reason
                ))
                return
            else:
                hypothesis.corroborating_evidence.append(EvidenceItem(
                    source="CMDB_TOPOLOGY",
                    evidence_type="CORROBORATING",
                    description=f"Cache node state: {target_node.status}.",
                    weight=1.0
                ))

        hypothesis.status = HypothesisStatus.CORROBORATED

    def compute_bayesian_posteriors(
        self,
        hypotheses: List[HypothesisNode]
    ) -> List[HypothesisNode]:
        """
        Calculates normalized Bayesian posterior probabilities:
        P(H_i | E) = ( P(E | H_i) * P(H_i) ) / Sum_j ( P(E | H_j) * P(H_j) )
        """
        unnormalized_scores: List[float] = []

        for h in hypotheses:
            if h.is_falsified or h.status == HypothesisStatus.REFUTED:
                h.likelihood_score = 0.0
                h.posterior_probability = 0.0
                unnormalized_scores.append(0.0)
                continue

            # Likelihood based on corroborating weights
            evidence_sum = sum(e.weight for e in h.corroborating_evidence)
            # Base likelihood formula with exponential scale
            likelihood = math.exp(min(evidence_sum, 5.0))
            h.likelihood_score = round(likelihood, 4)

            unnorm = h.prior_probability * likelihood
            unnormalized_scores.append(unnorm)

        total_unnormalized = sum(unnormalized_scores)

        for idx, h in enumerate(hypotheses):
            if total_unnormalized > 0 and not h.is_falsified:
                h.posterior_probability = round(unnormalized_scores[idx] / total_unnormalized, 4)
            else:
                h.posterior_probability = 0.0

        return hypotheses

    def build_ascii_tree(
        self,
        symptom_node_id: str,
        alert_name: str,
        hypotheses: List[HypothesisNode],
        winner: Optional[HypothesisNode]
    ) -> str:
        """
        Generates an explainable Tree-of-Thought ASCII visualization.
        """
        lines = []
        lines.append(f"Tree-of-Thought RCA Exploration: [{alert_name}] on '{symptom_node_id}'")
        lines.append(" │")

        for idx, h in enumerate(hypotheses):
            is_last = (idx == len(hypotheses) - 1)
            prefix = " └── " if is_last else " ├── "
            sub_prefix = "     " if is_last else " │   "

            status_icon = "👑 [WINNER]" if (winner and winner.hypothesis_id == h.hypothesis_id) else (
                "❌ [REFUTED]" if h.is_falsified else "🔍 [CANDIDATE]"
            )
            lines.append(f"{prefix}{status_icon} Branch {h.hypothesis_id}: {h.category.value} on {h.target_ci_id}")
            lines.append(f"{sub_prefix}├─ Statement: {h.statement}")
            lines.append(f"{sub_prefix}├─ Prior: {h.prior_probability:.2f} | Posterior: {h.posterior_probability * 100:.1f}%")

            if h.is_falsified:
                lines.append(f"{sub_prefix}└─ Counterfactual Falsification: {h.falsification_reason}")
            else:
                evidence_desc = "; ".join([e.description for e in h.corroborating_evidence[:2]])
                lines.append(f"{sub_prefix}└─ Corroborating Evidence: {evidence_desc}")

        return "\n".join(lines)

    def run_reasoning_tree(
        self,
        symptom_node_id: str,
        alert_name: str,
        incident_id: Optional[str] = None
    ) -> ToTReasoningReport:
        """
        Full end-to-end Tree-of-Thought Counterfactual Reasoning pipeline:
        1. Multi-branch Hypothesis Generation
        2. Counterfactual Testing & Falsification
        3. Bayesian Posterior Calculation
        4. Winner Selection & Mitigation Suggestion
        """
        inc_id = incident_id or f"INC-{uuid.uuid4().hex[:8].upper()}"
        symptom_node = self.topology.get_node(symptom_node_id)
        symptom_name = symptom_node.name if symptom_node else symptom_node_id

        # Step 1: Expand hypothesis space
        hypotheses = self.generate_competing_hypotheses(symptom_node_id, alert_name)

        # Step 2: Evaluate counterfactuals and corroborating evidence
        pruned_count = 0
        for h in hypotheses:
            self.evaluate_counterfactuals_and_evidence(h, symptom_node_id)
            if h.is_falsified:
                pruned_count += 1

        # Step 3: Compute Bayesian posterior probabilities
        hypotheses = self.compute_bayesian_posteriors(hypotheses)

        # Step 4: Select winning hypothesis (Maximum A Posteriori / MAP)
        surviving = [h for h in hypotheses if not h.is_falsified]
        surviving.sort(key=lambda x: x.posterior_probability, reverse=True)

        winner: Optional[HypothesisNode] = surviving[0] if surviving else None
        if winner:
            winner.status = HypothesisStatus.ACCEPTED

        # Mitigation formulation
        recommended_mitigation = "No conclusive root cause could be validated. Manual SRE triage required."
        if winner:
            if winner.category == HypothesisCategory.DATABASE_EXHAUSTION:
                recommended_mitigation = f"Terminate stuck long-running queries or recycle connection pool on {winner.target_ci_id} via 'SELECT pg_terminate_backend(pid)'."
            elif winner.category == HypothesisCategory.DEPLOYMENT_REGRESSION:
                recommended_mitigation = f"Perform automated rollback on recent deployment for {winner.target_ci_id} using 'helm rollback {winner.target_ci_id} 1'."
            elif winner.category == HypothesisCategory.RESOURCE_STARVATION:
                recommended_mitigation = f"Vertically scale pod memory limits or increase replica count on {winner.target_ci_id} via 'kubectl scale deployment/{winner.target_ci_id} --replicas=+2'."
            elif winner.category == HypothesisCategory.CACHE_DEGRADATION:
                recommended_mitigation = f"Flush stale cache partitions and scale Redis replica read-nodes for {winner.target_ci_id}."

        ascii_tree = self.build_ascii_tree(symptom_node_id, alert_name, hypotheses, winner)

        report = ToTReasoningReport(
            incident_id=inc_id,
            symptom_node_id=symptom_node_id,
            symptom_node_name=symptom_name,
            alert_name=alert_name,
            total_hypotheses_evaluated=len(hypotheses),
            pruned_hypotheses_count=pruned_count,
            winning_hypothesis=winner,
            posterior_confidence_pct=round((winner.posterior_probability * 100.0) if winner else 0.0, 1),
            hypotheses=hypotheses,
            reasoning_tree_ascii=ascii_tree,
            recommended_mitigation=recommended_mitigation
        )

        return report
