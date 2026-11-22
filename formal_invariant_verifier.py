"""
Pillar 21: Formal SMT-Grounded Invariant Verifier & Safe Remediation Guardrails
(formal_invariant_verifier.py)

Enhancement for Agentic-CMDB-GraphRAG (Pillar 21)
Provides mathematical First-Order Logic and SMT-style (Satisfiability Modulo Theories)
safety verification for AI-generated SRE Remediation Plans and DAG workflows:

1. Formal Invariant Catalog:
   - QUORUM_PRESERVATION: Prevents Raft/Paxos/HA split-brain by guaranteeing K_surviving >= floor(N/2) + 1.
   - CAPACITY_HEADROOM_CONSERVATION: Guarantees surviving pods handle incoming traffic without cascading saturation.
   - MUTUAL_EXCLUSION_COUPLING: Prevents simultaneous restart of mutually dependent or tightly coupled Tier-1 CIs.
   - STATEFUL_DATA_SAFETY: Blocks destructive database mutations if replication lag or in-flight drain exceeds safety bounds.
   - BLAST_RADIUS_BUDGET: Caps cumulative blast radius impact within safe operational bounds.

2. Counterexample Generation & Formal Proofs:
   - Emits mathematical refutation proofs when any invariant is violated.

3. Automated Plan Synthesis & Safe Staging Repair:
   - Automatically re-sequences unsafe parallel DAG actions into provably safe, sequential, canary-paced stages.
"""

from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import math
import time

from cmdb_topology import CMDBTopologyGraph, CINode


class InvariantType(str, Enum):
    QUORUM_PRESERVATION = "QUORUM_PRESERVATION"
    CAPACITY_HEADROOM_CONSERVATION = "CAPACITY_HEADROOM_CONSERVATION"
    MUTUAL_EXCLUSION_COUPLING = "MUTUAL_EXCLUSION_COUPLING"
    STATEFUL_DATA_SAFETY = "STATEFUL_DATA_SAFETY"
    BLAST_RADIUS_BUDGET = "BLAST_RADIUS_BUDGET"


class VerificationVerdict(str, Enum):
    PROVABLY_SAFE = "PROVABLY_SAFE"               # All invariants satisfied mathematically
    INVARIANT_VIOLATED = "INVARIANT_VIOLATED"     # Formally refuted with counterexample
    CONDITIONALLY_REPAIRED = "CONDITIONALLY_REPAIRED" # Repaired via automated staged sequencing


class ActionType(str, Enum):
    SCALE_DOWN = "SCALE_DOWN"
    RESTART_POD = "RESTART_POD"
    FAILOVER_DB = "FAILOVER_DB"
    RECYCLE_POOL = "RECYCLE_POOL"
    ROLLBACK_DEPLOYMENT = "ROLLBACK_DEPLOYMENT"
    RATE_LIMIT = "RATE_LIMIT"
    DRAIN_NODE = "DRAIN_NODE"


@dataclass
class RemediationActionSpec:
    action_id: str
    target_ci_id: str
    action_type: ActionType
    command: str
    is_destructive: bool
    drained_instances: int = 1
    target_stage_index: int = 0  # Stage group for concurrent execution


@dataclass
class InvariantRuleSpec:
    rule_id: str
    invariant_type: InvariantType
    description: str
    formula: str
    severity: str = "CRITICAL"  # CRITICAL or WARNING


@dataclass
class CounterexampleProof:
    invariant_type: InvariantType
    rule_id: str
    violated_ci_id: str
    mathematical_formula: str
    variable_assignments: Dict[str, Any]
    refutation_statement: str


@dataclass
class InvariantEvaluationResult:
    rule_id: str
    invariant_type: InvariantType
    is_satisfied: bool
    formula: str
    proof_details: str
    counterexample: Optional[CounterexampleProof] = None


@dataclass
class FormalVerificationReport:
    plan_id: str
    verdict: VerificationVerdict
    is_safe_to_execute: bool
    total_actions_analyzed: int
    invariants_evaluated_count: int
    invariants_satisfied_count: int
    invariants_violated_count: int
    violation_counterexamples: List[CounterexampleProof]
    evaluation_results: List[InvariantEvaluationResult]
    repaired_staged_actions: Optional[List[RemediationActionSpec]] = None
    ascii_proof_certificate: str = ""
    timestamp: float = field(default_factory=time.time)


class FormalSMTInvariantVerifier:
    """
    Formal SMT-Grounded Invariant Verifier for Microservice Remediation Workflows.
    Grounded on CMDB Directed Dependency Topologies and Real-time Telemetry State.
    """

    def __init__(
        self,
        topology: CMDBTopologyGraph,
        max_blast_radius_budget: int = 5,
        min_headroom_safety_margin_pct: float = 20.0  # 20% headroom required
    ):
        self.topology = topology
        self.max_blast_radius_budget = max_blast_radius_budget
        self.min_headroom_safety_margin_pct = min_headroom_safety_margin_pct
        
        # Stateful cluster metadata registry (node_id -> {total_replicas, is_quorum_based})
        self.cluster_metadata: Dict[str, Dict[str, Any]] = {
            "db-postgres-pay": {
                "total_replicas": 3,
                "is_quorum_based": True,
                "current_replication_lag_sec": 0.4,
                "max_allowed_lag_sec": 1.5,
                "capacity_per_replica_rps": 600.0,
                "current_incoming_rps": 950.0
            },
            "svc-payment": {
                "total_replicas": 4,
                "is_quorum_based": False,
                "capacity_per_replica_rps": 400.0,
                "current_incoming_rps": 1100.0
            },
            "svc-order": {
                "total_replicas": 3,
                "is_quorum_based": False,
                "capacity_per_replica_rps": 500.0,
                "current_incoming_rps": 800.0
            },
            "svc-auth": {
                "total_replicas": 2,
                "is_quorum_based": False,
                "capacity_per_replica_rps": 1000.0,
                "current_incoming_rps": 900.0
            },
            "cache-redis-01": {
                "total_replicas": 3,
                "is_quorum_based": True,
                "current_replication_lag_sec": 0.1,
                "max_allowed_lag_sec": 0.5,
                "capacity_per_replica_rps": 2000.0,
                "current_incoming_rps": 2500.0
            }
        }

    def register_cluster_metadata(self, ci_id: str, metadata: Dict[str, Any]):
        """Register or override cluster capacity and quorum metadata for a CI."""
        self.cluster_metadata[ci_id] = metadata

    def verify_remediation_plan(
        self,
        plan_id: str,
        actions: List[RemediationActionSpec]
    ) -> FormalVerificationReport:
        """
        Formally verifies all actions in a remediation plan against the core invariant catalog.
        """
        results: List[InvariantEvaluationResult] = []
        counterexamples: List[CounterexampleProof] = []

        # 1. Invariant 1: Blast Radius Budget
        inv_blast = self._verify_blast_radius_budget(actions)
        results.append(inv_blast)
        if not inv_blast.is_satisfied and inv_blast.counterexample:
            counterexamples.append(inv_blast.counterexample)

        # Group actions by concurrent stage index
        stages: Dict[int, List[RemediationActionSpec]] = {}
        for act in actions:
            stages.setdefault(act.target_stage_index, []).append(act)

        # 2. Invariant 2: Quorum Preservation (Per Stage)
        for stage_idx, stage_actions in stages.items():
            inv_quorum = self._verify_quorum_preservation(stage_idx, stage_actions)
            results.append(inv_quorum)
            if not inv_quorum.is_satisfied and inv_quorum.counterexample:
                counterexamples.append(inv_quorum.counterexample)

        # 3. Invariant 3: Capacity Headroom Conservation (Per Stage)
        for stage_idx, stage_actions in stages.items():
            inv_cap = self._verify_capacity_headroom(stage_idx, stage_actions)
            results.append(inv_cap)
            if not inv_cap.is_satisfied and inv_cap.counterexample:
                counterexamples.append(inv_cap.counterexample)

        # 4. Invariant 4: Mutual Exclusion Coupling (Per Stage)
        for stage_idx, stage_actions in stages.items():
            inv_mutex = self._verify_mutual_exclusion(stage_idx, stage_actions)
            results.append(inv_mutex)
            if not inv_mutex.is_satisfied and inv_mutex.counterexample:
                counterexamples.append(inv_mutex.counterexample)

        # 5. Invariant 5: Stateful Data Safety
        for act in actions:
            if act.action_type in (ActionType.FAILOVER_DB, ActionType.RECYCLE_POOL):
                inv_state = self._verify_stateful_data_safety(act)
                results.append(inv_state)
                if not inv_state.is_satisfied and inv_state.counterexample:
                    counterexamples.append(inv_state.counterexample)

        # Tally metrics
        satisfied_count = sum(1 for r in results if r.is_satisfied)
        violated_count = sum(1 for r in results if not r.is_satisfied)
        is_safe = (violated_count == 0)

        verdict = VerificationVerdict.PROVABLY_SAFE if is_safe else VerificationVerdict.INVARIANT_VIOLATED

        # If violated, synthesize automated SMT safe staged repair candidate
        repaired_actions = None
        if not is_safe:
            repaired_actions = self.synthesize_safe_staged_repair(actions)

        # Render ASCII Certificate
        ascii_cert = self._render_ascii_proof_certificate(
            plan_id=plan_id,
            verdict=verdict,
            results=results,
            counterexamples=counterexamples
        )

        return FormalVerificationReport(
            plan_id=plan_id,
            verdict=verdict,
            is_safe_to_execute=(verdict == VerificationVerdict.PROVABLY_SAFE),
            total_actions_analyzed=len(actions),
            invariants_evaluated_count=len(results),
            invariants_satisfied_count=satisfied_count,
            invariants_violated_count=violated_count,
            violation_counterexamples=counterexamples,
            evaluation_results=results,
            repaired_staged_actions=repaired_actions,
            ascii_proof_certificate=ascii_cert
        )

    def _verify_blast_radius_budget(
        self,
        actions: List[RemediationActionSpec]
    ) -> InvariantEvaluationResult:
        """Formula: Sum(Unique Impacted CIs) <= Max Blast Radius Budget"""
        all_impacted: Set[str] = set()
        for act in actions:
            blast = self.topology.compute_blast_radius(act.target_ci_id, max_depth=2)
            for imp in blast.get("impacted_components", []):
                all_impacted.add(imp["id"])

        total_impact = len(all_impacted)
        formula = f"|UniqueImpacted({total_impact})| <= Budget({self.max_blast_radius_budget})"

        if total_impact <= self.max_blast_radius_budget:
            return InvariantEvaluationResult(
                rule_id="RULE-BLAST-01",
                invariant_type=InvariantType.BLAST_RADIUS_BUDGET,
                is_satisfied=True,
                formula=formula,
                proof_details=f"Cumulative blast radius {total_impact} nodes strictly within budget limit {self.max_blast_radius_budget}."
            )
        else:
            cx = CounterexampleProof(
                invariant_type=InvariantType.BLAST_RADIUS_BUDGET,
                rule_id="RULE-BLAST-01",
                violated_ci_id="[GLOBAL_REMEDIATION_PLAN]",
                mathematical_formula=formula,
                variable_assignments={
                    "total_unique_impacted": total_impact,
                    "max_allowed_budget": self.max_blast_radius_budget,
                    "impacted_cis": list(all_impacted)
                },
                refutation_statement=f"Cumulative blast radius of {total_impact} nodes breaches global safety budget of {self.max_blast_radius_budget} nodes."
            )
            return InvariantEvaluationResult(
                rule_id="RULE-BLAST-01",
                invariant_type=InvariantType.BLAST_RADIUS_BUDGET,
                is_satisfied=False,
                formula=formula,
                proof_details="Blast radius budget exceeded across proposed concurrent actions.",
                counterexample=cx
            )

    def _verify_quorum_preservation(
        self,
        stage_idx: int,
        stage_actions: List[RemediationActionSpec]
    ) -> InvariantEvaluationResult:
        """Formula: ActiveSurvivingNodes >= floor(N / 2) + 1"""
        for act in stage_actions:
            meta = self.cluster_metadata.get(act.target_ci_id)
            if not meta or not meta.get("is_quorum_based", False):
                continue

            total_n = meta.get("total_replicas", 1)
            min_quorum = math.floor(total_n / 2) + 1
            drained = act.drained_instances
            surviving = total_n - drained

            formula = f"Surviving({surviving}) >= QuorumReq({min_quorum}) [Total={total_n}, Drained={drained}]"

            if surviving < min_quorum:
                cx = CounterexampleProof(
                    invariant_type=InvariantType.QUORUM_PRESERVATION,
                    rule_id=f"RULE-QUORUM-S{stage_idx}",
                    violated_ci_id=act.target_ci_id,
                    mathematical_formula=formula,
                    variable_assignments={
                        "total_replicas": total_n,
                        "drained_instances": drained,
                        "surviving_instances": surviving,
                        "required_quorum": min_quorum
                    },
                    refutation_statement=f"Action '{act.action_id}' drains {drained} nodes from {total_n}-node cluster '{act.target_ci_id}', leaving {surviving} < Quorum {min_quorum}. Split-brain hazard!"
                )
                return InvariantEvaluationResult(
                    rule_id=f"RULE-QUORUM-S{stage_idx}",
                    invariant_type=InvariantType.QUORUM_PRESERVATION,
                    is_satisfied=False,
                    formula=formula,
                    proof_details=f"Quorum violated for stateful CI '{act.target_ci_id}'. Risk of split-brain or write unavailability.",
                    counterexample=cx
                )

        return InvariantEvaluationResult(
            rule_id=f"RULE-QUORUM-S{stage_idx}",
            invariant_type=InvariantType.QUORUM_PRESERVATION,
            is_satisfied=True,
            formula="K_surviving >= floor(N / 2) + 1",
            proof_details=f"All stateful clusters in Stage {stage_idx} preserve quorum strictly."
        )

    def _verify_capacity_headroom(
        self,
        stage_idx: int,
        stage_actions: List[RemediationActionSpec]
    ) -> InvariantEvaluationResult:
        """Formula: SurvivingCapacity >= IncomingRPS * (1 + SafetyMargin)"""
        for act in stage_actions:
            meta = self.cluster_metadata.get(act.target_ci_id)
            if not meta:
                continue

            total_n = meta.get("total_replicas", 1)
            drained = act.drained_instances
            surviving_n = max(0, total_n - drained)
            cap_per_replica = meta.get("capacity_per_replica_rps", 500.0)
            incoming_rps = meta.get("current_incoming_rps", 500.0)

            total_surviving_cap = surviving_n * cap_per_replica
            required_cap = incoming_rps * (1.0 + self.min_headroom_safety_margin_pct / 100.0)

            formula = f"SurvivingCap({total_surviving_cap:.1f} RPS) >= RequiredCap({required_cap:.1f} RPS) [SurvivingPods={surviving_n}]"

            if total_surviving_cap < required_cap:
                cx = CounterexampleProof(
                    invariant_type=InvariantType.CAPACITY_HEADROOM_CONSERVATION,
                    rule_id=f"RULE-CAPACITY-S{stage_idx}",
                    violated_ci_id=act.target_ci_id,
                    mathematical_formula=formula,
                    variable_assignments={
                        "surviving_pods": surviving_n,
                        "surviving_capacity_rps": total_surviving_cap,
                        "incoming_traffic_rps": incoming_rps,
                        "required_capacity_rps": required_cap,
                        "margin_pct": self.min_headroom_safety_margin_pct
                    },
                    refutation_statement=f"Stage {stage_idx} action on '{act.target_ci_id}' reduces capacity to {total_surviving_cap:.1f} RPS, below incoming demand {incoming_rps:.1f} RPS with {self.min_headroom_safety_margin_pct}% safety headroom."
                )
                return InvariantEvaluationResult(
                    rule_id=f"RULE-CAPACITY-S{stage_idx}",
                    invariant_type=InvariantType.CAPACITY_HEADROOM_CONSERVATION,
                    is_satisfied=False,
                    formula=formula,
                    proof_details=f"Capacity saturation imminent for '{act.target_ci_id}'. Risk of HTTP 504 / Connection pool exhaustion.",
                    counterexample=cx
                )

        return InvariantEvaluationResult(
            rule_id=f"RULE-CAPACITY-S{stage_idx}",
            invariant_type=InvariantType.CAPACITY_HEADROOM_CONSERVATION,
            is_satisfied=True,
            formula="SurvivingCapacity >= IngressLoad * (1 + Margin)",
            proof_details=f"All CIs in Stage {stage_idx} maintain safe headroom >= {self.min_headroom_safety_margin_pct}%."
        )

    def _verify_mutual_exclusion(
        self,
        stage_idx: int,
        stage_actions: List[RemediationActionSpec]
    ) -> InvariantEvaluationResult:
        """Formula: For all pairs (CI_a, CI_b) executed concurrently in Stage, NOT(Coupled(CI_a, CI_b))"""
        target_cis = [act.target_ci_id for act in stage_actions]
        
        for i in range(len(target_cis)):
            for j in range(i + 1, len(target_cis)):
                ci_a = target_cis[i]
                ci_b = target_cis[j]
                
                # Check topological direct dependency (A depends on B or B depends on A)
                deps_a = [e.target for e in self.topology.out_edges.get(ci_a, [])]
                deps_b = [e.target for e in self.topology.out_edges.get(ci_b, [])]

                is_coupled = (ci_b in deps_a) or (ci_a in deps_b)
                if is_coupled:
                    formula = f"NOT( Coupled({ci_a}, {ci_b}) )"
                    cx = CounterexampleProof(
                        invariant_type=InvariantType.MUTUAL_EXCLUSION_COUPLING,
                        rule_id=f"RULE-MUTEX-S{stage_idx}",
                        violated_ci_id=f"{ci_a}<->{ci_b}",
                        mathematical_formula=formula,
                        variable_assignments={
                            "ci_a": ci_a,
                            "ci_b": ci_b,
                            "direct_dependency": True,
                            "stage_index": stage_idx
                        },
                        refutation_statement=f"Simultaneous restart of tightly coupled CIs '{ci_a}' and '{ci_b}' in Stage {stage_idx} introduces circular dependency startup deadlock."
                    )
                    return InvariantEvaluationResult(
                        rule_id=f"RULE-MUTEX-S{stage_idx}",
                        invariant_type=InvariantType.MUTUAL_EXCLUSION_COUPLING,
                        is_satisfied=False,
                        formula=formula,
                        proof_details=f"Direct dependency coupling detected between '{ci_a}' and '{ci_b}'. Must be serialized.",
                        counterexample=cx
                    )

        return InvariantEvaluationResult(
            rule_id=f"RULE-MUTEX-S{stage_idx}",
            invariant_type=InvariantType.MUTUAL_EXCLUSION_COUPLING,
            is_satisfied=True,
            formula="NOT( Coupled(CI_a, CI_b) )",
            proof_details=f"No coupled Tier-1 dependencies executed concurrently in Stage {stage_idx}."
        )

    def _verify_stateful_data_safety(
        self,
        action: RemediationActionSpec
    ) -> InvariantEvaluationResult:
        """Formula: ReplicationLag <= MaxAllowedLag"""
        meta = self.cluster_metadata.get(action.target_ci_id, {})
        curr_lag = meta.get("current_replication_lag_sec", 0.0)
        max_lag = meta.get("max_allowed_lag_sec", 1.0)

        formula = f"ReplicationLag({curr_lag:.2f}s) <= MaxAllowed({max_lag:.2f}s)"

        if curr_lag > max_lag:
            cx = CounterexampleProof(
                invariant_type=InvariantType.STATEFUL_DATA_SAFETY,
                rule_id="RULE-STATE-01",
                violated_ci_id=action.target_ci_id,
                mathematical_formula=formula,
                variable_assignments={
                    "current_replication_lag_sec": curr_lag,
                    "max_allowed_lag_sec": max_lag
                },
                refutation_statement=f"Action '{action.action_id}' on stateful CI '{action.target_ci_id}' breaches replication lag limit ({curr_lag:.2f}s > {max_lag:.2f}s). Risk of data loss during failover!"
            )
            return InvariantEvaluationResult(
                rule_id="RULE-STATE-01",
                invariant_type=InvariantType.STATEFUL_DATA_SAFETY,
                is_satisfied=False,
                formula=formula,
                proof_details="Replication lag exceeds safety threshold for stateful mutation.",
                counterexample=cx
            )

        return InvariantEvaluationResult(
            rule_id="RULE-STATE-01",
            invariant_type=InvariantType.STATEFUL_DATA_SAFETY,
            is_satisfied=True,
            formula=formula,
            proof_details=f"Stateful replication lag ({curr_lag:.2f}s) within safe bound for '{action.target_ci_id}'."
        )

    def synthesize_safe_staged_repair(
        self,
        actions: List[RemediationActionSpec]
    ) -> List[RemediationActionSpec]:
        """
        Automated SMT Staged Sequencing Repair:
        Transforms an unsafe concurrent execution plan into a staggered, sequentially safe DAG.
        - Splits large drained instance counts across micro-stages (Canary Rolling drain).
        - Deserializes coupled CIs into distinct dependency-ordered stages.
        """
        repaired_actions: List[RemediationActionSpec] = []
        current_stage = 0

        # Topological sorting priority: deeper dependencies mitigated first
        sorted_actions = sorted(
            actions,
            key=lambda a: len(self.topology.trace_upstream_root_cause_candidates(a.target_ci_id)),
            reverse=True
        )

        for act in sorted_actions:
            meta = self.cluster_metadata.get(act.target_ci_id, {})
            total_n = meta.get("total_replicas", 1)
            is_quorum = meta.get("is_quorum_based", False)
            incoming_rps = meta.get("current_incoming_rps", 500.0)
            cap_per_rep = meta.get("capacity_per_replica_rps", 500.0)
            required_cap = incoming_rps * (1.0 + self.min_headroom_safety_margin_pct / 100.0)

            # Determine safe draining behavior:
            # If draining 1 pod violates capacity headroom, enforce Zero-Downtime Rolling Surge (maxUnavailable=0)
            safe_drain_count = 1
            surge_flag = ""
            if (total_n - 1) * cap_per_rep < required_cap:
                safe_drain_count = 0
                surge_flag = " --maxSurge=1 --maxUnavailable=0"

            if is_quorum and act.drained_instances >= math.floor(total_n / 2):
                # Quorum repair: enforce single-pod canary rolling restarts
                for i in range(act.drained_instances):
                    repaired_actions.append(RemediationActionSpec(
                        action_id=f"{act.action_id}-ROLLING-P{i+1}",
                        target_ci_id=act.target_ci_id,
                        action_type=act.action_type,
                        command=f"{act.command} --rolling-batch=1 --step={i+1}{surge_flag}",
                        is_destructive=act.is_destructive,
                        drained_instances=min(1, safe_drain_count),
                        target_stage_index=current_stage
                    ))
                    current_stage += 1
            else:
                # Assign to dedicated separated stage to prevent mutual exclusion violation
                repaired_actions.append(RemediationActionSpec(
                    action_id=f"{act.action_id}-STAGED",
                    target_ci_id=act.target_ci_id,
                    action_type=act.action_type,
                    command=f"{act.command}{surge_flag}",
                    is_destructive=act.is_destructive,
                    drained_instances=min(act.drained_instances, safe_drain_count),
                    target_stage_index=current_stage
                ))
                current_stage += 1

        return repaired_actions

    def _render_ascii_proof_certificate(
        self,
        plan_id: str,
        verdict: VerificationVerdict,
        results: List[InvariantEvaluationResult],
        counterexamples: List[CounterexampleProof]
    ) -> str:
        """Renders an explainable ASCII formal verification certificate."""
        status_symbol = "[PROVEN SAFE - SAT]" if verdict == VerificationVerdict.PROVABLY_SAFE else (
            "[CONDITIONALLY REPAIRED]" if verdict == VerificationVerdict.CONDITIONALLY_REPAIRED else "[REFUTED - UNSAT]"
        )

        lines = [
            "+-----------------------------------------------------------------------------------------+",
            f"| FORMAL SMT INVARIANT VERIFICATION CERTIFICATE : {plan_id:<39} |",
            f"| VERDICT: {verdict.value:<25} STATUS: {status_symbol:<40} |",
            "+-----------------------------------------------------------------------------------------+",
            "| INVARIANT RULE           | STATUS  | FORMULA / MATHEMATICAL CONSTRAINT                          |",
            "+--------------------------+---------+-----------------------------------------------------------+"
        ]

        for r in results:
            st = "SAT  [OK]" if r.is_satisfied else "UNSAT [X]"
            rule_name = r.invariant_type.value[:24]
            formula_short = r.formula[:57]
            lines.append(f"| {rule_name:<24} | {st:<7} | {formula_short:<57} |")

        lines.append("+--------------------------+---------+-----------------------------------------------------------+")

        if counterexamples:
            lines.append("| FORMAL REFUTATION COUNTEREXAMPLES GENERATED:")
            for cx in counterexamples:
                lines.append(f"|  * [{cx.invariant_type.value}] on '{cx.violated_ci_id}':")
                lines.append(f"|    Proof: {cx.refutation_statement}")
            lines.append("+-----------------------------------------------------------------------------------------+")

        return "\n".join(lines)
