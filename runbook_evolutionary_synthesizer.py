"""
Pillar 22: Autonomous Evolutionary Runbook Synthesizer & Execution Trace Reflector
(runbook_evolutionary_synthesizer.py)

Part of Agentic-CMDB-GraphRAG
Solves "Runbook Rot" and static playbook obsolescence in dynamic cloud-native architectures:
1. Synthesizes executable runbooks from multi-modal incident telemetry, CMDB topology, and historical postmortems.
2. Employs Genetic / Evolutionary Optimization (Selection, Crossover, Parameter Mutation, Action Inversion).
3. Evaluates candidate runbook chromosomes against:
   - CMDB Digital-Twin Shadow Sandbox (Pillar 16) for Empirical Recovery Delta (Delta Health).
   - Downstream Blast-Radius Topological Boundaries (Pillar 1).
   - Formal SMT Invariant Verifier (Pillar 21) for Mathematical Safety Guarantees.
4. Auto-promotes Pareto-optimal evolved runbooks into the Hybrid Knowledge Base (Pillars 3 & 8)
   and automatically prunes degraded/stale legacy runbooks.
5. Emits explainable ASCII Evolutionary Trees & Fitness Trajectories.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import uuid
import time
import copy
import random
import math

from cmdb_topology import CMDBTopologyGraph, CINode
from digital_twin_sandbox import (
    DigitalTwinShadowSandbox,
    SyntheticRunbook,
    SandboxActionType,
    ShadowSimulationResult,
    SandboxGateVerdict
)
from formal_invariant_verifier import (
    FormalSMTInvariantVerifier,
    RemediationActionSpec,
    ActionType as SMTActionType,
    VerificationVerdict
)
from knowledge_base import HybridKnowledgeBase, KnowledgeDocument


class FailureArchetype(str, Enum):
    DATABASE_POOL_EXHAUSTION = "DATABASE_POOL_EXHAUSTION"
    POD_OOM_CRASH_LOOP = "POD_OOM_CRASH_LOOP"
    UPSTREAM_LATENCY_SATURATION = "UPSTREAM_LATENCY_SATURATION"
    CACHE_STAMPEDE_DEGRADATION = "CACHE_STAMPEDE_DEGRADATION"
    SPLIT_BRAIN_HAZARD = "SPLIT_BRAIN_HAZARD"


@dataclass
class RunbookGene:
    gene_id: str
    step_name: str
    action_type: SandboxActionType
    parameters: Dict[str, Any]
    is_precheck: bool = False
    is_destructive: bool = False
    estimated_latency_sec: float = 5.0


@dataclass
class RunbookChromosome:
    chromosome_id: str
    name: str
    target_ci_id: str
    failure_archetype: FailureArchetype
    genes: List[RunbookGene] = field(default_factory=list)
    generation: int = 0
    fitness_score: float = 0.0
    recovery_delta: float = 0.0
    blast_radius_nodes: int = 0
    blast_radius_penalty: float = 0.0
    smt_invariant_passed: bool = True
    smt_invariant_penalty: float = 0.0
    execution_latency_cost: float = 0.0
    verdict: str = "PENDING"
    mutational_lineage: List[str] = field(default_factory=list)


@dataclass
class GenerationHistory:
    generation: int
    best_fitness: float
    avg_fitness: float
    best_chromosome_id: str
    best_chromosome_name: str
    mutation_events: List[str] = field(default_factory=list)


@dataclass
class EvolutionaryRunbookReport:
    report_id: str
    target_ci_id: str
    failure_archetype: FailureArchetype
    generations_count: int
    population_size: int
    best_runbook: RunbookChromosome
    generation_history: List[GenerationHistory]
    promoted_to_kb: bool = False
    pruned_runbook_ids: List[str] = field(default_factory=list)
    ascii_evolution_tree: str = ""


class EvolutionaryRunbookSynthesizer:
    """
    Topologically-Grounded Evolutionary Runbook Synthesizer (ERS).
    Uses genetic optimization to mutate and breed SRE runbooks,
    verifying fitness through Digital-Twin simulations and SMT mathematical invariants.
    """

    def __init__(
        self,
        topo: CMDBTopologyGraph,
        sandbox: DigitalTwinShadowSandbox,
        formal_verifier: FormalSMTInvariantVerifier,
        kb: Optional[HybridKnowledgeBase] = None
    ):
        self.topo = topo
        self.sandbox = sandbox
        self.formal_verifier = formal_verifier
        self.kb = kb
        self.population_registry: Dict[str, RunbookChromosome] = {}

    def create_seed_population(
        self,
        target_ci_id: str,
        archetype: FailureArchetype,
        population_size: int = 6
    ) -> List[RunbookChromosome]:
        """
        Creates initial generation (Gen 0) seed chromosomes based on failure archetype
        and topological node characteristics.
        """
        ci = self.topo.get_node(target_ci_id)
        ci_name = ci.name if ci else target_ci_id

        population: List[RunbookChromosome] = []

        if archetype == FailureArchetype.DATABASE_POOL_EXHAUSTION:
            # Seed 1: Conservative Graceful Drain & Recycle
            c1 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Graceful Pool Recycle with Connection Drain on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-PRE-01",
                        step_name="Verify Replication & In-Flight Transactions",
                        action_type=SandboxActionType.DRAIN_AND_RECYCLE_POOL,
                        parameters={"drain_timeout_sec": 30, "max_lag_sec": 2.0},
                        is_precheck=True,
                        estimated_latency_sec=3.0
                    ),
                    RunbookGene(
                        gene_id="GENE-ACT-01",
                        step_name="Graceful PgBouncer / Connection Pool Recycle",
                        action_type=SandboxActionType.DRAIN_AND_RECYCLE_POOL,
                        parameters={"concurrency": 2, "kill_idle": True},
                        is_destructive=False,
                        estimated_latency_sec=8.0
                    )
                ],
                mutational_lineage=["Seed:Graceful_Drain"]
            )
            population.append(c1)

            # Seed 2: Aggressive Abrupt Kill (Dangerous Baseline)
            c2 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Abrupt Force Kill Database Backends on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-ACT-02",
                        step_name="Force Kill All Active Backends Immediately",
                        action_type=SandboxActionType.ABRUPT_KILL_SERVICE,
                        parameters={"force": True, "signal": "SIGKILL"},
                        is_destructive=True,
                        estimated_latency_sec=2.0
                    )
                ],
                mutational_lineage=["Seed:Abrupt_Kill"]
            )
            population.append(c2)

            # Seed 3: Rolling Restart Upstream Callers
            c3 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Rolling Restart Dependent Microservice Pods on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-ACT-03",
                        step_name="Rolling Restart Payment Pods",
                        action_type=SandboxActionType.ROLLING_RESTART_POD,
                        parameters={"maxSurge": 1, "maxUnavailable": 0},
                        is_destructive=True,
                        estimated_latency_sec=15.0
                    )
                ],
                mutational_lineage=["Seed:Rolling_Pod_Restart"]
            )
            population.append(c3)

            # Seed 4: Scale Up Pod Replicas
            c4 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Scale Replicas & Connection Capacity on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-ACT-04",
                        step_name="Dynamic Scale Up Workload Capacity",
                        action_type=SandboxActionType.SCALE_REPLICAS_UP,
                        parameters={"added_replicas": 2},
                        is_destructive=False,
                        estimated_latency_sec=12.0
                    )
                ],
                mutational_lineage=["Seed:Scale_Replicas"]
            )
            population.append(c4)

        else:
            # Generic archetype seeds
            c_gen1 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Adaptive Rolling Recovery on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-ACT-ROLL",
                        step_name="Canary Staged Rolling Restart",
                        action_type=SandboxActionType.ROLLING_RESTART_POD,
                        parameters={"maxSurge": 1, "maxUnavailable": 0},
                        is_destructive=True,
                        estimated_latency_sec=10.0
                    )
                ],
                mutational_lineage=["Seed:Generic_Canary"]
            )
            population.append(c_gen1)

            c_gen2 = RunbookChromosome(
                chromosome_id=f"RB-EVO-{uuid.uuid4().hex[:6].upper()}",
                name=f"Targeted Cache Eviction & Recovery on {ci_name}",
                target_ci_id=target_ci_id,
                failure_archetype=archetype,
                genes=[
                    RunbookGene(
                        gene_id="GENE-ACT-CACHE",
                        step_name="Clean Cache and Warmup",
                        action_type=SandboxActionType.CLEAR_DISK_CACHE,
                        parameters={"flush_sync": False},
                        is_destructive=False,
                        estimated_latency_sec=5.0
                    )
                ],
                mutational_lineage=["Seed:Generic_Cache"]
            )
            population.append(c_gen2)

        # Fill remaining population up to population_size via variation
        while len(population) < population_size:
            base = copy.deepcopy(population[0])
            base.chromosome_id = f"RB-EVO-{uuid.uuid4().hex[:6].upper()}"
            base.name = f"Variant: {base.name} [Var {len(population)}]"
            base.mutational_lineage = ["Seed:Variation_Clone"]
            self._mutate_parameters(base)
            population.append(base)

        for p in population:
            self.population_registry[p.chromosome_id] = p

        return population

    def evaluate_fitness(self, chromosome: RunbookChromosome) -> float:
        """
        Multi-Objective Fitness Evaluation:
        Fitness = w1 * RecoveryDelta + w2 * (100 - BlastRadiusPenalty) + w3 * SMTInvariantSafety - w4 * LatencyCost
        """
        # 1. Digital-Twin Shadow Sandbox Simulation
        primary_action = next((g for g in chromosome.genes if not g.is_precheck), chromosome.genes[0])
        sim_runbook = SyntheticRunbook(
            runbook_id=f"SIM-{chromosome.chromosome_id}",
            target_ci=chromosome.target_ci_id,
            action_type=primary_action.action_type,
            parameters=primary_action.parameters,
            rationale=f"Evolutionary candidate: {chromosome.name}"
        )

        sim_res: ShadowSimulationResult = self.sandbox.simulate_execution(
            target_ci=chromosome.target_ci_id,
            runbook=sim_runbook
        )

        chromosome.recovery_delta = max(0.0, sim_res.delta_health_score)
        if sim_res.collateral_damage_detected:
            # Heavy penalty for collateral damage
            chromosome.recovery_delta = max(0.0, chromosome.recovery_delta - 40.0)

        # 2. CMDB Blast Radius
        blast_info = self.topo.compute_blast_radius(chromosome.target_ci_id)
        impact_count = blast_info.get("impact_count", 0)
        chromosome.blast_radius_nodes = impact_count
        # Normalize blast radius penalty (0 to 100)
        total_nodes = len(self.topo.nodes) or 10
        blast_penalty = min(100.0, (impact_count / total_nodes) * 100.0)
        if any(g.is_destructive for g in chromosome.genes):
            blast_penalty *= 1.2
        chromosome.blast_radius_penalty = blast_penalty

        # 3. Formal SMT Invariant Verification
        smt_actions = []
        for idx, g in enumerate(chromosome.genes):
            smt_act = SMTActionType.RECYCLE_POOL
            if g.action_type == SandboxActionType.ROLLING_RESTART_POD:
                smt_act = SMTActionType.RESTART_POD
            elif g.action_type == SandboxActionType.ABRUPT_KILL_SERVICE:
                smt_act = SMTActionType.DRAIN_NODE
            elif g.action_type == SandboxActionType.SCALE_REPLICAS_UP:
                smt_act = SMTActionType.RATE_LIMIT

            smt_actions.append(RemediationActionSpec(
                action_id=f"ACT-{g.gene_id}",
                target_ci_id=chromosome.target_ci_id,
                action_type=smt_act,
                command=f"run {g.step_name}",
                is_destructive=g.is_destructive,
                drained_instances=1 if g.is_destructive else 0,
                target_stage_index=idx
            ))

        ver_report = self.formal_verifier.verify_remediation_plan(
            plan_id=f"PLAN-{chromosome.chromosome_id}",
            actions=smt_actions
        )

        if ver_report.is_safe_to_execute and ver_report.verdict == VerificationVerdict.PROVABLY_SAFE:
            chromosome.smt_invariant_passed = True
            chromosome.smt_invariant_penalty = 0.0
        else:
            chromosome.smt_invariant_passed = False
            # SMT violation incurs heavy deduction
            chromosome.smt_invariant_penalty = 50.0

        # 4. Execution Latency Cost
        total_latency = sum(g.estimated_latency_sec for g in chromosome.genes)
        chromosome.execution_latency_cost = total_latency

        # Composite Fitness Function Calculation:
        # Scale: [0, 100]
        # w1 = 0.50 (Recovery efficacy)
        # w2 = 0.20 (Blast radius containment)
        # w3 = 0.25 (Formal SMT safety)
        # w4 = 0.05 (Latency penalty)
        recovery_component = chromosome.recovery_delta * 0.50
        blast_containment_component = max(0.0, 100.0 - chromosome.blast_radius_penalty) * 0.20
        smt_safety_component = max(0.0, 100.0 - chromosome.smt_invariant_penalty) * 0.25
        latency_penalty_component = min(20.0, (total_latency / 60.0) * 10.0)

        raw_fitness = recovery_component + blast_containment_component + smt_safety_component - latency_penalty_component
        chromosome.fitness_score = round(max(0.0, min(100.0, raw_fitness)), 2)

        if chromosome.fitness_score >= 70.0 and chromosome.smt_invariant_passed:
            chromosome.verdict = "PARETO_OPTIMAL"
        elif chromosome.fitness_score >= 45.0:
            chromosome.verdict = "VIABLE_SUBOPTIMAL"
        else:
            chromosome.verdict = "DISQUALIFIED_UNSAFE"

        return chromosome.fitness_score

    def _mutate_parameters(self, chromosome: RunbookChromosome) -> str:
        """Mutates tuning parameters of chromosome genes (e.g., drain timeout, batch surge)."""
        mutations = []
        for gene in chromosome.genes:
            if "drain_timeout_sec" in gene.parameters:
                delta = random.choice([-5, 5, 10])
                gene.parameters["drain_timeout_sec"] = max(10, gene.parameters["drain_timeout_sec"] + delta)
                mutations.append(f"Param:drain_timeout={gene.parameters['drain_timeout_sec']}s")
            if "concurrency" in gene.parameters:
                gene.parameters["concurrency"] = random.choice([1, 2, 4])
                mutations.append(f"Param:concurrency={gene.parameters['concurrency']}")
            if gene.action_type == SandboxActionType.ROLLING_RESTART_POD:
                gene.parameters["maxUnavailable"] = 0
                gene.parameters["maxSurge"] = 1
                mutations.append("Guard:ZeroDowntimeRollingSurge")
        return ", ".join(mutations) or "Param:Noop"

    def _mutate_structure(self, chromosome: RunbookChromosome) -> str:
        """Structural mutation: adds a pre-check guard or replaces abrupt kill with graceful drain."""
        if any(g.action_type == SandboxActionType.ABRUPT_KILL_SERVICE for g in chromosome.genes):
            # Mutational repair: convert abrupt kill to drain and recycle pool
            for g in chromosome.genes:
                if g.action_type == SandboxActionType.ABRUPT_KILL_SERVICE:
                    g.action_type = SandboxActionType.DRAIN_AND_RECYCLE_POOL
                    g.step_name = "Graceful Pool Drain & Backend Recycle"
                    g.is_destructive = False
                    g.parameters = {"drain_timeout_sec": 20, "kill_idle": True}
            return "Struct:ConvertAbruptKillToGracefulDrain"

        # If missing pre-check, add safety pre-check
        if not any(g.is_precheck for g in chromosome.genes):
            guard_gene = RunbookGene(
                gene_id=f"GENE-PRE-{uuid.uuid4().hex[:4].upper()}",
                step_name="Pre-flight Capacity Headroom & Lag Guard",
                action_type=SandboxActionType.DRAIN_AND_RECYCLE_POOL,
                parameters={"precheck_only": True, "required_headroom_pct": 20.0},
                is_precheck=True,
                estimated_latency_sec=2.0
            )
            chromosome.genes.insert(0, guard_gene)
            return "Struct:AddPreflightGuard"

        return "Struct:RefinedStepSequence"

    def crossover(
        self,
        parent1: RunbookChromosome,
        parent2: RunbookChromosome
    ) -> RunbookChromosome:
        """
        Crossover operator: splices high-performing pre-checks from parent1
        with high-efficacy mitigation steps from parent2.
        """
        child_id = f"RB-EVO-{uuid.uuid4().hex[:6].upper()}"
        child_name = f"Evolved Variant ({parent1.name[:18]} x {parent2.name[:18]})"

        # Splice genes
        prechecks = [copy.deepcopy(g) for g in parent1.genes if g.is_precheck]
        if not prechecks:
            prechecks = [copy.deepcopy(g) for g in parent2.genes if g.is_precheck]

        mitigations = [copy.deepcopy(g) for g in parent2.genes if not g.is_precheck]
        if not mitigations:
            mitigations = [copy.deepcopy(g) for g in parent1.genes if not g.is_precheck]

        combined_genes = prechecks + mitigations
        child = RunbookChromosome(
            chromosome_id=child_id,
            name=child_name,
            target_ci_id=parent1.target_ci_id,
            failure_archetype=parent1.failure_archetype,
            genes=combined_genes,
            generation=max(parent1.generation, parent2.generation) + 1,
            mutational_lineage=[f"Crossover({parent1.chromosome_id[:10]} x {parent2.chromosome_id[:10]})"]
        )
        return child

    def evolve_population(
        self,
        target_ci_id: str,
        archetype: FailureArchetype,
        generations: int = 4,
        population_size: int = 6
    ) -> EvolutionaryRunbookReport:
        """
        Runs the full Evolutionary Genetic Optimization across multiple generations:
        Selection (Tournament) -> Crossover -> Mutation -> Sandbox/SMT Evaluation -> Elitism.
        """
        population = self.create_seed_population(target_ci_id, archetype, population_size)

        generation_histories: List[GenerationHistory] = []

        for gen in range(generations):
            # 1. Evaluate fitness for all chromosomes in current generation
            for ind in population:
                ind.generation = gen
                self.evaluate_fitness(ind)

            # Sort population by fitness descending (Elitism)
            population.sort(key=lambda x: x.fitness_score, reverse=True)

            best = population[0]
            avg_fit = sum(p.fitness_score for p in population) / len(population)

            gen_mutations = []

            # 2. Record generation metrics
            hist = GenerationHistory(
                generation=gen,
                best_fitness=best.fitness_score,
                avg_fitness=round(avg_fit, 2),
                best_chromosome_id=best.chromosome_id,
                best_chromosome_name=best.name,
                mutation_events=gen_mutations
            )
            generation_histories.append(hist)

            if gen == generations - 1:
                break  # Finished last generation

            # 3. Breed next generation
            next_generation: List[RunbookChromosome] = []

            # Elitism: retain top 2 directly
            next_generation.append(copy.deepcopy(population[0]))
            next_generation.append(copy.deepcopy(population[1]))

            # Fill remaining with crossover and mutation
            while len(next_generation) < population_size:
                # Tournament selection (pick 2 random, keep best)
                candidates = random.sample(population[:4], 2)
                p1 = max(candidates, key=lambda x: x.fitness_score)

                candidates2 = random.sample(population[:4], 2)
                p2 = max(candidates2, key=lambda x: x.fitness_score)

                child = self.crossover(p1, p2)

                # Mutate child
                m_type = random.choice(["param", "struct", "both"])
                mut_log = []
                if m_type in ("param", "both"):
                    log_p = self._mutate_parameters(child)
                    mut_log.append(log_p)
                if m_type in ("struct", "both"):
                    log_s = self._mutate_structure(child)
                    mut_log.append(log_s)

                child.mutational_lineage.extend(mut_log)
                gen_mutations.extend(mut_log)

                self.population_registry[child.chromosome_id] = child
                next_generation.append(child)

            population = next_generation

        # Final best chromosome
        best_runbook = population[0]

        # Auto-promote to Knowledge Base if available
        promoted = False
        if self.kb and best_runbook.fitness_score >= 65.0:
            self.promote_to_knowledge_base(best_runbook)
            promoted = True

        # Prune stale runbooks
        pruned_ids = self.prune_stale_runbooks(archetype.value, min_fitness_threshold=40.0)

        report = EvolutionaryRunbookReport(
            report_id=f"EVO-REP-{uuid.uuid4().hex[:8].upper()}",
            target_ci_id=target_ci_id,
            failure_archetype=archetype,
            generations_count=generations,
            population_size=population_size,
            best_runbook=best_runbook,
            generation_history=generation_histories,
            promoted_to_kb=promoted,
            pruned_runbook_ids=pruned_ids
        )
        report.ascii_evolution_tree = self.render_ascii_evolution_tree(report)

        return report

    def promote_to_knowledge_base(self, chromosome: RunbookChromosome) -> str:
        """Registers the evolved Pareto-optimal runbook into the SRE Hybrid Knowledge Base."""
        if not self.kb:
            return ""

        steps_text = "\n".join([
            f"Step {idx+1}: [{'PRECHECK' if g.is_precheck else 'ACTION'}] {g.step_name} (Action: {g.action_type.value}, Params: {g.parameters})"
            for idx, g in enumerate(chromosome.genes)
        ])

        doc_content = (
            f"# [EVOLVED_RUNBOOK] {chromosome.name}\n"
            f"Runbook ID: {chromosome.chromosome_id}\n"
            f"Target CI: {chromosome.target_ci_id}\n"
            f"Failure Archetype: {chromosome.failure_archetype.value}\n"
            f"Fitness Score: {chromosome.fitness_score}/100.0 (Recovery Delta: +{chromosome.recovery_delta} pts)\n"
            f"SMT Invariant Passed: {chromosome.smt_invariant_passed}\n"
            f"Blast Radius Impact: {chromosome.blast_radius_nodes} nodes\n\n"
            f"## Remediation Execution Steps:\n"
            f"{steps_text}\n\n"
            f"Lineage: {' -> '.join(chromosome.mutational_lineage)}"
        )

        doc = KnowledgeDocument(
            doc_id=chromosome.chromosome_id,
            title=f"[EVOLVED-RB] {chromosome.name}",
            doc_type="EVOLVED_RUNBOOK",
            content=doc_content,
            tags=["evolved_runbook", chromosome.failure_archetype.value.lower(), chromosome.target_ci_id],
            target_components=[chromosome.target_ci_id]
        )
        self.kb.add_document(doc)
        return doc.doc_id

    def prune_stale_runbooks(
        self,
        archetype_name: str,
        min_fitness_threshold: float = 40.0
    ) -> List[str]:
        """Identifies and marks obsolete runbooks whose fitness fell below the retirement threshold."""
        pruned = []
        for chrom_id, chrom in self.population_registry.items():
            if chrom.failure_archetype.value == archetype_name:
                if chrom.fitness_score < min_fitness_threshold or not chrom.smt_invariant_passed:
                    chrom.verdict = "RETIRED_STALE"
                    pruned.append(chrom_id)
        return pruned

    def render_ascii_evolution_tree(self, report: EvolutionaryRunbookReport) -> str:
        """Renders an executive ASCII tree showing evolutionary fitness trajectory and mutational lineage."""
        lines = [
            "================================================================================",
            f"🧬 AUTONOMOUS EVOLUTIONARY RUNBOOK SYNTHESIS REPORT ({report.report_id})",
            "================================================================================",
            f"Target CI: {report.target_ci_id} | Failure Archetype: {report.failure_archetype.value}",
            f"Generations: {report.generations_count} | Population Size: {report.population_size}",
            "",
            "📈 [GENERATIONAL FITNESS TRAJECTORY]:"
        ]

        for gh in report.generation_history:
            bar_len = int(gh.best_fitness / 2.5)
            bar = "█" * bar_len + "░" * (40 - bar_len)
            lines.append(
                f"  Gen {gh.generation:02d} | Best: {gh.best_fitness:5.1f} | Avg: {gh.avg_fitness:5.1f} | [{bar}] | Winner: {gh.best_chromosome_id[:12]}"
            )

        best = report.best_runbook
        lines.extend([
            "",
            "🏆 [PARETO-OPTIMAL EVOLVED RUNBOOK]:",
            f"  ID: {best.chromosome_id} - '{best.name}'",
            f"  Verdict: {best.verdict} | Overall Fitness: {best.fitness_score}/100.0",
            f"  -> Recovery Delta: +{best.recovery_delta:.1f} pts (Empirically Shadow Verified)",
            f"  -> Blast Radius:   {best.blast_radius_nodes} nodes (Penalty: {best.blast_radius_penalty:.1f} pts)",
            f"  -> SMT Invariants: {'PASS (0 penalty)' if best.smt_invariant_passed else 'FAIL (SMT Penalty: ' + str(best.smt_invariant_penalty) + ')'}",
            f"  -> Est. Latency:   {best.execution_latency_cost:.1f}s",
            "",
            "🔧 [REMEDIAL EXECUTION PLAN]:"
        ])

        for idx, g in enumerate(best.genes):
            kind = "PRECHECK" if g.is_precheck else "ACTION"
            dest = "DESTRUCTIVE" if g.is_destructive else "SAFE"
            lines.append(
                f"  [{idx+1}] [{kind}] {g.step_name}"
            )
            lines.append(
                f"      Type: {g.action_type.value} | Mode: {dest} | Latency: {g.estimated_latency_sec}s | Params: {g.parameters}"
            )

        lines.extend([
            "",
            "🧬 [MUTATIONAL LINEAGE & PROVENANCE]:",
            f"  {' -> '.join(best.mutational_lineage)}",
            "",
            f"Knowledge Base Status: {'Promoted & Indexed' if report.promoted_to_kb else 'Staged in Registry'}",
            f"Pruned Stale Runbooks: {len(report.pruned_runbook_ids)} legacy entries de-indexed.",
            "================================================================================"
        ])

        return "\n".join(lines)

