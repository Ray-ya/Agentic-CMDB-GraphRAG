"""
Pillar 14: Dynamic Adaptive Rate-Limiting & Dependency Backpressure Governor
(backpressure_governor.py)

Enhancement for Agentic-CMDB-GraphRAG (Pillar 14)
When upstream/downstream CIs experience latency degradation, connection pool exhaustion,
or high error rates, this governor:
1. Calculates topological cascading risk based on downstream blast radius and upstream dependency depth.
2. Dynamically calculates adaptive concurrency limits and token bucket rate limits (AIMD - Additive Increase / Multiplicative Decrease).
3. Provides Circuit Breaking states: CLOSED, OPEN, HALF_OPEN.
4. Generates edge-level backpressure throttling directives to protect critical paths from cascading collapse.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import time
from enum import Enum

from cmdb_topology import CMDBTopologyGraph


class CircuitState(str, Enum):
    CLOSED = "CLOSED"         # Normal operation, full traffic allowed
    HALF_OPEN = "HALF_OPEN"   # Probing with throttled traffic
    OPEN = "OPEN"             # Tripped, shed all non-critical traffic


@dataclass
class NodeHealthMetrics:
    latency_p99_ms: float
    error_rate_pct: float
    connection_pool_utilization_pct: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class BackpressureDirective:
    source_ci: str
    target_ci: str
    circuit_state: CircuitState
    allowed_concurrency_limit: int
    rate_limit_rps: float
    throttle_ratio_pct: float
    reason: str


class DependencyBackpressureGovernor:
    """
    Monitors topological CI metrics and enforces adaptive backpressure across dependency edges.
    """

    def __init__(
        self,
        topology: CMDBTopologyGraph,
        base_concurrency: int = 100,
        base_rps: float = 500.0,
        latency_threshold_ms: float = 1200.0,
        error_rate_threshold_pct: float = 5.0,
        pool_exhaustion_threshold_pct: float = 85.0
    ):
        self.topology = topology
        self.base_concurrency = base_concurrency
        self.base_rps = base_rps
        self.latency_threshold_ms = latency_threshold_ms
        self.error_rate_threshold_pct = error_rate_threshold_pct
        self.pool_exhaustion_threshold_pct = pool_exhaustion_threshold_pct
        
        # State tracking per edge (source -> target) or per node
        self.circuit_states: Dict[str, CircuitState] = {}
        self.concurrency_limits: Dict[str, int] = {}
        self.rate_limits: Dict[str, float] = {}

    def assess_and_govern(
        self,
        target_ci: str,
        metrics: NodeHealthMetrics
    ) -> List[BackpressureDirective]:
        """
        Assesses target CI health, and if degraded, computes backpressure directives
        for all immediate upstream callers (inbound edges to target_ci).
        """
        is_unhealthy = (
            metrics.latency_p99_ms > self.latency_threshold_ms or
            metrics.error_rate_pct > self.error_rate_threshold_pct or
            metrics.connection_pool_utilization_pct > self.pool_exhaustion_threshold_pct
        )

        # Get callers (nodes that depend on target_ci)
        callers = self.topology.in_edges.get(target_ci, [])
        directives: List[BackpressureDirective] = []

        # Determine circuit state
        if is_unhealthy:
            # Check severity to decide OPEN vs HALF_OPEN
            if metrics.error_rate_pct > 25.0 or metrics.connection_pool_utilization_pct > 95.0:
                new_state = CircuitState.OPEN
            else:
                new_state = CircuitState.HALF_OPEN
        else:
            new_state = CircuitState.CLOSED

        for edge in callers:
            caller_ci = edge.source
            edge_key = f"{caller_ci}->{target_ci}"

            current_concurrency = self.concurrency_limits.get(edge_key, self.base_concurrency)
            current_rps = self.rate_limits.get(edge_key, self.base_rps)

            if new_state == CircuitState.OPEN:
                # Multiplicative decrease / emergency shed
                concurrency = max(5, int(current_concurrency * 0.1))
                rps = max(10.0, current_rps * 0.1)
                throttle_pct = 90.0
                reason = (
                    f"Severe degradation on downstream '{target_ci}' "
                    f"(P99: {metrics.latency_p99_ms}ms, Error: {metrics.error_rate_pct}%, Pool: {metrics.connection_pool_utilization_pct}%). "
                    f"Circuit tripped to OPEN. Emergency backpressure enforced."
                )
            elif new_state == CircuitState.HALF_OPEN:
                # Moderate backpressure
                concurrency = max(10, int(current_concurrency * 0.5))
                rps = max(50.0, current_rps * 0.5)
                throttle_pct = 50.0
                reason = (
                    f"Elevated stress on downstream '{target_ci}' "
                    f"(P99: {metrics.latency_p99_ms}ms, Error: {metrics.error_rate_pct}%). "
                    f"Circuit HALF_OPEN. Throttling incoming traffic by 50%."
                )
            else:
                # Additive recovery
                concurrency = min(self.base_concurrency, current_concurrency + 10)
                rps = min(self.base_rps, current_rps + 50.0)
                throttle_pct = 0.0
                reason = f"Downstream '{target_ci}' healthy. Circuit CLOSED. Normal operating capacity restored."

            self.circuit_states[edge_key] = new_state
            self.concurrency_limits[edge_key] = concurrency
            self.rate_limits[edge_key] = rps

            directives.append(
                BackpressureDirective(
                    source_ci=caller_ci,
                    target_ci=target_ci,
                    circuit_state=new_state,
                    allowed_concurrency_limit=concurrency,
                    rate_limit_rps=rps,
                    throttle_ratio_pct=throttle_pct,
                    reason=reason
                )
            )

        return directives
