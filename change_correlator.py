"""
Temporal Change Correlation Engine (change_correlator.py)
Correlates real-time infrastructure incidents with recent CI/CD deployments,
configuration mutations, and schema migrations across the CMDB topology.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import time
import math


@dataclass
class ChangeEvent:
    change_id: str
    node_id: str
    change_type: str  # DEPLOYMENT, CONFIG_UPDATE, SCHEMA_MIGRATION, SECRET_ROTATION
    timestamp: float  # Unix epoch
    author: str
    description: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CorrelatedChange:
    change: ChangeEvent
    topological_distance: int
    minutes_before_alert: float
    correlation_score: float  # 0.0 ~ 1.0
    evidence_statement: str


class TemporalChangeCorrelator:
    """
    Tracks infrastructure mutations and computes correlation scores for alerting nodes
    using topological distance and temporal decay.
    """

    def __init__(self, time_window_seconds: float = 3600.0, half_life_seconds: float = 900.0):
        self.time_window_seconds = time_window_seconds  # Default 60 minutes
        self.half_life_seconds = half_life_seconds      # 15 minutes half-life decay
        self.events: List[ChangeEvent] = []

    def record_change(self, event: ChangeEvent) -> None:
        self.events.append(event)

    def correlate_incident(
        self,
        alert_timestamp: float,
        upstream_candidates: List[Dict[str, Any]],
        symptom_node_id: str
    ) -> List[CorrelatedChange]:
        """
        Correlates recent changes within the time window against the symptom node
        and any of its upstream dependencies discovered via CMDB traversal.
        """
        # Build candidate distance map: node_id -> topological depth
        node_depth_map: Dict[str, int] = {symptom_node_id: 0}
        for cand in upstream_candidates:
            node_depth_map[cand["id"]] = cand.get("depth", 1)

        correlated: List[CorrelatedChange] = []

        for event in self.events:
            if event.node_id not in node_depth_map:
                continue

            delta_seconds = alert_timestamp - event.timestamp
            if delta_seconds < 0 or delta_seconds > self.time_window_seconds:
                # Outside correlation window (future event or too old)
                continue

            topological_dist = node_depth_map[event.node_id]

            # 1. Temporal score with exponential half-life decay: exp(-ln(2) * t / t_half)
            decay_factor = math.exp(-0.693147 * (delta_seconds / self.half_life_seconds))

            # 2. Distance penalty: 1.0 for self, 0.85 for 1 hop, 0.7 for 2 hops, etc.
            distance_factor = max(0.4, 1.0 - (topological_dist * 0.15))

            # 3. Change severity factor
            type_multiplier = {
                "SCHEMA_MIGRATION": 1.0,
                "DEPLOYMENT": 0.95,
                "CONFIG_UPDATE": 0.90,
                "SECRET_ROTATION": 0.80
            }.get(event.change_type, 0.70)

            raw_score = decay_factor * distance_factor * type_multiplier
            final_score = round(min(1.0, max(0.0, raw_score)), 3)

            minutes_ago = round(delta_seconds / 60.0, 1)
            evidence = (
                f"Recent {event.change_type} on '{event.node_id}' by {event.author} "
                f"({minutes_ago} mins before alert, topological depth={topological_dist}): {event.description}"
            )

            correlated.append(CorrelatedChange(
                change=event,
                topological_distance=topological_dist,
                minutes_before_alert=minutes_ago,
                correlation_score=final_score,
                evidence_statement=evidence
            ))

        # Sort by correlation score descending
        correlated.sort(key=lambda x: x.correlation_score, reverse=True)
        return correlated
