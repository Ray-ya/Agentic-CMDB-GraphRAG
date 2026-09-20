"""
Alert Storm Deduplication & Topological Flapping Suppressor (Pillar 11)
Part of Agentic-CMDB-GraphRAG
Suppresses alert fatigue by collapsing cascading alerts along the CMDB graph
and filtering high-frequency flapping metric alerts.
"""

from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import hashlib
from cmdb_topology import CMDBTopologyGraph


@dataclass
class AlertItem:
    alert_id: str
    source_ci: str
    metric_name: str
    severity: str  # CRITICAL, WARNING, INFO
    timestamp: datetime
    message: str
    state: str = "FIRING"  # FIRING or RESOLVED
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IncidentCluster:
    cluster_id: str
    root_ci_candidate: str
    root_ci_name: str
    severity: str
    alerts_count: int
    suppression_ratio: float
    affected_cis: List[str]
    representative_summary: str
    alerts: List[AlertItem]
    blast_radius_depth: int


class AlertStormEngine:
    def __init__(
        self,
        topology: CMDBTopologyGraph,
        flapping_window_seconds: int = 60,
        flapping_threshold: int = 3
    ):
        self.topology = topology
        self.flapping_window_seconds = flapping_window_seconds
        self.flapping_threshold = flapping_threshold
        # Key: f"{source_ci}:{metric_name}", Value: List of (datetime, state)
        self.alert_history: Dict[str, List[Dict[str, Any]]] = {}

    def record_alert_state(self, alert: AlertItem) -> bool:
        """
        Records alert state and evaluates if the alert is currently 'flapping'.
        Returns True if flapping (should be suppressed), False if legitimate.
        """
        key = f"{alert.source_ci}:{alert.metric_name}"
        now = alert.timestamp or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        if key not in self.alert_history:
            self.alert_history[key] = []

        self.alert_history[key].append({
            "timestamp": now,
            "state": alert.state
        })

        cutoff = now - timedelta(seconds=self.flapping_window_seconds)
        self.alert_history[key] = [
            h for h in self.alert_history[key] if h["timestamp"] >= cutoff
        ]

        # Count state transitions
        records = self.alert_history[key]
        if len(records) < self.flapping_threshold:
            return False

        transitions = 0
        for i in range(1, len(records)):
            if records[i]["state"] != records[i - 1]["state"]:
                transitions += 1

        return transitions >= self.flapping_threshold

    def find_common_root_ci(self, cis: List[str]) -> str:
        """
        Given a list of CIs that fired alerts, find the deepest upstream
        common root or the most critical upstream node in the topology.
        """
        if not cis:
            return "unknown"
        if len(cis) == 1:
            return cis[0]

        # For each CI, compute its dependency closure (nodes it reaches downstream)
        # Note: in cmdb_topology, out_edges represent dependency (svc-order -> svc-payment -> pod -> db)
        # A root failure is an upstream component that others depend on.
        reachable_sets: Dict[str, Set[str]] = {}
        for ci in cis:
            candidates = self.topology.trace_upstream_root_cause_candidates(ci)
            # reachable set includes self and all upstream candidates
            s = {ci}
            for c in candidates:
                s.add(c["id"])
            reachable_sets[ci] = s

        # Find candidates that appear in dependencies of the most alerting CIs
        candidate_scores: Dict[str, int] = {}
        for ci, deps in reachable_sets.items():
            for d in deps:
                candidate_scores[d] = candidate_scores.get(d, 0) + 1

        # Sort candidate by frequency of coverage, then status criticality
        best_candidate = cis[0]
        max_score = -1
        for cand, score in candidate_scores.items():
            node = self.topology.get_node(cand)
            crit_bonus = 2 if node and node.status == "CRITICAL" else (1 if node and node.status in ("WARNING", "DEGRADED") else 0)
            final_score = score * 10 + crit_bonus
            if final_score > max_score:
                max_score = final_score
                best_candidate = cand

        return best_candidate

    def cluster_alerts(self, alerts: List[AlertItem]) -> tuple[List[IncidentCluster], Dict[str, Any]]:
        """
        Collapses cascading alerts into unified incident clusters using CMDB topology.
        """
        if not alerts:
            return [], {"total_raw": 0, "suppressed_flapping": 0, "clusters_formed": 0, "noise_reduction_percentage": "0.0%"}

        valid_alerts: List[AlertItem] = []
        suppressed_flapping_count = 0

        for alert in alerts:
            is_flapping = self.record_alert_state(alert)
            if is_flapping:
                suppressed_flapping_count += 1
            else:
                valid_alerts.append(alert)

        if not valid_alerts:
            return [], {
                "total_raw": len(alerts),
                "suppressed_flapping": suppressed_flapping_count,
                "clusters_formed": 0,
                "noise_reduction_percentage": "100.0%"
            }

        # Check connectivity/dependency between alerting CIs
        # If CIs belong to the same dependency component, group them
        alerting_cis = list({a.source_ci for a in valid_alerts})
        
        # Build adjacency graph between alerting CIs based on CMDB topology
        groups: List[List[AlertItem]] = []
        
        # Test if they share a common root dependency or lie along a dependency chain
        # A simple connected component / root isolation:
        root_ci = self.find_common_root_ci(alerting_cis)
        
        # If all alerting CIs are related to this root CI, collapse into 1 cluster
        # In typical microservice cascading storms, all symptom alerts link to 1 root cause
        clusters: List[IncidentCluster] = []
        
        affected_cis = sorted(list({a.source_ci for a in valid_alerts}))
        has_crit = any(a.severity == "CRITICAL" for a in valid_alerts)
        has_warn = any(a.severity == "WARNING" for a in valid_alerts)
        cluster_sev = "CRITICAL" if has_crit else ("WARNING" if has_warn else "INFO")

        blast = self.topology.compute_blast_radius(root_ci)
        max_depth = blast.get("impact_count", 1) if "error" not in blast else 1

        cid = f"INC-CLUSTER-{hashlib.md5((root_ci + str(len(valid_alerts))).encode()).hexdigest()[:8].upper()}"
        suppression_rate = (1.0 - (1.0 / len(valid_alerts))) if len(valid_alerts) > 1 else 0.0

        root_node = self.topology.get_node(root_ci)
        root_name = root_node.name if root_node else root_ci

        summary = (
            f"Cascading Storm [{cid}] isolated to root CI [{root_name}] ({root_ci}). "
            f"Collapsed {len(valid_alerts)} raw alerts across {len(affected_cis)} CIs "
            f"({', '.join(affected_cis[:3])}{'...' if len(affected_cis) > 3 else ''})."
        )

        cluster = IncidentCluster(
            cluster_id=cid,
            root_ci_candidate=root_ci,
            root_ci_name=root_name,
            severity=cluster_sev,
            alerts_count=len(valid_alerts),
            suppression_ratio=round(suppression_rate, 4),
            affected_cis=affected_cis,
            representative_summary=summary,
            alerts=valid_alerts,
            blast_radius_depth=max_depth
        )
        clusters.append(cluster)

        noise_red = round((1.0 - (len(clusters) / len(alerts))) * 100, 2)
        metrics = {
            "total_raw_alerts": len(alerts),
            "suppressed_flapping": suppressed_flapping_count,
            "valid_processed": len(valid_alerts),
            "clusters_formed": len(clusters),
            "noise_reduction_percentage": f"{noise_red}%"
        }

        return clusters, metrics
