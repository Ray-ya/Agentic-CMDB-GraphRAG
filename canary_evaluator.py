"""
Automated Canary SLI Drift Evaluator & Smart Rollback Gate
(canary_evaluator.py)

Enhancement for Agentic-CMDB-GraphRAG (Pillar 13)
Integrates with RemediationOrchestrator to perform dynamic metric-driven canary verification
using Prometheus-style SLI/SLO metrics (p99 latency, 5xx error budget degradation)
before promoting a remediation step or triggering an automated contingency rollback.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import time


@dataclass
class SLIMetricThreshold:
    metric_name: str
    max_allowed_value: float
    unit: str
    description: str


@dataclass
class CanaryEvaluationResult:
    passed: bool
    target_node_id: str
    evaluated_at: float
    sli_metrics: Dict[str, Any]
    violations: List[str]
    recommendation: str  # "PROMOTE" or "TRIGGER_ROLLBACK"


class CanarySLIEvaluator:
    """
    Evaluates real-time canary metrics post-remediation to prevent regressions.
    """

    def __init__(self, thresholds: Optional[Dict[str, SLIMetricThreshold]] = None):
        self.thresholds = thresholds or {
            "http_p99_latency_ms": SLIMetricThreshold(
                metric_name="http_p99_latency_ms",
                max_allowed_value=1200.0,
                unit="ms",
                description="P99 Latency must stay below 1200ms during canary window."
            ),
            "http_5xx_error_rate_pct": SLIMetricThreshold(
                metric_name="http_5xx_error_rate_pct",
                max_allowed_value=1.5,
                unit="%",
                description="HTTP 5xx error rate must stay below 1.5%."
            ),
            "cpu_throttling_pct": SLIMetricThreshold(
                metric_name="cpu_throttling_pct",
                max_allowed_value=25.0,
                unit="%",
                description="CPU throttling must not exceed 25%."
            )
        }

    def evaluate_canary(
        self,
        target_node_id: str,
        observed_metrics: Dict[str, float]
    ) -> CanaryEvaluationResult:
        """
        Compares observed canary metrics against defined SLI thresholds.
        """
        violations = []
        for metric_name, observed_val in observed_metrics.items():
            if metric_name in self.thresholds:
                thresh = self.thresholds[metric_name]
                if observed_val > thresh.max_allowed_value:
                    violations.append(
                        f"SLI Violation: {metric_name} is {observed_val}{thresh.unit} (Limit: {thresh.max_allowed_value}{thresh.unit}) - {thresh.description}"
                    )

        passed = len(violations) == 0
        recommendation = "PROMOTE" if passed else "TRIGGER_ROLLBACK"

        return CanaryEvaluationResult(
            passed=passed,
            target_node_id=target_node_id,
            evaluated_at=time.time(),
            sli_metrics=observed_metrics,
            violations=violations,
            recommendation=recommendation
        )
