"""
Multi-Modal Multi-Source Operational Telemetry Ingestion Engine
(multimodal_ingestion.py)

Ingests:
1. Kubernetes Event Streams (Pod, Node, OOMKilled, Eviction)
2. Metric PromQL Alert Stream (CPU, Memory, Network, Connection Pool saturation)
3. Structured Application Tracing Spans (OpenTelemetry JSON format)
4. CloudTrail / Audit Logs (IAM, Security Group mutations)

Normalizes into:
- Topology node state updates (Dynamic CMDB enrichment)
- Synthetic Incident Anomaly contexts for Dual-Level GraphRAG
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import time
import json


@dataclass
class TelemetryEvent:
    event_id: str
    source: str  # "K8S_EVENT", "PROMETHEUS", "OPENTELEMETRY", "CLOUDTRAIL"
    timestamp: float
    target_node_id: str
    severity: str  # "INFO", "WARNING", "CRITICAL"
    event_name: str
    payload: Dict[str, Any]
    tags: List[str] = field(default_factory=list)


class MultiModalTelemetryIngestionEngine:
    """
    Ingests and normalizes high-velocity distributed telemetry events
    and maps them onto CMDB Configuration Items (CIs).
    """

    def __init__(self):
        self.event_store: List[TelemetryEvent] = []
        self.source_counters: Dict[str, int] = {
            "K8S_EVENT": 0,
            "PROMETHEUS": 0,
            "OPENTELEMETRY": 0,
            "CLOUDTRAIL": 0
        }

    def ingest_k8s_event(self, raw_event: Dict[str, Any]) -> TelemetryEvent:
        """
        Parses Kubernetes Event (e.g., OOMKilled, FailedScheduling, CrashLoopBackOff)
        """
        reason = raw_event.get("reason", "Unknown")
        involved_obj = raw_event.get("involvedObject", {})
        pod_name = involved_obj.get("name", "unknown-pod")
        namespace = involved_obj.get("namespace", "default")
        
        # Determine severity
        severity = "WARNING"
        if reason in ["OOMKilled", "CrashLoopBackOff", "FailedCreate"]:
            severity = "CRITICAL"
        elif raw_event.get("type") == "Normal":
            severity = "INFO"

        event = TelemetryEvent(
            event_id=f"K8S-{int(time.time()*1000)}-{len(self.event_store)}",
            source="K8S_EVENT",
            timestamp=raw_event.get("firstTimestamp", time.time()),
            target_node_id=pod_name,
            severity=severity,
            event_name=f"K8s_{reason}",
            payload={
                "namespace": namespace,
                "message": raw_event.get("message", ""),
                "count": raw_event.get("count", 1),
                "component": raw_event.get("source", {}).get("component", "kubelet")
            },
            tags=["k8s", namespace, reason.lower()]
        )
        self.event_store.append(event)
        self.source_counters["K8S_EVENT"] += 1
        return event

    def ingest_prometheus_alert(self, raw_alert: Dict[str, Any]) -> TelemetryEvent:
        """
        Parses Prometheus / Alertmanager Webhook payload
        """
        labels = raw_alert.get("labels", {})
        annotations = raw_alert.get("annotations", {})
        alert_name = labels.get("alertname", "PrometheusAlert")
        severity_label = labels.get("severity", "warning").upper()
        severity = "CRITICAL" if severity_label == "CRITICAL" else "WARNING"
        node_id = labels.get("instance") or labels.get("service") or labels.get("pod") or "unknown-service"

        event = TelemetryEvent(
            event_id=f"PROM-{int(time.time()*1000)}-{len(self.event_store)}",
            source="PROMETHEUS",
            timestamp=raw_alert.get("startsAt", time.time()),
            target_node_id=node_id,
            severity=severity,
            event_name=alert_name,
            payload={
                "summary": annotations.get("summary", ""),
                "description": annotations.get("description", ""),
                "value": raw_alert.get("value", "N/A"),
                "labels": labels
            },
            tags=["prometheus", "metrics", alert_name.lower()]
        )
        self.event_store.append(event)
        self.source_counters["PROMETHEUS"] += 1
        return event

    def ingest_opentelemetry_span(self, raw_span: Dict[str, Any]) -> TelemetryEvent:
        """
        Parses OpenTelemetry Tracing Span with Error Status / Latency Spike
        """
        service_name = raw_span.get("service_name", "unknown-service")
        span_id = raw_span.get("span_id", "span-0")
        trace_id = raw_span.get("trace_id", "trace-0")
        status_code = raw_span.get("status_code", "OK")  # OK, ERROR
        duration_ms = raw_span.get("duration_ms", 0.0)

        severity = "CRITICAL" if status_code == "ERROR" or duration_ms > 5000.0 else "INFO"

        event = TelemetryEvent(
            event_id=f"OTEL-{span_id[:8]}",
            source="OPENTELEMETRY",
            timestamp=raw_span.get("start_time", time.time()),
            target_node_id=service_name,
            severity=severity,
            event_name="OTel_Span_Error" if status_code == "ERROR" else "OTel_Slow_Span",
            payload={
                "trace_id": trace_id,
                "span_id": span_id,
                "duration_ms": duration_ms,
                "http_status": raw_span.get("http_status_code", 200),
                "error_message": raw_span.get("error_message", "")
            },
            tags=["otel", "trace", service_name]
        )
        self.event_store.append(event)
        self.source_counters["OPENTELEMETRY"] += 1
        return event

    def ingest_cloudtrail_event(self, raw_record: Dict[str, Any]) -> TelemetryEvent:
        """
        Parses AWS CloudTrail security/infrastructure mutation event
        """
        event_name = raw_record.get("eventName", "CloudTrailEvent")
        event_source = raw_record.get("eventSource", "aws")
        user_identity = raw_record.get("userIdentity", {}).get("userName", "system")
        resource_id = "unknown-resource"
        if raw_record.get("resources"):
            resource_id = raw_record["resources"][0].get("resourceName", "unknown-resource")

        event = TelemetryEvent(
            event_id=f"TRAIL-{int(time.time()*1000)}-{len(self.event_store)}",
            source="CLOUDTRAIL",
            timestamp=time.time(),
            target_node_id=resource_id,
            severity="WARNING" if "AuthorizeSecurityGroup" in event_name or "Delete" in event_name else "INFO",
            event_name=f"CloudTrail_{event_name}",
            payload={
                "event_source": event_source,
                "user": user_identity,
                "aws_region": raw_record.get("awsRegion", "ap-northeast-1"),
                "request_parameters": raw_record.get("requestParameters", {})
            },
            tags=["cloudtrail", "audit", event_name.lower()]
        )
        self.event_store.append(event)
        self.source_counters["CLOUDTRAIL"] += 1
        return event

    def get_recent_telemetry_for_ci(self, node_id: str, max_events: int = 5) -> List[Dict[str, Any]]:
        """
        Returns recent telemetry events mapped to a specific CI or related name
        """
        matches = [
            {
                "event_id": e.event_id,
                "source": e.source,
                "severity": e.severity,
                "event_name": e.event_name,
                "payload": e.payload,
                "timestamp": e.timestamp
            }
            for e in reversed(self.event_store)
            if e.target_node_id == node_id or node_id in e.tags
        ]
        return matches[:max_events]
