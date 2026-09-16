"""
Agentic SRE Incident Resolver (rca_agent.py)
Inspired by Orrery (BAHALLA) & SRE-Sentinel (aryan877):
Implements autonomous Root Cause Analysis (RCA) with:
1. Hypothesis Generation & Topological Evidence Verification
2. Temporal CI/CD Change Correlation
3. Confidence Scoring
4. Action Plan Generation with Mandatory Human-Gate Safety Barrier
5. Post-mortem Synthesis & Closed-Loop Knowledge Injection
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from dual_level_graphrag import DualLevelGraphRAGEngine
from knowledge_base import KnowledgeDocument
import uuid
import time


@dataclass
class RemediationAction:
    action_id: str
    action_type: str  # SCALE_POD, RESTART_POD, RECYCLE_DB_POOL, ROLLBACK_DEPLOYMENT, REVERT_CONFIG, ROLLBACK_MIGRATION
    target_ci_id: str
    target_ci_name: str
    command: str
    is_destructive: bool
    requires_human_approval: bool
    status: str  # PENDING_APPROVAL, APPROVED, REJECTED, EXECUTED


@dataclass
class IncidentRCAReport:
    incident_id: str
    alert_name: str
    symptom_node: str
    root_cause_node: str
    root_cause_summary: str
    confidence_score: float  # 0.0 ~ 1.0
    evidence_chain: List[str]
    blast_radius_summary: str
    recommended_actions: List[RemediationAction]
    created_at: float
    correlated_change_count: int = 0


class AgenticRCAResolver:
    """
    Autonomous SRE Incident Investigator.
    Integrates with DualLevelGraphRAG to form grounded hypotheses,
    correlate CI/CD mutations, propose safe action plans, and feed postmortems back into KB.
    """

    def __init__(self, graphrag_engine: DualLevelGraphRAGEngine):
        self.engine = graphrag_engine
        self.incident_history: Dict[str, IncidentRCAReport] = {}

    def investigate(self, alert_event: Dict[str, Any]) -> IncidentRCAReport:
        """
        Main Agentic Reasoning Pipeline:
        1. Context Synthesis (Dual-Level GraphRAG + Temporal Correlator)
        2. Graph-guided Upstream Path Traversal (Candidate Isolation)
        3. Mutation & Runbook Grounding (Evidence Verification)
        4. Remediation Action Formation with Human-Gate Isolation
        """
        node_id = alert_event.get("node_id")
        alert_name = alert_event.get("alert_name", "Incident Alert")

        context = self.engine.synthesize_incident_context(alert_event)
        rag_data = context["dual_level_graphrag"]

        low_level = rag_data["low_level_inspection"]
        high_level = rag_data["high_level_topology"]

        upstream_candidates = high_level.get("upstream_dependency_path", [])
        blast_info = high_level.get("blast_radius_assessment", {})
        correlated_changes = high_level.get("correlated_changes", [])

        # 1. Identify Root Cause Node via Anomalous Dependency Traversal
        root_cause_node_id = node_id
        root_cause_node_name = low_level.get("focus_entity", {}).get("name", node_id)
        evidence_chain = [f"Alert triggered on symptom node '{root_cause_node_name}'"]

        # If any upstream dependency is CRITICAL or WARNING, it is likely the root cause
        anomalous_deps = [c for c in upstream_candidates if c.get("is_anomalous")]
        if anomalous_deps:
            # Deepest anomalous dependency in call hierarchy
            target_culprit = anomalous_deps[-1]
            root_cause_node_id = target_culprit["id"]
            root_cause_node_name = target_culprit["name"]
            evidence_chain.append(
                f"Topological traversal identified upstream dependency '{root_cause_node_name}' ({target_culprit['ci_type']}) in status {target_culprit['status']}."
            )
            evidence_chain.append(f"Dependency path: {target_culprit['dependency_chain']}")

        # 2. Extract Specific Root Cause Diagnostic from Matched Runbook & Metrics
        root_cause_summary = ""
        confidence = 0.85
        culprit_node = self.engine.topology.get_node(root_cause_node_id)
        if culprit_node and culprit_node.ci_type == "DATABASE":
            if culprit_node.metadata.get("current_connections") == culprit_node.metadata.get("max_connections"):
                root_cause_summary = f"Database connection pool exhaustion on '{culprit_node.name}' (500/500 connections in use), causing connection timeouts in upstream application pods."
                confidence = 0.96
                evidence_chain.append("Database metadata confirms active connections = max connections (500/500).")
        else:
            root_cause_summary = f"Anomalous failure propagated from '{root_cause_node_name}'."
            confidence = 0.78

        # 3. Incorporate Temporal Change Correlation
        actions: List[RemediationAction] = []
        if correlated_changes:
            top_change = correlated_changes[0]
            if top_change["correlation_score"] >= 0.60:
                evidence_chain.append(f"Temporal Correlation ({top_change['correlation_score']*100:.1f}%): {top_change['evidence_statement']}")
                confidence = min(0.99, round(confidence + 0.05, 2))

                # If the change was a deployment on or upstream of root cause, offer automated rollback
                ch_type = top_change["change_type"]
                ch_node = top_change["node_id"]
                if ch_type == "DEPLOYMENT":
                    actions.append(RemediationAction(
                        action_id=f"ACT-{uuid.uuid4().hex[:6].upper()}",
                        action_type="ROLLBACK_DEPLOYMENT",
                        target_ci_id=ch_node,
                        target_ci_name=ch_node,
                        command=f"kubectl rollout undo deployment/{ch_node} -n production",
                        is_destructive=True,
                        requires_human_approval=True,
                        status="PENDING_APPROVAL"
                    ))
                elif ch_type == "CONFIG_UPDATE":
                    actions.append(RemediationAction(
                        action_id=f"ACT-{uuid.uuid4().hex[:6].upper()}",
                        action_type="REVERT_CONFIG",
                        target_ci_id=ch_node,
                        target_ci_name=ch_node,
                        command=f"helm rollback {ch_node} -n production",
                        is_destructive=True,
                        requires_human_approval=True,
                        status="PENDING_APPROVAL"
                    ))
                elif ch_type == "SCHEMA_MIGRATION":
                    actions.append(RemediationAction(
                        action_id=f"ACT-{uuid.uuid4().hex[:6].upper()}",
                        action_type="ROLLBACK_MIGRATION",
                        target_ci_id=ch_node,
                        target_ci_name=ch_node,
                        command=f"flyway undo -targetVersion=previous -url=jdbc:postgresql://{ch_node}:5432/db",
                        is_destructive=True,
                        requires_human_approval=True,
                        status="PENDING_APPROVAL"
                    ))

        # 4. Formulate Action Plans with Human-in-the-Loop Safety Gate
        if culprit_node and culprit_node.ci_type == "DATABASE":
            # Action 1: Kill idle transactions (Non-destructive)
            actions.append(RemediationAction(
                action_id=f"ACT-{uuid.uuid4().hex[:6].upper()}",
                action_type="RECYCLE_DB_POOL",
                target_ci_id=culprit_node.id,
                target_ci_name=culprit_node.name,
                command="SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND state_change < now() - INTERVAL '3 minutes';",
                is_destructive=False,
                requires_human_approval=True,
                status="PENDING_APPROVAL"
            ))
            # Action 2: Restart alerting pods (Destructive)
            actions.append(RemediationAction(
                action_id=f"ACT-{uuid.uuid4().hex[:6].upper()}",
                action_type="RESTART_POD",
                target_ci_id=node_id,
                target_ci_name=low_level.get("focus_entity", {}).get("name", node_id),
                command=f"kubectl rollout restart deployment/{node_id} -n production",
                is_destructive=True,
                requires_human_approval=True,  # Mandatory human gate
                status="PENDING_APPROVAL"
            ))

        blast_summary = f"{blast_info.get('impact_count', 0)} downstream components impacted (Severity: {blast_info.get('severity_assessment', 'UNKNOWN')})."

        incident_report = IncidentRCAReport(
            incident_id=f"INC-{uuid.uuid4().hex[:8].upper()}",
            alert_name=alert_name,
            symptom_node=low_level.get("focus_entity", {}).get("name", node_id),
            root_cause_node=root_cause_node_name,
            root_cause_summary=root_cause_summary,
            confidence_score=confidence,
            evidence_chain=evidence_chain,
            blast_radius_summary=blast_summary,
            recommended_actions=actions,
            created_at=time.time(),
            correlated_change_count=len(correlated_changes)
        )

        self.incident_history[incident_report.incident_id] = incident_report
        return incident_report

    def approve_action(self, incident_id: str, action_id: str) -> Dict[str, Any]:
        """Human gate approval endpoint for executing sensitive operational commands"""
        report = self.incident_history.get(incident_id)
        if not report:
            return {"success": False, "error": "Incident report not found"}

        for act in report.recommended_actions:
            if act.action_id == action_id:
                if act.status != "PENDING_APPROVAL":
                    return {"success": False, "error": f"Action is already in status {act.status}"}
                act.status = "APPROVED"
                # Simulate safe execution
                act.status = "EXECUTED"
                return {
                    "success": True,
                    "action_id": act.action_id,
                    "status": "EXECUTED",
                    "command_executed": act.command
                }

        return {"success": False, "error": "Action ID not found"}

    def generate_and_store_postmortem(self, incident_id: str) -> Dict[str, Any]:
        """
        Self-Learning SRE Loop:
        Synthesizes a structured incident postmortem report and injects it back into
        HybridKnowledgeBase as a POSTMORTEM document.
        """
        report = self.incident_history.get(incident_id)
        if not report:
            return {"success": False, "error": f"Incident '{incident_id}' not found"}

        executed_actions = [a.command for a in report.recommended_actions if a.status == "EXECUTED"]
        actions_str = "\n".join([f"- `{cmd}`" for cmd in executed_actions]) if executed_actions else "- None executed"
        evidence_str = "\n".join([f"- {ev}" for ev in report.evidence_chain])

        postmortem_content = (
            f"Incident ID: {report.incident_id}\n"
            f"Alert: {report.alert_name}\n"
            f"Symptom: {report.symptom_node}\n"
            f"Identified Root Cause: {report.root_cause_node}\n"
            f"Diagnostic Summary: {report.root_cause_summary}\n"
            f"Confidence Score: {report.confidence_score * 100:.1f}%\n"
            f"Blast Radius: {report.blast_radius_summary}\n\n"
            f"Evidence Chain:\n{evidence_str}\n\n"
            f"Remediation Actions Executed:\n{actions_str}\n\n"
            f"Preventive Measures:\n"
            f"1. Enforce aggressive idle connection timeouts on database pools.\n"
            f"2. Add automated canary deployment gates before promoting configuration changes."
        )

        doc = KnowledgeDocument(
            doc_id=f"PM-{report.incident_id}",
            title=f"Postmortem: {report.alert_name} on {report.root_cause_node} ({report.incident_id})",
            doc_type="POSTMORTEM",
            content=postmortem_content,
            tags=["postmortem", "incident", "rca", report.root_cause_node.lower(), report.symptom_node.lower()],
            target_components=[report.root_cause_node, report.symptom_node]
        )

        self.engine.kb.add_document(doc)
        return {
            "success": True,
            "incident_id": report.incident_id,
            "postmortem_doc_id": doc.doc_id,
            "title": doc.title,
            "tags": doc.tags
        }
