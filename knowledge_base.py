"""
SRE Knowledge Base & Hybrid Retrieval (knowledge_base.py)
Implements Hybrid Vector + Keyword (BM25-style) Retrieval for:
- SRE Runbooks / SOPs
- Historical Postmortem & RCA Incident Reports
- Architectural Best Practices
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import math
import re


@dataclass
class KnowledgeDocument:
    doc_id: str
    title: str
    doc_type: str  # RUNBOOK, POSTMORTEM, ARCH_DOC
    content: str
    tags: List[str]
    target_components: List[str]


class HybridKnowledgeBase:
    """
    Simulates production Hybrid Search (Dense Semantic + Sparse Keyword)
    without requiring heavy external C++ vector bindings on developer workstations.
    """

    def __init__(self):
        self.documents: Dict[str, KnowledgeDocument] = {}

    def add_document(self, doc: KnowledgeDocument):
        self.documents[doc.doc_id] = doc

    def _tokenize(self, text: str) -> List[str]:
        return re.findall(r'[a-zA-Z0-9_\u4e00-\u9fa5]+', text.lower())

    def search(self, query: str, target_component: Optional[str] = None, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Hybrid ranking combining:
        1. Term Frequency overlap (BM25 approximate)
        2. Exact tag & component matching boost
        """
        query_tokens = set(self._tokenize(query))
        results = []

        for doc in self.documents.values():
            score = 0.0
            doc_tokens = self._tokenize(doc.title + " " + doc.content)
            total_doc_words = len(doc_tokens) or 1

            # Keyword matching score
            for q_token in query_tokens:
                count = doc_tokens.count(q_token)
                if count > 0:
                    tf = count / total_doc_words
                    score += (tf * 10.0) + 1.0

            # Component affinity bonus
            if target_component:
                t_lower = target_component.lower()
                for comp in doc.target_components:
                    if comp.lower() in t_lower or t_lower in comp.lower():
                        score += 5.0

            # Tag bonus
            for tag in doc.tags:
                if tag.lower() in query.lower():
                    score += 2.0

            if score > 0:
                results.append({
                    "doc_id": doc.doc_id,
                    "title": doc.title,
                    "doc_type": doc.doc_type,
                    "score": round(score, 3),
                    "target_components": doc.target_components,
                    "content": doc.content
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]


def build_sample_sre_knowledge_base() -> HybridKnowledgeBase:
    """Populates realistic incident postmortems and SRE runbooks"""
    kb = HybridKnowledgeBase()

    kb.add_document(KnowledgeDocument(
        doc_id="RB-PG-001",
        title="PostgreSQL Connection Pool Exhaustion Runbook",
        doc_type="RUNBOOK",
        tags=["postgres", "rds", "connection_pool", "database", "500"],
        target_components=["rds-postgres-payment-primary", "db-postgres-pay"],
        content="""
Symptom: Services report 'connection timeout' or 'remaining connection slots are reserved for non-superuser connections'.
Root Cause Candidates:
1. Microservice connection leak or unclosed database sessions in worker pools.
2. Sudden traffic spike without PgBouncer multiplexing.
3. Long running transactions holding locks.
Action Items:
1. Check active connections: SELECT count(*) FROM pg_stat_activity WHERE state = 'active';
2. Kill idle/stuck transactions: SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND state_change < now() - INTERVAL '5 minutes';
3. Trigger PgBouncer pool recycle or scale max_connections dynamically in parameter group.
4. Scale up Pod replicas ONLY after connection pooler is stabilised.
"""
    ))

    kb.add_document(KnowledgeDocument(
        doc_id="PM-2025-08-PAY",
        title="Postmortem: Payment Gateway Cascading Outage (Aug 2025)",
        doc_type="POSTMORTEM",
        tags=["payment", "cascading_failure", "postgres", "timeout"],
        target_components=["payment-gateway-service", "order-api-service"],
        content="""
Incident Summary: Order creation failed with HTTP 504 Gateway Timeout for 42 minutes.
Timeline & Root Cause:
- Database 'rds-postgres-payment-primary' reached 500/500 connection limit due to unindexed query in new deployment.
- Payment pods (payment-pod) blocked on DB acquire, exhausting thread pool and failing readiness probes.
- Ingress continued routing to dead pods, causing order-api-service to cascade fail.
Remediation: Terminated idle DB connections, rolled back deployment v2.14, and enforced PgBouncer transaction pooling.
"""
    ))

    kb.add_document(KnowledgeDocument(
        doc_id="RB-K8S-004",
        title="Kubernetes Pod CrashLoopBackOff & High Restart Diagnostic",
        doc_type="RUNBOOK",
        tags=["k8s", "crashloop", "pod", "oom", "readiness"],
        target_components=["payment-pod-7df9f8-1", "payment-pod-7df9f8-2"],
        content="""
Diagnostic Steps:
1. Inspect termination reason: kubectl describe pod <pod_name>
2. If OOMKilled: Check memory limits in deployment spec; increase memory limit by 50%.
3. If CrashLoop without OOM: Check application startup logs: kubectl logs <pod_name> --previous.
4. If failed liveness/readiness probe: Verify upstream database and redis network connectivity.
"""
    ))

    return kb
