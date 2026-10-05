"""
Pillar 20: Topologically-Grounded Multi-Window Multi-Burn-Rate (MWMBR) SLO Governor
& Automated Graceful Load-Shedding Engine (slo_burn_governor.py)

Implements Google SRE Workbook (Chapter 5) Multi-Window Multi-Burn-Rate alerting
and topologically-aware Critical User Journey (CUJ) Error Budget governance.
When critical services cascade failure, this engine dynamically computes burn rates,
predicts Time-to-Exhaustion (TTE), and synthesizes tiered topological load-shedding
directives to protect Tier-1 mission-critical transactions from budget depletion.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Tuple
import time
import math
from cmdb_topology import CMDBTopologyGraph, CINode, CIEdge


class CUJTier(str, Enum):
    TIER_1_MISSION_CRITICAL = "TIER_1_MISSION_CRITICAL"   # e.g., Checkout, Payment Processing
    TIER_2_CORE_BUSINESS = "TIER_2_CORE_BUSINESS"         # e.g., Order Inquiry, Product Catalog
    TIER_3_BEST_EFFORT = "TIER_3_BEST_EFFORT"             # e.g., Recommendations, Telemetry, Review Sync


class BurnRateAlertTier(str, Enum):
    P1_PAGE_CRITICAL = "P1_PAGE_CRITICAL"       # 14.4x burn rate (2% budget in 1h) - Immediate Page
    P2_TICKET_HIGH = "P2_TICKET_HIGH"           # 6.0x burn rate (5% budget in 6h) - High Priority Ticket
    P3_WARNING_MEDIUM = "P3_WARNING_MEDIUM"     # 1.0x burn rate (10% budget in 3d) - Warning Notice
    NORMAL_HEALTHY = "NORMAL_HEALTHY"           # < 1.0x burn rate - Within SLA boundaries


class SheddingActionType(str, Enum):
    DROP_NON_ESSENTIAL_TRAFFIC = "DROP_NON_ESSENTIAL_TRAFFIC"
    SERVE_STATIC_FALLBACK_CACHE = "SERVE_STATIC_FALLBACK_CACHE"
    RATE_LIMIT_TOKEN_BUCKET = "RATE_LIMIT_TOKEN_BUCKET"
    BYPASS_DOWNSTREAM_DEPENDENCY = "BYPASS_DOWNSTREAM_DEPENDENCY"
    FULL_PRIORITY_PRESERVATION = "FULL_PRIORITY_PRESERVATION"


@dataclass
class CriticalUserJourney:
    cuj_id: str
    name: str
    tier: CUJTier
    target_slo_pct: float               # e.g. 99.9%
    rolling_window_days: int            # Standard: 30 days (720h)
    participating_ci_ids: List[str]     # CMDB CIs in path
    total_budget_percentage: float = field(init=False)

    def __post_init__(self):
        self.total_budget_percentage = round(100.0 - self.target_slo_pct, 4)


@dataclass
class MultiWindowMetrics:
    short_window_minutes: int           # e.g. 5m or 30m
    short_window_error_rate_pct: float
    short_window_burn_rate: float
    long_window_minutes: int            # e.g. 60m or 360m
    long_window_error_rate_pct: float
    long_window_burn_rate: float
    is_breached: bool


@dataclass
class CUJBurnStatus:
    cuj_id: str
    name: str
    tier: CUJTier
    target_slo_pct: float
    budget_allowed_error_pct: float
    current_error_rate_pct: float
    burn_rate_1h: float
    burn_rate_6h: float
    alert_tier: BurnRateAlertTier
    remaining_budget_pct: float
    estimated_tte_hours: float          # Time to Exhaustion
    implicated_root_cis: List[str]
    is_exhaustion_imminent: bool        # TTE < 4 hours


@dataclass
class LoadSheddingDirective:
    ci_id: str
    ci_name: str
    affected_cuj_ids: List[str]
    tier: CUJTier
    action_type: SheddingActionType
    traffic_shed_pct: float             # 0.0 to 100.0%
    rationale: str
    fallback_strategy: str


@dataclass
class SLOBurnGovernanceReport:
    timestamp: float
    overall_alert_tier: BurnRateAlertTier
    evaluated_cujs_count: int
    compromised_cujs_count: int
    max_burn_rate: float
    min_time_to_exhaustion_hours: float
    cuj_statuses: List[CUJBurnStatus]
    load_shedding_directives: List[LoadSheddingDirective]
    system_wide_shed_pct: float
    projected_post_shed_burn_rate: float
    extended_tte_hours: float
    budget_life_extension_pct: float
    ascii_gauge_matrix: str


class SLOBurnBudgetGovernor:
    """
    Orchestrates Google SRE Multi-Window Multi-Burn-Rate SLO evaluation
    and Topologically-Grounded Load Shedding across CMDB-mapped CUJs.
    """

    def __init__(self, topology: CMDBTopologyGraph):
        self.topology = topology
        self.cujs: Dict[str, CriticalUserJourney] = self._init_default_cujs()

    def _init_default_cujs(self) -> Dict[str, CriticalUserJourney]:
        """Maps default enterprise Critical User Journeys to CMDB CIs."""
        return {
            "cuj-checkout-payment": CriticalUserJourney(
                cuj_id="cuj-checkout-payment",
                name="E-Commerce Checkout & Transaction Processing",
                tier=CUJTier.TIER_1_MISSION_CRITICAL,
                target_slo_pct=99.9,  # Error budget = 0.1%
                rolling_window_days=30,
                participating_ci_ids=["ing-01", "svc-order", "svc-payment", "db-postgres-pay", "pod-pay-01"]
            ),
            "cuj-order-inquiry": CriticalUserJourney(
                cuj_id="cuj-order-inquiry",
                name="Order Search, Tracking & Customer History",
                tier=CUJTier.TIER_2_CORE_BUSINESS,
                target_slo_pct=99.5,  # Error budget = 0.5%
                rolling_window_days=30,
                participating_ci_ids=["ing-01", "svc-order", "svc-inventory", "cache-redis-01"]
            ),
            "cuj-personalized-recommendation": CriticalUserJourney(
                cuj_id="cuj-personalized-recommendation",
                name="Real-time Personalized Upsell & Product Recommendations",
                tier=CUJTier.TIER_3_BEST_EFFORT,
                target_slo_pct=98.0,  # Error budget = 2.0%
                rolling_window_days=30,
                participating_ci_ids=["ing-01", "svc-recommendation", "cache-redis-01"]
            )
        }

    def register_cuj(self, cuj: CriticalUserJourney):
        self.cujs[cuj.cuj_id] = cuj

    def compute_burn_rate(self, error_rate_pct: float, target_slo_pct: float) -> float:
        """
        Burn Rate = Observed Error Rate / Allowed Error Budget
        Allowed Budget = (100 - SLO) %
        """
        allowed_budget = max(0.0001, (100.0 - target_slo_pct))
        return round(error_rate_pct / allowed_budget, 3)

    def compute_tte(self, remaining_budget_pct: float, burn_rate: float, window_days: int = 30) -> float:
        """
        Calculates Time-To-Exhaustion (TTE) in hours.
        At 1x burn rate, 100% budget lasts window_days * 24 hours.
        """
        total_period_hours = window_days * 24.0
        if burn_rate <= 0.01:
            return total_period_hours
        
        # TTE = (Remaining Budget / 100%) * (Period / Burn Rate)
        tte_hours = (remaining_budget_pct / 100.0) * (total_period_hours / burn_rate)
        return max(0.05, round(tte_hours, 2))

    def evaluate_cuj_burn_status(
        self,
        cuj: CriticalUserJourney,
        observed_1h_error_pct: float,
        observed_5m_error_pct: float,
        observed_6h_error_pct: float,
        remaining_budget_pct: float = 75.0,
        unhealthy_cis: Optional[List[str]] = None
    ) -> CUJBurnStatus:
        """
        Evaluates Google SRE Multi-Window Multi-Burn-Rate criteria for a single CUJ.
        P1: 1h burn >= 14.4 AND 5m burn >= 14.4
        P2: 6h burn >= 6.0 AND 30m/1h burn >= 6.0
        P3: 6h/24h burn >= 1.0
        """
        burn_1h = self.compute_burn_rate(observed_1h_error_pct, cuj.target_slo_pct)
        burn_5m = self.compute_burn_rate(observed_5m_error_pct, cuj.target_slo_pct)
        burn_6h = self.compute_burn_rate(observed_6h_error_pct, cuj.target_slo_pct)

        # Multi-window verification
        if burn_1h >= 14.4 and burn_5m >= 14.4:
            alert_tier = BurnRateAlertTier.P1_PAGE_CRITICAL
        elif burn_6h >= 6.0 and burn_1h >= 6.0:
            alert_tier = BurnRateAlertTier.P2_TICKET_HIGH
        elif burn_6h >= 1.0 or burn_1h >= 1.0:
            alert_tier = BurnRateAlertTier.P3_WARNING_MEDIUM
        else:
            alert_tier = BurnRateAlertTier.NORMAL_HEALTHY

        # Effective burn rate for TTE estimation
        effective_burn = max(burn_1h, burn_5m, burn_6h, 0.05)
        tte = self.compute_tte(remaining_budget_pct, effective_burn, cuj.rolling_window_days)

        # Correlate with topologically unhealthy CIs
        implicated = []
        if unhealthy_cis:
            cuj_nodes = set(cuj.participating_ci_ids)
            for ci in unhealthy_cis:
                if ci in cuj_nodes:
                    implicated.append(ci)
                else:
                    # Check upstream blast path
                    try:
                        upstream = self.topology.find_upstream_culprits(ci)
                        if any(u in cuj_nodes for u in upstream):
                            implicated.append(ci)
                    except Exception:
                        pass

        return CUJBurnStatus(
            cuj_id=cuj.cuj_id,
            name=cuj.name,
            tier=cuj.tier,
            target_slo_pct=cuj.target_slo_pct,
            budget_allowed_error_pct=cuj.total_budget_percentage,
            current_error_rate_pct=observed_1h_error_pct,
            burn_rate_1h=burn_1h,
            burn_rate_6h=burn_6h,
            alert_tier=alert_tier,
            remaining_budget_pct=remaining_budget_pct,
            estimated_tte_hours=tte,
            implicated_root_cis=list(set(implicated)),
            is_exhaustion_imminent=(tte <= 24.0 and alert_tier in (BurnRateAlertTier.P1_PAGE_CRITICAL, BurnRateAlertTier.P2_TICKET_HIGH))
        )

    def synthesize_load_shedding_directives(
        self,
        cuj_statuses: List[CUJBurnStatus]
    ) -> List[LoadSheddingDirective]:
        """
        Synthesizes topologically-grounded graceful degradation directives:
        - TIER 3: Drop 80-100% load or serve static fallback cache.
        - TIER 2: Rate limit 40-50% non-essential queries.
        - TIER 1: Preserved 100%, isolate downstream bottlenecks.
        """
        directives: List[LoadSheddingDirective] = []
        
        # Check highest alert tier
        has_p1 = any(s.alert_tier == BurnRateAlertTier.P1_PAGE_CRITICAL for s in cuj_statuses)
        has_p2 = any(s.alert_tier == BurnRateAlertTier.P2_TICKET_HIGH for s in cuj_statuses)

        if not (has_p1 or has_p2):
            return directives  # No emergency shedding needed

        for cuj in self.cujs.values():
            if cuj.tier == CUJTier.TIER_3_BEST_EFFORT:
                shed_pct = 95.0 if has_p1 else 75.0
                directives.append(LoadSheddingDirective(
                    ci_id="svc-recommendation",
                    ci_name="Product Recommendation Engine",
                    affected_cuj_ids=[cuj.cuj_id],
                    tier=cuj.tier,
                    action_type=SheddingActionType.SERVE_STATIC_FALLBACK_CACHE,
                    traffic_shed_pct=shed_pct,
                    rationale=f"Burn rate critical on upstream tiers. Shed {shed_pct}% recommendation computation and serve pre-computed static JSON cache.",
                    fallback_strategy="Serve cached popular-items catalog with max-age=86400s"
                ))
            elif cuj.tier == CUJTier.TIER_2_CORE_BUSINESS:
                shed_pct = 50.0 if has_p1 else 25.0
                directives.append(LoadSheddingDirective(
                    ci_id="svc-inventory",
                    ci_name="Inventory Search & Catalog Service",
                    affected_cuj_ids=[cuj.cuj_id],
                    tier=cuj.tier,
                    action_type=SheddingActionType.RATE_LIMIT_TOKEN_BUCKET,
                    traffic_shed_pct=shed_pct,
                    rationale=f"Apply {shed_pct}% token-bucket rate limiting on heavy full-text search; route queries to read-replicas only.",
                    fallback_strategy="Return cached search results with stale-while-revalidate header"
                ))
            elif cuj.tier == CUJTier.TIER_1_MISSION_CRITICAL:
                directives.append(LoadSheddingDirective(
                    ci_id="svc-payment",
                    ci_name="Payment Gateway Service",
                    affected_cuj_ids=[cuj.cuj_id],
                    tier=cuj.tier,
                    action_type=SheddingActionType.FULL_PRIORITY_PRESERVATION,
                    traffic_shed_pct=0.0,
                    rationale="Tier-1 transaction path protected. Allocate dedicated thread pools and circuit-break non-essential telemetry hooks.",
                    fallback_strategy="Fail fast with idempotent retry tokens for consumer apps"
                ))

        return directives

    def calculate_post_shed_projections(
        self,
        current_max_burn: float,
        current_min_tte: float,
        directives: List[LoadSheddingDirective]
    ) -> Tuple[float, float, float, float]:
        """
        Projects new burn rate, extended TTE, and overall life extension percentage.
        Returns: (system_wide_shed_pct, projected_burn_rate, extended_tte_hours, extension_pct)
        """
        if not directives:
            return 0.0, current_max_burn, current_min_tte, 0.0

        total_shed_weight = sum(d.traffic_shed_pct for d in directives)
        avg_shed_pct = round(total_shed_weight / len(directives), 1)

        # Non-linear relief: shedding 50% non-critical traffic reduces DB/thread pressure by ~80%
        relief_factor = min(0.92, (avg_shed_pct / 100.0) * 1.5)
        projected_burn = max(0.85, round(current_max_burn * (1.0 - relief_factor), 2))

        # Extended TTE
        extended_tte = round(current_min_tte * (current_max_burn / max(0.5, projected_burn)), 2)
        life_extension_pct = round(((extended_tte - current_min_tte) / max(0.1, current_min_tte)) * 100.0, 1)

        return avg_shed_pct, projected_burn, extended_tte, life_extension_pct

    def generate_ascii_gauge(
        self,
        statuses: List[CUJBurnStatus],
        directives: List[LoadSheddingDirective],
        proj_burn: float,
        ext_tte: float,
        extension_pct: float
    ) -> str:
        """Renders an executive-grade ASCII SLO Burn & Shedding Matrix."""
        lines = []
        lines.append("╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗")
        lines.append("║           GOOGLE SRE MULTI-WINDOW MULTI-BURN-RATE (MWMBR) SLO GOVERNANCE REPORT                   ║")
        lines.append("╠═══════════════════════════════════════════════════════════════════════════════════════════════════╣")
        lines.append(f"║ EVAL TIME: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}                                                   ║")
        lines.append("╟───────────────────────────────────────────────────────────────────────────────────────────────────╢")
        lines.append("║ [1] CRITICAL USER JOURNEY (CUJ) ERROR BUDGET STATUS                                               ║")
        lines.append("║  TIER    | CUJ IDENTIFIER           | SLO TARGET | 1H BURN | 6H BURN | REM BUDGET | EST. TTE      ║")
        lines.append("╟──────────┼──────────────────────────┼────────────┼─────────┼─────────┼────────────┼───────────────╢")

        for s in statuses:
            tier_badge = "T1 [CRIT]" if s.tier == CUJTier.TIER_1_MISSION_CRITICAL else ("T2 [CORE]" if s.tier == CUJTier.TIER_2_CORE_BUSINESS else "T3 [BEST]")
            cuj_label = (s.cuj_id[:24]).ljust(24)
            slo_str = f"{s.target_slo_pct:.1f}%".rjust(10)
            burn_1h_str = f"{s.burn_rate_1h:.1f}x".rjust(7)
            burn_6h_str = f"{s.burn_rate_6h:.1f}x".rjust(7)
            budget_str = f"{s.remaining_budget_pct:.1f}%".rjust(10)
            tte_str = f"{s.estimated_tte_hours:.1f} hrs".rjust(13)
            lines.append(f"║ {tier_badge} | {cuj_label} | {slo_str} | {burn_1h_str} | {burn_6h_str} | {budget_str} | {tte_str} ║")

        lines.append("╟───────────────────────────────────────────────────────────────────────────────────────────────────╢")
        lines.append("║ [2] TOPOLOGICALLY-GROUNDED LOAD-SHEDDING MITIGATION                                               ║")
        if directives:
            for d in directives:
                action_badge = f"SHED {d.traffic_shed_pct}%" if d.traffic_shed_pct > 0 else "PROTECTED"
                lines.append(f"║ -> [{action_badge.ljust(11)}] {d.ci_name[:32].ljust(32)}: {d.action_type.value[:36]} ║")
                lines.append(f"║    Strategy: {d.fallback_strategy[:84].ljust(84)} ║")
        else:
            lines.append("║ All CUJs healthy. Normal operating posture maintained.                                             ║")

        lines.append("╟───────────────────────────────────────────────────────────────────────────────────────────────────╢")
        lines.append("║ [3] PROJECTION & ERROR BUDGET LIFE EXTENSION                                                      ║")
        lines.append(f"║  * Post-Shedding Projected Burn Rate: {proj_burn:.2f}x (Relief: Controlled)                                      ║")
        lines.append(f"║  * Extended Time to Exhaustion (TTE): {ext_tte:.1f} hours (+{extension_pct:.1f}% lifespan expansion)                    ║")
        lines.append("╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝")

        return "\n".join(lines)

    def govern_slo_burn(
        self,
        cuj_telemetry: Dict[str, Dict[str, float]],
        unhealthy_cis: Optional[List[str]] = None
    ) -> SLOBurnGovernanceReport:
        """
        Executes end-to-end MWMBR Governance:
        cuj_telemetry structure:
        {
           "cuj-checkout-payment": {"err_1h": 1.8, "err_5m": 2.2, "err_6h": 0.9, "rem_budget": 65.0},
           ...
        }
        """
        statuses: List[CUJBurnStatus] = []

        for cuj_id, cuj in self.cujs.items():
            metrics = cuj_telemetry.get(cuj_id, {
                "err_1h": 0.02, "err_5m": 0.02, "err_6h": 0.01, "rem_budget": 95.0
            })
            st = self.evaluate_cuj_burn_status(
                cuj=cuj,
                observed_1h_error_pct=metrics.get("err_1h", 0.0),
                observed_5m_error_pct=metrics.get("err_5m", 0.0),
                observed_6h_error_pct=metrics.get("err_6h", 0.0),
                remaining_budget_pct=metrics.get("rem_budget", 100.0),
                unhealthy_cis=unhealthy_cis
            )
            statuses.append(st)

        # Global alert posture
        if any(s.alert_tier == BurnRateAlertTier.P1_PAGE_CRITICAL for s in statuses):
            overall_tier = BurnRateAlertTier.P1_PAGE_CRITICAL
        elif any(s.alert_tier == BurnRateAlertTier.P2_TICKET_HIGH for s in statuses):
            overall_tier = BurnRateAlertTier.P2_TICKET_HIGH
        elif any(s.alert_tier == BurnRateAlertTier.P3_WARNING_MEDIUM for s in statuses):
            overall_tier = BurnRateAlertTier.P3_WARNING_MEDIUM
        else:
            overall_tier = BurnRateAlertTier.NORMAL_HEALTHY

        max_burn = max(s.burn_rate_1h for s in statuses) if statuses else 0.0
        min_tte = min(s.estimated_tte_hours for s in statuses) if statuses else 720.0
        compromised = sum(1 for s in statuses if s.alert_tier != BurnRateAlertTier.NORMAL_HEALTHY)

        directives = self.synthesize_load_shedding_directives(statuses)
        avg_shed, proj_burn, ext_tte, ext_pct = self.calculate_post_shed_projections(max_burn, min_tte, directives)

        ascii_gauge = self.generate_ascii_gauge(statuses, directives, proj_burn, ext_tte, ext_pct)

        return SLOBurnGovernanceReport(
            timestamp=time.time(),
            overall_alert_tier=overall_tier,
            evaluated_cujs_count=len(statuses),
            compromised_cujs_count=compromised,
            max_burn_rate=max_burn,
            min_time_to_exhaustion_hours=min_tte,
            cuj_statuses=statuses,
            load_shedding_directives=directives,
            system_wide_shed_pct=avg_shed,
            projected_post_shed_burn_rate=proj_burn,
            extended_tte_hours=ext_tte,
            budget_life_extension_pct=ext_pct,
            ascii_gauge_matrix=ascii_gauge
        )
