"""
Verification & Safety Module

Risk scoring, dry-run simulation, and remediation gate.
"""

from enum import Enum
from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field
from dataclasses import dataclass
import structlog

logger = structlog.get_logger()


class ActionType(str, Enum):
    ROLLBACK = "rollback"
    CONFIG_CHANGE = "config_change"
    SCALE = "scale"
    RESTART = "restart"
    CIRCUIT_BREAKER = "circuit_breaker"
    TRAFFIC_SHIFT = "traffic_shift"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskFactor(BaseModel):
    """Individual risk factor contributing to overall score"""
    name: str
    weight: float = Field(ge=0.0, le=1.0)
    score: float = Field(ge=0.0, le=1.0)
    rationale: str


class RiskAssessment(BaseModel):
    """Complete risk assessment for a remediation action"""
    action_type: ActionType
    target: str
    namespace: str
    overall_score: float = Field(ge=0.0, le=1.0)
    risk_level: RiskLevel
    factors: List[RiskFactor] = Field(default_factory=list)
    mitigation: List[str] = Field(default_factory=list)
    requires_approval: bool = True
    approval_threshold: float = 0.4  # Auto-approve below this


# Risk scoring rubric
RISK_RUBRIC = {
    ActionType.ROLLBACK: {
        "base_score": 0.15,
        "factors": {
            "revision_distance": {"weight": 0.3, "description": "How many revisions back"},
            "data_migrations": {"weight": 0.25, "description": "DB schema changes in between"},
            "stateful_service": {"weight": 0.2, "description": "Service has persistent state"},
            "recent_incidents": {"weight": 0.15, "description": "Rollback loops in history"},
            "canary_deployment": {"weight": 0.1, "description": "Canary can mitigate risk"},
        }
    },
    ActionType.CONFIG_CHANGE: {
        "base_score": 0.35,
        "factors": {
            "config_scope": {"weight": 0.3, "description": "Global vs service-specific"},
            "validation_available": {"weight": 0.25, "description": "Schema validation exists"},
            "hot_reload": {"weight": 0.2, "description": "Supports runtime reload"},
            "previous_failures": {"weight": 0.15, "description": "Similar changes caused issues"},
            "feature_flag": {"weight": 0.1, "description": "Can be toggled instantly"},
        }
    },
    ActionType.SCALE: {
        "base_score": 0.25,
        "factors": {
            "resource_quota": {"weight": 0.3, "description": "Cluster capacity headroom"},
            "hpa_exists": {"weight": 0.25, "description": "HPA can auto-revert"},
            "stateful_scaling": {"weight": 0.2, "description": "StatefulSet scaling complexity"},
            "startup_time": {"weight": 0.15, "description": "Pod readiness delay"},
            "cost_impact": {"weight": 0.1, "description": "Cloud cost implications"},
        }
    },
    ActionType.RESTART: {
        "base_score": 0.45,
        "factors": {
            "pod_disruption_budget": {"weight": 0.3, "description": "PDB protects availability"},
            "graceful_shutdown": {"weight": 0.25, "description": "App handles SIGTERM"},
            "connection_draining": {"weight": 0.2, "description": "Load balancer drains connections"},
            "startup_probe": {"weight": 0.15, "description": "Readiness validation exists"},
            "cascade_risk": {"weight": 0.1, "description": "Downstream dependency impact"},
        }
    },
    ActionType.CIRCUIT_BREAKER: {
        "base_score": 0.3,
        "factors": {
            "fallback_exists": {"weight": 0.35, "description": "Graceful degradation path"},
            "threshold_tuning": {"weight": 0.25, "description": "Properly calibrated thresholds"},
            "monitoring_visibility": {"weight": 0.2, "description": "CB state observable"},
            "recovery_strategy": {"weight": 0.2, "description": "Auto-recovery configured"},
        }
    },
    ActionType.TRAFFIC_SHIFT: {
        "base_score": 0.5,
        "factors": {
            "canary_analysis": {"weight": 0.3, "description": "Automated canary validation"},
            "rollback_speed": {"weight": 0.25, "description": "Instant rollback capability"},
            "session_affinity": {"weight": 0.2, "description": "Sticky sessions complexity"},
            "data_consistency": {"weight": 0.15, "description": "Cross-version data compatibility"},
            "observability": {"weight": 0.1, "description": "Real-time metrics during shift"},
        }
    },
}


class RiskScorer:
    """Calculate risk scores for remediation actions"""
    
    def __init__(self, rubric: Dict = None):
        self.rubric = rubric or RISK_RUBRIC
    
    def assess(self, action_type: ActionType, context: Dict[str, Any]) -> RiskAssessment:
        """Assess risk for a remediation action"""
        rubric_entry = self.rubric.get(action_type, {})
        base_score = rubric_entry.get("base_score", 0.5)
        factor_defs = rubric_entry.get("factors", {})
        
        factors = []
        weighted_score = base_score
        
        for factor_name, factor_def in factor_defs.items():
            weight = factor_def["weight"]
            # Get factor score from context (0-1, higher = riskier)
            factor_score = context.get(factor_name, 0.5)
            factors.append(RiskFactor(
                name=factor_name,
                weight=weight,
                score=factor_score,
                rationale=factor_def["description"]
            ))
            weighted_score += weight * (factor_score - 0.5)  # Adjust from base
        
        # Clamp
        overall_score = max(0.0, min(1.0, weighted_score))
        
        # Determine risk level
        if overall_score < 0.25:
            risk_level = RiskLevel.LOW
        elif overall_score < 0.5:
            risk_level = RiskLevel.MEDIUM
        elif overall_score < 0.75:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.CRITICAL
        
        # Generate mitigations
        mitigations = self._generate_mitigations(action_type, factors)
        
        # Determine if approval required
        requires_approval = overall_score > 0.4 or risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
        
        return RiskAssessment(
            action_type=action_type,
            target=context.get("target", "unknown"),
            namespace=context.get("namespace", "default"),
            overall_score=overall_score,
            risk_level=risk_level,
            factors=factors,
            mitigation=mitigations,
            requires_approval=requires_approval,
        )
    
    def _generate_mitigations(self, action_type: ActionType, factors: List[RiskFactor]) -> List[str]:
        """Generate mitigation suggestions based on risk factors"""
        mitigations = []
        
        for factor in factors:
            if factor.score > 0.6:  # High-risk factor
                if factor.name == "revision_distance":
                    mitigations.append("Validate rollback target with staging deployment first")
                elif factor.name == "data_migrations":
                    mitigations.append("Check for backward-compatible migrations; prepare DB rollback script")
                elif factor.name == "resource_quota":
                    mitigations.append("Request quota increase or scale down non-critical workloads first")
                elif factor.name == "pod_disruption_budget":
                    mitigations.append("Ensure PDB allows at least 1 replica; use maxUnavailable=0")
                elif factor.name == "graceful_shutdown":
                    mitigations.append("Add preStop hook with sleep; increase terminationGracePeriodSeconds")
                elif factor.name == "canary_analysis":
                    mitigations.append("Enable automated canary analysis with Argo Rollouts/Flagger")
                elif factor.name == "fallback_exists":
                    mitigations.append("Implement and test fallback response before enabling CB")
        
        # General mitigations
        if action_type == ActionType.ROLLBACK:
            mitigations.append("Verify previous revision is healthy in staging")
        elif action_type == ActionType.CONFIG_CHANGE:
            mitigations.append("Use ConfigMap/Secret validation webhook")
        elif action_type == ActionType.SCALE:
            mitigations.append("Set min/max replicas on HPA; monitor cluster autoscaler")
        elif action_type == ActionType.RESTART:
            mitigations.append("Use rolling restart with maxSurge/maxUnavailable")
        elif action_type == ActionType.CIRCUIT_BREAKER:
            mitigations.append("Configure half-open state for automatic recovery")
        elif action_type == ActionType.TRAFFIC_SHIFT:
            mitigations.append("Use mirror traffic for validation before full shift")
        
        return mitigations


class DryRunSimulator:
    """Simulate remediation actions in a safe environment"""
    
    def __init__(self):
        self.simulation_history = []
    
    async def simulate(
        self,
        action_type: ActionType,
        target: str,
        namespace: str,
        parameters: Dict[str, Any],
        cluster_state: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Run a dry-run simulation"""
        
        simulation_id = f"sim-{action_type.value}-{target}-{namespace}"
        
        # In production, this would:
        # 1. Spin up a test namespace/cluster
        # 2. Apply the change
        # 3. Run synthetic traffic
        # 4. Measure impact
        # 5. Clean up
        
        # For now, return a realistic mock result
        risk_scorer = RiskScorer()
        risk_assessment = risk_scorer.assess(action_type, {
            "target": target,
            "namespace": namespace,
            **parameters,
        })
        
        # Simulate outcomes based on action type
        predicted_impact = self._predict_impact(action_type, parameters, cluster_state or {})
        
        result = {
            "simulation_id": simulation_id,
            "action_type": action_type.value,
            "target": target,
            "namespace": namespace,
            "parameters": parameters,
            "risk_assessment": risk_assessment.model_dump(),
            "predicted_impact": predicted_impact,
            "success_probability": 1.0 - risk_assessment.overall_score,
            "estimated_duration_seconds": self._estimate_duration(action_type, parameters),
            "warnings": risk_assessment.mitigation,
            "rollback_available": True,
            "rollback_time_seconds": self._estimate_rollback_time(action_type),
        }
        
        self.simulation_history.append(result)
        return result
    
    def _predict_impact(self, action_type: ActionType, params: Dict, cluster_state: Dict) -> Dict[str, Any]:
        """Predict the impact of an action"""
        impacts = {
            "availability": "maintained",
            "latency_change_pct": 0,
            "error_rate_change_pct": 0,
            "throughput_change_pct": 0,
            "cost_change_pct": 0,
        }
        
        if action_type == ActionType.ROLLBACK:
            impacts["availability"] = "maintained"
            impacts["latency_change_pct"] = -5  # Usually improves
        elif action_type == ActionType.RESTART:
            impacts["availability"] = "brief_interruption"
            impacts["latency_change_pct"] = 10  # During restart
            impacts["error_rate_change_pct"] = 2
        elif action_type == ActionType.SCALE:
            impacts["availability"] = "maintained"
            impacts["cost_change_pct"] = params.get("replica_change", 0) * 20
        elif action_type == ActionType.TRAFFIC_SHIFT:
            impacts["availability"] = "maintained"
            impacts["latency_change_pct"] = 5  # During shift
        
        return impacts
    
    def _estimate_duration(self, action_type: ActionType, params: Dict) -> int:
        """Estimate action duration in seconds"""
        durations = {
            ActionType.ROLLBACK: 30,
            ActionType.CONFIG_CHANGE: 15,
            ActionType.SCALE: 60,
            ActionType.RESTART: 120,
            ActionType.CIRCUIT_BREAKER: 5,
            ActionType.TRAFFIC_SHIFT: 300,
        }
        return durations.get(action_type, 60)
    
    def _estimate_rollback_time(self, action_type: ActionType) -> int:
        """Estimate rollback time in seconds"""
        return self._estimate_duration(action_type, {}) // 2


class RemediationGate:
    """Gate that enforces safety policies before remediation execution"""
    
    def __init__(self, risk_scorer: RiskScorer = None, simulator: DryRunSimulator = None):
        self.risk_scorer = risk_scorer or RiskScorer()
        self.simulator = simulator or DryRunSimulator()
        self.policies = self._default_policies()
    
    def _default_policies(self) -> Dict[str, Any]:
        return {
            "max_risk_score_auto_approve": 0.4,
            "require_approval_for": [RiskLevel.HIGH, RiskLevel.CRITICAL],
            "blocked_actions": [],  # Action types never allowed
            "require_dry_run": True,
            "max_simultaneous_remediations": 3,
            "namespace_allowlist": ["default", "production", "staging"],
            "protected_namespaces": ["kube-system", "monitoring", "infra"],
        }
    
    async def evaluate(
        self,
        action_type: ActionType,
        target: str,
        namespace: str,
        parameters: Dict[str, Any],
        cluster_state: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Evaluate a remediation action through the safety gate"""
        
        # Check namespace policy
        if namespace in self.policies["protected_namespaces"]:
            return {
                "allowed": False,
                "reason": f"Namespace {namespace} is protected",
                "action": "blocked",
            }
        
        if namespace not in self.policies["namespace_allowlist"]:
            return {
                "allowed": False,
                "reason": f"Namespace {namespace} not in allowlist",
                "action": "blocked",
            }
        
        # Check if action is blocked
        if action_type in self.policies["blocked_actions"]:
            return {
                "allowed": False,
                "reason": f"Action {action_type.value} is blocked by policy",
                "action": "blocked",
            }
        
        # Run dry-run simulation if required
        simulation_result = None
        if self.policies["require_dry_run"]:
            simulation_result = await self.simulator.simulate(
                action_type, target, namespace, parameters, cluster_state
            )
        
        # Risk assessment
        risk_assessment = self.risk_scorer.assess(action_type, {
            "target": target,
            "namespace": namespace,
            **parameters,
        })
        
        # Decision
        allowed = True
        reason = "Approved"
        action = "execute"
        
        if risk_assessment.requires_approval:
            allowed = False
            reason = f"Requires human approval (risk: {risk_assessment.risk_level.value}, score: {risk_assessment.overall_score:.2f})"
            action = "await_approval"
        
        if risk_assessment.overall_score > 0.8:
            allowed = False
            reason = f"Risk score too high: {risk_assessment.overall_score:.2f}"
            action = "blocked"
        
        return {
            "allowed": allowed,
            "reason": reason,
            "action": action,
            "risk_assessment": risk_assessment.model_dump(),
            "simulation": simulation_result,
            "gate_policies": self.policies,
        }


# Convenience functions
async def score_remediation_risk(
    action_type: str,
    target: str,
    namespace: str,
    **context
) -> RiskAssessment:
    """Quick risk scoring for a remediation action"""
    scorer = RiskScorer()
    return scorer.assess(ActionType(action_type), {"target": target, "namespace": namespace, **context})


async def simulate_remediation_action(
    action_type: str,
    target: str,
    namespace: str,
    **parameters
) -> Dict[str, Any]:
    """Quick dry-run simulation"""
    simulator = DryRunSimulator()
    return await simulator.simulate(
        ActionType(action_type), target, namespace, parameters
    )


async def gate_remediation(
    action_type: str,
    target: str,
    namespace: str,
    **parameters
) -> Dict[str, Any]:
    """Run remediation through safety gate"""
    gate = RemediationGate()
    return await gate.evaluate(
        ActionType(action_type), target, namespace, parameters
    )