"""
Agent State Machine Definitions

State flow: DETECT -> CORRELATE -> ISOLATE -> VERIFY -> HUMAN_IN_LOOP -> REMEDIATE
"""

from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import uuid


class AgentState(str, Enum):
    """Deterministic state transitions for the remediation agent"""
    DETECT = "detect"
    CORRELATE = "correlate"
    ISOLATE = "isolate"
    VERIFY = "verify"
    HUMAN_IN_LOOP = "human_in_loop"
    REMEDIATE = "remediate"
    COMPLETE = "complete"
    FAILED = "failed"


class AnomalyType(str, Enum):
    ERROR_RATE_SPIKE = "error_rate_spike"
    LATENCY_DEGRADATION = "latency_degradation"
    THROUGHPUT_DROP = "throughput_drop"
    LOG_ENTROPY_BURST = "log_entropy_burst"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CASCADE_FAILURE = "cascade_failure"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class Incident(BaseModel):
    """Incoming incident from watchdog"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    service: str
    anomaly_type: AnomalyType
    severity: Severity
    metrics: Dict[str, float] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)


class CorrelationResult(BaseModel):
    """Result of correlation analysis"""
    incident_id: str
    correlated_incidents: List[str] = Field(default_factory=list)
    blast_radius: List[str] = Field(default_factory=list)  # affected services
    root_cause_hypothesis: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[str] = Field(default_factory=list)


class IsolationResult(BaseModel):
    """Result of root cause isolation"""
    incident_id: str
    root_cause: str
    root_cause_type: str  # "code", "config", "infrastructure", "dependency", "capacity"
    affected_components: List[str] = Field(default_factory=list)
    contributing_factors: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    git_commits: List[str] = Field(default_factory=list)  # recent commits to investigate


class VerificationResult(BaseModel):
    """Result of verification/simulation"""
    incident_id: str
    proposed_action: str
    action_type: str  # "rollback", "config_change", "scale", "restart", "circuit_breaker"
    risk_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    dry_run_result: Optional[Dict[str, Any]] = None
    rollback_plan: Optional[str] = None
    requires_approval: bool = True


class RemediationPlan(BaseModel):
    """Final remediation output"""
    incident_id: str
    plan_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    summary: str
    actions: List[Dict[str, Any]] = Field(default_factory=list)
    risk_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    git_patch: Optional[str] = None
    kubectl_commands: List[str] = Field(default_factory=list)
    pr_description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None


class AgentContext(BaseModel):
    """Full context passed through the state machine"""
    incident: Incident
    state: AgentState = AgentState.DETECT
    correlation: Optional[CorrelationResult] = None
    isolation: Optional[IsolationResult] = None
    verification: Optional[VerificationResult] = None
    remediation: Optional[RemediationPlan] = None
    error: Optional[str] = None
    iteration: int = 0
    max_iterations: int = 3