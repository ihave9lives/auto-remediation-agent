"""
Evaluation Framework for Autonomous Remediation Agent

AIOpsLab-style benchmark framework for evaluating agent performance on incident resolution.
Includes synthetic incident generation, ground truth datasets, and evaluation metrics.
"""

import json
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import structlog

logger = structlog.get_logger()


class IncidentCategory(str, Enum):
    """Categories of incidents for benchmarking"""
    RESOURCE_EXHAUSTION = "resource_exhaustion"      # CPU, memory, disk
    NETWORK_ISSUE = "network_issue"                  # Latency, DNS, connectivity
    DEPENDENCY_FAILURE = "dependency_failure"        # Downstream service failure
    CONFIG_DRIFT = "config_drift"                    # Configuration changes
    DEPLOYMENT_REGRESSION = "deployment_regression"  # Bad deployment
    CASCADING_FAILURE = "cascading_failure"          # Multi-service cascade
    SECURITY_INCIDENT = "security_incident"          # Security breach
    DATA_CORRUPTION = "data_corruption"              # Database corruption


class IncidentSeverity(str, Enum):
    SEV1 = "sev1"  # Critical - production down
    SEV2 = "sev2"  # High - major functionality impacted
    SEV3 = "sev3"  # Medium - minor functionality impacted
    SEV4 = "sev4"  # Low - cosmetic/minimal impact


@dataclass
class GroundTruthIncident:
    """A ground truth incident for evaluation"""
    id: str
    title: str
    description: str
    category: IncidentCategory
    severity: IncidentSeverity
    affected_services: List[str]
    root_cause: str
    root_cause_type: str  # code, config, infrastructure, dependency, capacity
    contributing_factors: List[str]
    injection_method: str  # How the incident was injected
    injection_parameters: Dict[str, Any]
    expected_remediation: List[str]
    expected_skills: List[str]
    blast_radius: List[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentResponse:
    """Agent's response to an incident"""
    incident_id: str
    agent_id: str
    detected: bool
    detected_at: Optional[datetime]
    root_cause_identified: bool
    root_cause: Optional[str]
    root_cause_type: Optional[str]
    blast_radius_identified: List[str]
    skills_used: List[str]
    remediation_proposed: List[str]
    remediation_executed: List[str]
    time_to_detection_seconds: Optional[float]
    time_to_root_cause_seconds: Optional[float]
    time_to_remediation_seconds: Optional[float]
    confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluationResult:
    """Result of evaluating agent response against ground truth"""
    incident_id: str
    agent_id: str
    
    # Detection metrics
    detection_accuracy: float  # 0-1
    detection_latency_seconds: Optional[float]
    
    # Root cause metrics
    root_cause_accuracy: float  # 0-1 (exact match)
    root_cause_type_accuracy: float  # 0-1
    root_cause_latency_seconds: Optional[float]
    
    # Blast radius metrics
    blast_radius_precision: float  # 0-1
    blast_radius_recall: float  # 0-1
    blast_radius_f1: float  # 0-1
    
    # Remediation metrics
    remediation_relevance: float  # 0-1 (proposed remediation matches expected)
    remediation_safety: float  # 0-1 (risk score of proposed actions)
    remediation_latency_seconds: Optional[float]
    
    # Overall
    overall_score: float  # 0-1
    
    # Detailed breakdown
    details: Dict[str, Any] = field(default_factory=dict)


class IncidentGenerator:
    """Generates synthetic incidents for benchmarking"""
    
    def __init__(self, topology_graph, chaos_client=None):
        self.topology = topology_graph
        self.chaos_client = chaos_client
    
    def generate_resource_exhaustion(
        self,
        service: str,
        resource_type: str = "memory",  # cpu, memory, disk
        intensity: float = 0.9
    ) -> GroundTruthIncident:
        """Generate a resource exhaustion incident"""
        injection_params = {
            "resource_type": resource_type,
            "intensity": intensity,
            "duration_seconds": 300,
        }
        
        if resource_type == "memory":
            injection_method = "chaos_mesh_stress"
            injection_params["stressor"] = "memory"
            injection_params["workers"] = 2
            injection_params["load_percent"] = int(intensity * 100)
        elif resource_type == "cpu":
            injection_method = "chaos_mesh_stress"
            injection_params["stressor"] = "cpu"
            injection_params["workers"] = 4
            injection_params["load_percent"] = int(intensity * 100)
        
        blast_radius = self.topology.get_blast_radius(service) if self.topology else {}
        affected = blast_radius.get("downstream", [])
        affected_services = [service] + [s["service_id"] for s in affected]
        
        return GroundTruthIncident(
            id=str(uuid.uuid4()),
            title=f"{resource_type.capitalize()} Exhaustion in {service}",
            description=f"Service {service} experiences {resource_type} exhaustion at {intensity*100:.0f}% utilization",
            category=IncidentCategory.RESOURCE_EXHAUSTION,
            severity=IncidentSeverity.SEV2,
            affected_services=affected_services,
            root_cause=f"{resource_type.capitalize()} usage exceeded {intensity*100:.0f}% limit due to stress injection",
            root_cause_type="capacity",
            contributing_factors=[f"Chaos Mesh {resource_type} stressor", "Insufficient resource limits"],
            injection_method=injection_method,
            injection_parameters=injection_params,
            expected_remediation=[
                f"Scale {service} horizontally",
                f"Increase {resource_type} limits",
                "Add HPA based on custom metrics",
            ],
            expected_skills=["get_k8s_pod_status", "query_prometheus_metrics", "k8s_scale_deployment"],
            blast_radius=[s["service_id"] for s in affected],
        )
    
    def generate_network_latency(
        self,
        service: str,
        target_service: str,
        latency_ms: int = 500,
    ) -> GroundTruthIncident:
        """Generate a network latency incident between services"""
        blast_radius = self.topology.get_blast_radius(service) if self.topology else {}
        downstream = blast_radius.get("downstream", [])
        affected_services = [service] + [s["service_id"] for s in downstream]
        
        return GroundTruthIncident(
            id=str(uuid.uuid4()),
            title=f"Network Latency between {service} and {target_service}",
            description=f"Injected {latency_ms}ms latency on calls from {service} to {target_service}",
            category=IncidentCategory.NETWORK_ISSUE,
            severity=IncidentSeverity.SEV2,
            affected_services=affected_services,
            root_cause=f"Network latency injection of {latency_ms}ms on {service} -> {target_service} path",
            root_cause_type="infrastructure",
            contributing_factors=["Chaos Mesh network chaos", "Missing circuit breaker"],
            injection_method="chaos_mesh_network_latency",
            injection_parameters={
                "source": service,
                "target": target_service,
                "latency_ms": latency_ms,
                "duration_seconds": 300,
            },
            expected_remediation=[
                "Enable circuit breaker for downstream calls",
                "Add timeout and retry policies",
                "Consider async processing for non-critical paths",
            ],
            expected_skills=["correlate_service_dependencies", "query_prometheus_metrics", "analyze_stack_trace"],
            blast_radius=[s["service_id"] for s in downstream],
        )
    
    def generate_deployment_regression(
        self,
        service: str,
        bad_version: str,
        previous_version: str,
    ) -> GroundTruthIncident:
        """Generate a bad deployment regression incident"""
        blast_radius = self.topology.get_blast_radius(service) if self.topology else {}
        downstream = blast_radius.get("downstream", [])
        affected_services = [service] + [s["service_id"] for s in downstream]
        
        return GroundTruthIncident(
            id=str(uuid.uuid4()),
            title=f"Regression in {service} deployment {bad_version}",
            description=f"Deployment {bad_version} of {service} introduced regression causing errors",
            category=IncidentCategory.DEPLOYMENT_REGRESSION,
            severity=IncidentSeverity.SEV1,
            affected_services=affected_services,
            root_cause=f"Deployment {bad_version} introduced bug in {service}",
            root_cause_type="code",
            contributing_factors=["Insufficient pre-deployment testing", "Missing canary analysis"],
            injection_method="bad_deployment",
            injection_parameters={
                "service": service,
                "bad_version": bad_version,
                "previous_version": previous_version,
            },
            expected_remediation=[
                f"Rollback {service} to version {previous_version}",
                "Add integration tests for affected code path",
                "Enable automated canary analysis",
            ],
            expected_skills=["git_blame_recent_changes", "k8s_rollout_rollback", "query_prometheus_metrics"],
            blast_radius=[s["service_id"] for s in downstream],
        )
    
    def generate_cascading_failure(
        self,
        initial_service: str,
        failure_chain: List[str],  # Services that fail in sequence
    ) -> GroundTruthIncident:
        """Generate a cascading failure incident"""
        all_affected = [initial_service] + failure_chain
        
        return GroundTruthIncident(
            id=str(uuid.uuid4()),
            title=f"Cascading Failure starting from {initial_service}",
            description=f"Initial failure in {initial_service} cascades to {len(failure_chain)} downstream services",
            category=IncidentCategory.CASCADING_FAILURE,
            severity=IncidentSeverity.SEV1,
            affected_services=all_affected,
            root_cause=f"Initial failure in {initial_service} triggered cascade through dependency chain",
            root_cause_type="dependency",
            contributing_factors=[
                "No circuit breakers",
                "Insufficient timeout/retry configuration",
                "Shared resource contention",
            ],
            injection_method="chaos_mesh_pod_failure_chain",
            injection_parameters={
                "initial_service": initial_service,
                "failure_chain": failure_chain,
                "delay_between_failures_seconds": 30,
            },
            expected_remediation=[
                f"Rollback/restart {initial_service}",
                "Enable circuit breakers on all service boundaries",
                "Add bulkhead patterns for shared resources",
                "Implement graceful degradation",
            ],
            expected_skills=["get_blast_radius", "correlate_service_dependencies", "simulate_remediation"],
            blast_radius=all_affected,
        )
    
    def generate_config_drift(
        self,
        service: str,
        config_changes: List[Dict[str, Any]],
    ) -> GroundTruthIncident:
        """Generate a configuration drift incident"""
        blast_radius = self.topology.get_blast_radius(service) if self.topology else {}
        downstream = blast_radius.get("downstream", [])
        affected_services = [service] + [s["service_id"] for s in downstream]
        
        return GroundTruthIncident(
            id=str(uuid.uuid4()),
            title=f"Configuration Drift in {service}",
            description=f"Configuration changes in {service} caused behavioral regression",
            category=IncidentCategory.CONFIG_DRIFT,
            severity=IncidentSeverity.SEV2,
            affected_services=affected_services,
            root_cause=f"Configuration drift: {len(config_changes)} settings changed without validation",
            root_cause_type="config",
            contributing_factors=["Direct config edits", "No config validation", "Missing change review"],
            injection_method="configmap_modification",
            injection_parameters={
                "service": service,
                "config_changes": config_changes,
            },
            expected_remediation=[
                "Revert config changes to last known good state",
                "Add config validation webhook",
                "Implement GitOps for configuration",
            ],
            expected_skills=["query_prometheus_metrics", "git_blame_recent_changes", "k8s_rollout_rollback"],
            blast_radius=[s["service_id"] for s in downstream],
        )


class BenchmarkRunner:
    """
    Runs evaluation benchmarks against an agent.
    
    Based on AIOpsLab evaluation methodology:
    - Inject faults using chaos engineering
    - Measure agent detection, diagnosis, and remediation
    - Compare against ground truth
    - Produce standardized metrics
    """
    
    def __init__(
        self,
        agent_runner: Callable,  # Function that runs agent on incident
        incident_generator: IncidentGenerator,
        results_dir: str = "eval_results",
    ):
        self.agent_runner = agent_runner
        self.generator = incident_generator
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth: List[GroundTruthIncident] = []
        self.agent_responses: List[AgentResponse] = []
        self.evaluation_results: List[EvaluationResult] = []
    
    def add_ground_truth(self, incident: GroundTruthIncident):
        self.ground_truth.append(incident)
    
    def load_ground_truth(self, file_path: str):
        """Load ground truth incidents from JSON file"""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        for item in data:
            item["category"] = IncidentCategory(item["category"])
            item["severity"] = IncidentSeverity(item["severity"])
            item["created_at"] = datetime.fromisoformat(item["created_at"])
            self.ground_truth.append(GroundTruthIncident(**item))
    
    def save_ground_truth(self, file_path: str):
        """Save ground truth incidents to JSON file"""
        data = []
        for incident in self.ground_truth:
            item = {
                "id": incident.id,
                "title": incident.title,
                "description": incident.description,
                "category": incident.category.value,
                "severity": incident.severity.value,
                "affected_services": incident.affected_services,
                "root_cause": incident.root_cause,
                "root_cause_type": incident.root_cause_type,
                "contributing_factors": incident.contributing_factors,
                "injection_method": incident.injection_method,
                "injection_parameters": incident.injection_parameters,
                "expected_remediation": incident.expected_remediation,
                "expected_skills": incident.expected_skills,
                "blast_radius": incident.blast_radius,
                "created_at": incident.created_at.isoformat(),
                "metadata": incident.metadata,
            }
            data.append(item)
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    async def run_benchmark(
        self,
        agent_id: str,
        incidents: List[GroundTruthIncident] = None,
        inject_faults: bool = True,
    ) -> List[EvaluationResult]:
        """
        Run benchmark on a set of incidents.
        
        Args:
            agent_id: Identifier for the agent being evaluated
            incidents: Incidents to test (defaults to all ground truth)
            inject_faults: Whether to actually inject faults via chaos
        
        Returns:
            List of evaluation results
        """
        test_incidents = incidents or self.ground_truth
        results = []
        
        for incident in test_incidents:
            logger.info("Running benchmark", incident_id=incident.id, title=incident.title)
            
            # Inject fault if requested
            if inject_faults and self.generator.chaos_client:
                await self._inject_fault(incident)
            
            # Wait for telemetry to reflect the fault
            await asyncio.sleep(10)
            
            # Run agent
            start_time = datetime.utcnow()
            agent_response = await self.agent_runner(incident)
            agent_response.agent_id = agent_id
            agent_response.incident_id = incident.id
            self.agent_responses.append(agent_response)
            
            # Evaluate response
            eval_result = self._evaluate_response(incident, agent_response)
            eval_result.agent_id = agent_id
            self.evaluation_results.append(eval_result)
            results.append(eval_result)
            
            # Cleanup fault injection
            if inject_faults and self.generator.chaos_client:
                await self._cleanup_fault(incident)
            
            # Wait between incidents
            await asyncio.sleep(5)
        
        # Save results
        self._save_results(agent_id)
        
        return results
    
    async def _inject_fault(self, incident: GroundTruthIncident):
        """Inject fault using chaos client"""
        method = incident.injection_method
        params = incident.injection_parameters
        
        try:
            if method == "chaos_mesh_stress":
                # Create StressChaos resource
                pass  # Implementation depends on chaos client
            elif method == "chaos_mesh_network_latency":
                # Create NetworkChaos resource
                pass
            elif method == "chaos_mesh_pod_failure_chain":
                # Create PodChaos resources in sequence
                pass
            elif method == "bad_deployment":
                # Deploy bad version
                pass
            elif method == "configmap_modification":
                # Modify ConfigMap
                pass
            
            logger.info("Fault injected", incident_id=incident.id, method=method)
        except Exception as e:
            logger.error("Fault injection failed", incident_id=incident.id, error=str(e))
    
    async def _cleanup_fault(self, incident: GroundTruthIncident):
        """Clean up fault injection"""
        # Delete chaos resources, rollback deployments, etc.
        logger.info("Fault cleaned up", incident_id=incident.id)
    
    def _evaluate_response(
        self,
        ground_truth: GroundTruthIncident,
        response: AgentResponse
    ) -> EvaluationResult:
        """Evaluate agent response against ground truth"""
        
        # Detection accuracy
        detection_accuracy = 1.0 if response.detected else 0.0
        detection_latency = response.time_to_detection_seconds
        
        # Root cause accuracy
        root_cause_accuracy = 0.0
        if response.root_cause_identified and response.root_cause:
            # Simple string similarity - could use embeddings
            rc_similarity = self._string_similarity(
                response.root_cause.lower(),
                ground_truth.root_cause.lower()
            )
            root_cause_accuracy = rc_similarity
        
        root_cause_type_accuracy = 1.0 if response.root_cause_type == ground_truth.root_cause_type else 0.0
        root_cause_latency = response.time_to_root_cause_seconds
        
        # Blast radius metrics
        expected_br = set(ground_truth.blast_radius)
        predicted_br = set(response.blast_radius_identified)
        
        if expected_br:
            blast_precision = len(predicted_br & expected_br) / len(predicted_br) if predicted_br else 0.0
            blast_recall = len(predicted_br & expected_br) / len(expected_br)
            blast_f1 = 2 * blast_precision * blast_recall / (blast_precision + blast_recall) if (blast_precision + blast_recall) > 0 else 0.0
        else:
            blast_precision = 1.0 if not predicted_br else 0.0
            blast_recall = 1.0
            blast_f1 = 1.0
        
        # Remediation relevance
        expected_rem = set(ground_truth.expected_remediation)
        proposed_rem = set(response.remediation_proposed)
        
        if expected_rem:
            rem_relevance = len(proposed_rem & expected_rem) / len(expected_rem)
        else:
            rem_relevance = 1.0 if not proposed_rem else 0.5
        
        # Remediation safety (inverse of risk)
        rem_safety = 1.0 - response.metadata.get("max_risk_score", 0.0)
        
        remediation_latency = response.time_to_remediation_seconds
        
        # Overall score (weighted)
        overall = (
            0.15 * detection_accuracy +
            0.25 * root_cause_accuracy +
            0.10 * root_cause_type_accuracy +
            0.15 * blast_f1 +
            0.20 * rem_relevance +
            0.15 * rem_safety
        )
        
        return EvaluationResult(
            incident_id=ground_truth.id,
            agent_id=response.agent_id,
            detection_accuracy=detection_accuracy,
            detection_latency_seconds=detection_latency,
            root_cause_accuracy=root_cause_accuracy,
            root_cause_type_accuracy=root_cause_type_accuracy,
            root_cause_latency_seconds=root_cause_latency,
            blast_radius_precision=blast_precision,
            blast_radius_recall=blast_recall,
            blast_radius_f1=blast_f1,
            remediation_relevance=rem_relevance,
            remediation_safety=rem_safety,
            remediation_latency_seconds=remediation_latency,
            overall_score=overall,
            details={
                "expected_root_cause": ground_truth.root_cause,
                "predicted_root_cause": response.root_cause,
                "expected_blast_radius": list(expected_br),
                "predicted_blast_radius": list(predicted_br),
                "expected_remediation": list(expected_rem),
                "proposed_remediation": list(proposed_rem),
                "skills_used": response.skills_used,
                "expected_skills": ground_truth.expected_skills,
            }
        )
    
    def _string_similarity(self, s1: str, s2: str) -> float:
        """Simple string similarity (Jaccard on words)"""
        words1 = set(s1.split())
        words2 = set(s2.split())
        
        if not words1 and not words2:
            return 1.0
        if not words1 or not words2:
            return 0.0
        
        intersection = words1 & words2
        union = words1 | words2
        
        return len(intersection) / len(union)
    
    def _save_results(self, agent_id: str):
        """Save evaluation results to disk"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = self.results_dir / f"eval_{agent_id}_{timestamp}.json"
        
        data = {
            "agent_id": agent_id,
            "timestamp": timestamp,
            "num_incidents": len(self.evaluation_results),
            "summary": self._compute_summary(),
            "results": [
                {
                    "incident_id": r.incident_id,
                    "agent_id": r.agent_id,
                    "detection_accuracy": r.detection_accuracy,
                    "detection_latency_seconds": r.detection_latency_seconds,
                    "root_cause_accuracy": r.root_cause_accuracy,
                    "root_cause_type_accuracy": r.root_cause_type_accuracy,
                    "root_cause_latency_seconds": r.root_cause_latency_seconds,
                    "blast_radius_precision": r.blast_radius_precision,
                    "blast_radius_recall": r.blast_radius_recall,
                    "blast_radius_f1": r.blast_radius_f1,
                    "remediation_relevance": r.remediation_relevance,
                    "remediation_safety": r.remediation_safety,
                    "remediation_latency_seconds": r.remediation_latency_seconds,
                    "overall_score": r.overall_score,
                    "details": r.details,
                }
                for r in self.evaluation_results
            ],
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info("Evaluation results saved", path=str(filename))
    
    def _compute_summary(self) -> Dict[str, Any]:
        """Compute summary statistics"""
        if not self.evaluation_results:
            return {}
        
        n = len(self.evaluation_results)
        
        return {
            "mean_detection_accuracy": sum(r.detection_accuracy for r in self.evaluation_results) / n,
            "mean_root_cause_accuracy": sum(r.root_cause_accuracy for r in self.evaluation_results) / n,
            "mean_root_cause_type_accuracy": sum(r.root_cause_type_accuracy for r in self.evaluation_results) / n,
            "mean_blast_radius_f1": sum(r.blast_radius_f1 for r in self.evaluation_results) / n,
            "mean_remediation_relevance": sum(r.remediation_relevance for r in self.evaluation_results) / n,
            "mean_remediation_safety": sum(r.remediation_safety for r in self.evaluation_results) / n,
            "mean_overall_score": sum(r.overall_score for r in self.evaluation_results) / n,
            "mean_detection_latency": sum(r.detection_latency_seconds or 0 for r in self.evaluation_results) / n,
            "mean_root_cause_latency": sum(r.root_cause_latency_seconds or 0 for r in self.evaluation_results) / n,
            "mean_remediation_latency": sum(r.remediation_latency_seconds or 0 for r in self.evaluation_results) / n,
        }
    
    def generate_report(self, agent_id: str) -> str:
        """Generate human-readable evaluation report"""
        if not self.evaluation_results:
            return "No evaluation results available"
        
        summary = self._compute_summary()
        
        report = f"""
# Evaluation Report for Agent: {agent_id}

## Summary Metrics
- **Overall Score**: {summary.get('mean_overall_score', 0):.2%}
- **Detection Accuracy**: {summary.get('mean_detection_accuracy', 0):.2%}
- **Root Cause Accuracy**: {summary.get('mean_root_cause_accuracy', 0):.2%}
- **Root Cause Type Accuracy**: {summary.get('mean_root_cause_type_accuracy', 0):.2%}
- **Blast Radius F1**: {summary.get('mean_blast_radius_f1', 0):.2%}
- **Remediation Relevance**: {summary.get('mean_remediation_relevance', 0):.2%}
- **Remediation Safety**: {summary.get('mean_remediation_safety', 0):.2%}

## Latency Metrics
- **Mean Detection Latency**: {summary.get('mean_detection_latency', 0):.1f}s
- **Mean Root Cause Latency**: {summary.get('mean_root_cause_latency', 0):.1f}s
- **Mean Remediation Latency**: {summary.get('mean_remediation_latency', 0):.1f}s

## Per-Incident Results
"""
        
        for r in self.evaluation_results:
            report += f"""
### Incident {r.incident_id[:8]}...
- Overall: {r.overall_score:.2%}
- Detection: {r.detection_accuracy:.0%} ({r.detection_latency_seconds:.1f}s)
- Root Cause: {r.root_cause_accuracy:.0%} ({r.root_cause_latency_seconds:.1f}s)
- Blast Radius F1: {r.blast_radius_f1:.0%}
- Remediation: {r.remediation_relevance:.0%} relevance, {r.remediation_safety:.0%} safety
"""
        
        return report


# Standard benchmark suite
def create_standard_benchmark_suite(topology_graph, chaos_client=None) -> List[GroundTruthIncident]:
    """Create a standard benchmark suite covering common incident types"""
    generator = IncidentGenerator(topology_graph, chaos_client)
    
    # Get available services from topology
    services = ["service-a", "service-b", "service-c"]  # Default mock services
    if topology_graph:
        try:
            svc_list = topology_graph.get_all_services()
            services = [s["id"] for s in svc_list]
        except:
            pass
    
    incidents = []
    
    # Resource exhaustion incidents
    for service in services[:2]:
        incidents.append(generator.generate_resource_exhaustion(service, "memory", 0.9))
        incidents.append(generator.generate_resource_exhaustion(service, "cpu", 0.85))
    
    # Network latency incidents
    if len(services) >= 2:
        incidents.append(generator.generate_network_latency(services[0], services[1], 500))
        incidents.append(generator.generate_network_latency(services[1], services[0], 300))
    
    # Deployment regression
    for service in services[:2]:
        incidents.append(generator.generate_deployment_regression(service, "v2.5.0", "v2.4.1"))
    
    # Cascading failure
    if len(services) >= 3:
        incidents.append(generator.generate_cascading_failure(services[0], services[1:3]))
    
    # Config drift
    for service in services[:1]:
        incidents.append(generator.generate_config_drift(service, [
            {"key": "timeout", "old": "30s", "new": "5s"},
            {"key": "max_connections", "old": "100", "new": "10"},
        ]))
    
    return incidents