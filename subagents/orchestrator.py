"""
Subagent System for Autonomous Remediation Agent

Specialist subagents for parallel investigation following OpenSRE and HolmesGPT patterns.
Each subagent handles a specific investigation domain.
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import structlog

logger = structlog.get_logger()


class SubagentType(str, Enum):
    """Types of specialist subagents"""
    KUBERNETES = "kubernetes"           # K8s pod/deployment/node health
    METRICS = "metrics"                 # Prometheus metrics analysis
    LOGS = "logs"                       # Log analysis and pattern detection
    TRACES = "traces"                   # Distributed trace analysis
    GIT = "git"                         # Code change analysis
    NETWORK = "network"                 # Network/connectivity issues
    DATABASE = "database"               # Database performance/issues
    DEPLOYMENT = "deployment"           # Deployment/rollback analysis
    CONFIGURATION = "configuration"     # Config drift/validation
    SECURITY = "security"               # Security incident analysis
    MEMORY = "memory"                   # Episodic memory search
    TOPOLOGY = "topology"               # Service topology/blast radius


class SubagentStatus(str, Enum):
    """Subagent execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class SubagentTask:
    """A task assigned to a subagent"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: SubagentType = SubagentType.KUBERNETES
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    priority: int = 1  # Higher = more urgent
    timeout_seconds: int = 120
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: SubagentStatus = SubagentStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class SubagentResult:
    """Result from a subagent execution"""
    task_id: str
    subagent_type: SubagentType
    success: bool
    findings: List[str] = field(default_factory=list)
    evidence: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    confidence: float = 0.0  # 0-1
    execution_time_seconds: float = 0.0
    error: Optional[str] = None


class BaseSubagent(ABC):
    """Base class for all specialist subagents"""
    
    def __init__(
        self,
        subagent_type: SubagentType,
        tool_executor: Callable,
        config: Dict[str, Any] = None
    ):
        self.subagent_type = subagent_type
        self.tool_executor = tool_executor
        self.config = config or {}
    
    @abstractmethod
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        """Execute the investigation for the given task"""
        pass
    
    def _create_result(
        self,
        task: SubagentTask,
        success: bool,
        findings: List[str] = None,
        evidence: Dict[str, Any] = None,
        recommendations: List[str] = None,
        confidence: float = 0.0,
        error: str = None
    ) -> SubagentResult:
        return SubagentResult(
            task_id=task.id,
            subagent_type=self.subagent_type,
            success=success,
            findings=findings or [],
            evidence=evidence or {},
            recommendations=recommendations or [],
            confidence=confidence,
            error=error,
        )


class KubernetesSubagent(BaseSubagent):
    """Specialist for Kubernetes infrastructure investigation"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.KUBERNETES, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            namespace = task.parameters.get("namespace", "default")
            pod = task.parameters.get("pod")
            deployment = task.parameters.get("deployment")
            label_selector = task.parameters.get("label_selector")
            
            # Get pod status
            if pod or label_selector:
                pod_result = await self.tool_executor("get_k8s_pods", {
                    "namespace": namespace,
                    "pod": pod,
                    "label_selector": label_selector,
                    "include_logs": True,
                    "log_lines": 200,
                })
                evidence["pods"] = pod_result
                
                # Analyze pod health
                for p in pod_result.get("pods", []):
                    if p.get("phase") != "Running":
                        findings.append(f"Pod {p['name']} is in {p['phase']} phase")
                    if p.get("restarts", 0) > 5:
                        findings.append(f"Pod {p['name']} has high restart count: {p['restarts']}")
                    
                    for container in p.get("containers", []):
                        if not container.get("ready"):
                            findings.append(f"Container {container['name']} in pod {p['name']} is not ready")
                            state = container.get("state", {})
                            if state.get("state") == "waiting":
                                findings.append(f"  Waiting reason: {state.get('reason')}: {state.get('message')}")
                            elif state.get("state") == "terminated":
                                findings.append(f"  Terminated: {state.get('reason')} (exit code: {state.get('exit_code')})")
            
            # Get deployment status
            if deployment:
                deploy_result = await self.tool_executor("get_k8s_deployment", {
                    "namespace": namespace,
                    "deployment": deployment,
                })
                evidence["deployment"] = deploy_result
                
                if deploy_result.get("ready_replicas", 0) < deploy_result.get("replicas", 0):
                    findings.append(f"Deployment {deployment} has {deploy_result['ready_replicas']}/{deploy_result['replicas']} ready replicas")
                
                for condition in deploy_result.get("conditions", []):
                    if condition.get("status") != "True" and condition.get("type") in ["Available", "Progressing"]:
                        findings.append(f"Deployment condition {condition['type']}: {condition.get('reason')}: {condition.get('message')}")
            
            # Get events
            events_result = await self.tool_executor("get_k8s_events", {
                "namespace": namespace,
                "pod": pod,
                "deployment": deployment,
                "since_minutes": 60,
            })
            evidence["events"] = events_result
            
            for event in events_result.get("events", []):
                if event.get("type") in ["Warning", "Error"]:
                    findings.append(f"K8s Event: {event['reason']} - {event['message']} (on {event.get('object', {}).get('kind', '')}/{event.get('object', {}).get('name', '')})")
            
            # Generate recommendations
            if any("OOMKilled" in str(f) for f in findings):
                recommendations.append("Check memory limits and requests; consider increasing memory limit")
            if any("CrashLoopBackOff" in str(f) for f in findings):
                recommendations.append("Investigate container crash cause via logs; check application health checks")
            if any("ImagePullBackOff" in str(f) for f in findings):
                recommendations.append("Verify image name and registry credentials")
            if any("Pending" in str(f) for f in findings):
                recommendations.append("Check node resources, scheduler, and PVC binding")
            
            confidence = min(0.9, 0.3 + len(findings) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Kubernetes subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class MetricsSubagent(BaseSubagent):
    """Specialist for Prometheus metrics analysis"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.METRICS, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            service = task.parameters.get("service")
            namespace = task.parameters.get("namespace", "default")
            lookback = task.parameters.get("lookback", "1h")
            
            # Define key metrics to check
            metric_queries = {
                "error_rate": f'sum(rate(http_requests_total{{service="{service}",status=~"5.."}}[{lookback}])) by (pod)',
                "latency_p99": f'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{service="{service}"}}[{lookback}])) by (le, pod))',
                "latency_p95": f'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{service="{service}"}}[{lookback}])) by (le, pod))',
                "throughput": f'sum(rate(http_requests_total{{service="{service}"}}[{lookback}])) by (pod)',
                "cpu_usage": f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{service}-.*"}}[{lookback}])) by (pod)',
                "memory_usage": f'container_memory_usage_bytes{{namespace="{namespace}",pod=~"{service}-.*"}} / container_spec_memory_limit_bytes',
                "restart_rate": f'rate(kube_pod_container_status_restarts_total{{namespace="{namespace}",pod=~"{service}-.*"}}[{lookback}])',
            }
            
            for metric_name, query in metric_queries.items():
                try:
                    result = await self.tool_executor("query_prometheus_range", {
                        "query": query,
                        "start": f"-{lookback}",
                        "end": "now",
                        "step": "30s",
                    })
                    evidence[metric_name] = result
                    
                    # Analyze results
                    data = result.get("data", {}).get("result", [])
                    for series in data:
                        metric = series.get("metric", {})
                        pod = metric.get("pod", "unknown")
                        values = series.get("values", [])
                        
                        if values:
                            latest_value = float(values[-1][1])
                            
                            # Threshold-based analysis
                            if metric_name == "error_rate" and latest_value > 0.05:
                                findings.append(f"High error rate on {pod}: {latest_value:.2%}")
                            elif metric_name == "latency_p99" and latest_value > 2.0:
                                findings.append(f"High p99 latency on {pod}: {latest_value:.2f}s")
                            elif metric_name == "cpu_usage" and latest_value > 0.9:
                                findings.append(f"High CPU usage on {pod}: {latest_value:.0%}")
                            elif metric_name == "memory_usage" and latest_value > 0.85:
                                findings.append(f"High memory usage on {pod}: {latest_value:.0%}")
                            elif metric_name == "restart_rate" and latest_value > 0.01:
                                findings.append(f"Frequent restarts on {pod}: {latest_value:.2f}/min")
                
                except Exception as e:
                    logger.warning("Metric query failed", metric=metric_name, error=str(e))
            
            # Recommendations based on findings
            if any("error rate" in f for f in findings):
                recommendations.append("Check application logs for error details; consider rollback if recent deployment")
            if any("latency" in f for f in findings):
                recommendations.append("Profile application for bottlenecks; check downstream dependencies")
            if any("CPU" in f for f in findings):
                recommendations.append("Consider horizontal scaling or optimizing CPU-intensive code paths")
            if any("memory" in f for f in findings):
                recommendations.append("Check for memory leaks; consider increasing memory limits")
            if any("restart" in f for f in findings):
                recommendations.append("Investigate crash cause; check OOM kills and liveness probes")
            
            confidence = min(0.85, 0.3 + len(findings) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Metrics subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class LogsSubagent(BaseSubagent):
    """Specialist for log analysis"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.LOGS, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            service = task.parameters.get("service")
            namespace = task.parameters.get("namespace", "default")
            start = task.parameters.get("start")
            end = task.parameters.get("end")
            query = task.parameters.get("query", f'{{service="{service}"}} |= "error"')
            
            # Query logs
            log_result = await self.tool_executor("query_loki_logs", {
                "query": query,
                "start": start,
                "end": end,
                "limit": 5000,
                "direction": "backward",
            })
            evidence["logs"] = log_result
            
            # Extract log lines
            log_lines = []
            for stream in log_result.get("data", {}).get("result", []):
                for entry in stream.get("values", []):
                    log_lines.append(entry[1])
            
            if log_lines:
                # Analyze log patterns
                pattern_result = await self.tool_executor("analyze_log_templates", {
                    "log_lines": log_lines[:1000],  # Limit for analysis
                    "threshold": 0.5,
                })
                evidence["patterns"] = pattern_result
                
                # Check for common error patterns
                error_keywords = [
                    "OutOfMemoryError", "NullPointerException", "ConnectionPoolExhausted",
                    "TimeoutException", "StackOverflowError", "Deadlock", "CircuitBreaker",
                    "RateLimitExceeded", "Unauthorized", "Forbidden", "NotFound",
                    "DatabaseConnectionFailed", "ConnectionRefused", "DNS resolution failed",
                ]
                
                for line in log_lines[:500]:  # Sample first 500
                    for keyword in error_keywords:
                        if keyword.lower() in line.lower():
                            findings.append(f"Found error pattern: {keyword}")
                            break
                
                # Compute entropy if many unique templates
                if "templates" in pattern_result:
                    entropy_result = await self.tool_executor("compute_shannon_entropy", {
                        "templates": pattern_result["templates"],
                    })
                    evidence["entropy"] = entropy_result
                    
                    if entropy_result.get("entropy", 0) > 3.0:
                        findings.append(f"High log template entropy: {entropy_result['entropy']:.2f} - diverse error patterns")
            
            # Recommendations
            if any("OutOfMemoryError" in f for f in findings):
                recommendations.append("Application experiencing OOM; check heap settings and memory leaks")
            if any("ConnectionPoolExhausted" in f for f in findings):
                recommendations.append("Connection pool exhausted; increase pool size or fix connection leaks")
            if any("TimeoutException" in f for f in findings):
                recommendations.append("Downstream timeouts; check dependency health and network")
            if any("entropy" in f for f in findings):
                recommendations.append("Diverse error patterns suggest systemic issue; check recent deployments")
            
            confidence = min(0.8, 0.2 + len(findings) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Logs subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class TracesSubagent(BaseSubagent):
    """Specialist for distributed trace analysis"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.TRACES, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            service = task.parameters.get("service")
            lookback_hours = task.parameters.get("lookback_hours", 24)
            
            # Get service dependencies
            deps_result = await self.tool_executor("get_service_dependencies", {
                "service": service,
                "lookback_hours": lookback_hours,
            })
            evidence["dependencies"] = deps_result
            
            # Analyze upstream
            for upstream in deps_result.get("upstream", []):
                if upstream.get("error_count", 0) > 0:
                    findings.append(f"Upstream {upstream['service']} has {upstream['error_count']} errors")
                if upstream.get("p99_latency", 0) > 1000:
                    findings.append(f"Upstream {upstream['service']} has high p99 latency: {upstream['p99_latency']}ms")
            
            # Analyze downstream
            for downstream in deps_result.get("downstream", []):
                if downstream.get("error_count", 0) > 0:
                    findings.append(f"Downstream {downstream['service']} has {downstream['error_count']} errors")
                if downstream.get("p99_latency", 0) > 1000:
                    findings.append(f"Downstream {downstream['service']} has high p99 latency: {downstream['p99_latency']}ms")
            
            # Query error traces
            trace_query = f"""
                SELECT service_name, span_name, count() as count,
                       quantile(0.99)(duration_ms) as p99,
                       sum(has_error) as errors
                FROM traces
                WHERE service_name = '{service}'
                  AND timestamp > now() - INTERVAL {lookback_hours} HOUR
                  AND has_error = 1
                GROUP BY service_name, span_name
                ORDER BY errors DESC
                LIMIT 20
            """
            
            trace_result = await self.tool_executor("query_clickhouse", {
                "query": trace_query,
                "limit": 100,
            })
            evidence["error_traces"] = trace_result
            
            for trace in trace_result.get("results", []):
                if trace.get("errors", 0) > 0:
                    findings.append(f"Error trace: {trace['service_name']}.{trace['span_name']} - {trace['errors']} errors (p99: {trace['p99']}ms)")
            
            # Recommendations
            if findings:
                recommendations.append("Check error traces for root cause span; correlate with logs and metrics")
                recommendations.append("Verify circuit breaker configuration for failing dependencies")
            
            confidence = min(0.85, 0.3 + len(findings) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Traces subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class GitSubagent(BaseSubagent):
    """Specialist for code change analysis"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.GIT, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            repo_path = task.parameters.get("repo_path", ".")
            service = task.parameters.get("service")
            since_days = task.parameters.get("since_days", 14)
            
            # Get recent commits for service
            # This would need mapping from service to code paths
            service_paths = task.parameters.get("service_paths", [f"services/{service}"])
            
            for path in service_paths:
                blame_result = await self.tool_executor("fetch_git_blame_diff", {
                    "repo_path": repo_path,
                    "file_path": path,
                    "since_days": since_days,
                    "include_diff": True,
                })
                evidence[f"git_{path.replace('/', '_')}"] = blame_result
                
                commits = blame_result.get("commits", [])
                for commit in commits:
                    message = commit.get("message", "").lower()
                    if any(keyword in message for keyword in ["fix", "bug", "hotfix", "revert", "rollback"]):
                        findings.append(f"Recent fix commit: {commit['hash']} - {commit['message']}")
                    if any(keyword in message for keyword in ["refactor", "migrate", "upgrade", "change"]):
                        findings.append(f"Potential breaking change: {commit['hash']} - {commit['message']}")
            
            # Recommendations
            if findings:
                recommendations.append("Review recent commits for potential regressions")
                recommendations.append("Consider reverting suspicious changes if correlated with incident start time")
            
            confidence = min(0.75, 0.2 + len(findings) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Git subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class MemorySubagent(BaseSubagent):
    """Specialist for episodic memory search"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.MEMORY, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            query_text = task.parameters.get("query_text", "")
            issue_type = task.parameters.get("issue_type")
            components = task.parameters.get("components", [])
            
            # Search episodic memory
            memory_result = await self.tool_executor("episodic_memory_search", {
                "query_text": query_text,
                "limit": 10,
                "min_score": 0.6,
                "issue_type": issue_type,
                "components": components,
            })
            evidence["similar_episodes"] = memory_result
            
            episodes = memory_result.get("episodes", [])
            if episodes:
                findings.append(f"Found {len(episodes)} similar past incidents")
                
                # Analyze patterns
                root_causes = []
                remediations = []
                for ep in episodes:
                    if ep.get("root_cause"):
                        root_causes.append(ep["root_cause"])
                    if ep.get("remediation_actions"):
                        remediations.extend(ep["remediation_actions"])
                
                if root_causes:
                    from collections import Counter
                    cause_counts = Counter(root_causes)
                    for cause, count in cause_counts.most_common(3):
                        findings.append(f"Common root cause: {cause} (seen in {count} similar incidents)")
                
                if remediations:
                    from collections import Counter
                    action_counts = Counter(remediations)
                    for action, count in action_counts.most_common(3):
                        findings.append(f"Effective remediation: {action} (used in {count} cases)")
                        recommendations.append(f"Consider: {action}")
                
                # Synthesize playbook if enough episodes
                if len(episodes) >= 3:
                    playbook_result = await self.tool_executor("episodic_memory_synthesize", {
                        "issue_type": issue_type or "unknown",
                        "min_episodes": 3,
                    })
                    evidence["playbook"] = playbook_result
                    if playbook_result.get("playbook"):
                        findings.append("Strategy playbook synthesized from similar incidents")
            
            confidence = min(0.8, 0.2 + len(episodes) * 0.1)
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Memory subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


class TopologySubagent(BaseSubagent):
    """Specialist for service topology and blast radius analysis"""
    
    def __init__(self, tool_executor: Callable, config: Dict[str, Any] = None):
        super().__init__(SubagentType.TOPOLOGY, tool_executor, config)
    
    async def investigate(self, task: SubagentTask) -> SubagentResult:
        start_time = datetime.utcnow()
        findings = []
        evidence = {}
        recommendations = []
        
        try:
            service_id = task.parameters.get("service_id")
            max_hops = task.parameters.get("max_hops", 3)
            
            # Get blast radius
            blast_result = await self.tool_executor("topology_blast_radius", {
                "service_id": service_id,
                "max_hops": max_hops,
            })
            evidence["blast_radius"] = blast_result
            
            upstream_count = len(blast_result.get("upstream", []))
            downstream_count = len(blast_result.get("downstream", []))
            shared_db_count = len(blast_result.get("shared_databases", []))
            shared_mq_count = len(blast_result.get("shared_message_queues", []))
            
            findings.append(f"Blast radius: {upstream_count} upstream, {downstream_count} downstream services")
            
            if shared_db_count > 0:
                findings.append(f"Shares {shared_db_count} database(s) with other services")
                for db in blast_result.get("shared_databases", [])[:3]:
                    findings.append(f"  Shared DB: {db.get('db_name')} with {db.get('name')}")
            
            if shared_mq_count > 0:
                findings.append(f"Shares {shared_mq_count} message queue(s) with other services")
            
            # Get ownership
            ownership_result = await self.tool_executor("topology_ownership", {
                "service_id": service_id,
            })
            evidence["ownership"] = ownership_result
            
            if ownership_result:
                findings.append(f"Service owned by: {ownership_result.get('team_name', 'unknown')}")
            
            # Get oncall
            oncall_result = await self.tool_executor("topology_oncall", {
                "service_id": service_id,
            })
            evidence["oncall"] = oncall_result
            
            if oncall_result:
                findings.append(f"Current on-call: {oncall_result.get('name', 'unknown')}")
            
            # Recommendations
            if downstream_count > 5:
                recommendations.append("Large downstream blast radius; consider circuit breakers for critical paths")
            if shared_db_count > 0:
                recommendations.append("Shared databases create coupling; verify DB capacity and connection pooling")
            if upstream_count > 3:
                recommendations.append("Multiple upstream dependencies; check for cascade failure patterns")
            
            confidence = 0.9
            
            return self._create_result(
                task, True, findings, evidence, recommendations, confidence
            )
            
        except Exception as e:
            logger.error("Topology subagent failed", error=str(e))
            return self._create_result(task, False, error=str(e))


# ==================== Subagent Orchestrator ====================

class SubagentOrchestrator:
    """
    Orchestrates parallel subagent execution for incident investigation.
    
    Based on OpenSRE's subagent architecture:
    - Root agent plans work and dispatches specialists
    - Subagents run in parallel for independent sub-questions
    - Progress streams back via SSE
    - Parent waits for all background tasks before synthesizing
    """
    
    def __init__(
        self,
        tool_executor: Callable,
        subagent_configs: Dict[SubagentType, Dict[str, Any]] = None,
        max_concurrent: int = 5
    ):
        self.tool_executor = tool_executor
        self.max_concurrent = max_concurrent
        self.subagents: Dict[SubagentType, BaseSubagent] = {}
        self._initialize_subagents(subagent_configs or {})
    
    def _initialize_subagents(self, configs: Dict[SubagentType, Dict[str, Any]]):
        """Initialize all subagent types"""
        subagent_classes = {
            SubagentType.KUBERNETES: KubernetesSubagent,
            SubagentType.METRICS: MetricsSubagent,
            SubagentType.LOGS: LogsSubagent,
            SubagentType.TRACES: TracesSubagent,
            SubagentType.GIT: GitSubagent,
            SubagentType.MEMORY: MemorySubagent,
            SubagentType.TOPOLOGY: TopologySubagent,
        }
        
        for subagent_type, cls in subagent_classes.items():
            config = configs.get(subagent_type, {})
            self.subagents[subagent_type] = cls(self.tool_executor, config)
    
    def get_subagent(self, subagent_type: SubagentType) -> Optional[BaseSubagent]:
        return self.subagents.get(subagent_type)
    
    def list_subagent_types(self) -> List[SubagentType]:
        return list(self.subagents.keys())
    
    async def execute_parallel(
        self,
        tasks: List[SubagentTask],
        progress_callback: Optional[Callable] = None
    ) -> List[SubagentResult]:
        """
        Execute multiple subagent tasks in parallel with concurrency control.
        
        Args:
            tasks: List of subagent tasks
            progress_callback: Optional callback(task_id, status, progress) for streaming updates
        
        Returns:
            List of subagent results in task order
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def run_with_semaphore(task: SubagentTask) -> SubagentResult:
            async with semaphore:
                subagent = self.subagents.get(task.type)
                if not subagent:
                    return SubagentResult(
                        task_id=task.id,
                        subagent_type=task.type,
                        success=False,
                        error=f"No subagent registered for type {task.type}"
                    )
                
                task.status = SubagentStatus.RUNNING
                task.started_at = datetime.utcnow()
                
                if progress_callback:
                    await progress_callback(task.id, "running", 0)
                
                try:
                    result = await asyncio.wait_for(
                        subagent.investigate(task),
                        timeout=task.timeout_seconds
                    )
                    task.status = SubagentStatus.COMPLETED
                    task.completed_at = datetime.utcnow()
                    task.result = result.__dict__
                    
                    if progress_callback:
                        await progress_callback(task.id, "completed", 100)
                    
                    return result
                    
                except asyncio.TimeoutError:
                    task.status = SubagentStatus.TIMEOUT
                    task.completed_at = datetime.utcnow()
                    result = SubagentResult(
                        task_id=task.id,
                        subagent_type=task.type,
                        success=False,
                        error=f"Timeout after {task.timeout_seconds}s"
                    )
                    if progress_callback:
                        await progress_callback(task.id, "timeout", 100)
                    return result
                    
                except Exception as e:
                    task.status = SubagentStatus.FAILED
                    task.completed_at = datetime.utcnow()
                    result = SubagentResult(
                        task_id=task.id,
                        subagent_type=task.type,
                        success=False,
                        error=str(e)
                    )
                    if progress_callback:
                        await progress_callback(task.id, "failed", 100)
                    return result
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*[run_with_semaphore(task) for task in tasks])
        return list(results)
    
    async def execute_sequential(
        self,
        tasks: List[SubagentTask],
        progress_callback: Optional[Callable] = None
    ) -> List[SubagentResult]:
        """Execute tasks sequentially (for dependent tasks)"""
        results = []
        for task in tasks:
            task.status = SubagentStatus.RUNNING
            task.started_at = datetime.utcnow()
            
            if progress_callback:
                await progress_callback(task.id, "running", 0)
            
            subagent = self.subagents.get(task.type)
            if not subagent:
                result = SubagentResult(
                    task_id=task.id,
                    subagent_type=task.type,
                    success=False,
                    error=f"No subagent registered for type {task.type}"
                )
            else:
                try:
                    result = await asyncio.wait_for(
                        subagent.investigate(task),
                        timeout=task.timeout_seconds
                    )
                    task.status = SubagentStatus.COMPLETED
                except asyncio.TimeoutError:
                    task.status = SubagentStatus.TIMEOUT
                    result = SubagentResult(
                        task_id=task.id,
                        subagent_type=task.type,
                        success=False,
                        error=f"Timeout after {task.timeout_seconds}s"
                    )
                except Exception as e:
                    task.status = SubagentStatus.FAILED
                    result = SubagentResult(
                        task_id=task.id,
                        subagent_type=task.type,
                        success=False,
                        error=str(e)
                    )
            
            task.completed_at = datetime.utcnow()
            task.result = result.__dict__
            results.append(result)
            
            if progress_callback:
                await progress_callback(task.id, task.status.value, 100)
        
        return results
    
    def create_investigation_plan(
        self,
        incident: Dict[str, Any],
        available_subagents: List[SubagentType] = None
    ) -> List[SubagentTask]:
        """
        Create an investigation plan based on incident characteristics.
        This would typically be done by the root agent/planner.
        """
        available = available_subagents or list(self.subagents.keys())
        tasks = []
        
        service = incident.get("service")
        namespace = incident.get("namespace", "default")
        anomaly_type = incident.get("anomaly_type", "")
        
        # Always include core investigation subagents
        core_subagents = [
            SubagentType.KUBERNETES,
            SubagentType.METRICS,
            SubagentType.LOGS,
            SubagentType.TOPOLOGY,
        ]
        
        for subagent_type in core_subagents:
            if subagent_type in available:
                tasks.append(SubagentTask(
                    type=subagent_type,
                    description=f"Investigate {subagent_type.value} for {service}",
                    parameters={
                        "service": service,
                        "namespace": namespace,
                    },
                    priority=1,
                ))
        
        # Add specialized subagents based on anomaly type
        if anomaly_type in ["error_rate_spike", "latency_degradation"]:
            if SubagentType.TRACES in available:
                tasks.append(SubagentTask(
                    type=SubagentType.TRACES,
                    description=f"Trace analysis for {service}",
                    parameters={"service": service},
                    priority=1,
                ))
        
        if anomaly_type in ["resource_exhaustion", "cascade_failure"]:
            if SubagentType.MEMORY in available:
                tasks.append(SubagentTask(
                    type=SubagentType.MEMORY,
                    description=f"Search memory for similar {anomaly_type} incidents",
                    parameters={
                        "query_text": f"{anomaly_type} in {service}",
                        "components": [service],
                    },
                    priority=2,
                ))
        
        if SubagentType.GIT in available:
            tasks.append(SubagentTask(
                type=SubagentType.GIT,
                description=f"Check recent code changes for {service}",
                parameters={"service": service, "repo_path": "."},
                priority=2,
            ))
        
        return tasks


# Factory function
def create_subagent_orchestrator(
    tool_executor: Callable,
    max_concurrent: int = 5,
    **subagent_configs
) -> SubagentOrchestrator:
    return SubagentOrchestrator(tool_executor, subagent_configs, max_concurrent)