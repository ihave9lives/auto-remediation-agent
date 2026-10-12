"""
Tool Implementations for the Agentic Root-Cause Engine

Each tool implements the contract defined in schemas.py
"""

import os
import json
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

import httpx
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from git import Repo
from clickhouse_driver import Client as ClickHouseClient
from prometheus_client import CollectorRegistry, generate_latest
import structlog

logger = structlog.get_logger()


class ToolError(Exception):
    """Raised when a tool execution fails"""
    pass


class PrometheusClient:
    """Async Prometheus client for range and instant queries"""
    
    def __init__(self, base_url: str = "http://prometheus:9090", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)
    
    async def query_range(self, query: str, start: str, end: str, step: str = "15s") -> Dict[str, Any]:
        """Execute a range query"""
        params = {"query": query, "start": start, "end": end, "step": step}
        resp = await self.client.get(f"{self.base_url}/api/v1/query_range", params=params)
        resp.raise_for_status()
        return resp.json()
    
    async def query_instant(self, query: str, time: str = "now") -> Dict[str, Any]:
        """Execute an instant query"""
        params = {"query": query, "time": time}
        resp = await self.client.get(f"{self.base_url}/api/v1/query", params=params)
        resp.raise_for_status()
        return resp.json()
    
    async def close(self):
        await self.client.aclose()


class KubernetesClient:
    """Kubernetes client wrapper for common operations"""
    
    def __init__(self):
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_objects = client.CustomObjectsApi()
    
    def get_events(self, namespace: str, pod: Optional[str] = None, 
                   deployment: Optional[str] = None, since_minutes: int = 60,
                   event_types: List[str] = None) -> List[Dict[str, Any]]:
        """Get Kubernetes events"""
        if event_types is None:
            event_types = ["Warning", "Error"]
        
        field_selector_parts = []
        if pod:
            field_selector_parts.append(f"involvedObject.name={pod}")
        if deployment:
            # Get pods for deployment
            pods = self.core_v1.list_namespaced_pod(
                namespace, label_selector=f"app={deployment}"
            )
            pod_names = [p.metadata.name for p in pods.items]
            if pod_names:
                field_selector_parts.append(f"involvedObject.name in ({','.join(pod_names)})")
        
        field_selector = ",".join(field_selector_parts) if field_selector_parts else None
        
        since_time = datetime.utcnow() - timedelta(minutes=since_minutes)
        
        events = self.core_v1.list_namespaced_event(
            namespace, field_selector=field_selector
        )
        
        result = []
        for event in events.items:
            if event.type in event_types:
                if event.last_timestamp and event.last_timestamp.replace(tzinfo=None) < since_time:
                    continue
                result.append({
                    "name": event.metadata.name,
                    "namespace": event.metadata.namespace,
                    "type": event.type,
                    "reason": event.reason,
                    "message": event.message,
                    "source": event.source.component if event.source else None,
                    "object": {
                        "kind": event.involved_object.kind,
                        "name": event.involved_object.name,
                    } if event.involved_object else None,
                    "first_timestamp": event.first_timestamp.isoformat() if event.first_timestamp else None,
                    "last_timestamp": event.last_timestamp.isoformat() if event.last_timestamp else None,
                    "count": event.count,
                })
        return result
    
    def get_pod_status(self, namespace: str, pod: str, 
                       include_logs: bool = False, log_lines: int = 100) -> Dict[str, Any]:
        """Get detailed pod status"""
        try:
            pod_obj = self.core_v1.read_namespaced_pod(pod, namespace)
        except ApiException as e:
            if e.status == 404:
                raise ToolError(f"Pod {pod} not found in namespace {namespace}")
            raise
        
        containers = []
        total_restarts = 0
        for container in pod_obj.status.container_statuses or []:
            total_restarts += container.restart_count
            containers.append({
                "name": container.name,
                "ready": container.ready,
                "restart_count": container.restart_count,
                "state": self._container_state(container.state),
                "image": container.image,
                "image_id": container.image_id,
            })
        
        result = {
            "pod": pod_obj.metadata.name,
            "namespace": pod_obj.metadata.namespace,
            "phase": pod_obj.status.phase,
            "conditions": [
                {"type": c.type, "status": c.status, "reason": c.reason, "message": c.message}
                for c in (pod_obj.status.conditions or [])
            ],
            "containers": containers,
            "restarts": total_restarts,
            "start_time": pod_obj.status.start_time.isoformat() if pod_obj.status.start_time else None,
            "pod_ip": pod_obj.status.pod_ip,
            "host_ip": pod_obj.status.host_ip,
        }
        
        if include_logs:
            try:
                logs = self.core_v1.read_namespaced_pod_log(
                    pod, namespace, tail_lines=log_lines, timestamps=True
                )
                result["logs"] = logs
            except ApiException:
                result["logs"] = "Unable to fetch logs"
        
        return result
    
    def _container_state(self, state) -> Dict[str, Any]:
        if state.waiting:
            return {"state": "waiting", "reason": state.waiting.reason, "message": state.waiting.message}
        elif state.running:
            return {"state": "running", "started_at": state.running.started_at.isoformat() if state.running.started_at else None}
        elif state.terminated:
            return {
                "state": "terminated",
                "reason": state.terminated.reason,
                "exit_code": state.terminated.exit_code,
                "started_at": state.terminated.started_at.isoformat() if state.terminated.started_at else None,
                "finished_at": state.terminated.finished_at.isoformat() if state.terminated.finished_at else None,
            }
        return {"state": "unknown"}
    
    def get_deployment_status(self, namespace: str, deployment: str) -> Dict[str, Any]:
        """Get deployment status"""
        try:
            dep = self.apps_v1.read_namespaced_deployment(deployment, namespace)
        except ApiException as e:
            if e.status == 404:
                raise ToolError(f"Deployment {deployment} not found in namespace {namespace}")
            raise
        
        return {
            "name": dep.metadata.name,
            "namespace": dep.metadata.namespace,
            "replicas": dep.spec.replicas or 0,
            "ready_replicas": dep.status.ready_replicas or 0,
            "available_replicas": dep.status.available_replicas or 0,
            "updated_replicas": dep.status.updated_replicas or 0,
            "observed_generation": dep.status.observed_generation,
            "conditions": [
                {"type": c.type, "status": c.status, "reason": c.reason, "message": c.message}
                for c in (dep.status.conditions or [])
            ],
            "strategy": dep.spec.strategy.type if dep.spec.strategy else "RollingUpdate",
            "selector": dep.spec.selector.match_labels if dep.spec.selector else {},
            "template_hash": dep.spec.template.metadata.labels.get("pod-template-hash") if dep.spec.template.metadata else None,
        }


class GitClient:
    """Git operations for blame, diff, and commit analysis"""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.repo = Repo(self.repo_path)
    
    def get_blame_and_recent_commits(self, file_path: str, since_days: int = 14, 
                                     include_diff: bool = True) -> Dict[str, Any]:
        """Get git blame and recent commits for a file"""
        full_path = self.repo_path / file_path
        if not full_path.exists():
            raise ToolError(f"File {file_path} not found in repo")
        
        # Get blame
        blame_output = self.repo.git.blame("-w", "-C", "-C", "--", file_path)
        blame_lines = []
        for line in blame_output.split("\n"):
            if line.strip():
                parts = line.split(")", 1)
                if len(parts) == 2:
                    commit_info = parts[0] + ")"
                    code = parts[1].strip()
                    blame_lines.append({"commit": commit_info.strip(), "code": code})
        
        # Get recent commits
        since_date = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        commits = list(self.repo.iter_commits(paths=file_path, since=since_date, max_count=20))
        
        commit_data = []
        for commit in commits:
            commit_info = {
                "hash": commit.hexsha[:12],
                "author": str(commit.author),
                "date": datetime.fromtimestamp(commit.committed_date).isoformat(),
                "message": commit.message.strip(),
                "files": list(commit.stats.files.keys()),
            }
            if include_diff:
                try:
                    diff = commit.diff(commit.parents[0] if commit.parents else None, paths=file_path)
                    commit_info["diff"] = str(diff)
                except Exception:
                    commit_info["diff"] = "Unable to generate diff"
            commit_data.append(commit_info)
        
        return {
            "file": file_path,
            "blame": blame_lines,
            "commits": commit_data,
        }


class StackTraceAnalyzer:
    """Analyze stack traces for common patterns"""
    
    # Common error patterns
    PATTERNS = {
        "OutOfMemoryError": {
            "type": "OOM",
            "causes": ["Memory leak", "Insufficient heap", "Large object allocation"],
            "patterns": ["java.lang.OutOfMemoryError", "OutOfMemoryError"]
        },
        "NullPointerException": {
            "type": "NPE",
            "causes": ["Uninitialized object", "Missing null check", "Race condition"],
            "patterns": ["NullPointerException", "null pointer"]
        },
        "ConnectionPoolExhausted": {
            "type": "CONNECTION_POOL",
            "causes": ["Connection leak", "Pool too small", "Downstream timeout"],
            "patterns": ["Connection pool exhausted", "PoolExhaustedException", "No connections available"]
        },
        "TimeoutException": {
            "type": "TIMEOUT",
            "causes": ["Downstream latency", "Network issues", "Deadlock"],
            "patterns": ["TimeoutException", "Read timed out", "Connection timed out"]
        },
        "StackOverflowError": {
            "type": "STACK_OVERFLOW",
            "causes": ["Infinite recursion", "Deep call stack", "Cyclic dependency"],
            "patterns": ["StackOverflowError", "stack overflow"]
        },
    }
    
    def analyze(self, stack_trace: str, language: str = "python", service: str = "") -> Dict[str, Any]:
        """Analyze stack trace and identify error type and causes"""
        trace_lower = stack_trace.lower()
        
        matched = []
        for error_name, pattern_info in self.PATTERNS.items():
            for pattern in pattern_info["patterns"]:
                if pattern.lower() in trace_lower:
                    matched.append({
                        "error_type": error_name,
                        "type": pattern_info["type"],
                        "potential_causes": pattern_info["causes"],
                    })
                    break
        
        # Extract origin (last file:line in trace)
        origin = "unknown"
        import re
        # Common patterns: File "x.py", line 123 or at x.java:123
        file_line_matches = re.findall(r'(?:File|at)\s+"?([^"\s]+)"?[,\s]+line\s+(\d+)', stack_trace)
        if file_line_matches:
            origin = f"{file_line_matches[-1][0]}:{file_line_matches[-1][1]}"
        else:
            # Try Java style
            java_matches = re.findall(r'at\s+([\w.$]+)\(([^:]+):(\d+)\)', stack_trace)
            if java_matches:
                origin = f"{java_matches[-1][1]}:{java_matches[-1][2]}"
        
        return {
            "error_type": matched[0]["error_type"] if matched else "Unknown",
            "type": matched[0]["type"] if matched else "UNKNOWN",
            "origin": origin,
            "potential_causes": matched[0]["potential_causes"] if matched else ["Unknown error pattern"],
            "related_patterns": [m["type"] for m in matched],
            "service": service,
        }


class ClickHouseClientWrapper:
    """ClickHouse client for log queries"""
    
    def __init__(self, host: str = "clickhouse", port: int = 9000, 
                 database: str = "otel", user: str = "default", password: str = ""):
        self.client = ClickHouseClient(
            host=host, port=port, database=database, user=user, password=password
        )
    
    def query(self, sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dicts"""
        # Add limit if not present
        if "LIMIT" not in sql.upper():
            sql = f"{sql} LIMIT {limit}"
        
        rows = self.client.execute(sql, with_column_types=True)
        if not rows:
            return []
        
        data, columns = rows
        column_names = [col[0] for col in columns]
        return [dict(zip(column_names, row)) for row in data]


class LokiClient:
    """Loki client for log queries"""
    
    def __init__(self, base_url: str = "http://loki:3100", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)
    
    async def query_range(self, query: str, start: str, end: str, 
                          limit: int = 1000, direction: str = "backward") -> Dict[str, Any]:
        """Execute a LogQL range query"""
        params = {
            "query": query,
            "start": start,
            "end": end,
            "limit": limit,
            "direction": direction,
        }
        resp = await self.client.get(f"{self.base_url}/loki/api/v1/query_range", params=params)
        resp.raise_for_status()
        return resp.json()
    
    async def close(self):
        await self.client.aclose()


class ServiceDependencyClient:
    """Get service dependencies from trace data"""
    
    def __init__(self, clickhouse: ClickHouseClientWrapper):
        self.clickhouse = clickhouse
    
    def get_dependencies(self, service: str, lookback_hours: int = 24) -> Dict[str, Any]:
        """Get upstream and downstream dependencies from traces"""
        query = f"""
        SELECT 
            span_name,
            count() as call_count,
            quantile(0.99)(duration_ms) as p99_latency,
            sum(has_error) as error_count
        FROM traces
        WHERE service_name = '{service}'
          AND timestamp > now() - INTERVAL {lookback_hours} HOUR
        GROUP BY span_name
        ORDER BY call_count DESC
        LIMIT 50
        """
        
        results = self.clickhouse.query(query)
        
        # Parse span names to extract upstream/downstream
        upstream = []
        downstream = []
        for row in results:
            span = row["span_name"]
            if "client" in span.lower() or "outbound" in span.lower():
                downstream.append({
                    "service": span,
                    "calls": row["call_count"],
                    "p99_latency_ms": row["p99_latency"],
                    "errors": row["error_count"],
                })
            else:
                upstream.append({
                    "service": span,
                    "calls": row["call_count"],
                    "p99_latency_ms": row["p99_latency"],
                    "errors": row["error_count"],
                })
        
        return {
            "service": service,
            "upstream": upstream,
            "downstream": downstream,
        }


# Tool function implementations for LangGraph
async def query_prometheus_range_tool(query: str, start: str, end: str, step: str = "15s") -> Dict[str, Any]:
    """Tool: Query Prometheus range"""
    client = PrometheusClient()
    try:
        result = await client.query_range(query, start, end, step)
        return {"status": "success", "data": result, "query": query, "start": start, "end": end, "step": step}
    finally:
        await client.close()


async def query_prometheus_instant_tool(query: str, time: str = "now") -> Dict[str, Any]:
    """Tool: Query Prometheus instant"""
    client = PrometheusClient()
    try:
        result = await client.query_instant(query, time)
        return {"status": "success", "data": result, "query": query, "time": time}
    finally:
        await client.close()


def get_k8s_events_tool(namespace: str, pod: Optional[str] = None, 
                        deployment: Optional[str] = None, since_minutes: int = 60,
                        event_types: List[str] = None) -> Dict[str, Any]:
    """Tool: Get Kubernetes events"""
    k8s = KubernetesClient()
    events = k8s.get_events(namespace, pod, deployment, since_minutes, event_types)
    return {"events": events, "namespace": namespace, "filter": {"pod": pod, "deployment": deployment}}


def get_pod_status_tool(namespace: str, pod: str, 
                        include_logs: bool = False, log_lines: int = 100) -> Dict[str, Any]:
    """Tool: Get pod status"""
    k8s = KubernetesClient()
    return k8s.get_pod_status(namespace, pod, include_logs, log_lines)


def get_deployment_status_tool(namespace: str, deployment: str) -> Dict[str, Any]:
    """Tool: Get deployment status"""
    k8s = KubernetesClient()
    return k8s.get_deployment_status(namespace, deployment)


def fetch_git_blame_diff_tool(repo_path: str, file_path: str, 
                              since_days: int = 14, include_diff: bool = True) -> Dict[str, Any]:
    """Tool: Fetch git blame and recent commits"""
    git = GitClient(repo_path)
    return git.get_blame_and_recent_commits(file_path, since_days, include_diff)


def analyze_stack_trace_tool(stack_trace: str, language: str = "python", service: str = "") -> Dict[str, Any]:
    """Tool: Analyze stack trace"""
    analyzer = StackTraceAnalyzer()
    return analyzer.analyze(stack_trace, language, service)


def query_clickhouse_logs_tool(query: str, limit: int = 1000) -> Dict[str, Any]:
    """Tool: Query ClickHouse logs"""
    ch = ClickHouseClientWrapper()
    results = ch.query(query, limit)
    return {"results": results, "row_count": len(results)}


async def query_loki_logs_tool(query: str, start: str, end: str, 
                               limit: int = 1000, direction: str = "backward") -> Dict[str, Any]:
    """Tool: Query Loki logs"""
    client = LokiClient()
    try:
        result = await client.query_range(query, start, end, limit, direction)
        return {"status": "success", "data": result}
    finally:
        await client.close()


def get_service_dependencies_tool(service: str, lookback_hours: int = 24) -> Dict[str, Any]:
    """Tool: Get service dependencies"""
    ch = ClickHouseClientWrapper()
    dep_client = ServiceDependencyClient(ch)
    return dep_client.get_dependencies(service, lookback_hours)


async def simulate_remediation_tool(action_type: str, target: str, namespace: str, 
                                    parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Tool: Simulate remediation (dry-run)"""
    # In production, this would call a staging environment or simulator
    # For now, return a mock simulation result
    risk_scores = {
        "rollback": 0.2,
        "config_change": 0.4,
        "scale": 0.3,
        "restart": 0.5,
        "circuit_breaker": 0.3,
        "traffic_shift": 0.6,
    }
    
    return {
        "action_type": action_type,
        "target": target,
        "predicted_impact": {
            "availability": "maintained" if action_type != "restart" else "brief_interruption",
            "latency_change_pct": 0,
            "error_rate_change_pct": 0,
        },
        "risk_score": risk_scores.get(action_type, 0.5),
        "confidence": 0.8,
        "warnings": [
            "This is a simulation - validate in staging before production",
            f"Action {action_type} on {target} requires approval",
        ],
    }


def generate_git_patch_tool(repo_path: str, changes: List[Dict[str, str]], 
                            commit_message: str) -> Dict[str, Any]:
    """Tool: Generate git patch"""
    git = GitClient(repo_path)
    
    patch_lines = []
    for change in changes:
        file_path = change["file"]
        old_content = change["old_content"]
        new_content = change["new_content"]
        
        # Generate unified diff
        import difflib
        old_lines = old_content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)
        diff = difflib.unified_diff(old_lines, new_lines, fromfile=f"a/{file_path}", tofile=f"b/{file_path}")
        patch_lines.extend(diff)
    
    patch = "".join(patch_lines)
    
    # Also create the commit if requested
    full_message = f"{commit_message}\n\nGenerated by Autonomous Remediation Agent"
    
    return {
        "patch": patch,
        "commit_message": full_message,
        "files_changed": [c["file"] for c in changes],
    }


def create_kubectl_rollback_tool(namespace: str, deployment: str, revision: int = 0) -> Dict[str, Any]:
    """Tool: Create kubectl rollback command"""
    if revision == 0:
        cmd = f"kubectl rollout undo deployment/{deployment} -n {namespace}"
    else:
        cmd = f"kubectl rollout undo deployment/{deployment} -n {namespace} --to-revision={revision}"
    
    # Also provide verification commands
    verify_cmds = [
        f"kubectl rollout status deployment/{deployment} -n {namespace}",
        f"kubectl get pods -n {namespace} -l app={deployment}",
    ]
    
    return {
        "rollback_command": cmd,
        "verification_commands": verify_cmds,
        "description": f"Rollback {deployment} in {namespace}" + (f" to revision {revision}" if revision else " to previous revision"),
    }