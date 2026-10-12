"""
Tool Schema Contracts for the Agentic Root-Cause Engine

Each tool has a strict JSON schema for deterministic LLM tool calling.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import json


# Tool definitions for LLM function calling
TOOL_SCHEMAS = {
    "query_prometheus_range": {
        "type": "function",
        "function": {
            "name": "query_prometheus_range",
            "description": "Query Prometheus for time-series data over a range. Returns metric values with timestamps.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "PromQL query string (e.g., 'rate(http_requests_total[5m])')"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start timestamp (RFC3339 or unix timestamp)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End timestamp (RFC3339 or unix timestamp)"
                    },
                    "step": {
                        "type": "string",
                        "description": "Query resolution step (e.g., '15s', '1m')",
                        "default": "15s"
                    }
                },
                "required": ["query", "start", "end"],
                "additionalProperties": False
            }
        }
    },
    "query_prometheus_instant": {
        "type": "function",
        "function": {
            "name": "query_prometheus_instant",
            "description": "Query Prometheus for instant vector at a specific timestamp.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "PromQL query string"
                    },
                    "time": {
                        "type": "string",
                        "description": "Evaluation timestamp (RFC3339 or unix timestamp)",
                        "default": "now"
                    }
                },
                "required": ["query"],
                "additionalProperties": False
            }
        }
    },
    "get_k8s_events": {
        "type": "function",
        "function": {
            "name": "get_k8s_events",
            "description": "Fetch Kubernetes events for a namespace, pod, or deployment. Returns warning/error events.",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Kubernetes namespace"
                    },
                    "pod": {
                        "type": "string",
                        "description": "Optional pod name to filter events"
                    },
                    "deployment": {
                        "type": "string",
                        "description": "Optional deployment name to filter events"
                    },
                    "since_minutes": {
                        "type": "integer",
                        "description": "Look back window in minutes",
                        "default": 60
                    },
                    "event_types": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["Warning", "Error", "Normal"]},
                        "description": "Event types to include",
                        "default": ["Warning", "Error"]
                    }
                },
                "required": ["namespace"],
                "additionalProperties": False
            }
        }
    },
    "get_pod_status": {
        "type": "function",
        "function": {
            "name": "get_pod_status",
            "description": "Get detailed pod status including container states, restarts, resource limits, and conditions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "description": "Kubernetes namespace"},
                    "pod": {"type": "string", "description": "Pod name"},
                    "include_logs": {"type": "boolean", "description": "Include recent container logs", "default": False},
                    "log_lines": {"type": "integer", "description": "Number of log lines if include_logs=true", "default": 100}
                },
                "required": ["namespace", "pod"],
                "additionalProperties": False
            }
        }
    },
    "get_deployment_status": {
        "type": "function",
        "function": {
            "name": "get_deployment_status",
            "description": "Get deployment status including replica counts, rollout status, and pod template hash.",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "description": "Kubernetes namespace"},
                    "deployment": {"type": "string", "description": "Deployment name"}
                },
                "required": ["namespace", "deployment"],
                "additionalProperties": False
            }
        }
    },
    "fetch_git_blame_diff": {
        "type": "function",
        "function": {
            "name": "fetch_git_blame_diff",
            "description": "Get git blame and recent commits for a file or directory. Identifies recent changes that could cause regressions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "repo_path": {"type": "string", "description": "Path to git repository"},
                    "file_path": {"type": "string", "description": "File or directory path relative to repo root"},
                    "since_days": {"type": "integer", "description": "Look back window in days", "default": 14},
                    "include_diff": {"type": "boolean", "description": "Include diff for recent commits", "default": True}
                },
                "required": ["repo_path", "file_path"],
                "additionalProperties": False
            }
        }
    },
    "analyze_stack_trace": {
        "type": "function",
        "function": {
            "name": "analyze_stack_trace",
            "description": "Parse and analyze a stack trace to identify error type, origin, and potential causes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "stack_trace": {"type": "string", "description": "Raw stack trace text"},
                    "language": {"type": "string", "description": "Programming language (python, java, go, rust, node)", "default": "python"},
                    "service": {"type": "string", "description": "Service name for context"}
                },
                "required": ["stack_trace"],
                "additionalProperties": False
            }
        }
    },
    "query_clickhouse_logs": {
        "type": "function",
        "function": {
            "name": "query_clickhouse_logs",
            "description": "Query ClickHouse for structured logs with filtering and aggregation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query for ClickHouse"},
                    "limit": {"type": "integer", "description": "Max rows to return", "default": 1000}
                },
                "required": ["query"],
                "additionalProperties": False
            }
        }
    },
    "query_loki_logs": {
        "type": "function",
        "function": {
            "name": "query_loki_logs",
            "description": "Query Loki for log lines with label filters and time range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "LogQL query (e.g., '{service=\"service-a\"} |= \"error\"')"},
                    "start": {"type": "string", "description": "Start timestamp (RFC3339)"},
                    "end": {"type": "string", "description": "End timestamp (RFC3339)"},
                    "limit": {"type": "integer", "description": "Max log lines", "default": 1000},
                    "direction": {"type": "string", "enum": ["forward", "backward"], "default": "backward"}
                },
                "required": ["query", "start", "end"],
                "additionalProperties": False
            }
        }
    },
    "get_service_dependencies": {
        "type": "function",
        "function": {
            "name": "get_service_dependencies",
            "description": "Get service dependency graph from trace data. Returns upstream/downstream services.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service": {"type": "string", "description": "Service name"},
                    "lookback_hours": {"type": "integer", "description": "Time window for trace analysis", "default": 24}
                },
                "required": ["service"],
                "additionalProperties": False
            }
        }
    },
    "simulate_remediation": {
        "type": "function",
        "function": {
            "name": "simulate_remediation",
            "description": "Dry-run a remediation action against a staging environment or simulator. Returns predicted impact.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action_type": {"type": "string", "enum": ["rollback", "config_change", "scale", "restart", "circuit_breaker", "traffic_shift"]},
                    "target": {"type": "string", "description": "Target resource (deployment, configmap, service)"},
                    "namespace": {"type": "string", "description": "Kubernetes namespace"},
                    "parameters": {"type": "object", "description": "Action-specific parameters"}
                },
                "required": ["action_type", "target", "namespace", "parameters"],
                "additionalProperties": False
            }
        }
    },
    "generate_git_patch": {
        "type": "function",
        "function": {
            "name": "generate_git_patch",
            "description": "Generate a git patch file for a proposed code/config change.",
            "parameters": {
                "type": "object",
                "properties": {
                    "repo_path": {"type": "string", "description": "Path to git repository"},
                    "changes": {"type": "array", "items": {"type": "object", "properties": {"file": {"type": "string"}, "old_content": {"type": "string"}, "new_content": {"type": "string"}}, "required": ["file", "old_content", "new_content"]}},
                    "commit_message": {"type": "string", "description": "Commit message for the patch"}
                },
                "required": ["repo_path", "changes", "commit_message"],
                "additionalProperties": False
            }
        }
    },
    "create_kubectl_rollback": {
        "type": "function",
        "function": {
            "name": "create_kubectl_rollback",
            "description": "Generate kubectl rollout undo command for a deployment.",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string", "description": "Kubernetes namespace"},
                    "deployment": {"type": "string", "description": "Deployment name"},
                    "revision": {"type": "integer", "description": "Specific revision to rollback to (0 = previous)", "default": 0}
                },
                "required": ["namespace", "deployment"],
                "additionalProperties": False
            }
        }
    }
}


def get_tool_schemas() -> List[Dict[str, Any]]:
    """Return all tool schemas as a list for LLM function calling."""
    return list(TOOL_SCHEMAS.values())


def get_tool_schema(name: str) -> Optional[Dict[str, Any]]:
    """Get a specific tool schema by name."""
    return TOOL_SCHEMAS.get(name)


# Response models for tool outputs
class PrometheusRangeResponse(BaseModel):
    status: str
    data: Dict[str, Any]
    query: str
    start: str
    end: str
    step: str


class K8sEventsResponse(BaseModel):
    events: List[Dict[str, Any]]
    namespace: str
    filter: Dict[str, Any]


class PodStatusResponse(BaseModel):
    pod: str
    namespace: str
    phase: str
    conditions: List[Dict[str, Any]]
    containers: List[Dict[str, Any]]
    restarts: int
    logs: Optional[str] = None


class GitBlameResponse(BaseModel):
    file: str
    commits: List[Dict[str, Any]]
    blame: List[Dict[str, Any]]


class StackTraceAnalysis(BaseModel):
    error_type: str
    origin: str  # file:line
    service: str
    potential_causes: List[str]
    related_patterns: List[str]


class SimulationResult(BaseModel):
    action_type: str
    target: str
    predicted_impact: Dict[str, Any]
    risk_score: float
    confidence: float
    warnings: List[str]


def validate_tool_output(tool_name: str, output: Dict[str, Any]) -> bool:
    """Validate tool output against expected schema."""
    # In production, use JSON schema validation
    return True