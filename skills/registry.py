"""
Skills System for Autonomous Remediation Agent

Modular, loadable skill definitions following OpenSRE and HolmesGPT patterns.
Skills are the primary integration surface - methodology docs plus optional scripts.
"""

import os
import yaml
import importlib.util
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import structlog

logger = structlog.get_logger()


class SkillCategory(str, Enum):
    """Categories of skills following OpenSRE taxonomy"""
    OBSERVABILITY = "observability"
    KUBERNETES = "kubernetes"
    CLOUD = "cloud"
    DATABASE = "database"
    MESSAGING = "messaging"
    INCIDENT_TOOLING = "incident_tooling"
    VERSION_CONTROL = "version_control"
    TICKETING = "ticketing"
    MEMORY = "memory"
    INFRASTRUCTURE = "infrastructure"
    REMEDIATION = "remediation"
    SECURITY = "security"
    NETWORK = "network"
    STORAGE = "storage"


@dataclass
class SkillParameter:
    """Parameter definition for a skill"""
    name: str
    type: str  # string, integer, boolean, array, object
    description: str
    required: bool = True
    default: Any = None
    enum: List[Any] = field(default_factory=list)


@dataclass
class SkillStep:
    """A single step in a skill execution"""
    name: str
    description: str
    tool: str  # Tool to call (e.g., "query_prometheus", "get_k8s_pods")
    parameters: Dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None  # Optional condition to execute this step
    output_key: Optional[str] = None  # Key to store output in context


@dataclass
class Skill:
    """A complete skill definition"""
    id: str
    name: str
    description: str
    category: SkillCategory
    version: str = "1.0.0"
    author: str = ""
    tags: List[str] = field(default_factory=list)
    parameters: List[SkillParameter] = field(default_factory=list)
    steps: List[SkillStep] = field(default_factory=list)
    requires_approval: bool = False
    risk_level: str = "low"  # low, medium, high, critical
    estimated_duration_seconds: int = 60
    # Optional Python script for complex logic
    script_path: Optional[str] = None
    # Skill metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "version": self.version,
            "author": self.author,
            "tags": self.tags,
            "parameters": [
                {
                    "name": p.name,
                    "type": p.type,
                    "description": p.description,
                    "required": p.required,
                    "default": p.default,
                    "enum": p.enum,
                }
                for p in self.parameters
            ],
            "steps": [
                {
                    "name": s.name,
                    "description": s.description,
                    "tool": s.tool,
                    "parameters": s.parameters,
                    "condition": s.condition,
                    "output_key": s.output_key,
                }
                for s in self.steps
            ],
            "requires_approval": self.requires_approval,
            "risk_level": self.risk_level,
            "estimated_duration_seconds": self.estimated_duration_seconds,
            "script_path": self.script_path,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Skill":
        return cls(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            category=SkillCategory(data["category"]),
            version=data.get("version", "1.0.0"),
            author=data.get("author", ""),
            tags=data.get("tags", []),
            parameters=[
                SkillParameter(**p) for p in data.get("parameters", [])
            ],
            steps=[
                SkillStep(**s) for s in data.get("steps", [])
            ],
            requires_approval=data.get("requires_approval", False),
            risk_level=data.get("risk_level", "low"),
            estimated_duration_seconds=data.get("estimated_duration_seconds", 60),
            script_path=data.get("script_path"),
            metadata=data.get("metadata", {}),
        )


class SkillRegistry:
    """
    Registry for managing skills - loading, discovery, and execution.
    
    Based on OpenSRE's skill system:
    - Skills loaded progressively (metadata first, full content on demand)
    - Organized by category
    - Can be enabled/disabled per team
    - Credentials supplied via environment variables at runtime
    """
    
    def __init__(self, skills_dir: str = "skills"):
        self.skills_dir = Path(skills_dir)
        self.skills: Dict[str, Skill] = {}
        self.skill_scripts: Dict[str, Callable] = {}
        self.enabled_skills: Set[str] = set()
        self._load_skills()
    
    def _load_skills(self):
        """Load all skills from the skills directory"""
        if not self.skills_dir.exists():
            logger.warning("Skills directory not found", path=str(self.skills_dir))
            return
        
        for category_dir in self.skills_dir.iterdir():
            if not category_dir.is_dir():
                continue
            
            for skill_dir in category_dir.iterdir():
                if not skill_dir.is_dir():
                    continue
                
                skill_file = skill_dir / "SKILL.yaml"
                if not skill_file.exists():
                    skill_file = skill_dir / "SKILL.yml"
                
                if skill_file.exists():
                    try:
                        skill = self._load_skill(skill_file, skill_dir)
                        self.skills[skill.id] = skill
                        self.enabled_skills.add(skill.id)
                        logger.info("Loaded skill", skill_id=skill.id, category=skill.category.value)
                    except Exception as e:
                        logger.error("Failed to load skill", path=str(skill_file), error=str(e))
    
    def _load_skill(self, skill_file: Path, skill_dir: Path) -> Skill:
        """Load a single skill from YAML file"""
        with open(skill_file, 'r') as f:
            data = yaml.safe_load(f)
        
        skill = Skill.from_dict(data)
        
        # Load optional script
        script_file = skill_dir / "script.py"
        if script_file.exists() or skill.script_path:
            script_path = skill.script_path or str(script_file)
            self._load_skill_script(skill.id, script_path)
        
        return skill
    
    def _load_skill_script(self, skill_id: str, script_path: str):
        """Load a skill's Python script as a callable"""
        try:
            spec = importlib.util.spec_from_file_location(f"skill_{skill_id}", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Expect a `execute` function in the script
            if hasattr(module, 'execute'):
                self.skill_scripts[skill_id] = module.execute
                logger.info("Loaded skill script", skill_id=skill_id)
            else:
                logger.warning("Skill script missing execute function", skill_id=skill_id)
        except Exception as e:
            logger.error("Failed to load skill script", skill_id=skill_id, error=str(e))
    
    def get_skill(self, skill_id: str) -> Optional[Skill]:
        """Get a skill by ID"""
        return self.skills.get(skill_id)
    
    def list_skills(self, category: Optional[SkillCategory] = None, enabled_only: bool = True) -> List[Skill]:
        """List all skills, optionally filtered by category"""
        skills = self.skills.values()
        if enabled_only:
            skills = [s for s in skills if s.id in self.enabled_skills]
        if category:
            skills = [s for s in skills if s.category == category]
        return list(skills)
    
    def get_skill_metadata(self, skill_id: str) -> Optional[Dict[str, Any]]:
        """Get lightweight metadata for a skill (for agent skill selection)"""
        skill = self.skills.get(skill_id)
        if not skill:
            return None
        
        return {
            "id": skill.id,
            "name": skill.name,
            "description": skill.description,
            "category": skill.category.value,
            "tags": skill.tags,
            "parameters": [
                {"name": p.name, "type": p.type, "description": p.description, "required": p.required}
                for p in skill.parameters
            ],
            "requires_approval": skill.requires_approval,
            "risk_level": skill.risk_level,
            "estimated_duration_seconds": skill.estimated_duration_seconds,
        }
    
    def get_all_metadata(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """Get metadata for all skills (lightweight, ~100 tokens each)"""
        return [
            self.get_skill_metadata(s.id)
            for s in self.list_skills(enabled_only=enabled_only)
        ]
    
    def enable_skill(self, skill_id: str) -> bool:
        """Enable a skill"""
        if skill_id in self.skills:
            self.enabled_skills.add(skill_id)
            return True
        return False
    
    def disable_skill(self, skill_id: str) -> bool:
        """Disable a skill"""
        self.enabled_skills.discard(skill_id)
        return True
    
    async def execute_skill(
        self,
        skill_id: str,
        parameters: Dict[str, Any],
        tool_executor: Callable,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Execute a skill with given parameters.
        
        Args:
            skill_id: ID of skill to execute
            parameters: Input parameters for the skill
            tool_executor: Function to execute tools (tool_name, params) -> result
            context: Optional context from previous steps
        
        Returns:
            Dict with execution results
        """
        skill = self.skills.get(skill_id)
        if not skill:
            return {"error": f"Skill {skill_id} not found"}
        
        if skill_id not in self.enabled_skills:
            return {"error": f"Skill {skill_id} is disabled"}
        
        # Validate required parameters
        for param in skill.parameters:
            if param.required and param.name not in parameters:
                if param.default is not None:
                    parameters[param.name] = param.default
                else:
                    return {"error": f"Missing required parameter: {param.name}"}
        
        # Execute skill
        context = context or {}
        context.update(parameters)
        results = {}
        
        # If skill has a custom script, use it
        if skill_id in self.skill_scripts:
            try:
                script_result = await self.skill_scripts[skill_id](parameters, tool_executor, context)
                return {"success": True, "result": script_result, "skill_id": skill_id}
            except Exception as e:
                logger.error("Skill script execution failed", skill_id=skill_id, error=str(e))
                return {"error": f"Script execution failed: {str(e)}", "skill_id": skill_id}
        
        # Otherwise execute steps sequentially
        for step in skill.steps:
            # Check condition
            if step.condition:
                # Simple condition evaluation (could be enhanced)
                if not self._eval_condition(step.condition, context):
                    continue
            
            # Prepare tool parameters with context substitution
            tool_params = self._substitute_params(step.parameters, context)
            
            try:
                result = await tool_executor(step.tool, tool_params)
                
                # Store output in context if output_key specified
                if step.output_key:
                    context[step.output_key] = result
                
                results[step.name] = result
                
            except Exception as e:
                logger.error("Skill step failed", skill_id=skill_id, step=step.name, error=str(e))
                return {
                    "error": f"Step {step.name} failed: {str(e)}",
                    "skill_id": skill_id,
                    "completed_steps": list(results.keys()),
                    "partial_results": results,
                }
        
        return {"success": True, "results": results, "skill_id": skill_id}
    
    def _eval_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a simple condition against context"""
        # Simple implementation - could be enhanced with a proper expression evaluator
        try:
            # Replace context variables
            for key, value in context.items():
                condition = condition.replace(f"${key}", str(value))
            return eval(condition)  # Note: use safe eval in production
        except Exception:
            return False
    
    def _substitute_params(self, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute context variables in parameters"""
        result = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                var_name = value[2:-1]
                result[key] = context.get(var_name, value)
            else:
                result[key] = value
        return result


# ==================== Built-in Skills ====================

def create_builtin_skills() -> List[Skill]:
    """Create the built-in skill definitions"""
    
    skills = []
    
    # --- Observability Skills ---
    
    skills.append(Skill(
        id="query_prometheus_metrics",
        name="Query Prometheus Metrics",
        description="Query Prometheus for time-series metrics with PromQL",
        category=SkillCategory.OBSERVABILITY,
        parameters=[
            SkillParameter("query", "string", "PromQL query string", required=True),
            SkillParameter("start", "string", "Start time (RFC3339 or relative like '1h')", required=False, default="1h"),
            SkillParameter("end", "string", "End time (RFC3339 or 'now')", required=False, default="now"),
            SkillParameter("step", "string", "Query step (e.g., '15s', '1m')", required=False, default="15s"),
        ],
        steps=[
            SkillStep(
                name="query_range",
                description="Execute PromQL range query",
                tool="query_prometheus_range",
                parameters={"query": "${query}", "start": "${start}", "end": "${end}", "step": "${step}"},
                output_key="prometheus_results",
            ),
        ],
        tags=["metrics", "prometheus", "promql"],
    ))
    
    skills.append(Skill(
        id="query_loki_logs",
        name="Query Loki Logs",
        description="Query Loki for log lines with LogQL",
        category=SkillCategory.OBSERVABILITY,
        parameters=[
            SkillParameter("query", "string", "LogQL query (e.g., '{service=\"api\"} |= \"error\"')", required=True),
            SkillParameter("start", "string", "Start time (RFC3339)", required=True),
            SkillParameter("end", "string", "End time (RFC3339)", required=True),
            SkillParameter("limit", "integer", "Max log lines", required=False, default=1000),
            SkillParameter("direction", "string", "Query direction", required=False, default="backward", enum=["forward", "backward"]),
        ],
        steps=[
            SkillStep(
                name="query_logs",
                description="Execute LogQL query",
                tool="query_loki_logs",
                parameters={"query": "${query}", "start": "${start}", "end": "${end}", "limit": "${limit}", "direction": "${direction}"},
                output_key="loki_results",
            ),
        ],
        tags=["logs", "loki", "logql"],
    ))
    
    skills.append(Skill(
        id="analyze_log_patterns",
        name="Analyze Log Patterns",
        description="Extract log templates and compute entropy using Drain-like clustering",
        category=SkillCategory.OBSERVABILITY,
        parameters=[
            SkillParameter("log_lines", "array", "Array of log lines to analyze", required=True),
            SkillParameter("similarity_threshold", "number", "Template similarity threshold", required=False, default=0.5),
        ],
        steps=[
            SkillStep(
                name="cluster_logs",
                description="Cluster log lines into templates",
                tool="analyze_log_templates",
                parameters={"log_lines": "${log_lines}", "threshold": "${similarity_threshold}"},
                output_key="log_templates",
            ),
            SkillStep(
                name="compute_entropy",
                description="Compute Shannon entropy of template distribution",
                tool="compute_shannon_entropy",
                parameters={"templates": "${log_templates}"},
                output_key="entropy_score",
            ),
        ],
        tags=["logs", "entropy", "anomaly", "drain"],
    ))
    
    # --- Kubernetes Skills ---
    
    skills.append(Skill(
        id="get_k8s_pod_status",
        name="Get Kubernetes Pod Status",
        description="Get detailed pod status including containers, restarts, and conditions",
        category=SkillCategory.KUBERNETES,
        parameters=[
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("pod", "string", "Pod name (optional, if omitted gets all pods)", required=False),
            SkillParameter("label_selector", "string", "Label selector for pods", required=False),
            SkillParameter("include_logs", "boolean", "Include recent container logs", required=False, default=False),
            SkillParameter("log_lines", "integer", "Number of log lines", required=False, default=100),
        ],
        steps=[
            SkillStep(
                name="get_pods",
                description="Get pod status from Kubernetes API",
                tool="get_k8s_pods",
                parameters={"namespace": "${namespace}", "pod": "${pod}", "label_selector": "${label_selector}", "include_logs": "${include_logs}", "log_lines": "${log_lines}"},
                output_key="pod_status",
            ),
        ],
        tags=["kubernetes", "pods", "status"],
    ))
    
    skills.append(Skill(
        id="get_k8s_deployment_status",
        name="Get Kubernetes Deployment Status",
        description="Get deployment status including replica counts and rollout status",
        category=SkillCategory.KUBERNETES,
        parameters=[
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("deployment", "string", "Deployment name", required=True),
        ],
        steps=[
            SkillStep(
                name="get_deployment",
                description="Get deployment status",
                tool="get_k8s_deployment",
                parameters={"namespace": "${namespace}", "deployment": "${deployment}"},
                output_key="deployment_status",
            ),
        ],
        tags=["kubernetes", "deployment", "rollout"],
    ))
    
    skills.append(Skill(
        id="get_k8s_events",
        name="Get Kubernetes Events",
        description="Fetch Kubernetes events for a namespace, pod, or deployment",
        category=SkillCategory.KUBERNETES,
        parameters=[
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("pod", "string", "Optional pod name to filter events", required=False),
            SkillParameter("deployment", "string", "Optional deployment name to filter events", required=False),
            SkillParameter("since_minutes", "integer", "Look back window in minutes", required=False, default=60),
            SkillParameter("event_types", "array", "Event types to include", required=False, default=["Warning", "Error"]),
        ],
        steps=[
            SkillStep(
                name="get_events",
                description="Fetch Kubernetes events",
                tool="get_k8s_events",
                parameters={"namespace": "${namespace}", "pod": "${pod}", "deployment": "${deployment}", "since_minutes": "${since_minutes}", "event_types": "${event_types}"},
                output_key="k8s_events",
            ),
        ],
        tags=["kubernetes", "events", "troubleshooting"],
    ))
    
    skills.append(Skill(
        id="k8s_rollout_rollback",
        name="Kubernetes Rollout Rollback",
        description="Generate kubectl rollout undo command for a deployment",
        category=SkillCategory.KUBERNETES,
        requires_approval=True,
        risk_level="medium",
        parameters=[
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("deployment", "string", "Deployment name", required=True),
            SkillParameter("revision", "integer", "Specific revision to rollback to (0 = previous)", required=False, default=0),
        ],
        steps=[
            SkillStep(
                name="generate_rollback",
                description="Generate kubectl rollback command",
                tool="create_kubectl_rollback",
                parameters={"namespace": "${namespace}", "deployment": "${deployment}", "revision": "${revision}"},
                output_key="rollback_command",
            ),
        ],
        tags=["kubernetes", "rollback", "remediation"],
    ))
    
    skills.append(Skill(
        id="k8s_scale_deployment",
        name="Scale Kubernetes Deployment",
        description="Scale a deployment to a target replica count",
        category=SkillCategory.KUBERNETES,
        requires_approval=True,
        risk_level="medium",
        parameters=[
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("deployment", "string", "Deployment name", required=True),
            SkillParameter("replicas", "integer", "Target replica count", required=True),
        ],
        steps=[
            SkillStep(
                name="scale",
                description="Scale deployment via kubectl",
                tool="kubectl_scale",
                parameters={"namespace": "${namespace}", "deployment": "${deployment}", "replicas": "${replicas}"},
                output_key="scale_result",
            ),
        ],
        tags=["kubernetes", "scale", "hpa", "remediation"],
    ))
    
    # --- Git/Version Control Skills ---
    
    skills.append(Skill(
        id="git_blame_recent_changes",
        name="Git Blame and Recent Changes",
        description="Get git blame and recent commits for a file or directory to identify regressions",
        category=SkillCategory.VERSION_CONTROL,
        parameters=[
            SkillParameter("repo_path", "string", "Path to git repository", required=True),
            SkillParameter("file_path", "string", "File or directory path relative to repo root", required=True),
            SkillParameter("since_days", "integer", "Look back window in days", required=False, default=14),
            SkillParameter("include_diff", "boolean", "Include diff for recent commits", required=False, default=True),
        ],
        steps=[
            SkillStep(
                name="get_blame",
                description="Get git blame and recent commits",
                tool="fetch_git_blame_diff",
                parameters={"repo_path": "${repo_path}", "file_path": "${file_path}", "since_days": "${since_days}", "include_diff": "${include_diff}"},
                output_key="git_blame",
            ),
        ],
        tags=["git", "blame", "commits", "regression"],
    ))
    
    skills.append(Skill(
        id="generate_git_patch",
        name="Generate Git Patch",
        description="Generate a git patch file for a proposed code/config change",
        category=SkillCategory.VERSION_CONTROL,
        requires_approval=True,
        risk_level="low",
        parameters=[
            SkillParameter("repo_path", "string", "Path to git repository", required=True),
            SkillParameter("changes", "array", "Array of changes with file, old_content, new_content", required=True),
            SkillParameter("commit_message", "string", "Commit message for the patch", required=True),
        ],
        steps=[
            SkillStep(
                name="generate_patch",
                description="Generate git patch",
                tool="generate_git_patch",
                parameters={"repo_path": "${repo_path}", "changes": "${changes}", "commit_message": "${commit_message}"},
                output_key="git_patch",
            ),
        ],
        tags=["git", "patch", "remediation", "pr"],
    ))
    
    # --- Root Cause Analysis Skills ---
    
    skills.append(Skill(
        id="analyze_stack_trace",
        name="Analyze Stack Trace",
        description="Parse and analyze a stack trace to identify error type, origin, and potential causes",
        category=SkillCategory.OBSERVABILITY,
        parameters=[
            SkillParameter("stack_trace", "string", "Raw stack trace text", required=True),
            SkillParameter("language", "string", "Programming language", required=False, default="python", enum=["python", "java", "go", "rust", "node", "cpp"]),
            SkillParameter("service", "string", "Service name for context", required=False),
        ],
        steps=[
            SkillStep(
                name="analyze_trace",
                description="Analyze stack trace",
                tool="analyze_stack_trace",
                parameters={"stack_trace": "${stack_trace}", "language": "${language}", "service": "${service}"},
                output_key="stack_analysis",
            ),
        ],
        tags=["stacktrace", "error", "debugging", "root_cause"],
    ))
    
    skills.append(Skill(
        id="correlate_service_dependencies",
        name="Correlate Service Dependencies",
        description="Get service dependency graph from trace data to understand blast radius",
        category=SkillCategory.INFRASTRUCTURE,
        parameters=[
            SkillParameter("service", "string", "Service name", required=True),
            SkillParameter("lookback_hours", "integer", "Time window for trace analysis", required=False, default=24),
        ],
        steps=[
            SkillStep(
                name="get_dependencies",
                description="Get upstream and downstream dependencies",
                tool="get_service_dependencies",
                parameters={"service": "${service}", "lookback_hours": "${lookback_hours}"},
                output_key="service_dependencies",
            ),
        ],
        tags=["dependencies", "blast_radius", "topology", "traces"],
    ))
    
    skills.append(Skill(
        id="query_clickhouse_traces",
        name="Query ClickHouse Traces",
        description="Query ClickHouse for distributed trace data",
        category=SkillCategory.OBSERVABILITY,
        parameters=[
            SkillParameter("query", "string", "SQL query for ClickHouse", required=True),
            SkillParameter("limit", "integer", "Max rows to return", required=False, default=1000),
        ],
        steps=[
            SkillStep(
                name="query_traces",
                description="Execute ClickHouse query",
                tool="query_clickhouse",
                parameters={"query": "${query}", "limit": "${limit}"},
                output_key="trace_results",
            ),
        ],
        tags=["traces", "clickhouse", "distributed_tracing"],
    ))
    
    # --- Remediation Skills ---
    
    skills.append(Skill(
        id="simulate_remediation",
        name="Simulate Remediation Action",
        description="Dry-run a remediation action against a staging environment or simulator",
        category=SkillCategory.REMEDIATION,
        parameters=[
            SkillParameter("action_type", "string", "Type of remediation action", required=True, enum=["rollback", "config_change", "scale", "restart", "circuit_breaker", "traffic_shift"]),
            SkillParameter("target", "string", "Target resource (deployment, configmap, service)", required=True),
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("parameters", "object", "Action-specific parameters", required=True),
        ],
        steps=[
            SkillStep(
                name="simulate",
                description="Run dry-run simulation",
                tool="simulate_remediation",
                parameters={"action_type": "${action_type}", "target": "${target}", "namespace": "${namespace}", "parameters": "${parameters}"},
                output_key="simulation_result",
            ),
        ],
        tags=["remediation", "dry_run", "simulation", "safety"],
    ))
    
    skills.append(Skill(
        id="assess_remediation_risk",
        name="Assess Remediation Risk",
        description="Calculate risk score for a remediation action using the risk rubric",
        category=SkillCategory.REMEDIATION,
        parameters=[
            SkillParameter("action_type", "string", "Type of remediation action", required=True, enum=["rollback", "config_change", "scale", "restart", "circuit_breaker", "traffic_shift"]),
            SkillParameter("target", "string", "Target resource", required=True),
            SkillParameter("namespace", "string", "Kubernetes namespace", required=True),
            SkillParameter("context", "object", "Additional context for risk factors", required=False, default={}),
        ],
        steps=[
            SkillStep(
                name="assess_risk",
                description="Calculate risk score",
                tool="score_remediation_risk",
                parameters={"action_type": "${action_type}", "target": "${target}", "namespace": "${namespace}", "context": "${context}"},
                output_key="risk_assessment",
            ),
        ],
        tags=["risk", "safety", "remediation", "approval"],
    ))
    
    # --- Memory Skills ---
    
    skills.append(Skill(
        id="search_episodic_memory",
        name="Search Episodic Memory",
        description="Search past incident episodes for similar cases using vector similarity",
        category=SkillCategory.MEMORY,
        parameters=[
            SkillParameter("query_text", "string", "Query text describing the incident", required=True),
            SkillParameter("limit", "integer", "Max results", required=False, default=5),
            SkillParameter("min_score", "number", "Minimum similarity score", required=False, default=0.7),
            SkillParameter("issue_type", "string", "Filter by issue type", required=False),
            SkillParameter("components", "array", "Filter by affected components", required=False),
        ],
        steps=[
            SkillStep(
                name="search_memory",
                description="Search episodic memory",
                tool="episodic_memory_search",
                parameters={"query_text": "${query_text}", "limit": "${limit}", "min_score": "${min_score}", "issue_type": "${issue_type}", "components": "${components}"},
                output_key="similar_episodes",
            ),
        ],
        tags=["memory", "episodic", "similarity", "past_incidents"],
    ))
    
    skills.append(Skill(
        id="synthesize_playbook",
        name="Synthesize Strategy Playbook",
        description="Generate a strategy playbook from similar resolved incidents",
        category=SkillCategory.MEMORY,
        parameters=[
            SkillParameter("issue_type", "string", "Issue type to synthesize playbook for", required=True),
            SkillParameter("min_episodes", "integer", "Minimum episodes required", required=False, default=3),
        ],
        steps=[
            SkillStep(
                name="synthesize",
                description="Synthesize playbook from memory",
                tool="episodic_memory_synthesize",
                parameters={"issue_type": "${issue_type}", "min_episodes": "${min_episodes}"},
                output_key="strategy_playbook",
            ),
        ],
        tags=["memory", "playbook", "strategy", "best_practices"],
    ))
    
    # --- Infrastructure/Topology Skills ---
    
    skills.append(Skill(
        id="get_blast_radius",
        name="Get Blast Radius",
        description="Analyze blast radius for a service including upstream, downstream, and shared infrastructure",
        category=SkillCategory.INFRASTRUCTURE,
        parameters=[
            SkillParameter("service_id", "string", "Service ID to analyze", required=True),
            SkillParameter("max_hops", "integer", "Maximum hops for dependency traversal", required=False, default=3),
        ],
        steps=[
            SkillStep(
                name="analyze_blast_radius",
                description="Compute blast radius",
                tool="topology_blast_radius",
                parameters={"service_id": "${service_id}", "max_hops": "${max_hops}"},
                output_key="blast_radius",
            ),
        ],
        tags=["blast_radius", "topology", "dependencies", "impact_analysis"],
    ))
    
    skills.append(Skill(
        id="get_deployment_impact",
        name="Get Deployment Impact",
        description="Get impact analysis for a specific deployment",
        category=SkillCategory.INFRASTRUCTURE,
        parameters=[
            SkillParameter("deployment_id", "string", "Deployment ID", required=True),
        ],
        steps=[
            SkillStep(
                name="analyze_deployment",
                description="Analyze deployment impact",
                tool="topology_deployment_impact",
                parameters={"deployment_id": "${deployment_id}"},
                output_key="deployment_impact",
            ),
        ],
        tags=["deployment", "impact", "rollback", "risk"],
    ))
    
    skills.append(Skill(
        id="get_service_ownership",
        name="Get Service Ownership",
        description="Get team ownership and on-call information for a service",
        category=SkillCategory.INFRASTRUCTURE,
        parameters=[
            SkillParameter("service_id", "string", "Service ID", required=True),
        ],
        steps=[
            SkillStep(
                name="get_ownership",
                description="Get service ownership",
                tool="topology_ownership",
                parameters={"service_id": "${service_id}"},
                output_key="ownership",
            ),
            SkillStep(
                name="get_oncall",
                description="Get current on-call",
                tool="topology_oncall",
                parameters={"service_id": "${service_id}"},
                output_key="oncall",
            ),
        ],
        tags=["ownership", "oncall", "team", "escalation"],
    ))
    
    return skills


def save_skills_to_disk(skills: List[Skill], skills_dir: str = "skills"):
    """Save skill definitions to disk as YAML files"""
    skills_path = Path(skills_dir)
    
    for skill in skills:
        category_dir = skills_path / skill.category.value
        category_dir.mkdir(parents=True, exist_ok=True)
        
        skill_dir = category_dir / skill.id
        skill_dir.mkdir(exist_ok=True)
        
        skill_file = skill_dir / "SKILL.yaml"
        with open(skill_file, 'w') as f:
            yaml.dump(skill.to_dict(), f, default_flow_style=False, sort_keys=False)
        
        logger.info("Saved skill", skill_id=skill.id, path=str(skill_file))


# Initialize built-in skills on import
BUILTIN_SKILLS = create_builtin_skills()