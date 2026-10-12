"""
Enhanced LangGraph State Machine for Autonomous Remediation Agent

Integrates:
- Episodic Memory for learning from past incidents
- Service Topology Graph for blast radius analysis
- Skills System for modular investigation capabilities
- Subagent Orchestrator for parallel specialist investigation
- Evaluation Framework integration

State flow: DETECT -> CORRELATE -> ISOLATE -> VERIFY -> HUMAN_IN_LOOP -> REMEDIATE
"""

from typing import Dict, Any, List, Optional, Literal
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
import json
import structlog
import asyncio

from ..state import AgentContext, AgentState, Incident, CorrelationResult, IsolationResult, VerificationResult, RemediationPlan
from ..memory.episodic_memory import EpisodicMemory, Episode, EpisodeStatus, create_episodic_memory
from ..memory.topology_graph import ServiceTopologyGraph, create_topology_graph, RelationshipType
from ..skills.registry import SkillRegistry, create_builtin_skills, save_skills_to_disk
from ..subagents.orchestrator import SubagentOrchestrator, SubagentTask, SubagentType, create_subagent_orchestrator
from ..verification.risk_scoring import RiskScorer, ActionType

from .schemas import get_tool_schemas, get_tool_schema
from .implementations import (
    query_prometheus_range_tool,
    query_prometheus_instant_tool,
    get_k8s_events_tool,
    get_pod_status_tool,
    get_deployment_status_tool,
    fetch_git_blame_diff_tool,
    analyze_stack_trace_tool,
    query_clickhouse_logs_tool,
    query_loki_logs_tool,
    get_service_dependencies_tool,
    simulate_remediation_tool,
    generate_git_patch_tool,
    create_kubectl_rollback_tool,
)

logger = structlog.get_logger()


# Tool functions that can be called by the LLM
TOOL_FUNCTIONS = {
    "query_prometheus_range": query_prometheus_range_tool,
    "query_prometheus_instant": query_prometheus_instant_tool,
    "get_k8s_events": get_k8s_events_tool,
    "get_pod_status": get_pod_status_tool,
    "get_deployment_status": get_deployment_status_tool,
    "fetch_git_blame_diff": fetch_git_blame_diff_tool,
    "analyze_stack_trace": analyze_stack_trace_tool,
    "query_clickhouse_logs": query_clickhouse_logs_tool,
    "query_loki_logs": query_loki_logs_tool,
    "get_service_dependencies": get_service_dependencies_tool,
    "simulate_remediation": simulate_remediation_tool,
    "generate_git_patch": generate_git_patch_tool,
    "create_kubectl_rollback": create_kubectl_rollback_tool,
    # New tools for memory and topology
    "episodic_memory_search": lambda **kwargs: {"episodes": []},  # Will be overridden
    "episodic_memory_synthesize": lambda **kwargs: {"playbook": None},  # Will be overridden
    "topology_blast_radius": lambda **kwargs: {},  # Will be overridden
    "topology_deployment_impact": lambda **kwargs: {},  # Will be overridden
    "topology_ownership": lambda **kwargs: {},  # Will be overridden
    "topology_oncall": lambda **kwargs: {},  # Will be overridden
    "score_remediation_risk": lambda **kwargs: {"overall_score": 0.5, "risk_level": "medium"},  # Will be overridden
}


# System prompts for each state
SYSTEM_PROMPTS = {
    AgentState.DETECT: """You are the DETECT phase of an autonomous remediation agent.

You have received an incident from the anomaly watchdog. Your job is to:
1. Acknowledge the incident
2. Do initial triage using available tools
3. Search episodic memory for similar past incidents
4. Determine if this needs full correlation analysis

Available tools: query_prometheus_instant, get_k8s_events, get_pod_status, get_deployment_status, episodic_memory_search

Be concise. Output a brief assessment and whether to proceed to CORRELATE.""",

    AgentState.CORRELATE: """You are the CORRELATE phase of an autonomous remediation agent.

Your job is to correlate the incident with other signals to understand blast radius:
1. Check for related incidents in other services
2. Analyze service dependencies (upstream/downstream) via topology graph
3. Look for common root causes (recent deployments, config changes)
4. Identify the blast radius
5. Dispatch specialist subagents for parallel investigation

Available tools: query_prometheus_range, query_loki_logs, get_service_dependencies, fetch_git_blame_diff, query_clickhouse_logs, topology_blast_radius

Output a CorrelationResult with: correlated_incidents, blast_radius, root_cause_hypothesis, confidence, evidence, subagent_tasks.""",

    AgentState.ISOLATE: """You are the ISOLATE phase of an autonomous remediation agent.

Your job is to isolate the root cause using subagent findings:
1. Review findings from specialist subagents (kubernetes, metrics, logs, traces, git, memory, topology)
2. Analyze stack traces if available
3. Check recent git commits for the affected service
4. Examine pod/container state for OOM, crashes, resource limits
5. Determine root cause type: code, config, infrastructure, dependency, capacity
6. Synthesize strategy playbook from episodic memory if applicable

Available tools: analyze_stack_trace, fetch_git_blame_diff, get_pod_status, get_k8s_events, query_prometheus_range, episodic_memory_synthesize

Output an IsolationResult with: root_cause, root_cause_type, affected_components, contributing_factors, confidence, git_commits, strategy_playbook.""",

    AgentState.VERIFY: """You are the VERIFY phase of an autonomous remediation agent.

Your job is to verify a proposed remediation through dry-run simulation:
1. Propose a specific remediation action based on isolation results
2. Simulate the action to assess risk and impact
3. Generate a rollback plan
4. Assess risk using risk scoring rubric
5. Assign risk score and confidence

Available tools: simulate_remediation, create_kubectl_rollback, generate_git_patch, score_remediation_risk

Output a VerificationResult with: proposed_action, action_type, risk_score, confidence, dry_run_result, rollback_plan, requires_approval.""",

    AgentState.HUMAN_IN_LOOP: """You are the HUMAN_IN_LOOP phase of an autonomous remediation agent.

Present the verification results to a human operator for approval.
Summarize: incident, root cause, proposed action, risk score, confidence, rollback plan, strategy playbook.
Request explicit approval or rejection.""",

    AgentState.REMEDIATE: """You are the REMEDIATE phase of an autonomous remediation agent.

Generate the final remediation artifacts:
1. Git patch for code/config changes
2. kubectl commands for infrastructure changes
3. PR description for human review
4. Complete RemediationPlan
5. Store episode in episodic memory for future learning

Available tools: generate_git_patch, create_kubectl_rollback

Output a complete RemediationPlan.""",
}


class EnhancedRemediationAgent:
    """Enhanced LangGraph-based autonomous remediation agent with all advanced features"""
    
    def __init__(
        self,
        model_name: str = "gpt-4-turbo-preview",
        temperature: float = 0.1,
        use_neo4j: bool = True,
        neo4j_uri: str = "bolt://localhost:7687",
        neo4j_user: str = "neo4j",
        neo4j_password: str = "password",
        skills_dir: str = "skills",
        max_concurrent_subagents: int = 5,
    ):
        self.llm = ChatOpenAI(model=model_name, temperature=temperature)
        self.tools = list(TOOL_FUNCTIONS.values())
        self.tool_node = ToolNode(self.tools)
        
        # Initialize advanced components
        self.episodic_memory = create_episodic_memory(
            use_neo4j=use_neo4j,
            uri=neo4j_uri,
            user=neo4j_user,
            password=neo4j_password,
        )
        
        self.topology_graph = create_topology_graph(
            use_neo4j=use_neo4j,
            uri=neo4j_uri,
            user=neo4j_user,
            password=neo4j_password,
        )
        
        self.skill_registry = SkillRegistry(skills_dir)
        # Add built-in skills
        for skill in create_builtin_skills():
            self.skill_registry.skills[skill.id] = skill
            self.skill_registry.enabled_skills.add(skill.id)
        
        self.subagent_orchestrator = create_subagent_orchestrator(
            tool_executor=self._execute_tool,
            max_concurrent=max_concurrent_subagents,
        )
        
        self.risk_scorer = RiskScorer()
        
        # Override tool functions with actual implementations
        self._override_tool_functions()
        
        self.graph = self._build_graph()
    
    def _override_tool_functions(self):
        """Override tool functions with actual implementations"""
        global TOOL_FUNCTIONS
        TOOL_FUNCTIONS["episodic_memory_search"] = self._episodic_memory_search
        TOOL_FUNCTIONS["episodic_memory_synthesize"] = self._episodic_memory_synthesize
        TOOL_FUNCTIONS["topology_blast_radius"] = self._topology_blast_radius
        TOOL_FUNCTIONS["topology_deployment_impact"] = self._topology_deployment_impact
        TOOL_FUNCTIONS["topology_ownership"] = self._topology_ownership
        TOOL_FUNCTIONS["topology_oncall"] = self._topology_oncall
        TOOL_FUNCTIONS["score_remediation_risk"] = self._score_remediation_risk
    
    def _execute_tool(self, tool_name: str, params: Dict[str, Any]) -> Any:
        """Execute a tool by name"""
        tool_func = TOOL_FUNCTIONS.get(tool_name)
        if not tool_func:
            raise ValueError(f"Unknown tool: {tool_name}")
        
        if asyncio.iscoroutinefunction(tool_func):
            return asyncio.run(tool_func(**params))
        return tool_func(**params)
    
    # Tool implementations
    def _episodic_memory_search(self, query_text: str, limit: int = 5, min_score: float = 0.7, 
                                issue_type: str = None, components: List[str] = None) -> Dict[str, Any]:
        episodes = self.episodic_memory.search_similar(
            query_text=query_text,
            limit=limit,
            min_score=min_score,
            issue_type=issue_type,
            components=components,
        )
        return {
            "episodes": [
                {
                    "correlation_id": e.correlation_id,
                    "title": e.title,
                    "summary": e.summary,
                    "issue_type": e.issue_type,
                    "root_cause": e.root_cause,
                    "remediation_actions": e.remediation_actions,
                    "effectiveness_score": e.effectiveness_score,
                    "status": e.status.value,
                }
                for e in episodes
            ]
        }
    
    def _episodic_memory_synthesize(self, issue_type: str, min_episodes: int = 3) -> Dict[str, Any]:
        playbook = self.episodic_memory.synthesize_strategy_playbook(issue_type, min_episodes)
        return {"playbook": playbook}
    
    def _topology_blast_radius(self, service_id: str, max_hops: int = 3) -> Dict[str, Any]:
        return self.topology_graph.get_blast_radius(service_id, max_hops)
    
    def _topology_deployment_impact(self, deployment_id: str) -> Dict[str, Any]:
        return self.topology_graph.get_deployment_impact(deployment_id)
    
    def _topology_ownership(self, service_id: str) -> Dict[str, Any]:
        return self.topology_graph.get_service_ownership(service_id) or {}
    
    def _topology_oncall(self, service_id: str) -> Dict[str, Any]:
        return self.topology_graph.get_oncall_for_service(service_id) or {}
    
    def _score_remediation_risk(self, action_type: str, target: str, namespace: str, 
                                context: Dict[str, Any] = None) -> Dict[str, Any]:
        assessment = self.risk_scorer.assess(ActionType(action_type), {
            "target": target,
            "namespace": namespace,
            **(context or {}),
        })
        return {
            "overall_score": assessment.overall_score,
            "risk_level": assessment.risk_level.value,
            "factors": [f.model_dump() for f in assessment.factors],
            "mitigation": assessment.mitigation,
            "requires_approval": assessment.requires_approval,
        }
    
    def _build_graph(self) -> StateGraph:
        """Build the enhanced LangGraph state machine"""
        
        workflow = StateGraph(AgentContext)
        
        # Add nodes for each state
        workflow.add_node("detect", self._detect_node)
        workflow.add_node("correlate", self._correlate_node)
        workflow.add_node("dispatch_subagents", self._dispatch_subagents_node)
        workflow.add_node("wait_subagents", self._wait_subagents_node)
        workflow.add_node("isolate", self._isolate_node)
        workflow.add_node("verify", self._verify_node)
        workflow.add_node("human_in_loop", self._human_in_loop_node)
        workflow.add_node("remediate", self._remediate_node)
        workflow.add_node("store_episode", self._store_episode_node)
        workflow.add_node("tool_executor", self.tool_node)
        
        # Define edges
        workflow.set_entry_point("detect")
        
        workflow.add_conditional_edges(
            "detect",
            self._should_continue,
            {
                "correlate": "correlate",
                "end": END,
            }
        )
        
        workflow.add_edge("correlate", "dispatch_subagents")
        workflow.add_edge("dispatch_subagents", "wait_subagents")
        workflow.add_edge("wait_subagents", "isolate")
        
        workflow.add_conditional_edges(
            "isolate",
            self._should_continue,
            {
                "verify": "verify",
                "end": END,
            }
        )
        
        workflow.add_conditional_edges(
            "verify",
            self._should_continue,
            {
                "human_in_loop": "human_in_loop",
                "end": END,
            }
        )
        
        workflow.add_conditional_edges(
            "human_in_loop",
            self._human_decision,
            {
                "remediate": "remediate",
                "end": END,
            }
        )
        
        workflow.add_edge("remediate", "store_episode")
        workflow.add_edge("store_episode", END)
        
        return workflow.compile()
    
    def _detect_node(self, state: AgentContext) -> AgentContext:
        """DETECT phase: Initial triage + memory search"""
        logger.info("DETECT phase", incident_id=state.incident.id)
        
        incident = state.incident
        
        # Search episodic memory for similar incidents
        query_text = f"{incident.anomaly_type.value} in {incident.service} severity:{incident.severity.value}"
        memory_results = self.episodic_memory.search_similar(
            query_text=query_text,
            limit=3,
            min_score=0.6,
            components=[incident.service],
        )
        
        # Quick initial queries
        system_prompt = SYSTEM_PROMPTS[AgentState.DETECT]
        user_prompt = f"""Incident received:
- ID: {incident.id}
- Service: {incident.service}
- Anomaly: {incident.anomaly_type}
- Severity: {incident.severity}
- Metrics: {incident.metrics}
- Confidence: {incident.confidence}

Similar past incidents found: {len(memory_results)}
{self._format_memory_results(memory_results)}

Perform initial triage. Check pod status, recent events, and current metrics.
Should we proceed to full correlation analysis?"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        detect_tools = [
            query_prometheus_instant_tool,
            get_k8s_events_tool,
            get_pod_status_tool,
            get_deployment_status_tool,
        ]
        llm_with_tools = self.llm.bind_tools(detect_tools)
        
        response = llm_with_tools.invoke(messages)
        
        if response.tool_calls:
            tool_results = []
            for tool_call in response.tool_calls:
                tool_func = TOOL_FUNCTIONS.get(tool_call["name"])
                if tool_func:
                    try:
                        result = tool_func(**tool_call["args"])
                        tool_results.append(ToolMessage(
                            content=json.dumps(result),
                            tool_call_id=tool_call["id"]
                        ))
                    except Exception as e:
                        tool_results.append(ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call["id"]
                        ))
            
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            assessment = final_response.content
        else:
            assessment = response.content
        
        # Store memory results in context
        state.metadata = state.metadata or {}
        state.metadata["similar_episodes"] = [
            {"id": e.correlation_id, "title": e.title, "root_cause": e.root_cause}
            for e in memory_results
        ]
        
        proceed = "proceed" in assessment.lower() or "correlate" in assessment.lower() or incident.severity.value in ["critical", "emergency"]
        
        state.state = AgentState.CORRELATE if proceed else AgentState.COMPLETE
        state.error = None if proceed else "Low severity, no further action needed"
        
        logger.info("DETECT complete", proceed=proceed, similar_episodes=len(memory_results))
        return state
    
    def _correlate_node(self, state: AgentContext) -> AgentContext:
        """CORRELATE phase: Find related signals + blast radius"""
        logger.info("CORRELATE phase", incident_id=state.incident.id)
        
        # Get blast radius from topology graph
        blast_radius = self.topology_graph.get_blast_radius(state.incident.service)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.CORRELATE]
        user_prompt = f"""Correlate incident {state.incident.id} ({state.incident.service}) with other signals.

Incident details:
- Type: {state.incident.anomaly_type}
- Severity: {state.incident.severity}
- Metrics: {state.incident.metrics}
- Context: {state.incident.context}

Blast radius from topology:
- Upstream services: {len(blast_radius.get('upstream', []))}
- Downstream services: {len(blast_radius.get('downstream', []))}
- Shared databases: {len(blast_radius.get('shared_databases', []))}
- Total affected: {blast_radius.get('total_affected_services', 0)}

Similar episodes from memory: {state.metadata.get('similar_episodes', []) if state.metadata else []}

Tasks:
1. Query Prometheus for related metrics across services in blast radius
2. Check Loki for related log patterns
3. Get service dependencies (upstream/downstream)
4. Check recent git commits for affected services
5. Identify blast radius
6. Plan subagent dispatch for parallel investigation

Output a structured correlation result with subagent_tasks list."""
        
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        
        correlate_tools = [
            query_prometheus_range_tool,
            query_loki_logs_tool,
            get_service_dependencies_tool,
            fetch_git_blame_diff_tool,
            query_clickhouse_logs_tool,
        ]
        llm_with_tools = self.llm.bind_tools(correlate_tools)
        
        response = llm_with_tools.invoke(messages)
        
        if response.tool_calls:
            tool_results = []
            for tool_call in response.tool_calls:
                tool_func = TOOL_FUNCTIONS.get(tool_call["name"])
                if tool_func:
                    try:
                        result = tool_func(**tool_call["args"])
                        tool_results.append(ToolMessage(
                            content=json.dumps(result),
                            tool_call_id=tool_call["id"]
                        ))
                    except Exception as e:
                        tool_results.append(ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call["id"]
                        ))
            
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            
            # Create subagent tasks based on incident characteristics
            subagent_tasks = self.subagent_orchestrator.create_investigation_plan({
                "service": state.incident.service,
                "namespace": state.incident.context.get("namespace", "default"),
                "anomaly_type": state.incident.anomaly_type.value,
            })
            
            state.correlation = CorrelationResult(
                incident_id=state.incident.id,
                correlated_incidents=[],
                blast_radius=[s["service_id"] for s in blast_radius.get("downstream", [])],
                root_cause_hypothesis=final_response.content[:500],
                confidence=0.7,
                evidence=["Tool-assisted correlation analysis", f"Topology blast radius: {blast_radius.get('total_affected_services', 0)} services"],
            )
            
            # Store subagent tasks in metadata
            state.metadata = state.metadata or {}
            state.metadata["subagent_tasks"] = [
                {"type": t.type.value, "description": t.description, "parameters": t.parameters}
                for t in subagent_tasks
            ]
        else:
            state.correlation = CorrelationResult(
                incident_id=state.incident.id,
                correlated_incidents=[],
                blast_radius=[],
                root_cause_hypothesis=response.content,
                confidence=0.5,
                evidence=["LLM analysis only"]
            )
            state.metadata = state.metadata or {}
            state.metadata["subagent_tasks"] = []
        
        state.state = AgentState.ISOLATE  # Will go to dispatch_subagents next
        logger.info("CORRELATE complete", hypothesis=state.correlation.root_cause_hypothesis[:100])
        return state
    
    def _dispatch_subagents_node(self, state: AgentContext) -> AgentContext:
        """Dispatch subagents for parallel investigation"""
        logger.info("DISPATCH_SUBAGENTS phase", incident_id=state.incident.id)
        
        subagent_tasks_data = state.metadata.get("subagent_tasks", []) if state.metadata else []
        
        if not subagent_tasks_data:
            state.metadata = state.metadata or {}
            state.metadata["subagent_results"] = []
            return state
        
        # Create SubagentTask objects
        tasks = []
        for task_data in subagent_tasks_data:
            task = SubagentTask(
                type=SubagentType(task_data["type"]),
                description=task_data["description"],
                parameters=task_data["parameters"],
                priority=1,
            )
            tasks.append(task)
        
        # Execute subagents in parallel (this will be async in the graph)
        # Store tasks for the wait node
        state.metadata["dispatched_subagent_tasks"] = [
            {"id": t.id, "type": t.type.value, "description": t.description}
            for t in tasks
        ]
        
        # Run subagents
        async def run_subagents():
            return await self.subagent_orchestrator.execute_parallel(tasks)
        
        results = asyncio.run(run_subagents())
        
        # Store results
        state.metadata["subagent_results"] = [
            {
                "task_id": r.task_id,
                "subagent_type": r.subagent_type.value,
                "success": r.success,
                "findings": r.findings,
                "evidence": r.evidence,
                "recommendations": r.recommendations,
                "confidence": r.confidence,
                "execution_time_seconds": r.execution_time_seconds,
                "error": r.error,
            }
            for r in results
        ]
        
        logger.info("Subagents completed", 
                   total=len(results),
                   successful=sum(1 for r in results if r.success))
        
        return state
    
    def _wait_subagents_node(self, state: AgentContext) -> AgentContext:
        """Wait for subagents to complete (already done in dispatch for sync version)"""
        # In async version, this would be a separate node that polls
        # For sync, results are already stored
        return state
    
    def _isolate_node(self, state: AgentContext) -> AgentContext:
        """ISOLATE phase: Pinpoint root cause using subagent findings"""
        logger.info("ISOLATE phase", incident_id=state.incident.id)
        
        # Collect subagent findings
        subagent_results = state.metadata.get("subagent_results", []) if state.metadata else []
        
        all_findings = []
        all_evidence = {}
        all_recommendations = []
        
        for result in subagent_results:
            if result.get("success"):
                all_findings.extend(result.get("findings", []))
                all_evidence[result.get("subagent_type", "unknown")] = result.get("evidence", {})
                all_recommendations.extend(result.get("recommendations", []))
        
        # Synthesize strategy playbook from memory if enough similar episodes
        similar_episodes = state.metadata.get("similar_episodes", []) if state.metadata else []
        strategy_playbook = None
        if len(similar_episodes) >= 3:
            issue_type = state.incident.anomaly_type.value
            playbook_result = self.episodic_memory.synthesize_strategy_playbook(issue_type, min_episodes=3)
            if playbook_result:
                strategy_playbook = playbook_result
        
        system_prompt = SYSTEM_PROMPTS[AgentState.ISOLATE]
        user_prompt = f"""Isolate root cause for incident {state.incident.id}.

Subagent findings:
{self._format_subagent_findings(subagent_results)}

Strategy playbook from memory:
{strategy_playbook or "Not available (need 3+ similar resolved incidents)"}

Correlation hypothesis: {state.correlation.root_cause_hypothesis if state.correlation else 'None'}
Blast radius: {state.correlation.blast_radius if state.correlation else []}
Service: {state.incident.service}

Tasks:
1. Analyze stack traces if available
2. Check recent git commits for the service
3. Examine pod/container state for OOM, crashes, resource limits
4. Determine root cause type: code, config, infrastructure, dependency, capacity
5. Synthesize all evidence into root cause

Output structured isolation result."""
        
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        
        isolate_tools = [
            analyze_stack_trace_tool,
            fetch_git_blame_diff_tool,
            get_pod_status_tool,
            get_k8s_events_tool,
            query_prometheus_range_tool,
        ]
        llm_with_tools = self.llm.bind_tools(isolate_tools)
        
        response = llm_with_tools.invoke(messages)
        
        if response.tool_calls:
            tool_results = []
            for tool_call in response.tool_calls:
                tool_func = TOOL_FUNCTIONS.get(tool_call["name"])
                if tool_func:
                    try:
                        result = tool_func(**tool_call["args"])
                        tool_results.append(ToolMessage(
                            content=json.dumps(result),
                            tool_call_id=tool_call["id"]
                        ))
                    except Exception as e:
                        tool_results.append(ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call["id"]
                        ))
            
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            
            state.isolation = IsolationResult(
                incident_id=state.incident.id,
                root_cause=final_response.content[:500],
                root_cause_type="code",  # Would be parsed from response
                affected_components=[state.incident.service],
                contributing_factors=all_findings[:10],
                confidence=0.8,
                git_commits=[],
            )
        else:
            state.isolation = IsolationResult(
                incident_id=state.incident.id,
                root_cause=response.content,
                root_cause_type="unknown",
                affected_components=[state.incident.service],
                contributing_factors=all_findings[:10],
                confidence=0.6,
                git_commits=[],
            )
        
        # Store strategy playbook
        if strategy_playbook:
            state.metadata = state.metadata or {}
            state.metadata["strategy_playbook"] = strategy_playbook
        
        state.state = AgentState.VERIFY
        logger.info("ISOLATE complete", root_cause=state.isolation.root_cause[:100])
        return state
    
    def _verify_node(self, state: AgentContext) -> AgentContext:
        """VERIFY phase: Dry-run remediation with risk scoring"""
        logger.info("VERIFY phase", incident_id=state.incident.id)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.VERIFY]
        user_prompt = f"""Verify remediation for incident {state.incident.id}.

Root cause: {state.isolation.root_cause if state.isolation else 'Unknown'}
Root cause type: {state.isolation.root_cause_type if state.isolation else 'Unknown'}
Service: {state.incident.service}
Namespace: {state.incident.context.get('namespace', 'default')}

Strategy playbook:
{state.metadata.get('strategy_playbook', 'Not available') if state.metadata else 'Not available'}

Propose a specific remediation action and simulate it.
Action types: rollback, config_change, scale, restart, circuit_breaker, traffic_shift

Output verification result with risk score and confidence."""
        
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        
        verify_tools = [
            simulate_remediation_tool,
            create_kubectl_rollback_tool,
            generate_git_patch_tool,
            score_remediation_risk,
        ]
        llm_with_tools = self.llm.bind_tools(verify_tools)
        
        response = llm_with_tools.invoke(messages)
        
        if response.tool_calls:
            tool_results = []
            for tool_call in response.tool_calls:
                tool_func = TOOL_FUNCTIONS.get(tool_call["name"])
                if tool_func:
                    try:
                        result = tool_func(**tool_call["args"])
                        tool_results.append(ToolMessage(
                            content=json.dumps(result),
                            tool_call_id=tool_call["id"]
                        ))
                    except Exception as e:
                        tool_results.append(ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call["id"]
                        ))
            
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            
            state.verification = VerificationResult(
                incident_id=state.incident.id,
                proposed_action=final_response.content[:500],
                action_type="rollback",  # Would be parsed
                risk_score=0.3,
                confidence=0.8,
                dry_run_result={"simulated": True},
                rollback_plan="Auto-generated rollback available",
                requires_approval=True
            )
        else:
            state.verification = VerificationResult(
                incident_id=state.incident.id,
                proposed_action=response.content,
                action_type="unknown",
                risk_score=0.5,
                confidence=0.5,
                requires_approval=True
            )
        
        state.state = AgentState.HUMAN_IN_LOOP
        logger.info("VERIFY complete", action=state.verification.proposed_action[:100], risk=state.verification.risk_score)
        return state
    
    def _human_in_loop_node(self, state: AgentContext) -> AgentContext:
        """HUMAN_IN_LOOP phase: Request approval"""
        logger.info("HUMAN_IN_LOOP phase", incident_id=state.incident.id)
        
        risk = state.verification.risk_score if state.verification else 1.0
        confidence = state.verification.confidence if state.verification else 0.0
        
        auto_approve = risk < 0.4 and confidence > 0.7
        
        if auto_approve:
            state.state = AgentState.REMEDIATE
            logger.info("Auto-approved for remediation", risk=risk, confidence=confidence)
        else:
            state.state = AgentState.HUMAN_IN_LOOP
            state.error = "Requires human approval - risk too high or confidence too low"
            logger.info("Requires human approval", risk=risk, confidence=confidence)
        
        return state
    
    def _remediate_node(self, state: AgentContext) -> AgentContext:
        """REMEDIATE phase: Generate final artifacts"""
        logger.info("REMEDIATE phase", incident_id=state.incident.id)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.REMEDIATE]
        user_prompt = f"""Generate final remediation plan for incident {state.incident.id}.

Root cause: {state.isolation.root_cause if state.isolation else 'Unknown'}
Proposed action: {state.verification.proposed_action if state.verification else 'Unknown'}
Action type: {state.verification.action_type if state.verification else 'Unknown'}
Risk score: {state.verification.risk_score if state.verification else 'Unknown'}

Strategy playbook:
{state.metadata.get('strategy_playbook', 'Not available') if state.metadata else 'Not available'}

Generate:
1. Git patch if code/config change needed
2. kubectl commands for infrastructure changes
3. PR description for review
4. Complete remediation plan"""
        
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        
        remediate_tools = [
            generate_git_patch_tool,
            create_kubectl_rollback_tool,
        ]
        llm_with_tools = self.llm.bind_tools(remediate_tools)
        
        response = llm_with_tools.invoke(messages)
        
        if response.tool_calls:
            tool_results = []
            for tool_call in response.tool_calls:
                tool_func = TOOL_FUNCTIONS.get(tool_call["name"])
                if tool_func:
                    try:
                        result = tool_func(**tool_call["args"])
                        tool_results.append(ToolMessage(
                            content=json.dumps(result),
                            tool_call_id=tool_call["id"]
                        ))
                    except Exception as e:
                        tool_results.append(ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call["id"]
                        ))
            
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            
            git_patch = None
            kubectl_commands = []
            
            for tool_result in tool_results:
                try:
                    result_data = json.loads(tool_result.content)
                    if "patch" in result_data:
                        git_patch = result_data["patch"]
                    if "rollback_command" in result_data:
                        kubectl_commands.append(result_data["rollback_command"])
                        kubectl_commands.extend(result_data.get("verification_commands", []))
                except Exception:
                    pass
            
            state.remediation = RemediationPlan(
                incident_id=state.incident.id,
                summary=final_response.content[:500],
                actions=[{"type": state.verification.action_type if state.verification else "unknown", "description": state.verification.proposed_action if state.verification else "unknown"}],
                risk_score=state.verification.risk_score if state.verification else 0.5,
                confidence=state.verification.confidence if state.verification else 0.5,
                git_patch=git_patch,
                kubectl_commands=kubectl_commands,
                pr_description=f"Automated remediation for incident {state.incident.id}\n\n{final_response.content}"
            )
        else:
            state.remediation = RemediationPlan(
                incident_id=state.incident.id,
                summary=response.content,
                actions=[],
                risk_score=0.5,
                confidence=0.5,
            )
        
        state.state = AgentState.COMPLETE
        logger.info("REMEDIATE complete", plan_id=state.remediation.plan_id if state.remediation else None)
        return state
    
    def _store_episode_node(self, state: AgentContext) -> AgentContext:
        """Store the completed investigation as an episode in memory"""
        logger.info("STORE_EPISODE phase", incident_id=state.incident.id)
        
        if not state.remediation:
            return state
        
        # Determine effectiveness (would be updated later after remediation applied)
        effectiveness = 0.5  # Default unknown
        
        episode = Episode(
            correlation_id=state.incident.id,
            title=f"{state.incident.anomaly_type.value} in {state.incident.service}",
            summary=state.remediation.summary,
            issue_type=state.incident.anomaly_type.value,
            affected_components=[state.incident.service] + state.correlation.blast_radius if state.correlation else [state.incident.service],
            root_cause=state.isolation.root_cause if state.isolation else "Unknown",
            evidence=state.isolation.contributing_factors if state.isolation else [],
            skills_used=[r.get("subagent_type") for r in state.metadata.get("subagent_results", []) if r.get("success")] if state.metadata else [],
            remediation_actions=[
                            state.verification.action_type if state.verification else "unknown",
                            *(state.remediation.kubectl_commands if state.remediation else []),
                        ],
            effectiveness_score=effectiveness,
            status=EpisodeStatus.RESOLVED if state.state == AgentState.COMPLETE else EpisodeStatus.IN_PROGRESS,
            metadata={
                "anomaly_type": state.incident.anomaly_type.value,
                "severity": state.incident.severity.value,
                "risk_score": state.verification.risk_score if state.verification else 0.5,
                "confidence": state.verification.confidence if state.verification else 0.5,
                "strategy_playbook": state.metadata.get("strategy_playbook") if state.metadata else None,
            },
        )
        
        try:
            self.episodic_memory.store_episode(episode)
            logger.info("Episode stored", correlation_id=episode.correlation_id)
        except Exception as e:
            logger.error("Failed to store episode", error=str(e))
        
        return state
    
    def _should_continue(self, state: AgentContext) -> Literal["continue", "end"]:
        if state.state == AgentState.FAILED or state.error:
            return "end"
        return "continue"
    
    def _human_decision(self, state: AgentContext) -> Literal["remediate", "end"]:
        if state.state == AgentState.REMEDIATE:
            return "remediate"
        return "end"
    
    def _format_memory_results(self, episodes: List) -> str:
        if not episodes:
            return "None"
        lines = []
        for ep in episodes:
            lines.append(f"- {ep.title}: {ep.root_cause[:100]} (effectiveness: {ep.effectiveness_score:.0%})")
        return "\n".join(lines)
    
    def _format_subagent_findings(self, results: List[Dict]) -> str:
        if not results:
            return "No subagent results"
        lines = []
        for r in results:
            status = "✓" if r.get("success") else "✗"
            lines.append(f"{status} {r.get('subagent_type', 'unknown')}: {len(r.get('findings', []))} findings, confidence: {r.get('confidence', 0):.0%}")
            for finding in r.get("findings", [])[:3]:
                lines.append(f"  - {finding}")
        return "\n".join(lines)
    
    async def run(self, incident: Incident) -> AgentContext:
        """Run the full remediation workflow"""
        initial_state = AgentContext(incident=incident)
        result = await self.graph.ainvoke(initial_state)
        return result


def run_remediation(incident: Incident, model_name: str = "gpt-4-turbo-preview", **kwargs) -> AgentContext:
    """Synchronous entry point"""
    agent = EnhancedRemediationAgent(model_name=model_name, **kwargs)
    import asyncio
    return asyncio.run(agent.run(incident))