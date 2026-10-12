"""
LangGraph State Machine for Autonomous Remediation Agent

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

from ..state import AgentContext, AgentState, Incident, CorrelationResult, IsolationResult, VerificationResult, RemediationPlan
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


# Tool wrapper for LangGraph
class RemediationTool(BaseTool):
    """Base class for remediation tools"""
    name: str
    description: str
    args_schema: type[BaseModel]
    
    def _run(self, **kwargs) -> str:
        raise NotImplementedError
    
    async def _arun(self, **kwargs) -> str:
        raise NotImplementedError


# Define tool functions that can be called by the LLM
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
}


# System prompts for each state
SYSTEM_PROMPTS = {
    AgentState.DETECT: """You are the DETECT phase of an autonomous remediation agent.
    
You have received an incident from the anomaly watchdog. Your job is to:
1. Acknowledge the incident
2. Do initial triage using available tools
3. Determine if this needs full correlation analysis

Available tools: query_prometheus_instant, get_k8s_events, get_pod_status, get_deployment_status

Be concise. Output a brief assessment and whether to proceed to CORRELATE.""",

    AgentState.CORRELATE: """You are the CORRELATE phase of an autonomous remediation agent.
    
Your job is to correlate the incident with other signals to understand blast radius:
1. Check for related incidents in other services
2. Analyze service dependencies (upstream/downstream)
3. Look for common root causes (recent deployments, config changes)
4. Identify the blast radius

Available tools: query_prometheus_range, query_loki_logs, get_service_dependencies, fetch_git_blame_diff, query_clickhouse_logs

Output a CorrelationResult with: correlated_incidents, blast_radius, root_cause_hypothesis, confidence, evidence.""",

    AgentState.ISOLATE: """You are the ISOLATE phase of an autonomous remediation agent.
    
Your job is to isolate the root cause:
1. Analyze stack traces if available
2. Check recent git commits for the affected service
3. Examine pod/container state for OOM, crashes, resource limits
4. Determine root cause type: code, config, infrastructure, dependency, capacity

Available tools: analyze_stack_trace, fetch_git_blame_diff, get_pod_status, get_k8s_events, query_prometheus_range

Output an IsolationResult with: root_cause, root_cause_type, affected_components, contributing_factors, confidence, git_commits.""",

    AgentState.VERIFY: """You are the VERIFY phase of an autonomous remediation agent.
    
Your job is to verify a proposed remediation through dry-run simulation:
1. Propose a specific remediation action based on isolation results
2. Simulate the action to assess risk and impact
3. Generate a rollback plan
4. Assign risk score and confidence

Available tools: simulate_remediation, create_kubectl_rollback, generate_git_patch

Output a VerificationResult with: proposed_action, action_type, risk_score, confidence, dry_run_result, rollback_plan, requires_approval.""",

    AgentState.HUMAN_IN_LOOP: """You are the HUMAN_IN_LOOP phase of an autonomous remediation agent.
    
Present the verification results to a human operator for approval.
Summarize: incident, root cause, proposed action, risk score, confidence, rollback plan.
Request explicit approval or rejection.""",

    AgentState.REMEDIATE: """You are the REMEDIATE phase of an autonomous remediation agent.
    
Generate the final remediation artifacts:
1. Git patch for code/config changes
2. kubectl commands for infrastructure changes
3. PR description for human review
4. Complete RemediationPlan

Available tools: generate_git_patch, create_kubectl_rollback

Output a complete RemediationPlan.""",
}


class RemediationAgent:
    """LangGraph-based autonomous remediation agent"""
    
    def __init__(self, model_name: str = "gpt-4-turbo-preview", temperature: float = 0.1):
        self.llm = ChatOpenAI(model=model_name, temperature=temperature)
        self.tools = list(TOOL_FUNCTIONS.values())
        self.tool_node = ToolNode(self.tools)
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph state machine"""
        
        # Define the state graph
        workflow = StateGraph(AgentContext)
        
        # Add nodes for each state
        workflow.add_node("detect", self._detect_node)
        workflow.add_node("correlate", self._correlate_node)
        workflow.add_node("isolate", self._isolate_node)
        workflow.add_node("verify", self._verify_node)
        workflow.add_node("human_in_loop", self._human_in_loop_node)
        workflow.add_node("remediate", self._remediate_node)
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
        
        workflow.add_conditional_edges(
            "correlate",
            self._should_continue,
            {
                "isolate": "isolate",
                "end": END,
            }
        )
        
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
        
        workflow.add_edge("remediate", END)
        
        return workflow.compile()
    
    def _detect_node(self, state: AgentContext) -> AgentContext:
        """DETECT phase: Initial triage"""
        logger.info("DETECT phase", incident_id=state.incident.id)
        
        incident = state.incident
        
        # Quick initial queries
        system_prompt = SYSTEM_PROMPTS[AgentState.DETECT]
        user_prompt = f"""Incident received:
- ID: {incident.id}
- Service: {incident.service}
- Anomaly: {incident.anomaly_type}
- Severity: {incident.severity}
- Metrics: {incident.metrics}
- Confidence: {incident.confidence}

Perform initial triage. Check pod status, recent events, and current metrics.
Should we proceed to full correlation analysis?"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        # Bind tools for this phase
        detect_tools = [
            query_prometheus_instant_tool,
            get_k8s_events_tool,
            get_pod_status_tool,
            get_deployment_status_tool,
        ]
        llm_with_tools = self.llm.bind_tools(detect_tools)
        
        response = llm_with_tools.invoke(messages)
        
        # Check if tool calls were made
        if response.tool_calls:
            # Execute tools
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
            
            # Get final assessment
            messages.extend([response] + tool_results)
            final_response = llm_with_tools.invoke(messages)
            assessment = final_response.content
        else:
            assessment = response.content
        
        # Decide whether to continue
        proceed = "proceed" in assessment.lower() or "correlate" in assessment.lower() or incident.severity in [Severity.CRITICAL, Severity.EMERGENCY]
        
        state.state = AgentState.CORRELATE if proceed else AgentState.COMPLETE
        state.error = None if proceed else "Low severity, no further action needed"
        
        logger.info("DETECT complete", proceed=proceed, assessment=assessment[:200])
        return state
    
    def _correlate_node(self, state: AgentContext) -> AgentContext:
        """CORRELATE phase: Find related signals"""
        logger.info("CORRELATE phase", incident_id=state.incident.id)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.CORRELATE]
        user_prompt = f"""Correlate incident {state.incident.id} ({state.incident.service}) with other signals.

Incident details:
- Type: {state.incident.anomaly_type}
- Severity: {state.incident.severity}
- Metrics: {state.incident.metrics}
- Context: {state.incident.context}

Tasks:
1. Query Prometheus for related metrics across services
2. Check Loki for related log patterns
3. Get service dependencies (upstream/downstream)
4. Check recent git commits for affected services
5. Identify blast radius

Output a structured correlation result."""
        
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
            
            # Parse correlation result (in production, use structured output)
            state.correlation = CorrelationResult(
                incident_id=state.incident.id,
                correlated_incidents=[],
                blast_radius=state.incident.context.get("related_services", []),
                root_cause_hypothesis=final_response.content[:500],
                confidence=0.7,
                evidence=["Tool-assisted correlation analysis"]
            )
        else:
            state.correlation = CorrelationResult(
                incident_id=state.incident.id,
                correlated_incidents=[],
                blast_radius=[],
                root_cause_hypothesis=response.content,
                confidence=0.5,
                evidence=["LLM analysis only"]
            )
        
        state.state = AgentState.ISOLATE
        logger.info("CORRELATE complete", hypothesis=state.correlation.root_cause_hypothesis[:100])
        return state
    
    def _isolate_node(self, state: AgentContext) -> AgentContext:
        """ISOLATE phase: Pinpoint root cause"""
        logger.info("ISOLATE phase", incident_id=state.incident.id)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.ISOLATE]
        user_prompt = f"""Isolate root cause for incident {state.incident.id}.

Correlation hypothesis: {state.correlation.root_cause_hypothesis if state.correlation else 'None'}
Blast radius: {state.correlation.blast_radius if state.correlation else []}
Service: {state.incident.service}

Tasks:
1. Get detailed pod status for affected pods
2. Check for stack traces in logs
3. Analyze recent git commits for the service
4. Check Kubernetes events for OOM, crashes, evictions
5. Determine root cause type: code, config, infrastructure, dependency, capacity

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
                contributing_factors=["Recent deployment", "Resource pressure"],
                confidence=0.75,
                git_commits=[]
            )
        else:
            state.isolation = IsolationResult(
                incident_id=state.incident.id,
                root_cause=response.content,
                root_cause_type="unknown",
                affected_components=[state.incident.service],
                contributing_factors=[],
                confidence=0.5,
                git_commits=[]
            )
        
        state.state = AgentState.VERIFY
        logger.info("ISOLATE complete", root_cause=state.isolation.root_cause[:100])
        return state
    
    def _verify_node(self, state: AgentContext) -> AgentContext:
        """VERIFY phase: Dry-run remediation"""
        logger.info("VERIFY phase", incident_id=state.incident.id)
        
        system_prompt = SYSTEM_PROMPTS[AgentState.VERIFY]
        user_prompt = f"""Verify remediation for incident {state.incident.id}.

Root cause: {state.isolation.root_cause if state.isolation else 'Unknown'}
Root cause type: {state.isolation.root_cause_type if state.isolation else 'Unknown'}
Service: {state.incident.service}
Namespace: {state.incident.context.get('namespace', 'default')}

Propose a specific remediation action and simulate it.
Action types: rollback, config_change, scale, restart, circuit_breaker, traffic_shift

Output verification result with risk score and confidence."""
        
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        
        verify_tools = [
            simulate_remediation_tool,
            create_kubectl_rollback_tool,
            generate_git_patch_tool,
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
        
        # In production, this would pause and wait for human approval
        # For now, auto-approve if risk is low
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
            
            # Parse results from tool calls
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
    
    def _should_continue(self, state: AgentContext) -> Literal["continue", "end"]:
        """Determine if workflow should continue"""
        if state.state == AgentState.FAILED or state.error:
            return "end"
        return "continue"
    
    def _human_decision(self, state: AgentContext) -> Literal["remediate", "end"]:
        """Determine human decision"""
        if state.state == AgentState.REMEDIATE:
            return "remediate"
        return "end"
    
    async def run(self, incident: Incident) -> AgentContext:
        """Run the full remediation workflow"""
        initial_state = AgentContext(incident=incident)
        result = await self.graph.ainvoke(initial_state)
        return result


# Synchronous runner for simpler use cases
def run_remediation(incident: Incident, model_name: str = "gpt-4-turbo-preview") -> AgentContext:
    """Synchronous entry point"""
    agent = RemediationAgent(model_name=model_name)
    # In production, use async version
    import asyncio
    return asyncio.run(agent.run(incident))