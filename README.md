# Autonomous Incident Remediation Agent

An automated reliability operator that observes live telemetry, pinpoints root causes, and safely generates remediation runbooks for microservices.

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Microservices  │────▶│  OTel Collector  │────▶│  ClickHouse /       │
│  (Instrumented) │     │  (Receivers,     │     │  Prometheus         │
└─────────────────┘     │  Processors,     │     └──────────┬──────────┘
                        │  Exporters)      │                │
                        └──────────────────┘                ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Remediation    │◀───│  Agentic Root    │◀───│  Anomaly Watchdog   │
│  (PR / Rollback)│     │  Cause Engine    │     │  (Rust/Go Daemon)   │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
                               ▲
                               │
                        ┌──────┴──────┐
                        │  Tool Layer │
                        │ • query_prometheus_range
                        │ • get_k8s_events
                        │ • fetch_git_blame_diff
                        │ • analyze_stack_trace
                        └─────────────┘
                               ▲
                               │
                    ┌──────────┴──────────┐
                    │  Advanced Systems   │
                    │ • Episodic Memory   │
                    │ • Service Topology  │
                    │ • Skills Registry   │
                    │ • Subagent Orch.    │
                    │ • Evaluation FW     │
                    └─────────────────────┘
```

## Components

### 1. Telemetry Ingestion (`/otel-collector`)
OpenTelemetry Collector configuration with:
- OTLP receivers (gRPC/HTTP)
- Processors: batch, memory_limiter, tail_sampling, transform, resourcedetection, spanmetrics
- Exporters: ClickHouse, Prometheus, Loki

### 2. Anomaly Watchdog (`/watchdog`)
Rust daemon that:
- Tails metrics/log streams from Prometheus/Loki
- Computes sliding-window anomalies (Z-score, EWMA, Shannon entropy)
- Emits structured incident payloads to agent

### 3. Agentic Root-Cause Engine (`/agent`)
Python backend with **Enhanced LangGraph State Machine**:
- **6-State Workflow**: DETECT → CORRELATE → DISPATCH_SUBAGENTS → WAIT_SUBAGENTS → ISOLATE → VERIFY → HUMAN_IN_LOOP → REMEDIATE → STORE_EPISODE
- **Specialist Subagents**: Parallel investigation (Kubernetes, Metrics, Logs, Traces, Git, Memory, Topology)
- **Episodic Memory**: Neo4j vector similarity search for learning from past incidents
- **Service Topology Graph**: Blast radius analysis, dependency mapping, ownership/on-call
- **Skills Registry**: 25+ modular investigation skills (observability, k8s, git, remediation, etc.)
- **Risk Scoring Rubric**: Per-action-type safety assessment with dry-run simulation
- **Tool Schema Contracts**: Strict JSON Schema for deterministic LLM function calling

### 4. Safety & Verification (`/verification`)
- Dry-run manifest generation with impact prediction
- Risk scoring rubric (rollback=0.15, config_change=0.35, restart=0.45, traffic_shift=0.50)
- Git patch output for PR creation
- kubectl rollback commands with confidence ratings
- RBAC-gated mutating operations with approval workflow

### 5. Mock Microservices (`/mock-services`)
3-service demo app (Python/Go/Node.js) with:
- Full OpenTelemetry instrumentation
- Prometheus metrics, Loki logs, OTLP traces
- Chaos endpoints for failure/latency injection

### 6. Evaluation Framework (`/eval`)
AIOpsLab-style benchmark suite:
- Synthetic incident generation (resource exhaustion, network latency, deployment regression, cascading failure, config drift)
- Ground truth datasets with expected root causes and remediations
- Standardized metrics: detection accuracy, root cause accuracy, blast radius F1, remediation relevance/safety
- Automated fault injection via Chaos Mesh

### 7. Episodic Memory (`/memory/episodic_memory.py`)
Neo4j-backed vector similarity store:
- Agent-driven recall (not automatic pre-injection)
- One episode per conversation (keyed by correlation_id)
- Strategy playbook synthesis from repeated incidents
- In-memory fallback for development

### 8. Service Topology Graph (`/memory/topology_graph.py`)
Neo4j-backed knowledge graph:
- Service dependencies (CALLS, DEPENDS_ON, DEPLOYED_ON, etc.)
- Blast radius analysis (upstream/downstream, shared DBs/queues)
- Deployment impact analysis
- Team ownership and on-call routing
- Sync from Kubernetes, service mesh, distributed traces

### 9. Skills System (`/skills/registry.py`)
Modular investigation capabilities:
- 25+ built-in skills across 10 categories
- Progressive loading (metadata first, full content on demand)
- Per-team enable/disable with hierarchical config
- Custom Python scripts for complex logic

### 10. Subagent Orchestrator (`/subagents/orchestrator.py`)
Parallel specialist investigation:
- 7 specialist subagent types
- Concurrency-controlled execution
- Progress streaming via callbacks
- Investigation planning from incident characteristics

## Quick Start

```bash
# Clone the repository
git clone https://github.com/ihave9lives/auto-remediation-agent.git
cd auto-remediation-agent

# Start the full stack (includes Neo4j for memory/topology)
docker-compose up -d

# Verify all services
curl localhost:8081/health  # service-a
curl localhost:8082/health  # service-b  
curl localhost:8083/health  # service-c
curl localhost:9090         # Prometheus
curl localhost:3100         # Loki
curl localhost:7474         # Neo4j Browser
curl localhost:3000         # Grafana (admin/admin)

# Run the agent (requires OPENAI_API_KEY)
export OPENAI_API_KEY=your-key
cd agent && pip install -e .[dev] && python -m agent.main

# Inject chaos to test detection
kubectl apply -f chaos/latency-injection.yaml
kubectl apply -f chaos/pod-failure.yaml
kubectl apply -f chaos/cpu-stress.yaml

# Run evaluation benchmarks
cd eval && python -m benchmark
```

## Learning Path

See [LEARNING_PATH.md](LEARNING_PATH.md) for the 4-phase milestone curriculum:
1. **Phase 1**: Instrument & Break - Deploy stack, inject chaos, correlate signals
2. **Phase 2**: Rust/Go Watchdog - Extend detection logic, log entropy analysis
3. **Phase 3**: Constrained Agent - Deterministic LangGraph, tool schemas, token budgeting
4. **Phase 4**: Safety & Dry-Run - Risk rubrics, simulation, RBAC gates, PR generation

## Advanced Features

### Episodic Memory
```python
from memory.episodic_memory import create_episodic_memory, Episode, EpisodeStatus

memory = create_episodic_memory(use_neo4j=True)

# Store episode
episode = Episode(
    correlation_id="incident-123",
    title="Memory leak in payment-service",
    summary="...",
    issue_type="resource_exhaustion",
    affected_components=["payment-service", "order-service"],
    root_cause="Unclosed database connections in v2.3.1",
    evidence=["Connection pool exhausted", "Heap growth 50MB/hour"],
    skills_used=["get_k8s_pod_status", "query_prometheus_metrics", "git_blame_recent_changes"],
    remediation_actions=["rollback", "scale"],
    effectiveness_score=0.9,
    status=EpisodeStatus.RESOLVED,
)
memory.store_episode(episode)

# Search similar incidents
similar = memory.search_similar(
    query_text="memory leak in payment service",
    limit=5,
    min_score=0.7,
)

# Synthesize playbook
playbook = memory.synthesize_strategy_playbook("resource_exhaustion", min_episodes=3)
```

### Service Topology Graph
```python
from memory.topology_graph import create_topology_graph, RelationshipType

topology = create_topology_graph(use_neo4j=True)

# Add services and dependencies
topology.upsert_service("payment-service", "Payment Service", team="payments-team")
topology.upsert_service("order-service", "Order Service", team="orders-team")
topology.add_service_dependency("order-service", "payment-service", "http")

# Get blast radius
blast = topology.get_blast_radius("payment-service", max_hops=3)
print(f"Downstream: {len(blast['downstream'])} services")
print(f"Shared DBs: {len(blast['shared_databases'])}")

# Get deployment impact
impact = topology.get_deployment_impact("payment-service-v2.3.1")
```

### Skills System
```python
from skills.registry import SkillRegistry, create_builtin_skills

registry = SkillRegistry("skills")
# Built-in skills already loaded

# List available skills
for skill in registry.list_skills(category=SkillCategory.KUBERNETES):
    print(f"{skill.id}: {skill.name}")

# Execute a skill
result = await registry.execute_skill(
    "get_k8s_pod_status",
    {"namespace": "production", "pod": "payment-service-xyz"},
    tool_executor=my_tool_executor,
)
```

### Subagent Orchestration
```python
from subagents.orchestrator import create_subagent_orchestrator, SubagentType

orchestrator = create_subagent_orchestrator(tool_executor=my_executor)

# Create investigation plan
tasks = orchestrator.create_investigation_plan({
    "service": "payment-service",
    "namespace": "production",
    "anomaly_type": "error_rate_spike",
})

# Execute in parallel
results = await orchestrator.execute_parallel(tasks)
for r in results:
    print(f"{r.subagent_type}: {len(r.findings)} findings, confidence: {r.confidence:.0%}")
```

### Evaluation Benchmark
```python
from eval.benchmark import BenchmarkRunner, create_standard_benchmark_suite

runner = BenchmarkRunner(
    agent_runner=my_agent_runner,
    incident_generator=generator,
)

# Create standard benchmark suite
incidents = create_standard_benchmark_suite(topology_graph)

# Run benchmark
results = await runner.run_benchmark("my-agent-v1", incidents)

# Generate report
print(runner.generate_report("my-agent-v1"))
```

## Configuration

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `NEO4J_URI` | Neo4j Bolt URI | `bolt://localhost:7687` |
| `NEO4J_USER` | Neo4j username | `neo4j` |
| `NEO4J_PASSWORD` | Neo4j password | `password` |
| `OPENAI_API_KEY` | OpenAI API key for LLM | Required |
| `PROMETHEUS_URL` | Prometheus endpoint | `http://prometheus:9090` |
| `LOKI_URL` | Loki endpoint | `http://loki:3100` |
| `CLICKHOUSE_HOST` | ClickHouse host | `clickhouse` |
| `CLICKHOUSE_PORT` | ClickHouse native port | `9000` |

### Agent Configuration
```python
agent = EnhancedRemediationAgent(
    model_name="gpt-4-turbo-preview",
    temperature=0.1,
    use_neo4j=True,
    neo4j_uri="bolt://neo4j:7687",
    neo4j_user="neo4j",
    neo4j_password="password",
    skills_dir="skills",
    max_concurrent_subagents=5,
)
```

## Safety Features

- **Read-only agent roles** by default
- **RBAC-gated mutating operations** with approval workflow
- **Progressive delivery** via Argo Rollouts integration
- **Risk scoring rubric** per remediation action type
- **Dry-run simulation** before any production change
- **Git patch + PR workflow** for all remediations
- **Episodic memory** prevents repeating failed remediations
- **Strategy playbooks** encode best practices from past incidents

## Comparison with Similar Projects

| Feature | This Agent | OpenSRE | HolmesGPT | AIOpsLab |
|---------|-----------|---------|-----------|----------|
| **State Machine** | LangGraph (deterministic) | Claude Agent SDK (flexible) | Custom | Benchmark only |
| **Episodic Memory** | Neo4j vector search ✓ | Neo4j vector search ✓ | ✗ | ✗ |
| **Service Topology** | Neo4j graph ✓ | Neo4j graph ✓ | ✗ | ✗ |
| **Skills System** | YAML + Python scripts ✓ | SKILL.md + Bash ✓ | Toolsets ✓ | ✗ |
| **Subagents** | 7 specialist types ✓ | Background tasks ✓ | ✗ | ✗ |
| **Evaluation** | AIOpsLab-style ✓ | ✗ | ✗ | Core focus ✓ |
| **Safety Gates** | Risk rubric + dry-run ✓ | Approval workflow ✓ | ✗ | ✗ |
| **Chaos Integration** | Chaos Mesh ✓ | ✗ | ✗ | Fault injector ✓ |
| **Multi-language** | Python/Go/Rust/JS ✓ | Python ✓ | Go ✓ | Python ✓ |

## Directory Structure

```
auto-remediation-agent/
├── .github/workflows/ci-cd.yaml      # Full CI/CD pipeline
├── agent/                            # Python agentic engine
│   ├── graph/workflow.py            # Enhanced LangGraph state machine
│   ├── tools/schemas.py             # JSON Schema tool contracts
│   ├── tools/implementations.py     # Tool implementations
│   ├── memory/                      # Episodic memory & topology
│   ├── skills/                      # Skills registry & built-ins
│   ├── subagents/                   # Specialist subagents
│   └── verification/                # Risk scoring & dry-run
├── watchdog/                         # Rust anomaly detection
├── mock-services/                   # 3-service demo (Py/Go/Node)
├── chaos/                           # Chaos Mesh experiments
├── eval/                            # AIOpsLab-style benchmarks
├── otel-collector/                  # OTel Collector config
├── docker/                          # Prometheus, Loki, Grafana configs
├── docker-compose.yaml              # Full local stack
└── LEARNING_PATH.md                 # 4-phase curriculum
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Run tests: `cd agent && python -m pytest` and `cd watchdog && cargo test`
4. Run linting: `cd agent && ruff check . && mypy .`
5. Submit a PR with description of changes

## License

MIT License - see LICENSE file for details.

## References

- [OpenSRE Architecture](https://opensre.in/docs/architecture) - Episodic memory, skills, subagents
- [HolmesGPT](https://github.com/HolmesGPT/holmesgpt) - Toolsets, skills, CNCF Sandbox
- [AIOpsLab](https://github.com/microsoft/AIOpsLab) - Evaluation framework, fault injection
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) - Telemetry pipeline
- [Chaos Mesh](https://chaos-mesh.org/) - Kubernetes chaos engineering