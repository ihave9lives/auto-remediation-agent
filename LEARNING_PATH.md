# Learning Path: Autonomous Incident Remediation Agent

This document outlines the 4-phase learning curriculum to master the skills needed to build, operate, and extend this autonomous remediation agent.

## Phase 1: Instrument & Break (Observability Fundamentals)

### Goals
- Deploy a fully instrumented 3-service microservice application
- Configure OpenTelemetry Collector with production-grade pipelines
- Inject controlled failures and observe telemetry signals

### Tasks

#### 1.1 Deploy the Stack
```bash
docker-compose up -d
```

Verify all services:
- `curl localhost:8081/health` (service-a)
- `curl localhost:8082/health` (service-b)
- `curl localhost:8083/health` (service-c)
- `curl localhost:9090` (Prometheus)
- `curl localhost:3100` (Loki)
- `curl localhost:3000` (Grafana - admin/admin)

#### 1.2 Explore OTel Collector Configuration
Study `otel-collector/config.yaml`:
- **Receivers**: OTLP gRPC/HTTP, Prometheus scrape
- **Processors**: batch, memory_limiter, tail_sampling, transform, resourcedetection, spanmetrics
- **Exporters**: ClickHouse, Prometheus, Loki, debug

Key concepts to understand:
- How `tail_sampling` reduces trace volume while keeping errors/slow traces
- How `spanmetrics` generates RED metrics from spans automatically
- How `transform` normalizes attributes across services

#### 1.3 Generate Traffic and Observe
```bash
# Generate baseline traffic
for i in {1..100}; do curl -s localhost:8081/api/users/user-1; done

# Check traces in Grafana Tempo/ClickHouse
# Check metrics in Prometheus
# Check logs in Loki
```

#### 1.4 Inject Chaos and Correlate
```bash
# Inject latency in service-b
kubectl apply -f chaos/latency-injection.yaml

# Inject pod failures in service-a
kubectl apply -f chaos/pod-failure.yaml

# Inject CPU stress in service-c
kubectl apply -f chaos/cpu-stress.yaml
```

Observe:
- How latency propagates upstream (service-a → service-b → service-c)
- How error rates correlate across services
- How the OTel collector's tail_sampling captures the error traces

### Learning Outcomes
- [ ] Can explain OTLP protocol and collector pipeline components
- [ ] Can write PromQL queries for rate, histogram_quantile, and anomaly detection
- [ ] Can correlate traces, metrics, and logs for a single request flow
- [ ] Understands W3C Trace Context propagation across service boundaries

---

## Phase 2: The Rust/Go Watchdog (Stream Processing)

### Goals
- Build a high-performance anomaly detection daemon
- Implement sliding-window statistical detectors (Z-score, EWMA, entropy)
- Emit structured incident payloads for the agent

### Tasks

#### 2.1 Study the Watchdog Code
Read `watchdog/src/main.rs`:
- `MetricSeries` struct with sliding window and EWMA
- Z-score calculation for anomaly detection
- Prometheus query execution and result parsing
- Incident payload structure

#### 2.2 Build and Run
```bash
cd watchdog
cargo build --release
./target/release/watchdog --config config.toml
```

#### 2.3 Extend Detection Logic
Add new detectors:
- **Rate of change detector**: `deriv(metric[5m])` for sudden spikes
- **Seasonal decomposition**: Compare against same time yesterday
- **Multi-variate correlation**: Detect when multiple metrics shift together

#### 2.4 Log Entropy Analysis
Implement Drain/Spell log clustering in `analyze_log_entropy()`:
1. Query Loki for recent logs
2. Tokenize log lines (split by delimiters, mask variables)
3. Cluster similar templates
4. Compute Shannon entropy: `H = -Σ p(x) log p(x)`
5. Alert when entropy spikes (new error patterns emerging)

### Learning Outcomes
- [ ] Can write Rust async code with tokio for high-throughput stream processing
- [ ] Understands statistical anomaly detection tradeoffs (sensitivity vs specificity)
- [ ] Can implement log template extraction and entropy calculation
- [ ] Knows how to tune detection thresholds for production noise

---

## Phase 3: Constrained Agent Architecture (Agentic Workflows)

### Goals
- Build a deterministic LangGraph state machine for root cause analysis
- Design clean tool schemas for LLM function calling
- Implement pre-filtering to cap token budgets

### Tasks

#### 3.1 Study the State Machine
Read `agent/state.py` and `agent/graph/workflow.py`:
- State enum: DETECT → CORRELATE → ISOLATE → VERIFY → HUMAN_IN_LOOP → REMEDIATE
- Pydantic models for each phase output
- Tool binding per phase (minimal tool set per state)

#### 3.2 Understand Tool Schema Design
Read `agent/tools/schemas.py`:
- JSON Schema for each tool (strict contracts)
- Response models for validation
- Why structured schemas beat free-form tool calling

#### 3.3 Run the Agent
```bash
cd agent
pip install -e .[dev]
python -m agent.main --model gpt-4-turbo-preview
```

#### 3.4 Test with Synthetic Incident
```bash
curl -X POST localhost:8080/incidents \
  -H "Content-Type: application/json" \
  -d '{
    "service": "service-b",
    "anomaly_type": "latency_degradation",
    "severity": "critical",
    "metrics": {"latency_p99": 2.5, "error_rate": 0.15},
    "context": {"namespace": "default", "pod": "service-b-xyz"},
    "confidence": 0.9
  }'
```

#### 3.5 Add Custom Tools
Create a new tool for your environment:
```python
# In tools/schemas.py - add new schema
# In tools/implementations.py - add implementation
# In graph/workflow.py - bind to appropriate phase
```

### Learning Outcomes
- [ ] Can design deterministic agent workflows with LangGraph
- [ ] Understands tool schema design for reliable function calling
- [ ] Can implement pre-filtering (log sampling, metric downsampling) for token budget
- [ ] Knows how to evaluate agent outputs with ground-truth incident datasets

---

## Phase 4: Safety & Dry-Run Verifications (Production Readiness)

### Goals
- Implement risk scoring rubric for remediation actions
- Build dry-run simulation framework
- Create remediation gate with RBAC enforcement

### Tasks

#### 4.1 Study Risk Scoring
Read `verification/risk_scoring.py`:
- `RiskScorer` with per-action-type rubrics
- `DryRunSimulator` for impact prediction
- `RemediationGate` with policy enforcement

#### 4.2 Configure Policies
Adjust `RemediationGate.policies` for your environment:
- Namespace allowlists/blocklists
- Risk thresholds for auto-approval
- Required dry-run for specific action types

#### 4.3 Implement Real Dry-Run
Replace mock simulator with:
- Kind/k3d test cluster for actual deployment simulation
- Argo Rollouts canary analysis for traffic shifts
- Load testing (k6/locust) during simulation

#### 4.4 Git Patch & PR Generation
Test the full pipeline:
1. Agent generates git patch for config change
2. Patch applied to fork/branch
3. PR created with risk assessment
4. CI runs verification
5. Human reviews and merges

#### 4.5 Rollback Automation
Verify kubectl rollback commands:
```bash
# Test rollback dry-run
kubectl rollout undo deployment/service-b -n default --dry-run=client

# Verify rollback history
kubectl rollout history deployment/service-b -n default
```

### Learning Outcomes
- [ ] Can design risk rubrics for different remediation types
- [ ] Understands dry-run simulation approaches (staging, kind, shadow traffic)
- [ ] Can implement RBAC-gated mutation with audit trails
- [ ] Knows progressive delivery patterns (Argo Rollouts, Flagger)

---

## Advanced Extensions

### Evaluation Framework
Build `agent/eval/` with:
- Ground-truth incident dataset (from chaos experiments)
- Metrics: precision/recall on root cause identification
- Latency benchmarks for end-to-end remediation
- Regression tests for agent workflow changes

### Multi-Cluster Support
- Watchdog federation across clusters
- Agent with cluster-aware tool routing
- Cross-cluster dependency mapping

### Self-Improving Agent
- Feedback loop: human corrections → few-shot examples → improved prompts
- Online learning for threshold tuning
- Automated runbook generation from successful remediations

---

## Recommended Reading

### Observability
- [OpenTelemetry Collector Documentation](https://opentelemetry.io/docs/collector/)
- [PromQL for Humans](https://promlabs.com/promql-for-humans/)
- [Distributed Tracing in Practice](https://www.oreilly.com/library/view/distributed-tracing-in/9781492058378/)

### Stream Processing
- [Rust Async Book](https://rust-lang.github.io/async-book/)
- [Statistical Anomaly Detection](https://arxiv.org/abs/1802.03903)
- [Drain Log Parser](https://www.cs.utexas.edu/~liner/papers/drain-icws17.pdf)

### Agentic Systems
- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [Structured Outputs for LLMs](https://openai.com/blog/structured-outputs)
- [ReAct vs State Machines](https://arxiv.org/abs/2310.03715)

### SRE/Kubernetes
- [Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Site Reliability Engineering Book](https://sre.google/books/)
- [Progressive Delivery with Argo Rollouts](https://argoproj.github.io/argo-rollouts/)

---

## Milestone Checklist

| Phase | Milestone | Completed |
|-------|-----------|-----------|
| 1 | Stack deployed, chaos injected, signals correlated | ☐ |
| 2 | Watchdog detecting anomalies, emitting incidents | ☐ |
| 3 | Agent running full workflow on test incidents | ☐ |
| 4 | Risk gate blocking unsafe actions, PRs generated | ☐ |

---

## Next Steps After Completion

1. **Add real integrations**: PagerDuty, Slack, Jira for incident management
2. **Scale storage**: Migrate ClickHouse to distributed cluster, add Tiered Storage
3. **Hardening**: mTLS between components, secret management, audit logging
4. **ML Enhancement**: Train anomaly detection models on historical data
5. **Community**: Open source the agent, contribute to OpenTelemetry/Argo ecosystems