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
```

## Components

### 1. Telemetry Ingestion (`/otel-collector`)
OpenTelemetry Collector configuration with:
- OTLP receivers (gRPC/HTTP)
- Processors: batch, memory_limiter, tail_sampling
- Exporters: ClickHouse, Prometheus, Loki

### 2. Anomaly Watchdog (`/watchdog`)
Rust daemon that:
- Tails metrics/log streams
- Computes sliding-window anomalies (Z-score, EWMA, Shannon entropy)
- Emits structured incident payloads

### 3. Agentic Root-Cause Engine (`/agent`)
Python backend with:
- LangGraph state machine: Detect → Correlate → Isolate → Verify → Human-in-Loop
- Tool schema contracts (JSON Schema)
- Deterministic evaluation framework

### 4. Safety & Verification (`/verification`)
- Dry-run manifest generation
- Risk scoring rubric
- Git patch output for PR creation
- kubectl rollback commands with confidence ratings

### 5. Mock Microservices (`/mock-services`)
3-service demo app with Chaos Mesh integration for testing.

## Quick Start

```bash
# Start the full stack
docker-compose up -d

# Run the watchdog
cd watchdog && cargo run --release

# Run the agent
cd agent && python -m agent.main

# Inject chaos
kubectl apply -f chaos/latency-injection.yaml
```

## Learning Path

See [LEARNING_PATH.md](LEARNING_PATH.md) for the 4-phase milestone curriculum.

## Safety

- Read-only agent roles by default
- RBAC-gated mutating operations
- Progressive delivery via Argo Rollouts
- All remediation goes through PR review