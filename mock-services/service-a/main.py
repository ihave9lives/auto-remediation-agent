"""
Mock Service A - Python/FastAPI with OpenTelemetry instrumentation
"""

import os
import random
import time
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse
import httpx
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME

# Configuration
SERVICE_NAME = os.getenv("SERVICE_NAME", "service-a")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://localhost:8080")
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))
LATENCY_MS = int(os.getenv("LATENCY_MS", "50"))

# Prometheus metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP request latency', ['method', 'endpoint'])
ACTIVE_REQUESTS = Gauge('http_active_requests', 'Active HTTP requests', ['method', 'endpoint'])
DOWNSTREAM_LATENCY = Histogram('downstream_request_duration_seconds', 'Downstream request latency')
DOWNSTREAM_ERRORS = Counter('downstream_errors_total', 'Downstream errors', ['error_type'])

# OpenTelemetry setup
resource = Resource(attributes={SERVICE_NAME: SERVICE_NAME})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# HTTP client for downstream calls
http_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0)
    HTTPXClientInstrumentor().instrument_client(http_client)
    yield
    await http_client.aclose()


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)

# Instrument FastAPI
FastAPIInstrumentor.instrument_app(app, tracer_provider=trace.get_tracer_provider())


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    ACTIVE_REQUESTS.labels(method=request.method, endpoint=request.url.path).inc()
    start = time.time()
    
    try:
        response = await call_next(request)
        duration = time.time() - start
        REQUEST_LATENCY.labels(method=request.method, endpoint=request.url.path).observe(duration)
        REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path, status=response.status_code).inc()
        return response
    except Exception as e:
        duration = time.time() - start
        REQUEST_LATENCY.labels(method=request.method, endpoint=request.url.path).observe(duration)
        REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path, status=500).inc()
        raise
    finally:
        ACTIVE_REQUESTS.labels(method=request.method, endpoint=request.url.path).dec()


@app.get("/health")
async def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type="text/plain")


@app.get("/api/users/{user_id}")
async def get_user(user_id: str, request: Request):
    """Get user - calls downstream service B"""
    with tracer.start_as_current_span("get_user") as span:
        span.set_attribute("user_id", user_id)
        
        # Simulate processing latency
        await asyncio.sleep(LATENCY_MS / 1000)
        
        # Inject failure
        if random.random() < FAILURE_RATE:
            span.set_attribute("error", True)
            span.set_attribute("error.type", "simulated_failure")
            raise HTTPException(status_code=500, detail="Simulated failure")
        
        # Call downstream service B
        downstream_start = time.time()
        try:
            resp = await http_client.get(f"{DOWNSTREAM_URL}/api/orders?user_id={user_id}")
            downstream_duration = time.time() - downstream_start
            DOWNSTREAM_LATENCY.observe(downstream_duration)
            
            if resp.status_code != 200:
                DOWNSTREAM_ERRORS.labels(error_type=f"http_{resp.status_code}").inc()
                span.set_attribute("downstream.error", True)
                raise HTTPException(status_code=502, detail="Downstream error")
            
            return {"user_id": user_id, "orders": resp.json()}
        except httpx.RequestError as e:
            downstream_duration = time.time() - downstream_start
            DOWNSTREAM_LATENCY.observe(downstream_duration)
            DOWNSTREAM_ERRORS.labels(error_type="connection_error").inc()
            span.set_attribute("downstream.error", True)
            span.set_attribute("error.message", str(e))
            raise HTTPException(status_code=503, detail="Downstream unavailable")


@app.post("/api/users")
async def create_user(request: Request):
    """Create user"""
    with tracer.start_as_current_span("create_user"):
        await asyncio.sleep(LATENCY_MS / 1000)
        
        if random.random() < FAILURE_RATE:
            raise HTTPException(status_code=500, detail="Simulated failure")
        
        body = await request.json()
        return {"user_id": "new-user-123", **body}


@app.get("/api/orders")
async def get_orders():
    """Get orders - calls downstream service B"""
    with tracer.start_as_current_span("get_orders"):
        await asyncio.sleep(LATENCY_MS / 1000)
        
        if random.random() < FAILURE_RATE:
            raise HTTPException(status_code=500, detail="Simulated failure")
        
        downstream_start = time.time()
        try:
            resp = await http_client.get(f"{DOWNSTREAM_URL}/api/orders")
            downstream_duration = time.time() - downstream_start
            DOWNSTREAM_LATENCY.observe(downstream_duration)
            
            if resp.status_code != 200:
                DOWNSTREAM_ERRORS.labels(error_type=f"http_{resp.status_code}").inc()
                raise HTTPException(status_code=502, detail="Downstream error")
            
            return resp.json()
        except httpx.RequestError as e:
            DOWNSTREAM_ERRORS.labels(error_type="connection_error").inc()
            raise HTTPException(status_code=503, detail="Downstream unavailable")


@app.post("/chaos/failure-rate")
async def set_failure_rate(rate: float):
    """Chaos endpoint to inject failures"""
    global FAILURE_RATE
    FAILURE_RATE = max(0.0, min(1.0, rate))
    return {"failure_rate": FAILURE_RATE}


@app.post("/chaos/latency")
async def set_latency(latency_ms: int):
    """Chaos endpoint to inject latency"""
    global LATENCY_MS
    LATENCY_MS = max(0, latency_ms)
    return {"latency_ms": LATENCY_MS}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)