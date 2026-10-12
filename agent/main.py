"""
Main entry point for the Autonomous Remediation Agent
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from .state import Incident, AnomalyType, Severity, AgentContext
from .graph.workflow import RemediationAgent, run_remediation

logger = structlog.get_logger()


class IncidentRequest(BaseModel):
    """Incoming incident from watchdog"""
    service: str
    anomaly_type: AnomalyType
    severity: Severity
    metrics: dict = {}
    context: dict = {}
    confidence: float = 0.8


class IncidentResponse(BaseModel):
    """Response after processing incident"""
    incident_id: str
    status: str
    remediation_plan_id: Optional[str] = None
    summary: str
    risk_score: Optional[float] = None
    confidence: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    version: str = "0.1.0"


agent: Optional[RemediationAgent] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global agent
    logger.info("Starting Autonomous Remediation Agent")
    agent = RemediationAgent()
    yield
    logger.info("Shutting down Autonomous Remediation Agent")


app = FastAPI(
    title="Autonomous Remediation Agent",
    description="Agentic root-cause analysis and remediation for microservices",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="healthy")


@app.post("/incidents", response_model=IncidentResponse)
async def receive_incident(incident_req: IncidentRequest, background_tasks: BackgroundTasks):
    """Receive incident from watchdog and start remediation workflow"""
    if agent is None:
        raise HTTPException(status_code=503, detail="Agent not initialized")
    
    incident = Incident(
        service=incident_req.service,
        anomaly_type=incident_req.anomaly_type,
        severity=incident_req.severity,
        metrics=incident_req.metrics,
        context=incident_req.context,
        confidence=incident_req.confidence,
    )
    
    logger.info("Received incident", incident_id=incident.id, service=incident.service)
    
    # Run remediation in background
    background_tasks.add_task(process_incident, incident)
    
    return IncidentResponse(
        incident_id=incident.id,
        status="accepted",
        summary=f"Incident {incident.id} accepted for processing",
    )


async def process_incident(incident: Incident):
    """Process incident through the remediation workflow"""
    try:
        logger.info("Processing incident", incident_id=incident.id)
        result = await agent.run(incident)
        
        if result.remediation:
            logger.info(
                "Remediation complete",
                incident_id=incident.id,
                plan_id=result.remediation.plan_id,
                risk_score=result.remediation.risk_score,
                confidence=result.remediation.confidence,
            )
        else:
            logger.warning("Remediation incomplete", incident_id=incident.id, error=result.error)
    
    except Exception as e:
        logger.error("Remediation failed", incident_id=incident.id, error=str(e))


@app.get("/incidents/{incident_id}/status")
async def get_incident_status(incident_id: str):
    """Get status of an incident (would query a database in production)"""
    # In production, query incident store
    return {"incident_id": incident_id, "status": "processing"}


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Autonomous Remediation Agent")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind")
    parser.add_argument("--model", default="gpt-4-turbo-preview", help="LLM model to use")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(getattr(structlog, args.log_level.upper())),
    )
    
    uvicorn.run(
        "agent.main:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()