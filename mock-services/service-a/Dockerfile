# Mock Service A - Python/FastAPI with OpenTelemetry

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    httpx==0.25.2 \
    opentelemetry-api==1.21.0 \
    opentelemetry-sdk==1.21.0 \
    opentelemetry-exporter-otlp==1.21.0 \
    opentelemetry-instrumentation-fastapi==0.42b0 \
    opentelemetry-instrumentation-httpx==0.42b0 \
    prometheus-client==0.19.0

COPY . .

EXPOSE 8080

CMD ["python", "main.py"]