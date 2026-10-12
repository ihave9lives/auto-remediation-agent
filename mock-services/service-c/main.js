const express = require('express');
const { MeterProvider, Histogram } = require('@opentelemetry/sdk-metrics');
const { Resource } = require('@opentelemetry/resources');
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { NodeTracerProvider } = require('@opentelemetry/sdk-trace-node');
const { BatchSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');
const { trace, propagation, context } = require('@opentelemetry/api');
const { registerInstrumentations } = require('@opentelemetry/instrumentation');
const { ExpressInstrumentation } = require('@opentelemetry/instrumentation-express');
const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');
const promClient = require('prom-client');

const app = express();
app.use(express.json());

// Configuration
const SERVICE_NAME = process.env.SERVICE_NAME || 'service-c';
const OTEL_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4317';
const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE) || 0.0;
const LATENCY_MS = parseInt(process.env.LATENCY_MS) || 20;

// Prometheus metrics
const requestCount = new promClient.Counter({
  name: 'http_requests_total',
  help: 'Total HTTP requests',
  labelNames: ['method', 'endpoint', 'status']
});

const requestLatency = new promClient.Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request latency',
  labelNames: ['method', 'endpoint'],
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5]
});

const activeRequests = new promClient.Gauge({
  name: 'http_active_requests',
  help: 'Active HTTP requests',
  labelNames: ['method', 'endpoint']
});

// OpenTelemetry setup
const resource = new Resource({
  [SemanticResourceAttributes.SERVICE_NAME]: SERVICE_NAME
});

const tracerProvider = new NodeTracerProvider({ resource });
const otlpExporter = new OTLPTraceExporter({
  url: OTEL_ENDPOINT,
});

tracerProvider.addSpanProcessor(new BatchSpanProcessor(otlpExporter));
trace.setGlobalTracerProvider(tracerProvider);

// Register auto-instrumentations
registerInstrumentations({
  instrumentations: [
    new ExpressInstrumentation(),
    new HttpInstrumentation(),
  ],
  tracerProvider: tracerProvider,
});

const tracer = trace.getTracer(SERVICE_NAME);

// Metrics middleware
app.use((req, res, next) => {
  const start = Date.now();
  activeRequests.inc({ method: req.method, endpoint: req.path });

  res.on('finish', () => {
    const duration = (Date.now() - start) / 1000;
    requestLatency.observe({ method: req.method, endpoint: req.path }, duration);
    requestCount.inc({ method: req.method, endpoint: req.path, status: res.statusCode });
    activeRequests.dec({ method: req.method, endpoint: req.path });
  });

  next();
});

// Routes
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: SERVICE_NAME });
});

app.get('/metrics', (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.send(promClient.register.metrics());
});

app.get('/api/items', async (req, res) => {
  const span = tracer.startSpan('get_items');
  try {
    // Simulate latency
    await new Promise(r => setTimeout(r, LATENCY_MS));

    // Inject failure
    if (Math.random() < FAILURE_RATE) {
      span.setAttribute('error', true);
      span.setAttribute('error.type', 'simulated_failure');
      span.end();
      return res.status(500).json({ error: 'Simulated failure' });
    }

    res.json([
      { id: 'item-1', name: 'Item 1', price: 10.99 },
      { id: 'item-2', name: 'Item 2', price: 20.50 },
      { id: 'item-3', name: 'Item 3', price: 5.99 },
    ]);
  } catch (err) {
    span.recordException(err);
    span.end();
    res.status(500).json({ error: err.message });
  } finally {
    span.end();
  }
});

app.get('/api/items/:id', async (req, res) => {
  const span = tracer.startSpan('get_item', {
    attributes: { 'item.id': req.params.id }
  });
  try {
    await new Promise(r => setTimeout(r, LATENCY_MS));

    if (Math.random() < FAILURE_RATE) {
      span.setAttribute('error', true);
      span.end();
      return res.status(500).json({ error: 'Simulated failure' });
    }

    res.json({ id: req.params.id, name: `Item ${req.params.id}`, price: 15.00 });
  } catch (err) {
    span.recordException(err);
    res.status(500).json({ error: err.message });
  } finally {
    span.end();
  }
});

// Chaos endpoints
app.post('/chaos/failure-rate', (req, res) => {
  const rate = Math.max(0, Math.min(1, req.body.rate));
  FAILURE_RATE = rate;
  res.json({ failure_rate: FAILURE_RATE });
});

app.post('/chaos/latency', (req, res) => {
  const latency = Math.max(0, req.body.latency_ms);
  LATENCY_MS = latency;
  res.json({ latency_ms: LATENCY_MS });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`${SERVICE_NAME} listening on port ${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down...');
  await tracerProvider.shutdown();
  process.exit(0);
});