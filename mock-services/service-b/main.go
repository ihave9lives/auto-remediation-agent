package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	serviceName = getEnv("SERVICE_NAME", "service-b")
	otelEndpoint = getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	downstreamURL = getEnv("DOWNSTREAM_URL", "http://localhost:8080")
	failureRate = getEnvFloat("FAILURE_RATE", 0.0)
	latencyMS = getEnvInt("LATENCY_MS", 30)
)

var (
	requestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests",
	}, []string{"method", "endpoint", "status"})
	requestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "endpoint"})
	activeRequests = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "http_active_requests",
		Help: "Active HTTP requests",
	}, []string{"method", "endpoint"})
	downstreamLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "downstream_request_duration_seconds",
		Help:    "Downstream request latency",
		Buckets: prometheus.DefBuckets,
	})
	downstreamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "downstream_errors_total",
		Help: "Downstream errors",
	}, []string{"error_type"})
)

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func initTracer() (*sdktrace.TracerProvider, error) {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otelEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

func metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		activeRequests.WithLabelValues(c.Request.Method, c.FullPath()).Inc()
		start := time.Now()

		c.Next()

		duration := time.Since(start).Seconds()
		requestLatency.WithLabelValues(c.Request.Method, c.FullPath()).Observe(duration)
		requestCount.WithLabelValues(c.Request.Method, c.FullPath(), strconv.Itoa(c.Writer.Status())).Inc()
		activeRequests.WithLabelValues(c.Request.Method, c.FullPath()).Dec()
	}
}

func simulateLatency() {
	time.Sleep(time.Duration(latencyMS) * time.Millisecond)
}

func maybeFail(c *gin.Context) bool {
	if rand.Float64() < failureRate {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "simulated failure"})
		return true
	}
	return false
}

func callDownstream(ctx context.Context, path string) ([]byte, error) {
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   10 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", downstreamURL+path, nil)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start).Seconds()
	downstreamLatency.Observe(duration)

	if err != nil {
		downstreamErrors.WithLabelValues("connection_error").Inc()
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		downstreamErrors.WithLabelValues(fmt.Sprintf("http_%d", resp.StatusCode)).Inc()
		return nil, fmt.Errorf("downstream returned %d", resp.StatusCode)
	}

	return nil, nil // simplified - would read body in real impl
}

func main() {
	rand.Seed(time.Now().UnixNano())

	tp, err := initTracer()
	if err != nil {
		log.Printf("Warning: failed to init tracer: %v", err)
	}
	defer func() {
		if tp != nil {
			tp.Shutdown(context.Background())
		}
	}()

	r := gin.Default()
	r.Use(metricsMiddleware())
	r.Use(otelgin.Middleware(serviceName))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy", "service": serviceName})
	})

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/api/orders", func(c *gin.Context) {
		simulateLatency()
		if maybeFail(c) {
			return
		}

		// Call downstream service C
		ctx := c.Request.Context()
		_, err := callDownstream(ctx, "/api/items")
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "downstream error"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"orders": []gin.H{
				{"id": "order-1", "user_id": "user-1", "item": "item-1"},
				{"id": "order-2", "user_id": "user-1", "item": "item-2"},
			},
		})
	})

	r.GET("/api/orders/:user_id", func(c *gin.Context) {
		userID := c.Param("user_id")
		simulateLatency()
		if maybeFail(c) {
			return
		}

		ctx := c.Request.Context()
		_, err := callDownstream(ctx, "/api/items")
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "downstream error"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"user_id": userID,
			"orders": []gin.H{
				{"id": "order-1", "item": "item-1"},
			},
		})
	})

	r.POST("/chaos/failure-rate", func(c *gin.Context) {
		var req struct{ Rate float64 `json:"rate"` }
		if c.BindJSON(&req) == nil {
			failureRate = req.Rate
			if failureRate < 0 {
				failureRate = 0
			}
			if failureRate > 1 {
				failureRate = 1
			}
		}
		c.JSON(http.StatusOK, gin.H{"failure_rate": failureRate})
	})

	r.POST("/chaos/latency", func(c *gin.Context) {
		var req struct{ LatencyMS int `json:"latency_ms"` }
		if c.BindJSON(&req) == nil {
			latencyMS = req.LatencyMS
			if latencyMS < 0 {
				latencyMS = 0
			}
		}
		c.JSON(http.StatusOK, gin.H{"latency_ms": latencyMS})
	})

	port := getEnv("PORT", "8080")
	log.Printf("Starting %s on port %s", serviceName, port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}