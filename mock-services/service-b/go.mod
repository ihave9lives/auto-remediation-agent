module service-b

go 1.21

require (
	github.com/gin-gonic/gin v1.9.1
	github.com/prometheus/client_golang v1.17.0
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.21.0
	go.opentelemetry.io/otel/sdk v1.21.0
	go.opentelemetry.io/otel/sdk/metric v1.21.0
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v1.21.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v1.21.0
)