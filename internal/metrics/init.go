package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

var Meter = otel.Meter("migration-verifier")

func Init(exporter metric.Reader) {
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)
}
