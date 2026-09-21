// Package otelx sets up distributed tracing.
//
// The reason Murmur needs traces rather than just logs: one GraphQL query
// becomes dozens of gRPC calls, each of which becomes one or more database
// queries. Logs tell you that sixty-two calls happened. A trace shows you the
// shape — sixty of them identical, issued in parallel, each waiting on the
// same table — which is the difference between knowing a number and knowing
// what to do about it.
package otelx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"go.opentelemetry.io/otel/sdk/resource"
)

// Config controls the tracer provider.
type Config struct {
	ServiceName string
	Environment string
	// Endpoint is an OTLP/gRPC collector address such as "jaeger:4317".
	// Empty disables tracing entirely.
	Endpoint string
	// SampleRatio is the fraction of traces recorded, between 0 and 1.
	SampleRatio float64
}

// Setup installs a global tracer provider and propagator. The returned
// function flushes pending spans and shuts the exporter down.
//
// With no endpoint configured it installs nothing and returns a no-op closer,
// so every instrumented call site keeps working — spans simply go nowhere.
// Tracing is an observability feature, not a dependency; a missing collector
// must never stop a service from serving.
func Setup(ctx context.Context, cfg Config, log *slog.Logger) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	if cfg.Endpoint == "" {
		log.Info("tracing disabled", "reason", "no OTEL_EXPORTER_OTLP_ENDPOINT")
		return noop, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return noop, fmt.Errorf("create otlp exporter: %w", err)
	}

	// NewSchemaless, not NewWithAttributes(semconv.SchemaURL, ...).
	//
	// resource.Merge refuses to combine two resources that declare different
	// schema URLs, and resource.Default() declares whichever version the SDK
	// was built against. Pinning a semconv import here makes the merge fail
	// the moment the SDK moves ahead of it — which it does on any routine
	// dependency bump, and the failure surfaces as "tracing silently off"
	// rather than as a build error. A schemaless resource carries no URL, so
	// it merges with any SDK version.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		attribute.String("deployment.environment", cfg.Environment),
	))
	if err != nil {
		return noop, fmt.Errorf("build trace resource: %w", err)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// ParentBased keeps a trace whole: once the gateway decides to record
		// a request, every downstream service records its part, rather than
		// each sampling independently and producing traces full of holes.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Info("tracing enabled", "endpoint", cfg.Endpoint, "sample_ratio", ratio)

	return func(ctx context.Context) error {
		// Flush on a deadline of its own: shutdown should not hang because a
		// collector has gone away.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return provider.Shutdown(ctx)
	}, nil
}
