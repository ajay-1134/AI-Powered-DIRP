// Package telemetry wires OpenTelemetry tracing and Prometheus metrics for a
// service. Every microservice and the platform control plane call Setup once at
// boot and Shutdown on exit. Keeping this in one place means all services emit
// signals in an identical, correlatable shape — which is exactly what the
// incident platform depends on when it reconstructs a failure after the fact.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Provider holds the initialized telemetry plumbing for a service. Shutdown
// must be called on exit so the last batch of spans is flushed to the collector.
type Provider struct {
	Tracer      trace.Tracer
	shutdownFns []func(context.Context) error
}

// Config controls how telemetry is initialized.
type Config struct {
	// ServiceName is the logical name that shows up in traces and metrics,
	// e.g. "payment-service". The platform keys its dependency graph off this.
	ServiceName string
	// OTLPEndpoint is the gRPC endpoint of the OpenTelemetry collector, e.g.
	// "localhost:4317". If empty, tracing is set up as a no-op so a service can
	// still run standalone without a collector present.
	OTLPEndpoint string
}

// Setup initializes the global OpenTelemetry tracer provider and text-map
// propagator. It returns a Provider whose Tracer should be used to start spans.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	// Propagate W3C trace context (traceparent header) across service hops so a
	// single request produces one connected trace end to end. This is what lets
	// the platform later say "this Order timeout was the same request as that
	// Payment DB stall".
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	p := &Provider{}

	if cfg.OTLPEndpoint == "" {
		// No collector configured: fall back to a provider with no exporter so
		// the service still starts. Spans are created but dropped.
		tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		p.shutdownFns = append(p.shutdownFns, tp.Shutdown)
		p.Tracer = tp.Tracer(cfg.ServiceName)
		return p, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		// Sample everything: this is a demo platform where every trace matters
		// for root-cause reconstruction. In production you'd sample adaptively.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	p.shutdownFns = append(p.shutdownFns, tp.Shutdown)
	p.Tracer = tp.Tracer(cfg.ServiceName)

	return p, nil
}

// Shutdown flushes and closes all telemetry exporters. Call it during graceful
// shutdown with a bounded context.
func (p *Provider) Shutdown(ctx context.Context) error {
	var firstErr error
	for _, fn := range p.shutdownFns {
		if err := fn(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
