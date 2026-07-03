package obs

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// InitTracing configures OpenTelemetry (FR-020, EW-OBS-003). The W3C
// propagator is always installed so render-origin requests carry trace
// context (FR-007); with an empty endpoint the global provider stays no-op
// and spans are zero-cost. With an endpoint (OTEL_EXPORTER_OTLP_ENDPOINT,
// e.g. http://otel-collector:4318) spans export over OTLP/HTTP.
func InitTracing(ctx context.Context, endpoint, serviceName, version string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(strings.TrimRight(endpoint, "/")+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("otel exporter: %w", err)
	}
	res := sdkresource.NewSchemaless(
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the worker tracer. otel.Tracer yields a lazy proxy, so a
// provider installed later (InitTracing, or a test provider) is honored.
func Tracer() trace.Tracer { return otel.Tracer("anvilkit-export-worker") }

// StartSpan opens one §15.3 pipeline-stage span.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

// EndSpan closes a span, recording err as its status.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
