package xtrace

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const Name = "queue"

func Tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(Name)
}
