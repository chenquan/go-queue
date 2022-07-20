package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*Headers)(nil)

type (
	Headers struct {
		headers *[]kafka.Header
	}
	headerKey struct{}
)

func (h *Headers) Get(key string) string {
	for _, header := range *h.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}

	return ""
}

func (h *Headers) Set(key string, value string) {
	for _, header := range *h.headers {
		if header.Key == key {
			header.Value = []byte(value)
			return
		}
	}
	*h.headers = append(*h.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (h *Headers) Keys() []string {
	keys := make([]string, 0, len(*h.headers))
	for _, header := range *h.headers {
		keys = append(keys, header.Key)
	}

	return keys
}

func NewHeadersContext(ctx context.Context, headers ...kafka.Header) context.Context {
	return context.WithValue(ctx, headerKey{}, headers)
}

func HeadersFromContext(ctx context.Context) ([]kafka.Header, bool) {
	value := ctx.Value(headerKey{})
	if value == nil {
		return nil, false
	}

	return value.([]kafka.Header), true
}
