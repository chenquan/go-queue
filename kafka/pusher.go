package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/executors"
)

type (
	PushOption func(options *pushOptions)

	Pusher struct {
		tracer       trace.Tracer
		producer     *kafka.Writer
		topic        string
		executor     *executors.ChunkExecutor
		once         sync.Once
		initExecutor func()
	}

	pushOptions struct {
		chunkSize     int
		flushInterval time.Duration
		// An optional function called when the writer succeeds or fails the
		// delivery of messages to a kafka partition.
		completion func(messages []kafka.Message, err error)
		balancer   Balancer
	}

	callOptions struct {
		isSync bool
	}

	HeaderKey struct{}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()
	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	producer := &kafka.Writer{
		Addr:        kafka.TCP(addrs...),
		Topic:       topic,
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Snappy,
		Completion:  options.completion,
	}

	pusher := &Pusher{
		tracer:   tracer,
		producer: producer,
		topic:    topic,
	}

	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}

	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}

	if options.balancer != nil {
		producer.Balancer = options.balancer
	}

	pusher.initExecutor = func() {
		pusher.once.Do(func() {
			pusher.executor = executors.NewChunkExecutor(
				func(tasks []interface{}) {
					chunk := make([]kafka.Message, len(tasks))
					for i := range tasks {
						chunk[i] = tasks[i].(kafka.Message)
					}
					if err := pusher.producer.WriteMessages(context.Background(), chunk...); err != nil {
						logx.Error(err)
					}
				}, chunkOpts...,
			)
		})

	}
	return pusher
}

func (p *Pusher) Close() error {
	return p.producer.Close()
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(ctx context.Context, k, v []byte, opts ...queue.CallOptions) (
	interface{}, error) {
	msg := kafka.Message{
		Key:   k,
		Value: v,
	}

	c := new(callOptions)
	for _, opt := range opts {
		opt(c)
	}
	headers, b := HeadersFromContext(ctx)
	if b {
		msg.Headers = headers
	}

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(p.topic),
	}
	ctx, span := p.tracer.Start(ctx,
		"kafka-pusher",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
	spanContext := span.SpanContext()
	if spanContext.IsValid() {
		propagator := otel.GetTextMapPropagator()
		propagator.Inject(ctx, &Headers{headers: &msg.Headers})

	}
	defer span.End()

	if c.isSync {
		err := p.producer.WriteMessages(ctx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		span.SetStatus(codes.Ok, "")
		return nil, nil
	} else {
		// asynchronous
		p.initExecutor()
		span.SetStatus(codes.Ok, "")
		return nil, p.executor.Add(msg, len(v))
	}
}

func WithChunkSize(chunkSize int) PushOption {
	return func(options *pushOptions) {
		options.chunkSize = chunkSize
	}
}

func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *pushOptions) {
		options.flushInterval = interval
	}
}

func WithSync() queue.CallOptions {
	return func(i interface{}) {
		options, ok := i.(*callOptions)
		if !ok {
			panic(queue.ErrNotSupport)
		}

		options.isSync = true
	}
}

func NewHeadersContext(ctx context.Context, headers ...kafka.Header) context.Context {
	return context.WithValue(ctx, HeaderKey{}, headers)
}

func HeadersFromContext(ctx context.Context) ([]kafka.Header, bool) {
	value := ctx.Value(HeaderKey{})
	if value == nil {
		return nil, false
	}

	return value.([]kafka.Header), true
}

func WithCompletion(completion func(messages []kafka.Message, err error)) PushOption {
	return func(options *pushOptions) {
		options.completion = completion
	}
}

func WithBalancer(balancer Balancer) PushOption {
	return func(options *pushOptions) {
		options.balancer = balancer
	}
}
