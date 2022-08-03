package kafka

import (
	"context"

	"github.com/chenquan/go-queue/internal/xtrace"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/segmentio/kafka-go"
)

type (
	PushOption func(options *pushOptions)

	Pusher struct {
		tracer   trace.Tracer
		producer *kafka.Writer
		metrics  *stat.Metrics
		topic    string
		stopOnce func()
	}

	pushOptions struct {

		// An optional function called when the writer succeeds or fails the
		// delivery of messages to a kafka partition.
		completion               func(messages []kafka.Message, err error)
		balancer                 Balancer
		disableAutoTopicCreation bool
		// Limit on how many messages will be buffered before being sent to a
		// partition.
		//
		// The default is to use a target batch size of 100 messages.
		batchSize int

		// Limit the maximum size of a request in bytes before being sent to
		// a partition.
		//
		// The default is to use a kafka default value of 1048576.
		batchBytes int64

		requiredAcks RequiredAcks
	}

	callOptions struct {
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	tracer := xtrace.Tracer()
	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(addrs...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Snappy,
		Completion:             options.completion,
		AllowAutoTopicCreation: !options.disableAutoTopicCreation,
		RequiredAcks:           options.requiredAcks,
	}

	pusher := &Pusher{
		tracer:   tracer,
		producer: producer,
		topic:    topic,
		metrics:  stat.NewMetrics("kafka-pusher"),
	}

	if options.balancer != nil {
		producer.Balancer = options.balancer
	}

	pusher.stopOnce = syncx.Once(pusher.doStop)

	return pusher
}

func (p *Pusher) Close() error {
	p.stopOnce()
	return nil
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Start() {
}

func (p *Pusher) doStop() {
	err := p.producer.Close()
	if err != nil {
		logx.Error(err)
	}
}

func (p *Pusher) Stop() {
	p.stopOnce()
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

	startTime := timex.Now()
	err := p.producer.WriteMessages(ctx, msg)
	if err != nil {
		p.metrics.AddDrop()

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	p.metrics.Add(
		stat.Task{
			Duration: timex.Since(startTime),
		},
	)
	span.SetStatus(codes.Ok, "")

	return nil, nil
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

func WithDisableAutoTopicCreation() PushOption {
	return func(options *pushOptions) {
		options.disableAutoTopicCreation = true
	}
}

func WithBatchSize(batchSize int) PushOption {
	return func(options *pushOptions) {
		options.batchSize = batchSize
	}
}

func WithBatchBytes(batchBytes int64) PushOption {
	return func(options *pushOptions) {
		options.batchBytes = batchBytes
	}
}

func WithRequiredAcks(requiredAcks RequiredAcks) PushOption {
	return func(options *pushOptions) {
		options.requiredAcks = requiredAcks
	}
}
